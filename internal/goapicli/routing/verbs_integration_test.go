//go:build integration

package routing

// End-to-end tests for the REAL verbs, against a REAL Postgres and a real
// HTTP server.
//
// This file exists because of r1's P3. Four mutations at real call sites
// -- the DSN-leaking flag default restored inside runEnable, the
// schema-agreement preflight disabled, the enabled_named_limit WARNING
// suppressed, and an ORDER BY tiebreak dropped while its phrase stayed in
// a SQL comment -- ALL survived both package suites at 53.2% command
// coverage, because nothing in the suite ever ran a verb end to end. Two
// of those four cannot be killed without a database: the warning is
// printed from the outcomes goapiproof.Enable returns, and the ordering
// is a property of rows a real server returns.
//
// The fixture schema is internal/testsupport/registryschema, shared with
// internal/goapiproof's suite -- one DDL, pinned to the alembic
// migrations by a Python test -- rather than a second hand-kept copy that
// would drift from it with nothing to notice.

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/goapiproof"
	"github.com/full-chaos/dev-health-ops/internal/mcpclass"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/registryschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/routingauditschema"
)

const (
	verbTestBuild = "b18e56fa79cfe20ce0f75df148144b832d92be36"
	// verbTestOperation is a catalog operation: every routing verb refuses it, and status lists it.
	verbTestOperation = "featureFlagTimeseries"
	// verbTestBearer is a syntactically valid effective-principal envelope
	// -- three base64url segments carrying verbTestPrincipalID as `sub` --
	// so CHAOS-5505's audit row can be written for a verb this suite
	// calls. It is NOT signed and would be rejected by a real verifier;
	// these tests never call one (startQueryAPI's fake /buildinfo below
	// accepts any non-empty Authorization header), and EnvelopeSubject
	// itself never verifies the envelope, only decodes it -- see its own
	// doc comment (internal/goapiproof/routing_audit.go) for why that is
	// safe here but would not be outside a verb that already called the
	// real /buildinfo first.
	verbTestBearer      = "eyJhbGciOiJFZERTQSIsImtpZCI6ImdvLWFwaS1lbnZlbG9wZS10ZXN0In0.eyJzdWIiOiJiMGExYzJkMy0wMDAwLTQwMDAtODAwMC0wMDAwMDAwMDAwMDEifQ.c2lnbmF0dXJl" // gitleaks:allow -- fabricated fixture, unsigned, never accepted by any real verifier
	verbTestPrincipalID = "b0a1c2d3-0000-4000-8000-000000000001"
)

func startVerbPostgres(t *testing.T) (*pgxpool.Pool, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate Postgres: %v", err)
		}
	})
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err := registryschema.Create(ctx, pool); err != nil {
		t.Fatal(err)
	}
	// CHAOS-5505: every write verb now also writes an audit row in the
	// SAME transaction, so a verb driven end to end here needs the audit
	// table too -- not just the tables enable/disable/repoint's own
	// preflights read.
	if err := routingauditschema.Create(ctx, pool); err != nil {
		t.Fatal(err)
	}
	return pool, instance.URI
}

// startQueryAPI serves /registry and /buildinfo the way the deployed
// process does. schemaDigest is a parameter so a test can make the two
// planes DISAGREE, which is the whole point of preflight 2.
func startQueryAPI(t *testing.T, schemaDigest string, documentDigest map[string]string) *httptest.Server {
	t.Helper()
	type operation struct {
		Operation      string `json:"operation"`
		DocumentDigest string `json:"document_digest"`
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/registry", func(w http.ResponseWriter, _ *http.Request) {
		operations := make([]operation, 0, len(documentDigest))
		for name, digest := range documentDigest {
			operations = append(operations, operation{Operation: name, DocumentDigest: digest})
		}
		writeJSON(t, w, map[string]any{"schema_digest": schemaDigest, "operations": operations})
	})
	mux.HandleFunc("/buildinfo", func(w http.ResponseWriter, r *http.Request) {
		// The ENVELOPE, exactly as /buildinfo checks it. An unauthenticated
		// read 401s, so a verb that stopped sending the credential fails
		// here rather than silently reading a build from an open endpoint.
		if r.Header.Get("Authorization") == "" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		// `modified` must be explicitly present now --
		// FetchBuildIdentity refuses an absent key rather than defaulting
		// unknown cleanliness to clean. This fixture's whole purpose is to
		// exercise a build the guards should ACCEPT, so it says so.
		writeJSON(t, w, map[string]any{"commit": verbTestBuild, "modified": false, "version": "test", "build_time": "2026-09-10T00:00:00Z"})
	})
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	return server
}

func writeJSON(t *testing.T, w http.ResponseWriter, body any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(body); err != nil {
		t.Errorf("encode response: %v", err)
	}
}

// writeCatalog writes a catalog file with the CANONICAL key spellings.
func writeCatalog(t *testing.T, digests map[string]string) string {
	t.Helper()
	type entry struct {
		Operation string `json:"operation"`
		Digest    string `json:"digest"`
	}
	entries := make([]entry, 0, len(digests))
	for name, digest := range digests {
		entries = append(entries, entry{Operation: name, Digest: digest})
	}
	raw, err := json.Marshal(entries)
	if err != nil {
		t.Fatal(err)
	}
	path := t.TempDir() + "/go_api_operations.json"
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// verbTestClassOperation is the MCP class operation the verbs act on.
var verbTestClassOperation = mcpclass.Operation("analytics")

// enableArgs builds a full, valid `enable` command line for the class operation against the test fixtures, so each
// test below changes exactly ONE thing about it.
func enableArgs(server *httptest.Server, dsn string, extra ...string) []string {
	argv := []string{
		"enable",
		"-operations", verbTestClassOperation,
		"-mode", "canary",
		"-registry-url", server.URL + "/registry",
		"-buildinfo-url", server.URL + "/buildinfo",
		"-postgres-uri", dsn,
		"-recorded-by", "lane-routing-verbs",
		"-review-evidence", "the end-to-end verb fixture",
	}
	return append(argv, extra...)
}

func repointArgs(server *httptest.Server, dsn string, extra ...string) []string {
	argv := []string{
		"repoint",
		"-registry-url", server.URL + "/registry",
		"-buildinfo-url", server.URL + "/buildinfo",
		"-postgres-uri", dsn,
		"-recorded-by", "lane-routing-verbs",
		"-review-evidence", "the end-to-end verb fixture",
	}
	return append(argv, extra...)
}

func queryAPIFor(t *testing.T) *httptest.Server {
	t.Helper()
	return startQueryAPI(t, localSchemaDigest(), map[string]string{verbTestOperation: "9999999999999999999999999999999999999999999999999999999999999999"})
}

func insertClassDecision(t *testing.T, pool *pgxpool.Pool, operation, mode, build string) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), `
		INSERT INTO go_api_class_decision (operation, mode, current_candidate_build, schema_digest, review_evidence, recorded_by)
		VALUES ($1, $2, $3, $4, 'pre-existing decision', 'operator')`, operation, mode, build, localSchemaDigest()); err != nil {
		t.Fatal(err)
	}
}

func assertNoRows(t *testing.T, dsn string) {
	t.Helper()
	for _, table := range []string{"go_api_class_decision", "go_api_routing_audits"} {
		var count int
		if err := queryRow(t, dsn, `SELECT count(*) FROM `+table, &count); err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Fatalf("%d row(s) in %s were written by a run that refused", count, table)
		}
	}
}

func queryRow(t *testing.T, dsn, sql string, into ...any) error {
	t.Helper()
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer pool.Close()
	return pool.QueryRow(ctx, sql).Scan(into...)
}

// PREFLIGHT 2, at its real call site: the planes must hash the same SDL. Guarding the defect of 2026-09-01, where
// a decision recorded against a digest the running binary does not have went unreported.
func TestEnableRefusesWhenThePlanesDisagreeOnTheSchemaDigest(t *testing.T) {
	_, dsn := startVerbPostgres(t)
	t.Setenv(bearerEnvVar, verbTestBearer)

	disagreeing := startQueryAPI(t, "sha256:not-the-digest-this-binary-has", map[string]string{verbTestOperation: "d"})
	_, _, err := captureVerb(t, enableArgs(disagreeing, dsn)...)
	if err == nil {
		t.Fatal("enable wrote while the planes disagreed on the schema digest")
	}
	if exitCodeFor(err) != 3 {
		t.Fatalf("exit %d, want 3", exitCodeFor(err))
	}
	if !strings.Contains(err.Error(), "schema digest MISMATCH") {
		t.Fatalf("refused for a different reason, so preflight 2 is not what stopped it: %v", err)
	}
	assertNoRows(t, dsn)
}

// `enable -dry-run` must never write a decision, and the same command line must get past every preflight against a
// process that agrees.
func TestEnableDryRunNeverWritesADecision(t *testing.T) {
	pool, dsn := startVerbPostgres(t)
	t.Setenv(bearerEnvVar, verbTestBearer)
	seedClassRoot(t, pool, nil)
	server := queryAPIFor(t)

	out, _, err := captureVerb(t, enableArgs(server, dsn, "-dry-run")...)
	if err != nil {
		t.Fatalf("enable -dry-run: %v", err)
	}
	if !strings.Contains(out, "would enable") {
		t.Fatalf("dry-run must say what it WOULD do: %s", out)
	}
	if got := classRowModeOf(t, pool, verbTestClassOperation); got != "shadow" {
		t.Fatalf("a dry run wrote: mode = %s", got)
	}
}

// A catalog operation has no routing state: every verb refuses it with exit 3 and writes nothing.
func TestEveryVerbRefusesACatalogOperationAndWritesNothing(t *testing.T) {
	_, dsn := startVerbPostgres(t)
	t.Setenv(bearerEnvVar, verbTestBearer)
	server := queryAPIFor(t)

	for name, argv := range map[string][]string{
		"enable":  enableArgs(server, dsn, "-operations", verbTestOperation),
		"disable": {"disable", "-postgres-uri", dsn, "-operations", verbTestOperation, "-mode", "python", "-apply", "-recorded-by", "w", "-review-evidence", "y"},
		"repoint": repointArgs(server, dsn, "-operations", verbTestOperation),
		"seed":    {"seed", "-registry-url", server.URL + "/registry", "-buildinfo-url", server.URL + "/buildinfo", "-postgres-uri", dsn, "-operations", verbTestOperation, "-recorded-by", "w", "-review-evidence", "y"},
	} {
		t.Run(name, func(t *testing.T) {
			// enable's -operations flag sits before the helper's own: the later duplicate wins in flag parsing.
			_, _, err := captureVerb(t, argv...)
			if err == nil || exitCodeFor(err) != 3 {
				t.Fatalf("%s = %v (exit %d), want a refusal with exit 3", name, err, exitCodeFor(err))
			}
			if !strings.Contains(err.Error(), "MCP class roots") || !strings.Contains(err.Error(), verbTestOperation) {
				t.Fatalf("%s refused for a different reason: %v", name, err)
			}
			assertNoRows(t, dsn)
		})
	}
}

// -operations is required: there is no "all-registered" for a verb that acts on class roots only.
func TestVerbsRequireAnExplicitClassOperationSet(t *testing.T) {
	_, dsn := startVerbPostgres(t)
	t.Setenv(bearerEnvVar, verbTestBearer)
	server := queryAPIFor(t)
	for name, argv := range map[string][]string{
		"enable":  enableArgs(server, dsn, "-operations", ""),
		"disable": {"disable", "-postgres-uri", dsn, "-mode", "python", "-recorded-by", "w", "-review-evidence", "y"},
		"seed":    {"seed", "-registry-url", server.URL + "/registry", "-buildinfo-url", server.URL + "/buildinfo", "-postgres-uri", dsn, "-recorded-by", "w", "-review-evidence", "y"},
	} {
		t.Run(name, func(t *testing.T) {
			_, _, err := captureVerb(t, argv...)
			if err == nil || !strings.Contains(err.Error(), "-operations is required") {
				t.Fatalf("%s = %v, want the -operations refusal", name, err)
			}
		})
	}
}

// The status verb never refuses, and a database that blocks past -timeout is REPORTED, never waited on.
func TestStatusReturnsWithinItsTimeoutUnderAHeldLock(t *testing.T) {
	pool, dsn := startVerbPostgres(t)
	catalogPath := writeCatalog(t, map[string]string{verbTestOperation: "1111111111111111111111111111111111111111111111111111111111111111"})
	server := startQueryAPI(t, localSchemaDigest(), map[string]string{verbTestOperation: "1111111111111111111111111111111111111111111111111111111111111111"})

	ctx := context.Background()
	holder, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = holder.Rollback(ctx) })
	if _, err := holder.Exec(ctx, `LOCK TABLE public.go_api_class_decision IN ACCESS EXCLUSIVE MODE`); err != nil {
		t.Fatal(err)
	}

	// Run on its own goroutine with a bounded external wait: under the mutation this test exists to kill (the
	// deadline removed) the blocked query never times out, and the lock-releasing cleanup would never run, so a hang
	// must become a clean failure.
	type verbResult struct {
		out string
		err error
	}
	done := make(chan verbResult, 1)
	start := time.Now()
	go func() {
		out, _, err := captureVerb(t, "status", "-registry-url", server.URL+"/registry", "-postgres-uri", dsn, "-catalog", catalogPath, "-timeout", "300ms")
		done <- verbResult{out, err}
	}()

	var out string
	select {
	case result := <-done:
		out = result.out
		if result.err != nil {
			t.Fatalf("status must NEVER refuse, even with the database blocked: %v", result.err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("status did not return within 10s under a held lock -- the deadline is not bounding the blocked query")
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("status took %s under a held lock with -timeout 300ms -- the deadline is not bounding the blocked query", elapsed)
	}
	if !strings.Contains(out, "local schema_digest") || !strings.Contains(out, "go plane schema_digest") || !strings.Contains(out, featureOperationLine(verbTestOperation)) {
		t.Fatalf("the half that needs no database did not print under a held lock:\n%s", out)
	}
	if !strings.Contains(out, "mcp class rows: UNAVAILABLE") {
		t.Fatalf("a query blocked past its deadline must be reported as UNAVAILABLE, not silently dropped:\n%s", out)
	}
}

func featureOperationLine(operation string) string { return operation }

// status reports every catalog operation against the deployed registry: AGREE, MISMATCH and UNREGISTERED each say what
// they are, and an unreachable go plane says UNKNOWN, never a silent "served".
func TestStatusReportsCatalogOperationsAgainstTheDeployedRegistry(t *testing.T) {
	_, dsn := startVerbPostgres(t)
	const agree, drift, missing = "featureFlagTimeseries", "flowMatrix", "hotspots"
	catalogPath := writeCatalog(t, map[string]string{agree: "aaaa", drift: "bbbb", missing: "cccc"})
	server := startQueryAPI(t, localSchemaDigest(), map[string]string{agree: "aaaa", drift: "zzzz"})

	out, _, err := captureVerb(t, "status", "-registry-url", server.URL+"/registry", "-postgres-uri", dsn, "-catalog", catalogPath, "-json")
	if err != nil {
		t.Fatal(err)
	}
	var report statusReport
	if err := json.Unmarshal([]byte(out), &report); err != nil {
		t.Fatalf("status -json did not decode: %v\n%s", err, out)
	}
	got := map[string]statusReportOperation{}
	for _, operation := range report.Operations {
		got[operation.Operation] = operation
	}
	for operation, want := range map[string]struct {
		state  string
		served bool
	}{agree: {"AGREE", true}, drift: {"MISMATCH", true}, missing: {"UNREGISTERED", false}} {
		row := got[operation]
		if row.DeployedDigestState != want.state || row.Served == nil || *row.Served != want.served {
			t.Fatalf("%s = %+v, want %s served=%v", operation, row, want.state, want.served)
		}
	}
	if report.RegistryDBError != nil || report.MCPClassError != nil {
		t.Fatalf("the database half failed: %v / %v", report.RegistryDBError, report.MCPClassError)
	}

	down, _, err := captureVerb(t, "status", "-registry-url", "http://127.0.0.1:1", "-postgres-uri", dsn, "-catalog", catalogPath, "-timeout", "2s", "-json")
	if err != nil {
		t.Fatal(err)
	}
	var unreachable statusReport
	if err := json.Unmarshal([]byte(down), &unreachable); err != nil {
		t.Fatal(err)
	}
	for _, operation := range unreachable.Operations {
		if operation.Served != nil || operation.DeployedDigestState != "UNKNOWN" {
			t.Fatalf("with the go plane unreachable %s = %+v, want served=null and UNKNOWN", operation.Operation, operation)
		}
	}
}

// The class table lists every allowlisted root, with a seeded decision named and the rest MISSING (dark).
func TestStatusListsTheClassDecisionsAndNamesTheMissingOnesDark(t *testing.T) {
	pool, dsn := startVerbPostgres(t)
	insertClassDecision(t, pool, verbTestClassOperation, "canary", verbTestBuild)
	server := queryAPIFor(t)
	catalogPath := writeCatalog(t, map[string]string{verbTestOperation: "1111111111111111111111111111111111111111111111111111111111111111"})

	out, _, err := captureVerb(t, "status", "-registry-url", server.URL+"/registry", "-postgres-uri", dsn, "-catalog", catalogPath)
	if err != nil {
		t.Fatal(err)
	}
	lines := map[string]string{}
	for _, line := range strings.Split(out, "\n") {
		if fields := strings.Fields(line); len(fields) > 1 && mcpclass.IsOperation(fields[0]) {
			lines[fields[0]] = line
		}
	}
	if len(lines) != len(mcpclass.SortedRoots()) {
		t.Fatalf("%d class rows printed, want one per allowlisted root (%d):\n%s", len(lines), len(mcpclass.SortedRoots()), out)
	}
	if line := lines[verbTestClassOperation]; !strings.Contains(line, "MATCH") || !strings.Contains(line, "canary") {
		t.Fatalf("the seeded decision does not read as MATCH canary: %q", line)
	}
	for operation, line := range lines {
		if operation != verbTestClassOperation && !strings.Contains(line, "MISSING") {
			t.Fatalf("%s has no decision and must read MISSING: %q", operation, line)
		}
	}
}

func TestEnableExpectBuildCrossCheckRefusesAMismatch(t *testing.T) {
	_, dsn := startVerbPostgres(t)
	t.Setenv(bearerEnvVar, verbTestBearer)
	server := queryAPIFor(t)

	_, _, err := captureVerb(t, enableArgs(server, dsn, "-expect-build", "0000000000000000000000000000000000000000")...)
	if err == nil || !strings.Contains(err.Error(), "-expect-build") || !strings.Contains(err.Error(), "does not match the running build") {
		t.Fatalf("enable = %v, want the -expect-build cross-check refusal", err)
	}
	assertNoRows(t, dsn)
}

func TestRepointExpectBuildCrossCheckRefusesAMismatchAndAMatchPasses(t *testing.T) {
	pool, dsn := startVerbPostgres(t)
	t.Setenv(bearerEnvVar, verbTestBearer)
	insertClassDecision(t, pool, verbTestClassOperation, "canary", "0000000000000000000000000000000000000001")
	server := queryAPIFor(t)

	_, _, err := captureVerb(t, repointArgs(server, dsn, "-expect-build", "0000000000000000000000000000000000000000")...)
	if err == nil || !strings.Contains(err.Error(), "does not match the running build") {
		t.Fatalf("repoint = %v, want the -expect-build cross-check refusal", err)
	}
	if _, _, err := captureVerb(t, repointArgs(server, dsn, "-expect-build", verbTestBuild, "-dry-run")...); err != nil {
		t.Fatalf("a MATCHING -expect-build must not be refused: %v", err)
	}
}

func TestEnableAndRepointRefuseARegistryThatRegistersNothingAtAll(t *testing.T) {
	_, dsn := startVerbPostgres(t)
	t.Setenv(bearerEnvVar, verbTestBearer)
	empty := startQueryAPI(t, localSchemaDigest(), map[string]string{})
	for name, argv := range map[string][]string{"enable": enableArgs(empty, dsn), "repoint": repointArgs(empty, dsn)} {
		t.Run(name, func(t *testing.T) {
			_, _, err := captureVerb(t, argv...)
			if err == nil || !strings.Contains(err.Error(), "registers no operations") {
				t.Fatalf("%s = %v, want the registers-nothing refusal", name, err)
			}
		})
	}
}

// extractRepointJSON reads repoint's -json line.
func extractRepointJSON(t *testing.T, out string) repointResult {
	t.Helper()
	for _, line := range strings.Split(out, "\n") {
		if rest, ok := strings.CutPrefix(line, repointJSONPrefix); ok {
			var result repointResult
			if err := json.Unmarshal([]byte(rest), &result); err != nil {
				t.Fatalf("repoint -json line did not decode: %v\nline: %s", err, rest)
			}
			return result
		}
	}
	t.Fatalf("no %q line found in repoint's stdout:\n%s", repointJSONPrefix, out)
	return repointResult{}
}

func TestRepointJSONReasonIsRepointedOnSuccessAndRefusedOnARefusal(t *testing.T) {
	pool, dsn := startVerbPostgres(t)
	t.Setenv(bearerEnvVar, verbTestBearer)
	insertClassDecision(t, pool, verbTestClassOperation, "canary", "0000000000000000000000000000000000000001")
	server := queryAPIFor(t)

	out, _, err := captureVerb(t, repointArgs(server, dsn, "-json")...)
	if err != nil {
		t.Fatalf("repoint: %v\n%s", err, out)
	}
	if result := extractRepointJSON(t, out); result.Reason != "repointed" {
		t.Fatalf("reason = %q, want \"repointed\" (out:\n%s)", result.Reason, out)
	}

	empty := startQueryAPI(t, localSchemaDigest(), map[string]string{})
	out, _, err = captureVerb(t, repointArgs(empty, dsn, "-json")...)
	if err == nil {
		t.Fatal("expected a real refusal against a registry that registers nothing at all")
	}
	if result := extractRepointJSON(t, out); result.Reason != "refused" {
		t.Fatalf("reason = %q, want \"refused\" (out:\n%s)", result.Reason, out)
	}
}

// Both endpoints come from the environment alone, and one explicit flag mixed with the environment is refused.
func TestEnableDerivesBothEndpointsFromTheEnvVarAloneNoFlags(t *testing.T) {
	pool, dsn := startVerbPostgres(t)
	t.Setenv(bearerEnvVar, verbTestBearer)
	seedClassRoot(t, pool, nil)
	server := queryAPIFor(t)
	t.Setenv("GO_API_QUERY_API_URL", server.URL)

	out, _, err := captureVerb(t, "enable", "-operations", verbTestClassOperation, "-mode", "canary", "-postgres-uri", dsn,
		"-recorded-by", "lane-routing-verbs", "-review-evidence", "env only", "-dry-run")
	if err != nil {
		t.Fatalf("enable with the endpoints taken from the environment: %v", err)
	}
	if !strings.Contains(out, "would enable") {
		t.Fatalf("out = %s", out)
	}
	_, _, err = captureVerb(t, "enable", "-operations", verbTestClassOperation, "-mode", "canary", "-postgres-uri", dsn,
		"-registry-url", server.URL+"/registry", "-recorded-by", "lane-routing-verbs", "-review-evidence", "mixed", "-dry-run")
	if err == nil {
		t.Fatal("one explicit URL flag mixed with the environment must be refused: the preflight would split across two processes")
	}
}

// A buildinfo refusal never prints the URL path (it can carry a credential-shaped segment).
func TestEnableAndRepointBuildInfoRefusalNeverPrintsTheURLPath(t *testing.T) {
	_, dsn := startVerbPostgres(t)
	t.Setenv(bearerEnvVar, verbTestBearer)
	mux := http.NewServeMux()
	mux.HandleFunc("/registry", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(t, w, map[string]any{"schema_digest": localSchemaDigest(), "operations": []map[string]string{{"operation": verbTestOperation, "document_digest": "d"}}})
	})
	mux.HandleFunc("/secret-looking-segment/buildinfo", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNotFound) })
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	for name, argv := range map[string][]string{
		"enable": {"enable", "-operations", verbTestClassOperation, "-mode", "canary", "-registry-url", server.URL + "/registry", "-buildinfo-url", server.URL + "/secret-looking-segment/buildinfo",
			"-postgres-uri", dsn, "-recorded-by", "w", "-review-evidence", "y"},
		"repoint": {"repoint", "-registry-url", server.URL + "/registry", "-buildinfo-url", server.URL + "/secret-looking-segment/buildinfo",
			"-postgres-uri", dsn, "-recorded-by", "w", "-review-evidence", "y"},
	} {
		t.Run(name, func(t *testing.T) {
			_, errOut, err := captureVerb(t, argv...)
			if err == nil {
				t.Fatal("a refused /buildinfo must refuse the verb")
			}
			if strings.Contains(err.Error()+errOut, "secret-looking-segment") {
				t.Fatalf("the URL path leaked into the refusal: %v / %s", err, errOut)
			}
		})
	}
}

// A guard that is passed but empty must not read as "no guard".
func TestDisableRefusesAnExplicitlyEmptyCandidateBuildGuard(t *testing.T) {
	pool, dsn := startVerbPostgres(t)
	insertClassDecision(t, pool, verbTestClassOperation, "canary", verbTestBuild)

	_, _, err := captureVerb(t, "disable", "-postgres-uri", dsn, "-operations", verbTestClassOperation, "-mode", "python", "-candidate-build", "",
		"-apply", "-recorded-by", "w", "-review-evidence", "y")
	if err == nil || !strings.Contains(err.Error(), "-candidate-build was passed but empty") {
		t.Fatalf("disable = %v, want the empty-guard refusal", err)
	}
	if got := classRowModeOf(t, pool, verbTestClassOperation); got != "canary" {
		t.Fatalf("an unguarded write applied: mode = %s", got)
	}
}

// Each verb names its endpoints and prints one structured line per decision it wrote, with the before-state read under
// the write's own lock.
func TestEnableDisableAndRepointEmitStructuredLinesPerDecision(t *testing.T) {
	pool, dsn := startVerbPostgres(t)
	t.Setenv(bearerEnvVar, verbTestBearer)
	op := seedClassRootWithExcludedShape(t, pool)
	server := queryAPIFor(t)

	out, errOut, err := captureVerb(t, classEnableArgs(server, dsn, "-allow-excluded", allowExcludedOp)...)
	if err != nil {
		t.Fatalf("enable: %v", err)
	}
	if !strings.Contains(out, "registry=") || !strings.Contains(out, "buildinfo=") {
		t.Fatalf("enable must name the endpoints it consulted:\n%s", out)
	}
	if !strings.Contains(errOut, "go_api_routing.enabled operation="+op+" mode_before=shadow mode_after=canary") {
		t.Fatalf("enable's per-decision line must carry the replaced state:\n%s", errOut)
	}

	_, errOut, err = captureVerb(t, "disable", "-postgres-uri", dsn, "-operations", op, "-mode", "python", "-apply", "-recorded-by", "w", "-review-evidence", "y")
	if err != nil {
		t.Fatalf("disable: %v", err)
	}
	if !strings.Contains(errOut, "go_api_routing.disabled operation="+op+" from=canary to=python") {
		t.Fatalf("disable's per-decision line is missing:\n%s", errOut)
	}
	if got := classRowModeOf(t, pool, op); got != "python" {
		t.Fatalf("mode = %s, want python", got)
	}
}

// seed creates one shadow decision, and a second run changes nothing.
func TestSeedCreatesOneShadowDecisionAndASecondRunChangesNothing(t *testing.T) {
	pool, dsn := startVerbPostgres(t)
	t.Setenv(bearerEnvVar, verbTestBearer)
	server := queryAPIFor(t)
	argv := []string{"seed", "-registry-url", server.URL + "/registry", "-buildinfo-url", server.URL + "/buildinfo", "-postgres-uri", dsn,
		"-operations", verbTestClassOperation, "-recorded-by", "lane-routing-verbs", "-review-evidence", "seed fixture"}

	out, errOut, err := captureVerb(t, argv...)
	if err != nil {
		t.Fatalf("seed: %v\n%s", err, errOut)
	}
	if !strings.Contains(out, goapiproof.SeedActionCreated) || classRowModeOf(t, pool, verbTestClassOperation) != goapiproof.SeedMode {
		t.Fatalf("seed did not create a shadow decision:\n%s", out)
	}
	again, _, err := captureVerb(t, argv...)
	if err != nil || !strings.Contains(again, goapiproof.SeedActionAlreadyPresent) {
		t.Fatalf("second seed = %v\n%s, want already-present", err, again)
	}
	var count int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM go_api_class_decision`).Scan(&count); err != nil || count != 1 {
		t.Fatalf("%d decisions after two seeds (err %v), want 1", count, err)
	}
}
