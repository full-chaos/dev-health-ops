package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/full-chaos/dev-health-ops/internal/goapidigest"
	"github.com/full-chaos/dev-health-ops/internal/goapiproof"
	"github.com/full-chaos/dev-health-ops/internal/platform/version"
)

// This file drives run() and parseFlags() end to end, against httptest
// fakes for the registry, /buildinfo, the edge and a fake Postgres pool
// (see dbPool/openPostgresPool in main.go). Nothing here calls
// t.Parallel: every test mutates the package-level flag.CommandLine,
// os.Args and openPostgresPool for the duration of one run() call and
// restores them via t.Cleanup, and two of these mutating a shared global
// at once would race.

// e2ePool is the smallest Postgres double run() needs to be driven end
// to end: readRoutingState's one Query and WriteReceipts' two Exec
// statements per receipt. It satisfies dbPool. It does not implement
// TxBeginner, so WriteAtomic falls back to a direct (non-transactional)
// Write -- enough to prove a receipt lands, without needing a real
// transaction.
type e2ePool struct {
	mu          sync.Mutex
	routingRows [][]any
	execs       []e2eExec
}

type e2eExec struct {
	sql  string
	args []any
}

func (p *e2ePool) Query(context.Context, string, ...any) (pgx.Rows, error) {
	return &fakeRoutingRows{rows: p.routingRows}, nil
}

func (p *e2ePool) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.execs = append(p.execs, e2eExec{sql: sql, args: append([]any(nil), args...)})
	return pgconn.CommandTag{}, nil
}

func (p *e2ePool) QueryRow(context.Context, string, ...any) pgx.Row { return e2eRow{} }

func (p *e2ePool) Close() {}

// proofRunInserts returns the go_api_proof_run rows this pool actually
// received, so a test can assert on what was WRITTEN rather than merely
// on what the report claims was written.
func (p *e2ePool) proofRunInserts() []e2eExec {
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []e2eExec
	for _, e := range p.execs {
		if strings.Contains(e.sql, "INSERT INTO go_api_proof_run") {
			out = append(out, e)
		}
	}
	return out
}

type e2eRow struct{}

func (e2eRow) Scan(...any) error { return nil }

// runCLI drives run() the way an operator invokes the binary: it points
// os.Args at args and gives parseFlags() a fresh flag.CommandLine, so
// every scenario in this file gets its own clean set of -flag
// definitions (parseFlags defines them anew on every call, and the
// package flag.CommandLine cannot accept the same name twice). Both are
// restored on cleanup.
func runCLI(t *testing.T, args []string) error {
	t.Helper()
	originalArgs := os.Args
	originalFlagSet := flag.CommandLine
	t.Cleanup(func() {
		os.Args = originalArgs
		flag.CommandLine = originalFlagSet
	})
	os.Args = append([]string{"go-api-prove"}, args...)
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ContinueOnError)
	return run()
}

// withFakePool substitutes openPostgresPool for the duration of one test,
// so run()'s own Postgres connection step hands back pool instead of
// dialling a real database. Restored on cleanup.
func withFakePool(t *testing.T, pool dbPool) {
	t.Helper()
	original := openPostgresPool
	t.Cleanup(func() { openPostgresPool = original })
	openPostgresPool = func(context.Context, string) (dbPool, error) { return pool, nil }
}

func writeStaticJSONHandler(body string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(body))
	}
}

// e2eBearerHelper writes a helper script printing token on stdout -- the
// -edge-bearer-exec/-proof-bearer-exec mechanism's own contract (JSON
// argv naming a helper, never a shell string; see credentials()'s doc
// comment in main.go). token is JWT-shaped so it passes
// ValidateEnvelopeShape, and it is generated at runtime by the caller
// (syntheticJWT) rather than written as a literal, so no `eyJ...`-shaped
// string sits in the tree for a secret scanner to flag.
func e2eBearerHelper(t *testing.T, token string) string {
	t.Helper()
	return writeHelper(t, fmt.Sprintf("#!/bin/sh\nprintf '%s'\n", token))
}

// jsonArgv renders argv the way -edge-bearer-exec/-proof-bearer-exec
// expect it: a JSON array of strings.
func jsonArgv(t *testing.T, argv ...string) string {
	t.Helper()
	encoded, err := json.Marshal(argv)
	if err != nil {
		t.Fatalf("marshal argv: %v", err)
	}
	return string(encoded)
}

// hotspotsRow/hotspotsBody mirror citation_shape_test.go's own fixtures
// in internal/goapiproof (unexported there, so this file writes the
// same shape rather than importing it): one hotspots row, and the rows
// list wrapped as a full GraphQL response body.
func hotspotsRow(file, repo string, churn int, blame string) string {
	return fmt.Sprintf(`{"filePath":%q,"repoId":%q,"churnCommits30d":%d,"blameConcentration":%s,"riskScore":1.5}`, file, repo, churn, blame)
}

func hotspotsBody(rows ...string) string {
	return `{"data":{"hotspots":{"rows":[` + strings.Join(rows, ",") + `]}}}`
}

// Kill (b): the endpoint-URL validation run() itself performs.
// validateEndpointFlags is unit-tested directly (minter_test.go's
// TestTheCLIsOwnGuardsAreReachable), which proves the FUNCTION refuses a
// credential-bearing endpoint -- it does not prove run() actually CALLS
// it. Deleting `if err := validateEndpointFlags(f); err != nil { return
// err }` from run() leaves that unit test green and this one failing,
// because nothing else in run() ever calls the guard.
func TestRunRefusesACredentialBearingEndpointBeforeAnyHTTPCall(t *testing.T) {
	err := runCLI(t, []string{
		"-registry-url=http://alice:s3cret@127.0.0.1:1/registry",
		"-documents=/nonexistent/documents.json",
		"-org=70d529e0",
		"-artifact-dir=/nonexistent/artifacts",
		"-recorded-by=harness",
		"-review-evidence=e2e harness",
		"-dry-run",
	})
	if err == nil {
		t.Fatal("a credential-bearing -registry-url must be refused before any HTTP call is made")
	}
	if strings.Contains(err.Error(), "s3cret") {
		t.Fatalf("the refusal echoed the credential: %v", err)
	}
	if !strings.Contains(err.Error(), "carries userinfo") {
		t.Fatalf("expected the endpoint-credential refusal (RefuseCredentialsInURL), got: %v", err)
	}
}

// Kill (a): the bracketing deadline on the OPENING /buildinfo read.
// FetchBuildIdentity's ctx there comes from run()'s own boundedCtx()
// closure, which -- per its comment -- exists specifically to bound the
// minting helper invoked before the first measurement, not merely the
// HTTP round trip (the http.Client itself already carries -timeout as
// its own Timeout field, so an HTTP-level stall is bounded either way;
// minting happens BEFORE client.Do and is only bounded by whichever ctx
// FetchBuildIdentity was actually given). Replacing `boundedCtx()` with
// `context.Background()` at that call site lets a hanging helper run
// for mintBearer's own internal 20s cap instead of -timeout: this test's
// elapsed-time bound catches exactly that.
//
// The helper never returns on its own; it is killed by the context this
// test exists to prove is applied, not by a sleep in this test's own
// goroutine.
func TestRunBoundsTheOpeningBuildinfoReadToTheConfiguredTimeout(t *testing.T) {
	registry := httptest.NewServer(writeStaticJSONHandler(
		`{"schema_digest":"sha256:e2e","operations":[{"operation":"featureFlags","document_digest":"sha256:unused"}]}`))
	t.Cleanup(registry.Close)

	hangingHelper := writeHelper(t, "#!/bin/sh\nsleep 300\n")

	started := time.Now()
	err := runCLI(t, []string{
		"-registry-url=" + registry.URL + "/registry",
		"-documents=/nonexistent/documents.json",
		"-org=70d529e0",
		"-artifact-dir=/nonexistent/artifacts",
		"-recorded-by=harness",
		"-review-evidence=e2e harness",
		"-edge-bearer-exec=" + jsonArgv(t, "/bin/true"),
		"-proof-bearer-exec=" + jsonArgv(t, hangingHelper),
		"-timeout=300ms",
		"-dry-run",
	})
	elapsed := time.Since(started)

	if err == nil {
		t.Fatal("expected the opening /buildinfo read's minting step to fail once -timeout elapses")
	}
	if !strings.Contains(err.Error(), "did not finish") {
		t.Fatalf("expected the minting-deadline refusal, got: %v", err)
	}
	// mintBearer's own internal cap is 20s; the bracketing deadline this
	// test pins bounds the run to -timeout (300ms) instead. A generous
	// margin over that, still far short of 20s, is the signal.
	if elapsed > 5*time.Second {
		t.Fatalf("the opening /buildinfo read was not bounded to -timeout: took %s (mintBearer's own cap is 20s)", elapsed)
	}
}

// Kill (d): the happy path. Two operations -- featureFlags (both planes
// agree) and hotspots (a genuine divergence fully covered by hotspots'
// own declared baseline defect, CHAOS-5447's citation of
// data.hotspots.rows) -- proven end to end against fake registry,
// /buildinfo and edge servers plus a fake receipt store, with
// -proof-bearer-exec/-edge-bearer-exec minting fixed tokens from a real
// subprocess. AssertCoverage requires the registry to register every
// operation this package's table covers, so the other known operations
// are registered too, each with no matching document -- they are
// refused by name (document_digest_drift) rather than measured, which is
// the correct, expected shape for an operation this run was not asked to
// exercise.
const e2eBuildSHA = "b18e56fa79cfe20ce0f75df148144b832d92be36"

// withProverCommit stamps this test binary's own build commit the way
// -ldflags stamps a released prover, restored on cleanup.
func withProverCommit(t *testing.T, commit string) {
	t.Helper()
	previous := version.Commit
	version.Commit = commit
	t.Cleanup(func() { version.Commit = previous })
}

func TestRunProvesTwoOperationsEndToEnd(t *testing.T) {
	withProverCommit(t, e2eBuildSHA)
	stdout, runErr, reportPath, pool := runTwoOperationsEndToEnd(t)
	if runErr != nil {
		t.Fatalf("run(): %v\nstdout:\n%s", runErr, stdout)
	}
	assertTwoOperationsProven(t, stdout, reportPath, pool)
}

// runTwoOperationsEndToEnd drives run() against fake registry,
// /buildinfo and edge servers plus a fake receipt store; extraArgs are
// appended to the operator's own flags.
func runTwoOperationsEndToEnd(t *testing.T, extraArgs ...string) (stdout string, runErr error, reportPath string, pool *e2ePool) {
	t.Helper()
	const buildSHA = e2eBuildSHA

	featureFlagsDoc := "query FeatureFlags { featureFlags { key } }"
	hotspotsDoc := "query Hotspots { hotspots { rows { filePath repoId churnCommits30d blameConcentration riskScore } } }"
	featureFlagsDigest := goapidigest.Document(featureFlagsDoc)
	hotspotsDigest := goapidigest.Document(hotspotsDoc)

	type registryOp struct {
		Operation      string `json:"operation"`
		DocumentDigest string `json:"document_digest"`
	}
	var ops []registryOp
	for _, name := range goapiproof.KnownOperations() {
		switch name {
		case "featureFlags":
			ops = append(ops, registryOp{Operation: name, DocumentDigest: featureFlagsDigest})
		case "hotspots":
			ops = append(ops, registryOp{Operation: name, DocumentDigest: hotspotsDigest})
		default:
			// Registered (AssertCoverage requires it) but never given a
			// matching document below, so proveRequest refuses each of
			// these by name (document_digest_drift) instead of measuring
			// them -- this run is only asked to prove two operations.
			ops = append(ops, registryOp{Operation: name, DocumentDigest: "sha256:unused-" + name})
		}
	}
	registryBody, err := json.Marshal(struct {
		SchemaDigest string       `json:"schema_digest"`
		Operations   []registryOp `json:"operations"`
	}{SchemaDigest: "sha256:e2e29d509cd", Operations: ops})
	if err != nil {
		t.Fatalf("marshal registry body: %v", err)
	}
	registry := httptest.NewServer(writeStaticJSONHandler(string(registryBody)))
	t.Cleanup(registry.Close)

	buildinfo := httptest.NewServer(writeStaticJSONHandler(`{"commit":"` + buildSHA + `","modified":false}`))
	t.Cleanup(buildinfo.Close)

	featureFlagsBody := `{"data":{"featureFlags":[{"key":"a"}]}}`
	// A genuine divergence (a different physical row selected on each
	// plane), fully covered by hotspots' own declared baseline defect --
	// the exact cell citation_shape_test.go's own fixture pins as
	// covering zero differences outside the defect.
	hotspotsBaseline := hotspotsBody(hotspotsRow("a.go", "r1", 5, "0.5"), hotspotsRow("b.go", "r1", 2, "0.25"))
	hotspotsCandidate := hotspotsBody(hotspotsRow("a.go", "r1", 9, "0.5"), hotspotsRow("c.go", "r2", 2, "0.75"))

	edge := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		var parsed struct {
			Query string `json:"query"`
		}
		_ = json.Unmarshal(raw, &parsed)
		isBaseline := strings.Contains(parsed.Query, "python-plane control")

		w.Header().Set("x-dev-health-build", buildSHA)
		switch {
		case strings.Contains(parsed.Query, "FeatureFlags"):
			if isBaseline {
				w.Header().Set("x-dev-health-plane", "python")
			} else {
				w.Header().Set("x-dev-health-plane", "go")
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(featureFlagsBody))
		case strings.Contains(parsed.Query, "Hotspots"):
			if isBaseline {
				w.Header().Set("x-dev-health-plane", "python")
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(hotspotsBaseline))
			} else {
				w.Header().Set("x-dev-health-plane", "go")
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(hotspotsCandidate))
			}
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(edge.Close)

	type documentEntry struct {
		Operation string `json:"operation"`
		Document  string `json:"document"`
	}
	docsJSON, err := json.Marshal([]documentEntry{
		{Operation: "featureFlags", Document: featureFlagsDoc},
		{Operation: "hotspots", Document: hotspotsDoc},
	})
	if err != nil {
		t.Fatalf("marshal documents: %v", err)
	}
	docsPath := filepath.Join(t.TempDir(), "documents.json")
	if err := os.WriteFile(docsPath, docsJSON, 0o600); err != nil {
		t.Fatalf("write documents file: %v", err)
	}

	pool = &e2ePool{routingRows: [][]any{
		{"featureFlags", featureFlagsDigest, "canary", buildSHA},
		{"hotspots", hotspotsDigest, "canary", buildSHA},
	}}
	withFakePool(t, pool)

	edgeToken := syntheticJWT(t, map[string]string{"sub": "edge"})
	proofToken := syntheticJWT(t, map[string]string{"sub": "proof"})
	reportPath = filepath.Join(t.TempDir(), "report.json")

	stdout = captureStdout(t, func() {
		runErr = runCLI(t, append([]string{
			"-registry-url=" + registry.URL + "/registry",
			"-buildinfo-url=" + buildinfo.URL + "/buildinfo",
			"-edge-url=" + edge.URL + "/graphql",
			"-documents=" + docsPath,
			"-postgres-uri=postgres://fake/ignored",
			"-org=70d529e0",
			"-artifact-dir=" + t.TempDir(),
			"-recorded-by=harness",
			"-review-evidence=e2e harness run",
			"-edge-bearer-exec=" + jsonArgv(t, e2eBearerHelper(t, edgeToken)),
			"-proof-bearer-exec=" + jsonArgv(t, e2eBearerHelper(t, proofToken)),
			"-report=" + reportPath,
			"-timeout=5s",
		}, extraArgs...))
	})
	return stdout, runErr, reportPath, pool
}

func assertTwoOperationsProven(t *testing.T, stdout, reportPath string, pool *e2ePool) {
	t.Helper()
	if !strings.Contains(stdout, "executed=2") {
		t.Fatalf("stdout summary line must report executed=2: %s", stdout)
	}
	if !strings.Contains(stdout, "receipts_written=2") {
		t.Fatalf("stdout summary line must report receipts_written=2: %s", stdout)
	}

	raw, err := os.ReadFile(reportPath)
	if err != nil {
		t.Fatalf("read report: %v", err)
	}
	var decoded struct {
		Summary struct {
			Executed        int `json:"executed"`
			ReceiptsWritten int `json:"receipts_written"`
		} `json:"summary"`
		Outcomes []struct {
			Operation                        string `json:"operation"`
			TerminalState                    string `json:"terminal_state"`
			ReceiptWritten                   bool   `json:"receipt_written"`
			DifferencesOutsideBaselineDefect int    `json:"differences_outside_baseline_defect"`
		} `json:"outcomes"`
	}
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("decode report: %v", err)
	}
	if decoded.Summary.Executed != 2 {
		t.Fatalf("report summary.executed = %d, want 2:\n%s", decoded.Summary.Executed, raw)
	}
	if decoded.Summary.ReceiptsWritten != 2 {
		t.Fatalf("report summary.receipts_written = %d, want 2:\n%s", decoded.Summary.ReceiptsWritten, raw)
	}

	type outcomeView struct {
		TerminalState  string
		ReceiptWritten bool
		Outside        int
	}
	byOperation := map[string]outcomeView{}
	for _, outcome := range decoded.Outcomes {
		if outcome.Operation == "featureFlags" || outcome.Operation == "hotspots" {
			byOperation[outcome.Operation] = outcomeView{outcome.TerminalState, outcome.ReceiptWritten, outcome.DifferencesOutsideBaselineDefect}
		}
	}
	featureFlags, ok := byOperation["featureFlags"]
	if !ok {
		t.Fatalf("no featureFlags outcome in report:\n%s", raw)
	}
	if featureFlags.TerminalState != goapiproof.TerminalStateMatch || !featureFlags.ReceiptWritten {
		t.Fatalf("featureFlags outcome = %+v, want a written match", featureFlags)
	}
	hotspots, ok := byOperation["hotspots"]
	if !ok {
		t.Fatalf("no hotspots outcome in report:\n%s", raw)
	}
	if hotspots.TerminalState != goapiproof.TerminalStateMismatch || !hotspots.ReceiptWritten {
		t.Fatalf("hotspots outcome = %+v, want a written mismatch", hotspots)
	}
	if hotspots.Outside != 0 {
		t.Fatalf("hotspots' divergence is fully covered by its declared baseline defect; got %d differences outside it", hotspots.Outside)
	}

	// The report claims two receipts were written; this checks the fake
	// store actually RECEIVED two go_api_proof_run inserts, so a bug that
	// counted a write without performing it would still be caught.
	if inserts := pool.proofRunInserts(); len(inserts) != 2 {
		t.Fatalf("fake pool recorded %d go_api_proof_run inserts, want 2: %+v", len(inserts), inserts)
	}
}

// A prover built from another commit than the candidate refuses by name
// before measuring anything: no report, no receipt.
func TestRunRefusesAProverBuiltFromAnotherCommit(t *testing.T) {
	for _, commit := range []string{"a38c5bb70bc926e10059f8b13d63d098d6756ba5", "unknown", ""} {
		t.Run("prover="+commit, func(t *testing.T) {
			withProverCommit(t, commit)
			stdout, runErr, reportPath, pool := runTwoOperationsEndToEnd(t)
			if !errors.Is(runErr, goapiproof.ErrProverBuildSkew) || !strings.Contains(runErr.Error(), proverBuildSkewFlag) {
				t.Fatalf("run() = %v, want ErrProverBuildSkew naming %s\nstdout:\n%s", runErr, proverBuildSkewFlag, stdout)
			}
			if want := goapiproof.NewProverBuild(version.Info{Commit: commit}, e2eBuildSHA, false).Line(); !strings.Contains(stdout, "go-api-prove: "+want) {
				t.Fatalf("stdout must carry the build line %q before refusing:\n%s", want, stdout)
			}
			if _, err := os.Stat(reportPath); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("a refused run wrote a report (stat err %v)", err)
			}
			if inserts := pool.proofRunInserts(); len(inserts) != 0 {
				t.Fatalf("a refused run wrote %d receipts", len(inserts))
			}
		})
	}
}

// The override runs the skewed prover and the report names both commits
// and the allowed skew.
func TestRunWithSkewOverrideRecordsBothCommits(t *testing.T) {
	const proverCommit = "a38c5bb70bc926e10059f8b13d63d098d6756ba5"
	withProverCommit(t, proverCommit)
	stdout, runErr, reportPath, pool := runTwoOperationsEndToEnd(t, proverBuildSkewFlag)
	if runErr != nil {
		t.Fatalf("run(): %v\nstdout:\n%s", runErr, stdout)
	}
	assertTwoOperationsProven(t, stdout, reportPath, pool)
	raw, err := os.ReadFile(reportPath)
	if err != nil {
		t.Fatalf("read report: %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("decode report: %v", err)
	}
	for field, want := range map[string]any{
		"prover_build":              proverCommit,
		"candidate_build":           e2eBuildSHA,
		"prover_build_skew":         true,
		"prover_build_skew_allowed": true,
		"prover_build_modified":     false,
	} {
		if decoded[field] != want {
			t.Fatalf("report %s = %v, want %v", field, decoded[field], want)
		}
	}
}

// -timeout=0 means no per-read deadline on every bounded read of the run
// (/registry, /buildinfo, the stability re-read and each leg).
func TestRunWithZeroTimeoutRunsEndToEnd(t *testing.T) {
	withProverCommit(t, e2eBuildSHA)
	stdout, runErr, reportPath, pool := runTwoOperationsEndToEnd(t, "-timeout=0")
	if runErr != nil {
		t.Fatalf("run(): %v\nstdout:\n%s", runErr, stdout)
	}
	assertTwoOperationsProven(t, stdout, reportPath, pool)
}
