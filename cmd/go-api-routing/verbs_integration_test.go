//go:build integration

package main

// End-to-end tests for the REAL verbs, against a REAL Postgres and a real
// HTTP server.
//
// This file exists because of r1's P3. Four mutations at real call sites
// -- the DSN-leaking flag default restored inside runEnable, the
// schema-agreement preflight disabled, the enabled_unproven WARNING
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
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/goapiproof"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/registryschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/routingauditschema"
)

const (
	verbTestBuild     = "b18e56fa79cfe20ce0f75df148144b832d92be36"
	verbTestOperation = "flowMatrix"
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
	verbTestBearer      = "eyJhbGciOiJFZERTQSIsImtpZCI6ImdvLWFwaS1lbnZlbG9wZS10ZXN0In0.eyJzdWIiOiJiMGExYzJkMy0wMDAwLTQwMDAtODAwMC0wMDAwMDAwMDAwMDEifQ.c2lnbmF0dXJl"
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

// enableArgs builds a full, valid `enable` command line against the test
// fixtures, so each test below changes exactly ONE thing about it.
func enableArgs(server *httptest.Server, dsn, catalogPath string, extra ...string) []string {
	argv := []string{
		"enable",
		"-operations", verbTestOperation,
		"-mode", "canary",
		"-registry-url", server.URL + "/registry",
		"-buildinfo-url", server.URL + "/buildinfo",
		"-postgres-uri", dsn,
		"-catalog", catalogPath,
		"-recorded-by", "lane-routing-verbs",
		"-review-evidence", "the end-to-end verb fixture",
	}
	return append(argv, extra...)
}

// PREFLIGHT 2, at its real call site: the planes must hash the same SDL.
//
// r1's M9 disabled this check inside runEnable and BOTH package suites
// stayed green. It is the guard against the defect of 2026-09-01 -- rows
// keyed by a schema_digest the running binary does not have, so every
// dispatch misses and nothing reports it -- which is the single most
// expensive failure this whole surface exists to prevent.
func TestEnableRefusesWhenThePlanesDisagreeOnTheSchemaDigest(t *testing.T) {
	_, dsn := startVerbPostgres(t)
	digest := "0000000000000000000000000000000000000000000000000000000000000000"
	catalogPath := writeCatalog(t, map[string]string{verbTestOperation: digest})
	t.Setenv(bearerEnvVar, verbTestBearer)

	// The running process reports a DIFFERENT schema digest from this
	// binary's embedded SDL.
	disagreeing := startQueryAPI(t, "sha256:not-the-digest-this-binary-has", map[string]string{verbTestOperation: digest})
	_, _, err := captureVerb(t, enableArgs(disagreeing, dsn, catalogPath)...)
	if err == nil {
		t.Fatal("enable wrote rows while the planes disagreed on the schema digest -- this is the 2026-09-01 defect")
	}
	if exitCodeFor(err) != 2 {
		t.Fatalf("exit %d, want 2", exitCodeFor(err))
	}
	if !strings.Contains(err.Error(), "schema digest MISMATCH") {
		t.Fatalf("refused for a different reason, so preflight 2 is not what stopped it: %v", err)
	}
	assertNoRows(t, dsn)

	// AGREEING is not enough on its own: the same command against a
	// process reporting THIS binary's digest must get past preflight 2.
	// Without this half, deleting the rows the guard protects would also
	// pass the assertion above.
	agreeing := startQueryAPI(t, localSchemaDigest(), map[string]string{verbTestOperation: digest})
	_, _, err = captureVerb(t, enableArgs(agreeing, dsn, catalogPath, "-acknowledge-unproven", "-dry-run")...)
	if err != nil {
		t.Fatalf("enable refused against an AGREEING process: %v", err)
	}
}

// `enable -dry-run` must never write a row. Every existing `-dry-run`
// fixture checks stdout/exit code but never counts rows afterwards, so a
// build that silently applies on a dry run looked identical to a correct
// one. `disable`'s opt-in half of this same invariant is already pinned by
// assertNoRows; this is the opt-out half.
func TestEnableDryRunNeverWritesARow(t *testing.T) {
	_, dsn := startVerbPostgres(t)
	digest := "9999999999999999999999999999999999999999999999999999999999999999"
	catalogPath := writeCatalog(t, map[string]string{verbTestOperation: digest})
	t.Setenv(bearerEnvVar, verbTestBearer)
	server := startQueryAPI(t, localSchemaDigest(), map[string]string{verbTestOperation: digest})

	out, _, err := captureVerb(t, enableArgs(server, dsn, catalogPath, "-acknowledge-unproven", "-dry-run")...)
	if err != nil {
		t.Fatalf("enable -dry-run: %v", err)
	}
	if !strings.Contains(out, "would enable") {
		t.Fatalf("dry-run must say what it WOULD do: %s", out)
	}
	assertNoRows(t, dsn)
}

// The enabled_unproven WARNING is emitted, per row, at its real call site.
//
// r1's M11 suppressed it and both suites stayed green. It is the only
// signal at the moment of the decision that an operator turned an
// operation on with no deployed-executed proof; `status` reports UNPROVEN
// afterwards, but nothing else says it HAPPENED.
func TestEnableWarnsOnEveryUnprovenRowItActuallyWrites(t *testing.T) {
	_, dsn := startVerbPostgres(t)
	digest := "1111111111111111111111111111111111111111111111111111111111111111"
	catalogPath := writeCatalog(t, map[string]string{verbTestOperation: digest})
	t.Setenv(bearerEnvVar, verbTestBearer)
	server := startQueryAPI(t, localSchemaDigest(), map[string]string{verbTestOperation: digest})

	// Without the acknowledgement, an unproven build is REFUSED outright.
	_, _, err := captureVerb(t, enableArgs(server, dsn, catalogPath)...)
	if err == nil {
		t.Fatal("enable wrote a row for a build with no deployed_executed proof and no acknowledgement")
	}
	assertNoRows(t, dsn)

	// With it, the row is written AND the warning names the operation.
	out, errOut, err := captureVerb(t, enableArgs(server, dsn, catalogPath, "-acknowledge-unproven")...)
	if err != nil {
		t.Fatalf("enable -acknowledge-unproven: %v", err)
	}
	for _, want := range []string{
		"WARNING:",
		"go_api_routing.enabled_unproven",
		"operation=" + verbTestOperation,
		"stage_evidence=none",
		"candidate_build=" + verbTestBuild,
		"dry_run=false",
	} {
		if !strings.Contains(errOut, want) {
			t.Fatalf("the unproven warning is missing %q -- an acknowledged-unproven enablement left no signal at the moment it happened.\nstderr:\n%s", want, errOut)
		}
	}
	if !strings.Contains(out, "(UNPROVEN)") {
		t.Fatalf("the report does not mark the row UNPROVEN:\n%s", out)
	}

	// ...and the acknowledgement is DURABLE, not merely logged: the row's
	// own review evidence carries it, which is what `status` reads to keep
	// saying UNPROVEN for as long as the row is in force.
	var evidence string
	if err := queryRow(t, dsn, `SELECT review_evidence FROM go_api_routing_state`, &evidence); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(evidence, "ACKNOWLEDGED-UNPROVEN: ") {
		t.Fatalf("review_evidence = %q, want the ACKNOWLEDGED-UNPROVEN prefix", evidence)
	}
}

// The whole reason status.go:155 covers the census
// AND the classification with one deadline: a diagnostic
// that never returns is worse than one that returns bad news. r2's M23
// replaced that `context.WithTimeout` with a cancellation-only context
// and BOTH suites stayed green, because nothing in either suite ever put
// a real lock on the table and measured how long `status` took. Executed
// against a real `LOCK TABLE go_api_routing_state IN ACCESS EXCLUSIVE
// MODE` held by another session: original `status -timeout 100ms -json`
// returned in ~0.1s with a timeout report; M23's mutant returned nothing
// until the external 2s test-harness cutoff.
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
	if _, err := holder.Exec(ctx, `LOCK TABLE public.go_api_routing_state IN ACCESS EXCLUSIVE MODE`); err != nil {
		t.Fatal(err)
	}

	// captureVerb calls run(argv) SYNCHRONOUSLY, and
	// the lock this test holds is only released by a t.Cleanup that fires
	// when the test function RETURNS -- so under the exact mutation this
	// test exists to kill (the deadline removed), the blocked query never
	// times out, captureVerb never returns, the test function never
	// returns, and the lock-releasing cleanup never runs: a genuine
	// deadlock, not a slow test. The reviewer's own repro needed to
	// cancel the query externally to get an assertion failure at all.
	// Running the call on its own goroutine with a bounded external wait
	// turns that hang into a clean, fast test FAILURE instead.
	type verbResult struct {
		out, errOut string
		err         error
	}
	done := make(chan verbResult, 1)
	start := time.Now()
	go func() {
		out, errOut, err := captureVerb(t,
			"status",
			"-registry-url", server.URL+"/registry",
			"-postgres-uri", dsn,
			"-catalog", catalogPath,
			"-timeout", "300ms",
		)
		done <- verbResult{out, errOut, err}
	}()

	var out string
	select {
	case result := <-done:
		out = result.out
		if result.err != nil {
			t.Fatalf("status must NEVER refuse, even with the database blocked: %v", result.err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("status did not return within 10s under a held lock -- the deadline is not bounding the blocked query (this would otherwise hang forever, not just fail slowly)")
	}
	elapsed := time.Since(start)
	// The bound is the verb's OWN -timeout, not the test harness's external
	// cutoff. Comfortably above 300ms to absorb scheduling jitter, and
	// nowhere near the 2s the mutant needed to be caught at.
	if elapsed > 2*time.Second {
		t.Fatalf("status took %s under a held lock with -timeout 300ms -- the deadline is not bounding the blocked query", elapsed)
	}
	// The HALF that needs no database -- this binary's own digest and the
	// live comparison against /registry -- must still print. r2 R2-04's
	// whole point: one failure must not erase a fact that already succeeded.
	if !strings.Contains(out, "local schema_digest") || !strings.Contains(out, "go plane schema_digest") {
		t.Fatalf("the DB-independent half did not print under a held lock:\n%s", out)
	}
	if !strings.Contains(out, "registry database") || !strings.Contains(out, "UNREACHABLE") {
		t.Fatalf("a query blocked past its deadline must be reported as UNREACHABLE, not silently dropped:\n%s", out)
	}
}

// r2 mutation ledger (M20, SURVIVED): preflight 3's "does the running
// process register every named operation" half, at its real call site.
// Nothing in either suite ever drove `enable` against a registry that is
// missing the requested operation entirely -- as opposed to registering
// it under a DIFFERENT document digest, which is the digest-divergence
// half covered by TestEnableRefusesADocumentDigestDivergentFromTheCatalog
// below.
func TestEnableRefusesAnOperationTheRunningProcessDoesNotRegister(t *testing.T) {
	_, dsn := startVerbPostgres(t)
	digest := "2222222222222222222222222222222222222222222222222222222222222222"
	catalogPath := writeCatalog(t, map[string]string{verbTestOperation: digest})
	t.Setenv(bearerEnvVar, verbTestBearer)
	// The running process registers SOME operation, agreeing on the
	// schema digest -- but not the one this run asks for. Without at
	// least one operation, enable's OWN "registers no operations" check
	// (moved out of the shared FetchRegistry so status can read an
	// empty registry's schema digest without being refused) would fire
	// first, which would prove nothing about preflight 3.
	server := startQueryAPI(t, localSchemaDigest(), map[string]string{"someOtherOperation": "3333333333333333333333333333333333333333333333333333333333333333"})

	_, _, err := captureVerb(t, enableArgs(server, dsn, catalogPath)...)
	if err == nil {
		t.Fatal("enable wrote a row for an operation the running query-api does not register at all")
	}
	if !strings.Contains(err.Error(), "does not register") {
		t.Fatalf("refused for a different reason, so preflight 3's not-registered half is not what stopped it: %v", err)
	}
	assertNoRows(t, dsn)
}

// r2 mutation ledger (M21, explicitly reported P3): preflight 3's OTHER
// half -- the running process registers the operation, but under a
// document digest that diverges from the edge's catalog. A row written
// with the catalog's digest here would never be looked up by the
// deployed binary.
func TestEnableRefusesADocumentDigestDivergentFromTheCatalog(t *testing.T) {
	_, dsn := startVerbPostgres(t)
	catalogDigest := "4444444444444444444444444444444444444444444444444444444444444444"
	registryDigest := "5555555555555555555555555555555555555555555555555555555555555555"
	catalogPath := writeCatalog(t, map[string]string{verbTestOperation: catalogDigest})
	t.Setenv(bearerEnvVar, verbTestBearer)
	server := startQueryAPI(t, localSchemaDigest(), map[string]string{verbTestOperation: registryDigest})

	_, _, err := captureVerb(t, enableArgs(server, dsn, catalogPath)...)
	if err == nil {
		t.Fatal("enable wrote a row while the catalog's document digest diverged from what the running process registers")
	}
	if !strings.Contains(err.Error(), "document digest MISMATCH") {
		t.Fatalf("refused for a different reason, so preflight 3's digest-divergence half is not what stopped it: %v", err)
	}
	assertNoRows(t, dsn)
}

// r2 mutation ledger (M22, explicitly reported P3): `-expect-build` is a
// CROSS-CHECK on the build read from /buildinfo, at its real call site in
// runEnable -- never the source of the value written (team-lead ruling
// R51). A mismatch must refuse before anything is written.
func TestEnableExpectBuildCrossCheckRefusesAMismatch(t *testing.T) {
	_, dsn := startVerbPostgres(t)
	digest := "6666666666666666666666666666666666666666666666666666666666666666"
	catalogPath := writeCatalog(t, map[string]string{verbTestOperation: digest})
	t.Setenv(bearerEnvVar, verbTestBearer)
	server := startQueryAPI(t, localSchemaDigest(), map[string]string{verbTestOperation: digest})

	_, _, err := captureVerb(t, enableArgs(server, dsn, catalogPath, "-expect-build", "0000000000000000000000000000000000000000")...)
	if err == nil {
		t.Fatal("enable wrote a row while -expect-build did not match the running build")
	}
	if !strings.Contains(err.Error(), "-expect-build") || !strings.Contains(err.Error(), "does not match the running build") {
		t.Fatalf("refused for a different reason, so the -expect-build cross-check is not what stopped it: %v", err)
	}
	assertNoRows(t, dsn)

	// The SAME build passed as -expect-build must not be refused: this is
	// a cross-check, not an extra unconditional refusal.
	_, _, err = captureVerb(t, enableArgs(server, dsn, catalogPath, "-expect-build", verbTestBuild, "-acknowledge-unproven", "-dry-run")...)
	if err != nil {
		t.Fatalf("a MATCHING -expect-build must not be refused: %v", err)
	}
}

// r2 mutation ledger (M80, explicitly reported P3): the warning loop's
// own test supplies only ONE unproven row, so a mutation that stops the
// warning/count loop after its first iteration is indistinguishable from
// correct there. Two operations discriminate: M80 would print/mark only
// the first.
func TestEnableWarnsOnEveryUnprovenRowAcrossMultipleOperations(t *testing.T) {
	_, dsn := startVerbPostgres(t)
	const secondOperation = "hotspots"
	digestA := "7777777777777777777777777777777777777777777777777777777777777777"
	digestB := "8888888888888888888888888888888888888888888888888888888888888888"
	catalogPath := writeCatalog(t, map[string]string{
		verbTestOperation: digestA,
		secondOperation:   digestB,
	})
	t.Setenv(bearerEnvVar, verbTestBearer)
	server := startQueryAPI(t, localSchemaDigest(), map[string]string{
		verbTestOperation: digestA,
		secondOperation:   digestB,
	})

	out, errOut, err := captureVerb(t, enableArgs(server, dsn, catalogPath,
		"-operations", verbTestOperation+","+secondOperation,
		"-acknowledge-unproven")...)
	if err != nil {
		t.Fatalf("enable -acknowledge-unproven: %v", err)
	}
	for _, op := range []string{verbTestOperation, secondOperation} {
		want := "operation=" + op
		if !strings.Contains(errOut, want) {
			t.Fatalf("the unproven warning for %s is missing -- the warning loop must not stop after the first operation.\nstderr:\n%s", op, errOut)
		}
	}
	if got := strings.Count(out, "(UNPROVEN)"); got != 2 {
		t.Fatalf("want both rows reported UNPROVEN, got %d marker(s):\n%s", got, out)
	}
}

// Pairwise-knob sweep (23:4xZ amendment): enable's -expect-build cross-check
// (TestEnableExpectBuildCrossCheckRefusesAMismatch above) had an end-to-end
// killer; repoint's OWN -expect-build -- the same knob, same contract,
// checked by a different validate() -- did not: only a unit-level test
// exercised RepointRequest.validate() directly, never runRepoint's real
// flag parse + real /buildinfo fetch + real dispatch. Same class M22 fixed
// for enable, found here by asking "which OTHER verb has this knob".
func TestRepointExpectBuildCrossCheckRefusesAMismatch(t *testing.T) {
	_, dsn := startVerbPostgres(t)
	digest := "9999999999999999999999999999999999999999999999999999999999999999"
	catalogPath := writeCatalog(t, map[string]string{verbTestOperation: digest})
	t.Setenv(bearerEnvVar, verbTestBearer)
	server := startQueryAPI(t, localSchemaDigest(), map[string]string{verbTestOperation: digest})

	// repoint refuses on an empty registry (ErrRepointNoRows), so seed one
	// row first -- enable is the fixture's own way to do that.
	if _, _, err := captureVerb(t, enableArgs(server, dsn, catalogPath, "-acknowledge-unproven")...); err != nil {
		t.Fatalf("seeding enable: %v", err)
	}

	_, _, err := captureVerb(t,
		"repoint",
		"-registry-url", server.URL+"/registry",
		"-buildinfo-url", server.URL+"/buildinfo",
		"-postgres-uri", dsn,
		"-recorded-by", "lane-routing-verbs",
		"-review-evidence", "repoint -expect-build killer",
		"-expect-build", "0000000000000000000000000000000000000000",
	)
	if err == nil {
		t.Fatal("repoint wrote/reported while -expect-build did not match the running build")
	}
	if !strings.Contains(err.Error(), "does not match the running build") {
		t.Fatalf("refused for a different reason, so the -expect-build cross-check is not what stopped it: %v", err)
	}

	// A MATCHING value must not be refused.
	_, _, err = captureVerb(t,
		"repoint",
		"-registry-url", server.URL+"/registry",
		"-buildinfo-url", server.URL+"/buildinfo",
		"-postgres-uri", dsn,
		"-recorded-by", "lane-routing-verbs",
		"-review-evidence", "repoint -expect-build killer",
		"-expect-build", verbTestBuild,
		"-dry-run",
	)
	if err != nil {
		t.Fatalf("a MATCHING -expect-build must not be refused: %v", err)
	}
}

// `printStatusText`'s PROOF column has no killer at
// its real call site -- both full suites stayed green with `if
// operation.Proven` mutated to `if false && operation.Proven`, which
// makes the text report label every matching row UNPROVEN even when a
// real deployed_executed/match proof exists. Function coverage for
// printStatusText was only 22.9%. This drives status in TEXT mode (no
// -json) against a row that is GENUINELY proven and asserts the real
// printed line, not the JSON projection main_test.go already covers.
func TestStatusTextMarksAGenuinelyProvenRowOkNotUnproven(t *testing.T) {
	pool, dsn := startVerbPostgres(t)
	digest := "8888888888888888888888888888888888888888888888888888888888888887"
	catalogPath := writeCatalog(t, map[string]string{verbTestOperation: digest})
	t.Setenv(bearerEnvVar, verbTestBearer)
	server := startQueryAPI(t, localSchemaDigest(), map[string]string{verbTestOperation: digest})

	ctx := context.Background()
	// Register the candidate build directly (enable's own preflight would
	// refuse before ever registering it, since no proof exists yet) so
	// the receipt's foreign key has something to reference.
	if _, err := pool.Exec(ctx, `
		INSERT INTO public.go_api_candidate_build (schema_digest, document_digest, selected_operation, candidate_build)
		VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING`,
		localSchemaDigest(), digest, verbTestOperation, verbTestBuild); err != nil {
		t.Fatalf("register candidate build: %v", err)
	}
	if _, err := goapiproof.Write(ctx, pool, goapiproof.Receipt{
		SchemaDigest:      localSchemaDigest(),
		DocumentDigest:    digest,
		SelectedOperation: verbTestOperation,
		CandidateBuild:    verbTestBuild,
		RequestIdentity:   "status-text-proven-row",
		Stage:             goapiproof.EnablementProofStage,
		TerminalState:     goapiproof.EnablementProofTerminalState,
		MeasurementRoute:  goapiproof.RouteEdge,
		BuildBinding:      goapiproof.EdgeBuildPresent,
		RecordedBy:        "lane-routing-verbs",
		ReviewEvidence:    "r3 P3 killer: genuinely proven row",
	}); err != nil {
		t.Fatalf("write receipt: %v", err)
	}

	// enable WITHOUT -acknowledge-unproven: the proof above must be what
	// lets this succeed, proving the row really is proven, not just
	// asserted to be.
	if _, _, err := captureVerb(t, enableArgs(server, dsn, catalogPath)...); err != nil {
		t.Fatalf("enable a genuinely proven operation must succeed without acknowledgement: %v", err)
	}

	out, _, err := captureVerb(t, "status", "-registry-url", server.URL+"/registry", "-postgres-uri", dsn, "-catalog", catalogPath)
	if err != nil {
		t.Fatalf("status must never refuse: %v", err)
	}
	for _, line := range strings.Split(out, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), verbTestOperation) {
			if strings.Contains(line, "UNPROVEN") {
				t.Fatalf("a genuinely proven row printed UNPROVEN in the text report:\n%s", out)
			}
			if !strings.Contains(line, "ok") {
				t.Fatalf("a genuinely proven row's PROOF column is not 'ok':\n%s", out)
			}
			return
		}
	}
	t.Fatalf("no line for %s found in the text report:\n%s", verbTestOperation, out)
}

func assertNoRows(t *testing.T, dsn string) {
	t.Helper()
	var count int
	if err := queryRow(t, dsn, `SELECT count(*) FROM go_api_routing_state`, &count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("%d row(s) were written by a run that refused", count)
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

// `status` must not classify an operation purely from
// the LOCAL catalog's document digest, discarding the deployed registry's
// own per-operation digest after checking only schema_digest -- so a
// deployed plane that agreed on schema_digest but registered a DIFFERENT
// document digest for one operation still printed that row healthy, MATCH,
// "ok". `enable`'s own preflight (the same map, cmd/go-api-routing/enable.go)
// refuses exactly this disagreement. This is the reviewer's own executed
// repro, reproduced here as a real end-to-end fixture: real Postgres, real
// HTTP registry, a real proven receipt, then the deployed registry's
// document digest for the SAME operation drifts while schema_digest does
// not move.
func TestStatusReportsDeployedDocumentDigestMismatchNotHealthy(t *testing.T) {
	pool, dsn := startVerbPostgres(t)
	digest := "8888888888888888888888888888888888888888888888888888888888888887"
	catalogPath := writeCatalog(t, map[string]string{verbTestOperation: digest})
	t.Setenv(bearerEnvVar, verbTestBearer)
	documentDigests := map[string]string{verbTestOperation: digest}
	server := startQueryAPI(t, localSchemaDigest(), documentDigests)

	ctx := context.Background()
	if _, err := pool.Exec(ctx, `
		INSERT INTO public.go_api_candidate_build (schema_digest, document_digest, selected_operation, candidate_build)
		VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING`,
		localSchemaDigest(), digest, verbTestOperation, verbTestBuild); err != nil {
		t.Fatalf("register candidate build: %v", err)
	}
	if _, err := goapiproof.Write(ctx, pool, goapiproof.Receipt{
		SchemaDigest:      localSchemaDigest(),
		DocumentDigest:    digest,
		SelectedOperation: verbTestOperation,
		CandidateBuild:    verbTestBuild,
		RequestIdentity:   "status-deployed-digest-mismatch",
		Stage:             goapiproof.EnablementProofStage,
		TerminalState:     goapiproof.EnablementProofTerminalState,
		MeasurementRoute:  goapiproof.RouteEdge,
		BuildBinding:      goapiproof.EdgeBuildPresent,
		RecordedBy:        "lane-routing-verbs",
		ReviewEvidence:    "r4 P1 killer: deployed document digest drift after enablement",
	}); err != nil {
		t.Fatalf("write receipt: %v", err)
	}
	if _, _, err := captureVerb(t, enableArgs(server, dsn, catalogPath)...); err != nil {
		t.Fatalf("enable a genuinely proven operation must succeed: %v", err)
	}

	// The deployed plane's document digest for the SAME operation now
	// drifts -- schema_digest (embedded in `server`'s handler, unrelated
	// to this map) does NOT move, so `planes_agree` stays true. This is
	// exactly the reviewer's repro shape.
	documentDigests[verbTestOperation] = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"

	// enable -dry-run must refuse on the same drift -- the control this
	// test's assertion is measured against.
	if _, _, err := captureVerb(t, append(enableArgs(server, dsn, catalogPath), "-dry-run")...); err == nil || !strings.Contains(err.Error(), "document digest MISMATCH") {
		t.Fatalf("control: enable -dry-run must refuse with a document digest MISMATCH once the deployed plane drifts, got err=%v", err)
	}

	out, _, err := captureVerb(t, "status", "-registry-url", server.URL+"/registry", "-postgres-uri", dsn, "-catalog", catalogPath)
	if err != nil {
		t.Fatalf("status must never refuse: %v", err)
	}
	for _, line := range strings.Split(out, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), verbTestOperation) {
			if strings.Contains(line, "ok") {
				t.Fatalf("status printed 'ok' for an operation enable would refuse right now:\n%s", out)
			}
			break
		}
	}
	if !strings.Contains(out, "DEPLOYED document digest MISMATCH") {
		t.Fatalf("the text report never named the deployed digest disagreement:\n%s", out)
	}

	jsonOut, _, err := captureVerb(t, "status", "-registry-url", server.URL+"/registry", "-postgres-uri", dsn, "-catalog", catalogPath, "-json")
	if err != nil {
		t.Fatalf("status -json must never refuse: %v", err)
	}
	var report struct {
		PlanesAgree *bool `json:"planes_agree"`
		Operations  []struct {
			Operation           string `json:"operation"`
			DigestState         string `json:"digest_state"`
			Reachable           *bool  `json:"reachable"`
			DeployedDigestState string `json:"deployed_digest_state"`
		} `json:"operations"`
	}
	if err := json.Unmarshal([]byte(jsonOut), &report); err != nil {
		t.Fatalf("decode status -json: %v\n%s", err, jsonOut)
	}
	if report.PlanesAgree == nil || !*report.PlanesAgree {
		t.Fatalf("this fixture's schema_digest must still agree -- the drift is in the PER-OPERATION document digest only: %+v", report.PlanesAgree)
	}
	for _, operation := range report.Operations {
		if operation.Operation != verbTestOperation {
			continue
		}
		if operation.DigestState != "MATCH" {
			t.Fatalf("this row's schema-level classification must still be MATCH -- only the deployed document digest drifted: %+v", operation)
		}
		if operation.DeployedDigestState != "MISMATCH" {
			t.Fatalf("deployed_digest_state = %q, want MISMATCH", operation.DeployedDigestState)
		}
		if operation.Reachable == nil || *operation.Reachable {
			t.Fatalf("reachable must be false (known, not unknown) for an operation enable would refuse right now -- the exact 'output that merely looks healthy' the reviewer found, got %v", operation.Reachable)
		}
		return
	}
	t.Fatalf("no operation %s in the JSON report:\n%s", verbTestOperation, jsonOut)
}

// The code at status.go's text
// PROOF column already handles DeployedDigestState == "UNREGISTERED"
// correctly, it was simply unpinned): the SAME "output that merely looks
// healthy" class as TestStatusReportsDeployedDocumentDigestMismatchNotHealthy
// above, but for the deployed plane not registering the operation AT ALL
// (UNREGISTERED) rather than registering it under a different digest
// (MISMATCH) -- a genuinely proven, enabled row whose operation the
// deployed plane later stops serving entirely.
func TestStatusTextReportsMismatchWhenTheDeployedPlaneStopsRegisteringTheOperation(t *testing.T) {
	pool, dsn := startVerbPostgres(t)
	digest := "8888888888888888888888888888888888888888888888888888888888888886"
	catalogPath := writeCatalog(t, map[string]string{verbTestOperation: digest})
	t.Setenv(bearerEnvVar, verbTestBearer)
	documentDigests := map[string]string{verbTestOperation: digest}
	server := startQueryAPI(t, localSchemaDigest(), documentDigests)

	ctx := context.Background()
	if _, err := pool.Exec(ctx, `
		INSERT INTO public.go_api_candidate_build (schema_digest, document_digest, selected_operation, candidate_build)
		VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING`,
		localSchemaDigest(), digest, verbTestOperation, verbTestBuild); err != nil {
		t.Fatalf("register candidate build: %v", err)
	}
	if _, err := goapiproof.Write(ctx, pool, goapiproof.Receipt{
		SchemaDigest:      localSchemaDigest(),
		DocumentDigest:    digest,
		SelectedOperation: verbTestOperation,
		CandidateBuild:    verbTestBuild,
		RequestIdentity:   "status-deployed-unregistered",
		Stage:             goapiproof.EnablementProofStage,
		TerminalState:     goapiproof.EnablementProofTerminalState,
		MeasurementRoute:  goapiproof.RouteEdge,
		BuildBinding:      goapiproof.EdgeBuildPresent,
		RecordedBy:        "lane-routing-verbs",
		ReviewEvidence:    "r8 T1a killer: deployed plane stops registering the operation",
	}); err != nil {
		t.Fatalf("write receipt: %v", err)
	}
	if _, _, err := captureVerb(t, enableArgs(server, dsn, catalogPath)...); err != nil {
		t.Fatalf("enable a genuinely proven operation must succeed: %v", err)
	}

	// The deployed plane now stops registering the operation at all --
	// UNREGISTERED, not MISMATCH. schema_digest is unrelated to this map,
	// so planes_agree stays true.
	delete(documentDigests, verbTestOperation)

	out, _, err := captureVerb(t, "status", "-registry-url", server.URL+"/registry", "-postgres-uri", dsn, "-catalog", catalogPath)
	if err != nil {
		t.Fatalf("status must never refuse: %v", err)
	}
	for _, line := range strings.Split(out, "\n") {
		if !strings.HasPrefix(strings.TrimSpace(line), verbTestOperation) {
			continue
		}
		if !strings.HasSuffix(strings.TrimRight(line, " \t"), "MISMATCH") {
			t.Fatalf("the text PROOF column must read MISMATCH when the deployed plane does not register this operation at all, got:\n%s", line)
		}
		return
	}
	t.Fatalf("no line for %s found in the text report:\n%s", verbTestOperation, out)
}

// `enable`'s Preflight 1 must not rely entirely on
// `FetchRegistry` refusing an empty `operations` array by itself; now
// that the refusal moved out of the shared reader (so `status` can read
// an otherwise-empty registry's schema digest -- see registry_test.go's
// TestFetchRegistryAcceptsAnEmptyRegistration), `enable` needs its OWN
// check at the real call site, not just a unit test of the removed
// behavior.
func TestEnableRefusesARegistryThatRegistersNothingAtAll(t *testing.T) {
	_, dsn := startVerbPostgres(t)
	digest := "4444444444444444444444444444444444444444444444444444444444444444"
	catalogPath := writeCatalog(t, map[string]string{verbTestOperation: digest})
	t.Setenv(bearerEnvVar, verbTestBearer)
	server := startQueryAPI(t, localSchemaDigest(), map[string]string{})

	_, _, err := captureVerb(t, enableArgs(server, dsn, catalogPath)...)
	if err == nil {
		t.Fatal("enable wrote/reported against a registry that registers nothing at all")
	}
	if !strings.Contains(err.Error(), "registers no operations") {
		t.Fatalf("refused for a different reason, so the moved 'nothing to prove' check is not what stopped it: %v", err)
	}
	assertNoRows(t, dsn)
}

// repoint's identical preflight.
func TestRepointRefusesARegistryThatRegistersNothingAtAll(t *testing.T) {
	_, dsn := startVerbPostgres(t)
	digest := "5555555555555555555555555555555555555555555555555555555555555555"
	catalogPath := writeCatalog(t, map[string]string{verbTestOperation: digest})
	t.Setenv(bearerEnvVar, verbTestBearer)
	// Seed a row first with a REGISTERING fixture, then point repoint at
	// an EMPTY one -- proves the check fires from repoint's own preflight
	// against the registry it actually reads, not from an incidental
	// empty-table refusal.
	seedServer := startQueryAPI(t, localSchemaDigest(), map[string]string{verbTestOperation: digest})
	if _, _, err := captureVerb(t, enableArgs(seedServer, dsn, catalogPath, "-acknowledge-unproven")...); err != nil {
		t.Fatalf("seeding enable: %v", err)
	}
	emptyServer := startQueryAPI(t, localSchemaDigest(), map[string]string{})

	_, _, err := captureVerb(t,
		"repoint",
		"-registry-url", emptyServer.URL+"/registry",
		"-buildinfo-url", emptyServer.URL+"/buildinfo",
		"-postgres-uri", dsn,
		"-recorded-by", "lane-routing-verbs",
		"-review-evidence", "repoint empty-registry killer",
	)
	if err == nil {
		t.Fatal("repoint wrote/reported against a registry that registers nothing at all")
	}
	if !strings.Contains(err.Error(), "registers no operations") {
		t.Fatalf("refused for a different reason: %v", err)
	}
}

// End to end. Two ways
// `status` used to report a positive `reachable` answer that `enable`
// would refuse: a SCHEMA-level digest disagreement (checked before the
// per-operation document digest ever is), and the go plane being
// genuinely UNREACHABLE (down). Both must now report reachable=false or
// nil (unknown) respectively -- never a silent true.
func TestStatusReachableDegradesOnSchemaMismatchAndOnAnUnreachableGoPlane(t *testing.T) {
	pool, dsn := startVerbPostgres(t)
	digest := "6666666666666666666666666666666666666666666666666666666666666666"
	catalogPath := writeCatalog(t, map[string]string{verbTestOperation: digest})
	t.Setenv(bearerEnvVar, verbTestBearer)
	server := startQueryAPI(t, localSchemaDigest(), map[string]string{verbTestOperation: digest})

	ctx := context.Background()
	if _, err := pool.Exec(ctx, `
		INSERT INTO public.go_api_candidate_build (schema_digest, document_digest, selected_operation, candidate_build)
		VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING`,
		localSchemaDigest(), digest, verbTestOperation, verbTestBuild); err != nil {
		t.Fatalf("register candidate build: %v", err)
	}
	if _, err := goapiproof.Write(ctx, pool, goapiproof.Receipt{
		SchemaDigest:      localSchemaDigest(),
		DocumentDigest:    digest,
		SelectedOperation: verbTestOperation,
		CandidateBuild:    verbTestBuild,
		RequestIdentity:   "status-reachable-degrade",
		Stage:             goapiproof.EnablementProofStage,
		TerminalState:     goapiproof.EnablementProofTerminalState,
		MeasurementRoute:  goapiproof.RouteEdge,
		BuildBinding:      goapiproof.EdgeBuildPresent,
		RecordedBy:        "lane-routing-verbs",
		ReviewEvidence:    "r5 P1 killer: reachable must degrade on schema mismatch and on a down plane",
	}); err != nil {
		t.Fatalf("write receipt: %v", err)
	}
	if _, _, err := captureVerb(t, enableArgs(server, dsn, catalogPath)...); err != nil {
		t.Fatalf("enable a genuinely proven operation must succeed: %v", err)
	}

	type reportShape struct {
		PlanesAgree *bool `json:"planes_agree"`
		Operations  []struct {
			Operation string `json:"operation"`
			Reachable *bool  `json:"reachable"`
		} `json:"operations"`
	}
	findOperation := func(t *testing.T, jsonOut string) *struct {
		Operation string `json:"operation"`
		Reachable *bool  `json:"reachable"`
	} {
		t.Helper()
		var report reportShape
		if err := json.Unmarshal([]byte(jsonOut), &report); err != nil {
			t.Fatalf("decode status -json: %v\n%s", err, jsonOut)
		}
		for i := range report.Operations {
			if report.Operations[i].Operation == verbTestOperation {
				return &report.Operations[i]
			}
		}
		t.Fatalf("no operation %s in the JSON report:\n%s", verbTestOperation, jsonOut)
		return nil
	}

	// --- Schema mismatch: the deployed plane serves a DIFFERENT
	// schema_digest, but happens to still agree on THIS operation's
	// document digest -- proving the downgrade is not just piggybacking
	// on the existing document-digest check.
	mismatchedServer := startQueryAPI(t, "sha256:"+strings.Repeat("f", 64), map[string]string{verbTestOperation: digest})
	schemaJSONOut, _, err := captureVerb(t, "status", "-registry-url", mismatchedServer.URL+"/registry", "-postgres-uri", dsn, "-catalog", catalogPath, "-json")
	if err != nil {
		t.Fatalf("status -json must never refuse: %v", err)
	}
	schemaOperation := findOperation(t, schemaJSONOut)
	if schemaOperation.Reachable == nil || *schemaOperation.Reachable {
		t.Fatalf("reachable must be false (known, not unknown) under a schema mismatch, got %v", schemaOperation.Reachable)
	}

	// The TEXT report's PROOF
	// column must degrade to MISMATCH under a schema-level disagreement
	// too -- not only a per-operation document-digest one -- even though
	// DeployedDigestState reads AGREE (the per-operation digest itself
	// still matches; only the SCHEMA disagrees). A mutant dropping
	// `|| schemaMismatch` from that column's condition would print "ok"
	// under a `[MISMATCH]` banner with nothing here to catch it.
	schemaTextOut, _, err := captureVerb(t, "status", "-registry-url", mismatchedServer.URL+"/registry", "-postgres-uri", dsn, "-catalog", catalogPath)
	if err != nil {
		t.Fatalf("status must never refuse: %v", err)
	}
	if !strings.Contains(schemaTextOut, "[MISMATCH]") {
		t.Fatalf("the top-of-report schema banner must say MISMATCH:\n%s", schemaTextOut)
	}
	for _, line := range strings.Split(schemaTextOut, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), verbTestOperation) {
			if strings.Contains(line, "ok") {
				t.Fatalf("the PROOF column must not print 'ok' under a schema-level disagreement:\n%s", schemaTextOut)
			}
			if !strings.Contains(line, "MISMATCH") {
				t.Fatalf("the PROOF column must print MISMATCH under a schema-level disagreement:\n%s", schemaTextOut)
			}
			break
		}
	}

	// --- Down: the go plane cannot be reached at all.
	downServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	t.Cleanup(downServer.Close)
	downJSONOut, _, err := captureVerb(t, "status", "-registry-url", downServer.URL+"/registry", "-postgres-uri", dsn, "-catalog", catalogPath, "-json")
	if err != nil {
		t.Fatalf("status -json must never refuse: %v", err)
	}
	downOperation := findOperation(t, downJSONOut)
	if downOperation.Reachable != nil {
		t.Fatalf("reachable must be nil (unknown) when the go plane is unreachable, got %v", *downOperation.Reachable)
	}

	// --- Control: the ORIGINAL, agreeing server must still report true.
	healthyJSONOut, _, err := captureVerb(t, "status", "-registry-url", server.URL+"/registry", "-postgres-uri", dsn, "-catalog", catalogPath, "-json")
	if err != nil {
		t.Fatalf("status -json must never refuse: %v", err)
	}
	healthyOperation := findOperation(t, healthyJSONOut)
	if healthyOperation.Reachable == nil || !*healthyOperation.Reachable {
		t.Fatalf("control: a genuinely healthy, agreeing deployment must report reachable=true, got %v", healthyOperation.Reachable)
	}
}

// `enable` must never default `-registry-url` and
// `-buildinfo-url` INDEPENDENTLY to a hardcoded `http://localhost:8090/
// ...` each -- so with neither flag given, it silently probed whatever
// happened to answer there instead of refusing, and if the two flags
// were given inconsistently it could write a row naming a DIFFERENT
// process's build than the one its preflights checked. This proves the
// fix's actual contract end to end: with ONLY GO_API_QUERY_API_URL set
// (no -registry-url, no -buildinfo-url at all), BOTH routes resolve to
// the SAME real process, and the row it writes names THAT process's
// build.
func TestEnableDerivesBothEndpointsFromTheEnvVarAloneNoFlags(t *testing.T) {
	_, dsn := startVerbPostgres(t)
	digest := "7777777777777777777777777777777777777777777777777777777777777777"
	catalogPath := writeCatalog(t, map[string]string{verbTestOperation: digest})
	t.Setenv(bearerEnvVar, verbTestBearer)
	server := startQueryAPI(t, localSchemaDigest(), map[string]string{verbTestOperation: digest})
	t.Setenv("GO_API_QUERY_API_URL", server.URL)

	_, _, err := captureVerb(t,
		"enable",
		"-operations", verbTestOperation,
		"-mode", "canary",
		// Deliberately NO -registry-url, NO -buildinfo-url.
		"-postgres-uri", dsn,
		"-catalog", catalogPath,
		"-recorded-by", "lane-routing-verbs",
		"-review-evidence", "r6 P2 killer: env-var-only endpoint resolution",
		"-acknowledge-unproven",
	)
	if err != nil {
		t.Fatalf("enable with only GO_API_QUERY_API_URL set must succeed: %v", err)
	}
	var mode, build string
	if err := queryRow(t, dsn, `SELECT mode, current_candidate_build FROM go_api_routing_state`, &mode, &build); err != nil {
		t.Fatal(err)
	}
	if mode != "canary" || build != verbTestBuild {
		t.Fatalf("row = mode=%q build=%q, want mode=canary build=%q -- the env-derived endpoints must both point at the SAME process", mode, build, verbTestBuild)
	}
}

// The write-verb refusal, at the real call site: no flags, no env var.
func TestEnableRefusesWithNoQueryAPIURLConfiguredAtAllRealBinary(t *testing.T) {
	_, dsn := startVerbPostgres(t)
	digest := "8888888888888888888888888888888888888888888888888888888888888886"
	catalogPath := writeCatalog(t, map[string]string{verbTestOperation: digest})
	t.Setenv(bearerEnvVar, verbTestBearer)
	t.Setenv("GO_API_QUERY_API_URL", "")

	_, _, err := captureVerb(t,
		"enable",
		"-operations", verbTestOperation,
		"-mode", "canary",
		"-postgres-uri", dsn,
		"-catalog", catalogPath,
		"-recorded-by", "lane-routing-verbs",
		"-review-evidence", "r6 P2 killer: no endpoint configured at all",
	)
	if err == nil {
		t.Fatal("enable with no query-api URL configured at all must refuse, not silently probe a hardcoded default")
	}
	if !strings.Contains(err.Error(), "no query-api URL") {
		t.Fatalf("refused for a different reason: %v", err)
	}
	assertNoRows(t, dsn)
}

// `disable`
// computes its schema digest from THIS BINARY's own embedded SDL and
// must check whether a named operation has a row at any OTHER digest
// -- so a stale checkout (built from an operator's own tree, not the
// deployed image -- see disable.go's own package comment) silently
// reported `applied: 0`, exit 0, while the real row, at the digest the
// deployed process actually uses, sat completely untouched. Fixed by
// REFUSING outright (exit 2) rather than merely warning, both dry-run
// and -apply, naming the digest(s) the row actually lives at.
// disable must never abort over a
// named operation whose only rows sit at another schema digest -- that
// contradicts this verb's own documented contract ("must work when the
// planes disagree and when the deployed process is down"). It SKIPS the
// operation, names the stale digest(s) in the plan, and leaves the row
// genuinely untouched -- whether the operation was named explicitly or
// picked up by `-operations all-registered`.
func TestDisableSkipsAndNamesAnOperationWithRowsOnlyAtOtherSchemaDigests(t *testing.T) {
	pool, dsn := startVerbPostgres(t)
	ctx := context.Background()
	staleDigest := "sha256:" + strings.Repeat("9", 64) // deliberately NOT this binary's own digest
	digest := "1111111111111111111111111111111111111111111111111111111111111111"
	catalogPath := writeCatalog(t, map[string]string{verbTestOperation: digest})

	if _, err := pool.Exec(ctx, `
		INSERT INTO public.go_api_candidate_build (schema_digest, document_digest, selected_operation, candidate_build)
		VALUES ($1, $2, $3, $4)`,
		staleDigest, digest, verbTestOperation, verbTestBuild); err != nil {
		t.Fatalf("seed candidate build at the stale digest: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO public.go_api_routing_state
			(schema_digest, document_digest, selected_operation, current_candidate_build, owner, mode, rollout_percentage, review_evidence, recorded_by)
		VALUES ($1, $2, $3, $4, 'go', 'canary', 100, 'seeded at a stale digest', 'test')`,
		staleDigest, digest, verbTestOperation, verbTestBuild); err != nil {
		t.Fatalf("seed routing row at the stale digest: %v", err)
	}

	out, _, err := captureVerb(t, "disable", "-operations", verbTestOperation, "-mode", "python", "-postgres-uri", dsn, "-catalog", catalogPath)
	if err != nil {
		t.Fatalf("disable must NOT refuse when the named operation has rows only at another schema digest, even named explicitly: %v", err)
	}
	if !strings.Contains(out, "!! 1 digest(s) OTHER than this checkout") || !strings.Contains(out, staleDigest) {
		t.Fatalf("the stale digest must still be NAMED in the plan:\n%s", out)
	}
	if !strings.Contains(out, "DRY RUN: 0 row(s) would change") {
		t.Fatalf("nothing at the live digest, so nothing should change:\n%s", out)
	}

	// -apply behaves identically: skips, names the stale digest, writes
	// nothing.
	out, _, err = captureVerb(t, "disable", "-operations", verbTestOperation, "-mode", "python", "-postgres-uri", dsn, "-catalog", catalogPath,
		"-apply", "-recorded-by", "lane-routing-verbs", "-review-evidence", "r7 F1 killer")
	if err != nil {
		t.Fatalf("disable -apply must not refuse either: %v", err)
	}
	if !strings.Contains(out, "!! 1 digest(s) OTHER than this checkout") {
		t.Fatalf("the stale digest must still be named under -apply:\n%s", out)
	}
	if !strings.Contains(out, "applied: 0 row(s)") {
		t.Fatalf("nothing at the live digest, so nothing should be applied:\n%s", out)
	}

	var mode string
	if err := queryRow(t, dsn, `SELECT mode FROM go_api_routing_state`, &mode); err != nil {
		t.Fatal(err)
	}
	if mode != "canary" {
		t.Fatalf("the stale-digest row must be genuinely untouched: mode = %q, want canary", mode)
	}
}

// `StaleSchemaDigests` is deduplicated (dedupeSorted
// in routing_disable.go), but nothing proved it -- two rows for the SAME
// operation at ONE stale schema digest (two different document digests,
// the shape an SDL move followed by a catalog change leaves behind) must
// still be reported as ONE digest, naming it once, not two.
func TestDisableDeduplicatesTwoRowsAtTheSameStaleSchemaDigest(t *testing.T) {
	pool, dsn := startVerbPostgres(t)
	ctx := context.Background()
	staleDigest := "sha256:" + strings.Repeat("4", 64) // deliberately NOT this binary's own digest
	liveDigest := "8888888888888888888888888888888888888888888888888888888888888887"
	staleDocDigestA := "9999999999999999999999999999999999999999999999999999999999999991"
	staleDocDigestB := "9999999999999999999999999999999999999999999999999999999999999992"
	catalogPath := writeCatalog(t, map[string]string{verbTestOperation: liveDigest})

	// The live row -- what makes this operation eligible to actually be
	// disabled, so the stale-digest note prints ALONGSIDE a real change,
	// not instead of one.
	if _, err := pool.Exec(ctx, `
		INSERT INTO public.go_api_candidate_build (schema_digest, document_digest, selected_operation, candidate_build)
		VALUES ($1, $2, $3, $4)`,
		localSchemaDigest(), liveDigest, verbTestOperation, verbTestBuild); err != nil {
		t.Fatalf("seed live candidate build: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO public.go_api_routing_state
			(schema_digest, document_digest, selected_operation, current_candidate_build, owner, mode, rollout_percentage, review_evidence, recorded_by)
		VALUES ($1, $2, $3, $4, 'go', 'canary', 100, 'seeded live', 'test')`,
		localSchemaDigest(), liveDigest, verbTestOperation, verbTestBuild); err != nil {
		t.Fatalf("seed live routing row: %v", err)
	}
	// TWO rows at the SAME stale schema digest, different document
	// digests -- this is what needs deduplicating.
	for _, docDigest := range []string{staleDocDigestA, staleDocDigestB} {
		if _, err := pool.Exec(ctx, `
			INSERT INTO public.go_api_candidate_build (schema_digest, document_digest, selected_operation, candidate_build)
			VALUES ($1, $2, $3, $4)`,
			staleDigest, docDigest, verbTestOperation, verbTestBuild); err != nil {
			t.Fatalf("seed stale candidate build (%s): %v", docDigest, err)
		}
		if _, err := pool.Exec(ctx, `
			INSERT INTO public.go_api_routing_state
				(schema_digest, document_digest, selected_operation, current_candidate_build, owner, mode, rollout_percentage, review_evidence, recorded_by)
			VALUES ($1, $2, $3, $4, 'go', 'canary', 100, 'seeded at a stale digest', 'test')`,
			staleDigest, docDigest, verbTestOperation, verbTestBuild); err != nil {
			t.Fatalf("seed stale routing row (%s): %v", docDigest, err)
		}
	}

	out, _, err := captureVerb(t, "disable", "-operations", verbTestOperation, "-mode", "python", "-postgres-uri", dsn, "-catalog", catalogPath)
	if err != nil {
		t.Fatalf("disable must not refuse: %v", err)
	}
	if !strings.Contains(out, "!! 1 digest(s) OTHER than this checkout") {
		t.Fatalf("two rows at ONE stale digest must be reported as 1 digest, deduplicated:\n%s", out)
	}
	if strings.Contains(out, "!! 2 digest(s)") {
		t.Fatalf("the stale digest count must not double-count two rows at the SAME digest:\n%s", out)
	}
	if strings.Count(out, staleDigest) != 1 {
		t.Fatalf("the stale digest must be NAMED exactly once, not once per row:\n%s", out)
	}
}

// Two lines in disable's -apply path
// were unpinned by every prior test -- the stale-digest note in the
// plan output (M38: neutralising the `if len(change.StaleSchemaDigests) >
// 0` guard left both suites green) and the per-row
// `go_api_routing.disabled` structured stderr line, the only durable log
// of an off-ramp write (M40: neutralising the `if !change.Applied {
// continue }` guard, so every row -- including no-ops -- logged, also left
// both suites green). Unlike TestDisableAllRegisteredSkipsStaleOnlyOperationsInsteadOfRefusing
// (two DIFFERENT operations, one wholly live, one wholly stale-only), this
// seeds ONE operation with a LIVE row (which -apply actually disables) AND
// a SEPARATE row at a stale schema digest for the SAME operation -- the
// exact shape that exercises the stale-digest note on a row that is also
// being written, matching the reviewer's own executed repro.
func TestDisableAppliesLiveRowAndReportsStaleDigestAndLogsTheWrite(t *testing.T) {
	pool, dsn := startVerbPostgres(t)
	ctx := context.Background()
	liveDigest := "6666666666666666666666666666666666666666666666666666666666666666"
	staleDigest := "sha256:" + strings.Repeat("7", 64)
	catalogPath := writeCatalog(t, map[string]string{verbTestOperation: liveDigest})

	if _, err := pool.Exec(ctx, `
		INSERT INTO public.go_api_candidate_build (schema_digest, document_digest, selected_operation, candidate_build)
		VALUES ($1, $2, $3, $4)`,
		localSchemaDigest(), liveDigest, verbTestOperation, verbTestBuild); err != nil {
		t.Fatalf("seed live candidate build: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO public.go_api_routing_state
			(schema_digest, document_digest, selected_operation, current_candidate_build, owner, mode, rollout_percentage, review_evidence, recorded_by)
		VALUES ($1, $2, $3, $4, 'go', 'canary', 100, 'seeded live', 'test')`,
		localSchemaDigest(), liveDigest, verbTestOperation, verbTestBuild); err != nil {
		t.Fatalf("seed live routing row: %v", err)
	}
	// A SEPARATE row for the SAME operation, at a stale schema digest --
	// this binary's own checkout does not match it, and disable's plan
	// output must still name it even though the live row above is what
	// actually gets written.
	if _, err := pool.Exec(ctx, `
		INSERT INTO public.go_api_candidate_build (schema_digest, document_digest, selected_operation, candidate_build)
		VALUES ($1, $2, $3, $4)`,
		staleDigest, liveDigest, verbTestOperation, verbTestBuild); err != nil {
		t.Fatalf("seed stale candidate build: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO public.go_api_routing_state
			(schema_digest, document_digest, selected_operation, current_candidate_build, owner, mode, rollout_percentage, review_evidence, recorded_by)
		VALUES ($1, $2, $3, $4, 'go', 'canary', 100, 'seeded at a stale digest', 'test')`,
		staleDigest, liveDigest, verbTestOperation, verbTestBuild); err != nil {
		t.Fatalf("seed stale routing row: %v", err)
	}

	out, errOut, err := captureVerb(t, "disable", "-operations", verbTestOperation, "-mode", "python",
		"-postgres-uri", dsn, "-catalog", catalogPath,
		"-apply", "-recorded-by", "lane-routing-verbs", "-review-evidence", "T1 M38/M40 killer")
	if err != nil {
		t.Fatalf("disable -apply: %v", err)
	}

	// M38: the stale-digest note in stdout's plan output.
	if !strings.Contains(out, "!! 1 digest(s) OTHER than this checkout") || !strings.Contains(out, staleDigest) {
		t.Fatalf("stdout must name the stale-digest row even though the live row is what changed:\n%s", out)
	}

	// M40: the per-row structured log, the only durable record of the
	// write -- stated in full so a mutant that logs an EMPTY line, or logs
	// the wrong operation/transition, is caught too.
	wantLog := fmt.Sprintf("go_api_routing.disabled operation=%s from=canary to=python schema_digest=%s document_digest=%s recorded_by=lane-routing-verbs",
		verbTestOperation, localSchemaDigest(), liveDigest)
	if !strings.Contains(errOut, wantLog) {
		t.Fatalf("stderr missing the disabled-row log line.\nwant substring: %s\ngot:\n%s", wantLog, errOut)
	}

	var liveMode string
	if err := pool.QueryRow(ctx, `SELECT mode FROM go_api_routing_state WHERE schema_digest = $1`, localSchemaDigest()).Scan(&liveMode); err != nil {
		t.Fatal(err)
	}
	if liveMode != "python" {
		t.Fatalf("the live row must actually be disabled: mode = %q, want python", liveMode)
	}
}

// The routing table's primary key is (schema_digest, document_digest,
// selected_operation): two rows of ONE operation, differing only in
// document digest, are two distinct rows. Neither the plan an operator
// reads before typing `-apply` nor the durable per-row log line can tell
// them apart unless both carry the document digest.
func TestDisableTwoRowsSameOperationDifferentDocumentDigestProduceDistinguishableOutput(t *testing.T) {
	pool, dsn := startVerbPostgres(t)
	ctx := context.Background()
	digestA := "1111111111111111111111111111111111111111111111111111111111111111"
	digestB := "2222222222222222222222222222222222222222222222222222222222222222"
	catalogPath := writeCatalog(t, map[string]string{verbTestOperation: digestA})

	for _, digest := range []string{digestA, digestB} {
		if _, err := pool.Exec(ctx, `
			INSERT INTO public.go_api_candidate_build (schema_digest, document_digest, selected_operation, candidate_build)
			VALUES ($1, $2, $3, $4)`,
			localSchemaDigest(), digest, verbTestOperation, verbTestBuild); err != nil {
			t.Fatalf("seed candidate build at digest %s: %v", digest, err)
		}
		if _, err := pool.Exec(ctx, `
			INSERT INTO public.go_api_routing_state
				(schema_digest, document_digest, selected_operation, current_candidate_build, owner, mode, rollout_percentage, review_evidence, recorded_by)
			VALUES ($1, $2, $3, $4, 'go', 'canary', 100, 'seeded', 'test')`,
			localSchemaDigest(), digest, verbTestOperation, verbTestBuild); err != nil {
			t.Fatalf("seed row at digest %s: %v", digest, err)
		}
	}

	out, errOut, err := captureVerb(t, "disable", "-operations", verbTestOperation, "-mode", "python",
		"-postgres-uri", dsn, "-catalog", catalogPath,
		"-apply", "-recorded-by", "lane-routing-verbs", "-review-evidence", "F3 two-row disable killer")
	if err != nil {
		t.Fatalf("disable -apply: %v", err)
	}
	if !strings.Contains(out, digestA) || !strings.Contains(out, digestB) {
		t.Fatalf("plan table must name BOTH document digests distinctly:\n%s", out)
	}
	if !strings.Contains(errOut, "document_digest="+digestA) || !strings.Contains(errOut, "document_digest="+digestB) {
		t.Fatalf("structured log lines must name BOTH document digests distinctly:\n%s", errOut)
	}
}

// Same invariant as above, for `repoint`: it moves EVERY row at the live
// schema digest for a requested operation, not only the one the catalog
// currently carries -- so two rows here is the normal multi-row case, not
// a corner, and its plan/log output must be equally distinguishable.
func TestRepointTwoRowsSameOperationDifferentDocumentDigestProduceDistinguishableOutput(t *testing.T) {
	pool, dsn := startVerbPostgres(t)
	ctx := context.Background()
	digestA := "3333333333333333333333333333333333333333333333333333333333333333"
	digestB := "4444444444444444444444444444444444444444444444444444444444444444"
	// Seeded with an OLD build, distinct from the running build the stub
	// reports below -- repoint only emits a structured line for a row it
	// actually CHANGES (matching disable's convention), so both rows must
	// have something to move.
	oldBuild := "0000000000000000000000000000000000000000"
	t.Setenv(bearerEnvVar, verbTestBearer)
	server := startQueryAPI(t, localSchemaDigest(), map[string]string{verbTestOperation: digestA})

	for _, digest := range []string{digestA, digestB} {
		if _, err := pool.Exec(ctx, `
			INSERT INTO public.go_api_candidate_build (schema_digest, document_digest, selected_operation, candidate_build)
			VALUES ($1, $2, $3, $4)`,
			localSchemaDigest(), digest, verbTestOperation, oldBuild); err != nil {
			t.Fatalf("seed candidate build at digest %s: %v", digest, err)
		}
		if _, err := pool.Exec(ctx, `
			INSERT INTO public.go_api_routing_state
				(schema_digest, document_digest, selected_operation, current_candidate_build, owner, mode, rollout_percentage, review_evidence, recorded_by)
			VALUES ($1, $2, $3, $4, 'go', 'canary', 100, 'seeded', 'test')`,
			localSchemaDigest(), digest, verbTestOperation, oldBuild); err != nil {
			t.Fatalf("seed row at digest %s: %v", digest, err)
		}
	}

	out, errOut, err := captureVerb(t, "repoint",
		"-registry-url", server.URL+"/registry",
		"-buildinfo-url", server.URL+"/buildinfo",
		"-postgres-uri", dsn,
		"-operations", verbTestOperation,
		"-recorded-by", "lane-routing-verbs",
		"-review-evidence", "F3 two-row repoint killer")
	if err != nil {
		t.Fatalf("repoint: %v", err)
	}
	if !strings.Contains(out, digestA) || !strings.Contains(out, digestB) {
		t.Fatalf("plan output must name BOTH document digests distinctly:\n%s", out)
	}
	if !strings.Contains(errOut, "document_digest="+digestA) || !strings.Contains(errOut, "document_digest="+digestB) {
		t.Fatalf("structured log lines must name BOTH document digests distinctly:\n%s", errOut)
	}
}

// `enable` writes only ONE row per operation -- the catalog's own document
// digest -- by design, so it cannot itself produce two outcomes for one
// operation in a single call. What it must still get right: its
// structured line names the LIVE row's own document digest, not a dead
// sibling row's, when one exists for the same operation at the same
// schema digest.
func TestEnableLogNamesTheLiveRowsDocumentDigestNotADeadSiblings(t *testing.T) {
	pool, dsn := startVerbPostgres(t)
	ctx := context.Background()
	liveDigest := "5555555555555555555555555555555555555555555555555555555555555555"
	deadDigest := "6666666666666666666666666666666666666666666666666666666666666668"
	catalogPath := writeCatalog(t, map[string]string{verbTestOperation: liveDigest})
	t.Setenv(bearerEnvVar, verbTestBearer)
	server := startQueryAPI(t, localSchemaDigest(), map[string]string{verbTestOperation: liveDigest})

	if _, err := pool.Exec(ctx, `
		INSERT INTO public.go_api_candidate_build (schema_digest, document_digest, selected_operation, candidate_build)
		VALUES ($1, $2, $3, $4)`,
		localSchemaDigest(), deadDigest, verbTestOperation, verbTestBuild); err != nil {
		t.Fatalf("seed dead candidate build: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO public.go_api_routing_state
			(schema_digest, document_digest, selected_operation, current_candidate_build, owner, mode, rollout_percentage, review_evidence, recorded_by)
		VALUES ($1, $2, $3, $4, 'go', 'primary', 100, 'seeded dead sibling', 'test')`,
		localSchemaDigest(), deadDigest, verbTestOperation, verbTestBuild); err != nil {
		t.Fatalf("seed dead row: %v", err)
	}

	_, errOut, err := captureVerb(t, enableArgs(server, dsn, catalogPath, "-acknowledge-unproven")...)
	if err != nil {
		t.Fatalf("enable: %v", err)
	}
	// Anchored to `recorded_by=` immediately after, the `go_api_routing.enabled`
	// line's own shape -- the `enabled_unproven` WARNING line (not exercised
	// here; this row is proven) has `document_digest=` followed by `mode=`
	// instead, so this cannot pass by accident via the sibling line.
	if !strings.Contains(errOut, "document_digest="+liveDigest+" recorded_by=") {
		t.Fatalf("enable's log line must name the LIVE row's own document digest:\n%s", errOut)
	}
	if strings.Contains(errOut, "document_digest="+deadDigest) {
		t.Fatalf("enable's log line must never name the dead sibling's document digest:\n%s", errOut)
	}
}

// When the deployed process cannot identify its build, the refusal must
// still name WHICH process was asked -- scheme://host[:port], never
// anything an operator put in the URL's own path. `enable` and `repoint`
// share this exact refusal shape; both are pinned here in one fixture.
func TestEnableAndRepointBuildInfoRefusalNeverPrintsTheURLPath(t *testing.T) {
	_, dsn := startVerbPostgres(t)
	digest := "7777777777777777777777777777777777777777777777777777777777777777"
	catalogPath := writeCatalog(t, map[string]string{verbTestOperation: digest})
	t.Setenv(bearerEnvVar, verbTestBearer)
	registry := startQueryAPI(t, localSchemaDigest(), map[string]string{verbTestOperation: digest})

	// A /buildinfo that answers 404 to EVERY path -- the ErrNoBuildIdentity
	// case -- whose own path carries something that would be a leak if
	// printed verbatim.
	pathToken := "/t/PROXY-PATH-TOKEN-hunter2/buildinfo"
	noBuildInfo := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(noBuildInfo.Close)
	buildInfoURL := noBuildInfo.URL + pathToken

	_, _, err := captureVerb(t, enableArgs(registry, dsn, catalogPath, "-buildinfo-url", buildInfoURL)...)
	if err == nil {
		t.Fatal("enable must refuse when the deployed process cannot identify its build")
	}
	if strings.Contains(err.Error(), "PROXY-PATH-TOKEN-hunter2") {
		t.Fatalf("enable's refusal must never print the buildinfo URL's path:\n%v", err)
	}
	if !strings.Contains(err.Error(), noBuildInfo.URL) {
		t.Fatalf("enable's refusal must still name the endpoint (scheme://host[:port]):\n%v", err)
	}

	_, _, err = captureVerb(t, "repoint",
		"-registry-url", registry.URL+"/registry",
		"-buildinfo-url", buildInfoURL,
		"-postgres-uri", dsn,
		"-recorded-by", "lane-routing-verbs",
		"-review-evidence", "F1 repoint sibling",
	)
	if err == nil {
		t.Fatal("repoint must refuse when the deployed process cannot identify its build")
	}
	if strings.Contains(err.Error(), "PROXY-PATH-TOKEN-hunter2") {
		t.Fatalf("repoint's refusal must never print the buildinfo URL's path:\n%v", err)
	}
	if !strings.Contains(err.Error(), noBuildInfo.URL) {
		t.Fatalf("repoint's refusal must still name the endpoint (scheme://host[:port]):\n%v", err)
	}
}

// `enable`'s fallback path for a `/buildinfo` failure that is NOT
// ErrNoBuildIdentity (a transport failure, say) must still say "refused"
// -- the word every OTHER refusal in this package carries, and the one
// `repoint`'s identical call site already used.
func TestEnableBuildInfoGenericFailureIsWordedRefused(t *testing.T) {
	_, dsn := startVerbPostgres(t)
	digest := "8888888888888888888888888888888888888888888888888888888888888888"
	catalogPath := writeCatalog(t, map[string]string{verbTestOperation: digest})
	t.Setenv(bearerEnvVar, verbTestBearer)
	registry := startQueryAPI(t, localSchemaDigest(), map[string]string{verbTestOperation: digest})

	_, _, err := captureVerb(t, enableArgs(registry, dsn, catalogPath, "-buildinfo-url", "http://127.0.0.1:9/buildinfo")...)
	if err == nil {
		t.Fatal("enable must refuse when /buildinfo is unreachable")
	}
	if !strings.Contains(err.Error(), "refused:") {
		t.Fatalf("enable's generic /buildinfo failure must be worded \"refused\", matching every other refusal in this package: %v", err)
	}
}

// The `-candidate-build` guard must not check
// EVERY row at the live schema digest, including DEAD ones (a document
// digest the catalog does not carry) -- rows `status` never shows a
// build for at all. An operator who copied `-candidate-build` from
// `status`'s own output was refused with "somebody has repointed it
// since you looked" for a row they were never shown and never asked
// about.
func TestDisableCandidateBuildGuardIgnoresDeadRows(t *testing.T) {
	pool, dsn := startVerbPostgres(t)
	ctx := context.Background()
	liveDigest := "2222222222222222222222222222222222222222222222222222222222222222"
	deadDigest := "3333333333333333333333333333333333333333333333333333333333333333" // NOT in the catalog
	catalogPath := writeCatalog(t, map[string]string{verbTestOperation: liveDigest})
	const liveBuild = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	const deadBuild = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" // status never shows this one

	for _, row := range []struct{ digest, build string }{
		{liveDigest, liveBuild},
		{deadDigest, deadBuild},
	} {
		if _, err := pool.Exec(ctx, `
			INSERT INTO public.go_api_candidate_build (schema_digest, document_digest, selected_operation, candidate_build)
			VALUES ($1, $2, $3, $4)`,
			localSchemaDigest(), row.digest, verbTestOperation, row.build); err != nil {
			t.Fatalf("seed candidate build (%s): %v", row.digest, err)
		}
		if _, err := pool.Exec(ctx, `
			INSERT INTO public.go_api_routing_state
				(schema_digest, document_digest, selected_operation, current_candidate_build, owner, mode, rollout_percentage, review_evidence, recorded_by)
			VALUES ($1, $2, $3, $4, 'go', 'canary', 100, 'seeded', 'test')`,
			localSchemaDigest(), row.digest, verbTestOperation, row.build); err != nil {
			t.Fatalf("seed routing row (%s): %v", row.digest, err)
		}
	}

	// Guarded by the LIVE row's build only -- must succeed even though a
	// DEAD row exists with a completely different build.
	out, _, err := captureVerb(t, "disable",
		"-operations", verbTestOperation, "-mode", "python",
		"-postgres-uri", dsn, "-catalog", catalogPath,
		"-candidate-build", liveBuild,
		"-apply", "-recorded-by", "lane-routing-verbs", "-review-evidence", "r6 F3(c) killer",
	)
	if err != nil {
		t.Fatalf("the guard must not fire on a DEAD row's unrelated build: %v", err)
	}
	if !strings.Contains(out, "applied: 2 row(s)") {
		t.Fatalf("both the live and the dead row must still be disabled (unguarded, always eligible): %s", out)
	}

	rows, err := pool.Query(ctx, `SELECT document_digest, mode FROM go_api_routing_state WHERE selected_operation = $1 ORDER BY document_digest`, verbTestOperation)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var digest, mode string
		if err := rows.Scan(&digest, &mode); err != nil {
			t.Fatal(err)
		}
		if mode != "python" {
			t.Fatalf("row at document digest %s = mode %q, want python (disabled)", digest, mode)
		}
		count++
	}
	if count != 2 {
		t.Fatalf("expected both rows disabled, found %d", count)
	}
}

// When an
// operation's ONLY row at the live digest is a DEAD one (no catalog row
// exists at all for it), the guard must not silently guard ZERO rows --
// the write must not go ahead completely unguarded with nothing said
// about it. Python refuses the identical command. A guard the operation
// has NO catalog row to check against REFUSES unconditionally ("nothing
// to compare"), the SAME way whether the dead row's build happens to
// match the guard or not -- the guard is defined to never read a dead
// row's build at all, so there being no OTHER row to check is itself the
// refusal, not a build comparison against the dead one.
func TestDisableCandidateBuildGuardRefusesWhenNoCatalogRowExistsToCheck(t *testing.T) {
	pool, dsn := startVerbPostgres(t)
	ctx := context.Background()
	deadDigest := "6666666666666666666666666666666666666666666666666666666666666666" // NOT in the catalog
	liveDigestNeverWritten := "7777777777777777777777777777777777777777777777777777777777777777"
	catalogPath := writeCatalog(t, map[string]string{verbTestOperation: liveDigestNeverWritten})
	const actualBuild = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

	if _, err := pool.Exec(ctx, `
		INSERT INTO public.go_api_candidate_build (schema_digest, document_digest, selected_operation, candidate_build)
		VALUES ($1, $2, $3, $4)`,
		localSchemaDigest(), deadDigest, verbTestOperation, actualBuild); err != nil {
		t.Fatalf("seed candidate build: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO public.go_api_routing_state
			(schema_digest, document_digest, selected_operation, current_candidate_build, owner, mode, rollout_percentage, review_evidence, recorded_by)
		VALUES ($1, $2, $3, $4, 'go', 'canary', 100, 'seeded dead-only', 'test')`,
		localSchemaDigest(), deadDigest, verbTestOperation, actualBuild); err != nil {
		t.Fatalf("seed routing row: %v", err)
	}

	// A guard that does NOT match the dead row's build -- must refuse.
	_, _, err := captureVerb(t, "disable",
		"-operations", verbTestOperation, "-mode", "python",
		"-postgres-uri", dsn, "-catalog", catalogPath,
		"-candidate-build", "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", // deliberately WRONG
		"-apply", "-recorded-by", "lane-routing-verbs", "-review-evidence", "r8 F2 killer (mismatch)",
	)
	if err == nil {
		t.Fatal("a mismatched -candidate-build against an operation with no catalog row must refuse")
	}
	if exitCodeFor(err) != 2 {
		t.Fatalf("exit %d, want 2 -- operator-actionable, not a crash", exitCodeFor(err))
	}

	// The SAME guard, this time MATCHING the dead row's actual build --
	// must ALSO refuse. The guard never reads a dead row's build at all,
	// so a coincidental match does not make this write safe;
	// "nothing to compare" is unconditional.
	_, _, err = captureVerb(t, "disable",
		"-operations", verbTestOperation, "-mode", "python",
		"-postgres-uri", dsn, "-catalog", catalogPath,
		"-candidate-build", actualBuild,
		"-apply", "-recorded-by", "lane-routing-verbs", "-review-evidence", "r8 F2 killer (match)",
	)
	if err == nil {
		t.Fatal("a MATCHING -candidate-build against an operation with no catalog row must STILL refuse -- the guard never reads a dead row's build, so a coincidental match must not silently apply the write")
	}
	if exitCodeFor(err) != 2 {
		t.Fatalf("exit %d, want 2", exitCodeFor(err))
	}

	var mode string
	if err := pool.QueryRow(ctx, `SELECT mode FROM go_api_routing_state WHERE selected_operation = $1`, verbTestOperation).Scan(&mode); err != nil {
		t.Fatal(err)
	}
	if mode != "canary" {
		t.Fatalf("the dead row must be genuinely untouched by either attempt: mode = %q, want canary", mode)
	}

	// Control: OMITTING the guard entirely still disables the dead row --
	// this refusal is about a GUARD with nothing to check, not about
	// dead rows being permanently undisableable.
	out, _, err := captureVerb(t, "disable",
		"-operations", verbTestOperation, "-mode", "python",
		"-postgres-uri", dsn, "-catalog", catalogPath,
		"-apply", "-recorded-by", "lane-routing-verbs", "-review-evidence", "r8 F2 control",
	)
	if err != nil {
		t.Fatalf("control: omitting the guard entirely must still disable the dead row: %v", err)
	}
	if !strings.Contains(out, "applied: 1 row(s)") {
		t.Fatalf("control: the dead row must be disabled when no guard is named: %s", out)
	}
}

// A guarded `-operations all-registered` rollback -- the runbook's
// own documented recipe -- must not abort EVERY operation because ONE
// unrelated operation's only live-schema rows are dead (no catalog
// document digest). The healthy operation is disabled; the dead-rows-only
// one is SKIPPED and named on its own plan line; the run still exits
// non-zero because something needed attention -- matching Python, which
// disables what it can and warns about what it cannot, never Go's
// previous "disable nothing at all".
func TestDisableAllRegisteredSkipsADeadRowsOnlyOperationAndStillDisablesTheHealthyOne(t *testing.T) {
	pool, dsn := startVerbPostgres(t)
	ctx := context.Background()
	liveDigest := "8888888888888888888888888888888888888888888888888888888888888880"
	deadDigest := "9999999999999999999999999999999999999999999999999999999999999990" // NOT in the catalog
	catalogPath := writeCatalog(t, map[string]string{
		verbTestOperation: liveDigest,
		"hotspots":        "aaaa111111111111111111111111111111111111111111111111111111111111",
	})
	const build = "cccccccccccccccccccccccccccccccccccccccc"

	// verbTestOperation: healthy, at the catalog digest.
	if _, err := pool.Exec(ctx, `
		INSERT INTO public.go_api_candidate_build (schema_digest, document_digest, selected_operation, candidate_build)
		VALUES ($1, $2, $3, $4)`,
		localSchemaDigest(), liveDigest, verbTestOperation, build); err != nil {
		t.Fatalf("seed healthy candidate build: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO public.go_api_routing_state
			(schema_digest, document_digest, selected_operation, current_candidate_build, owner, mode, rollout_percentage, review_evidence, recorded_by)
		VALUES ($1, $2, $3, $4, 'go', 'canary', 100, 'seeded healthy', 'test')`,
		localSchemaDigest(), liveDigest, verbTestOperation, build); err != nil {
		t.Fatalf("seed healthy routing row: %v", err)
	}
	// hotspots: only a DEAD row at the live schema digest -- its own
	// catalog digest is never written here.
	if _, err := pool.Exec(ctx, `
		INSERT INTO public.go_api_candidate_build (schema_digest, document_digest, selected_operation, candidate_build)
		VALUES ($1, $2, 'hotspots', $3)`,
		localSchemaDigest(), deadDigest, build); err != nil {
		t.Fatalf("seed dead candidate build: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO public.go_api_routing_state
			(schema_digest, document_digest, selected_operation, current_candidate_build, owner, mode, rollout_percentage, review_evidence, recorded_by)
		VALUES ($1, $2, 'hotspots', $3, 'go', 'primary', 100, 'seeded dead-only', 'test')`,
		localSchemaDigest(), deadDigest, build); err != nil {
		t.Fatalf("seed dead routing row: %v", err)
	}

	out, _, err := captureVerb(t, "disable",
		"-operations", "all-registered", "-mode", "python",
		"-postgres-uri", dsn, "-catalog", catalogPath,
		"-candidate-build", build,
		"-apply", "-recorded-by", "lane-routing-verbs", "-review-evidence", "R145 killer",
	)
	if err == nil {
		t.Fatal("a guarded all-registered rollback with one dead-rows-only sibling must still exit non-zero -- something was skipped")
	}
	if exitCodeFor(err) != 2 {
		t.Fatalf("exit %d, want 2 -- operator-actionable, not a crash", exitCodeFor(err))
	}
	if !strings.Contains(out, "applied: 1 row(s)") {
		t.Fatalf("the healthy operation must still be disabled despite the sibling's guard state: %s", out)
	}
	if !strings.Contains(out, "SKIPPED, not disabled") {
		t.Fatalf("the skipped operation's plan line must name why: %s", out)
	}

	var flowMode, hotspotsMode string
	if err := pool.QueryRow(ctx, `SELECT mode FROM go_api_routing_state WHERE selected_operation = $1`, verbTestOperation).Scan(&flowMode); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT mode FROM go_api_routing_state WHERE selected_operation = 'hotspots'`).Scan(&hotspotsMode); err != nil {
		t.Fatal(err)
	}
	if flowMode != "python" {
		t.Fatalf("%s must be rolled back: mode = %q, want python", verbTestOperation, flowMode)
	}
	if hotspotsMode != "primary" {
		t.Fatalf("hotspots (dead-rows-only, guard uncheckable) must be genuinely untouched: mode = %q, want primary", hotspotsMode)
	}
}

// A classification failure
// (`RoutingStatusRows`) must file into `ClassificationError`, NEVER into
// `RegistryDBError` -- the census (`CountRowsBySchemaDigest`) reads only
// `go_api_routing_state` and can succeed on its own even when the
// per-operation classification (which also needs `go_api_proof_run`)
// cannot. A mutant that misfiles the classification error as a database
// error would suppress the (successful) census silently -- exactly r2
// R2-04's own defect, reintroduced.
func TestStatusFilesAClassificationFailureSeparatelyFromADatabaseFailure(t *testing.T) {
	pool, dsn := startVerbPostgres(t)
	digest := "4444444444444444444444444444444444444444444444444444444444444445"
	catalogPath := writeCatalog(t, map[string]string{verbTestOperation: digest})
	ctx := context.Background()

	// A real row, so the census has something to count.
	if _, err := pool.Exec(ctx, `
		INSERT INTO public.go_api_candidate_build (schema_digest, document_digest, selected_operation, candidate_build)
		VALUES ($1, $2, $3, $4)`,
		localSchemaDigest(), digest, verbTestOperation, verbTestBuild); err != nil {
		t.Fatalf("seed candidate build: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO public.go_api_routing_state
			(schema_digest, document_digest, selected_operation, current_candidate_build, owner, mode, rollout_percentage, review_evidence, recorded_by)
		VALUES ($1, $2, $3, $4, 'go', 'canary', 100, 'seeded', 'test')`,
		localSchemaDigest(), digest, verbTestOperation, verbTestBuild); err != nil {
		t.Fatalf("seed routing row: %v", err)
	}

	// Break ONLY the per-operation classification's own dependency --
	// the census's query never touches go_api_proof_run at all.
	if _, err := pool.Exec(ctx, `DROP TABLE public.go_api_proof_run`); err != nil {
		t.Fatalf("drop go_api_proof_run: %v", err)
	}

	jsonOut, _, err := captureVerb(t, "status", "-postgres-uri", dsn, "-catalog", catalogPath, "-json")
	if err != nil {
		t.Fatalf("status must never refuse: %v", err)
	}
	var report struct {
		RegistryDBError     *string        `json:"registry_db_error"`
		ClassificationError *string        `json:"classification_error"`
		RowsBySchemaDigest  map[string]int `json:"rows_by_schema_digest"`
	}
	if err := json.Unmarshal([]byte(jsonOut), &report); err != nil {
		t.Fatalf("decode status -json: %v\n%s", err, jsonOut)
	}
	if report.RegistryDBError != nil {
		t.Fatalf("the CENSUS query never touches go_api_proof_run and must still succeed: registry_db_error = %q", *report.RegistryDBError)
	}
	if len(report.RowsBySchemaDigest) == 0 {
		t.Fatalf("the census must be populated and printed: %+v", report.RowsBySchemaDigest)
	}
	if report.ClassificationError == nil {
		t.Fatal("the classification failure (go_api_proof_run missing) must be reported in ITS OWN field")
	}

	textOut, _, err := captureVerb(t, "status", "-postgres-uri", dsn, "-catalog", catalogPath)
	if err != nil {
		t.Fatalf("status must never refuse: %v", err)
	}
	if !strings.Contains(textOut, "per-operation classification: UNAVAILABLE") {
		t.Fatalf("text report must name the classification failure, not print a blanket 'registry database: UNREACHABLE':\n%s", textOut)
	}
	if strings.Contains(textOut, "registry database          : UNREACHABLE") {
		t.Fatalf("the census succeeded and must not be reported as an unreachable database:\n%s", textOut)
	}
}

// A genuine server-side write failure -- here a
// synthetic trigger -- must exit 1, not 2. Before
// this fix EVERY error out of Enable's write path exited 2, indistinguishable
// from an ordinary "-mode is required" typo.
func TestEnableExitsOneOnAGenuineServerSideWriteFailure(t *testing.T) {
	pool, dsn := startVerbPostgres(t)
	ctx := context.Background()
	digest := "9999999999999999999999999999999999999999999999999999999999999998"
	catalogPath := writeCatalog(t, map[string]string{verbTestOperation: digest})
	t.Setenv(bearerEnvVar, verbTestBearer)
	server := startQueryAPI(t, localSchemaDigest(), map[string]string{verbTestOperation: digest})

	if _, err := pool.Exec(ctx, `
		CREATE OR REPLACE FUNCTION test_synthetic_write_failure() RETURNS trigger AS $$
		BEGIN
			RAISE EXCEPTION 'synthetic write failure';
		END;
		$$ LANGUAGE plpgsql;
		CREATE TRIGGER test_synthetic_write_failure BEFORE INSERT ON go_api_routing_state
			FOR EACH ROW EXECUTE FUNCTION test_synthetic_write_failure();
	`); err != nil {
		t.Fatal(err)
	}

	_, _, err := captureVerb(t, enableArgs(server, dsn, catalogPath, "-acknowledge-unproven")...)
	if err == nil {
		t.Fatal("enable must refuse when the database itself raises inside the write")
	}
	if !strings.Contains(err.Error(), "synthetic write failure") {
		t.Fatalf("refused for a different reason: %v", err)
	}
	if got := exitCodeFor(err); got != 1 {
		t.Fatalf("exit %d, want 1 -- a genuine server-side failure is a CRASH, not an ordinary operator-actionable refusal", got)
	}
}

// r6 observability (1)/(2)/(5) (team-lead ruling): enable and repoint
// must print which endpoint they consulted on success, and emit one
// structured per-row line naming BEFORE-and-AFTER mode/build -- the same
// shape disable's own `go_api_routing.disabled` line already had.
func TestEnableAndRepointEmitEndpointAndPerRowStructuredLines(t *testing.T) {
	_, dsn := startVerbPostgres(t)
	digest := "6666666666666666666666666666666666666666666666666666666666666667"
	catalogPath := writeCatalog(t, map[string]string{verbTestOperation: digest})
	t.Setenv(bearerEnvVar, verbTestBearer)
	server := startQueryAPI(t, localSchemaDigest(), map[string]string{verbTestOperation: digest})

	enableOut, enableErrOut, err := captureVerb(t, enableArgs(server, dsn, catalogPath, "-acknowledge-unproven")...)
	if err != nil {
		t.Fatalf("enable: %v", err)
	}
	if !strings.Contains(enableOut, "go-api-routing: registry=") || !strings.Contains(enableOut, "buildinfo=") {
		t.Fatalf("enable must print which endpoint it consulted:\n%s", enableOut)
	}
	if !strings.Contains(enableErrOut, "go_api_routing.enabled operation="+verbTestOperation) ||
		!strings.Contains(enableErrOut, "mode_before=(no row)") ||
		!strings.Contains(enableErrOut, "mode_after=canary") {
		t.Fatalf("enable must emit a structured before/after line:\n%s", enableErrOut)
	}

	_, repointErrOut, err := captureVerb(t, "repoint",
		"-registry-url", server.URL+"/registry",
		"-buildinfo-url", server.URL+"/buildinfo",
		"-postgres-uri", dsn,
		"-recorded-by", "lane-routing-verbs",
		"-review-evidence", "r6 observability killer",
		"-expect-build", verbTestBuild,
	)
	if err != nil {
		t.Fatalf("repoint: %v", err)
	}
	// The seeded row already names verbTestBuild (enable wrote it above),
	// so repoint has nothing to CHANGE -- confirming the per-row line
	// fires only on an actual change, matching disable's own convention.
	if strings.Contains(repointErrOut, "go_api_routing.repointed") {
		t.Fatalf("an UNCHANGED row must not emit a per-row line:\n%s", repointErrOut)
	}

	// Force an actual repoint by pointing -buildinfo-url at a fresh stub
	// reporting a DIFFERENT build -- -registry-url stays the original
	// server, so the schema-agreement preflight still passes.
	movedBuild := "cccccccccccccccccccccccccccccccccccccccc"
	buildinfoOnly := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") == "" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		writeJSON(t, w, map[string]any{"commit": movedBuild, "modified": false})
	}))
	t.Cleanup(buildinfoOnly.Close)

	repointOut, repointErrOut2, err := captureVerb(t, "repoint",
		"-registry-url", server.URL+"/registry",
		"-buildinfo-url", buildinfoOnly.URL,
		"-postgres-uri", dsn,
		"-recorded-by", "lane-routing-verbs",
		"-review-evidence", "r6 observability killer 2",
	)
	if err != nil {
		t.Fatalf("repoint: %v", err)
	}
	if !strings.Contains(repointOut, "go-api-routing: registry=") || !strings.Contains(repointOut, "buildinfo=") {
		t.Fatalf("repoint must print which endpoint it consulted:\n%s", repointOut)
	}
	if !strings.Contains(repointErrOut2, "go_api_routing.repointed operation="+verbTestOperation) ||
		!strings.Contains(repointErrOut2, "build_before="+verbTestBuild) ||
		!strings.Contains(repointErrOut2, "build_after="+movedBuild) {
		t.Fatalf("repoint must emit a structured before/after line once a row actually changed:\n%s", repointErrOut2)
	}
}

// The stale-digest-only refusal must NOT fire
// under `-operations all-registered` -- the documented rollback recipe's
// own flag value (docs/contribute/architecture/go-api-wave-0-proof-
// infrastructure.md, "The same procedure with the Go verbs"). One
// leftover old-digest row for an operation nobody explicitly asked about
// must not block the off-ramp for every operation that IS live: every
// schema move leaves rows like this behind (no verb deletes old rows),
// so this is the state the rollback is MOST likely to be needed in.
func TestDisableAllRegisteredSkipsStaleOnlyOperationsInsteadOfRefusing(t *testing.T) {
	pool, dsn := startVerbPostgres(t)
	ctx := context.Background()
	liveDigest := "4444444444444444444444444444444444444444444444444444444444444444"
	staleOnlyDigest := "5555555555555555555555555555555555555555555555555555555555555555"
	staleSchemaDigest := "sha256:" + strings.Repeat("8", 64)
	const staleOnlyOperation = "hotspots"
	catalogPath := writeCatalog(t, map[string]string{
		verbTestOperation:  liveDigest,
		staleOnlyOperation: staleOnlyDigest,
	})

	// verbTestOperation has a LIVE row (canary, at this binary's own
	// digest) -- the off-ramp must reach it.
	if _, err := pool.Exec(ctx, `
		INSERT INTO public.go_api_candidate_build (schema_digest, document_digest, selected_operation, candidate_build)
		VALUES ($1, $2, $3, $4)`,
		localSchemaDigest(), liveDigest, verbTestOperation, verbTestBuild); err != nil {
		t.Fatalf("seed live candidate build: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO public.go_api_routing_state
			(schema_digest, document_digest, selected_operation, current_candidate_build, owner, mode, rollout_percentage, review_evidence, recorded_by)
		VALUES ($1, $2, $3, $4, 'go', 'canary', 100, 'seeded live', 'test')`,
		localSchemaDigest(), liveDigest, verbTestOperation, verbTestBuild); err != nil {
		t.Fatalf("seed live routing row: %v", err)
	}

	// staleOnlyOperation ONLY has a row at an OLD digest -- nobody named
	// it explicitly; `all-registered` still auto-selects it.
	if _, err := pool.Exec(ctx, `
		INSERT INTO public.go_api_candidate_build (schema_digest, document_digest, selected_operation, candidate_build)
		VALUES ($1, $2, $3, $4)`,
		staleSchemaDigest, staleOnlyDigest, staleOnlyOperation, verbTestBuild); err != nil {
		t.Fatalf("seed stale-only candidate build: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO public.go_api_routing_state
			(schema_digest, document_digest, selected_operation, current_candidate_build, owner, mode, rollout_percentage, review_evidence, recorded_by)
		VALUES ($1, $2, $3, $4, 'go', 'canary', 100, 'seeded at a stale digest', 'test')`,
		staleSchemaDigest, staleOnlyDigest, staleOnlyOperation, verbTestBuild); err != nil {
		t.Fatalf("seed stale-only routing row: %v", err)
	}

	out, _, err := captureVerb(t, "disable", "-operations", "all-registered", "-mode", "python", "-postgres-uri", dsn, "-catalog", catalogPath,
		"-apply", "-recorded-by", "lane-routing-verbs", "-review-evidence", "r7 F1 killer")
	if err != nil {
		t.Fatalf("disable -operations all-registered must NOT refuse over a stale-only operation nobody named: %v", err)
	}
	if !strings.Contains(out, "applied: 1 row(s)") {
		t.Fatalf("the LIVE operation must still be disabled:\n%s", out)
	}
	if !strings.Contains(out, "!! 1 digest(s) OTHER than this checkout") || !strings.Contains(out, staleSchemaDigest) {
		t.Fatalf("the stale-only operation must still be NAMED, just not refused over:\n%s", out)
	}

	var liveMode string
	if err := pool.QueryRow(ctx, `SELECT mode FROM go_api_routing_state WHERE selected_operation = $1`, verbTestOperation).Scan(&liveMode); err != nil {
		t.Fatal(err)
	}
	if liveMode != "python" {
		t.Fatalf("the live operation's row must be disabled: mode = %q, want python", liveMode)
	}
	var staleMode string
	if err := pool.QueryRow(ctx, `SELECT mode FROM go_api_routing_state WHERE selected_operation = $1`, staleOnlyOperation).Scan(&staleMode); err != nil {
		t.Fatal(err)
	}
	if staleMode != "canary" {
		t.Fatalf("the stale-only row must be genuinely untouched: mode = %q, want canary", staleMode)
	}

	// Control (team-lead ruling, corrected: explicit naming does NOT
	// refuse either -- see TestDisableSkipsAndNamesAnOperationWithRowsOnlyAtOtherSchemaDigests,
	// which is the dedicated test for that shape): naming the stale-only
	// operation EXPLICITLY skips it identically, same as under
	// all-registered.
	out, _, err = captureVerb(t, "disable", "-operations", staleOnlyOperation, "-mode", "python", "-postgres-uri", dsn, "-catalog", catalogPath,
		"-apply", "-recorded-by", "lane-routing-verbs", "-review-evidence", "r7 F1 control")
	if err != nil {
		t.Fatalf("control: explicitly naming the stale-only operation must not refuse either: %v", err)
	}
	if !strings.Contains(out, "applied: 0 row(s)") {
		t.Fatalf("control: nothing to apply for the stale-only operation:\n%s", out)
	}
}

// ONE explicit URL flag plus GO_API_QUERY_API_URL set
// to a DIFFERENT process used to split the preflight across two
// processes silently -- no second flag was ever named on the command
// line. Must now refuse instead.
func TestEnableRefusesAMixOfOneExplicitURLFlagAndTheEnvVar(t *testing.T) {
	_, dsn := startVerbPostgres(t)
	digest := "7777777777777777777777777777777777777777777777777777777777777770"
	catalogPath := writeCatalog(t, map[string]string{verbTestOperation: digest})
	t.Setenv(bearerEnvVar, verbTestBearer)
	deployedServer := startQueryAPI(t, localSchemaDigest(), map[string]string{verbTestOperation: digest})
	t.Setenv("GO_API_QUERY_API_URL", deployedServer.URL) // a DIFFERENT process than the explicit flag below
	strayServer := startQueryAPI(t, localSchemaDigest(), map[string]string{verbTestOperation: digest})

	_, _, err := captureVerb(t,
		"enable", "-operations", verbTestOperation, "-mode", "canary",
		"-registry-url", strayServer.URL+"/registry", // explicit -- buildinfo left to fall back to the env var
		"-postgres-uri", dsn, "-catalog", catalogPath,
		"-recorded-by", "lane-routing-verbs", "-review-evidence", "r7 F2 killer",
	)
	if err == nil {
		t.Fatal("enable must refuse a mix of one explicit URL flag and the env var, not silently split the preflight across two processes")
	}
	if !strings.Contains(err.Error(), "-registry-url was named but -buildinfo-url was not") {
		t.Fatalf("refused for a different reason: %v", err)
	}
	assertNoRows(t, dsn)
}

// The endpoint-print line must be able to tell two
// LOCAL processes apart -- EndpointLabel (used elsewhere for credential
// safety) drops the port, and 127.0.0.1:A vs 127.0.0.1:B differ ONLY by
// port.
func TestEnableEndpointLineDistinguishesTwoLocalProcessesByPort(t *testing.T) {
	_, dsn := startVerbPostgres(t)
	digest := "7777777777777777777777777777777777777777777777777777777777777771"
	catalogPath := writeCatalog(t, map[string]string{verbTestOperation: digest})
	t.Setenv(bearerEnvVar, verbTestBearer)
	server := startQueryAPI(t, localSchemaDigest(), map[string]string{verbTestOperation: digest})

	out, _, err := captureVerb(t, enableArgs(server, dsn, catalogPath, "-acknowledge-unproven")...)
	if err != nil {
		t.Fatalf("enable: %v", err)
	}
	parsed, parseErr := url.Parse(server.URL)
	if parseErr != nil {
		t.Fatal(parseErr)
	}
	if !strings.Contains(out, "registry=http://"+parsed.Host+" buildinfo=http://"+parsed.Host) {
		t.Fatalf("the endpoint line must name the PORT (both routes are the same server here, so both must show it):\n%s", out)
	}
}

// `-candidate-build ""` (e.g. `-candidate-build
// "$SEEN"` where $SEEN happens to be unset in a script) must refuse, not
// silently apply the write UNGUARDED -- Python refuses the identical
// command.
func TestDisableRefusesAnExplicitlyEmptyCandidateBuildGuard(t *testing.T) {
	pool, dsn := startVerbPostgres(t)
	ctx := context.Background()
	digest := "9999999999999999999999999999999999999999999999999999999999999997"
	catalogPath := writeCatalog(t, map[string]string{verbTestOperation: digest})

	if _, err := pool.Exec(ctx, `
		INSERT INTO public.go_api_candidate_build (schema_digest, document_digest, selected_operation, candidate_build)
		VALUES ($1, $2, $3, $4)`,
		localSchemaDigest(), digest, verbTestOperation, verbTestBuild); err != nil {
		t.Fatalf("seed candidate build: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO public.go_api_routing_state
			(schema_digest, document_digest, selected_operation, current_candidate_build, owner, mode, rollout_percentage, review_evidence, recorded_by)
		VALUES ($1, $2, $3, $4, 'go', 'canary', 100, 'seeded', 'test')`,
		localSchemaDigest(), digest, verbTestOperation, verbTestBuild); err != nil {
		t.Fatalf("seed routing row: %v", err)
	}

	_, _, err := captureVerb(t, "disable", "-operations", verbTestOperation, "-mode", "python", "-postgres-uri", dsn, "-catalog", catalogPath,
		"-candidate-build", "", // EXPLICITLY passed, empty
		"-apply", "-recorded-by", "lane-routing-verbs", "-review-evidence", "r7 F3 killer",
	)
	if err == nil {
		t.Fatal("disable must refuse an explicitly empty -candidate-build, not silently apply the write unguarded")
	}
	if exitCodeFor(err) != 2 {
		t.Fatalf("exit %d, want 2", exitCodeFor(err))
	}
	var mode string
	if err := pool.QueryRow(ctx, `SELECT mode FROM go_api_routing_state`).Scan(&mode); err != nil {
		t.Fatal(err)
	}
	if mode != "canary" {
		t.Fatalf("the row must be genuinely untouched: mode = %q, want canary", mode)
	}

	// Control: OMITTING the flag entirely must still work (no guard, as
	// always).
	_, _, err = captureVerb(t, "disable", "-operations", verbTestOperation, "-mode", "python", "-postgres-uri", dsn, "-catalog", catalogPath,
		"-apply", "-recorded-by", "lane-routing-verbs", "-review-evidence", "r7 F3 control",
	)
	if err != nil {
		t.Fatalf("omitting -candidate-build entirely must still work unguarded: %v", err)
	}
}

// The before-state log line must not be keyed by
// OPERATION ALONE, with no ORDER BY -- so with a DEAD row (a document
// digest not in the catalog) and the LIVE row both present at the live
// schema digest, the log could name the dead row's state as if it were
// what enable actually replaced.
func TestEnableBeforeStateLogNamesTheCorrectRowNotADeadOne(t *testing.T) {
	pool, dsn := startVerbPostgres(t)
	ctx := context.Background()
	liveDigest := "aaaa111111111111111111111111111111111111111111111111111111111111"
	deadDigest := "bbbb222222222222222222222222222222222222222222222222222222222222"
	catalogPath := writeCatalog(t, map[string]string{verbTestOperation: liveDigest})
	t.Setenv(bearerEnvVar, verbTestBearer)
	server := startQueryAPI(t, localSchemaDigest(), map[string]string{verbTestOperation: liveDigest})

	// The LIVE row (catalog's document digest): mode=python.
	if _, err := pool.Exec(ctx, `
		INSERT INTO public.go_api_candidate_build (schema_digest, document_digest, selected_operation, candidate_build)
		VALUES ($1, $2, $3, $4)`,
		localSchemaDigest(), liveDigest, verbTestOperation, verbTestBuild); err != nil {
		t.Fatalf("seed live candidate build: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO public.go_api_routing_state
			(schema_digest, document_digest, selected_operation, current_candidate_build, owner, mode, rollout_percentage, review_evidence, recorded_by)
		VALUES ($1, $2, $3, $4, 'go', 'python', 0, 'seeded live', 'test')`,
		localSchemaDigest(), liveDigest, verbTestOperation, verbTestBuild); err != nil {
		t.Fatalf("seed live routing row: %v", err)
	}
	// A DEAD row (a document digest the catalog does NOT carry), same
	// operation, same live schema digest: mode=primary -- deliberately
	// the state a wrong-row read would print instead.
	if _, err := pool.Exec(ctx, `
		INSERT INTO public.go_api_candidate_build (schema_digest, document_digest, selected_operation, candidate_build)
		VALUES ($1, $2, $3, $4)`,
		localSchemaDigest(), deadDigest, verbTestOperation, verbTestBuild); err != nil {
		t.Fatalf("seed dead candidate build: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO public.go_api_routing_state
			(schema_digest, document_digest, selected_operation, current_candidate_build, owner, mode, rollout_percentage, review_evidence, recorded_by)
		VALUES ($1, $2, $3, $4, 'go', 'primary', 100, 'seeded dead', 'test')`,
		localSchemaDigest(), deadDigest, verbTestOperation, verbTestBuild); err != nil {
		t.Fatalf("seed dead routing row: %v", err)
	}

	_, errOut, err := captureVerb(t, enableArgs(server, dsn, catalogPath, "-acknowledge-unproven")...)
	if err != nil {
		t.Fatalf("enable: %v", err)
	}
	if !strings.Contains(errOut, "mode_before=python") {
		t.Fatalf("the log must name the LIVE row's prior state (python), not the dead row's (primary):\n%s", errOut)
	}
	if strings.Contains(errOut, "mode_before=primary") {
		t.Fatalf("the log must NOT name the dead row's state:\n%s", errOut)
	}
}
