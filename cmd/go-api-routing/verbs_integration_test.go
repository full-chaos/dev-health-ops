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
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/goapiproof"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/registryschema"
)

const (
	verbTestBuild     = "b18e56fa79cfe20ce0f75df148144b832d92be36"
	verbTestOperation = "flowMatrix"
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
		// r4 P2 (reproduced): `modified` must be explicitly present now --
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
	t.Setenv(bearerEnvVar, "envelope-for-the-fixture")

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
	t.Setenv(bearerEnvVar, "envelope-for-the-fixture")
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

// r2 P3 (reproduced): the whole reason status.go:155 covers the census
// AND the classification with one deadline is r1's F10 -- a diagnostic
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

	// r3 P3 (reproduced): captureVerb calls run(argv) SYNCHRONOUSLY, and
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
	t.Setenv(bearerEnvVar, "envelope-for-the-fixture")
	// The running process registers SOME operation, agreeing on the
	// schema digest -- but not the one this run asks for. Without at
	// least one operation, enable's OWN "registers no operations" check
	// (r5 P1: moved out of the shared FetchRegistry so status can read an
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
	t.Setenv(bearerEnvVar, "envelope-for-the-fixture")
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
	t.Setenv(bearerEnvVar, "envelope-for-the-fixture")
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
	t.Setenv(bearerEnvVar, "envelope-for-the-fixture")
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
	t.Setenv(bearerEnvVar, "envelope-for-the-fixture")
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

// r3 P3 (reproduced): `printStatusText`'s PROOF column has no killer at
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
	t.Setenv(bearerEnvVar, "envelope-for-the-fixture")
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

// r4 P1 (reproduced): `status` used to classify an operation purely from
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
	t.Setenv(bearerEnvVar, "envelope-for-the-fixture")
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

// r5 P1 (reproduced): `enable`'s Preflight 1 used to rely entirely on
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
	t.Setenv(bearerEnvVar, "envelope-for-the-fixture")
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

// r5 P1 (reproduced), repoint's identical preflight.
func TestRepointRefusesARegistryThatRegistersNothingAtAll(t *testing.T) {
	_, dsn := startVerbPostgres(t)
	digest := "5555555555555555555555555555555555555555555555555555555555555555"
	catalogPath := writeCatalog(t, map[string]string{verbTestOperation: digest})
	t.Setenv(bearerEnvVar, "envelope-for-the-fixture")
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

// r5 P1 (reproduced): the reviewer's own repro, end to end. Two ways
// `status` used to report a positive `reachable` answer that `enable`
// would refuse: a SCHEMA-level digest disagreement (checked before the
// per-operation document digest ever is), and the go plane being
// genuinely UNREACHABLE (down). Both must now report reachable=false or
// nil (unknown) respectively -- never a silent true.
func TestStatusReachableDegradesOnSchemaMismatchAndOnAnUnreachableGoPlane(t *testing.T) {
	pool, dsn := startVerbPostgres(t)
	digest := "6666666666666666666666666666666666666666666666666666666666666666"
	catalogPath := writeCatalog(t, map[string]string{verbTestOperation: digest})
	t.Setenv(bearerEnvVar, "envelope-for-the-fixture")
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
