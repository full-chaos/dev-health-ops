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
		writeJSON(t, w, map[string]any{"commit": verbTestBuild, "version": "test", "build_time": "2026-09-10T00:00:00Z"})
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

	start := time.Now()
	out, _, err := captureVerb(t,
		"status",
		"-registry-url", server.URL+"/registry",
		"-postgres-uri", dsn,
		"-catalog", catalogPath,
		"-timeout", "300ms",
	)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("status must NEVER refuse, even with the database blocked: %v", err)
	}
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
	// least one operation, FetchRegistry itself would refuse first
	// ("registers no operations"), which would prove nothing about
	// preflight 3.
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
