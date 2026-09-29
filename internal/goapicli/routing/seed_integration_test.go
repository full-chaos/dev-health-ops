//go:build integration

package routing

// End-to-end tests for `seed` (CHAOS-7165): real command, real Postgres,
// fake deployed process. Every write-path assertion reads the rows back.

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/goapiproof"
)

const (
	seedOpA   = "home"
	seedOpB   = "recommendations"
	seedOpC   = "workItemTeamAttributions"
	seedDigA  = "1111111111111111111111111111111111111111111111111111111111111111"
	seedDigB  = "2222222222222222222222222222222222222222222222222222222222222222"
	seedDigC  = "3333333333333333333333333333333333333333333333333333333333333333"
	seedOlder = "sha256:0ldd00000000000000000000000000000000000000000000000000000000000d"
)

func seedDigests() map[string]string {
	return map[string]string{seedOpA: seedDigA, seedOpB: seedDigB, seedOpC: seedDigC}
}

func seedCmd(url, dsn, catalogPath string, extra ...string) []string {
	argv := []string{
		"seed",
		"-registry-url", url + "/registry",
		"-buildinfo-url", url + "/buildinfo",
		"-postgres-uri", dsn,
		"-catalog", catalogPath,
		"-recorded-by", "lane-seed-test",
		"-review-evidence", "CHAOS-7165 seed fixture",
	}
	return append(argv, extra...)
}

type seededRow struct {
	schema, doc, op, build, owner, mode, evidence, by string
	rollout                                           int
}

func readSeedRows(t *testing.T, pool *pgxpool.Pool) []seededRow {
	t.Helper()
	rows, err := pool.Query(context.Background(), `
		SELECT schema_digest, document_digest, selected_operation, current_candidate_build,
		       owner, mode, rollout_percentage, review_evidence, recorded_by
		  FROM go_api_routing_state ORDER BY selected_operation, schema_digest`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []seededRow
	for rows.Next() {
		var r seededRow
		if err := rows.Scan(&r.schema, &r.doc, &r.op, &r.build, &r.owner, &r.mode, &r.rollout, &r.evidence, &r.by); err != nil {
			t.Fatal(err)
		}
		out = append(out, r)
	}
	return out
}

func countTable(t *testing.T, pool *pgxpool.Pool, table string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM `+table).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func insertRowAt(t *testing.T, pool *pgxpool.Pool, schema, doc, op, mode string) {
	t.Helper()
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `INSERT INTO go_api_candidate_build (schema_digest, document_digest, selected_operation, candidate_build)
		VALUES ($1,$2,$3,$4) ON CONFLICT DO NOTHING`, schema, doc, op, verbTestBuild); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO go_api_routing_state
		(schema_digest, document_digest, selected_operation, current_candidate_build, owner, mode, rollout_percentage, review_evidence, recorded_by)
		VALUES ($1,$2,$3,$4,'go',$5,100,'pre-existing decision','operator')`, schema, doc, op, verbTestBuild, mode); err != nil {
		t.Fatal(err)
	}
}

// RED on the old behaviour: with no row, `disable -mode shadow -apply` writes
// nothing (the gap CHAOS-7165 names). The verb writes exactly one shadow row;
// a second run changes nothing.
func TestSeedCreatesExactlyOneShadowRowAndASecondRunChangesNothing(t *testing.T) {
	pool, dsn := startVerbPostgres(t)
	catalog := writeCatalog(t, map[string]string{seedOpA: seedDigA})
	t.Setenv(bearerEnvVar, verbTestBearer)
	server := startQueryAPI(t, localSchemaDigest(), map[string]string{seedOpA: seedDigA})

	if _, _, err := captureVerb(t, "disable", "-postgres-uri", dsn, "-catalog", catalog,
		"-operations", seedOpA, "-mode", "shadow", "-apply", "-recorded-by", "x", "-review-evidence", "y"); err != nil {
		t.Fatalf("disable: %v", err)
	}
	if n := countTable(t, pool, "go_api_routing_state"); n != 0 {
		t.Fatalf("OLD behaviour: disable -mode shadow on an operation with no row must leave 0 rows, got %d", n)
	}

	out, _, err := captureVerb(t, seedCmd(server.URL, dsn, catalog, "-operations", seedOpA)...)
	if err != nil {
		t.Fatalf("seed: %v", err)
	}
	if !strings.Contains(out, "created") {
		t.Fatalf("output must say created: %s", out)
	}
	rows := readSeedRows(t, pool)
	want := seededRow{localSchemaDigest(), seedDigA, seedOpA, verbTestBuild, "go", "shadow", "CHAOS-7165 seed fixture", "lane-seed-test", 0}
	if len(rows) != 1 || rows[0] != (seededRow{schema: want.schema, doc: want.doc, op: want.op, build: want.build, owner: "go", mode: "shadow", evidence: want.evidence, by: want.by, rollout: 0}) {
		t.Fatalf("want exactly one shadow row %+v, got %+v", want, rows)
	}
	if n := countTable(t, pool, "go_api_routing_audits"); n != 1 {
		t.Fatalf("audit rows = %d, want 1", n)
	}
	var updatedBefore string
	if err := pool.QueryRow(context.Background(), `SELECT updated_at::text FROM go_api_routing_state`).Scan(&updatedBefore); err != nil {
		t.Fatal(err)
	}

	out, _, err = captureVerb(t, seedCmd(server.URL, dsn, catalog, "-operations", seedOpA)...)
	if err != nil {
		t.Fatalf("second seed must succeed: %v", err)
	}
	if !strings.Contains(out, "already-present") || strings.Contains(out, " created ") {
		t.Fatalf("second run must say already-present: %s", out)
	}
	var updatedAfter string
	if err := pool.QueryRow(context.Background(), `SELECT updated_at::text FROM go_api_routing_state`).Scan(&updatedAfter); err != nil {
		t.Fatal(err)
	}
	if updatedAfter != updatedBefore || countTable(t, pool, "go_api_routing_state") != 1 ||
		countTable(t, pool, "go_api_routing_audits") != 1 || countTable(t, pool, "go_api_candidate_build") != 1 {
		t.Fatal("a second run must write nothing at all (rows, audit, candidate build, updated_at)")
	}
}

func TestSeedRefusesAnExistingRowInAnyOtherModeAndTouchesNothing(t *testing.T) {
	for _, mode := range []string{"canary", "primary", "python", "disabled"} {
		t.Run(mode, func(t *testing.T) {
			pool, dsn := startVerbPostgres(t)
			catalog := writeCatalog(t, map[string]string{seedOpA: seedDigA})
			t.Setenv(bearerEnvVar, verbTestBearer)
			server := startQueryAPI(t, localSchemaDigest(), map[string]string{seedOpA: seedDigA})
			insertRowAt(t, pool, localSchemaDigest(), seedDigA, seedOpA, mode)

			_, stderrOut, err := captureVerb(t, seedCmd(server.URL, dsn, catalog, "-operations", seedOpA)...)
			if err == nil {
				t.Fatalf("seed must refuse a %s row", mode)
			}
			rows := readSeedRows(t, pool)
			if len(rows) != 1 || rows[0].mode != mode || rows[0].evidence != "pre-existing decision" {
				t.Fatalf("the existing row must be untouched: %+v (stderr %s)", rows, stderrOut)
			}
			if countTable(t, pool, "go_api_routing_audits") != 0 {
				t.Fatal("a refusal must audit nothing")
			}
		})
	}
}

func TestSeedRefusesAnOperationWithARowOnlyAtAnOlderDigestAndSaysCarry(t *testing.T) {
	pool, dsn := startVerbPostgres(t)
	catalog := writeCatalog(t, map[string]string{seedOpA: seedDigA})
	t.Setenv(bearerEnvVar, verbTestBearer)
	server := startQueryAPI(t, localSchemaDigest(), map[string]string{seedOpA: seedDigA})
	insertRowAt(t, pool, seedOlder, seedDigA, seedOpA, "canary")

	out, _, err := captureVerb(t, seedCmd(server.URL, dsn, catalog, "-operations", seedOpA)...)
	if err == nil {
		t.Fatal("seed must refuse")
	}
	if !strings.Contains(out, "routing carry") {
		t.Fatalf("refusal must say to run routing carry: %s", out)
	}
	if rows := readSeedRows(t, pool); len(rows) != 1 || rows[0].schema != seedOlder {
		t.Fatalf("no row may be written at the running digest: %+v", rows)
	}
}

func TestSeedRefusesMismatchesBeforeWritingAnything(t *testing.T) {
	t.Setenv(bearerEnvVar, verbTestBearer)
	cases := map[string]struct {
		schema     string
		registry   map[string]string
		catalog    map[string]string
		operations string
		want       string
	}{
		"schema digest mismatch": {"sha256:ffff", map[string]string{seedOpA: seedDigA}, map[string]string{seedOpA: seedDigA}, seedOpA, "MISMATCH"},
		"op not registered":      {"", map[string]string{seedOpB: seedDigB}, map[string]string{seedOpA: seedDigA, seedOpB: seedDigB}, seedOpA, "does not register"},
		"catalog digest diverge": {"", map[string]string{seedOpA: seedDigA}, map[string]string{seedOpA: seedDigB}, seedOpA, "MISMATCH"},
		"op outside catalog":     {"", map[string]string{seedOpA: seedDigA}, map[string]string{seedOpB: seedDigB}, seedOpA, "not in the registered-operation catalog"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			pool, dsn := startVerbPostgres(t)
			schema := tc.schema
			if schema == "" {
				schema = localSchemaDigest()
			}
			server := startQueryAPI(t, schema, tc.registry)
			_, stderrOut, err := captureVerb(t, seedCmd(server.URL, dsn, writeCatalog(t, tc.catalog), "-operations", tc.operations)...)
			if err == nil || !strings.Contains(err.Error()+stderrOut, tc.want) {
				t.Fatalf("want refusal containing %q, got err=%v stderr=%s", tc.want, err, stderrOut)
			}
			if countTable(t, pool, "go_api_routing_state") != 0 || countTable(t, pool, "go_api_candidate_build") != 0 {
				t.Fatal("a preflight refusal must write nothing")
			}
		})
	}
}

func TestSeedDryRunWritesNothing(t *testing.T) {
	pool, dsn := startVerbPostgres(t)
	catalog := writeCatalog(t, map[string]string{seedOpA: seedDigA})
	t.Setenv(bearerEnvVar, verbTestBearer)
	server := startQueryAPI(t, localSchemaDigest(), map[string]string{seedOpA: seedDigA})
	out, _, err := captureVerb(t, seedCmd(server.URL, dsn, catalog, "-operations", seedOpA, "-dry-run")...)
	if err != nil || !strings.Contains(out, "would-create") {
		t.Fatalf("dry-run: err=%v out=%s", err, out)
	}
	for _, table := range []string{"go_api_routing_state", "go_api_candidate_build", "go_api_routing_audits"} {
		if countTable(t, pool, table) != 0 {
			t.Fatalf("dry-run wrote to %s", table)
		}
	}
}

func TestSeedAllUnroutedSeedsExactlyTheOperationsWithNoRowAtAnyDigest(t *testing.T) {
	pool, dsn := startVerbPostgres(t)
	catalog := writeCatalog(t, seedDigests())
	t.Setenv(bearerEnvVar, verbTestBearer)
	server := startQueryAPI(t, localSchemaDigest(), seedDigests())
	insertRowAt(t, pool, localSchemaDigest(), seedDigA, seedOpA, "shadow") // routed here
	insertRowAt(t, pool, seedOlder, seedDigB, seedOpB, "canary")           // routed at an older digest only

	out, _, err := captureVerb(t, seedCmd(server.URL, dsn, catalog, "-all-unrouted")...)
	if err != nil {
		t.Fatalf("seed -all-unrouted: %v\n%s", err, out)
	}
	var seeded []string
	for _, r := range readSeedRows(t, pool) {
		if r.schema == localSchemaDigest() && r.by == "lane-seed-test" {
			seeded = append(seeded, r.op)
		}
	}
	if len(seeded) != 1 || seeded[0] != seedOpC {
		t.Fatalf("only %s has no row at any digest; seeded %v\n%s", seedOpC, seeded, out)
	}
	if strings.Contains(out, seedOpB) && !strings.Contains(out, seedOpC) {
		t.Fatalf("an older-digest-only op must not be in the set: %s", out)
	}
}

func TestSeedNeedsExactlyOneOfOperationsOrAllUnrouted(t *testing.T) {
	_, dsn := startVerbPostgres(t)
	for name, extra := range map[string][]string{"neither": nil, "both": {"-operations", seedOpA, "-all-unrouted"}} {
		if _, _, err := captureVerb(t, seedCmd("http://127.0.0.1:1", dsn, writeCatalog(t, seedDigests()), extra...)...); err == nil {
			t.Fatalf("%s must refuse", name)
		}
	}
}

// A refusal on one operation does not stop the others (one transaction per
// operation), but the exit is non-zero.
func TestSeedMixedRunSeedsTheCleanOperationAndExitsNonZero(t *testing.T) {
	pool, dsn := startVerbPostgres(t)
	catalog := writeCatalog(t, seedDigests())
	t.Setenv(bearerEnvVar, verbTestBearer)
	server := startQueryAPI(t, localSchemaDigest(), seedDigests())
	insertRowAt(t, pool, localSchemaDigest(), seedDigA, seedOpA, "canary")
	if _, _, err := captureVerb(t, seedCmd(server.URL, dsn, catalog, "-operations", seedOpA+","+seedOpB)...); err == nil {
		t.Fatal("exit must be non-zero when any operation is refused")
	}
	got := map[string]string{}
	for _, r := range readSeedRows(t, pool) {
		got[r.op] = r.mode
	}
	if got[seedOpA] != "canary" || got[seedOpB] != "shadow" {
		t.Fatalf("rows = %v", got)
	}
}

// Two seeds racing on the same operation: exactly one creates, the other
// reports already-present, one row, one audit row.
func TestSeedConcurrentDoubleRunCreatesOneRow(t *testing.T) {
	pool, _ := startVerbPostgres(t)
	req := goapiproof.SeedRequest{
		SchemaDigest: localSchemaDigest(), RunningBuild: verbTestBuild,
		Operations: []string{seedOpA}, DocumentDigest: map[string]string{seedOpA: seedDigA},
		RecordedBy: "lane-seed-test", ReviewEvidence: "race", PrincipalID: verbTestPrincipalID,
	}
	const runners = 8
	actions := make([]string, runners)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < runners; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			outcomes, err := goapiproof.Seed(context.Background(), pool, req)
			if err != nil || len(outcomes) != 1 {
				t.Errorf("runner %d: %v %v", i, err, outcomes)
				return
			}
			actions[i] = outcomes[0].Action
		}()
	}
	close(start)
	wg.Wait()
	created := 0
	for _, a := range actions {
		switch a {
		case goapiproof.SeedActionCreated:
			created++
		case goapiproof.SeedActionAlreadyPresent:
		default:
			t.Fatalf("unexpected action %q in %v", a, actions)
		}
	}
	if created != 1 || countTable(t, pool, "go_api_routing_state") != 1 || countTable(t, pool, "go_api_routing_audits") != 1 {
		t.Fatalf("created=%d rows=%d audits=%d actions=%v", created,
			countTable(t, pool, "go_api_routing_state"), countTable(t, pool, "go_api_routing_audits"), actions)
	}
}

// DETERMINISTIC race: a competing writer holds the key uncommitted while Seed
// runs; Seed's read saw no row, so its insert blocks then conflicts. It must
// answer on what is actually there, not fail. The 8-way test above cannot
// promise it reaches this branch; this one always does.
func TestSeedInsertLosingTheRaceReportsAlreadyPresent(t *testing.T) {
	pool, _ := startVerbPostgres(t)
	ctx := context.Background()
	rival, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rival.Rollback(ctx) }()
	if _, err := rival.Exec(ctx, `INSERT INTO go_api_candidate_build (schema_digest, document_digest, selected_operation, candidate_build)
		VALUES ($1,$2,$3,$4)`, localSchemaDigest(), seedDigA, seedOpA, verbTestBuild); err != nil {
		t.Fatal(err)
	}
	if _, err := rival.Exec(ctx, `INSERT INTO go_api_routing_state
		(schema_digest, document_digest, selected_operation, current_candidate_build, owner, mode, rollout_percentage, review_evidence, recorded_by)
		VALUES ($1,$2,$3,$4,'go','shadow',0,'rival','rival')`, localSchemaDigest(), seedDigA, seedOpA, verbTestBuild); err != nil {
		t.Fatal(err)
	}
	type result struct {
		outcomes []goapiproof.SeedOutcome
		err      error
	}
	done := make(chan result, 1)
	go func() {
		o, err := goapiproof.Seed(ctx, pool, goapiproof.SeedRequest{
			SchemaDigest: localSchemaDigest(), RunningBuild: verbTestBuild,
			Operations: []string{seedOpA}, DocumentDigest: map[string]string{seedOpA: seedDigA},
			RecordedBy: "lane-seed-test", ReviewEvidence: "race", PrincipalID: verbTestPrincipalID,
		})
		done <- result{o, err}
	}()
	time.Sleep(500 * time.Millisecond) // Seed is now blocked on the rival's uncommitted key
	select {
	case r := <-done:
		t.Fatalf("Seed must be blocked on the rival's key, returned %+v", r)
	default:
	}
	if err := rival.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	r := <-done
	if r.err != nil || len(r.outcomes) != 1 || r.outcomes[0].Action != goapiproof.SeedActionAlreadyPresent {
		t.Fatalf("want already-present, got %+v err=%v", r.outcomes, r.err)
	}
	if n := countTable(t, pool, "go_api_routing_audits"); n != 0 {
		t.Fatalf("the loser must audit nothing, got %d", n)
	}
}
