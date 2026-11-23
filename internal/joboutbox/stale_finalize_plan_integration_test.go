//go:build integration

package joboutbox

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	riverstore "github.com/full-chaos/dev-health-ops/internal/storage/river"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// The plan of the stale-finalize survey at prod sizes (CHAOS-14090 (b)), on synthetic rows: worker_job_outbox 481,609
// rows (49,014 metrics.daily_finalize), daily_metrics_runs 49,009, daily_metrics_partitions 95,410, river_job 6,104.
//
// WANT at base 0: statement 0 is false, so the survey reads no outbox and no river_job (pinned by the reconciler cell
// TestStaleFinalizeBaseZeroReadsNoOutboxAndNoRiver). WANT at base 7 x perClass (seedBaseClasses: every class, k = 2
// finalize outbox rows per run): statements A and B read the outbox through ix_worker_job_outbox_finalize_domain_id
// (revision 0167) and never with a Seq Scan; every outbox and river_job node runs; an outbox node probed per base run
// reads at most k x base rows, one run once (a hashed subplan) at most the index's entries; A returns the perClass
// candidates and the survey counts each class.
// Timings are logged as INFO only (D6418: no wall-time assertion).
func TestStaleFinalizeSurveyPlanAtProdSizes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		_ = instance.Close(closeCtx)
	}()
	admin := openIntegrationPool(t, ctx, instance.URI)
	defer admin.Close()
	createStrandSchema(t, ctx, admin)
	domainRole, err := containers.RoleName("sfplan_domain", instance)
	if err != nil {
		t.Fatal(err)
	}
	defer containers.DropRole(admin, domainRole, t.Logf)
	queueRole, err := containers.RoleName("sfplan_queue", instance)
	if err != nil {
		t.Fatal(err)
	}
	defer containers.DropRole(admin, queueRole, t.Logf)
	createStrandRoles(t, ctx, admin, domainRole, queueRole)
	if _, err := riverstore.ApplyPinnedMigrations(ctx, admin, riverstore.MigrationOptions{
		Schema: "river", DomainRole: domainRole, QueueRole: queueRole,
	}); err != nil {
		t.Fatal(err)
	}

	now := time.Now().UTC().Truncate(time.Microsecond)
	old := now.Add(-72 * time.Hour)
	const (
		outboxRows    = 481609
		finalizeRows  = 49014
		runs          = 49009
		partitionRows = 95410
		riverRows     = 6104
		perClass      = 15
		base          = 7 * perClass
		k             = 2
	)
	seed := func(what, query string, args ...any) {
		t.Helper()
		started := time.Now()
		if _, err := admin.Exec(ctx, query, args...); err != nil {
			t.Fatalf("seed %s: %v", what, err)
		}
		t.Logf("INFO seeded %s in %s", what, time.Since(started))
	}
	// Not one of these runs is in the base: every one ended.
	seed("runs", `
INSERT INTO public.daily_metrics_runs (id, org_id, target_day, generation, status, finalization_status, created_at, updated_at)
SELECT gen_random_uuid(), '00000000-0000-4000-8000-000000014090'::uuid, DATE '2020-01-01' + (n / 3), 'plan:' || n,
       'succeeded', 'succeeded', $1, $1
FROM generate_series(1, $2::int) AS n`, old, runs-base)
	seed("partitions", `
INSERT INTO public.daily_metrics_partitions (id, run_id, ordinal, repo_ids, status, attempt_count, completed_at, created_at, updated_at)
SELECT gen_random_uuid(), run.id, ordinal, '[]'::json, 'succeeded', 1, $1, $1, $1
FROM (SELECT id, row_number() OVER (ORDER BY id) AS rank FROM public.daily_metrics_runs) AS run
CROSS JOIN generate_series(0, 1) AS ordinal
WHERE run.rank * 2 - (1 - ordinal) <= $2::int`, old, partitionRows-2*6*perClass)
	seed("outbox", `
INSERT INTO public.worker_job_outbox
    (id, dedupe_key, job_kind, contract_version, args, payload_hash, queue, priority, max_attempts, scheduled_at,
     status, attempt_count, next_attempt_at, river_job_id, delivered_at, created_at, updated_at)
SELECT gen_random_uuid(), 'plan:' || n,
       CASE WHEN n <= $2::int THEN 'metrics.daily_finalize' ELSE 'metrics.daily_partition' END, 1,
       json_build_object('contract_version', 1, 'domain', json_build_object('type', 'daily_metrics_run', 'id', gen_random_uuid()::text)),
       'sha256:' || repeat('0', 64), 'metrics', 2, 4, $1, 'delivered', 1, $1, 1000000000 + n, $1, $1, $1
FROM generate_series(1, $3::int) AS n`, old, finalizeRows, outboxRows)
	seed("river jobs", `
INSERT INTO river.river_job (args, attempt, created_at, finalized_at, kind, max_attempts, metadata, priority, queue, state, scheduled_at)
SELECT jsonb_build_object('domain', jsonb_build_object('type', 'daily_metrics_run', 'id', gen_random_uuid()::text)),
       1, $1, $1, 'metrics.daily_finalize', 4, '{}'::jsonb, 2, 'metrics', 'completed', $1
FROM generate_series(1, $2::int) AS n`, old, riverRows)
	analyze := func() {
		t.Helper()
		for _, statement := range []string{
			"VACUUM (ANALYZE) public.worker_job_outbox", "VACUUM (ANALYZE) river.river_job",
			"ANALYZE public.daily_metrics_runs, public.daily_metrics_partitions",
		} {
			if _, err := admin.Exec(ctx, statement); err != nil {
				t.Fatalf("%s: %v", statement, err)
			}
		}
	}
	analyze()

	jobTable := `"river"."river_job"`
	baseSQL := staleFinalizeBaseExistsSQL
	candidatesSQL := fmt.Sprintf(staleFinalizeCandidatesSQLTemplate, jobTable)
	levelsSQL := fmt.Sprintf(staleFinalizeLevelsSQLTemplate, jobTable)
	cutoff := now.Add(-StaleFinalizeHorizon)
	// explain runs EXPLAIN (ANALYZE, BUFFERS) with JIT off, as the survey and the redrive re-check run, and checks the
	// statement's Execution Time against half of its server-side timeout (50 % margin).
	explain := func(name, query string, bound float64, args ...any) string {
		t.Helper()
		tx, err := admin.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
		if _, err := tx.Exec(ctx, staleFinalizeJITOff); err != nil {
			t.Fatal(err)
		}
		rows, err := tx.Query(ctx, "EXPLAIN (ANALYZE, BUFFERS) "+query, args...)
		if err != nil {
			t.Fatalf("EXPLAIN %s: %v", name, err)
		}
		defer rows.Close()
		var plan strings.Builder
		for rows.Next() {
			var line string
			if err := rows.Scan(&line); err != nil {
				t.Fatal(err)
			}
			plan.WriteString(line + "\n")
		}
		if err := rows.Err(); err != nil || plan.Len() == 0 {
			t.Fatalf("EXPLAIN %s: %v, %d bytes", name, err, plan.Len())
		}
		t.Logf("INFO plan of %s:\n%s", name, plan.String())
		match := executionTime.FindStringSubmatch(plan.String())
		if match == nil {
			t.Fatalf("EXPLAIN %s: no Execution Time line", name)
		}
		executed, err := strconv.ParseFloat(match[1], 64)
		if err != nil {
			t.Fatal(err)
		}
		if executed > bound {
			t.Fatalf("%s executed in %.1f ms with JIT off, bound %.0f ms (half its statement timeout):\n%s", name, executed, bound, plan.String())
		}
		return plan.String()
	}
	survey := func(name string) staleFinalizeSurvey {
		t.Helper()
		started := time.Now()
		var jitInA []string
		begin := func(ctx context.Context) (pgx.Tx, error) {
			tx, err := admin.Begin(ctx)
			if err != nil {
				return nil, err
			}
			return &jitProbeTx{Tx: tx, probeBefore: candidatesSQL, seen: &jitInA}, nil
		}
		result := surveyStaleFinalize(ctx, begin, baseSQL, candidatesSQL, levelsSQL, cutoff, base, nil)
		if len(result.candidates) > 0 && (len(jitInA) != 1 || jitInA[0] != "off") {
			t.Fatalf("survey at %s: jit inside the survey transaction at statement A = %v, want [off]", name, jitInA)
		}
		t.Logf("INFO survey at %s: %s, %d candidates, levels %+v, failures %+v", name, time.Since(started),
			len(result.candidates), result.levels, result.failures)
		return result
	}

	// Base 0.
	explain("statement 0 at base 0", baseSQL, 75, cutoff)
	if result := survey("base 0"); len(result.candidates) != 0 || result.levels == nil || *result.levels != (StaleFinalizeLevels{}) {
		t.Fatalf("base 0: %+v, want no candidate and exact zero levels", result)
	}

	// The base: perClass runs of each class, with k finalize outbox rows per run where the class has outbox rows, so every
	// outbox and River sub-plan of A and B runs (fcc F4: a node that never ran meets any rows bound).
	seedBaseClasses(t, ctx, admin, perClass, old, now)
	analyze()
	var finalizeEntries float64
	if err := admin.QueryRow(ctx, `SELECT count(*) FROM public.worker_job_outbox WHERE job_kind = 'metrics.daily_finalize'`).Scan(&finalizeEntries); err != nil {
		t.Fatal(err)
	}
	explain("statement 0 at base", baseSQL, 75, cutoff)
	var recheckRun string
	if err := admin.QueryRow(ctx, `SELECT id::text FROM public.daily_metrics_runs WHERE generation = 'plan-candidate:1'`).Scan(&recheckRun); err != nil {
		t.Fatal(err)
	}
	for _, statement := range []struct {
		name  string
		query string
		bound float64
		args  []any
	}{
		{"statement A", candidatesSQL, 200, []any{cutoff, base, nil, nil, nil}},
		{"statement B", levelsSQL, 125, []any{cutoff, staleFinalizeLevelCap + 1}},
		// The redrive's re-check runs under the 200 ms per-run deadline.
		{"redrive re-check", StaleFinalizeOutboxRecheckSQL, 100, []any{recheckRun, cutoff}},
	} {
		name, query, args := statement.name, statement.query, statement.args
		plan := explain(name+" at base", query, statement.bound, args...)
		if strings.Contains(plan, "Seq Scan on worker_job_outbox") {
			t.Fatalf("%s at base scans the whole outbox:\n%s", name, plan)
		}
		if !strings.Contains(plan, "ix_worker_job_outbox_finalize_domain_id") {
			t.Fatalf("%s at base does not read the outbox through ix_worker_job_outbox_finalize_domain_id:\n%s", name, plan)
		}
		if name == "redrive re-check" {
			continue
		}
		checkSubplanReads(t, ctx, admin, name, query, args, float64(k*base), finalizeEntries)
	}
	var candidates int
	if err := admin.QueryRow(ctx, "SELECT count(*) FROM ("+candidatesSQL+") AS a", cutoff, base, nil, nil, nil).Scan(&candidates); err != nil {
		t.Fatal(err)
	}
	if candidates != perClass {
		t.Fatalf("statement A returned %d candidates, want %d", candidates, perClass)
	}
	result := survey("base")
	want := StaleFinalizeLevels{Live: 2 * perClass, StrandShape: perClass, Recent: 2 * perClass, NoPartitions: perClass}
	if len(result.candidates) != perClass || result.levels == nil || *result.levels != want {
		t.Fatalf("survey at base: %d candidates, levels %+v, want %d and %+v", len(result.candidates), result.levels, perClass, want)
	}
}

var executionTime = regexp.MustCompile(`Execution Time: ([0-9.]+) ms`)

// jitProbeTx reads the session's jit setting inside the survey transaction just before statement A runs.
type jitProbeTx struct {
	pgx.Tx
	probeBefore string
	seen        *[]string
}

func (tx *jitProbeTx) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	if sql == tx.probeBefore {
		var setting string
		if err := tx.Tx.QueryRow(ctx, "SHOW jit").Scan(&setting); err != nil {
			return nil, err
		}
		*tx.seen = append(*tx.seen, setting)
	}
	return tx.Tx.Query(ctx, sql, args...)
}

// planNode is one node of EXPLAIN (ANALYZE, FORMAT JSON). Actual Rows is the average per loop.
type planNode struct {
	NodeType         string     `json:"Node Type"`
	RelationName     string     `json:"Relation Name"`
	IndexName        string     `json:"Index Name"`
	ActualRows       float64    `json:"Actual Rows"`
	ActualLoops      float64    `json:"Actual Loops"`
	RemovedByFilter  float64    `json:"Rows Removed by Filter"`
	RemovedByRecheck float64    `json:"Rows Removed by Index Recheck"`
	Plans            []planNode `json:"Plans"`
}

// checkSubplanReads walks the analyzed plan of the statement. Every node on worker_job_outbox or river_job must have
// run (loops > 0). An outbox node probed per base run (loops > 1) reads at most perProbeBound rows in all (k rows per
// base run); an outbox node run once (a hashed subplan, allowed) reads at most the partial index's entries. A Seq Scan
// on the outbox is refused by the caller's plan-text WANT.
func checkSubplanReads(t *testing.T, ctx context.Context, admin *pgxpool.Pool, name, query string, args []any, perProbeBound, indexEntries float64) {
	t.Helper()
	var raw []byte
	tx, err := admin.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if _, err := tx.Exec(ctx, staleFinalizeJITOff); err != nil {
		t.Fatal(err)
	}
	if err := tx.QueryRow(ctx, "EXPLAIN (ANALYZE, FORMAT JSON) "+query, args...).Scan(&raw); err != nil {
		t.Fatalf("EXPLAIN JSON %s: %v", name, err)
	}
	var plans []struct {
		Plan planNode `json:"Plan"`
	}
	if err := json.Unmarshal(raw, &plans); err != nil || len(plans) != 1 {
		t.Fatalf("EXPLAIN JSON %s: %v, %d plans", name, err, len(plans))
	}
	var outboxNodes, riverNodes int
	var walk func(node planNode)
	walk = func(node planNode) {
		switch node.RelationName {
		case "worker_job_outbox", "river_job":
			if node.ActualLoops == 0 {
				t.Fatalf("%s: the %s node on %s never executed: the seed does not reach it", name, node.NodeType, node.RelationName)
			}
		}
		if node.RelationName == "river_job" {
			riverNodes++
		}
		if node.RelationName == "worker_job_outbox" {
			outboxNodes++
			read := (node.ActualRows + node.RemovedByFilter + node.RemovedByRecheck) * node.ActualLoops
			form, bound := "per-probe", perProbeBound
			if node.ActualLoops == 1 {
				form, bound = "once", indexEntries
			}
			t.Logf("INFO %s: outbox %s (%s) loops %.0f read %.0f, bound %.0f (%s)", name, node.NodeType, node.IndexName, node.ActualLoops, read, bound, form)
			if read > bound {
				t.Fatalf("%s: the %s outbox node read %.0f rows over %.0f loops, bound %.0f (%s)", name, node.NodeType, read, node.ActualLoops, bound, form)
			}
		}
		for _, child := range node.Plans {
			walk(child)
		}
	}
	walk(plans[0].Plan)
	if outboxNodes == 0 || riverNodes == 0 {
		t.Fatalf("%s: %d outbox and %d river_job nodes in the plan, want both > 0", name, outboxNodes, riverNodes)
	}
}

// seedBaseClasses writes perClass base runs (running, finalize pending, written before the horizon) of each class, one
// org per class. k = 2 finalize outbox rows per run where the class has outbox rows: the canonical row and a redrive-key
// row. River job ids of dangling rows (the River job was pruned) start at 2,000,000,000, above every real id.
//
//	candidate      2 partitions, 2 delivered outbox rows whose River jobs are gone          -> redrive
//	live_river     2 partitions, a River job still available                                 -> live
//	live_outbox    2 partitions, 2 pending outbox rows                                       -> live
//	strand         2 partitions, 2 delivered outbox rows, each with its completed River job  -> strand_shape
//	recent_river   2 partitions, a River job created after the cutoff                        -> recent
//	recent_outbox  2 partitions, 2 delivered outbox rows created after the cutoff, jobs gone -> recent
//	no_partitions  no partition, 2 delivered outbox rows whose River jobs are gone            -> no_partitions
func seedBaseClasses(t *testing.T, ctx context.Context, admin *pgxpool.Pool, perClass int, at, now time.Time) {
	t.Helper()
	recent := now.Add(-time.Hour)
	classes := []string{"candidate", "live_river", "live_outbox", "strand", "recent_river", "recent_outbox", "no_partitions"}
	exec := func(what, query string, args ...any) {
		t.Helper()
		if _, err := admin.Exec(ctx, query, args...); err != nil {
			t.Fatalf("seed %s: %v", what, err)
		}
	}
	for index, class := range classes {
		exec(class+" runs", `
INSERT INTO public.daily_metrics_runs (id, org_id, target_day, generation, status, finalization_status, created_at, updated_at)
SELECT gen_random_uuid(), ('00000000-0000-4000-8000-0000001409' || $3)::uuid, DATE '2026-01-01' + n,
       'plan-' || $4 || ':' || n, 'running', 'pending', $1, $1
FROM generate_series(1, $2::int) AS n`, at, perClass, fmt.Sprintf("%02d", index), class)
	}
	runsOf := `SELECT id FROM public.daily_metrics_runs WHERE generation LIKE 'plan-' || $1 || ':%'`
	exec("partitions", `
INSERT INTO public.daily_metrics_partitions (id, run_id, ordinal, repo_ids, status, attempt_count, completed_at, created_at, updated_at)
SELECT gen_random_uuid(), run.id, ordinal, '[]'::json, 'succeeded', 1, $1, $1, $1
FROM public.daily_metrics_runs AS run CROSS JOIN generate_series(0, 1) AS ordinal
WHERE run.generation LIKE 'plan-%' AND run.generation NOT LIKE 'plan-no_partitions:%'`, at)
	outbox := func(class, status string, created time.Time, riverJobID string) {
		t.Helper()
		delivered := "NULL"
		if status == "delivered" {
			delivered = "$2"
		}
		exec(class+" outbox", `
INSERT INTO public.worker_job_outbox
    (id, dedupe_key, job_kind, contract_version, args, payload_hash, queue, priority, max_attempts, scheduled_at,
     status, attempt_count, next_attempt_at, river_job_id, delivered_at, created_at, updated_at)
SELECT gen_random_uuid(), 'plan-' || $1 || ':' || run.id || ':' || copy, 'metrics.daily_finalize', 1,
       json_build_object('contract_version', 1, 'domain', json_build_object('type', 'daily_metrics_run', 'id', run.id::text)),
       'sha256:' || repeat('0', 64), 'metrics', 2, 4, $2, $3, 1, $2, `+riverJobID+`, `+delivered+`, $2, $2
FROM (`+runsOf+`) AS run CROSS JOIN generate_series(1, 2) AS copy`, class, created, status)
	}
	dangling := "2000000000::bigint + (abs(hashtext(run.id::text || copy)) % 1000000000)"
	outbox("candidate", "delivered", at, dangling)
	outbox("live_outbox", "pending", at, "NULL")
	outbox("recent_outbox", "delivered", recent, dangling)
	outbox("no_partitions", "delivered", at, dangling)
	river := func(class, state string, created time.Time) {
		t.Helper()
		finalized := "NULL"
		attempt := "0"
		if state == "completed" {
			finalized, attempt = "$3", "1"
		}
		exec(class+" river jobs", `
INSERT INTO river.river_job (args, attempt, created_at, finalized_at, kind, max_attempts, metadata, priority, queue, state, scheduled_at)
SELECT jsonb_build_object('domain', jsonb_build_object('type', 'daily_metrics_run', 'id', run.id::text)),
       `+attempt+`, $3, `+finalized+`, 'metrics.daily_finalize', 4, '{}'::jsonb, 2, 'metrics', $2::river.river_job_state, $3
FROM (`+runsOf+`) AS run`, class, state, created)
	}
	river("live_river", "available", at)
	river("recent_river", "completed", recent)
	exec("strand River jobs and outbox rows", `
WITH job AS (
    INSERT INTO river.river_job (args, attempt, created_at, finalized_at, kind, max_attempts, metadata, priority, queue, state, scheduled_at)
    SELECT jsonb_build_object('domain', jsonb_build_object('type', 'daily_metrics_run', 'id', run.id::text), 'copy', copy),
           1, $1, $1, 'metrics.daily_finalize', 4, '{}'::jsonb, 2, 'metrics', 'completed', $1
    FROM (SELECT id FROM public.daily_metrics_runs WHERE generation LIKE 'plan-strand:%') AS run CROSS JOIN generate_series(1, 2) AS copy
    RETURNING id, args #>> '{domain,id}' AS run_id, args ->> 'copy' AS copy
)
INSERT INTO public.worker_job_outbox
    (id, dedupe_key, job_kind, contract_version, args, payload_hash, queue, priority, max_attempts, scheduled_at,
     status, attempt_count, next_attempt_at, river_job_id, delivered_at, created_at, updated_at)
SELECT gen_random_uuid(), 'plan-strand:' || job.run_id || ':' || job.copy, 'metrics.daily_finalize', 1,
       json_build_object('contract_version', 1, 'domain', json_build_object('type', 'daily_metrics_run', 'id', job.run_id)),
       'sha256:' || repeat('0', 64), 'metrics', 2, 4, $1, 'delivered', 1, $1, job.id, $1, $1, $1
FROM job`, at)
}
