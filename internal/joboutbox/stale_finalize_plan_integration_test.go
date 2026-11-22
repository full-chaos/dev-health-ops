//go:build integration

package joboutbox

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	riverstore "github.com/full-chaos/dev-health-ops/internal/storage/river"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// The plan of the stale-finalize survey at prod sizes (CHAOS-14090 (b)), on synthetic rows: worker_job_outbox 481,609
// rows (49,014 metrics.daily_finalize), daily_metrics_runs 49,009, daily_metrics_partitions 95,410, river_job 6,104.
//
// WANT at base 0: statement 0 is false, so the survey reads no outbox and no river_job (pinned by the reconciler cell
// TestStaleFinalizeBaseZeroReadsNoOutboxAndNoRiver). WANT at base 100: statements A and B read the outbox through
// ix_worker_job_outbox_finalize_domain_id (revision 0167) and never with a Seq Scan, and A returns the 100 candidates.
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
		base          = 100
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
WHERE run.rank * 2 - (1 - ordinal) <= $2::int`, old, partitionRows-base)
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
	explain := func(name, query string, args ...any) string {
		t.Helper()
		rows, err := admin.Query(ctx, "EXPLAIN (ANALYZE, BUFFERS) "+query, args...)
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
		return plan.String()
	}
	survey := func(name string) staleFinalizeSurvey {
		t.Helper()
		started := time.Now()
		result := surveyStaleFinalize(ctx, admin.Begin, baseSQL, candidatesSQL, levelsSQL, cutoff, base, nil)
		t.Logf("INFO survey at %s: %s, %d candidates, levels %+v, failures %+v", name, time.Since(started),
			len(result.candidates), result.levels, result.failures)
		return result
	}

	// Base 0.
	explain("statement 0 at base 0", baseSQL, cutoff)
	if result := survey("base 0"); len(result.candidates) != 0 || result.levels == nil || *result.levels != (StaleFinalizeLevels{}) {
		t.Fatalf("base 0: %+v, want no candidate and exact zero levels", result)
	}

	// Base 100: shape-D runs, every partition succeeded, nothing of theirs in the outbox or River.
	seedBase(t, ctx, admin, base, old)
	analyze()
	explain("statement 0 at base 100", baseSQL, cutoff)
	for name, query := range map[string]string{"statement A": candidatesSQL, "statement B": levelsSQL} {
		args := []any{cutoff, base, nil, nil}
		if name == "statement B" {
			args = []any{cutoff, staleFinalizeLevelCap + 1}
		}
		plan := explain(name+" at base 100", query, args...)
		if strings.Contains(plan, "Seq Scan on worker_job_outbox") {
			t.Fatalf("%s at base 100 scans the whole outbox:\n%s", name, plan)
		}
		if !strings.Contains(plan, "ix_worker_job_outbox_finalize_domain_id") {
			t.Fatalf("%s at base 100 does not read the outbox through ix_worker_job_outbox_finalize_domain_id:\n%s", name, plan)
		}
	}
	var candidates int
	if err := admin.QueryRow(ctx, "SELECT count(*) FROM ("+candidatesSQL+") AS a", cutoff, base, nil, nil).Scan(&candidates); err != nil {
		t.Fatal(err)
	}
	if candidates != base {
		t.Fatalf("statement A returned %d candidates at base 100, want %d", candidates, base)
	}
	survey("base 100")
}

// seedBase writes n runs in the base: running, finalize pending, written before the horizon, with two succeeded
// partitions each and nothing in the outbox or River.
func seedBase(t *testing.T, ctx context.Context, admin *pgxpool.Pool, n int, at time.Time) {
	t.Helper()
	if _, err := admin.Exec(ctx, `
WITH base AS (
    INSERT INTO public.daily_metrics_runs (id, org_id, target_day, generation, status, finalization_status, created_at, updated_at)
    SELECT gen_random_uuid(), '00000000-0000-4000-8000-000000014091'::uuid, DATE '2026-01-01' + n, 'plan-base:' || n,
           'running', 'pending', $1, $1
    FROM generate_series(1, $2::int) AS n
    RETURNING id
)
INSERT INTO public.daily_metrics_partitions (id, run_id, ordinal, repo_ids, status, attempt_count, completed_at, created_at, updated_at)
SELECT gen_random_uuid(), base.id, ordinal, '[]'::json, 'succeeded', 1, $1, $1, $1
FROM base CROSS JOIN generate_series(0, 1) AS ordinal`, at, n); err != nil {
		t.Fatalf("seed the base: %v", err)
	}
}
