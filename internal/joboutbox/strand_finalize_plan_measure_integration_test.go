//go:build integration

package joboutbox

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	riverstore "github.com/full-chaos/dev-health-ops/internal/storage/river"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// MEASUREMENT ONLY (CHAOS-14511 PLAN-14511-STRAND-1), never merged: the existing finalize strand-shape survey, the
// stale-finalize statement A and the redrive re-check, in 4 index states, at 49k and 490k finalize outbox rows, generic
// and custom plans, JIT off. Synthetic rows only. It logs MEASURE lines and the plans; it asserts nothing about them.
func TestMeasureFinalizeStrandSurveyPlans(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = instance.Close(context.Background()) }()
	admin := openIntegrationPool(t, ctx, instance.URI)
	defer admin.Close()
	createStrandSchema(t, ctx, admin)
	domainRole, err := containers.RoleName("sfm_domain", instance)
	if err != nil {
		t.Fatal(err)
	}
	queueRole, err := containers.RoleName("sfm_queue", instance)
	if err != nil {
		t.Fatal(err)
	}
	createStrandRoles(t, ctx, admin, domainRole, queueRole)
	if _, err := riverstore.ApplyPinnedMigrations(ctx, admin, riverstore.MigrationOptions{
		Schema: "river", DomainRole: domainRole, QueueRole: queueRole,
	}); err != nil {
		t.Fatal(err)
	}
	exec := func(what, query string, args ...any) {
		t.Helper()
		started := time.Now()
		if _, err := admin.Exec(ctx, query, args...); err != nil {
			t.Fatalf("%s: %v", what, err)
		}
		t.Logf("INFO %s in %s", what, time.Since(started))
	}
	jobTable := `"river"."river_job"`
	strandSurvey := newStrandShape("finalize", repairStrandedFinalizeSQL, jobTable).survey
	candidatesSQL := fmt.Sprintf(staleFinalizeCandidatesSQLTemplate, jobTable)
	states := []struct{ name, create string }{
		{"S1-no-index", ""},
		{"S2-current", "CREATE INDEX ix_measure ON public.worker_job_outbox ((args #>> '{domain,id}')) WHERE job_kind = 'metrics.daily_finalize'"},
		{"S3-trailing-river-job-id", "CREATE INDEX ix_measure ON public.worker_job_outbox ((args #>> '{domain,id}'), river_job_id) WHERE job_kind = 'metrics.daily_finalize'"},
		{"S4-include-river-job-id", "CREATE INDEX ix_measure ON public.worker_job_outbox ((args #>> '{domain,id}')) INCLUDE (river_job_id, status) WHERE job_kind = 'metrics.daily_finalize'"},
	}
	const (
		otherOutboxRows = 432595
		runs            = 49009
		strandRuns      = 100
		riverRows       = 6104
	)
	topBuffers := regexp.MustCompile(`Buffers: shared hit=(\d+)(?: read=(\d+))?`)
	execution := regexp.MustCompile(`Execution Time: ([0-9.]+) ms`)
	for _, finalizeRows := range []int{49014, 490140} {
		exec("reset", `TRUNCATE public.worker_job_outbox, public.daily_metrics_partitions, public.daily_metrics_runs, river.river_job RESTART IDENTITY CASCADE`)
		exec("drop the index", "DROP INDEX IF EXISTS public.ix_worker_job_outbox_finalize_domain_id")
		now := time.Now().UTC().Truncate(time.Microsecond)
		old := now.Add(-72 * time.Hour)
		org := "00000000-0000-4000-8000-000000014511"
		exec("runs", `
INSERT INTO public.daily_metrics_runs (id, org_id, target_day, generation, status, finalization_status, created_at, updated_at)
SELECT gen_random_uuid(), $1::uuid, DATE '2000-01-01' + n, 'measure:' || n,
       CASE WHEN n <= $3::int THEN 'running' ELSE 'succeeded' END,
       CASE WHEN n <= $3::int THEN 'pending' ELSE 'succeeded' END, $2, $2
FROM generate_series(1, $4::int) AS n`, org, old, strandRuns, runs)
		exec("partitions", `
INSERT INTO public.daily_metrics_partitions (id, run_id, ordinal, repo_ids, status, attempt_count, completed_at, created_at, updated_at)
SELECT gen_random_uuid(), run.id, ordinal, '[]'::jsonb, 'succeeded', 1, $1, $1, $1
FROM public.daily_metrics_runs AS run CROSS JOIN generate_series(0, 1) AS ordinal`, old)
		exec("strand River jobs", `
INSERT INTO river.river_job (id, args, attempt, created_at, finalized_at, kind, max_attempts, metadata, priority, queue, state, scheduled_at)
SELECT 10000000 + row_number() OVER (ORDER BY run.id),
       jsonb_build_object('domain', jsonb_build_object('type', 'daily_metrics_run', 'id', run.id::text)),
       1, $1, $1, 'metrics.daily_finalize', 4, '{}'::jsonb, 2, 'metrics', 'completed', $1
FROM public.daily_metrics_runs AS run WHERE run.status = 'running'`, old)
		exec("other River jobs", `
INSERT INTO river.river_job (id, args, attempt, created_at, finalized_at, kind, max_attempts, metadata, priority, queue, state, scheduled_at)
SELECT 10100000 + n,
       jsonb_build_object('domain', jsonb_build_object('type', 'daily_metrics_partition', 'id', gen_random_uuid()::text)),
       1, $1, $1, 'metrics.daily_partition', 4, '{}'::jsonb, 2, 'metrics', 'completed', $1
FROM generate_series(1, $2::int) AS n`, old, riverRows-strandRuns)
		pad := strings.Repeat("0123456789abcdef", 32)
		exec("other outbox rows", `
INSERT INTO public.worker_job_outbox
    (id, dedupe_key, job_kind, contract_version, args, payload_hash, queue, priority, max_attempts, scheduled_at,
     status, attempt_count, next_attempt_at, river_job_id, delivered_at, created_at, updated_at)
SELECT gen_random_uuid(), 'measure-other:' || n, 'metrics.daily_partition', 1,
       json_build_object('contract_version', 1, 'organization_id', $3::text, 'pad', $4::text,
           'domain', json_build_object('type', 'daily_metrics_partition', 'id', gen_random_uuid()::text)),
       'sha256:' || repeat('0', 64), 'metrics', 2, 4, $1, 'delivered', 1, $1, n, $1, $1, $1
FROM generate_series(1, $2::int) AS n`, old, otherOutboxRows, org, pad)
		exec("finalize outbox rows of cleaned jobs", `
INSERT INTO public.worker_job_outbox
    (id, dedupe_key, job_kind, contract_version, args, payload_hash, queue, priority, max_attempts, scheduled_at,
     status, attempt_count, next_attempt_at, river_job_id, delivered_at, created_at, updated_at)
SELECT gen_random_uuid(), 'measure-finalize:' || n, 'metrics.daily_finalize', 1,
       json_build_object('contract_version', 1, 'organization_id', $4::text, 'pad', $5::text,
           'domain', json_build_object('type', 'daily_metrics_run', 'id', gen_random_uuid()::text)),
       'sha256:' || repeat('0', 64), 'metrics', 2, 4, $1, 'delivered', 1, $1, $3::bigint + n, $1, $1, $1
FROM generate_series(1, $2::int) AS n`, old, finalizeRows-strandRuns, otherOutboxRows, org, pad)
		exec("finalize outbox rows of the strand runs", `
INSERT INTO public.worker_job_outbox
    (id, dedupe_key, job_kind, contract_version, args, payload_hash, queue, priority, max_attempts, scheduled_at,
     status, attempt_count, next_attempt_at, river_job_id, delivered_at, created_at, updated_at)
SELECT gen_random_uuid(), 'metrics.daily_finalize:' || (job.args #>> '{domain,id}'), 'metrics.daily_finalize', 1,
       json_build_object('contract_version', 1, 'organization_id', $2::text, 'pad', $3::text,
           'domain', json_build_object('type', 'daily_metrics_run', 'id', job.args #>> '{domain,id}')),
       'sha256:' || repeat('0', 64), 'metrics', 2, 4, $1, 'delivered', 1, $1, job.id, $1, $1, $1
FROM river.river_job AS job WHERE job.kind = 'metrics.daily_finalize'`, old, org, pad)
		exec("analyze", "VACUUM (ANALYZE) public.worker_job_outbox")
		exec("analyze river", "VACUUM (ANALYZE) river.river_job")
		exec("analyze runs", "ANALYZE public.daily_metrics_runs, public.daily_metrics_partitions")
		var recheckRun string
		if err := admin.QueryRow(ctx, `SELECT id::text FROM public.daily_metrics_runs WHERE generation = 'measure:1'`).Scan(&recheckRun); err != nil {
			t.Fatal(err)
		}
		for _, state := range states {
			exec(state.name+" drop", "DROP INDEX IF EXISTS public.ix_measure")
			if state.create != "" {
				exec(state.name+" create", state.create)
				exec(state.name+" analyze", "ANALYZE public.worker_job_outbox")
			}
			for _, statement := range []struct {
				name, types, query, args string
			}{
				{"strand-finalize-survey", "timestamptz, int", strandSurvey, "now(), 100"},
				{"stale-A", "timestamptz, int, timestamptz, uuid, timestamptz", candidatesSQL, "now() - interval '24 hours', 100, NULL, NULL, NULL"},
				{"recheck", "text, timestamptz", StaleFinalizeOutboxRecheckSQL, "'" + recheckRun + "', now() - interval '24 hours'"},
			} {
				for _, mode := range []string{"force_generic_plan", "force_custom_plan"} {
					conn, err := admin.Acquire(ctx)
					if err != nil {
						t.Fatal(err)
					}
					var plan strings.Builder
					func() {
						defer conn.Release()
						// The simple-query protocol: pgx's own modes refuse a statement text that names $1 with no value.
						simple := func(sql string) [][][]byte {
							results, err := conn.Conn().PgConn().Exec(ctx, sql).ReadAll()
							if err != nil {
								t.Fatalf("%.120s: %v", sql, err)
							}
							var rows [][][]byte
							for _, result := range results {
								if result.Err != nil {
									t.Fatalf("%.120s: %v", sql, result.Err)
								}
								rows = append(rows, result.Rows...)
							}
							return rows
						}
						simple("SET jit = off")
						simple("SET plan_cache_mode = " + mode)
						simple("PREPARE measure(" + statement.types + ") AS " + statement.query)
						defer simple("DEALLOCATE measure")
						// One warm-up run, then the measured run.
						simple("EXECUTE measure(" + statement.args + ")")
						for _, row := range simple("EXPLAIN (ANALYZE, BUFFERS) EXECUTE measure(" + statement.args + ")") {
							plan.Write(row[0])
							plan.WriteString("\n")
						}
					}()
					text := plan.String()
					hit, read := "", "0"
					if m := topBuffers.FindStringSubmatch(text); m != nil {
						hit = m[1]
						if m[2] != "" {
							read = m[2]
						}
					}
					ms := ""
					if m := execution.FindStringSubmatch(text); m != nil {
						ms = m[1]
					}
					t.Logf("MEASURE finalize_rows=%d state=%s stmt=%s plan=%s exec_ms=%s shared_hit=%s shared_read=%s probe=%v seq_outbox=%v uses_measure_index=%v",
						finalizeRows, state.name, statement.name, strings.TrimPrefix(strings.TrimSuffix(mode, "_plan"), "force_"), ms, hit, read,
						strings.Contains(text, "(river_job_id = job.id)"), strings.Contains(text, "Seq Scan on worker_job_outbox"),
						strings.Contains(text, "ix_measure"))
					t.Logf("PLAN finalize_rows=%d state=%s stmt=%s plan=%s\n%s", finalizeRows, state.name, statement.name, mode, text)
				}
			}
		}
	}
}
