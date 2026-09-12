//go:build integration

package externalrecompute

// enqueue_integration_test.go is the end-to-end proof of the whole point of
// CHAOS-5296: a recompute row the stream runner writes must turn into real
// native job handoffs on worker_job_outbox.
//
// Every earlier test in this package stops short of that. drain_integration_test
// fakes the Enqueuer so its failures are unambiguously about claiming; plan_test
// is pure. Neither would notice if the enqueue seam published nothing, published
// the wrong kind, or published a payload the job contract refuses -- which is
// precisely the class of silent no-op this ticket exists to end.

import (
	"context"
	"encoding/json"
	"log/slog"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// executableRegistry routes both kinds this consumer publishes as executable,
// which is their production state (Celery's daily route retired in CHAOS-4026).
type executableRegistry struct{}

func (executableRegistry) Descriptor(kind string) (jobruntime.Descriptor, bool) {
	switch kind {
	case jobcontract.KindDailyMetricsDispatch, jobcontract.KindInvestmentMaterialize:
		return jobruntime.Descriptor{
			Kind:              kind,
			CurrentVersion:    jobcontract.ContractVersionV1,
			SupportedVersions: []int{jobcontract.ContractVersionV1},
			Queue:             "metrics",
			Priority:          2,
			MaxAttempts:       5,
			MigrationState:    "go_implemented",
			Route:             "river",
			RollbackRoute:     "river",
		}, true
	default:
		return jobruntime.Descriptor{}, false
	}
}

func enqueueTestPool(t *testing.T, ctx context.Context) *pgxpool.Pool {
	t.Helper()
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
	createCompatibilityTables(t, ctx, pool)
	// Column shapes follow the real migrations (0057 for the daily tables, 0046
	// for the outbox, and the work-graph request table as
	// cmd/dev-health-workerctl/trigger_integration_test.go declares it), so a
	// type or length this code cannot actually satisfy fails here rather than
	// in production.
	if _, err := pool.Exec(ctx, `
CREATE TABLE public.daily_metrics_runs (
    id uuid PRIMARY KEY,
    org_id uuid NOT NULL,
    target_day date NOT NULL,
    generation varchar(64) NOT NULL,
    status varchar(16) NOT NULL DEFAULT 'pending',
    finalization_status varchar(16) NOT NULL DEFAULT 'pending',
    finalization_claim_token uuid NULL,
    finalization_lease_expires_at timestamptz NULL,
    finalized_at timestamptz NULL,
    repository_discovery_required boolean NOT NULL DEFAULT false,
    blocked_at timestamptz NULL,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    CONSTRAINT uq_daily_metrics_run_scope UNIQUE (org_id, target_day, generation)
);
CREATE TABLE public.daily_metrics_partitions (
    id uuid PRIMARY KEY,
    run_id uuid NOT NULL REFERENCES public.daily_metrics_runs(id) ON DELETE CASCADE,
    ordinal integer NOT NULL,
    repo_ids jsonb NOT NULL,
    status varchar(16) NOT NULL DEFAULT 'pending',
    claim_token uuid NULL,
    lease_expires_at timestamptz NULL,
    attempt_count integer NOT NULL DEFAULT 0,
    completed_at timestamptz NULL,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL
);
CREATE TABLE public.work_graph_execution_requests (
    id uuid PRIMARY KEY, org_id uuid NOT NULL, kind text NOT NULL, scope jsonb NOT NULL,
    model_ref text NULL, prompt_ref text NULL, llm_concurrency integer NOT NULL,
    spend_limit_microunits bigint NOT NULL, correlation_id text NOT NULL,
    idempotency_key text NOT NULL UNIQUE, state text NOT NULL,
    claim_token uuid NULL, lease_expires_at timestamptz NULL,
    attempt_count integer NOT NULL DEFAULT 0,
    created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT statement_timestamp()
);
CREATE TABLE public.worker_job_outbox (
    id uuid PRIMARY KEY, dedupe_key varchar(256) NOT NULL UNIQUE, job_kind varchar(96) NOT NULL,
    contract_version integer NOT NULL, args json NOT NULL, payload_hash varchar(71) NOT NULL,
    queue varchar(96) NOT NULL, priority smallint NOT NULL, max_attempts smallint NOT NULL,
    scheduled_at timestamptz NOT NULL, status varchar(16) NOT NULL, attempt_count integer NOT NULL,
    next_attempt_at timestamptz NOT NULL, prerequisite_completion_key text NULL,
    created_at timestamptz NOT NULL, updated_at timestamptz NOT NULL
);
CREATE TABLE public.worker_job_completion_fences (
    completion_key text PRIMARY KEY,
    completed_at timestamptz NOT NULL DEFAULT statement_timestamp()
)`); err != nil {
		t.Fatal(err)
	}
	return pool
}

func realEnqueuer(t *testing.T, pool *pgxpool.Pool) PostgresEnqueuer {
	t.Helper()
	store, err := daily.NewPostgresStore(pool)
	if err != nil {
		t.Fatal(err)
	}
	publisher, err := daily.NewPostgresPublisher(pool, executableRegistry{})
	if err != nil {
		t.Fatal(err)
	}
	writer, err := workgraph.NewRequestWriter(executableRegistry{})
	if err != nil {
		t.Fatal(err)
	}
	enqueuer, err := NewPostgresEnqueuer(store, publisher, writer)
	if err != nil {
		t.Fatal(err)
	}
	return enqueuer
}

type outboxRow struct {
	Kind      string
	DedupeKey string
	Args      string
}

func readOutbox(t *testing.T, ctx context.Context, pool *pgxpool.Pool) []outboxRow {
	t.Helper()
	rows, err := pool.Query(ctx, `
SELECT job_kind, dedupe_key, args::text FROM public.worker_job_outbox
ORDER BY job_kind, dedupe_key`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []outboxRow
	for rows.Next() {
		var row outboxRow
		if err := rows.Scan(&row.Kind, &row.DedupeKey, &row.Args); err != nil {
			t.Fatal(err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// TestDrainProducesNativeOutboxRowsEndToEnd is the acceptance test for the
// ticket: one recompute row in, real metrics.daily_dispatch and
// investment.materialize handoffs out.
func TestDrainProducesNativeOutboxRowsEndToEnd(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	pool := enqueueTestPool(t, ctx)
	now := time.Date(2026, 8, 20, 9, 0, 0, 0, time.UTC)

	orgID := uuid.New().String()
	repoID := uuid.New().String()
	bridgeID, correlation := seedNativeRow(t, ctx, pool, orgID, "acme/api",
		now.Add(-time.Minute), statusPending, func(scope map[string]any) {
			scope["repoIds"] = []string{repoID}
			scope["teamIds"] = []string{"team-a"}
			scope["recordKinds"] = []string{"pull_request.v1"}
			// A three-day window, so the per-day native daily contract has to
			// produce three runs where Python produced one task with
			// backfill_days=3. Getting this wrong is a silent loss of two days
			// of metrics, which no single-day fixture would catch.
			scope["windowStartedAt"] = "2026-08-17T00:00:00Z"
			scope["windowEndedAt"] = "2026-08-19T00:00:00Z"
		})

	drain, err := NewDrain(pool, realEnqueuer(t, pool), DefaultDrainConfig(),
		slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	drain.now = func() time.Time { return now }
	if _, err := drain.Step(ctx); err != nil {
		t.Fatal(err)
	}

	rows := readOutbox(t, ctx, pool)
	var dispatches, materializes int
	for _, row := range rows {
		switch row.Kind {
		case jobcontract.KindDailyMetricsDispatch:
			dispatches++
		case jobcontract.KindInvestmentMaterialize:
			materializes++
		default:
			t.Fatalf("unexpected job kind %q on the outbox", row.Kind)
		}
	}
	if dispatches != 3 {
		t.Fatalf("metrics.daily_dispatch rows = %d, want one per day of the window", dispatches)
	}
	if materializes != 1 {
		t.Fatalf("investment.materialize rows = %d, want exactly one per flush", materializes)
	}

	// The daily runs must be scoped to the batch's repository and carry the
	// external-recompute generation prefix -- without that prefix
	// MaterializeScheduledFanout refuses the run and it never terminalizes.
	var runCount int
	if err := pool.QueryRow(ctx, `
SELECT count(*) FROM public.daily_metrics_runs
WHERE org_id = $1::uuid AND generation = $2`, orgID, correlation).Scan(&runCount); err != nil {
		t.Fatal(err)
	}
	if runCount != 3 {
		t.Fatalf("daily runs = %d", runCount)
	}
	var days []time.Time
	dayRows, err := pool.Query(ctx, `
SELECT target_day FROM public.daily_metrics_runs ORDER BY target_day`)
	if err != nil {
		t.Fatal(err)
	}
	for dayRows.Next() {
		var day time.Time
		if err := dayRows.Scan(&day); err != nil {
			dayRows.Close()
			t.Fatal(err)
		}
		days = append(days, day)
	}
	dayRows.Close()
	if len(days) != 3 || days[0].Format(time.DateOnly) != "2026-08-17" ||
		days[2].Format(time.DateOnly) != "2026-08-19" {
		t.Fatalf("target days = %v, want 2026-08-17..2026-08-19", days)
	}

	// The materialize scope must be DATE-only. The native executor parses
	// from_date/to_date with time.DateOnly and refuses anything else, so an ISO
	// datetime here would fail at execution time, long after this enqueue
	// reported success.
	var scope map[string]any
	var scopeRaw []byte
	if err := pool.QueryRow(ctx, `
SELECT scope FROM public.work_graph_execution_requests WHERE org_id = $1::uuid`,
		orgID).Scan(&scopeRaw); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(scopeRaw, &scope); err != nil {
		t.Fatal(err)
	}
	if scope["from_date"] != "2026-08-17" || scope["to_date"] != "2026-08-19" {
		t.Fatalf("materialize scope dates = %v / %v", scope["from_date"], scope["to_date"])
	}
	if scope["force"] != false {
		t.Fatalf("materialize force = %v, want false (a freshness pass, not a rematerialize)", scope["force"])
	}
	repoIDs, ok := scope["repo_ids"].([]any)
	if !ok || len(repoIDs) != 1 || repoIDs[0] != repoID {
		t.Fatalf("materialize repo scope = %v", scope["repo_ids"])
	}

	if status := rowStatus(t, ctx, pool, bridgeID); status != statusDispatched {
		t.Fatalf("recompute row status = %s", status)
	}
	var batchStatus string
	if err := pool.QueryRow(ctx,
		`SELECT recompute_status FROM external_ingest_batches LIMIT 1`).Scan(&batchStatus); err != nil {
		t.Fatal(err)
	}
	if batchStatus != outcomeDispatched {
		t.Fatalf("batch recompute status = %s", batchStatus)
	}

	// A lease reclaim of an already-drained row must not double-publish. The
	// outbox dedupes on a deterministic key derived from the run/request id, so
	// the second pass adds nothing.
	if _, err := pool.Exec(ctx, `
UPDATE external_ingest_recompute_jobs SET status = $2, dispatched_at = $3 WHERE id = $1`,
		bridgeID, statusClaimed, now.Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
UPDATE external_ingest_batches SET recompute_status = 'pending'`); err != nil {
		t.Fatal(err)
	}
	if _, err := drain.Step(ctx); err != nil {
		t.Fatal(err)
	}
	if after := readOutbox(t, ctx, pool); len(after) != len(rows) {
		t.Fatalf("outbox rows after reclaim = %d, want %d (no duplicate handoffs)",
			len(after), len(rows))
	}
}

// TestDrainOrgWideFallbackDefersRepositoryDiscovery pins D8 through the real
// enqueue path. A work-item batch with no repository linkage must still produce
// a daily run -- one with NO partitions, so the heavy worker resolves the whole
// org from live ClickHouse. Producing zero runs (or a run with an empty
// partition, which reports success while computing nothing) are the two ways
// this silently under-recomputes.
func TestDrainOrgWideFallbackDefersRepositoryDiscovery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	pool := enqueueTestPool(t, ctx)
	now := time.Date(2026, 8, 20, 9, 0, 0, 0, time.UTC)

	orgID := uuid.New().String()
	seedNativeRow(t, ctx, pool, orgID, "acme/jira", now.Add(-time.Minute),
		statusPending, func(scope map[string]any) {
			scope["repoIds"] = []string{}
			scope["teamIds"] = []string{"team-a"}
			scope["recordKinds"] = []string{"work_item.v1"}
			scope["windowStartedAt"] = "2026-08-19T00:00:00Z"
			scope["windowEndedAt"] = "2026-08-19T00:00:00Z"
		})

	drain, err := NewDrain(pool, realEnqueuer(t, pool), DefaultDrainConfig(),
		slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	drain.now = func() time.Time { return now }
	if _, err := drain.Step(ctx); err != nil {
		t.Fatal(err)
	}

	var runCount, partitionCount int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM public.daily_metrics_runs`).Scan(&runCount); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM public.daily_metrics_partitions`).Scan(&partitionCount); err != nil {
		t.Fatal(err)
	}
	if runCount != 1 {
		t.Fatalf("daily runs = %d, want the org-wide fallback run", runCount)
	}
	if partitionCount != 0 {
		t.Fatalf("partitions = %d, want deferred discovery (zero durable partitions)", partitionCount)
	}
	var generation string
	if err := pool.QueryRow(ctx,
		`SELECT generation FROM public.daily_metrics_runs`).Scan(&generation); err != nil {
		t.Fatal(err)
	}
	// The generation prefix is load-bearing, not cosmetic: a deferred-discovery
	// run whose generation MaterializeScheduledFanout does not recognise is
	// refused at discovery time and stays 'running' forever.
	if generation[:len(daily.ExternalRecomputeGenerationPrefix)] !=
		daily.ExternalRecomputeGenerationPrefix {
		t.Fatalf("generation = %q, want the %q prefix",
			generation, daily.ExternalRecomputeGenerationPrefix)
	}
	// Team scope alone is still a scope, so investment must NOT be skipped.
	var materializeCount int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM public.work_graph_execution_requests`).Scan(&materializeCount); err != nil {
		t.Fatal(err)
	}
	if materializeCount != 1 {
		t.Fatalf("materialize requests = %d", materializeCount)
	}
}

// TestDrainSkipsInvestmentWithoutAnyScope is the D4 hard invariant reaching the
// database. An unscoped materialize fuses every tenant's work graph into one
// component and writes it under whichever org the run carried
// (internal/jobs/investment/scope.go), so "no request row at all" is the only
// acceptable outcome here.
func TestDrainSkipsInvestmentWithoutAnyScope(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	pool := enqueueTestPool(t, ctx)
	now := time.Date(2026, 8, 20, 9, 0, 0, 0, time.UTC)

	seedNativeRow(t, ctx, pool, uuid.New().String(), "acme/jira",
		now.Add(-time.Minute), statusPending, func(scope map[string]any) {
			scope["repoIds"] = []string{}
			scope["teamIds"] = []string{}
			scope["recordKinds"] = []string{"work_item.v1"}
			scope["windowStartedAt"] = "2026-08-19T00:00:00Z"
			scope["windowEndedAt"] = "2026-08-19T00:00:00Z"
		})

	drain, err := NewDrain(pool, realEnqueuer(t, pool), DefaultDrainConfig(),
		slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	drain.now = func() time.Time { return now }
	if _, err := drain.Step(ctx); err != nil {
		t.Fatal(err)
	}

	var materializeCount int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM public.work_graph_execution_requests`).Scan(&materializeCount); err != nil {
		t.Fatal(err)
	}
	if materializeCount != 0 {
		t.Fatal("investment materialize was dispatched with no repo and no team scope (D4)")
	}
	// The daily half still runs: work items with no repo linkage are exactly
	// what the org-wide fallback is for.
	var runCount int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM public.daily_metrics_runs`).Scan(&runCount); err != nil {
		t.Fatal(err)
	}
	if runCount != 1 {
		t.Fatalf("daily runs = %d", runCount)
	}
}

// TestReplayEnqueuesThroughTheSameSeamAsTheDrain proves the collapse path
// publishes real handoffs too. A replay that reported success while writing
// nothing would leave the operator believing a backlog had been drained.
func TestReplayEnqueuesThroughTheSameSeamAsTheDrain(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	pool := enqueueTestPool(t, ctx)
	now := time.Date(2026, 9, 7, 0, 0, 0, 0, time.UTC)

	orgID := uuid.New().String()
	for day := range 2 {
		bridgeID := uuid.New()
		scope, err := json.Marshal(map[string]any{
			"bridgeVersion":   1,
			"bridgeKind":      CompatibilityBridgeKind,
			"bridgeId":        bridgeID.String(),
			"repoIds":         []string{uuid.New().String()},
			"recordKinds":     []string{"commit.v1"},
			"windowStartedAt": now.AddDate(0, 0, -10+day).Format(time.RFC3339),
			"windowEndedAt":   now.AddDate(0, 0, -10+day).Format(time.RFC3339),
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `
INSERT INTO external_ingest_batches (
    ingestion_id, org_id, source_system, source_instance,
    recompute_status, recompute_scope, updated_at
) VALUES ($1,$2,'github','acme/api','pending',$3,now())`,
			uuid.New(), orgID, scope); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `
INSERT INTO external_ingest_recompute_jobs (
    id, org_id, source_system, source_instance, celery_task_name,
    celery_task_id, queue, repo_id, status, dispatched_at
) VALUES ($1,$2,'github','acme/api',$3,$4,'default',NULL,'bridge_pending',$5)`,
			bridgeID, orgID, LegacyCeleryTaskName, bridgeID.String(),
			now.AddDate(0, 0, -10+day)); err != nil {
			t.Fatal(err)
		}
	}

	report, err := Replay(ctx, pool, realEnqueuer(t, pool), now, 100, false)
	if err != nil {
		t.Fatal(err)
	}
	if report.Retired != 2 || report.Failed != 0 {
		t.Fatalf("replay report = %+v", report)
	}
	rows := readOutbox(t, ctx, pool)
	if len(rows) == 0 {
		t.Fatal("replay published nothing")
	}
	// Two backlog rows spanning two days collapse to ONE plan whose window
	// covers both, so the days are what multiply, not the rows.
	var dispatches, materializes int
	for _, row := range rows {
		switch row.Kind {
		case jobcontract.KindDailyMetricsDispatch:
			dispatches++
		case jobcontract.KindInvestmentMaterialize:
			materializes++
		}
	}
	if dispatches != 2 {
		t.Fatalf("daily dispatch rows = %d, want one per collapsed day", dispatches)
	}
	if materializes != 1 {
		t.Fatalf("materialize rows = %d, want one per collapsed grain", materializes)
	}
}

// TestDrainCoalescesRepeatedFlushesOfTheSameWindow is CHAOS-5642's claim
// measured at the seam that produced the backlog: two flushes naming the same
// org, the same day range and the same scope must leave ONE pending
// materialization, not two.
//
// The production shape it reproduces is the ordinary one, not a pathological
// one -- each flush is a separate bridge row with its own correlation, which is
// why each derived its own request id and why 2206 of them accumulated across
// 65 distinct days. Nothing about the second flush is a duplicate in the
// idempotency-key sense; it is a duplicate in the WORK sense, and only the
// coalescing key can see that.
//
// The assertions that matter are the STATE of the first request, not just the
// count: 'canceled' is the one state internal/joboutbox/strand_repair.go's
// work-graph sweep cannot select, so it is the only state in which the first
// request stays superseded across a reconciler tick.
func TestDrainCoalescesRepeatedFlushesOfTheSameWindow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	pool := enqueueTestPool(t, ctx)
	now := time.Date(2026, 8, 20, 9, 0, 0, 0, time.UTC)

	orgID := uuid.New().String()
	repoID := uuid.New().String()
	sameWindow := func(scope map[string]any) {
		scope["repoIds"] = []string{repoID}
		scope["teamIds"] = []string{"team-a"}
		scope["recordKinds"] = []string{"pull_request.v1"}
		scope["windowStartedAt"] = "2026-08-19T00:00:00Z"
		scope["windowEndedAt"] = "2026-08-19T00:00:00Z"
	}
	seedNativeRow(t, ctx, pool, orgID, "acme/api", now.Add(-2*time.Minute), statusPending, sameWindow)
	seedNativeRow(t, ctx, pool, orgID, "acme/api", now.Add(-time.Minute), statusPending, sameWindow)

	drain, err := NewDrain(pool, realEnqueuer(t, pool), DefaultDrainConfig(),
		slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	drain.now = func() time.Time { return now }
	for flush := range 2 {
		if _, err := drain.Step(ctx); err != nil {
			t.Fatalf("flush %d: %v", flush, err)
		}
	}

	// Both flushes wrote a request -- coalescing supersedes, it does not
	// suppress -- so the row count is 2 and only the STATES differ.
	var total, pending, canceled int
	if err := pool.QueryRow(ctx, `
SELECT count(*),
       count(*) FILTER (WHERE state = 'pending'),
       count(*) FILTER (WHERE state = 'canceled')
FROM public.work_graph_execution_requests
WHERE org_id = $1::uuid AND kind = $2`,
		orgID, string(workgraph.KindMaterialize)).Scan(&total, &pending, &canceled); err != nil {
		t.Fatal(err)
	}
	if total != 2 {
		t.Fatalf("materialize requests = %d, want one per flush", total)
	}
	if pending != 1 || canceled != 1 {
		t.Fatalf("pending=%d canceled=%d, want exactly one pending materialization per key",
			pending, canceled)
	}
}
