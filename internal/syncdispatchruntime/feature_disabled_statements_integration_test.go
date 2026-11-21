//go:build integration

package syncdispatchruntime

import (
	"context"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// statementHook records the statements one pool sends and, once, runs fire on another connection
// when a statement containing match is about to be sent: a deterministic interleaving point.
type statementHook struct {
	mu         sync.Mutex
	match      string
	fire       func()
	fired      bool
	statements []string
}

func (hook *statementHook) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	hook.mu.Lock()
	hook.statements = append(hook.statements, collapseStatement(data.SQL))
	fire := !hook.fired && hook.match != "" && strings.Contains(data.SQL, hook.match)
	if fire {
		hook.fired = true
	}
	hook.mu.Unlock()
	if fire {
		hook.fire()
	}
	return ctx
}

func (*statementHook) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func (hook *statementHook) arm(match string, fire func()) {
	hook.mu.Lock()
	defer hook.mu.Unlock()
	hook.match, hook.fire, hook.fired, hook.statements = match, fire, false, nil
}

func (hook *statementHook) didFire() bool {
	hook.mu.Lock()
	defer hook.mu.Unlock()
	return hook.fired
}

// startCancelHookPools returns an admin pool and a pool whose statements go through hook.
func startCancelHookPools(t *testing.T) (context.Context, *pgxpool.Pool, *pgxpool.Pool, *statementHook) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	instance := pgschema.Start(ctx, t)
	t.Cleanup(func() { instance.Close(context.Background()) })
	admin, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	config, err := pgxpool.ParseConfig(instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	hook := &statementHook{}
	config.ConnConfig.Tracer = hook
	hooked, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(hooked.Close)
	return ctx, admin, hooked, hook
}

// seedFeatureDisabledStatementFixture is one open run with a unit in each open status, a success
// unit, a pending ledger, a pending wakeup, a running job run and a running backfill job: every
// statement of the feature-disabled Run and Graph has a row to act on.
func seedFeatureDisabledStatementFixture(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	seedFeatureDisabledRun(t, ctx, pool, 5)
	insertFeatureDisabledUnit(t, ctx, pool, "00000000-0000-4000-8000-0000000001e1", "planned", nil)
	insertFeatureDisabledUnit(t, ctx, pool, "00000000-0000-4000-8000-0000000001e2", "retrying", nil)
	insertFeatureDisabledUnit(t, ctx, pool, "00000000-0000-4000-8000-0000000001e3", "dispatching", nil)
	owner := "worker-1"
	insertFeatureDisabledUnit(t, ctx, pool, "00000000-0000-4000-8000-0000000001e4", "running", &owner)
	insertFeatureDisabledUnit(t, ctx, pool, "00000000-0000-4000-8000-0000000001e5", "success", nil)
	if _, err := pool.Exec(ctx, `
INSERT INTO sync_run_reference_discoveries (id, sync_run_id, org_id, status, attempts, available_at, created_at, updated_at)
VALUES ('00000000-0000-4000-8000-0000000001e6', $1, $2, 'planned', 0, now(), now(), now())`, featureDisabledTestRun, featureDisabledTestOrg); err != nil {
		t.Fatal(err)
	}
	pgseed.SyncDispatchOutbox(ctx, t, pool, "00000000-0000-4000-8000-0000000001e7", featureDisabledTestRun, featureDisabledTestOrg, "dispatch_sync_run", "pending", "", 0)
	pgseed.ScheduledJob(ctx, t, pool, "00000000-0000-4000-8000-0000000001e8", featureDisabledTestOrg)
	pgseed.JobRun(ctx, t, pool, "00000000-0000-4000-8000-0000000001e9", "00000000-0000-4000-8000-0000000001e8", jobRunStatusRunning, `{"sync_run_id":"`+featureDisabledTestRun+`"}`)
	pgseed.SyncConfiguration(ctx, t, pool, "00000000-0000-4000-8000-0000000001ea", featureDisabledTestOrg, featureDisabledTestIntegration, `{}`, time.Now())
	if _, err := pool.Exec(ctx, `
INSERT INTO backfill_jobs (id, org_id, sync_config_id, celery_task_id, status, since_date, before_date)
VALUES ('00000000-0000-4000-8000-0000000001eb', $1, '00000000-0000-4000-8000-0000000001ea', $2, 'running', '2026-01-01', '2026-01-02')`,
		featureDisabledTestOrg, "sync_run:"+featureDisabledTestRun); err != nil {
		t.Fatal(err)
	}
}

// The feature-disabled callers send exactly the statements they sent on main, in the same order.
func TestFeatureDisabledTerminationSendsTheStatementsOfMain(t *testing.T) {
	ctx, admin, hooked, hook := startCancelHookPools(t)
	now := time.Now().UTC().Truncate(time.Microsecond)
	seedFeatureDisabledStatementFixture(t, ctx, admin)
	hook.arm("", nil)
	withTx(t, ctx, hooked, func(tx pgx.Tx) {
		run, err := loadFinalizeRun(ctx, tx, featureDisabledTestOrg, featureDisabledTestRun)
		if err != nil || run == nil {
			t.Fatalf("load: run=%v err=%v", run, err)
		}
		transition, err := terminalizeFeatureDisabledRun(ctx, tx, run, testFeatureDisabledMessage, now)
		if err != nil || !transition.RunTerminal {
			t.Fatalf("terminalizeFeatureDisabledRun: %+v %v", transition, err)
		}
		if err := terminalizeFeatureDisabledGraph(ctx, tx, run, testFeatureDisabledMessage, *run.completedAt); err != nil {
			t.Fatalf("terminalizeFeatureDisabledGraph: %v", err)
		}
	})
	hook.mu.Lock()
	got := slices.Clone(hook.statements)
	hook.mu.Unlock()
	want := featureDisabledStatementsOfMain()
	if !slices.Equal(got, want) {
		t.Fatalf("feature-disabled statements changed:\ngot  %d %q\nwant %d %q", len(got), got, len(want), want)
	}
}

// featureDisabledStatementsOfMain is the statement list the feature-disabled terminator sent on
// main 70817a1ff0ef (Run then Graph), recorded by this cell there for seedFeatureDisabledStatementFixture, whitespace-collapsed, in order.
func featureDisabledStatementsOfMain() []string {
	return []string{
		"begin",
		"SELECT id::text, org_id, integration_id::text, status, total_units, completed_units, failed_units, completed_at, result, error FROM public.sync_runs WHERE id = $1::uuid AND org_id = $2",
		"SELECT DISTINCT org_id, provider, cost_class FROM public.sync_run_units WHERE sync_run_id = $1::uuid AND status IN ('planned', 'retrying', 'dispatching', 'running')",
		"SELECT pg_try_advisory_xact_lock($1)",
		"SELECT id::text, lease_owner FROM public.sync_run_units WHERE sync_run_id = $1::uuid AND status = 'running'",
		"UPDATE public.sync_run_units SET status = $2, available_at = NULL, error = $3, result = $4::json, lease_owner = NULL, lease_expires_at = NULL, updated_at = $5 WHERE sync_run_id = $1::uuid AND status IN ('planned', 'retrying', 'dispatching')",
		"UPDATE public.sync_run_units SET status = $2, available_at = NULL, error = $3, result = $4::json, lease_owner = NULL, lease_expires_at = NULL, updated_at = $5 WHERE id = $1::uuid AND sync_run_id = $6::uuid AND status = 'running' AND lease_owner IS NOT DISTINCT FROM $7",
		"SELECT 1 FROM public.sync_runs WHERE id = $1 FOR UPDATE",
		"SELECT status FROM public.sync_run_units WHERE sync_run_id = $1::uuid",
		"UPDATE public.sync_runs SET failed_units = $2, completed_units = $3, error = $4, result = (CASE WHEN (result::jsonb)->'skipped_datasets' IS NOT NULL THEN (($5::json)::jsonb || jsonb_build_object('skipped_datasets', (result::jsonb)->'skipped_datasets'))::json ELSE $5::json END), status = $6, completed_at = $7 WHERE id = $1::uuid",
		"UPDATE public.sync_run_reference_discoveries SET status = $2, lease_owner = NULL, lease_expires_at = NULL, last_heartbeat_at = $3, completed_at = $3, error = $4, result = $5::json, updated_at = $3 WHERE sync_run_id = $1::uuid AND status IN ($6, $7, $8)",
		"UPDATE public.sync_dispatch_outbox SET status = 'dispatched', dispatched_at = $2, last_error = $3, claim_token = NULL, claim_expires_at = NULL, claim_transport = NULL, claim_route_generation = NULL, dispatched_transport = NULL, dispatched_route_generation = NULL, transport_job_id = NULL, updated_at = $2 WHERE sync_run_id = $1::uuid AND status = 'pending'",
		"INSERT INTO public.sync_dispatch_outbox ( id, org_id, sync_run_id, kind, status, available_at, attempts, dispatched_at, last_error, created_at, updated_at ) VALUES ($1::uuid, $2, $3::uuid, $4, 'dispatched', $5, 0, $5, $6, $5, $5) ON CONFLICT (sync_run_id, kind) DO UPDATE SET status = 'dispatched', last_error = EXCLUDED.last_error, dispatched_at = EXCLUDED.dispatched_at, claim_token = NULL, claim_expires_at = NULL, claim_transport = NULL, claim_route_generation = NULL, dispatched_transport = NULL, dispatched_route_generation = NULL, transport_job_id = NULL, updated_at = EXCLUDED.updated_at",
		"SELECT id::text, celery_task_id FROM public.backfill_jobs WHERE org_id = $1 AND status IN ('pending', 'running') AND celery_task_id LIKE '%' || $2 || '%'",
		"UPDATE public.backfill_jobs SET status = $1, total_chunks = $2, completed_chunks = $3, failed_chunks = $4, completed_at = $5, error_message = $6 WHERE org_id = $7 AND (celery_task_id LIKE '%' || $8 || '%' OR id = ANY($9::uuid[]))",
		"UPDATE public.job_runs SET status = $1, completed_at = $2, error = $3, result = (result::jsonb || $4::jsonb)::json WHERE status IN ($5, $6) AND result->>'sync_run_id' = $7",
		"commit",
	}
}
