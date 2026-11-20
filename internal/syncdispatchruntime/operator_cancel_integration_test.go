//go:build integration

package syncdispatchruntime

import (
	"context"
	"errors"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providersync"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	cancelTestRun      = "00000000-0000-4000-8000-000000143821"
	cancelTestOtherRun = "00000000-0000-4000-8000-000000143822"
	cancelTestReason   = "operator_test"
	cancelTestMessage  = "cancelled: sync run cancelled by the operator (reason operator_test)"
)

// cancelFixture is one unfinished run with one unit per open status, a success unit, a pending
// reference-discovery ledger, a pending dispatch_sync_run wakeup, a running job_runs row and a
// running backfill_jobs row. units maps each unit's dataset_key (its label) to its id.
type cancelFixture struct {
	runID string
	units map[string]string
}

func seedCancelFixture(t *testing.T, ctx context.Context, pool *pgxpool.Pool, runID string, now time.Time, extra ...pgseed.SyncRunUnit) cancelFixture {
	t.Helper()
	pgseed.EnsureSyncRun(ctx, t, pool, pgseed.SyncRun{
		ID: runID, OrgID: featureDisabledTestOrg, IntegrationID: featureDisabledTestIntegration, Status: "dispatching",
	})
	expired := now.Add(-time.Hour)
	owner := "worker-expired"
	units := []pgseed.SyncRunUnit{
		{DatasetKey: "a-planned", Status: "planned"},
		{DatasetKey: "b-retrying", Status: "retrying"},
		{DatasetKey: "c-dispatching", Status: "dispatching"},
		{DatasetKey: "d-dispatching-unrelayed", Status: "dispatching"},
		{DatasetKey: "e-running-expired", Status: "running", LeaseOwner: &owner, LeaseExpiresAt: &expired},
		{DatasetKey: "f-success", Status: "success"},
	}
	units = append(units, extra...)
	fixture := cancelFixture{runID: runID, units: map[string]string{}}
	for _, unit := range units {
		unit.ID = uuid.NewString()
		unit.RunID, unit.OrgID, unit.IntegrationID = runID, featureDisabledTestOrg, featureDisabledTestIntegration
		unit.CostClass = "standard"
		pgseed.InsertSyncRunUnit(ctx, t, pool, unit)
		fixture.units[unit.DatasetKey] = unit.ID
	}
	if _, err := pool.Exec(ctx, `UPDATE public.sync_runs SET total_units = $2 WHERE id = $1`, runID, len(units)); err != nil {
		t.Fatal(err)
	}
	pgseed.WorkerJobOutbox(ctx, t, pool, "sync.provider_unit:"+fixture.units["c-dispatching"], "sync.provider_unit", "delivered")
	pgseed.WorkerJobOutbox(ctx, t, pool, "sync.provider_unit:"+fixture.units["d-dispatching-unrelayed"], "sync.provider_unit", "pending")
	if _, err := pool.Exec(ctx, `
INSERT INTO sync_run_reference_discoveries (id, sync_run_id, org_id, status, attempts, available_at, created_at, updated_at)
VALUES ($1, $2, $3, 'planned', 0, now(), now(), now())`, uuid.NewString(), runID, featureDisabledTestOrg); err != nil {
		t.Fatal(err)
	}
	pgseed.SyncDispatchOutbox(ctx, t, pool, uuid.NewString(), runID, featureDisabledTestOrg, "dispatch_sync_run", "pending", "", 0)
	jobID := uuid.NewString()
	pgseed.ScheduledJob(ctx, t, pool, jobID, featureDisabledTestOrg)
	pgseed.JobRun(ctx, t, pool, uuid.NewString(), jobID, jobRunStatusRunning, `{"sync_run_id":"`+runID+`"}`)
	configID := uuid.NewString()
	pgseed.SyncConfiguration(ctx, t, pool, configID, featureDisabledTestOrg, featureDisabledTestIntegration, `{}`, now)
	if _, err := pool.Exec(ctx, `
INSERT INTO backfill_jobs (id, org_id, sync_config_id, celery_task_id, status, since_date, before_date)
VALUES ($1, $2, $3, $4, 'running', '2026-01-01', '2026-01-02')`,
		uuid.NewString(), featureDisabledTestOrg, configID, "sync_run:"+runID); err != nil {
		t.Fatal(err)
	}
	return fixture
}

// cancelSnapshot is every row the cancel could write for the run, as text: equal before and after
// a call means the call wrote nothing.
func cancelSnapshot(t *testing.T, ctx context.Context, pool *pgxpool.Pool, runID string) string {
	t.Helper()
	var snapshot string
	if err := pool.QueryRow(ctx, `
SELECT json_build_object(
  'run', (SELECT row_to_json(r) FROM public.sync_runs AS r WHERE r.id = $1::uuid),
  'units', (SELECT json_agg(u ORDER BY u.dataset_key) FROM public.sync_run_units AS u WHERE u.sync_run_id = $1::uuid),
  'ledger', (SELECT json_agg(d) FROM public.sync_run_reference_discoveries AS d WHERE d.sync_run_id = $1::uuid),
  'outbox', (SELECT json_agg(o ORDER BY o.kind) FROM public.sync_dispatch_outbox AS o WHERE o.sync_run_id = $1::uuid),
  'backfill', (SELECT json_agg(b ORDER BY b.id) FROM public.backfill_jobs AS b WHERE b.celery_task_id = 'sync_run:' || $1::text),
  'job_runs', (SELECT json_agg(j ORDER BY j.id) FROM public.job_runs AS j WHERE j.result->>'sync_run_id' = $1::text),
  'worker_outbox', (SELECT json_agg(w ORDER BY w.dedupe_key) FROM public.worker_job_outbox AS w
                    WHERE w.dedupe_key IN (SELECT 'sync.provider_unit:' || id::text FROM public.sync_run_units WHERE sync_run_id = $1::uuid))
)::text`, runID).Scan(&snapshot); err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func cancelRequest(runID string, now time.Time) CancelSyncRunRequest {
	return CancelSyncRunRequest{OrgID: featureDisabledTestOrg, SyncRunID: runID, ReasonCode: cancelTestReason, Now: now}
}

func cancelTestNow() time.Time { return time.Now().UTC().Truncate(time.Microsecond) }

// A queued run is closed the way the code's own terminator closes one: every open unit failed with
// the cancelled category and no lease, the run failed, the ledger failed, no pending wakeup, one
// terminally-dispatched finalize row, the backfill job failed and the job run failed (3).
func TestCancelSyncRunClosesAQueuedRunAndEveryObserver(t *testing.T) {
	ctx, pool := startFeatureDisabledPool(t)
	now := cancelTestNow()
	seedCancelFixture(t, ctx, pool, cancelTestRun, now)

	outcome, err := CancelSyncRun(ctx, pool, cancelRequest(cancelTestRun, now))
	if err != nil {
		t.Fatalf("CancelSyncRun: %v", err)
	}
	wantUnits := CancelSyncRunUnits{Planned: 1, Retrying: 1, Dispatching: 2, RunningExpired: 1, Success: 1}
	if outcome.Outcome != CancelOutcomeCancelled || outcome.PriorStatus != "dispatching" || outcome.Units != wantUnits ||
		outcome.UnitsFailed != 5 || outcome.ProviderUnitDeliveries != (CancelSyncRunDeliveries{Delivered: 1, Pending: 1}) {
		t.Fatalf("outcome %+v, want cancelled from dispatching, units %+v, 5 failed, 1 delivered and 1 pending delivery",
			outcome, wantUnits)
	}

	var runStatus, runError, runResult string
	var completed, failed int
	var completedAt *time.Time
	if err := pool.QueryRow(ctx, `SELECT status, completed_units, failed_units, completed_at, error, result::text
FROM public.sync_runs WHERE id = $1`, cancelTestRun).Scan(&runStatus, &completed, &failed, &completedAt, &runError, &runResult); err != nil {
		t.Fatal(err)
	}
	if runStatus != "failed" || completed != 1 || failed != 5 || completedAt == nil || !completedAt.Equal(now) ||
		runError != cancelTestMessage || !strings.Contains(runResult, `"error_category":"cancelled"`) {
		t.Fatalf("run status=%s completed=%d failed=%d completed_at=%v error=%q result=%s", runStatus, completed, failed, completedAt, runError, runResult)
	}
	rows, err := pool.Query(ctx, `SELECT dataset_key, status, coalesce(error, ''), coalesce(result::text, ''),
	lease_owner IS NULL AND lease_expires_at IS NULL AND available_at IS NULL
FROM public.sync_run_units WHERE sync_run_id = $1 ORDER BY dataset_key`, cancelTestRun)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var label, status, unitError, result string
		var cleared bool
		if err := rows.Scan(&label, &status, &unitError, &result, &cleared); err != nil {
			t.Fatal(err)
		}
		if label == "f-success" {
			if status != "success" || unitError != "" {
				t.Fatalf("success unit touched: status=%s error=%q", status, unitError)
			}
			continue
		}
		if status != "failed" || unitError != cancelTestMessage || result != `{"error_category":"cancelled"}` || !cleared {
			t.Fatalf("unit %s status=%s error=%q result=%s lease/available cleared=%v", label, status, unitError, result, cleared)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}

	var ledgerStatus, ledgerResult string
	if err := pool.QueryRow(ctx, `SELECT status, result::text FROM sync_run_reference_discoveries WHERE sync_run_id = $1`,
		cancelTestRun).Scan(&ledgerStatus, &ledgerResult); err != nil {
		t.Fatal(err)
	}
	if ledgerStatus != discoveryStatusFailed || ledgerResult != `{"error_category":"cancelled"}` {
		t.Fatalf("ledger status=%s result=%s", ledgerStatus, ledgerResult)
	}
	var pendingOutbox, finalizeRows int
	if err := pool.QueryRow(ctx, `SELECT count(*) FILTER (WHERE status = 'pending'),
	count(*) FILTER (WHERE kind = 'finalize_sync_run' AND status = 'dispatched' AND last_error = 'feature_disabled')
FROM sync_dispatch_outbox WHERE sync_run_id = $1`, cancelTestRun).Scan(&pendingOutbox, &finalizeRows); err != nil {
		t.Fatal(err)
	}
	if pendingOutbox != 0 || finalizeRows != 1 {
		t.Fatalf("outbox pending=%d finalize dispatched=%d, want 0 and 1", pendingOutbox, finalizeRows)
	}
	var jobRunStatus int
	var jobRunCategory string
	if err := pool.QueryRow(ctx, `SELECT status, result->>'error_category' FROM job_runs WHERE result->>'sync_run_id' = $1`,
		cancelTestRun).Scan(&jobRunStatus, &jobRunCategory); err != nil {
		t.Fatal(err)
	}
	if jobRunStatus != jobRunStatusFailed || jobRunCategory != "cancelled" {
		t.Fatalf("job_runs status=%d category=%q, want 3 and cancelled", jobRunStatus, jobRunCategory)
	}
	var backfillStatus string
	var failedChunks int
	if err := pool.QueryRow(ctx, `SELECT status, failed_chunks FROM backfill_jobs WHERE celery_task_id = $1`,
		"sync_run:"+cancelTestRun).Scan(&backfillStatus, &failedChunks); err != nil {
		t.Fatal(err)
	}
	if backfillStatus != "failed" || failedChunks != 5 {
		t.Fatalf("backfill_jobs status=%s failed_chunks=%d, want failed and 5", backfillStatus, failedChunks)
	}
}

// A running unit whose lease is live is refused without --allow-running, and nothing is written.
// With the flag it is failed by its lease owner, and the run closes.
func TestCancelSyncRunRefusesALiveLeaseUnlessAllowed(t *testing.T) {
	ctx, pool := startFeatureDisabledPool(t)
	now := cancelTestNow()
	live := now.Add(time.Hour)
	owner := "worker-live"
	fixture := seedCancelFixture(t, ctx, pool, cancelTestRun, now,
		pgseed.SyncRunUnit{DatasetKey: "g-running-live", Status: "running", LeaseOwner: &owner, LeaseExpiresAt: &live})
	before := cancelSnapshot(t, ctx, pool, cancelTestRun)

	outcome, err := CancelSyncRun(ctx, pool, cancelRequest(cancelTestRun, now))
	if !errors.Is(err, ErrSyncRunHasRunningUnit) || outcome.Units.RunningLeased != 1 {
		t.Fatalf("without the flag: err=%v outcome=%+v, want ErrSyncRunHasRunningUnit with 1 leased unit", err, outcome)
	}
	if after := cancelSnapshot(t, ctx, pool, cancelTestRun); after != before {
		t.Fatalf("a refused cancel wrote:\nbefore %s\nafter  %s", before, after)
	}

	request := cancelRequest(cancelTestRun, now)
	request.AllowRunning = true
	if outcome, err = CancelSyncRun(ctx, pool, request); err != nil || outcome.Outcome != CancelOutcomeCancelled {
		t.Fatalf("with the flag: err=%v outcome=%+v", err, outcome)
	}
	var status string
	var leaseCleared bool
	if err := pool.QueryRow(ctx, `SELECT status, lease_owner IS NULL FROM sync_run_units WHERE id = $1`,
		fixture.units["g-running-live"]).Scan(&status, &leaseCleared); err != nil {
		t.Fatal(err)
	}
	if status != "failed" || !leaseCleared {
		t.Fatalf("live-lease unit status=%s lease cleared=%v, want failed and cleared", status, leaseCleared)
	}
}

// A running unit whose lease expired has no live worker: it is failed without the flag.
func TestCancelSyncRunFailsAnExpiredLeaseWithoutTheFlag(t *testing.T) {
	ctx, pool := startFeatureDisabledPool(t)
	now := cancelTestNow()
	fixture := seedCancelFixture(t, ctx, pool, cancelTestRun, now)
	if outcome, err := CancelSyncRun(ctx, pool, cancelRequest(cancelTestRun, now)); err != nil || outcome.Outcome != CancelOutcomeCancelled {
		t.Fatalf("err=%v outcome=%+v", err, outcome)
	}
	var status string
	if err := pool.QueryRow(ctx, `SELECT status FROM sync_run_units WHERE id = $1`, fixture.units["e-running-expired"]).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "failed" {
		t.Fatalf("expired-lease unit status=%s, want failed", status)
	}
}

// A run that is still open after the unit write (here: a unit in a status the terminator does not
// close) is rolled back whole.
func TestCancelSyncRunRollsBackARunThatStaysOpen(t *testing.T) {
	ctx, pool := startFeatureDisabledPool(t)
	now := cancelTestNow()
	seedCancelFixture(t, ctx, pool, cancelTestRun, now, pgseed.SyncRunUnit{DatasetKey: "h-unmodeled", Status: "some_unmodeled_status"})
	before := cancelSnapshot(t, ctx, pool, cancelTestRun)
	if _, err := CancelSyncRun(ctx, pool, cancelRequest(cancelTestRun, now)); !errors.Is(err, ErrSyncRunNotTerminal) {
		t.Fatalf("err=%v, want ErrSyncRunNotTerminal", err)
	}
	if after := cancelSnapshot(t, ctx, pool, cancelTestRun); after != before {
		t.Fatalf("a rolled-back cancel wrote:\nbefore %s\nafter  %s", before, after)
	}
}

// A second call on the same run reports already_terminal and writes nothing.
func TestCancelSyncRunSecondCallIsAlreadyTerminal(t *testing.T) {
	ctx, pool := startFeatureDisabledPool(t)
	now := cancelTestNow()
	seedCancelFixture(t, ctx, pool, cancelTestRun, now)
	if _, err := CancelSyncRun(ctx, pool, cancelRequest(cancelTestRun, now)); err != nil {
		t.Fatal(err)
	}
	before := cancelSnapshot(t, ctx, pool, cancelTestRun)
	outcome, err := CancelSyncRun(ctx, pool, cancelRequest(cancelTestRun, now.Add(time.Minute)))
	if err != nil || outcome.Outcome != CancelOutcomeAlreadyTerminal || outcome.PriorStatus != "failed" || outcome.UnitsFailed != 0 {
		t.Fatalf("second call: err=%v outcome=%+v, want already_terminal from failed", err, outcome)
	}
	if after := cancelSnapshot(t, ctx, pool, cancelTestRun); after != before {
		t.Fatalf("the second call wrote:\nbefore %s\nafter  %s", before, after)
	}
}

// Another organization's run reads exactly like a run that does not exist, and nothing is written.
func TestCancelSyncRunWrongOrgReadsAsNotFound(t *testing.T) {
	ctx, pool := startFeatureDisabledPool(t)
	now := cancelTestNow()
	seedCancelFixture(t, ctx, pool, cancelTestRun, now)
	before := cancelSnapshot(t, ctx, pool, cancelTestRun)
	wrongOrg := cancelRequest(cancelTestRun, now)
	wrongOrg.OrgID = "00000000-0000-4000-8000-0000000000ff"
	if _, err := CancelSyncRun(ctx, pool, wrongOrg); !errors.Is(err, ErrSyncRunNotFound) {
		t.Fatalf("wrong org: err=%v, want ErrSyncRunNotFound", err)
	}
	if _, err := CancelSyncRun(ctx, pool, cancelRequest(uuid.NewString(), now)); !errors.Is(err, ErrSyncRunNotFound) {
		t.Fatalf("unknown run: err=%v, want ErrSyncRunNotFound", err)
	}
	if after := cancelSnapshot(t, ctx, pool, cancelTestRun); after != before {
		t.Fatalf("a refused cancel wrote:\nbefore %s\nafter  %s", before, after)
	}
}

// --dry-run reports the units and would_cancel and writes nothing; it needs no reason.
func TestCancelSyncRunDryRunWritesNothing(t *testing.T) {
	ctx, pool := startFeatureDisabledPool(t)
	now := cancelTestNow()
	seedCancelFixture(t, ctx, pool, cancelTestRun, now)
	before := cancelSnapshot(t, ctx, pool, cancelTestRun)
	request := cancelRequest(cancelTestRun, now)
	request.DryRun, request.ReasonCode = true, ""
	outcome, err := CancelSyncRun(ctx, pool, request)
	wantUnits := CancelSyncRunUnits{Planned: 1, Retrying: 1, Dispatching: 2, RunningExpired: 1, Success: 1}
	if err != nil || outcome.Outcome != CancelOutcomeWouldCancel || outcome.Units != wantUnits {
		t.Fatalf("dry run: err=%v outcome=%+v", err, outcome)
	}
	if after := cancelSnapshot(t, ctx, pool, cancelTestRun); after != before {
		t.Fatalf("a dry run wrote:\nbefore %s\nafter  %s", before, after)
	}
}

// differentialSnapshot is the closed state of a run without its ids, the category or the message.
func differentialSnapshot(t *testing.T, ctx context.Context, pool *pgxpool.Pool, runID string, message string) string {
	t.Helper()
	var snapshot string
	if err := pool.QueryRow(ctx, `
SELECT json_build_object(
  'run', (SELECT json_build_object('status', status, 'total', total_units, 'completed', completed_units, 'failed', failed_units,
          'completed_at', completed_at, 'error', error, 'result', result) FROM public.sync_runs WHERE id = $1::uuid),
  'units', (SELECT json_agg(json_build_object('label', dataset_key, 'status', status, 'available_at', available_at, 'error', error,
          'result', result, 'lease_owner', lease_owner, 'lease_expires_at', lease_expires_at, 'attempts', attempts)
          ORDER BY dataset_key) FROM public.sync_run_units WHERE sync_run_id = $1::uuid),
  'ledger', (SELECT json_agg(json_build_object('status', status, 'lease_owner', lease_owner, 'lease_expires_at', lease_expires_at,
          'last_heartbeat_at', last_heartbeat_at, 'completed_at', completed_at, 'error', error, 'result', result, 'updated_at', updated_at))
          FROM public.sync_run_reference_discoveries WHERE sync_run_id = $1::uuid),
  'outbox', (SELECT json_agg(json_build_object('kind', kind, 'status', status, 'last_error', last_error, 'dispatched_at', dispatched_at,
          'claim_token', claim_token, 'claim_expires_at', claim_expires_at, 'transport_job_id', transport_job_id,
          'dispatched_transport', dispatched_transport, 'dispatched_route_generation', dispatched_route_generation,
          'attempts', attempts, 'updated_at', updated_at) ORDER BY kind) FROM public.sync_dispatch_outbox WHERE sync_run_id = $1::uuid),
  'backfill', (SELECT json_agg(json_build_object('status', status, 'total', total_chunks, 'completed', completed_chunks,
          'failed', failed_chunks, 'completed_at', completed_at, 'error', error_message)) FROM public.backfill_jobs
          WHERE celery_task_id = 'sync_run:' || $1::text),
  'job_runs', (SELECT json_agg(json_build_object('status', status, 'completed_at', completed_at, 'error', error,
          'result', result::jsonb - 'sync_run_id')) FROM public.job_runs WHERE result->>'sync_run_id' = $1::text),
  'unit_updated_at', (SELECT json_agg(DISTINCT updated_at) FROM public.sync_run_units WHERE sync_run_id = $1::uuid AND status = 'failed')
)::text`, runID).Scan(&snapshot); err != nil {
		t.Fatal(err)
	}
	snapshot = strings.ReplaceAll(snapshot, message, "<message>")
	snapshot = strings.ReplaceAll(snapshot, sanitizeErrorText(message), "<message>")
	errorCategoryValue := regexp.MustCompile(`("error_category"\s*:\s*)"(cancelled|feature_disabled)"`)
	return errorCategoryValue.ReplaceAllString(snapshot, `$1"<category>"`)
}

// The verb's end state equals the feature-disabled terminator's on the same fixture: only the
// category and the message differ.
func TestCancelSyncRunEndStateEqualsTheFeatureDisabledTerminator(t *testing.T) {
	ctx, pool := startFeatureDisabledPool(t)
	now := cancelTestNow()
	seedCancelFixture(t, ctx, pool, cancelTestRun, now)
	seedCancelFixture(t, ctx, pool, cancelTestOtherRun, now)

	if outcome, err := CancelSyncRun(ctx, pool, cancelRequest(cancelTestRun, now)); err != nil || outcome.Outcome != CancelOutcomeCancelled {
		t.Fatalf("CancelSyncRun: err=%v outcome=%+v", err, outcome)
	}
	withTx(t, ctx, pool, func(tx pgx.Tx) {
		run, err := loadFinalizeRun(ctx, tx, featureDisabledTestOrg, cancelTestOtherRun)
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

	verb := differentialSnapshot(t, ctx, pool, cancelTestRun, cancelTestMessage)
	terminator := differentialSnapshot(t, ctx, pool, cancelTestOtherRun, testFeatureDisabledMessage)
	if verb != terminator {
		t.Fatalf("end states differ beyond category and message:\nverb       %s\nterminator %s", verb, terminator)
	}
	if !strings.Contains(verb, `"status" : "failed"`) || !strings.Contains(verb, "<category>") || !strings.Contains(verb, "<message>") {
		t.Fatalf("the snapshot does not hold the closed state it compares: %s", verb)
	}
	// What the normalization hides: each path writes its own category, the terminator still its own.
	for runID, category := range map[string]string{cancelTestRun: "cancelled", cancelTestOtherRun: "feature_disabled"} {
		var run, units, ledger, jobRuns int
		if err := pool.QueryRow(ctx, `
SELECT (SELECT count(*) FROM public.sync_runs WHERE id = $1::uuid AND result::jsonb ->> 'error_category' = $2),
       (SELECT count(*) FROM public.sync_run_units WHERE sync_run_id = $1::uuid AND result::jsonb ->> 'error_category' = $2),
       (SELECT count(*) FROM public.sync_run_reference_discoveries WHERE sync_run_id = $1::uuid AND result::jsonb ->> 'error_category' = $2),
       (SELECT count(*) FROM public.job_runs WHERE result->>'sync_run_id' = $1::text AND result::jsonb ->> 'error_category' = $2)`,
			runID, category).Scan(&run, &units, &ledger, &jobRuns); err != nil {
			t.Fatal(err)
		}
		if run != 1 || units != 5 || ledger != 1 || jobRuns != 1 {
			t.Fatalf("run %s: category %s on run=%d units=%d ledger=%d job_runs=%d, want 1, 5, 1, 1", runID, category, run, units, ledger, jobRuns)
		}
	}
}

// The claim of a cancelled unit is refused and writes nothing. A queued job of that unit therefore
// retries and is discarded by River (TestQueuedJobOfACancelledUnitRetriesThenIsDiscardedWithoutADomainWrite).
func TestCancelSyncRunRefusesTheClaimOfACancelledUnit(t *testing.T) {
	ctx, pool := startFeatureDisabledPool(t)
	now := cancelTestNow()
	fixture := seedCancelFixture(t, ctx, pool, cancelTestRun, now)
	if _, err := CancelSyncRun(ctx, pool, cancelRequest(cancelTestRun, now)); err != nil {
		t.Fatal(err)
	}
	repository, err := providersync.NewPostgresRepository(pool)
	if err != nil {
		t.Fatal(err)
	}
	before := cancelSnapshot(t, ctx, pool, cancelTestRun)
	for _, label := range []string{"c-dispatching", "e-running-expired"} {
		_, err := repository.Claim(ctx, providersync.ClaimRequest{
			UnitID: fixture.units[label], OrgID: featureDisabledTestOrg, Owner: uuid.NewString(),
			Now: now.Add(time.Minute), LeaseDuration: time.Minute, AllowExpiredRecovery: true,
		})
		if !errors.Is(err, providersync.ErrUnitNotClaimable) {
			t.Fatalf("claim of cancelled unit %s: err=%v, want ErrUnitNotClaimable", label, err)
		}
	}
	if after := cancelSnapshot(t, ctx, pool, cancelTestRun); after != before {
		t.Fatalf("a claim of a cancelled unit wrote:\nbefore %s\nafter  %s", before, after)
	}
}
