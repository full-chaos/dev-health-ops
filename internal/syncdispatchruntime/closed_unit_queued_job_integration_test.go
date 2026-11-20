//go:build integration

package syncdispatchruntime

import (
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/jobs/providerunit"
	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/domainroledb"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"
)

const (
	closedUnitOrg      = "00000000-0000-4000-8000-000000144000"
	closedUnitOtherOrg = "00000000-0000-4000-8000-000000144099"
)

// closedUnitRig runs one provider-unit job attempt through the real handler, River adapter and
// Postgres repository, as a worker does.
type closedUnitRig struct {
	t        *testing.T
	pool     *pgxpool.Pool
	registry *jobruntime.Registry
	spec     jobruntime.Descriptor
	handler  *providerunit.Handler
	now      time.Time
}

type closedUnitAttempt struct {
	err     error
	results []jobruntime.Result
	logs    string
}

func newClosedUnitRig(t *testing.T, pool *pgxpool.Pool, now time.Time) closedUnitRig {
	t.Helper()
	registry, err := jobruntime.Load("../../contracts/jobs/v1")
	if err != nil {
		t.Fatal(err)
	}
	spec, ok := registry.Descriptor(jobcontract.KindSyncProviderUnit)
	if !ok || spec.MaxAttempts != 5 {
		t.Fatalf("descriptor ok=%v max_attempts=%d, want the checked-in 5", ok, spec.MaxAttempts)
	}
	repository, err := providersync.NewPostgresRepository(pool)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(repository.Close)
	handler := &providerunit.Handler{
		Repository: repository, LeaseDuration: time.Minute, Heartbeat: 10 * time.Second,
		Now: func() time.Time { return now.Add(time.Minute) },
		BuildExecutor: func(*providersync.LeaseSession) (providersync.CompleteRouteExecutor, error) {
			t.Error("a unit that is not claimable reached the provider executor")
			return providersync.CompleteRouteExecutor{}, providerunit.ErrUnavailable
		},
	}
	return closedUnitRig{t: t, pool: pool, registry: registry, spec: spec, handler: handler, now: now}
}

func (rig closedUnitRig) attempt(ctx context.Context, org, unitID, runID string, attempt int) closedUnitAttempt {
	rig.t.Helper()
	args := jobruntime.ProviderUnitArgs{EnvelopeArgs: jobruntime.EnvelopeArgs[jobcontract.ProviderUnitPayload]{
		ContractVersion: jobcontract.ContractVersionV1, OrganizationID: &org,
		CorrelationID: "sync-run:" + runID, IdempotencyKey: "sync.provider_unit:" + unitID,
		Domain:  jobcontract.DomainLink{Type: "sync_run_unit", ID: unitID},
		Payload: jobcontract.ProviderUnitPayload{UnitID: unitID},
	}}
	raw, err := jobcontract.MarshalCanonical(args.ContractEnvelope())
	if err != nil {
		rig.t.Fatal(err)
	}
	observer := &resultObserver{}
	logs := &strings.Builder{}
	adapter, err := jobruntime.NewAdapter[jobruntime.ProviderUnitArgs](rig.registry, rig.spec, rig.handler, jobruntime.Dependencies{
		Logger: logging.NewJSON(logs, slog.LevelDebug), Observer: observer,
		TenantScope: cancelledQueuedTenantScope{}, Budget: cancelledQueuedBudget{}, Idempotency: providerunit.AuthoritativeIdempotency{},
	})
	if err != nil {
		rig.t.Fatal(err)
	}
	job := &river.Job[jobruntime.ProviderUnitArgs]{
		JobRow: &rivertype.JobRow{
			ID: 1440000 + int64(attempt), Attempt: attempt, CreatedAt: rig.now, EncodedArgs: raw,
			Kind: rig.spec.Kind, MaxAttempts: rig.spec.MaxAttempts, Priority: rig.spec.Priority, Queue: rig.spec.Queue,
			ScheduledAt: rig.now, State: rivertype.JobStateRunning,
		},
		Args: args,
	}
	workErr := adapter.Work(ctx, job)
	observer.mu.Lock()
	defer observer.mu.Unlock()
	return closedUnitAttempt{err: workErr, results: append([]jobruntime.Result(nil), observer.results...), logs: logs.String()}
}

func closedUnitSnapshot(t *testing.T, ctx context.Context, pool *pgxpool.Pool, runID string) string {
	t.Helper()
	var snapshot string
	if err := pool.QueryRow(ctx, `
SELECT json_build_object(
  'run', (SELECT row_to_json(r) FROM public.sync_runs AS r WHERE r.id = $1::uuid),
  'units', (SELECT json_agg(u ORDER BY u.id) FROM public.sync_run_units AS u WHERE u.sync_run_id = $1::uuid),
  'outbox', (SELECT json_agg(o ORDER BY o.kind) FROM public.sync_dispatch_outbox AS o WHERE o.sync_run_id = $1::uuid)
)::text`, runID).Scan(&snapshot); err != nil {
		t.Fatal(err)
	}
	return snapshot
}

// finishedLines returns the sync_provider_unit_finished records of one attempt's log.
func finishedLines(t *testing.T, logs string) []map[string]any {
	t.Helper()
	var records []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(logs), "\n") {
		var record map[string]any
		if json.Unmarshal([]byte(line), &record) == nil && record["msg"] == "sync_provider_unit_finished" {
			records = append(records, record)
		}
	}
	return records
}

// requireCompletedWithoutWork: one attempt, nil error, result success, no domain row changed, and
// one Info line that names the unit and why it completed without work.
func requireCompletedWithoutWork(t *testing.T, got closedUnitAttempt, before, after, unitID, runID string) {
	t.Helper()
	if got.err != nil || len(got.results) != 1 || got.results[0] != jobruntime.ResultSuccess {
		t.Errorf("err=%v results=%v, want nil error and [success]: the job completes on this attempt", got.err, got.results)
	}
	if after != before {
		t.Errorf("domain rows changed:\nbefore %s\nafter  %s", before, after)
	}
	lines := finishedLines(t, got.logs)
	if len(lines) != 1 {
		t.Fatalf("sync_provider_unit_finished lines=%d want 1; logs:\n%s", len(lines), got.logs)
	}
	for key, want := range map[string]any{
		"level": "INFO", "result": "unit_closed", "closed_by": "cancelled",
		"sync_unit_id": unitID, "sync_run_id": runID, "provider": "github", "dataset": "commits",
	} {
		if lines[0][key] != want {
			t.Errorf("finished line [%q]=%#v want %#v; line=%#v", key, lines[0][key], want, lines[0])
		}
	}
	if _, present := lines[0]["error_detail"]; present {
		t.Errorf("finished line carries error_detail: %#v", lines[0])
	}
}

// requireRetriedWithoutWork: today's behavior for a unit that is not closed by a cancel.
func requireRetriedWithoutWork(t *testing.T, got closedUnitAttempt, before, after string) {
	t.Helper()
	if got.err == nil || len(got.results) != 1 || got.results[0] != jobruntime.ResultRetry {
		t.Errorf("err=%v results=%v, want an error and [retry]", got.err, got.results)
	}
	if after != before {
		t.Errorf("domain rows changed:\nbefore %s\nafter  %s", before, after)
	}
	for _, line := range finishedLines(t, got.logs) {
		if line["result"] == "unit_closed" {
			t.Errorf("a unit that is not closed by a cancel logged unit_closed: %#v", line)
		}
	}
}

// A provider-unit job still queued for a unit that `dho workers sync-runs cancel` closed completes
// on its first attempt in every queued state, changes no domain row and logs why at Info.
func TestQueuedJobOfACancelledUnitCompletesOnItsFirstAttemptWithoutADomainWrite(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	instance := pgschema.Start(ctx, t)
	defer instance.Close(context.Background())
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	const runID, unitID = "00000000-0000-4000-8000-000000144001", "00000000-0000-4000-8000-000000144002"
	pgseed.EnsureSyncRun(ctx, t, pool, pgseed.SyncRun{ID: runID, OrgID: closedUnitOrg, Status: "dispatching", TotalUnits: 1})
	pgseed.InsertSyncRunUnit(ctx, t, pool, pgseed.SyncRunUnit{ID: unitID, RunID: runID, OrgID: closedUnitOrg, Status: "dispatching", DatasetKey: "commits"})
	now := time.Now().UTC()
	outcome, err := CancelSyncRun(ctx, pool, CancelSyncRunRequest{OrgID: closedUnitOrg, SyncRunID: runID, ReasonCode: "operator_test", Now: now})
	if err != nil || outcome.Outcome != CancelOutcomeCancelled {
		t.Fatalf("CancelSyncRun: err=%v outcome=%+v", err, outcome)
	}
	rig := newClosedUnitRig(t, pool, now)
	before := closedUnitSnapshot(t, ctx, pool, runID)
	// The queued state decides the job's next attempt: available and pending jobs have not run, a
	// retryable or scheduled job has run before.
	for _, queued := range []struct {
		state   string
		attempt int
	}{
		{"available", 1}, {"pending", 1}, {"retryable", 2}, {"scheduled", 3},
	} {
		t.Run(queued.state, func(t *testing.T) {
			got := rig.attempt(ctx, closedUnitOrg, unitID, runID, queued.attempt)
			requireCompletedWithoutWork(t, got, before, closedUnitSnapshot(t, ctx, pool, runID), unitID, runID)
		})
	}
}

// The guarded SQL cancel forms write the same unit and run state as the verb: unit failed with
// result {"error_category": "cancelled"}, run failed. A queued job of such a unit completes too.
func TestQueuedJobOfAUnitCancelledByTheSQLFormCompletesOnItsFirstAttempt(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	instance := pgschema.Start(ctx, t)
	defer instance.Close(context.Background())
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	const runID, unitID = "00000000-0000-4000-8000-000000144011", "00000000-0000-4000-8000-000000144012"
	pgseed.EnsureSyncRun(ctx, t, pool, pgseed.SyncRun{
		ID: runID, OrgID: closedUnitOrg, Status: "failed", TotalUnits: 1, FailedUnits: 1,
		ResultJSON: `{"error_category": "cancelled"}`, Error: "cancelled: scheduled run cancelled by the operator",
	})
	pgseed.InsertSyncRunUnit(ctx, t, pool, pgseed.SyncRunUnit{
		ID: unitID, RunID: runID, OrgID: closedUnitOrg, Status: "failed", DatasetKey: "commits",
		ResultJSON: `{"error_category": "cancelled"}`,
	})
	now := time.Now().UTC()
	rig := newClosedUnitRig(t, pool, now)
	before := closedUnitSnapshot(t, ctx, pool, runID)
	got := rig.attempt(ctx, closedUnitOrg, unitID, runID, 1)
	requireCompletedWithoutWork(t, got, before, closedUnitSnapshot(t, ctx, pool, runID), unitID, runID)
}

// Every unit that is not closed by a cancel in a terminal run keeps today's retry, one row each.
func TestQueuedJobOfAUnitNotClosedByACancelStillRetries(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	instance := pgschema.Start(ctx, t)
	defer instance.Close(context.Background())
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	now := time.Now().UTC()
	future := now.Add(time.Hour)
	owner := "00000000-0000-4000-8000-0000001440ff"
	rig := newClosedUnitRig(t, pool, now)
	for index, row := range []struct {
		name       string
		runStatus  string
		runResult  string
		unit       pgseed.SyncRunUnit
		jobOrg     string
		cancelRun  bool
		siblingRun bool
	}{
		// A unit the orphaned-unit repair or a stale reclaim handed to a new delivery: that
		// delivery's worker holds it under a live lease in the open run.
		{name: "reclaimed unit under a live lease", runStatus: "running",
			unit: pgseed.SyncRunUnit{Status: "running", LeaseOwner: &owner, LeaseExpiresAt: &future}},
		{name: "dispatching unit with a future available_at", runStatus: "dispatching",
			unit: pgseed.SyncRunUnit{Status: "dispatching", AvailableAt: &future}},
		{name: "planned unit", runStatus: "dispatching", unit: pgseed.SyncRunUnit{Status: "planned"}},
		{name: "retrying unit", runStatus: "dispatching", unit: pgseed.SyncRunUnit{Status: "retrying", AvailableAt: &future}},
		{name: "late duplicate of a success unit", runStatus: "success",
			unit: pgseed.SyncRunUnit{Status: "success", ResultJSON: `{}`}},
		{name: "unit failed with another category", runStatus: "failed", runResult: `{"error_category": "provider_unit_exhausted"}`,
			unit: pgseed.SyncRunUnit{Status: "failed", ResultJSON: `{"error_category": "provider_unit_exhausted"}`}},
		{name: "unit failed by the feature gate", runStatus: "failed", runResult: `{"error_category": "feature_disabled"}`,
			unit: pgseed.SyncRunUnit{Status: "failed", ResultJSON: `{"error_category": "feature_disabled"}`}},
		// The cancel category on a unit whose run is not terminal: a live sibling keeps it open.
		{name: "cancelled unit in an open run", runStatus: "running", siblingRun: true,
			unit: pgseed.SyncRunUnit{Status: "failed", ResultJSON: `{"error_category": "cancelled"}`}},
		{name: "cancelled unit, job of another organization", runStatus: "dispatching", cancelRun: true,
			jobOrg: closedUnitOtherOrg, unit: pgseed.SyncRunUnit{Status: "dispatching"}},
	} {
		t.Run(row.name, func(t *testing.T) {
			runID := "00000000-0000-4000-8000-0000001441" + string(rune('0'+index/10)) + string(rune('0'+index%10))
			unitID := "00000000-0000-4000-8000-0000001442" + string(rune('0'+index/10)) + string(rune('0'+index%10))
			pgseed.EnsureSyncRun(ctx, t, pool, pgseed.SyncRun{ID: runID, OrgID: closedUnitOrg, Status: row.runStatus, TotalUnits: 1, ResultJSON: row.runResult})
			unit := row.unit
			unit.ID, unit.RunID, unit.OrgID, unit.DatasetKey = unitID, runID, closedUnitOrg, "commits"
			pgseed.InsertSyncRunUnit(ctx, t, pool, unit)
			if row.siblingRun {
				siblingID := "00000000-0000-4000-8000-0000001443" + string(rune('0'+index/10)) + string(rune('0'+index%10))
				pgseed.InsertSyncRunUnit(ctx, t, pool, pgseed.SyncRunUnit{
					ID: siblingID, RunID: runID, OrgID: closedUnitOrg, Status: "running", DatasetKey: "prs",
					LeaseOwner: &owner, LeaseExpiresAt: &future,
				})
			}
			if row.cancelRun {
				outcome, err := CancelSyncRun(ctx, pool, CancelSyncRunRequest{OrgID: closedUnitOrg, SyncRunID: runID, ReasonCode: "operator_test", Now: now})
				if err != nil || outcome.Outcome != CancelOutcomeCancelled {
					t.Fatalf("CancelSyncRun: err=%v outcome=%+v", err, outcome)
				}
			}
			jobOrg := row.jobOrg
			if jobOrg == "" {
				jobOrg = closedUnitOrg
			}
			before := closedUnitSnapshot(t, ctx, pool, runID)
			got := rig.attempt(ctx, jobOrg, unitID, runID, 1)
			requireRetriedWithoutWork(t, got, before, closedUnitSnapshot(t, ctx, pool, runID))
		})
	}
}

// The claim and its classification run as the restricted domain role (go-river-migrate grants, no
// superuser): no statement is refused, and the queued job of a cancelled unit completes.
func TestQueuedJobOfACancelledUnitCompletesAsTheDomainRole(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	venue := domainroledb.Start(t, ctx, "closed_unit")
	const runID, unitID = "00000000-0000-4000-8000-000000144021", "00000000-0000-4000-8000-000000144022"
	pgseed.EnsureSyncRun(ctx, t, venue.Admin, pgseed.SyncRun{ID: runID, OrgID: closedUnitOrg, Status: "dispatching", TotalUnits: 1})
	pgseed.InsertSyncRunUnit(ctx, t, venue.Admin, pgseed.SyncRunUnit{ID: unitID, RunID: runID, OrgID: closedUnitOrg, Status: "dispatching", DatasetKey: "commits"})
	now := time.Now().UTC()
	outcome, err := CancelSyncRun(ctx, venue.Admin, CancelSyncRunRequest{OrgID: closedUnitOrg, SyncRunID: runID, ReasonCode: "operator_test", Now: now})
	if err != nil || outcome.Outcome != CancelOutcomeCancelled {
		t.Fatalf("CancelSyncRun: err=%v outcome=%+v", err, outcome)
	}
	rig := newClosedUnitRig(t, venue.Domain, now)
	before := closedUnitSnapshot(t, ctx, venue.Admin, runID)
	got := rig.attempt(ctx, closedUnitOrg, unitID, runID, 1)
	requireCompletedWithoutWork(t, got, before, closedUnitSnapshot(t, ctx, venue.Admin, runID), unitID, runID)
	if refused := venue.Denials.All(); len(refused) > 0 {
		t.Fatalf("the domain role was refused: %s", strings.Join(refused, "; "))
	}
}
