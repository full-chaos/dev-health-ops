//go:build integration

package syncdispatchruntime

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/jobs/providerunit"
	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"
)

const (
	cancelledQueuedOrg  = "00000000-0000-4000-8000-000000143850"
	cancelledQueuedRun  = "00000000-0000-4000-8000-000000143851"
	cancelledQueuedUnit = "00000000-0000-4000-8000-000000143852"
)

// resultObserver records the River result of every finished attempt.
type resultObserver struct {
	mu      sync.Mutex
	results []jobruntime.Result
}

func (*resultObserver) RuntimeRegistered(context.Context, jobruntime.RuntimeInfo) {}
func (*resultObserver) JobStarted(context.Context, jobruntime.JobLabels)          {}
func (observer *resultObserver) JobFinished(_ context.Context, _ jobruntime.JobLabels, result jobruntime.Result, _ jobruntime.ErrorCategory, _ time.Duration) {
	observer.mu.Lock()
	defer observer.mu.Unlock()
	observer.results = append(observer.results, result)
}
func (*resultObserver) JobPanicked(context.Context, jobruntime.JobLabels) {}
func (*resultObserver) JobCancelled(context.Context, jobruntime.JobLabels, jobruntime.ErrorCategory) {
}
func (*resultObserver) DomainMismatch(context.Context, string)                                  {}
func (*resultObserver) BudgetWait(context.Context, jobruntime.JobLabels, time.Duration, string) {}
func (*resultObserver) JobWait(context.Context, jobruntime.JobLabels, time.Duration)            {}
func (*resultObserver) ObserveDeterministicFailure(context.Context, jobruntime.JobLabels, jobruntime.Reason) {
}

type cancelledQueuedTenantScope struct{}

func (cancelledQueuedTenantScope) Supports(string) bool { return true }
func (cancelledQueuedTenantScope) Resolve(ctx context.Context, _ jobruntime.ScopeRequest) (context.Context, error) {
	return ctx, nil
}

type cancelledQueuedBudget struct{}

func (cancelledQueuedBudget) Supports(string, int) bool { return true }
func (cancelledQueuedBudget) Acquire(context.Context, jobruntime.BudgetRequest) (jobruntime.BudgetLease, error) {
	return cancelledQueuedLease{}, nil
}

type cancelledQueuedLease struct{}

func (cancelledQueuedLease) Release() {}

func cancelledQueuedSnapshot(t *testing.T, ctx context.Context, pool *pgxpool.Pool) string {
	t.Helper()
	var snapshot string
	if err := pool.QueryRow(ctx, `
SELECT json_build_object(
  'run', (SELECT row_to_json(r) FROM public.sync_runs AS r WHERE r.id = $1::uuid),
  'units', (SELECT json_agg(u ORDER BY u.id) FROM public.sync_run_units AS u WHERE u.sync_run_id = $1::uuid),
  'outbox', (SELECT json_agg(o ORDER BY o.kind) FROM public.sync_dispatch_outbox AS o WHERE o.sync_run_id = $1::uuid)
)::text`, cancelledQueuedRun).Scan(&snapshot); err != nil {
		t.Fatal(err)
	}
	return snapshot
}

// A provider-unit job still queued for a unit that `dho workers sync-runs cancel` closed: on each of
// its remaining attempts the claim refuses the failed unit and the adapter reports retry; on the
// last attempt it reports discard, so River discards the job. No attempt changes a domain row.
func TestQueuedJobOfACancelledUnitRetriesThenIsDiscardedWithoutADomainWrite(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	instance := pgschema.Start(ctx, t)
	defer instance.Close(context.Background())
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	pgseed.EnsureSyncRun(ctx, t, pool, pgseed.SyncRun{ID: cancelledQueuedRun, OrgID: cancelledQueuedOrg, Status: "dispatching", TotalUnits: 1})
	pgseed.InsertSyncRunUnit(ctx, t, pool, pgseed.SyncRunUnit{
		ID: cancelledQueuedUnit, RunID: cancelledQueuedRun, OrgID: cancelledQueuedOrg, Status: "dispatching", DatasetKey: "commits",
	})
	now := time.Now().UTC()
	outcome, err := CancelSyncRun(ctx, pool, CancelSyncRunRequest{
		OrgID: cancelledQueuedOrg, SyncRunID: cancelledQueuedRun, ReasonCode: "operator_test", Now: now,
	})
	if err != nil || outcome.Outcome != CancelOutcomeCancelled {
		t.Fatalf("CancelSyncRun: err=%v outcome=%+v", err, outcome)
	}
	before := cancelledQueuedSnapshot(t, ctx, pool)

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
	handler := &providerunit.Handler{
		Repository: repository, LeaseDuration: time.Minute, Heartbeat: 10 * time.Second,
		Now: func() time.Time { return now.Add(time.Minute) },
		BuildExecutor: func(*providersync.LeaseSession) (providersync.CompleteRouteExecutor, error) {
			t.Error("a cancelled unit reached the provider executor")
			return providersync.CompleteRouteExecutor{}, providerunit.ErrUnavailable
		},
	}
	org := cancelledQueuedOrg
	args := jobruntime.ProviderUnitArgs{EnvelopeArgs: jobruntime.EnvelopeArgs[jobcontract.ProviderUnitPayload]{
		ContractVersion: jobcontract.ContractVersionV1, OrganizationID: &org,
		CorrelationID: "sync-run:" + cancelledQueuedRun, IdempotencyKey: "sync.provider_unit:" + cancelledQueuedUnit,
		Domain:  jobcontract.DomainLink{Type: "sync_run_unit", ID: cancelledQueuedUnit},
		Payload: jobcontract.ProviderUnitPayload{UnitID: cancelledQueuedUnit},
	}}
	raw, err := jobcontract.MarshalCanonical(args.ContractEnvelope())
	if err != nil {
		t.Fatal(err)
	}

	// The queued state decides the job's next attempt: available and pending jobs have not run, a
	// retryable or scheduled job has run before.
	for _, queued := range []struct {
		state        string
		firstAttempt int
	}{
		{"available", 1}, {"pending", 1}, {"retryable", 2}, {"scheduled", 3},
	} {
		t.Run(queued.state, func(t *testing.T) {
			observer := &resultObserver{}
			adapter, err := jobruntime.NewAdapter[jobruntime.ProviderUnitArgs](registry, spec, handler, jobruntime.Dependencies{
				Logger: logging.NewJSON(&strings.Builder{}, slog.LevelDebug), Observer: observer,
				TenantScope: cancelledQueuedTenantScope{}, Budget: cancelledQueuedBudget{}, Idempotency: providerunit.AuthoritativeIdempotency{},
			})
			if err != nil {
				t.Fatal(err)
			}
			for attempt := queued.firstAttempt; attempt <= spec.MaxAttempts; attempt++ {
				job := &river.Job[jobruntime.ProviderUnitArgs]{
					JobRow: &rivertype.JobRow{
						ID: 1438500 + int64(attempt), Attempt: attempt, CreatedAt: now, EncodedArgs: raw,
						Kind: spec.Kind, MaxAttempts: spec.MaxAttempts, Priority: spec.Priority, Queue: spec.Queue,
						ScheduledAt: now, State: rivertype.JobStateRunning,
					},
					Args: args,
				}
				if err := adapter.Work(ctx, job); err == nil {
					t.Fatalf("attempt %d of a cancelled unit's job returned success", attempt)
				}
			}
			observer.mu.Lock()
			results := append([]jobruntime.Result(nil), observer.results...)
			observer.mu.Unlock()
			if want := spec.MaxAttempts - queued.firstAttempt + 1; len(results) != want {
				t.Fatalf("results %v: %d attempts, want %d", results, len(results), want)
			}
			for index, result := range results {
				want := jobruntime.ResultRetry
				if index == len(results)-1 {
					want = jobruntime.ResultDiscard
				}
				if result != want {
					t.Fatalf("attempt %d result %s, want %s (results %v)", queued.firstAttempt+index, result, want, results)
				}
			}
			if after := cancelledQueuedSnapshot(t, ctx, pool); after != before {
				t.Fatalf("a queued job of a cancelled unit changed domain rows:\nbefore %s\nafter  %s", before, after)
			}
		})
	}
}
