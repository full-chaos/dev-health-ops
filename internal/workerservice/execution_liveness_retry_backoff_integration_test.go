//go:build integration

package workerservice

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	riverstore "github.com/full-chaos/dev-health-ops/internal/storage/river"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/poolpg"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivermigrate"
)

// CHAOS-6864 (CHAOS-6818 r3 P2): execution_liveness must see a work pool that
// fails every job at its idempotency Begin even though River's own retry
// backoff spaces those failures apart.
//
// A job that fails at Begin is not "running" and not "available" for long: River
// parks it as `retryable` with scheduled_at in the future (5s, 10s, 20s ... up
// to 5m under the registry's bounded_exponential_jitter policy), so between two
// failures the queue reads Available=0 and Running=0. Every arm of judgeQueue
// that reasons from the queue snapshot then calls the queue idle and healthy,
// however many failures preceded it and however long no handler has run.
//
// These tests drive that exact state through a REAL River client, a real
// PostgreSQL river_job row, the production jobruntime.Adapter, the production
// claimLivenessObserver, the real queue-telemetry sampler and the real
// claimLivenessReady. The stale pooler is the poolpg stand-in the CHAOS-6818
// scenarios already use: a real pgxpool whose established connections die.

// backoffIdempotency fails Begin the way PostgresIdempotency.Begin's first line
// does: it needs a transaction from the work pool.
type backoffIdempotency struct {
	pool      *pgxpool.Pool
	beginCall atomic.Int64
	// failFirst, when > 0, fails only the first failFirst calls regardless of the
	// pool (a transient blip); otherwise Begin fails exactly when the pool does.
	failFirst int64
	proceed   backoffClaim
}

func (*backoffIdempotency) Supports(string) bool { return true }

func (store *backoffIdempotency) Begin(ctx context.Context, _ jobruntime.ClaimRequest) (jobruntime.IdempotencyClaim, error) {
	call := store.beginCall.Add(1)
	if store.failFirst > 0 {
		if call <= store.failFirst {
			return nil, errors.New("idempotency unavailable")
		}
		return store.proceed, nil
	}
	tx, err := store.pool.Begin(ctx)
	if err != nil {
		return nil, errors.New("idempotency unavailable")
	}
	_ = tx.Rollback(ctx)
	return store.proceed, nil
}

type backoffClaim struct{}

func (backoffClaim) State() jobruntime.ClaimState                        { return jobruntime.ClaimProceed }
func (backoffClaim) Finish(context.Context, jobruntime.Completion) error { return nil }

type backoffTenantScope struct{}

func (backoffTenantScope) Supports(string) bool { return true }
func (backoffTenantScope) Resolve(ctx context.Context, _ jobruntime.ScopeRequest) (context.Context, error) {
	return ctx, nil
}

type backoffBudget struct{}

func (backoffBudget) Supports(string, int) bool { return true }
func (backoffBudget) Acquire(context.Context, jobruntime.BudgetRequest) (jobruntime.BudgetLease, error) {
	return backoffLease{}, nil
}

type backoffLease struct{}

func (backoffLease) Release() {}

type backoffFixture struct {
	pool      *pgxpool.Pool
	claim     *claimLiveness
	ready     func(context.Context) error
	sampler   *riverstore.QueueTelemetrySampler
	workServe *poolpg.Server
	handled   *atomic.Int64
	insert    func(t *testing.T) int64
	window    time.Duration
}

func newBackoffFixture(t *testing.T, failFirst int64) *backoffFixture {
	t.Helper()
	t.Chdir(filepath.Join("..", ".."))
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	t.Cleanup(cancel)

	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate PostgreSQL: %v", err)
		}
	})
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	if _, err := pool.Exec(ctx, "CREATE SCHEMA river"); err != nil {
		t.Fatal(err)
	}
	migrator, err := rivermigrate.New(riverpgxv5.New(pool), &rivermigrate.Config{Logger: logger, Schema: "river"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := migrator.Migrate(ctx, rivermigrate.DirectionUp, nil); err != nil {
		t.Fatal(err)
	}

	// The work pool: a real pgxpool against a server we can "recreate".
	workServer := poolpg.Start(t)
	poolConfig, err := pgxpool.ParseConfig("postgres://role:secret@" + workServer.Addr() + "/db?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	poolConfig.MaxConns = 2
	workPool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(workPool.Close)

	runtimeRegistry, err := jobruntime.Load(defaultContractRoot)
	if err != nil {
		t.Fatal(err)
	}
	spec, ok := runtimeRegistry.Descriptor(jobcontract.KindRetentionCleanup)
	if !ok || !spec.Executable() {
		t.Fatalf("the retention kind is not executable in the checked-in registry: %v", ok)
	}
	const queue = "retention"

	window := 2 * time.Second
	claim := newClaimLiveness(time.Now(), []string{queue})
	claim.SetStaleWindow(window)
	claim.markRuntimeLive()
	collector, err := jobruntime.NewMetricsCollector(jobruntime.MetricDimensions{
		Jobs: []jobruntime.JobLabels{{Queue: queue, Kind: jobcontract.KindRetentionCleanup}},
	})
	if err != nil {
		t.Fatal(err)
	}
	observer := claimLivenessObserver{MetricsCollector: collector, liveness: claim}

	var handled atomic.Int64
	adapter, err := jobruntime.NewAdapter[jobruntime.RetentionCleanupArgs](
		runtimeRegistry, spec,
		jobruntime.HandlerFunc[jobruntime.RetentionCleanupArgs](func(context.Context, *jobruntime.Execution[jobruntime.RetentionCleanupArgs]) error {
			handled.Add(1)
			return nil
		}),
		jobruntime.Dependencies{
			Logger: logger, Observer: observer,
			TenantScope: backoffTenantScope{}, Budget: backoffBudget{},
			Idempotency: &backoffIdempotency{pool: workPool, failFirst: failFirst},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	workers := river.NewWorkers()
	if err := river.AddWorkerSafely(workers, adapter); err != nil {
		t.Fatal(err)
	}
	const clientID = "backoff-repro"
	client, err := river.NewClient(riverpgxv5.New(pool), &river.Config{
		FetchCooldown:     100 * time.Millisecond,
		FetchPollInterval: 100 * time.Millisecond,
		ID:                clientID,
		Logger:            logger,
		Queues:            map[string]river.QueueConfig{queue: {MaxWorkers: 1}},
		Schema:            "river",
		TestOnly:          true,
		Workers:           workers,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer stopCancel()
		if err := client.StopAndCancel(stopCtx); err != nil {
			t.Errorf("stop River client: %v", err)
		}
	})

	sampler, err := riverstore.NewQueueTelemetrySampler(pool, riverstore.QueueTelemetryConfig{
		Schema:   "river",
		ClientID: clientID,
		Queues:   []riverstore.QueueTelemetryQueue{{Name: queue, MaxWorkers: 1}},
		Jobs: []riverstore.QueueTelemetryJob{{
			Queue: queue, Kind: jobcontract.KindRetentionCleanup, SupportedVersions: []int{jobcontract.ContractVersionV1},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	dependencies := &workerDependencies{queueTelemetryRequired: true, queueTelemetry: sampler}

	return &backoffFixture{
		pool: pool, claim: claim, sampler: sampler, workServe: workServer, handled: &handled,
		ready:  dependencies.claimLivenessReady(claim),
		window: window,
		insert: func(t *testing.T) int64 {
			t.Helper()
			args := retentionRiverArgs(t)
			inserted, err := client.Insert(ctx, args, &river.InsertOpts{MaxAttempts: spec.MaxAttempts, Priority: spec.Priority, Queue: queue})
			if err != nil {
				t.Fatal(err)
			}
			return inserted.Job.ID
		},
	}
}

func retentionRiverArgs(t *testing.T) jobruntime.RetentionCleanupArgs {
	t.Helper()
	return jobruntime.RetentionCleanupArgs{EnvelopeArgs: jobruntime.EnvelopeArgs[jobcontract.RetentionCleanupPayload]{
		ContractVersion: jobcontract.ContractVersionV1,
		CorrelationID:   "corr-backoff-1",
		IdempotencyKey:  "retention:backoff-" + time.Now().Format("150405.000000"),
		Domain: jobcontract.DomainLink{
			Type: "maintenance_run", ID: "11111111-1111-4111-8111-111111111111",
		},
		Payload: jobcontract.RetentionCleanupPayload{
			BatchSize: 100, DeleteBefore: "2026-07-01T00:00:00Z",
			RetentionPolicy: jobcontract.RetentionWorkerTerminal,
		},
	}}
}

// jobState reads the row River itself keeps for a job.
func (f *backoffFixture) jobState(t *testing.T, id int64) (state string, attempt int) {
	t.Helper()
	if err := f.pool.QueryRow(context.Background(),
		`SELECT state::text, attempt FROM river.river_job WHERE id = $1`, id).Scan(&state, &attempt); err != nil {
		t.Fatal(err)
	}
	return state, attempt
}

// queueReads returns the queue as the production readiness poll sees it.
func (f *backoffFixture) queueReads(t *testing.T) (available, running int64) {
	t.Helper()
	snapshot, err := f.sampler.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, job := range snapshot.Jobs {
		available += job.Available
	}
	for _, capacity := range snapshot.QueueCapacities {
		running += capacity.Running
	}
	return available, running
}

// TestExecutionLivenessSeesIdempotencyFailuresSpacedByRiverRetryBackoff is the
// executed repro. A job fails its idempotency Begin on a recreated pooler; River
// retries it on its own backoff. During the gaps the row is `retryable`, the queue
// reads Available=0/Running=0, and after more than the window with several
// failures and no handler ever run, execution_liveness must be red.
func TestExecutionLivenessSeesIdempotencyFailuresSpacedByRiverRetryBackoff(t *testing.T) {
	f := newBackoffFixture(t, 0)
	f.workServe.DropConnections(true) // the pooler is recreated: Begin fails from now on
	id := f.insert(t)
	started := time.Now()

	deadline := time.Now().Add(45 * time.Second)
	var blindPolls int
	for {
		state, attempt := f.jobState(t, id)
		available, running := f.queueReads(t)
		readyErr := f.ready(context.Background())
		blind := state == "retryable" && attempt >= 2 && available == 0 && running == 0 &&
			time.Since(started) > f.window
		if blind {
			blindPolls++
			if readyErr == nil {
				t.Fatalf("execution_liveness is green in a retry-backoff gap: job state=%s attempt=%d (>=2 idempotency failures), "+
					"queue Available=%d Running=%d, %s since the first failure with no handler run (window %s)",
					state, attempt, available, running, time.Since(started).Round(time.Millisecond), f.window)
			}
			break
		}
		if state == "cancelled" || state == "discarded" || state == "completed" || time.Now().After(deadline) {
			t.Fatalf("the retry-backoff gap was never observed (blindPolls=%d state=%s attempt=%d after %s): the repro state is absent",
				blindPolls, state, attempt, time.Since(started).Round(time.Millisecond))
		}
		time.Sleep(100 * time.Millisecond)
	}
	if got := f.handled.Load(); got != 0 {
		t.Fatalf("a handler ran %d times against a dead pooler", got)
	}

	// Recovery: the pooler is back and a job reaches its handler, so the queue is
	// ready again with no restart.
	f.workServe.DropConnections(false)
	f.insert(t)
	recovered := time.Now().Add(20 * time.Second)
	for f.handled.Load() == 0 {
		if time.Now().After(recovered) {
			t.Fatal("no job reached its handler after the pooler recovered")
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err := f.ready(context.Background()); err != nil {
		t.Fatalf("execution_liveness did not recover once a job reached its handler: %v", err)
	}
}

// TestExecutionLivenessIgnoresATransientIdempotencyFailureThatRecovers is the
// negative control on the same live path: one Begin failure, River's backoff,
// then a normal run. Readiness must stay green through the whole sequence.
func TestExecutionLivenessIgnoresATransientIdempotencyFailureThatRecovers(t *testing.T) {
	f := newBackoffFixture(t, 1)
	id := f.insert(t)
	deadline := time.Now().Add(45 * time.Second)
	var sawRetryable bool
	for {
		state, attempt := f.jobState(t, id)
		if state == "retryable" {
			sawRetryable = true
		}
		if err := f.ready(context.Background()); err != nil {
			t.Fatalf("a single transient Begin failure turned readiness red (state=%s attempt=%d): %v", state, attempt, err)
		}
		if state == "completed" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the job never completed (state=%s attempt=%d)", state, attempt)
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !sawRetryable {
		t.Fatal("the transient failure never parked the job as retryable: the control ran the wrong state")
	}
}
