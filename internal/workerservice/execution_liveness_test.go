package workerservice

import (
	"context"
	"errors"
	"log/slog"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	riverstore "github.com/full-chaos/dev-health-ops/internal/storage/river"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/poolpg"
)

// TestDomainTransactionCatchesAWedgedReadinessPoolAfterAdmission is what remains
// of CHAOS-4029's "the pool moved under a live process" reproduction after
// CHAOS-6818: the synchronous BEGIN/rollback check (domain_transaction) runs on
// the dedicated READINESS pool, so a wedged readiness pool flips a running
// process to NOT ready with no restart and heals the moment it recovers, while
// execution_liveness now rests on real WORK evidence alone (claim liveness) and
// is NOT moved by that probe: the ticking DB self-probe on the work pool is gone.
func TestDomainTransactionCatchesAWedgedReadinessPoolAfterAdmission(t *testing.T) {
	t.Chdir(filepath.Join("..", ".."))
	queues := []string{"coverage", "heartbeat", "retention", "webhooks"}
	runtimeRegistry, err := jobruntime.Load(defaultContractRoot)
	if err != nil {
		t.Fatal(err)
	}
	database := &fakeWorkerDatabase{}
	sources := productionWorkerDependencySources
	sources.openDatabase = func(context.Context, config.Config) (workerDatabase, error) {
		return database, nil
	}
	sources.buildOperational = fakeHandlerBuilder(
		"operational",
		mustSelectedQueueSpecs(t, runtimeRegistry, queues...),
		selectedQueueBudgets(queues, queues, map[string]int{
			"coverage": 1, "heartbeat": 1, "retention": 1, "webhooks": 4,
		})...,
	)
	sources.buildRiverProcess = fakeRiverProcessBuilder("river-worker")

	registry := health.NewRegistry(2 * time.Second)
	components, err := configureWorkerDependenciesWithSources(
		context.Background(),
		config.Config{
			Queues:                 queues,
			WorkerQueueConcurrency: map[string]int{"coverage": 1, "heartbeat": 1, "retention": 1, "webhooks": 4},
			RiverDatabaseSchema:    "river",
			DomainDatabaseMaxConns: 4,
			QueueDatabaseMaxConns:  2,
		},
		registry,
		sources,
	)
	if err != nil {
		t.Fatalf("configureWorkerDependenciesWithSources() error = %v", err)
	}
	for _, component := range components {
		if component.Name() == "river-workers" {
			continue
		}
		if err := component.Start(context.Background()); err != nil {
			t.Fatalf("start %s: %v", component.Name(), err)
		}
		if strings.Contains(component.Name(), "self-probe") {
			t.Fatalf("component %q: the ticking self-probe on the work pool must not exist any more", component.Name())
		}
	}
	if err := (health.Gate{Registry: registry}).Start(context.Background()); err != nil {
		t.Fatalf("open readiness gate: %v", err)
	}

	// Admission: healthy and ready.
	status := registry.Readiness(context.Background())
	if !status.Ready {
		t.Fatalf("expected the freshly admitted worker to be ready, got %#v", status)
	}

	// The readiness pool wedges AFTER admission, never restarted.
	database.setTxOpenerErr(errors.New("dependency_unavailable"))
	status = registry.Readiness(context.Background())
	if status.Ready {
		t.Fatal("expected readiness to fail once the readiness pool wedges, got Ready=true")
	}
	if !slices.Contains(status.Failed, "domain_transaction") {
		t.Fatalf("expected domain_transaction to fail immediately, got failed=%v", status.Failed)
	}
	if slices.Contains(status.Failed, "execution_liveness") {
		t.Fatalf("execution_liveness moved with the readiness-pool probe: it must rest on work evidence alone, failed=%v", status.Failed)
	}

	// Self-heal, no restart.
	database.setTxOpenerErr(nil)
	status = registry.Readiness(context.Background())
	if !status.Ready {
		t.Fatalf("readiness did not self-heal after the readiness pool recovered; failed=%v", status.Failed)
	}

	for _, component := range components {
		if component.Name() == "river-workers" {
			continue
		}
		shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		if err := component.Shutdown(shutdownCtx); err != nil {
			t.Errorf("shutdown %s: %v", component.Name(), err)
		}
		cancel()
	}
}

// TestExecutionLivenessCatchesAWedgedConsumerWithAHealthyDatabase is the
// direct reproduction of the codex-review finding on this ticket's first
// round: an execution_liveness signal built ONLY from an independent
// self-probe goroutine keeps succeeding even when the real River consumer
// is deadlocked and the database is perfectly healthy -- exactly "recent
// jobs are all terminal-without-execution" from the ticket's Wanted
// section. This test wedges the CLAIM path specifically (backlog present,
// nothing claiming it) while leaving the domain pool fully healthy, and
// proves readiness still fails -- and that a single real claim (JobStarted)
// clears it immediately.
func TestExecutionLivenessCatchesAWedgedConsumerWithAHealthyDatabase(t *testing.T) {
	t.Chdir(filepath.Join("..", ".."))
	queues := []string{"coverage", "heartbeat", "retention", "webhooks"}
	runtimeRegistry, err := jobruntime.Load(defaultContractRoot)
	if err != nil {
		t.Fatal(err)
	}
	// The domain pool stays perfectly healthy throughout this test (no
	// txOpenerErr is ever set) -- the point is that execution_liveness must
	// still fail on the claim signal alone. Telemetry starts idle
	// (Available=0) so this test can reach admission before wedging the
	// claim path, matching CHAOS-4029's actual incident shape: the process
	// passes preclaim-readiness, THEN the dependency it depends on breaks
	// (see the mutation below) -- not "wedged from the first tick", which
	// preclaim-readiness already correctly refuses to start at all.
	// telemetry is captured by pointer into dependencies.queueTelemetry at
	// construction time (fakeWorkerDatabase.NewQueueTelemetrySampler returns
	// database.telemetry itself), so recovery below mutates ITS field in
	// place rather than reassigning database.telemetry -- a reassignment
	// after construction would not be visible to the already-captured
	// sampler.
	telemetry := &fakeQueueTelemetry{snapshot: riverstore.QueueTelemetrySnapshot{
		Jobs: []riverstore.QueueJobTelemetry{{Queue: "heartbeat", Kind: "system.heartbeat", Available: 0}},
	}}
	database := &fakeWorkerDatabase{telemetry: telemetry}
	sources := productionWorkerDependencySources
	sources.openDatabase = func(context.Context, config.Config) (workerDatabase, error) {
		return database, nil
	}
	sources.buildOperational = fakeHandlerBuilder(
		"operational",
		mustSelectedQueueSpecs(t, runtimeRegistry, queues...),
		selectedQueueBudgets(queues, queues, map[string]int{
			"coverage": 1, "heartbeat": 1, "retention": 1, "webhooks": 4,
		})...,
	)
	sources.buildRiverProcess = fakeRiverProcessBuilder("river-worker")
	// Capture the constructed *claimLiveness (see workerDependencySources'
	// injectable newClaimLiveness) so this test can shrink its staleness
	// window from the production 60s to a real-but-small duration, exactly
	// would be needed for a monitor -- otherwise proving staleness
	// would require sleeping out a full minute. newClaimLiveness's own
	// construction-time seeding (the codex-round-1 grace-period fix) still
	// applies: the window is shrunk immediately after construction, well
	// before the seed's grace period would matter for this test's timing.
	var claim *claimLiveness
	sources.newClaimLiveness = func(now time.Time, queues []string) *claimLiveness {
		claim = newClaimLiveness(now, queues)
		claim.SetStaleWindow(80 * time.Millisecond)
		return claim
	}

	registry := health.NewRegistry(2 * time.Second)
	components, err := configureWorkerDependenciesWithSources(
		context.Background(),
		config.Config{
			Queues:                 queues,
			WorkerQueueConcurrency: map[string]int{"coverage": 1, "heartbeat": 1, "retention": 1, "webhooks": 4},
			RiverDatabaseSchema:    "river",
			DomainDatabaseMaxConns: 4,
			QueueDatabaseMaxConns:  2,
		},
		registry,
		sources,
	)
	if err != nil {
		t.Fatalf("configureWorkerDependenciesWithSources() error = %v", err)
	}
	if claim == nil {
		t.Fatal("expected sources.newClaimLiveness to have been called")
	}
	for _, component := range components {
		if component.Name() == "river-workers" {
			continue
		}
		if err := component.Start(context.Background()); err != nil {
			t.Fatalf("start %s: %v", component.Name(), err)
		}
	}
	if err := (health.Gate{Registry: registry}).Start(context.Background()); err != nil {
		t.Fatalf("open readiness gate: %v", err)
	}

	// Admission: idle queue, healthy database, fully ready.
	status := registry.Readiness(context.Background())
	if !status.Ready {
		t.Fatalf("expected the freshly admitted worker to be ready, got %#v", status)
	}

	// The wedge: backlog appears on "heartbeat" AFTER admission, with the
	// process never restarted -- mirroring CHAOS-4029's actual shape (the
	// process passed preclaim-readiness; the dependency it depends on broke
	// afterward). Nothing has claimed from this queue.
	telemetry.setSnapshot(riverstore.QueueTelemetrySnapshot{
		Jobs: []riverstore.QueueJobTelemetry{{Queue: "heartbeat", Kind: "system.heartbeat", Available: 5}},
	})

	// The readiness pool stays perfectly healthy throughout (domain_transaction
	// passes): execution_liveness must still fail on the claim evidence alone.
	if status = registry.Readiness(context.Background()); slices.Contains(status.Failed, "domain_transaction") {
		t.Fatalf("expected domain_transaction to be healthy throughout, got failed=%v", status.Failed)
	}

	// Readiness must still fail once claim's (shrunk) staleness window
	// elapses: a queue has backlog and nothing has claimed from it. An
	// execution_liveness built only from livenessMonitor could not see
	// this; the claim half must. Polled rather than asserted instantly
	// because claim was seeded at construction time (the codex-round-1
	// grace-period fix), so it only goes stale once its window elapses.
	deadline := time.Now().Add(2 * time.Second)
	for {
		status = registry.Readiness(context.Background())
		if slices.Contains(status.Failed, "execution_liveness") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected execution_liveness to fail on a wedged consumer with backlog; failed=%v", status.Failed)
		}
		time.Sleep(5 * time.Millisecond)
	}
	if status.Ready {
		t.Fatal("expected readiness to be false once execution_liveness failed")
	}

	// Recovery via a REAL claim: the same JobFinished signal River's
	// execution wrapper reports for every real, actually-executed job,
	// clearing readiness immediately without waiting for the queue to drain.
	claim.recordClaim("heartbeat", time.Now())
	status = registry.Readiness(context.Background())
	if !status.Ready {
		t.Fatalf("expected readiness to recover immediately after a real claim, got %#v", status)
	}

	// Second demonstration: the OTHER externally-observable recovery path.
	// Re-wedge first (the previous claim is now fresh, which would make an
	// idle-drain assertion here trivially true regardless of the idle
	// fallback actually working), then prove the queue draining to
	// genuinely empty clears readiness on its own, with no claim at all.
	telemetry.setSnapshot(riverstore.QueueTelemetrySnapshot{
		Jobs: []riverstore.QueueJobTelemetry{{Queue: "heartbeat", Kind: "system.heartbeat", Available: 5}},
	})
	deadline = time.Now().Add(2 * time.Second)
	for {
		status = registry.Readiness(context.Background())
		if slices.Contains(status.Failed, "execution_liveness") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected execution_liveness to fail again on re-wedge; failed=%v", status.Failed)
		}
		time.Sleep(5 * time.Millisecond)
	}
	telemetry.setSnapshot(riverstore.QueueTelemetrySnapshot{
		Jobs: []riverstore.QueueJobTelemetry{{Queue: "heartbeat", Kind: "system.heartbeat", Available: 0}},
	})
	status = registry.Readiness(context.Background())
	if !status.Ready {
		t.Fatalf("expected readiness to recover once the queue drains, with no claim at all, got %#v", status)
	}

	for _, component := range components {
		if component.Name() == "river-workers" {
			continue
		}
		shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		if err := component.Shutdown(shutdownCtx); err != nil {
			t.Errorf("shutdown %s: %v", component.Name(), err)
		}
		cancel()
	}
}

// CHAOS-6818 (lead D2588 addendum): the CHAOS-4029 signal "the work pool's pooled
// connections died after a pooler recreate, every job fails at its idempotency
// Begin" is NOT judged by a probe any more but by work evidence: a job that fails
// at that Begin never reaches its handler (jobruntime's
// TestHandlerInvokedNeverFiresBeforeTenantBudgetOrIdempotencyGatesPass pins that
// HandlerInvoked fires only after the idempotency gate passes), so it never
// refreshes its queue's claim clock, and a queue with available jobs and no
// handler invocation inside the window turns execution_liveness red. Here a REAL
// pgxpool against a test server whose connections are dropped (the recreated
// pgbouncer) fails Begin the way PostgresIdempotency.Begin's first line does;
// readiness flips inside the (shrunk) window with domain_transaction still green,
// and recovers the moment a job reaches its handler again.
func TestARecreatedPoolerFlipsReadinessThroughFailingJobsWithinTheWindow(t *testing.T) {
	runRecreatedPoolerScenario(t, nil)
}

// CHAOS-6818 r1b P1: the same scenario with the queue FULL. River counts a job
// that is failing at its idempotency Begin as running, so Running == Capacity
// while nothing is inside a handler; the old "saturated => healthy" exemption
// hid exactly the state the removed self-probe used to catch.
func TestARecreatedPoolerFlipsReadinessEvenWhenEveryClaimSlotIsRunning(t *testing.T) {
	runRecreatedPoolerScenario(t, []riverstore.QueueCapacityTelemetry{
		{Queue: "heartbeat", Capacity: 1, Running: 1, Saturation: 1},
	})
}

func runRecreatedPoolerScenario(t *testing.T, capacities []riverstore.QueueCapacityTelemetry) {
	t.Helper()
	t.Chdir(filepath.Join("..", ".."))
	queues := []string{"coverage", "heartbeat", "retention", "webhooks"}
	runtimeRegistry, err := jobruntime.Load(defaultContractRoot)
	if err != nil {
		t.Fatal(err)
	}
	telemetry := &fakeQueueTelemetry{snapshot: riverstore.QueueTelemetrySnapshot{
		Jobs: []riverstore.QueueJobTelemetry{{Queue: "heartbeat", Kind: "system.heartbeat", Available: 0}},
	}}
	database := &fakeWorkerDatabase{telemetry: telemetry}
	sources := productionWorkerDependencySources
	sources.openDatabase = func(context.Context, config.Config) (workerDatabase, error) { return database, nil }
	sources.buildOperational = fakeHandlerBuilder(
		"operational",
		mustSelectedQueueSpecs(t, runtimeRegistry, queues...),
		selectedQueueBudgets(queues, queues, map[string]int{
			"coverage": 1, "heartbeat": 1, "retention": 1, "webhooks": 4,
		})...,
	)
	sources.buildRiverProcess = fakeRiverProcessBuilder("river-worker")
	var claim *claimLiveness
	sources.newClaimLiveness = func(now time.Time, queues []string) *claimLiveness {
		claim = newClaimLiveness(now, queues)
		claim.SetStaleWindow(150 * time.Millisecond)
		return claim
	}
	registry := health.NewRegistry(2 * time.Second)
	components, err := configureWorkerDependenciesWithSources(
		context.Background(),
		config.Config{
			Queues:                 queues,
			WorkerQueueConcurrency: map[string]int{"coverage": 1, "heartbeat": 1, "retention": 1, "webhooks": 4},
			RiverDatabaseSchema:    "river",
			DomainDatabaseMaxConns: 4,
			QueueDatabaseMaxConns:  2,
		},
		registry,
		sources,
	)
	if err != nil {
		t.Fatalf("configureWorkerDependenciesWithSources() error = %v", err)
	}
	for _, component := range components {
		if component.Name() == "river-workers" {
			continue
		}
		if err := component.Start(context.Background()); err != nil {
			t.Fatalf("start %s: %v", component.Name(), err)
		}
	}
	if err := (health.Gate{Registry: registry}).Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if status := registry.Readiness(context.Background()); !status.Ready {
		t.Fatalf("the admitted worker is not ready: %v", status.Failed)
	}

	// The work pool: a real pgxpool against a server we can "recreate".
	workServer := poolpg.Start(t)
	poolConfig, err := pgxpool.ParseConfig("postgres://role:secret@" + workServer.Addr() + "/db?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	poolConfig.MaxConns = 2
	workPool, err := pgxpool.NewWithConfig(context.Background(), poolConfig)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(workPool.Close)
	attempt := func() error { // what PostgresIdempotency.Begin's first line does
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		tx, err := workPool.Begin(ctx)
		if err != nil {
			return err
		}
		return tx.Rollback(ctx)
	}
	if err := attempt(); err != nil {
		t.Fatalf("a job's Begin on a healthy pooler failed: %v", err)
	}

	// The pooler is recreated: established connections die, jobs are waiting.
	workServer.DropConnections(true)
	telemetry.setSnapshot(riverstore.QueueTelemetrySnapshot{
		Jobs:            []riverstore.QueueJobTelemetry{{Queue: "heartbeat", Kind: "system.heartbeat", Available: 5}},
		QueueCapacities: capacities,
	})
	deadline := time.Now().Add(3 * time.Second)
	for {
		if err := attempt(); err == nil {
			t.Fatal("a job's Begin succeeded against a recreated pooler")
		}
		status := registry.Readiness(context.Background())
		if slices.Contains(status.Failed, "execution_liveness") {
			if !slices.Equal(status.Failed, []string{"execution_liveness"}) {
				t.Fatalf("expected ONLY execution_liveness to flip (the probe pool is healthy), failed=%v", status.Failed)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("failing jobs with a backlog never flipped execution_liveness inside the window; failed=%v", status.Failed)
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Recovery: the pooler is back, a job's Begin succeeds, its handler runs (the
	// HandlerInvoked tap records the claim), readiness heals with no restart.
	workServer.DropConnections(false)
	if err := attempt(); err != nil {
		t.Fatalf("a job's Begin against the recovered pooler failed: %v", err)
	}
	claim.recordClaim("heartbeat", time.Now())
	if status := registry.Readiness(context.Background()); !status.Ready {
		t.Fatalf("readiness did not recover once a job reached its handler again: %v", status.Failed)
	}

	for _, component := range components {
		if component.Name() == "river-workers" {
			continue
		}
		shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		if err := component.Shutdown(shutdownCtx); err != nil {
			t.Errorf("shutdown %s: %v", component.Name(), err)
		}
		cancel()
	}
}

// CHAOS-6818 r1b P3: the observer the worker hands to every family builder must be
// the claim-liveness one, wired to BOTH taps. Swapping it for the plain metrics
// collector (or dropping the return tap) must fail here, not pass silently.
func TestProductionObserverFeedsClaimLivenessThroughBothHandlerTaps(t *testing.T) {
	t.Chdir(filepath.Join("..", ".."))
	queues := []string{"heartbeat"}
	runtimeRegistry, err := jobruntime.Load(defaultContractRoot)
	if err != nil {
		t.Fatal(err)
	}
	database := &fakeWorkerDatabase{telemetry: &fakeQueueTelemetry{}}
	sources := productionWorkerDependencySources
	sources.openDatabase = func(context.Context, config.Config) (workerDatabase, error) { return database, nil }
	specs := mustSelectedQueueSpecs(t, runtimeRegistry, queues...)
	budgets := selectedQueueBudgets(queues, queues, map[string]int{"heartbeat": 1})
	var observed jobruntime.Observer
	sources.buildOperational = func(
		_ config.Config, _ workerDatabase, _ *jobruntime.Registry, observer jobruntime.Observer, _ *slog.Logger, _ *river.Workers,
	) (workerFamily, error) {
		observed = observer
		return workerFamily{handlers: specs, queues: budgets}, nil
	}
	sources.buildRiverProcess = fakeRiverProcessBuilder("river-worker")
	var claim *claimLiveness
	sources.newClaimLiveness = func(now time.Time, queues []string) *claimLiveness {
		claim = newClaimLiveness(now, queues)
		return claim
	}
	if _, err := configureWorkerDependenciesWithSources(
		context.Background(),
		config.Config{
			Queues: queues, WorkerQueueConcurrency: map[string]int{"heartbeat": 1},
			RiverDatabaseSchema: "river", DomainDatabaseMaxConns: 4, QueueDatabaseMaxConns: 2,
		},
		health.NewRegistry(2*time.Second), sources,
	); err != nil {
		t.Fatalf("configureWorkerDependenciesWithSources() error = %v", err)
	}
	invoked, ok := observed.(jobruntime.HandlerInvocationObserver)
	if !ok {
		t.Fatalf("the observer given to family builders (%T) lacks HandlerInvoked", observed)
	}
	returned, ok := observed.(jobruntime.HandlerReturnObserver)
	if !ok {
		t.Fatalf("the observer given to family builders (%T) lacks HandlerReturned", observed)
	}
	labels := jobruntime.JobLabels{Queue: "heartbeat", Kind: "system.heartbeat"}
	invoked.HandlerInvoked(context.Background(), labels)
	if got := claim.handlersInside("heartbeat"); got != 1 {
		t.Fatalf("HandlerInvoked through the production observer left handlersInside = %d, want 1", got)
	}
	returned.HandlerReturned(context.Background(), labels)
	if got := claim.handlersInside("heartbeat"); got != 0 {
		t.Fatalf("HandlerReturned through the production observer left handlersInside = %d, want 0", got)
	}
}
