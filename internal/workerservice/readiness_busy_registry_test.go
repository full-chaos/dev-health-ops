package workerservice

import (
	"context"
	"path/filepath"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/platform/busyprobe"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/full-chaos/dev-health-ops/internal/platform/selfprobe"
)

// blockingDatabase is a fake whose domain pool has every connection held: Begin
// waits for a connection until the caller's context ends, like pool.Begin on an
// exhausted pgxpool.
type blockingDatabase struct {
	*fakeWorkerDatabase
	exhausted atomic.Bool
	// hideSaturation makes PoolSaturation report an idle pool while acquires
	// still block: a slow or gone database, not a busy pool.
	hideSaturation atomic.Bool
}

func (database *blockingDatabase) DomainTxOpener() selfprobe.TxOpener {
	return blockingOpener{database: database}
}

type blockingOpener struct{ database *blockingDatabase }

func (opener blockingOpener) Begin(ctx context.Context) (selfprobe.Tx, error) {
	if opener.database.exhausted.Load() {
		<-ctx.Done()
		return nil, &selfprobe.AcquireError{Err: ctx.Err()} // waiting for a pool connection
	}
	return fakeTxOpenerTx{}, nil
}

func (database *blockingDatabase) PoolSaturation() (float64, float64) {
	if database.exhausted.Load() && !database.hideSaturation.Load() {
		return 1, 0
	}
	return 0, 0
}

// busyComposition builds the production dependency wiring around a fake
// database (the scaffold TestExecutionLivenessCatchesAWedgedDomainPoolAfterAdmission
// uses) with the REAL health registry at the given per-check budget.
func busyComposition(t *testing.T, budget time.Duration) (*health.Registry, *blockingDatabase, *selfprobe.Monitor) {
	t.Helper()
	t.Chdir(filepath.Join("..", ".."))
	queues := []string{"coverage", "heartbeat", "retention", "webhooks"}
	runtimeRegistry, err := jobruntime.Load(defaultContractRoot)
	if err != nil {
		t.Fatal(err)
	}
	database := &blockingDatabase{fakeWorkerDatabase: &fakeWorkerDatabase{}}
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
	registry := health.NewRegistry(budget)
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
	monitor, ok := components[2].(*selfprobe.Monitor)
	if !ok {
		t.Fatalf("components[2] = %#v, want *selfprobe.Monitor", components[2])
	}
	for _, component := range components {
		if component.Name() == "river-workers" {
			continue
		}
		if err := component.Start(context.Background()); err != nil {
			t.Fatalf("start %s: %v", component.Name(), err)
		}
		shutdown := component.Shutdown
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = shutdown(ctx)
		})
	}
	if err := (health.Gate{Registry: registry}).Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	return registry, database, monitor
}

// CHAOS-6771 r1 P1: a busy pass has to reach the caller INSIDE the check's own
// deadline. The health registry gives up on a check at its budget, so a wrapper
// that spent the whole budget acquiring and only then proved progress counted a
// busy pass nobody saw: readiness stayed unready. With the real registry, an
// exhausted domain pool must read as ready, in time.
func TestABusyPassReachesTheRegistryInsideTheCheckBudget(t *testing.T) {
	// The production per-check budget (values.prod.yaml --health-check-timeout=10s).
	const budget = 10 * time.Second
	registry, database, _ := busyComposition(t, budget)
	if status := registry.Readiness(context.Background()); !status.Ready {
		t.Fatalf("a freshly admitted worker is not ready: %#v", status)
	}

	database.exhausted.Store(true)
	started := time.Now()
	status := registry.Readiness(context.Background())
	elapsed := time.Since(started)
	if !status.Ready {
		t.Fatalf("readiness with a fully acquired domain pool and green claim liveness = %#v after %s, want ready (busy, not broken)", status, elapsed)
	}
	for _, check := range status.Checks {
		if check.TimedOut {
			t.Fatalf("check %s timed out: %#v", check.Name, status)
		}
	}
	// A saturated pool frees a connection in milliseconds when it is healthy:
	// the probe waits busyprobe.AcquireWait, not the whole 10 s budget, so the kubelet
	// (10 s probe timeout) is not left racing the answer.
	if elapsed > busyprobe.AcquireWait+time.Second {
		t.Fatalf("the busy pass took %s, want about %s", elapsed, busyprobe.AcquireWait)
	}
}

// Not saturated is not busy: the same blocked acquire on a pool that is not
// fully acquired stays a failure (the database is slow or gone), and it is
// reported inside the budget too, not as a timeout.
func TestABlockedAcquireOnAnUnsaturatedPoolStillFailsReadiness(t *testing.T) {
	const budget = 2 * time.Second
	registry, database, _ := busyComposition(t, budget)
	database.exhausted.Store(true)
	database.hideSaturation.Store(true)
	started := time.Now()
	status := registry.Readiness(context.Background())
	if status.Ready || !slices.Contains(status.Failed, "idempotency_backend") {
		t.Fatalf("a blocked acquire on an unsaturated pool read as ready or lost the check: %#v", status)
	}
	if elapsed := time.Since(started); elapsed >= budget {
		t.Fatalf("the failure took %s, no faster than the %s check budget", elapsed, budget)
	}
}

// The ticking execution-liveness monitor is wired through the same tolerance:
// stale it, exhaust the pool, sample once, and it is fresh again. Handing the
// monitor the raw opener leaves it stale and readiness red.
func TestExecutionLivenessMonitorIsWiredThroughTheBusyTolerance(t *testing.T) {
	registry, database, monitor := busyComposition(t, 10*time.Second)
	monitor.SetStaleness(50 * time.Millisecond)
	time.Sleep(120 * time.Millisecond) // the admission sample is now stale
	database.exhausted.Store(true)

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	monitor.Probe(ctx)
	status := registry.Readiness(context.Background())
	if slices.Contains(status.Failed, "execution_liveness") {
		t.Fatalf("execution_liveness stayed failed after a busy sample: %#v", status)
	}
}
