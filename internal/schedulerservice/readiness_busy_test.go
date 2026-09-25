package schedulerservice

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	schedulersync "github.com/full-chaos/dev-health-ops/internal/scheduler/sync"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/poolpg"
)

// CHAOS-6771: the scheduler's execution_liveness self-probe runs on the domain
// pool; a pool fully acquired by progressing work is BUSY, not BROKEN. Real
// pgxpool connections (poolpg), the production builder and the real health
// registry: stale the monitor, hold every connection, sample once -- the sample
// passes as busy and readiness does not fail execution_liveness. Handing the
// monitor the raw opener fails this test.
func TestExecutionLivenessSamplePassesAsBusyOnAFullyAcquiredDomainPool(t *testing.T) {
	ctx := context.Background()
	addr := poolpg.Serve(t)
	poolConfig, err := pgxpool.ParseConfig("postgres://domain:secret@" + addr + "/devhealth?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	poolConfig.MaxConns = 2
	domainPool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer domainPool.Close()
	coordinatorPool, err := pgxpool.New(ctx, "postgresql://coordinator@127.0.0.1:1/devhealth")
	if err != nil {
		t.Fatal(err)
	}
	defer coordinatorPool.Close()

	database := &fakeSchedulerDatabase{pool: domainPool, coordinatorPool: coordinatorPool}
	sources := schedulerRuntimeSources{
		openDatabase: func(context.Context, config.Config) (schedulerDatabase, error) { return database, nil },
		newRepository: func(*pgxpool.Pool) (schedulersync.HandoffStepper, error) {
			return schedulerHandoffStepperFunc(func(
				context.Context, time.Time, int, schedulersync.Coordinator,
			) (schedulersync.HandoffResult, error) {
				return schedulersync.HandoffResult{}, nil
			}), nil
		},
		newCoordinator: schedulersync.NewOccurrenceCoordinator,
		newLoop:        schedulersync.NewLoop,
		newOccurrences: stubOccurrenceSource,
		newFixedLoop: func(*pgxpool.Pool, *health.Registry, *slog.Logger) (fixedScheduleRuntime, error) {
			return &fakeFixedLoop{}, nil
		},
	}
	previousWindow := busyProgressWindow
	busyProgressWindow = 300 * time.Millisecond
	t.Cleanup(func() { busyProgressWindow = previousWindow })
	registry := health.NewRegistry(readinessTestCheckTimeout)
	runtime, err := buildSchedulerLoopWithSources(ctx, config.Config{}, registry, sources, slog.Default())
	if err != nil {
		t.Fatalf("buildSchedulerLoopWithSources() error = %v", err)
	}
	scheduler, ok := runtime.(schedulerRuntime)
	if !ok || scheduler.livenessMonitor == nil {
		t.Fatalf("runtime = %#v, want a schedulerRuntime with an execution-liveness monitor", runtime)
	}
	if status := registry.CheckRequired(ctx); containsString(status.Failed, "execution_liveness") {
		t.Fatalf("the idle pool's admission sample failed: %v", status.Failed)
	}

	scheduler.livenessMonitor.SetStaleness(50 * time.Millisecond)
	time.Sleep(120 * time.Millisecond) // the admission sample is now stale
	if status := registry.CheckRequired(ctx); !containsString(status.Failed, "execution_liveness") {
		t.Fatalf("a stale monitor still reads ready: %v", status.Failed)
	}
	var held []*pgxpool.Conn
	for i := 0; i < 2; i++ {
		connection, err := domainPool.Acquire(ctx)
		if err != nil {
			t.Fatal(err)
		}
		held = append(held, connection)
	}
	defer func() {
		for _, connection := range held {
			connection.Release()
		}
	}()

	probeCtx, cancel := context.WithTimeout(ctx, 4*time.Second)
	defer cancel()
	scheduler.livenessMonitor.Probe(probeCtx)
	if status := registry.CheckRequired(ctx); containsString(status.Failed, "execution_liveness") {
		t.Fatalf("a busy sample left execution_liveness failed: %v", status.Failed)
	}
	// r3 P1: the busy pass refreshed the monitor's success, which the monitor
	// honours for its whole staleness allowance. Readiness must still go red once
	// the WORK evidence is older than the progress window (no work acquire since).
	scheduler.livenessMonitor.SetStaleness(time.Minute) // the monitor alone would stay fresh for the whole test
	time.Sleep(2 * busyProgressWindow)
	if status := registry.CheckRequired(ctx); !containsString(status.Failed, "execution_liveness") {
		t.Fatalf("a wedged pool stayed ready after the progress window: monitor freshness alone decided readiness (%v)", status.Failed)
	}
}

// cachedPostureDatabase is a fake that offers a cached domain posture check.
type cachedPostureDatabase struct {
	*fakeSchedulerDatabase
	check health.CheckFunc
}

func (database *cachedPostureDatabase) DomainPostureCheck(*slog.Logger) health.CheckFunc {
	return database.check
}

// CHAOS-6771 (r1 pin): domain_postgres must run the CACHED posture check when
// the database offers one, and only fall back to DomainReady when it does not.
// The provider check fails while DomainReady would pass, so a registration that
// ignored the provider reads as ready and fails this test.
func TestDomainPostgresRunsTheCachedPostureCheckWhenOffered(t *testing.T) {
	build := func(t *testing.T, database schedulerDatabase, fake *fakeSchedulerDatabase) *health.Registry {
		t.Helper()
		ctx := context.Background()
		fake.pool, _ = pgxpool.New(ctx, "postgresql://domain@127.0.0.1:1/devhealth")
		fake.coordinatorPool, _ = pgxpool.New(ctx, "postgresql://coordinator@127.0.0.1:1/devhealth")
		t.Cleanup(fake.pool.Close)
		t.Cleanup(fake.coordinatorPool.Close)
		sources := schedulerRuntimeSources{
			openDatabase: func(context.Context, config.Config) (schedulerDatabase, error) { return database, nil },
			newRepository: func(*pgxpool.Pool) (schedulersync.HandoffStepper, error) {
				return schedulerHandoffStepperFunc(func(
					context.Context, time.Time, int, schedulersync.Coordinator,
				) (schedulersync.HandoffResult, error) {
					return schedulersync.HandoffResult{}, nil
				}), nil
			},
			newCoordinator: schedulersync.NewOccurrenceCoordinator,
			newLoop:        schedulersync.NewLoop,
			newOccurrences: stubOccurrenceSource,
			newFixedLoop: func(*pgxpool.Pool, *health.Registry, *slog.Logger) (fixedScheduleRuntime, error) {
				return &fakeFixedLoop{}, nil
			},
		}
		registry := health.NewRegistry(readinessTestCheckTimeout)
		if _, err := buildSchedulerLoopWithSources(ctx, config.Config{}, registry, sources, slog.Default()); err != nil {
			t.Fatalf("buildSchedulerLoopWithSources() error = %v", err)
		}
		return registry
	}
	t.Run("provider check used", func(t *testing.T) {
		fake := &fakeSchedulerDatabase{}
		database := &cachedPostureDatabase{fakeSchedulerDatabase: fake, check: func(context.Context) error {
			return errors.New("cached posture refused")
		}}
		status := build(t, database, fake).CheckRequired(context.Background())
		if !containsString(status.Failed, "domain_postgres") {
			t.Fatalf("domain_postgres ignored the provider's cached check: failed=%v", status.Failed)
		}
		if fake.domainCalls.Load() != 0 {
			t.Fatalf("DomainReady ran %d times although a cached check was offered", fake.domainCalls.Load())
		}
	})
	t.Run("no provider check falls back to DomainReady", func(t *testing.T) {
		fake := &fakeSchedulerDatabase{}
		database := &cachedPostureDatabase{fakeSchedulerDatabase: fake, check: nil}
		status := build(t, database, fake).CheckRequired(context.Background())
		if containsString(status.Failed, "domain_postgres") {
			t.Fatalf("the fallback DomainReady (healthy) failed: %v", status.Failed)
		}
		if fake.domainCalls.Load() == 0 {
			t.Fatal("DomainReady was not consulted when no cached check was offered")
		}
	})
}
