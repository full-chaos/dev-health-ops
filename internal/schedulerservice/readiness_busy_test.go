package schedulerservice

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	schedulersync "github.com/full-chaos/dev-health-ops/internal/scheduler/sync"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakepg"
)

// CHAOS-6771: the scheduler's execution_liveness self-probe runs on the domain
// pool; a pool fully acquired by progressing work is BUSY, not BROKEN. Real
// pgxpool connections (fakepg), the production builder and the real health
// registry: stale the monitor, hold every connection, sample once -- the sample
// passes as busy and readiness does not fail execution_liveness. Handing the
// monitor the raw opener fails this test.
func TestExecutionLivenessSamplePassesAsBusyOnAFullyAcquiredDomainPool(t *testing.T) {
	ctx := context.Background()
	addr := fakepg.Serve(t)
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
}
