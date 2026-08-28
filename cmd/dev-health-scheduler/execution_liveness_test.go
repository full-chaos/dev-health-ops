package main

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	schedulersync "github.com/full-chaos/dev-health-ops/internal/scheduler/sync"
	"github.com/jackc/pgx/v5/pgxpool"
)

// TestExecutionLivenessIsWiredToTheRealDomainPool proves CHAOS-4029's
// scheduler-side signal: execution_liveness is registered as a required
// readiness check, probes the SAME domain pool the rest of this process
// depends on (not a disconnected placeholder), and starts unready when that
// pool cannot open a transaction -- exactly the class of failure the ticket
// exists to catch (a dependency that stops working after admission, not just
// at startup).
//
// The fake pool here is constructed via pgxpool.New against an address
// nothing listens on (127.0.0.1:1): pgxpool.New dials lazily, so
// construction succeeds and the first real use -- this check's Begin --
// fails, proving the wiring reaches an actual transaction attempt rather
// than a short-circuited stub.
func TestExecutionLivenessIsWiredToTheRealDomainPool(t *testing.T) {
	ctx := context.Background()
	domainPool, err := pgxpool.New(ctx, "postgresql://domain@127.0.0.1:1/devhealth")
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
	fixedLoop := &fakeFixedLoop{}
	sources := schedulerRuntimeSources{
		openDatabase: func(context.Context, config.Config) (schedulerDatabase, error) {
			return database, nil
		},
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
			return fixedLoop, nil
		},
	}

	registry := health.NewRegistry(500 * time.Millisecond)
	runtime, err := buildSchedulerLoopWithSources(ctx, config.Config{}, registry, sources, slog.Default())
	if err != nil {
		t.Fatalf("buildSchedulerLoopWithSources() error = %v", err)
	}
	scheduler, ok := runtime.(schedulerRuntime)
	if !ok {
		t.Fatalf("runtime = %#v, want schedulerRuntime", runtime)
	}
	if scheduler.livenessMonitor == nil {
		t.Fatal("expected a non-nil execution-liveness monitor wired to the real domain pool")
	}
	status := registry.CheckRequired(ctx)
	if !containsString(status.Failed, "execution_liveness") {
		t.Fatalf("expected execution_liveness to fail against an unreachable domain pool, got failed=%v", status.Failed)
	}
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
