package main

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	schedulersync "github.com/full-chaos/dev-health-ops/internal/scheduler/sync"
	"github.com/jackc/pgx/v5/pgxpool"
)

// TestSyncSchedulerLoopReceivesTheComposedLoggerFromSchedulerCompositionRoot
// proves the wiring, not just the field (CHAOS-3907): it goes through the
// real composition root (buildSchedulerLoopWithSources with a real
// schedulersync.NewLoop), not a direct
// schedulersync.NewLoop(..., LoopConfig{Logger: ...}) call, which would prove
// nothing about cmd/dev-health-scheduler. scheduler/sync.Loop is the literal
// sibling of scheduler/fixed.Loop (already fixed under CHAOS-3903); before
// this wiring, sync.Loop had no Logger field at all and a handoff window
// could fail forever with zero output.
func TestSyncSchedulerLoopReceivesTheComposedLoggerFromSchedulerCompositionRoot(t *testing.T) {
	database := &fakeSchedulerDatabase{pool: &pgxpool.Pool{}, coordinatorPool: &pgxpool.Pool{}}
	fixedLoop := &fakeFixedLoop{}
	forcedErr := errors.New("handoff due-result probe failure")
	sources := schedulerRuntimeSources{
		openDatabase: func(context.Context, config.Config) (schedulerDatabase, error) {
			return database, nil
		},
		newRepository: func(*pgxpool.Pool) (schedulersync.HandoffStepper, error) {
			return schedulerHandoffStepperFunc(func(
				context.Context, time.Time, int, schedulersync.Coordinator,
			) (schedulersync.HandoffResult, error) {
				return schedulersync.HandoffResult{}, forcedErr
			}), nil
		},
		newCoordinator: func() schedulersync.Coordinator {
			return schedulersync.CoordinatorFunc(func(
				context.Context, schedulersync.HandoffTransaction, schedulersync.Occurrence,
			) (schedulersync.HandoffOutcome, error) {
				return schedulersync.OccurrenceMinted, nil
			})
		},
		newLoop:        schedulersync.NewLoop,
		newOccurrences: stubOccurrenceSource,
		newFixedLoop: func(*pgxpool.Pool, *health.Registry, *slog.Logger) (fixedScheduleRuntime, error) {
			return fixedLoop, nil
		},
	}

	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))

	registry := health.NewRegistry(100 * time.Millisecond)
	component, err := buildSchedulerLoopWithSources(
		context.Background(),
		config.Config{RiverDatabaseSchema: "river"},
		registry,
		sources,
		logger,
	)
	if err != nil {
		t.Fatalf("buildSchedulerLoopWithSources() error = %v", err)
	}
	if startErr := component.Start(context.Background()); startErr == nil {
		t.Fatal("expected the forced handoff failure to fail Start")
	}

	output := buf.String()
	if !strings.Contains(output, "sync scheduler initial handoff failed") {
		t.Fatalf("composed logger did not observe the sync scheduler loop's failure: %s", output)
	}
	if !strings.Contains(output, forcedErr.Error()) {
		t.Fatalf("composed logger output omitted the failure cause: %s", output)
	}
}
