package main

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/joboutbox"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/full-chaos/dev-health-ops/internal/platform/lifecycle"
	"github.com/full-chaos/dev-health-ops/internal/syncdispatchcontract"
	"github.com/full-chaos/dev-health-ops/internal/syncreconciler"
	"github.com/jackc/pgx/v5/pgxpool"
)

// TestOutboxReconcilerLoopReceivesTheComposedLoggerFromReconcilerCompositionRoot
// proves the wiring, not just the field (CHAOS-3907): it goes through the
// real composition root (configureReconcilerDependenciesWithSourcesAndLogger),
// not a direct joboutbox.NewReconcilerLoop(..., ReconcilerLoopConfig{Logger:
// ...}) call, which would prove nothing about cmd/dev-health-reconciler.
// Before dependencies.go wired ReconcilerLoopConfig.Logger to the resolved
// logger, this loop had zero slog references at all: a failed step closed
// readiness with no output anywhere (CHAOS-3907).
func TestOutboxReconcilerLoopReceivesTheComposedLoggerFromReconcilerCompositionRoot(t *testing.T) {
	t.Chdir(filepath.Join("..", ".."))
	database := &fakeReconcilerDatabase{}
	sources := reconcilerSourcesForTest(t, database)
	forcedErr := errors.New("outbox relay step probe failure")
	sources.buildRelay = func(_, _, _ *pgxpool.Pool, _ string, _ *jobruntime.Registry) (joboutbox.RelayStepper, error) {
		return reconcilerStepFunc(func(context.Context, time.Time, int) (joboutbox.StepResult, error) {
			return joboutbox.StepResult{}, forcedErr
		}), nil
	}

	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))

	registry := health.NewRegistry(100 * time.Millisecond)
	components, err := configureReconcilerDependenciesWithSourcesAndLogger(
		context.Background(),
		config.Config{RiverDatabaseSchema: "river"},
		registry,
		logger,
		sources,
	)
	if err != nil {
		t.Fatalf("configureReconcilerDependenciesWithSourcesAndLogger() error = %v", err)
	}

	var loopComponent lifecycle.Component
	for _, component := range components {
		if component.Name() == "outbox-reconciler-loop" {
			loopComponent = component
		}
	}
	if loopComponent == nil {
		t.Fatalf("outbox-reconciler-loop was not constructed: components=%v", componentNames(components))
	}
	if startErr := loopComponent.Start(context.Background()); startErr == nil {
		t.Fatal("expected the forced relay step failure to fail Start")
	}

	output := buf.String()
	if !strings.Contains(output, "outbox reconciler initial step failed") {
		t.Fatalf("composed logger did not observe the reconciler loop's failure: %s", output)
	}
	if !strings.Contains(output, forcedErr.Error()) {
		t.Fatalf("composed logger output omitted the failure cause: %s", output)
	}
}

// TestSyncDispatchObserverLoopReceivesTheComposedLoggerFromReconcilerCompositionRoot
// is the same proof for syncreconciler.Loop, the fourth of the five silent
// components from CHAOS-3907.
func TestSyncDispatchObserverLoopReceivesTheComposedLoggerFromReconcilerCompositionRoot(t *testing.T) {
	t.Chdir(filepath.Join("..", ".."))
	database := &fakeReconcilerDatabase{}
	sources := reconcilerSourcesForTest(t, database)
	sources.buildRelay = func(_, _, _ *pgxpool.Pool, _ string, _ *jobruntime.Registry) (joboutbox.RelayStepper, error) {
		return reconcilerStepFunc(func(context.Context, time.Time, int) (joboutbox.StepResult, error) {
			return joboutbox.StepResult{}, nil
		}), nil
	}
	forcedErr := errors.New("sync dispatch observation probe failure")
	sources.buildSyncMutation = func(
		_, _, _ *pgxpool.Pool, _ string, _ *syncdispatchcontract.Registry, _ config.Config, _ *health.Registry,
	) (syncreconciler.Stepper, error) {
		return syncStepFunc(func(context.Context, time.Time, int) (syncreconciler.Observation, error) {
			return syncreconciler.Observation{}, forcedErr
		}), nil
	}

	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))

	registry := health.NewRegistry(100 * time.Millisecond)
	components, err := configureReconcilerDependenciesWithSourcesAndLogger(
		context.Background(),
		config.Config{RiverDatabaseSchema: "river"},
		registry,
		logger,
		sources,
	)
	if err != nil {
		t.Fatalf("configureReconcilerDependenciesWithSourcesAndLogger() error = %v", err)
	}

	var loopComponent lifecycle.Component
	for _, component := range components {
		if component.Name() == "sync-dispatch-observer-loop" {
			loopComponent = component
		}
	}
	if loopComponent == nil {
		t.Fatalf("sync-dispatch-observer-loop was not constructed: components=%v", componentNames(components))
	}
	if startErr := loopComponent.Start(context.Background()); startErr == nil {
		t.Fatal("expected the forced sync-dispatch observation failure to fail Start")
	}

	output := buf.String()
	if !strings.Contains(output, "sync dispatch observer initial step failed") {
		t.Fatalf("composed logger did not observe the sync loop's failure: %s", output)
	}
	if !strings.Contains(output, forcedErr.Error()) {
		t.Fatalf("composed logger output omitted the failure cause: %s", output)
	}
}
