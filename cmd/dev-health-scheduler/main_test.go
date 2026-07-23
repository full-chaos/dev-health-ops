package main

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/full-chaos/dev-health-ops/internal/platform/lifecycle"
	schedulersync "github.com/full-chaos/dev-health-ops/internal/scheduler/sync"
)

type schedulerTestComponent struct{}

func (schedulerTestComponent) Name() string                   { return "scheduler-test-loop" }
func (schedulerTestComponent) Start(context.Context) error    { return nil }
func (schedulerTestComponent) Shutdown(context.Context) error { return nil }

func TestSchedulerSpecConfiguresFailClosedDependencies(t *testing.T) {
	if schedulerOwnership != schedulersync.DefaultOwnershipPolicy() {
		t.Fatalf("scheduler ownership = %#v", schedulerOwnership)
	}
	if schedulerSpec.Service != "dev-health-scheduler" {
		t.Fatalf("service = %q", schedulerSpec.Service)
	}
	if schedulerSpec.ConfigureDependencies == nil {
		t.Fatal("scheduler dependency configuration is not wired")
	}

	registry := health.NewRegistry(100 * time.Millisecond)
	components, err := configureSchedulerDependencies(context.Background(), config.Config{}, registry)
	if err != nil {
		t.Fatalf("configureSchedulerDependencies() error = %v", err)
	}
	if len(components) != 0 {
		t.Fatalf("components = %d, want no scheduler runtime before its loop is implemented", len(components))
	}
	if err := (health.Gate{Registry: registry}).Start(context.Background()); err != nil {
		t.Fatalf("open readiness gate: %v", err)
	}

	want := []string{"domain_postgres", "queue_postgres", "river_schema", "scheduler_loop"}
	status := registry.Readiness(context.Background())
	if status.Ready || !slices.Equal(status.Failed, want) {
		t.Fatalf("readiness = %#v, want failed %v", status, want)
	}
}

func TestSchedulerActivationIsPrivateSourceReviewedComposition(t *testing.T) {
	registry := health.NewRegistry(100 * time.Millisecond)
	called := false
	components, err := configureSchedulerDependenciesWithSources(
		context.Background(),
		config.Config{},
		registry,
		schedulerActivation{goOwnsMarkers: true, coordinatorPolicyParity: true},
		schedulerDependencySources{buildLoop: func(context.Context, config.Config, *health.Registry) (lifecycle.Component, error) {
			called = true
			return schedulerTestComponent{}, nil
		}},
	)
	if err != nil || !called || len(components) != 1 || components[0].Name() != "scheduler-test-loop" {
		t.Fatalf("reviewed activation components=%v called=%v err=%v", components, called, err)
	}

	registry = health.NewRegistry(100 * time.Millisecond)
	_, err = configureSchedulerDependenciesWithSources(
		context.Background(), config.Config{}, registry,
		schedulerActivation{goOwnsMarkers: true},
		schedulerDependencySources{buildLoop: func(context.Context, config.Config, *health.Registry) (lifecycle.Component, error) {
			t.Fatal("activation without coordinator parity invoked the loop factory")
			return nil, nil
		}},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := (health.Gate{Registry: registry}).Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if status := registry.Readiness(context.Background()); status.Ready || !slices.Contains(status.Failed, "scheduler_loop") {
		t.Fatalf("non-parity readiness = %#v", status)
	}

	_, err = configureSchedulerDependenciesWithSources(
		context.Background(), config.Config{}, health.NewRegistry(time.Second),
		schedulerActivation{goOwnsMarkers: true, coordinatorPolicyParity: true},
		schedulerDependencySources{buildLoop: func(context.Context, config.Config, *health.Registry) (lifecycle.Component, error) {
			return nil, errors.New("private factory failure")
		}},
	)
	if !errors.Is(err, errSchedulerActivationUnavailable) {
		t.Fatalf("failed private factory error = %v", err)
	}
}
