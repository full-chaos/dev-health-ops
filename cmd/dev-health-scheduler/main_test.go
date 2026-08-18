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

func TestSchedulerSpecRejectsAnActivatedRuntimeWithoutDatabaseConfiguration(t *testing.T) {
	// CHAOS-3128: schedulerOwnership is the reviewed transfer, and the
	// checked-in activation now reaches the real factory. Missing required
	// role-specific database configuration must therefore fail closed.
	if schedulerOwnership != schedulersync.TransferScheduleMarkerOwnershipToGo() {
		t.Fatalf("scheduler ownership = %#v", schedulerOwnership)
	}
	if err := schedulerOwnership.Validate(); err != nil {
		t.Fatalf("transferred ownership failed validation: %v", err)
	}
	if schedulerSpec.Service != "dev-health-scheduler" {
		t.Fatalf("service = %q", schedulerSpec.Service)
	}
	if schedulerSpec.ConfigureDependencies == nil {
		t.Fatal("scheduler dependency configuration is not wired")
	}

	// Failing closed means LIVE AND UNREADY, not exiting. This previously
	// asserted errSchedulerActivationUnavailable, which is what made an
	// unconfigured scheduler container exit before it could publish its
	// operator port -- ci/check_go_containers.sh's smoke contract requires the
	// process to stay up, serve /healthz 200 and /readyz 503, and name the
	// checks that failed. An absent DSN is a DECLARED configuration rejection
	// (postgres.ConfigurationRejected), so it is reported through readiness an
	// operator can scrape rather than a crash loop that names nothing.
	registry := health.NewRegistry(100 * time.Millisecond)
	components, err := configureSchedulerDependencies(context.Background(), config.Config{}, registry)
	if err != nil || len(components) != 0 {
		t.Fatalf("unconfigured activated scheduler components=%v err=%v", components, err)
	}
	if err := (health.Gate{Registry: registry}).Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	status := registry.Readiness(context.Background())
	if status.Ready {
		t.Fatal("an unconfigured scheduler reported ready")
	}
	// Exactly the names ci/check_go_containers.sh greps for on this target,
	// plus coordinator_postgres, so the readiness surface does not change shape
	// depending on WHY the loop is not running.
	for _, name := range []string{
		"domain_postgres", "queue_postgres", "coordinator_postgres",
		"river_schema", "scheduler_loop",
	} {
		if !slices.Contains(status.Failed, name) {
			t.Fatalf("unconfigured readiness omitted %q: %#v", name, status)
		}
	}
}

// TestSchedulerCrashLoopsOnAnOperationalDatabaseFailure is the other half of
// the contract the test above pins. A declared configuration rejection stays
// live; anything operational -- an unreachable host, refused credentials, an
// unparseable DSN -- must still terminate the process rather than idle as an
// alive-but-unready zombie (CHAOS-3873).
func TestSchedulerCrashLoopsOnAnOperationalDatabaseFailure(t *testing.T) {
	registry := health.NewRegistry(100 * time.Millisecond)
	_, err := buildSchedulerLoopWithSources(
		context.Background(), config.Config{}, registry,
		schedulerRuntimeSources{
			openDatabase: func(context.Context, config.Config) (schedulerDatabase, error) {
				return nil, errors.New("dial tcp 10.0.0.1:5432: connect: connection refused")
			},
			newRepository:  productionSchedulerRuntimeSources.newRepository,
			newCoordinator: productionSchedulerRuntimeSources.newCoordinator,
			newLoop:        productionSchedulerRuntimeSources.newLoop,
			newFixedLoop:   productionSchedulerRuntimeSources.newFixedLoop,
			newOccurrences: productionSchedulerRuntimeSources.newOccurrences,
		},
	)
	if errors.Is(err, errSchedulerDatabaseUnconfigured) {
		t.Fatal("an operational database failure was treated as a configuration rejection")
	}
	if !errors.Is(err, errSchedulerActivationUnavailable) {
		t.Fatalf("operational failure error = %v", err)
	}
}

func TestSchedulerActivationIsPrivateSourceReviewedComposition(t *testing.T) {
	registry := health.NewRegistry(100 * time.Millisecond)
	called := false
	components, err := configureSchedulerDependenciesWithSources(
		context.Background(),
		config.Config{},
		registry,
		schedulerActivation{goOwnsMarkers: true},
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
		schedulerActivation{},
		schedulerDependencySources{buildLoop: func(context.Context, config.Config, *health.Registry) (lifecycle.Component, error) {
			t.Fatal("activation without goOwnsMarkers invoked the loop factory")
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
		t.Fatalf("non-activated readiness = %#v", status)
	}

	_, err = configureSchedulerDependenciesWithSources(
		context.Background(), config.Config{}, health.NewRegistry(time.Second),
		schedulerActivation{goOwnsMarkers: true},
		schedulerDependencySources{buildLoop: func(context.Context, config.Config, *health.Registry) (lifecycle.Component, error) {
			return nil, errors.New("private factory failure")
		}},
	)
	if !errors.Is(err, errSchedulerActivationUnavailable) {
		t.Fatalf("failed private factory error = %v", err)
	}
}
