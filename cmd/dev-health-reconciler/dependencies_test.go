package main

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/joboutbox"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/full-chaos/dev-health-ops/internal/platform/lifecycle"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestReconcilerMissingDependenciesStayLiveAndFailReadinessWithoutValues(t *testing.T) {
	secret := "postgresql://queue:do-not-print@database.internal/app"
	sources := productionReconcilerDependencySources
	sources.openDatabase = func(context.Context, config.Config) (reconcilerDatabase, error) {
		return nil, errors.New(secret)
	}
	sources.loadRuntimeRegistry = func(string) (*jobruntime.Registry, error) {
		return nil, errors.New("load " + secret)
	}

	registry := health.NewRegistry(100 * time.Millisecond)
	components, err := configureReconcilerDependenciesWithSources(
		context.Background(),
		config.Config{RiverDatabaseSchema: "river"},
		registry,
		sources,
	)
	if err != nil {
		t.Fatalf("configureReconcilerDependenciesWithSources() error = %v", err)
	}
	if len(components) != 0 {
		t.Fatalf("components = %d, want no lifecycle components", len(components))
	}
	if err := (health.Gate{Registry: registry}).Start(context.Background()); err != nil {
		t.Fatalf("open readiness gate: %v", err)
	}

	want := []string{"domain_postgres", "job_registry", "queue_postgres", "reconciler_loop", "river_schema"}
	status := registry.Readiness(context.Background())
	if status.Ready || !slices.Equal(status.Failed, want) {
		t.Fatalf("readiness = %#v, want failed %v", status, want)
	}
	if text := fmt.Sprint(status); strings.Contains(text, secret) || strings.Contains(text, "do-not-print") {
		t.Fatalf("readiness exposed dependency value: %s", text)
	}
}

func TestReconcilerComposesNoopLoopInDatabaseThenLoopOrder(t *testing.T) {
	t.Chdir(filepath.Join("..", ".."))
	database := &fakeReconcilerDatabase{}
	calls := 0
	sources := reconcilerSourcesForTest(t, database)
	sources.buildRelay = func(*pgxpool.Pool, string, *jobruntime.Registry) (joboutbox.RelayStepper, error) {
		return reconcilerStepFunc(func(context.Context, time.Time, int) (joboutbox.StepResult, error) {
			calls++
			return joboutbox.StepResult{}, nil
		}), nil
	}

	registry := health.NewRegistry(100 * time.Millisecond)
	components, err := configureReconcilerDependenciesWithSources(
		context.Background(),
		config.Config{RiverDatabaseSchema: "river"},
		registry,
		sources,
	)
	if err != nil {
		t.Fatalf("configureReconcilerDependenciesWithSources() error = %v", err)
	}
	if got := componentNames(components); !slices.Equal(got, []string{"postgres-runtime-pools", "outbox-reconciler-loop"}) {
		t.Fatalf("component order = %v", got)
	}
	for _, component := range components {
		if err := component.Start(context.Background()); err != nil {
			t.Fatalf("start %s: %v", component.Name(), err)
		}
	}
	if calls != 1 {
		t.Fatalf("immediate no-op relay calls = %d, want 1", calls)
	}
	if err := (health.Gate{Registry: registry}).Start(context.Background()); err != nil {
		t.Fatalf("open readiness gate: %v", err)
	}
	if status := registry.Readiness(context.Background()); !status.Ready {
		t.Fatalf("readiness = %#v, want ready", status)
	}
	for index := len(components) - 1; index >= 0; index-- {
		if err := components[index].Shutdown(context.Background()); err != nil {
			t.Fatalf("shutdown %s: %v", components[index].Name(), err)
		}
	}
	if !database.closed.Load() {
		t.Fatal("database lifecycle did not close pools")
	}
}

func TestReconcilerConstructionFailureClosesDatabaseAndFailsReadiness(t *testing.T) {
	t.Chdir(filepath.Join("..", ".."))
	database := &fakeReconcilerDatabase{}
	sources := reconcilerSourcesForTest(t, database)
	sources.buildRelay = func(*pgxpool.Pool, string, *jobruntime.Registry) (joboutbox.RelayStepper, error) {
		return nil, errors.New("dial postgresql://queue:do-not-print@database.internal/app")
	}

	registry := health.NewRegistry(100 * time.Millisecond)
	components, err := configureReconcilerDependenciesWithSources(
		context.Background(),
		config.Config{RiverDatabaseSchema: "river"},
		registry,
		sources,
	)
	if err != nil {
		t.Fatalf("configureReconcilerDependenciesWithSources() error = %v", err)
	}
	if len(components) != 0 {
		t.Fatalf("components = %d, want no partial runtime", len(components))
	}
	if !database.closed.Load() {
		t.Fatal("relay construction failure leaked runtime pools")
	}
	if err := (health.Gate{Registry: registry}).Start(context.Background()); err != nil {
		t.Fatalf("open readiness gate: %v", err)
	}
	want := []string{"domain_postgres", "queue_postgres", "reconciler_loop", "river_schema"}
	if status := registry.Readiness(context.Background()); status.Ready || !slices.Equal(status.Failed, want) {
		t.Fatalf("readiness = %#v, want failed %v", status, want)
	}
}

func TestReconcilerReadinessRegistrationFailureClosesConstructedDatabase(t *testing.T) {
	t.Chdir(filepath.Join("..", ".."))
	database := &fakeReconcilerDatabase{}
	sources := reconcilerSourcesForTest(t, database)
	sources.buildRelay = func(*pgxpool.Pool, string, *jobruntime.Registry) (joboutbox.RelayStepper, error) {
		return reconcilerStepFunc(func(context.Context, time.Time, int) (joboutbox.StepResult, error) {
			return joboutbox.StepResult{}, nil
		}), nil
	}
	registry := health.NewRegistry(100 * time.Millisecond)
	if err := registry.RegisterRequired("domain_postgres", func(context.Context) error { return nil }); err != nil {
		t.Fatalf("register collision: %v", err)
	}
	if _, err := configureReconcilerDependenciesWithSources(
		context.Background(),
		config.Config{RiverDatabaseSchema: "river"},
		registry,
		sources,
	); err == nil {
		t.Fatal("duplicate readiness registration unexpectedly succeeded")
	}
	if !database.closed.Load() {
		t.Fatal("readiness registration failure leaked runtime pools")
	}
}

func TestReconcilerPoolReadinessErrorsAreCollapsed(t *testing.T) {
	database := &fakeReconcilerDatabase{
		domainErr: errors.New("postgresql://domain:do-not-print@database.internal/app"),
		queueErr:  errors.New("postgresql://queue:do-not-print@database.internal/app"),
		schemaErr: errors.New("driver detail"),
	}
	dependencies := &reconcilerDependencies{database: database}
	if err := dependencies.domainReady(context.Background()); !errors.Is(err, errReconcilerDependencyUnavailable) {
		t.Fatalf("domainReady() error = %v", err)
	}
	if err := dependencies.queueReady(context.Background()); !errors.Is(err, errReconcilerDependencyUnavailable) {
		t.Fatalf("queueReady() error = %v", err)
	}
	if err := dependencies.riverSchemaReady("river")(context.Background()); !errors.Is(err, errReconcilerDependencyUnavailable) {
		t.Fatalf("riverSchemaReady() error = %v", err)
	}
}

func TestReconcilerRegistryReadinessIsExplicitAndValueFree(t *testing.T) {
	secret := "contracts/jobs/v1/postgresql://do-not-print"
	dependencies := &reconcilerDependencies{registryErr: errors.New(secret)}
	if err := dependencies.registryReady(context.Background()); !errors.Is(err, errReconcilerDependencyUnavailable) {
		t.Fatalf("registryReady() error = %v", err)
	} else if strings.Contains(err.Error(), secret) || strings.Contains(err.Error(), "do-not-print") {
		t.Fatalf("registry readiness exposed dependency value: %v", err)
	}
}

func reconcilerSourcesForTest(t *testing.T, database reconcilerDatabase) reconcilerDependencySources {
	t.Helper()
	sources := productionReconcilerDependencySources
	sources.openDatabase = func(context.Context, config.Config) (reconcilerDatabase, error) {
		return database, nil
	}
	sources.loadRuntimeRegistry = jobruntime.Load
	sources.contractRoot = "contracts/jobs/v1"
	return sources
}

func componentNames(components []lifecycle.Component) []string {
	names := make([]string, 0, len(components))
	for _, component := range components {
		names = append(names, component.Name())
	}
	return names
}

type reconcilerStepFunc func(context.Context, time.Time, int) (joboutbox.StepResult, error)

func (step reconcilerStepFunc) Step(ctx context.Context, now time.Time, limit int) (joboutbox.StepResult, error) {
	return step(ctx, now, limit)
}

type fakeReconcilerDatabase struct {
	domainErr error
	queueErr  error
	schemaErr error
	queuePool *pgxpool.Pool
	closed    atomic.Bool
}

func (database *fakeReconcilerDatabase) DomainReady(context.Context) error {
	return database.domainErr
}

func (database *fakeReconcilerDatabase) QueueReady(context.Context) error {
	return database.queueErr
}

func (database *fakeReconcilerDatabase) RiverSchemaReady(context.Context, string) error {
	return database.schemaErr
}

func (database *fakeReconcilerDatabase) QueuePool() *pgxpool.Pool {
	return database.queuePool
}

func (database *fakeReconcilerDatabase) Close() {
	database.closed.Store(true)
}
