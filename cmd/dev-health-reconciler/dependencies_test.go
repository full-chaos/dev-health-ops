package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
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
	"github.com/full-chaos/dev-health-ops/internal/syncdispatchcontract"
	"github.com/full-chaos/dev-health-ops/internal/syncreconciler"
	"github.com/full-chaos/dev-health-ops/internal/syncroute"
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
	components, err := configureReconcilerDependenciesWithSourcesAndLogger(
		context.Background(),
		config.Config{RiverDatabaseSchema: "river"},
		registry,
		reconcilerTestLogger(),
		sources,
	)
	if err != nil {
		t.Fatalf("configureReconcilerDependenciesWithSourcesAndLogger() error = %v", err)
	}
	if len(components) != 0 {
		t.Fatalf("components = %d, want no lifecycle components", len(components))
	}
	if err := (health.Gate{Registry: registry}).Start(context.Background()); err != nil {
		t.Fatalf("open readiness gate: %v", err)
	}

	want := []string{"domain_postgres", "job_registry", "queue_postgres", "reconciler_loop", "river_schema", "sync_dispatch_observer", "sync_dispatch_registry"}
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
	syncCalls := 0
	shadowBuilds := 0
	sources := reconcilerSourcesForTest(t, database)
	sources.buildRelay = func(*pgxpool.Pool, *pgxpool.Pool, string, *jobruntime.Registry) (joboutbox.RelayStepper, error) {
		return reconcilerStepFunc(func(context.Context, time.Time, int) (joboutbox.StepResult, error) {
			calls++
			return joboutbox.StepResult{}, nil
		}), nil
	}
	sources.buildSyncShadow = func(*pgxpool.Pool, *syncdispatchcontract.Registry) (syncreconciler.Stepper, error) {
		shadowBuilds++
		return syncStepFunc(func(context.Context, time.Time, int) (syncreconciler.Observation, error) {
			syncCalls++
			return syncreconciler.Observation{}, nil
		}), nil
	}
	sources.newSyncLoop = func(stepper syncreconciler.Stepper, loopConfig syncreconciler.LoopConfig) (*syncreconciler.Loop, error) {
		if loopConfig.Recorder == nil {
			t.Fatal("sync loop did not receive the command-owned recorder")
		}
		return syncreconciler.NewLoop(stepper, loopConfig)
	}

	registry := health.NewRegistry(100 * time.Millisecond)
	components, err := configureReconcilerDependenciesWithSourcesAndLogger(
		context.Background(),
		config.Config{RiverDatabaseSchema: "river"},
		registry,
		reconcilerTestLogger(),
		sources,
	)
	if err != nil {
		t.Fatalf("configureReconcilerDependenciesWithSourcesAndLogger() error = %v", err)
	}
	if got := componentNames(components); !slices.Equal(got, []string{"postgres-runtime-pools", "outbox-reconciler-loop", "sync-dispatch-observation-recorder", "sync-dispatch-observer-loop"}) {
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
	if syncCalls != 1 {
		t.Fatalf("immediate sync observer calls = %d, want 1", syncCalls)
	}
	if shadowBuilds != 1 {
		t.Fatalf("sync shadow builds = %d, want 1", shadowBuilds)
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

func TestReconcilerMutationActivationSelectsReviewedMutationPipeline(t *testing.T) {
	t.Chdir(filepath.Join("..", ".."))
	database := &fakeReconcilerDatabase{}
	sources := reconcilerSourcesForTest(t, database)
	relayCalls := 0
	mutationBuilds := 0
	mutationCalls := 0
	sources.buildRelay = func(*pgxpool.Pool, *pgxpool.Pool, string, *jobruntime.Registry) (joboutbox.RelayStepper, error) {
		return reconcilerStepFunc(func(context.Context, time.Time, int) (joboutbox.StepResult, error) {
			relayCalls++
			return joboutbox.StepResult{}, nil
		}), nil
	}
	sources.buildSyncShadow = func(*pgxpool.Pool, *syncdispatchcontract.Registry) (syncreconciler.Stepper, error) {
		t.Fatal("reviewed mutation activation constructed the shadow stepper")
		return nil, nil
	}
	sources.buildSyncMutation = func(
		*pgxpool.Pool,
		*pgxpool.Pool,
		string,
		*syncdispatchcontract.Registry,
	) (syncreconciler.Stepper, error) {
		mutationBuilds++
		return syncStepFunc(func(context.Context, time.Time, int) (syncreconciler.Observation, error) {
			mutationCalls++
			return syncreconciler.Observation{}, nil
		}), nil
	}

	registry := health.NewRegistry(100 * time.Millisecond)
	components, err := configureReconcilerDependenciesWithActivationSourcesAndLogger(
		context.Background(),
		config.Config{RiverDatabaseSchema: "river"},
		registry,
		reconcilerTestLogger(),
		reconcilerActivation{syncMutation: true},
		sources,
	)
	if err != nil {
		t.Fatal(err)
	}
	if mutationBuilds != 1 {
		t.Fatalf("mutation builds = %d, want 1", mutationBuilds)
	}
	for _, component := range components {
		if err := component.Start(context.Background()); err != nil {
			t.Fatalf("start %s: %v", component.Name(), err)
		}
	}
	if relayCalls != 1 || mutationCalls != 1 {
		t.Fatalf("immediate calls relay=%d mutation=%d, want 1 each", relayCalls, mutationCalls)
	}
	for index := len(components) - 1; index >= 0; index-- {
		if err := components[index].Shutdown(context.Background()); err != nil {
			t.Fatalf("shutdown %s: %v", components[index].Name(), err)
		}
	}
}

func TestReconcilerNilLoggerFailsClosedBeforeRecorderConstruction(t *testing.T) {
	t.Chdir(filepath.Join("..", ".."))
	database := &fakeReconcilerDatabase{}
	sources := reconcilerSourcesForTest(t, database)
	sources.buildRelay = func(*pgxpool.Pool, *pgxpool.Pool, string, *jobruntime.Registry) (joboutbox.RelayStepper, error) {
		return reconcilerStepFunc(func(context.Context, time.Time, int) (joboutbox.StepResult, error) {
			return joboutbox.StepResult{}, nil
		}), nil
	}
	recorderConstructed := false
	sources.newSyncRecorder = func(*slog.Logger) (reconcilerObservationRecorder, error) {
		recorderConstructed = true
		return nil, errors.New("recorder must not be built without logger")
	}

	components, err := configureReconcilerDependenciesWithSourcesAndLogger(
		context.Background(),
		config.Config{RiverDatabaseSchema: "river"},
		health.NewRegistry(100*time.Millisecond),
		nil,
		sources,
	)
	if err != nil || len(components) != 0 || recorderConstructed || !database.closed.Load() {
		t.Fatalf(
			"nil logger components=%d err=%v recorder_constructed=%v database_closed=%v",
			len(components),
			err,
			recorderConstructed,
			database.closed.Load(),
		)
	}
}

func TestReconcilerSyncLoopConstructionFailureClosesRecorderBeforeDatabase(t *testing.T) {
	t.Chdir(filepath.Join("..", ".."))
	database := &fakeReconcilerDatabase{}
	recorder := &fakeReconcilerRecorder{}
	sources := reconcilerSourcesForTest(t, database)
	sources.buildRelay = func(*pgxpool.Pool, *pgxpool.Pool, string, *jobruntime.Registry) (joboutbox.RelayStepper, error) {
		return reconcilerStepFunc(func(context.Context, time.Time, int) (joboutbox.StepResult, error) {
			return joboutbox.StepResult{}, nil
		}), nil
	}
	sources.newSyncRecorder = func(*slog.Logger) (reconcilerObservationRecorder, error) {
		return recorder, nil
	}
	sources.newSyncLoop = func(syncreconciler.Stepper, syncreconciler.LoopConfig) (*syncreconciler.Loop, error) {
		return nil, errors.New("sync loop construction failed")
	}

	components, err := configureReconcilerDependenciesWithSourcesAndLogger(
		context.Background(),
		config.Config{RiverDatabaseSchema: "river"},
		health.NewRegistry(100*time.Millisecond),
		reconcilerTestLogger(),
		sources,
	)
	if err != nil || len(components) != 0 || !recorder.closed.Load() || !database.closed.Load() {
		t.Fatalf(
			"sync loop failure components=%d err=%v recorder_closed=%v database_closed=%v",
			len(components),
			err,
			recorder.closed.Load(),
			database.closed.Load(),
		)
	}
}

func TestReconcilerRecorderConstructionFailureClosesReturnedRecorderAndDatabase(t *testing.T) {
	t.Chdir(filepath.Join("..", ".."))
	database := &fakeReconcilerDatabase{}
	recorder := &fakeReconcilerRecorder{}
	sources := reconcilerSourcesForTest(t, database)
	sources.buildRelay = func(*pgxpool.Pool, *pgxpool.Pool, string, *jobruntime.Registry) (joboutbox.RelayStepper, error) {
		return reconcilerStepFunc(func(context.Context, time.Time, int) (joboutbox.StepResult, error) {
			return joboutbox.StepResult{}, nil
		}), nil
	}
	sources.newSyncRecorder = func(*slog.Logger) (reconcilerObservationRecorder, error) {
		return recorder, errors.New("recorder construction failed")
	}

	components, err := configureReconcilerDependenciesWithSourcesAndLogger(
		context.Background(),
		config.Config{RiverDatabaseSchema: "river"},
		health.NewRegistry(100*time.Millisecond),
		reconcilerTestLogger(),
		sources,
	)
	if err != nil || len(components) != 0 || !recorder.closed.Load() || !database.closed.Load() {
		t.Fatalf(
			"recorder construction failure components=%d err=%v recorder_closed=%v database_closed=%v",
			len(components),
			err,
			recorder.closed.Load(),
			database.closed.Load(),
		)
	}
}

func TestReconcilerConstructionFailureClosesDatabaseAndFailsReadiness(t *testing.T) {
	t.Chdir(filepath.Join("..", ".."))
	database := &fakeReconcilerDatabase{}
	sources := reconcilerSourcesForTest(t, database)
	sources.buildRelay = func(*pgxpool.Pool, *pgxpool.Pool, string, *jobruntime.Registry) (joboutbox.RelayStepper, error) {
		return nil, errors.New("dial postgresql://queue:do-not-print@database.internal/app")
	}

	registry := health.NewRegistry(100 * time.Millisecond)
	components, err := configureReconcilerDependenciesWithSourcesAndLogger(
		context.Background(),
		config.Config{RiverDatabaseSchema: "river"},
		registry,
		reconcilerTestLogger(),
		sources,
	)
	if err != nil {
		t.Fatalf("configureReconcilerDependenciesWithSourcesAndLogger() error = %v", err)
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
	want := []string{"domain_postgres", "queue_postgres", "reconciler_loop", "river_schema", "sync_dispatch_observer"}
	if status := registry.Readiness(context.Background()); status.Ready || !slices.Equal(status.Failed, want) {
		t.Fatalf("readiness = %#v, want failed %v", status, want)
	}
}

func TestReconcilerSyncRegistryLoadFailureClosesDatabaseAndFailsReadiness(t *testing.T) {
	t.Chdir(filepath.Join("..", ".."))
	database := &fakeReconcilerDatabase{}
	sources := reconcilerSourcesForTest(t, database)
	sources.loadSyncDispatchRegistry = func(string) (*syncdispatchcontract.Registry, error) {
		return nil, errors.New("invalid sync-dispatch contract")
	}

	registry := health.NewRegistry(100 * time.Millisecond)
	components, err := configureReconcilerDependenciesWithSourcesAndLogger(
		context.Background(),
		config.Config{RiverDatabaseSchema: "river"},
		registry,
		reconcilerTestLogger(),
		sources,
	)
	if err != nil {
		t.Fatalf("configureReconcilerDependenciesWithSourcesAndLogger() error = %v", err)
	}
	if len(components) != 0 || !database.closed.Load() {
		t.Fatalf("components = %d, database closed = %v", len(components), database.closed.Load())
	}
	if err := (health.Gate{Registry: registry}).Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	want := []string{"domain_postgres", "queue_postgres", "reconciler_loop", "river_schema", "sync_dispatch_observer", "sync_dispatch_registry"}
	if status := registry.Readiness(context.Background()); status.Ready || !slices.Equal(status.Failed, want) {
		t.Fatalf("readiness = %#v, want failed %v", status, want)
	}
}

func TestReconcilerSyncShadowBuildFailureClosesDatabaseAndFailsReadiness(t *testing.T) {
	t.Chdir(filepath.Join("..", ".."))
	database := &fakeReconcilerDatabase{}
	sources := reconcilerSourcesForTest(t, database)
	sources.buildSyncShadow = func(*pgxpool.Pool, *syncdispatchcontract.Registry) (syncreconciler.Stepper, error) {
		return nil, errors.New("sync shadow construction failed")
	}

	registry := health.NewRegistry(100 * time.Millisecond)
	components, err := configureReconcilerDependenciesWithSourcesAndLogger(
		context.Background(),
		config.Config{RiverDatabaseSchema: "river"},
		registry,
		reconcilerTestLogger(),
		sources,
	)
	if err != nil {
		t.Fatalf("configureReconcilerDependenciesWithSourcesAndLogger() error = %v", err)
	}
	if len(components) != 0 || !database.closed.Load() {
		t.Fatalf("components = %d, database closed = %v", len(components), database.closed.Load())
	}
	if err := (health.Gate{Registry: registry}).Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	want := []string{"domain_postgres", "queue_postgres", "reconciler_loop", "river_schema", "sync_dispatch_observer"}
	if status := registry.Readiness(context.Background()); status.Ready || !slices.Equal(status.Failed, want) {
		t.Fatalf("readiness = %#v, want failed %v", status, want)
	}
}

func TestReconcilerReadinessRegistrationFailureClosesConstructedDatabase(t *testing.T) {
	t.Chdir(filepath.Join("..", ".."))
	database := &fakeReconcilerDatabase{}
	recorder := &fakeReconcilerRecorder{}
	sources := reconcilerSourcesForTest(t, database)
	sources.buildRelay = func(*pgxpool.Pool, *pgxpool.Pool, string, *jobruntime.Registry) (joboutbox.RelayStepper, error) {
		return reconcilerStepFunc(func(context.Context, time.Time, int) (joboutbox.StepResult, error) {
			return joboutbox.StepResult{}, nil
		}), nil
	}
	sources.newSyncRecorder = func(*slog.Logger) (reconcilerObservationRecorder, error) {
		return recorder, nil
	}
	registry := health.NewRegistry(100 * time.Millisecond)
	if err := registry.RegisterRequired("domain_postgres", func(context.Context) error { return nil }); err != nil {
		t.Fatalf("register collision: %v", err)
	}
	if _, err := configureReconcilerDependenciesWithSourcesAndLogger(
		context.Background(),
		config.Config{RiverDatabaseSchema: "river"},
		registry,
		reconcilerTestLogger(),
		sources,
	); err == nil {
		t.Fatal("duplicate readiness registration unexpectedly succeeded")
	}
	if !recorder.closed.Load() || !database.closed.Load() {
		t.Fatalf(
			"readiness registration failure recorder_closed=%v database_closed=%v",
			recorder.closed.Load(),
			database.closed.Load(),
		)
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

func TestReconcilerRouteFenceDriftClosesOnlyRouteFenceReadiness(t *testing.T) {
	t.Chdir(filepath.Join("..", ".."))
	database := &fakeReconcilerDatabase{}
	sources := reconcilerSourcesForTest(t, database)
	sources.buildRelay = func(*pgxpool.Pool, *pgxpool.Pool, string, *jobruntime.Registry) (joboutbox.RelayStepper, error) {
		return reconcilerStepFunc(func(context.Context, time.Time, int) (joboutbox.StepResult, error) {
			return joboutbox.StepResult{}, nil
		}), nil
	}
	sources.buildSyncRouteFence = func(*pgxpool.Pool, *syncdispatchcontract.Registry) (syncroute.Checker, error) {
		return syncrouteCheckFunc(func(context.Context) error {
			return errors.New("transport route differs from the checked-in contract")
		}), nil
	}

	registry := health.NewRegistry(100 * time.Millisecond)
	components, err := configureReconcilerDependenciesWithSourcesAndLogger(
		context.Background(),
		config.Config{RiverDatabaseSchema: "river"},
		registry,
		reconcilerTestLogger(),
		sources,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(components) == 0 {
		t.Fatal("route drift must not prevent the observer from being composed")
	}
	for _, component := range components {
		if err := component.Start(context.Background()); err != nil {
			t.Fatalf("start %s: %v", component.Name(), err)
		}
	}
	t.Cleanup(func() {
		for index := len(components) - 1; index >= 0; index-- {
			if err := components[index].Shutdown(context.Background()); err != nil {
				t.Errorf("shutdown %s: %v", components[index].Name(), err)
			}
		}
	})
	if err := (health.Gate{Registry: registry}).Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	status := registry.Readiness(context.Background())
	if status.Ready || !slices.Equal(status.Failed, []string{"sync_dispatch_route_fence"}) {
		t.Fatalf("readiness = %#v, want only route fence failed", status)
	}
}

func TestReconcilerRouteFenceConstructionFailureFailsClosed(t *testing.T) {
	t.Chdir(filepath.Join("..", ".."))
	database := &fakeReconcilerDatabase{}
	sources := reconcilerSourcesForTest(t, database)
	sources.buildRelay = func(*pgxpool.Pool, *pgxpool.Pool, string, *jobruntime.Registry) (joboutbox.RelayStepper, error) {
		return reconcilerStepFunc(func(context.Context, time.Time, int) (joboutbox.StepResult, error) {
			return joboutbox.StepResult{}, nil
		}), nil
	}
	sources.buildSyncRouteFence = func(*pgxpool.Pool, *syncdispatchcontract.Registry) (syncroute.Checker, error) {
		return nil, errors.New("route fence construction failed")
	}

	registry := health.NewRegistry(100 * time.Millisecond)
	components, err := configureReconcilerDependenciesWithSourcesAndLogger(
		context.Background(),
		config.Config{RiverDatabaseSchema: "river"},
		registry,
		reconcilerTestLogger(),
		sources,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(components) != 0 || !database.closed.Load() {
		t.Fatalf("components=%d database_closed=%v, want fail closed", len(components), database.closed.Load())
	}
	if err := (health.Gate{Registry: registry}).Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	want := []string{"domain_postgres", "queue_postgres", "reconciler_loop", "river_schema", "sync_dispatch_observer", "sync_dispatch_route_fence"}
	if status := registry.Readiness(context.Background()); status.Ready || !slices.Equal(status.Failed, want) {
		t.Fatalf("readiness = %#v, want failed %v", status, want)
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
	sources.loadSyncDispatchRegistry = syncdispatchcontract.Load
	sources.syncDispatchContractRoot = "contracts/sync-dispatch/v1"
	sources.buildSyncRouteFence = func(*pgxpool.Pool, *syncdispatchcontract.Registry) (syncroute.Checker, error) {
		return syncrouteCheckFunc(func(context.Context) error { return nil }), nil
	}
	sources.buildSyncShadow = func(*pgxpool.Pool, *syncdispatchcontract.Registry) (syncreconciler.Stepper, error) {
		return syncStepFunc(func(context.Context, time.Time, int) (syncreconciler.Observation, error) {
			return syncreconciler.Observation{}, nil
		}), nil
	}
	sources.newSyncLoop = syncreconciler.NewLoop
	return sources
}

func reconcilerTestLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(io.Discard, nil))
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

type syncStepFunc func(context.Context, time.Time, int) (syncreconciler.Observation, error)

func (step syncStepFunc) Step(ctx context.Context, now time.Time, limit int) (syncreconciler.Observation, error) {
	return step(ctx, now, limit)
}

type syncrouteCheckFunc func(context.Context) error

func (check syncrouteCheckFunc) Check(ctx context.Context) error { return check(ctx) }

type fakeReconcilerRecorder struct {
	closed atomic.Bool
}

func (recorder *fakeReconcilerRecorder) TryRecord(syncreconciler.Observation) bool {
	return !recorder.closed.Load()
}

func (recorder *fakeReconcilerRecorder) Shutdown(context.Context) error {
	recorder.closed.Store(true)
	return nil
}

type fakeReconcilerDatabase struct {
	domainErr  error
	queueErr   error
	schemaErr  error
	domainPool *pgxpool.Pool
	queuePool  *pgxpool.Pool
	closed     atomic.Bool
}

func (database *fakeReconcilerDatabase) DomainPool() *pgxpool.Pool {
	return database.domainPool
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
