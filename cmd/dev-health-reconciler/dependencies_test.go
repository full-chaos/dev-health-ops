package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
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

func TestProductionMutationPipelineConstructsTerminalDeliveryRepair(t *testing.T) {
	source, err := os.ReadFile("dependencies.go")
	if err != nil {
		t.Fatal(err)
	}
	text := string(source)
	if strings.Count(text, "syncreconciler.NewTerminalDeliveryRepair(queuePool, riverSchema)") != 1 {
		t.Fatal("production reconciler does not construct the queue-side terminal delivery repair")
	}
	if !strings.Contains(text, "repair,\n\t\tterminalRepair,\n\t\tmaterializer,") {
		t.Fatal("production mutation pipeline does not run terminal delivery recovery before materialization")
	}
}

func TestProductionGenericRelayConstructsBothRecoverySeams(t *testing.T) {
	source, err := os.ReadFile("dependencies.go")
	if err != nil {
		t.Fatal(err)
	}
	text := string(source)
	if strings.Count(text, "joboutbox.NewTerminalDeliveryRepair(queuePool, riverSchema)") != 1 {
		t.Fatal("production generic relay does not construct provider-unit terminal delivery repair")
	}
	// CHAOS-3997 added a second recovery seam. It is asserted separately from
	// the first because the two repair different things -- a delivery River
	// threw away, versus a domain row that says the work never finished -- and
	// a relay wired with only one of them would still satisfy the original
	// single-seam assertion while leaving every stranded run unhealed.
	//
	// The argument ORDER is load-bearing, not cosmetic: the queue pool selects
	// and rearms, the domain pool reads execution state. Swapping them would
	// compile and then fail at runtime as a 42501 -- the queue role is not
	// granted worker_job_runs, and must never be. Pinning the exact call is how
	// a wrong-pool wiring is caught here rather than in production.
	if strings.Count(text, "joboutbox.NewStrandRepair(queuePool, domainPool, riverSchema)") != 1 {
		t.Fatal("production generic relay does not construct the strand repair with the " +
			"queue pool for mutation and the domain pool for execution state")
	}
	// The domain pool must actually be threaded in rather than discarded. It
	// was previously dropped here with a blank assignment, and re-adding that
	// would silently make the pool split a fiction.
	if strings.Contains(text, "_ = domainPool") {
		t.Fatal("buildReconcilerRelay discards domainPool; the strand repair's execution-state " +
			"read has no pool to run on")
	}
	if strings.Count(text, "joboutbox.NewRelayWithRoutesRecoveryAndStrandRepair(") != 1 {
		t.Fatal("production generic relay does not run recovery before ordinary relay")
	}
	// The narrower constructors would drop a seam silently: both compile, and
	// both produce a working relay that simply never repairs.
	for _, superseded := range []string{
		"joboutbox.NewRelayWithRoutesAndRecovery(",
		"joboutbox.NewRelayWithRoutes(",
		"joboutbox.NewRelay(",
	} {
		if strings.Contains(text, superseded) {
			t.Fatalf("production generic relay still uses %s, which omits a recovery seam", superseded)
		}
	}
}

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

	want := []string{"coordinator_postgres", "domain_postgres", "execution_liveness", "job_registry", "queue_postgres", "reconciler_loop", "river_schema", "sync_dispatch_observer", "sync_dispatch_registry"}
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
	// A real (lazily-dialed, never-connecting) domain pool wires the
	// CHAOS-4029 execution_liveness self-probe into the composed component
	// list -- otherwise (a nil pool, every other reconciler test's fixture
	// shape) the check is never constructed at all, which would let this
	// test's readiness assertion pass for the wrong reason: not because the
	// signal works, but because it was never wired. 127.0.0.1:1 has nothing
	// listening, so the probe genuinely fails, exactly like a real reconciler
	// whose domain pool cannot open a transaction; that failure is asserted
	// below, not hidden.
	domainPool, err := pgxpool.New(context.Background(), "postgresql://reconciler@127.0.0.1:1/devhealth")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(domainPool.Close)
	database := &fakeReconcilerDatabase{domainPool: domainPool}
	calls := 0
	syncCalls := 0
	mutationBuilds := 0
	sources := reconcilerSourcesForTest(t, database)
	sources.buildRelay = func(*pgxpool.Pool, *pgxpool.Pool, *pgxpool.Pool, string, *jobruntime.Registry) (joboutbox.RelayStepper, error) {
		return reconcilerStepFunc(func(context.Context, time.Time, int) (joboutbox.StepResult, error) {
			calls++
			return joboutbox.StepResult{}, nil
		}), nil
	}
	sources.buildSyncMutation = func(
		*pgxpool.Pool, *pgxpool.Pool, *pgxpool.Pool, string,
		*syncdispatchcontract.Registry, config.Config, *health.Registry,
	) (syncreconciler.Stepper, error) {
		mutationBuilds++
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
	if got := componentNames(components); !slices.Equal(got, []string{
		"postgres-runtime-pools", "outbox-reconciler-loop", "sync-dispatch-observation-recorder",
		"sync-dispatch-observer-loop", "self-probe-reconciler_execution_liveness",
	}) {
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
	if mutationBuilds != 1 {
		t.Fatalf("sync mutation builds = %d, want 1", mutationBuilds)
	}
	if err := (health.Gate{Registry: registry}).Start(context.Background()); err != nil {
		t.Fatalf("open readiness gate: %v", err)
	}
	// execution_liveness genuinely fails here: domainPool above dials
	// 127.0.0.1:1, where nothing listens, so this asserts the real behavior
	// of a reconciler whose domain pool cannot open a transaction, not a
	// fixture artifact -- every OTHER check (including reconciler_loop and
	// sync_dispatch_observer, which this test's fake builders drive) is
	// still fully ready.
	if status := registry.Readiness(context.Background()); status.Ready || !slices.Equal(status.Failed, []string{"execution_liveness"}) {
		t.Fatalf("readiness = %#v, want only execution_liveness failed", status)
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

// TestSyncObservationTimeoutPropagatesFromConfig pins CHAOS-4092's wiring:
// cfg.SyncObservationTimeout, not the computed default, decides the observer
// loop's per-step budget when the operator set an override, and a bare
// config.Config{} (every other reconciler test's shape, and what a caller who
// never wires the option would produce) leaves the CHAOS-4239 composed
// default standing rather than tripping LoopConfig.validate's >= 10ms floor
// with a zero value. That composed default is
// syncreconciler.DefaultStageBudgets().Sum() (3.75s) plus
// stageBudgetOuterEnvelopeMargin (250ms) = 4s -- no longer
// DefaultLoopConfig's flat 2s, which described a single bounded Stepper call
// and was never sized for the 7-stage mutation pipeline (see the doc comment
// on DefaultLoopConfig and on the ObservationTimeout override below).
func TestSyncObservationTimeoutPropagatesFromConfig(t *testing.T) {
	t.Chdir(filepath.Join("..", ".."))

	newSources := func(t *testing.T, captured *time.Duration) reconcilerDependencySources {
		t.Helper()
		sources := reconcilerSourcesForTest(t, &fakeReconcilerDatabase{})
		// buildReconcilerDependencies returns no components (and no error) at
		// all when the outbox relay loop is nil -- reconcilerSourcesForTest
		// does not stub buildRelay, so this closes that gap the same way
		// TestReconcilerComposesNoopLoopInDatabaseThenLoopOrder does.
		sources.buildRelay = func(*pgxpool.Pool, *pgxpool.Pool, *pgxpool.Pool, string, *jobruntime.Registry) (joboutbox.RelayStepper, error) {
			return reconcilerStepFunc(func(context.Context, time.Time, int) (joboutbox.StepResult, error) {
				return joboutbox.StepResult{}, nil
			}), nil
		}
		sources.newSyncLoop = func(
			stepper syncreconciler.Stepper, loopConfig syncreconciler.LoopConfig,
		) (*syncreconciler.Loop, error) {
			*captured = loopConfig.ObservationTimeout
			return syncreconciler.NewLoop(stepper, loopConfig)
		}
		return sources
	}

	t.Run("override reaches the loop", func(t *testing.T) {
		cfg := reconcilerProductionShapedConfig(t)
		cfg.SyncObservationTimeout = 7 * time.Second
		// A hand-built override on a config.Config{} value must also flip
		// the bool: SyncObservationTimeoutExplicit, not the value, is what
		// dependencies.go now trusts (chris's ruling, CHAOS-4239 round 2).
		cfg.SyncObservationTimeoutExplicit = true
		var captured time.Duration
		_, err := configureReconcilerDependenciesWithSourcesAndLogger(
			context.Background(), cfg, health.NewRegistry(100*time.Millisecond),
			reconcilerTestLogger(), newSources(t, &captured),
		)
		if err != nil {
			t.Fatal(err)
		}
		if captured != 7*time.Second {
			t.Fatalf("ObservationTimeout = %s, want the configured 7s", captured)
		}
	})

	// The three subtests below drive config.Load itself (not a hand-built
	// config.Config{}) with a controlled SYNC_OBSERVATION_TIMEOUT lookup, per
	// chris's CHAOS-4239 readiness/precedence ruling: (a) Load with the
	// variable unset must still compose the 4s stage-budget envelope: (b) an
	// operator who explicitly sets it to exactly 2s -- the one value
	// indistinguishable from Load's own fallback by VALUE alone -- must still
	// have it honored, with a WARN that it undercuts the composed budget; (c)
	// a value clearly above the composed envelope is honored with no warning.

	t.Run("Load with the variable unset composes the stage-budget envelope", func(t *testing.T) {
		cfg := reconcilerProductionShapedConfig(t)
		if cfg.SyncObservationTimeoutExplicit {
			t.Fatal("reconcilerProductionShapedConfig sets no environment; SyncObservationTimeoutExplicit must be false")
		}
		var captured time.Duration
		_, err := configureReconcilerDependenciesWithSourcesAndLogger(
			context.Background(), cfg, health.NewRegistry(100*time.Millisecond),
			reconcilerTestLogger(), newSources(t, &captured),
		)
		if err != nil {
			t.Fatal(err)
		}
		want := syncreconciler.DefaultStageBudgets().Sum() + stageBudgetOuterEnvelopeMargin
		if captured != want {
			t.Fatalf("ObservationTimeout = %s, want the composed default %s", captured, want)
		}
	})

	t.Run("an explicit 2s is honored and warns it undercuts the composed envelope", func(t *testing.T) {
		cfg := reconcilerLoadedConfigWithSyncObservationTimeout(t, "2s")
		if !cfg.SyncObservationTimeoutExplicit || cfg.SyncObservationTimeout != 2*time.Second {
			t.Fatalf("test setup: SyncObservationTimeout=%s Set=%v, want 2s/true", cfg.SyncObservationTimeout, cfg.SyncObservationTimeoutExplicit)
		}
		var buf bytes.Buffer
		logger := slog.New(slog.NewJSONHandler(&buf, nil))
		var captured time.Duration
		_, err := configureReconcilerDependenciesWithSourcesAndLogger(
			context.Background(), cfg, health.NewRegistry(100*time.Millisecond),
			logger, newSources(t, &captured),
		)
		if err != nil {
			t.Fatal(err)
		}
		if captured != 2*time.Second {
			t.Fatalf("ObservationTimeout = %s, want the explicit 2s honored", captured)
		}
		if !strings.Contains(buf.String(), "sync_observation_timeout is below the composed") {
			t.Fatalf("expected a WARN that the explicit value undercuts the composed envelope; log:\n%s", buf.String())
		}
	})

	t.Run("an explicit 10s is honored without warning", func(t *testing.T) {
		cfg := reconcilerLoadedConfigWithSyncObservationTimeout(t, "10s")
		if !cfg.SyncObservationTimeoutExplicit || cfg.SyncObservationTimeout != 10*time.Second {
			t.Fatalf("test setup: SyncObservationTimeout=%s Set=%v, want 10s/true", cfg.SyncObservationTimeout, cfg.SyncObservationTimeoutExplicit)
		}
		var buf bytes.Buffer
		logger := slog.New(slog.NewJSONHandler(&buf, nil))
		var captured time.Duration
		_, err := configureReconcilerDependenciesWithSourcesAndLogger(
			context.Background(), cfg, health.NewRegistry(100*time.Millisecond),
			logger, newSources(t, &captured),
		)
		if err != nil {
			t.Fatal(err)
		}
		if captured != 10*time.Second {
			t.Fatalf("ObservationTimeout = %s, want the explicit 10s honored", captured)
		}
		if strings.Contains(buf.String(), "sync_observation_timeout is below the composed") {
			t.Fatalf("a value above the composed envelope must not warn; log:\n%s", buf.String())
		}
	})

	t.Run("bare config.Config{} keeps the package default", func(t *testing.T) {
		var captured time.Duration
		_, err := configureReconcilerDependenciesWithSourcesAndLogger(
			context.Background(), config.Config{RiverDatabaseSchema: "river"},
			health.NewRegistry(100*time.Millisecond),
			reconcilerTestLogger(), newSources(t, &captured),
		)
		if err != nil {
			t.Fatal(err)
		}
		want := syncreconciler.DefaultStageBudgets().Sum() + stageBudgetOuterEnvelopeMargin
		if captured != want {
			t.Fatalf("ObservationTimeout = %s, want the composed default %s (stage budgets sum + margin)", captured, want)
		}
	})

	// TestSyncObservationTimeoutPropagatesFromConfig/a_config.Load-shaped_config_still_gets_the_composed_default
	// pins the bug a codex review caught on CHAOS-4239: config.Load NEVER
	// leaves SyncObservationTimeout at Go's zero value for this service --
	// durationEnv falls back to config.DefaultSyncObservationTimeout (2s)
	// whenever SYNC_OBSERVATION_TIMEOUT is unset -- so every REAL deployment
	// carries a non-zero 2s here even when the operator configured nothing.
	// The "bare config.Config{}" subtest above cannot catch a regression of
	// this: it is the one shape config.Load can never actually produce.
	// Without the second comparison in dependencies.go, this exact
	// config.Load-shaped input silently clobbered the composed
	// syncreconciler.DefaultStageBudgets().Sum() envelope back down to the
	// flat 2s CHAOS-4239 exists to stop using, and the whole fix shipped
	// inert against the only input production ever actually sends it.
	t.Run("a config.Load-shaped config still gets the composed default", func(t *testing.T) {
		var captured time.Duration
		_, err := configureReconcilerDependenciesWithSourcesAndLogger(
			context.Background(),
			config.Config{
				RiverDatabaseSchema: "river",
				// This is exactly what config.Load produces for the
				// reconciler service when SYNC_OBSERVATION_TIMEOUT is unset
				// -- durationEnv's fallback, never Go's zero value.
				SyncObservationTimeout: config.DefaultSyncObservationTimeout,
			},
			health.NewRegistry(100*time.Millisecond),
			reconcilerTestLogger(), newSources(t, &captured),
		)
		if err != nil {
			t.Fatal(err)
		}
		want := syncreconciler.DefaultStageBudgets().Sum() + stageBudgetOuterEnvelopeMargin
		if captured != want {
			t.Fatalf("ObservationTimeout = %s, want the composed default %s: a config.Load-shaped "+
				"input (SyncObservationTimeout already at its own package default, not Go's zero) "+
				"must not be mistaken for an explicit operator override", captured, want)
		}
	})
}

func TestReconcilerMutationActivationSelectsReviewedMutationPipeline(t *testing.T) {
	t.Chdir(filepath.Join("..", ".."))
	database := &fakeReconcilerDatabase{}
	sources := reconcilerSourcesForTest(t, database)
	relayCalls := 0
	mutationBuilds := 0
	mutationCalls := 0
	sources.buildRelay = func(*pgxpool.Pool, *pgxpool.Pool, *pgxpool.Pool, string, *jobruntime.Registry) (joboutbox.RelayStepper, error) {
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
		*pgxpool.Pool,
		string,
		*syncdispatchcontract.Registry,
		config.Config,
		*health.Registry,
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

func TestSyncDispatchClaimPreservesDurableDeliveryAttempt(t *testing.T) {
	claim := syncDispatchClaimForTransport(syncreconciler.TransportClaim{
		ID: "10000000-0000-4000-8000-000000000001", Kind: "dispatch_sync_run",
		RouteGeneration: 4, Attempts: 7,
	})
	if claim.DeliveryAttempt != 7 || claim.RouteGeneration != 4 {
		t.Fatalf("transport claim = %#v, want attempt 7 at route generation 4", claim)
	}
}

func TestReconcilerNilLoggerFailsClosedBeforeRecorderConstruction(t *testing.T) {
	t.Chdir(filepath.Join("..", ".."))
	database := &fakeReconcilerDatabase{}
	sources := reconcilerSourcesForTest(t, database)
	sources.buildRelay = func(*pgxpool.Pool, *pgxpool.Pool, *pgxpool.Pool, string, *jobruntime.Registry) (joboutbox.RelayStepper, error) {
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
	sources.buildRelay = func(*pgxpool.Pool, *pgxpool.Pool, *pgxpool.Pool, string, *jobruntime.Registry) (joboutbox.RelayStepper, error) {
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
	sources.buildRelay = func(*pgxpool.Pool, *pgxpool.Pool, *pgxpool.Pool, string, *jobruntime.Registry) (joboutbox.RelayStepper, error) {
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
	sources.buildRelay = func(*pgxpool.Pool, *pgxpool.Pool, *pgxpool.Pool, string, *jobruntime.Registry) (joboutbox.RelayStepper, error) {
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
	want := []string{"coordinator_postgres", "domain_postgres", "execution_liveness", "queue_postgres", "reconciler_loop", "river_schema", "sync_dispatch_observer"}
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
	want := []string{"coordinator_postgres", "domain_postgres", "execution_liveness", "queue_postgres", "reconciler_loop", "river_schema", "sync_dispatch_observer", "sync_dispatch_registry"}
	if status := registry.Readiness(context.Background()); status.Ready || !slices.Equal(status.Failed, want) {
		t.Fatalf("readiness = %#v, want failed %v", status, want)
	}
}

func TestReconcilerSyncMutationBuildFailureClosesDatabaseAndFailsReadiness(t *testing.T) {
	t.Chdir(filepath.Join("..", ".."))
	database := &fakeReconcilerDatabase{}
	sources := reconcilerSourcesForTest(t, database)
	sources.buildSyncMutation = func(
		*pgxpool.Pool, *pgxpool.Pool, *pgxpool.Pool, string,
		*syncdispatchcontract.Registry, config.Config, *health.Registry,
	) (syncreconciler.Stepper, error) {
		return nil, errors.New("sync mutation construction failed")
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
	want := []string{"coordinator_postgres", "domain_postgres", "execution_liveness", "queue_postgres", "reconciler_loop", "river_schema", "sync_dispatch_observer"}
	if status := registry.Readiness(context.Background()); status.Ready || !slices.Equal(status.Failed, want) {
		t.Fatalf("readiness = %#v, want failed %v", status, want)
	}
}

func TestReconcilerReadinessRegistrationFailureClosesConstructedDatabase(t *testing.T) {
	t.Chdir(filepath.Join("..", ".."))
	database := &fakeReconcilerDatabase{}
	recorder := &fakeReconcilerRecorder{}
	sources := reconcilerSourcesForTest(t, database)
	sources.buildRelay = func(*pgxpool.Pool, *pgxpool.Pool, *pgxpool.Pool, string, *jobruntime.Registry) (joboutbox.RelayStepper, error) {
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
	// See TestReconcilerComposesNoopLoopInDatabaseThenLoopOrder's identical
	// comment: a real (never-connecting) domain pool is required to wire the
	// CHAOS-4029 execution_liveness check at all, and it genuinely fails
	// here since nothing listens on 127.0.0.1:1 -- asserted below alongside
	// the route-fence failure this test exists to isolate.
	domainPool, err := pgxpool.New(context.Background(), "postgresql://reconciler@127.0.0.1:1/devhealth")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(domainPool.Close)
	database := &fakeReconcilerDatabase{domainPool: domainPool}
	sources := reconcilerSourcesForTest(t, database)
	sources.buildRelay = func(*pgxpool.Pool, *pgxpool.Pool, *pgxpool.Pool, string, *jobruntime.Registry) (joboutbox.RelayStepper, error) {
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
	if status.Ready || !slices.Equal(status.Failed, []string{"execution_liveness", "sync_dispatch_route_fence"}) {
		t.Fatalf("readiness = %#v, want only execution_liveness and route fence failed", status)
	}
}

func TestReconcilerRouteFenceConstructionFailureFailsClosed(t *testing.T) {
	t.Chdir(filepath.Join("..", ".."))
	database := &fakeReconcilerDatabase{}
	sources := reconcilerSourcesForTest(t, database)
	sources.buildRelay = func(*pgxpool.Pool, *pgxpool.Pool, *pgxpool.Pool, string, *jobruntime.Registry) (joboutbox.RelayStepper, error) {
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
	want := []string{"coordinator_postgres", "domain_postgres", "execution_liveness", "queue_postgres", "reconciler_loop", "river_schema", "sync_dispatch_observer", "sync_dispatch_route_fence"}
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
	sources.buildSyncMutation = func(
		*pgxpool.Pool, *pgxpool.Pool, *pgxpool.Pool, string,
		*syncdispatchcontract.Registry, config.Config, *health.Registry,
	) (syncreconciler.Stepper, error) {
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

// reconcilerProductionShapedConfig is config.Load's own output for
// reconcilerSpec.Service with no environment variables set -- not a hand-built
// config.Config{} literal. Adversarial review found that a hand-picked subset
// of fields (even one including Service) still isn't what production
// computes: every field config.Load defaults to a non-empty value with no
// environment set (cfg.CoordinatorDatabaseRole, for instance) was left zero in
// a literal, unlike production. Using config.Load directly removes that gap
// for every field it can populate without a real secret being configured.
func reconcilerProductionShapedConfig(t *testing.T) config.Config {
	t.Helper()
	cfg, err := config.Load(config.Spec{
		Service:   reconcilerSpec.Service,
		LookupEnv: func(string) (string, bool) { return "", false },
	})
	if err != nil {
		t.Fatalf("loading a production-defaulted config for %q: %v", reconcilerSpec.Service, err)
	}
	return cfg
}

// reconcilerLoadedConfigWithSyncObservationTimeout is config.Load's own
// output with every environment variable unset EXCEPT
// SYNC_OBSERVATION_TIMEOUT, which is set to raw -- so
// cfg.SyncObservationTimeoutExplicit comes out true the same way a real operator
// setting the env var or --sync-observation-timeout flag would produce, not
// hand-set on a config.Config{} literal.
func reconcilerLoadedConfigWithSyncObservationTimeout(t *testing.T, raw string) config.Config {
	t.Helper()
	cfg, err := config.Load(config.Spec{
		Service: reconcilerSpec.Service,
		LookupEnv: func(key string) (string, bool) {
			if key == "SYNC_OBSERVATION_TIMEOUT" {
				return raw, true
			}
			return "", false
		},
	})
	if err != nil {
		t.Fatalf("loading a config with SYNC_OBSERVATION_TIMEOUT=%q for %q: %v", raw, reconcilerSpec.Service, err)
	}
	return cfg
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
	domainErr       error
	queueErr        error
	coordinatorErr  error
	schemaErr       error
	domainPool      *pgxpool.Pool
	queuePool       *pgxpool.Pool
	coordinatorPool *pgxpool.Pool
	closed          atomic.Bool
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

func (database *fakeReconcilerDatabase) CoordinatorReady(context.Context) error {
	return database.coordinatorErr
}

func (database *fakeReconcilerDatabase) CoordinatorPool() *pgxpool.Pool {
	return database.coordinatorPool
}

func (database *fakeReconcilerDatabase) Close() {
	database.closed.Store(true)
}
