package reconcilerservice

import (
	"context"
	"errors"
	"log/slog"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/joboutbox"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/full-chaos/dev-health-ops/internal/syncdispatchcontract"
	"github.com/full-chaos/dev-health-ops/internal/syncreconciler"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/poolpg"
)

// CHAOS-6771: the reconciler's execution_liveness self-probe runs on the domain
// pool; a pool fully acquired by progressing work is BUSY, not BROKEN. Real
// pgxpool connections (poolpg), the production composition and the real health
// registry: stale the monitor, hold every connection, sample once -- the sample
// passes as busy and readiness does not fail execution_liveness. Handing the
// monitor the raw opener fails this test.
func TestExecutionLivenessSamplePassesAsBusyOnAFullyAcquiredDomainPool(t *testing.T) {
	t.Chdir(filepath.Join("..", ".."))
	ctx := context.Background()
	addr := poolpg.Serve(t)
	poolConfig, err := pgxpool.ParseConfig("postgres://reconciler:secret@" + addr + "/devhealth?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	poolConfig.MaxConns = 2
	domainPool, err := pgxpool.NewWithConfig(ctx, poolConfig)
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
	sources.buildSyncMutation = func(
		*pgxpool.Pool, *pgxpool.Pool, *pgxpool.Pool, string,
		*syncdispatchcontract.Registry, config.Config, *health.Registry,
	) (syncreconciler.Stepper, error) {
		return syncStepFunc(func(context.Context, time.Time, int) (syncreconciler.Observation, error) {
			return syncreconciler.Observation{}, nil
		}), nil
	}
	previousWindow := busyProgressWindow
	busyProgressWindow = 300 * time.Millisecond
	t.Cleanup(func() { busyProgressWindow = previousWindow })
	registry := health.NewRegistry(readinessTestCheckTimeout)
	components, err := configureReconcilerDependenciesWithSourcesAndLogger(
		ctx, config.Config{RiverDatabaseSchema: "river"}, registry, reconcilerTestLogger(), sources,
	)
	if err != nil {
		t.Fatalf("configureReconcilerDependenciesWithSourcesAndLogger() error = %v", err)
	}
	var monitor interface {
		SetStaleness(time.Duration)
		Probe(context.Context)
	}
	for _, component := range components {
		if component.Name() == "self-probe-reconciler_execution_liveness" {
			monitor = component.(interface {
				SetStaleness(time.Duration)
				Probe(context.Context)
			})
		}
	}
	if monitor == nil {
		t.Fatalf("no execution-liveness monitor among %v", componentNames(components))
	}
	if err := (health.Gate{Registry: registry}).Start(ctx); err != nil {
		t.Fatal(err)
	}
	if status := registry.Readiness(ctx); slices.Contains(status.Failed, "execution_liveness") {
		t.Fatalf("the idle pool's admission sample failed: %v", status.Failed)
	}

	monitor.SetStaleness(50 * time.Millisecond)
	time.Sleep(120 * time.Millisecond) // the admission sample is now stale
	if status := registry.Readiness(ctx); !slices.Contains(status.Failed, "execution_liveness") {
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
	t.Cleanup(func() {
		for _, connection := range held {
			connection.Release()
		}
	})
	probeCtx, cancel := context.WithTimeout(ctx, 4*time.Second)
	defer cancel()
	monitor.Probe(probeCtx)
	if status := registry.Readiness(ctx); slices.Contains(status.Failed, "execution_liveness") {
		t.Fatalf("a busy sample left execution_liveness failed: %v", status.Failed)
	}
	// r3 P1: the busy pass refreshed the monitor's success, which the monitor
	// honours for its whole staleness allowance. Readiness must still go red once
	// the WORK evidence is older than the progress window (no work acquire since).
	monitor.SetStaleness(time.Minute) // the monitor alone would stay fresh for the whole test
	time.Sleep(2 * busyProgressWindow)
	if status := registry.Readiness(ctx); !slices.Contains(status.Failed, "execution_liveness") {
		t.Fatalf("a wedged pool stayed ready after the progress window: monitor freshness alone decided readiness (%v)", status.Failed)
	}
}

// cachedPostureDatabase is a fake that offers a cached domain posture check.
type cachedPostureDatabase struct {
	*fakeReconcilerDatabase
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
	t.Chdir(filepath.Join("..", ".."))
	// The composition whose only failing check is execution_liveness (a lazy,
	// never-connecting domain pool), so domain_postgres reflects the check under
	// test and nothing else.
	newSources := func(t *testing.T, database reconcilerDatabase) reconcilerDependencySources {
		t.Helper()
		sources := reconcilerSourcesForTest(t, database)
		sources.buildRelay = func(*pgxpool.Pool, *pgxpool.Pool, *pgxpool.Pool, string, *jobruntime.Registry) (joboutbox.RelayStepper, error) {
			return reconcilerStepFunc(func(context.Context, time.Time, int) (joboutbox.StepResult, error) {
				return joboutbox.StepResult{}, nil
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
		return sources
	}
	lazyPool := func(t *testing.T) *pgxpool.Pool {
		t.Helper()
		pool, err := pgxpool.New(context.Background(), "postgresql://reconciler@127.0.0.1:1/devhealth")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(pool.Close)
		return pool
	}
	readinessOf := func(t *testing.T, database reconcilerDatabase) []string {
		t.Helper()
		registry := health.NewRegistry(readinessTestCheckTimeout)
		if _, err := configureReconcilerDependenciesWithSourcesAndLogger(
			context.Background(), config.Config{RiverDatabaseSchema: "river"}, registry,
			reconcilerTestLogger(), newSources(t, database),
		); err != nil {
			t.Fatalf("configureReconcilerDependenciesWithSourcesAndLogger() error = %v", err)
		}
		if err := (health.Gate{Registry: registry}).Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		return registry.Readiness(context.Background()).Failed
	}
	t.Run("provider check used", func(t *testing.T) {
		fake := &fakeReconcilerDatabase{domainPool: lazyPool(t)} // DomainReady passes
		database := &cachedPostureDatabase{fakeReconcilerDatabase: fake, check: func(context.Context) error {
			return errors.New("cached posture refused")
		}}
		if failed := readinessOf(t, database); !slices.Contains(failed, "domain_postgres") {
			t.Fatalf("domain_postgres ignored the provider's cached check: failed=%v", failed)
		}
	})
	t.Run("healthy provider check leaves domain_postgres ready", func(t *testing.T) {
		fake := &fakeReconcilerDatabase{domainPool: lazyPool(t), domainErr: errors.New("DomainReady must not run")}
		database := &cachedPostureDatabase{fakeReconcilerDatabase: fake, check: func(context.Context) error { return nil }}
		if failed := readinessOf(t, database); slices.Contains(failed, "domain_postgres") {
			t.Fatalf("a healthy cached check still failed domain_postgres (DomainReady ran?): failed=%v", failed)
		}
	})
	t.Run("no provider check falls back to DomainReady", func(t *testing.T) {
		fake := &fakeReconcilerDatabase{domainPool: lazyPool(t), domainErr: errors.New("domain refused")}
		database := &cachedPostureDatabase{fakeReconcilerDatabase: fake, check: nil}
		if failed := readinessOf(t, database); !slices.Contains(failed, "domain_postgres") {
			t.Fatalf("the fallback DomainReady (refusing) did not fail domain_postgres: failed=%v", failed)
		}
	})
}
