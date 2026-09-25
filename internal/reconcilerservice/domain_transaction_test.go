package reconcilerservice

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/joboutbox"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/full-chaos/dev-health-ops/internal/platform/selfprobe"
	"github.com/full-chaos/dev-health-ops/internal/syncdispatchcontract"
	"github.com/full-chaos/dev-health-ops/internal/syncreconciler"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/poolpg"
)

// CHAOS-6800 (lead D2588): readiness for the reconciler is checks on the
// dedicated READINESS pool only. Whether the shared WORK pool is fully acquired
// is contention: a metric and a log line, never a readiness input.

// probeDatabase is a fake whose readiness pool is separate from its work pool.
type probeDatabase struct {
	*fakeReconcilerDatabase
	probe *pgxpool.Pool
}

// DomainTransactionReady is the real primitive on the probe pool, exactly what
// the production database does on its readiness pool.
func (database *probeDatabase) DomainTransactionReady(ctx context.Context) error {
	return selfprobe.Once(ctx, selfprobe.NewPool(database.probe))
}

func newPoolpgPool(t *testing.T, server *poolpg.Server, maxConns int32) *pgxpool.Pool {
	t.Helper()
	poolConfig, err := pgxpool.ParseConfig("postgres://role:secret@" + server.Addr() + "/db?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	poolConfig.MaxConns = maxConns
	pool, err := pgxpool.NewWithConfig(context.Background(), poolConfig)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func holdAll(t *testing.T, pool *pgxpool.Pool, count int) {
	t.Helper()
	var held []*pgxpool.Conn
	for i := 0; i < count; i++ {
		connection, err := pool.Acquire(context.Background())
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
}

// buildWithPools composes the production reconciler builder around a work pool
// and a readiness pool and returns the real, opened registry.
func buildWithPools(t *testing.T, work, probe *pgxpool.Pool, budget time.Duration) *health.Registry {
	t.Helper()
	t.Chdir(filepath.Join("..", ".."))
	database := &probeDatabase{fakeReconcilerDatabase: &fakeReconcilerDatabase{domainPool: work}, probe: probe}
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
	registry := health.NewRegistry(budget)
	components, err := configureReconcilerDependenciesWithSourcesAndLogger(
		context.Background(), config.Config{RiverDatabaseSchema: "river"}, registry, reconcilerTestLogger(), sources,
	)
	if err != nil {
		t.Fatalf("configureReconcilerDependenciesWithSourcesAndLogger() error = %v", err)
	}
	for _, component := range components {
		if err := component.Start(context.Background()); err != nil {
			t.Fatalf("start %s: %v", component.Name(), err)
		}
	}
	t.Cleanup(func() {
		for index := len(components) - 1; index >= 0; index-- {
			_ = components[index].Shutdown(context.Background())
		}
	})
	if err := (health.Gate{Registry: registry}).Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	return registry
}

// The executed proof (lead/scribe): a saturated WORK pool does not make the
// replica un-ready, and a wedged READINESS pool does.
func TestSaturatedWorkPoolDoesNotMakeTheReconcilerUnreadyAndAWedgedProbePoolDoes(t *testing.T) {
	work := newPoolpgPool(t, poolpg.Start(t), 2)
	healthy := poolpg.Start(t)
	probe := newPoolpgPool(t, healthy, 1)
	registry := buildWithPools(t, work, probe, 2*time.Second)
	holdAll(t, work, 2) // every work connection is held by "jobs"

	status := registry.Readiness(context.Background())
	if !status.Ready {
		t.Fatalf("a fully acquired WORK pool made the reconciler un-ready: %v", status.Failed)
	}
	var metrics bytes.Buffer
	if err := registry.WriteMetrics(&metrics); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(metrics.String(), `reconciler_database_pool_saturation_ratio{pool="domain"} 1`) {
		t.Fatalf("the saturated work pool is not visible as a metric: %q", metrics.String())
	}

	healthy.StallBegin(true) // the probe pool's database now accepts BEGIN and never answers
	status = registry.Readiness(context.Background())
	if !slices.Contains(status.Failed, "domain_transaction") {
		t.Fatalf("a wedged READINESS pool did not fail domain_transaction: %v", status.Failed)
	}
}

// The check's whole state space, generated: the probe pool's outcome decides
// readiness and the work pool's state never does.
func TestDomainTransactionStateTable(t *testing.T) {
	for _, outcome := range []string{"ok", "acquire-deadline", "begin-deadline", "other-error"} {
		for _, workState := range []string{"free", "saturated", "wedged"} {
			name := fmt.Sprintf("probe-%s/work-%s", outcome, workState)
			t.Run(name, func(t *testing.T) {
				workServer := poolpg.Start(t)
				work := newPoolpgPool(t, workServer, 2)
				server := poolpg.Start(t)
				probe := newPoolpgPool(t, server, 1)
				switch outcome {
				case "acquire-deadline":
					holdAll(t, probe, 1) // the probe pool's only connection is taken
				case "begin-deadline":
					server.StallBegin(true)
				case "other-error":
					probe.Close() // acquire fails at once, not by deadline
				}
				registry := buildWithPools(t, work, probe, time.Second)
				switch workState {
				case "saturated":
					holdAll(t, work, 2)
				case "wedged": // fully held AND its database no longer answers BEGIN
					workServer.StallBegin(true)
					holdAll(t, work, 2)
				}
				status := registry.Readiness(context.Background())
				gotReady := !slices.Contains(status.Failed, "domain_transaction")
				if wantReady := outcome == "ok"; gotReady != wantReady {
					t.Fatalf("%s: domain_transaction ready=%v, want %v (failed=%v)", name, gotReady, wantReady, status.Failed)
				}
				if workState != "free" && outcome == "ok" && !status.Ready {
					t.Fatalf("%s: a healthy probe pool must leave the replica ready whatever state the work pool is in: %v", name, status.Failed)
				}
			})
		}
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

// domain_postgres runs the CACHED posture check when the database offers one,
// and only falls back to DomainReady when it does not.
func TestDomainPostgresRunsTheCachedPostureCheckWhenOffered(t *testing.T) {
	t.Chdir(filepath.Join("..", ".."))
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
