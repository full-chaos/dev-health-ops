package schedulerservice

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/full-chaos/dev-health-ops/internal/platform/poolstat"
	"github.com/full-chaos/dev-health-ops/internal/platform/selfprobe"
	schedulersync "github.com/full-chaos/dev-health-ops/internal/scheduler/sync"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/poolpg"
)

// CHAOS-6800 (lead D2588): readiness for the scheduler is checks on the
// dedicated READINESS pool only. Whether the shared WORK pool is fully acquired
// is contention: it is a metric and a log line, never a readiness input.

// probeDatabase is a fake whose readiness pool is separate from its work pool.
type probeDatabase struct {
	*fakeSchedulerDatabase
	probe *pgxpool.Pool
}

// DomainTransactionReady is the real primitive on the probe pool, exactly what
// the production database does on its readiness pool.
func (database *probeDatabase) DomainTransactionReady(ctx context.Context) error {
	return selfprobe.Once(ctx, selfprobe.NewPool(database.probe))
}

func newPoolpgPool(t *testing.T, server *poolpg.Server, maxConns int32) *pgxpool.Pool {
	t.Helper()
	config, err := pgxpool.ParseConfig("postgres://role:secret@" + server.Addr() + "/db?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	config.MaxConns = maxConns
	pool, err := pgxpool.NewWithConfig(context.Background(), config)
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

// buildWithPools composes the production scheduler builder around a work pool and
// a readiness pool and returns the real registry.
func buildWithPools(t *testing.T, work, probe *pgxpool.Pool, budget time.Duration) *health.Registry {
	return buildWithPoolsLogged(t, work, probe, budget, slog.Default())
}

func buildWithPoolsLogged(t *testing.T, work, probe *pgxpool.Pool, budget time.Duration, logger *slog.Logger) *health.Registry {
	t.Helper()
	ctx := context.Background()
	coordinatorPool, err := pgxpool.New(ctx, "postgresql://coordinator@127.0.0.1:1/devhealth")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(coordinatorPool.Close)
	database := &probeDatabase{
		fakeSchedulerDatabase: &fakeSchedulerDatabase{pool: work, coordinatorPool: coordinatorPool}, probe: probe,
	}
	sources := schedulerRuntimeSources{
		openDatabase: func(context.Context, config.Config) (schedulerDatabase, error) { return database, nil },
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
			return &fakeFixedLoop{}, nil
		},
	}
	registry := health.NewRegistry(budget)
	runtime, err := buildSchedulerLoopWithSources(ctx, config.Config{}, registry, sources, logger)
	if err != nil {
		t.Fatalf("buildSchedulerLoopWithSources() error = %v", err)
	}
	// Start the runtime and open the gate: the "replica stays ready" claims below
	// are about OVERALL readiness (r4 P3), not just the one check.
	if err := runtime.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(func() { _ = runtime.Shutdown(context.Background()) })
	if err := (health.Gate{Registry: registry}).Start(ctx); err != nil {
		t.Fatal(err)
	}
	return registry
}

// The executed proof (lead/scribe): a saturated WORK pool does not make the
// replica un-ready, and a wedged READINESS pool does.
func TestSaturatedWorkPoolDoesNotMakeTheSchedulerUnreadyAndAWedgedProbePoolDoes(t *testing.T) {
	work := newPoolpgPool(t, poolpg.Start(t), 2)
	healthy := poolpg.Start(t)
	probe := newPoolpgPool(t, healthy, 1)
	registry := buildWithPools(t, work, probe, 2*time.Second)
	if status := registry.Readiness(context.Background()); !status.Ready {
		t.Fatalf("the scheduler is not ready before the work pool is loaded: %v", status.Failed)
	}
	holdAll(t, work, 2) // every work connection is held by "jobs"

	started := time.Now()
	status := registry.Readiness(context.Background())
	if !status.Ready {
		t.Fatalf("a fully acquired WORK pool made the scheduler un-ready after %s: %v", time.Since(started), status.Failed)
	}
	var metrics bytes.Buffer
	if err := registry.WriteMetrics(&metrics); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(metrics.String(), `scheduler_database_pool_saturation_ratio{pool="domain"} 1`) {
		t.Fatalf("the saturated work pool is not visible as a metric: %q", metrics.String())
	}

	healthy.StallBegin(true) // the probe pool's database now accepts BEGIN and never answers
	status = registry.Readiness(context.Background())
	if !containsString(status.Failed, "domain_transaction") {
		t.Fatalf("a wedged READINESS pool did not fail domain_transaction: %v", status.Failed)
	}
}

// The check's whole state space, generated: the probe pool's outcome decides
// readiness and the work pool's state never does.
func TestDomainTransactionStateTable(t *testing.T) {
	type probeOutcome string
	outcomes := []probeOutcome{"ok", "acquire-deadline", "begin-deadline", "other-error"}
	for _, outcome := range outcomes {
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
				gotReady := !containsString(status.Failed, "domain_transaction")
				if wantReady := outcome == "ok"; gotReady != wantReady {
					t.Fatalf("%s: domain_transaction ready=%v, want %v (failed=%v)", name, gotReady, wantReady, status.Failed)
				}
				if outcome == "ok" && !status.Ready {
					t.Fatalf("%s: a healthy probe pool must leave the whole replica ready whatever state the work pool is in: %v", name, status.Failed)
				}
			})
		}
	}
}

// domain_postgres runs the CACHED posture check when the database offers one,
// and only falls back to DomainReady when it does not.
func TestDomainPostgresRunsTheCachedPostureCheckWhenOffered(t *testing.T) {
	build := func(t *testing.T, check health.CheckFunc) (*fakeSchedulerDatabase, *health.Registry) {
		t.Helper()
		ctx := context.Background()
		fake := &fakeSchedulerDatabase{}
		fake.pool, _ = pgxpool.New(ctx, "postgresql://domain@127.0.0.1:1/devhealth")
		fake.coordinatorPool, _ = pgxpool.New(ctx, "postgresql://coordinator@127.0.0.1:1/devhealth")
		t.Cleanup(fake.pool.Close)
		t.Cleanup(fake.coordinatorPool.Close)
		database := &cachedPostureDatabase{fakeSchedulerDatabase: fake, check: check}
		sources := schedulerRuntimeSources{
			openDatabase: func(context.Context, config.Config) (schedulerDatabase, error) { return database, nil },
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
				return &fakeFixedLoop{}, nil
			},
		}
		registry := health.NewRegistry(readinessTestCheckTimeout)
		if _, err := buildSchedulerLoopWithSources(ctx, config.Config{}, registry, sources, slog.Default()); err != nil {
			t.Fatalf("buildSchedulerLoopWithSources() error = %v", err)
		}
		return fake, registry
	}
	t.Run("provider check used", func(t *testing.T) {
		fake, registry := build(t, func(context.Context) error { return fmt.Errorf("cached posture refused") })
		status := registry.CheckRequired(context.Background())
		if !containsString(status.Failed, "domain_postgres") {
			t.Fatalf("domain_postgres ignored the provider's cached check: failed=%v", status.Failed)
		}
		if fake.domainCalls.Load() != 0 {
			t.Fatalf("DomainReady ran %d times although a cached check was offered", fake.domainCalls.Load())
		}
	})
	t.Run("no provider check falls back to DomainReady", func(t *testing.T) {
		fake, registry := build(t, nil)
		status := registry.CheckRequired(context.Background())
		if containsString(status.Failed, "domain_postgres") {
			t.Fatalf("the fallback DomainReady (healthy) failed: %v", status.Failed)
		}
		if fake.domainCalls.Load() == 0 {
			t.Fatal("DomainReady was not consulted when no cached check was offered")
		}
	})
}

// cachedPostureDatabase is a fake that offers a cached domain posture check.
type cachedPostureDatabase struct {
	*fakeSchedulerDatabase
	check health.CheckFunc
}

func (database *cachedPostureDatabase) DomainPostureCheck(*slog.Logger) health.CheckFunc {
	return database.check
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

// An unconfigured scheduler (no database DSN) has no readiness pool to probe: it
// stays live and reports every readiness name unavailable, domain_transaction
// included, instead of silently omitting the name a configured one carries.
func TestUnconfiguredSchedulerReportsDomainTransactionUnavailable(t *testing.T) {
	registry := health.NewRegistry(readinessTestCheckTimeout)
	sources := schedulerRuntimeSources{
		openDatabase: func(context.Context, config.Config) (schedulerDatabase, error) {
			return nil, postgres.ErrDomainDatabaseRequired
		},
		newRepository: func(*pgxpool.Pool) (schedulersync.HandoffStepper, error) {
			return nil, errors.New("not reached: the database is unconfigured")
		},
		newCoordinator: schedulersync.NewOccurrenceCoordinator,
		newLoop:        schedulersync.NewLoop,
		newOccurrences: stubOccurrenceSource,
		newFixedLoop: func(*pgxpool.Pool, *health.Registry, *slog.Logger) (fixedScheduleRuntime, error) {
			return &fakeFixedLoop{}, nil
		},
	}
	if _, err := buildSchedulerLoopWithSources(context.Background(), config.Config{}, registry, sources, slog.Default()); !errors.Is(err, errSchedulerDatabaseUnconfigured) {
		t.Fatalf("buildSchedulerLoopWithSources() error = %v, want errSchedulerDatabaseUnconfigured", err)
	}
	if err := (health.Gate{Registry: registry}).Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	status := registry.Readiness(context.Background())
	for _, name := range []string{"domain_postgres", "domain_transaction", "posture_manifest_lockstep", "scheduler_loop"} {
		if !containsString(status.Failed, name) {
			t.Fatalf("unconfigured scheduler does not report %s unavailable: %v", name, status.Failed)
		}
	}
	if containsString(status.Failed, "execution_liveness") {
		t.Fatalf("the removed execution_liveness name is still reported: %v", status.Failed)
	}
}

type syncBuffer struct {
	mu     sync.Mutex
	buffer bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.String()
}

// r4 P1: a running scheduler warns about a saturated work pool on its own
// sampler, with no metrics scrape at all.
func TestSchedulerWarnsAboutASaturatedWorkPoolWithoutAScrape(t *testing.T) {
	previous := poolstat.SampleInterval
	poolstat.SampleInterval = 10 * time.Millisecond
	t.Cleanup(func() { poolstat.SampleInterval = previous })
	work := newPoolpgPool(t, poolpg.Start(t), 2)
	probe := newPoolpgPool(t, poolpg.Start(t), 1)
	holdAll(t, work, 2)
	var logs syncBuffer
	buildWithPoolsLogged(t, work, probe, 2*time.Second, slog.New(slog.NewTextHandler(&logs, nil)))
	deadline := time.Now().Add(5 * time.Second)
	for !strings.Contains(logs.String(), "readiness does not depend on it") {
		if time.Now().After(deadline) {
			t.Fatal("a saturated work pool produced no warning although nothing scraped the metrics")
		}
		time.Sleep(5 * time.Millisecond)
	}
}
