package reconcilerservice

import (
	"context"
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
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakepg"
)

// CHAOS-6771: the reconciler's execution_liveness self-probe runs on the domain
// pool; a pool fully acquired by progressing work is BUSY, not BROKEN. Real
// pgxpool connections (fakepg), the production composition and the real health
// registry: stale the monitor, hold every connection, sample once -- the sample
// passes as busy and readiness does not fail execution_liveness. Handing the
// monitor the raw opener fails this test.
func TestExecutionLivenessSamplePassesAsBusyOnAFullyAcquiredDomainPool(t *testing.T) {
	t.Chdir(filepath.Join("..", ".."))
	ctx := context.Background()
	addr := fakepg.Serve(t)
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
}
