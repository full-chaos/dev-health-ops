//go:build integration

package syncdispatchruntime

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/cacheinvalidation"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	valkeystore "github.com/full-chaos/dev-health-ops/internal/storage/valkey"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5/pgxpool"
	valkeygo "github.com/valkey-io/valkey-go"
)

// coverageCacheInvalidationCount reads one series of the emitted/consumed
// pair back from the collector's exposition text, the same way an operator's
// scrape (and the alert on emitted - consumed) would.
func coverageCacheInvalidationCount(t *testing.T, collector *jobruntime.MetricsCollector, series, provider string) uint64 {
	t.Helper()
	prefix := fmt.Sprintf(`devhealth_sync_coverage_cache_invalidations_%s_total{provider=%q} `, series, provider)
	for _, line := range strings.Split(collector.PrometheusText(), "\n") {
		if strings.HasPrefix(line, prefix) {
			var value uint64
			if _, err := fmt.Sscanf(strings.TrimPrefix(line, prefix), "%d", &value); err != nil {
				t.Fatalf("parse counter line %q: %v", line, err)
			}
			return value
		}
	}
	return 0
}

func seedSuccessfulUnitRun(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
INSERT INTO sync_runs (id,org_id,integration_id,status,total_units,completed_units,failed_units)
VALUES ($1,$2,$3,'dispatching',1,0,0)`, finalizeTestRun, finalizeTestOrg, finalizeTestIntegration); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO integration_sources (id) VALUES ($1)`, finalizeTestSource); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO sync_run_units (id,org_id,sync_run_id,provider,dataset_key,source_id,status,since_at,before_at,cost_class,mode)
VALUES ($1,$2,$3,'github','commits',$4,'success',$5,$6,'heavy','incremental')`,
		finalizeTestUnit, finalizeTestOrg, finalizeTestRun, finalizeTestSource,
		time.Date(2026, 8, 22, 0, 0, 0, 0, time.UTC), time.Date(2026, 8, 23, 0, 0, 0, 0, time.UTC)); err != nil {
		t.Fatal(err)
	}
}

func startFinalizePostgres(t *testing.T, ctx context.Context) *pgxpool.Pool {
	t.Helper()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	createFinalizeTables(t, ctx, pool)
	seedFinalizeRoute(t, ctx, pool)
	seedSuccessfulUnitRun(t, ctx, pool)
	return pool
}

func startFinalizeValkey(t *testing.T, ctx context.Context) valkeygo.Client {
	t.Helper()
	instance, err := containers.StartValkey(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		_ = instance.Close(closeCtx)
	})
	client, err := valkeystore.Open(ctx, valkeystore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	return client
}

// TestNativeFinalizeSyncRunBumpsTheCoverageCacheEpochOnce is the CHAOS-4226
// red-first test: a finalized run must leave the org's cache epoch bumped in
// Valkey (the key the Python home read folds into its cache key), exactly
// once per run -- a re-finalize hits the once-only ledger and must neither
// bump again nor re-emit telemetry.
func TestNativeFinalizeSyncRunBumpsTheCoverageCacheEpochOnce(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	pool := startFinalizePostgres(t, ctx)
	client := startFinalizeValkey(t, ctx)
	invalidator, err := cacheinvalidation.NewValkeyOrgCacheInvalidator(client)
	if err != nil {
		t.Fatal(err)
	}
	metrics, err := jobruntime.NewMetricsCollector(jobruntime.MetricDimensions{})
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewNativeFinalizeSyncRunService(pool, nil, metrics)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.UseCoverageCacheInvalidator(invalidator); err != nil {
		t.Fatal(err)
	}

	epochKey := cacheinvalidation.OrgCacheEpochKey(finalizeTestOrg)
	if err := client.Do(ctx, client.B().Get().Key(epochKey).Build()).Error(); !valkeygo.IsValkeyNil(err) {
		t.Fatalf("epoch key present before finalize (err=%v)", err)
	}

	if err := service.Finalize(ctx, newFinalizeArgs()); err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	epoch, err := client.Do(ctx, client.B().Get().Key(epochKey).Build()).AsInt64()
	if err != nil {
		t.Fatalf("GET %s after finalize: %v (the cache epoch was never bumped)", epochKey, err)
	}
	if epoch != 1 {
		t.Fatalf("epoch after first finalize=%d want=1", epoch)
	}
	if got := coverageCacheInvalidationCount(t, metrics, "emitted", "github"); got != 1 {
		t.Fatalf("emitted[github]=%d want=1", got)
	}
	if got := coverageCacheInvalidationCount(t, metrics, "consumed", "github"); got != 1 {
		t.Fatalf("consumed[github]=%d want=1", got)
	}

	// Re-finalize: once-only ledger branch -- no second bump, no re-emit.
	if err := service.Finalize(ctx, newFinalizeArgs()); err != nil {
		t.Fatalf("second Finalize: %v", err)
	}
	epoch, err = client.Do(ctx, client.B().Get().Key(epochKey).Build()).AsInt64()
	if err != nil {
		t.Fatal(err)
	}
	if epoch != 1 {
		t.Fatalf("epoch after re-finalize=%d want=1 (must not bump on the already_dispatched branch)", epoch)
	}
	if got := coverageCacheInvalidationCount(t, metrics, "emitted", "github"); got != 1 {
		t.Fatalf("emitted[github] after re-finalize=%d want=1", got)
	}
}

type failingInvalidator struct{ calls int }

func (f *failingInvalidator) InvalidateOrg(context.Context, string) error {
	f.calls++
	return errors.New("valkey unreachable")
}

// TestNativeFinalizeSyncRunCountsAnUnconsumedInvalidation pins the alert
// contract: a Valkey failure never fails the durably-committed finalize, but
// it leaves emitted - consumed > 0 behind for the alert to see.
func TestNativeFinalizeSyncRunCountsAnUnconsumedInvalidation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	pool := startFinalizePostgres(t, ctx)
	metrics, err := jobruntime.NewMetricsCollector(jobruntime.MetricDimensions{})
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewNativeFinalizeSyncRunService(pool, nil, metrics)
	if err != nil {
		t.Fatal(err)
	}
	failing := &failingInvalidator{}
	if err := service.UseCoverageCacheInvalidator(failing); err != nil {
		t.Fatal(err)
	}
	if err := service.Finalize(ctx, newFinalizeArgs()); err != nil {
		t.Fatalf("Finalize must not fail on a cache-invalidation error: %v", err)
	}
	if failing.calls != 1 {
		t.Fatalf("invalidator calls=%d want=1", failing.calls)
	}
	var status string
	if err := pool.QueryRow(ctx, `SELECT status FROM sync_runs WHERE id=$1`, finalizeTestRun).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != syncRunStatusSuccess {
		t.Fatalf("status=%q want=%q (finalize committed regardless of the cache hop)", status, syncRunStatusSuccess)
	}
	if got := coverageCacheInvalidationCount(t, metrics, "emitted", "github"); got != 1 {
		t.Fatalf("emitted[github]=%d want=1", got)
	}
	if got := coverageCacheInvalidationCount(t, metrics, "consumed", "github"); got != 0 {
		t.Fatalf("consumed[github]=%d want=0 (the gap is the alert)", got)
	}
}

// TestNativeFinalizeSyncRunWithoutAnInvalidatorStillEmits pins that a
// service built with no invalidator at all (a misconfigured worker) is
// visible as a permanent emitted - consumed gap rather than silence.
func TestNativeFinalizeSyncRunWithoutAnInvalidatorStillEmits(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	pool := startFinalizePostgres(t, ctx)
	metrics, err := jobruntime.NewMetricsCollector(jobruntime.MetricDimensions{})
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewNativeFinalizeSyncRunService(pool, nil, metrics)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Finalize(ctx, newFinalizeArgs()); err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	if got := coverageCacheInvalidationCount(t, metrics, "emitted", "github"); got != 1 {
		t.Fatalf("emitted[github]=%d want=1", got)
	}
	if got := coverageCacheInvalidationCount(t, metrics, "consumed", "github"); got != 0 {
		t.Fatalf("consumed[github]=%d want=0", got)
	}
}
