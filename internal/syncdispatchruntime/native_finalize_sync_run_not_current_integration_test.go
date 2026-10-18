//go:build integration

package syncdispatchruntime

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime/synclog"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// CHAOS-9964: a finalize delivery dispatched at generation 4 while the route is
// at generation 3 is refused as a committed no-op, and the run stays
// 'dispatching'. The refusal must be visible: a Warn with the reason and both
// generations, and a counter by reason.
func TestNativeFinalizeSyncRunLogsAndCountsADeliveryThatIsNotCurrent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	createFinalizeTables(t, ctx, pool)
	seedFinalizeRouteRow(t, ctx, pool)
	pgseed.EnsureSyncIntegration(ctx, t, pool, finalizeTestOrg, finalizeTestIntegration, finalizeTestSource)
	pgseed.SyncConfiguration(ctx, t, pool, finalizeTestSyncConfig, finalizeTestOrg, finalizeTestIntegration, `{}`, time.Now())
	pgseed.EnsureSyncRun(ctx, t, pool, pgseed.SyncRun{
		ID: finalizeTestRun, OrgID: finalizeTestOrg, IntegrationID: finalizeTestIntegration, Status: "dispatching", TotalUnits: 1,
	})
	const staleGeneration = finalizeRouteGeneration + 1
	pgseed.SyncDispatchOutbox(ctx, t, pool, finalizeTestOutbox, finalizeTestRun, finalizeTestOrg, "finalize_sync_run", "dispatched", "river", staleGeneration)
	day := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	pgseed.InsertSyncRunUnit(ctx, t, pool, pgseed.SyncRunUnit{
		ID: finalizeTestUnit, RunID: finalizeTestRun, OrgID: finalizeTestOrg, IntegrationID: finalizeTestIntegration,
		SourceID: finalizeTestSource, DatasetKey: "work-items", Status: syncRunUnitStatusSuccess, SinceAt: &day, BeforeAt: &day,
	})

	reader := sdkmetric.NewManualReader()
	previous := otel.GetMeterProvider()
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))
	t.Cleanup(func() { otel.SetMeterProvider(previous) })
	var logs bytes.Buffer
	logger := synclog.New(slog.New(slog.NewTextHandler(&logs, nil)))
	service, err := NewNativeFinalizeSyncRunService(pool, logger)
	if err != nil {
		t.Fatal(err)
	}
	args := newFinalizeArgs()
	args.TransportArgs.RouteGeneration = staleGeneration
	if err := service.Finalize(ctx, args); err != nil {
		t.Fatalf("Finalize: %v", err)
	}

	var status string
	if err := pool.QueryRow(ctx, `SELECT status FROM sync_runs WHERE id = $1`, finalizeTestRun).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "dispatching" {
		t.Fatalf("run status = %q, want dispatching (the stale delivery is a no-op)", status)
	}
	out := logs.String()
	for _, want := range []string{"finalize_sync_run.delivery_not_current", "reason=route_generation_mismatch", "delivery_route_generation=4", "current_route_generation=3"} {
		if !strings.Contains(out, want) {
			t.Fatalf("log lacks %q:\n%s", want, out)
		}
	}
	var resource metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &resource); err != nil {
		t.Fatal(err)
	}
	var counted int64
	for _, scope := range resource.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != FinalizeDeliveryNotCurrentCounter {
				continue
			}
			for _, point := range m.Data.(metricdata.Sum[int64]).DataPoints {
				if reason, _ := point.Attributes.Value("reason"); reason.AsString() == "route_generation_mismatch" {
					counted += point.Value
				}
			}
		}
	}
	if counted != 1 {
		t.Fatalf("%s{reason=route_generation_mismatch} = %d, want 1", FinalizeDeliveryNotCurrentCounter, counted)
	}
}

// Round 463 finding: a delivery whose outbox row is gone must still report the route's current generation.
func TestNativeFinalizeSyncRunReportsTheRouteGenerationWhenTheOutboxRowIsMissing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	createFinalizeTables(t, ctx, pool)
	seedFinalizeRoute(t, ctx, pool)
	insertFinalizeRun(t, ctx, pool, 1, "", "")
	if _, err := pool.Exec(ctx, `DELETE FROM sync_dispatch_outbox WHERE id = $1`, finalizeTestOutbox); err != nil {
		t.Fatal(err)
	}
	var logs bytes.Buffer
	service, err := NewNativeFinalizeSyncRunService(pool, synclog.New(slog.New(slog.NewTextHandler(&logs, nil))))
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Finalize(ctx, newFinalizeArgs()); err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	out := logs.String()
	for _, want := range []string{"reason=outbox_row_missing", "current_route_generation=3"} {
		if !strings.Contains(out, want) {
			t.Fatalf("log lacks %q:\n%s", want, out)
		}
	}
}
