//go:build integration

package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5/pgxpool"
)

// TestMetricsAndSyncQueueSelectionBootsWithMigratedClickHouse reproduces the
// live-e2e metrics-executed-proof failure mode: dev-health-worker exiting at
// startup with dependency_configuration_failed/worker_family_composition_failed
// when the "metrics"+"sync" queues are selected together (run 33814100751,
// artifact metrics-executed-proof-worker.log). TestEveryMultiFamilyQueueSelectionBoots
// already exercises this exact queue pair (its "daily+syncCoordinator" case),
// but against a BARE, unmigrated ClickHouse container -- every "remaining"
// kind's schema guard refuses gracefully there (ErrWorkItemAttributionSchemaIncompatible
// and its DORA/Capacity siblings), so that test never actually reaches the
// real construction path a fully migrated ClickHouse (as CI's
// ci/run_metrics_executed_proof.sh applies via `dev-hops migrate clickhouse
// upgrade` before starting the worker) exercises. This test closes that gap
// by applying the real migration chain (chschema.Apply) before composing.
func TestMetricsAndSyncQueueSelectionBootsWithMigratedClickHouse(t *testing.T) {
	t.Chdir("../..")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)

	postgres, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = postgres.Close(context.Background()) })
	admin, err := pgxpool.New(ctx, postgres.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	prepareMultiReplicaDatabase(t, ctx, admin)

	clickhouse, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = clickhouse.Close(context.Background()) })
	chschema.Apply(ctx, t, clickhouse)

	valkey, err := containers.StartValkey(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = valkey.Close(context.Background()) })

	bridge := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write([]byte(`{}`))
	}))
	t.Cleanup(bridge.Close)

	family, err := bootQueueSelection(
		t, ctx, postgres.URI, clickhouse.URI, valkey.URI, bridge.URL, []string{"metrics", "sync"},
	)
	if err != nil {
		t.Fatalf("queue selection metrics,sync did not compose against a migrated ClickHouse: %v", err)
	}
	if len(family.handlers) == 0 {
		t.Fatal("queue selection metrics,sync registered no handlers")
	}

	foundWorkItemAttribution := false
	for _, handler := range family.handlers {
		if handler.Kind == jobcontract.KindRemainingWorkItemAttribution {
			foundWorkItemAttribution = true
		}
	}
	if !foundWorkItemAttribution {
		t.Error("metrics.remaining.work_item_attribution was not registered against a migrated ClickHouse -- the executor refused instead of constructing")
	}
}
