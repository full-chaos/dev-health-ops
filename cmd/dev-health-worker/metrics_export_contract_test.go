package main

import (
	"context"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
)

// TestMetricsEndpointExposesAppCounterContract is CHAOS-4308's red-on-baseline
// proof. It builds the SAME real server wiring production uses --
// health.NewServer's Handler(), fed by configureWorkerDependenciesWithSources
// -- and scrapes it exactly the way a Prometheus receiver would (an HTTP GET
// against /metrics), never touching WritePrometheus or a MetricsCollector
// directly. That distinction is the whole bug: it is trivial to prove a
// counter's own WritePrometheus method formats correctly (the tests in
// internal/jobruntime already do that) without ever proving the process's
// live HTTP surface reaches it.
//
// This is a CONTRACT test, not a one-off regression check: wantFamilies is
// the closed list of app counter names this binary's production /metrics
// response must contain. Every family here already reaches a real deployed
// worker except dev_health_sync_coverage_datasets_excluded_by_intent_total,
// which this same change registers for the first time
// (cmd/dev-health-worker/dependencies.go, "sync_coverage_scope_intent") --
// before that registration, this test fails with that one family absent from
// the response body, on an otherwise-passing scrape. Run this file against a
// worktree checked out at origin/main (before the registration) to see that
// red state; the failure names exactly the missing family, not a generic
// diff, so a future counter that's registered but never wired reads the same
// way.
func TestMetricsEndpointExposesAppCounterContract(t *testing.T) {
	t.Chdir(filepath.Join("..", ".."))

	database := &fakeWorkerDatabase{domainSaturation: 0.1, queueSaturation: 0.1}
	// Demote every operational kind back to Celery so composeOperationalFamily
	// stays a no-op and this test needs no real Postgres pool. The counter
	// this test exists to pin is a process-wide singleton
	// (synccoverage.ScopeIntentMetricsSource()) registered unconditionally by
	// configureWorkerDependenciesWithSources regardless of which queues this
	// worker group actually selected -- see the registration's own comment
	// for why that matters (a coverage-only registration would have made this
	// family present on go-worker-ops scrapes but silently absent everywhere
	// else, which is a scrape-target-shaped gap of exactly the kind CHAOS-4308
	// was filed to close).
	_, contractRoot := demotedContractRoot(t, celeryRoutedOperationalKinds...)
	sources := productionWorkerDependencySources
	sources.contractRoot = contractRoot
	sources.openDatabase = func(context.Context, config.Config) (workerDatabase, error) {
		return database, nil
	}
	sources.newRiverClientID = func() string { return "test-client" }

	registry := health.NewRegistry(100 * time.Millisecond)
	if _, err := configureWorkerDependenciesWithSources(
		context.Background(),
		config.Config{
			Queues:                 []string{"heartbeat"},
			WorkerQueueConcurrency: map[string]int{"heartbeat": 1},
			RiverDatabaseSchema:    "river",
		},
		registry,
		sources,
	); err != nil {
		t.Fatal(err)
	}

	server, err := health.NewServer(health.ServerOptions{
		Address:  "127.0.0.1:0",
		Registry: registry,
		Service:  "dev-health-worker",
		Version:  "test",
	})
	if err != nil {
		t.Fatal(err)
	}

	request := httptest.NewRequest("GET", "/metrics", nil)
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, request)
	if response.Code != 200 {
		t.Fatalf("GET /metrics status = %d, body = %s", response.Code, response.Body.String())
	}
	body := response.Body.String()

	wantFamilies := []string{
		// Pre-existing worker_runtime family (jobruntime.MetricsCollector,
		// registered since 2026-07-22): proves this test's harness reaches
		// the same real wiring production does, not a weaker substitute.
		"dev_health_post_sync_fanout_total",
		// The CHAOS-4308 fix: previously constructed and incremented
		// (internal/synccoverage/repository.go) but never registered on any
		// binary's health.Registry, so this family never reached a live
		// /metrics response before this change.
		"dev_health_sync_coverage_datasets_excluded_by_intent_total",
	}
	for _, family := range wantFamilies {
		if !strings.Contains(body, family) {
			t.Errorf("GET /metrics response missing app counter family %q\nfull body:\n%s", family, body)
		}
	}
}
