package otlpmetrics

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/legacyingest"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/full-chaos/dev-health-ops/internal/platform/postureguard"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/synccoverage"
)

// TestEveryFamilyOfTheRealProducersArrivesOverOTLPUnchanged is the gate of the
// bridge, built from the real producers and no capture: a health.Registry
// holding the MetricsSources the worker binaries register that can be built
// without a database or a cluster (the job runtime collector with its
// histograms, the provider-foundation counters, the legacy-ingest and
// sync-coverage counters, the posture guard), plus the required readiness
// checks, is served through the real operator /metrics handler and pushed
// through the real Pipeline to a real OTLP/gRPC collector. Every family, type,
// label set and value the scrape body carries must be in the OTLP points,
// compared against an independent reading of that body.
//
// Not covered, because the producer needs a Postgres pool or a stream
// transport: poolstat, selfprobe, the reconcilers' and schedulers' loops, the
// stream runner. Their exposition goes through the same text path the covered
// sources use; their own formatting is tested in their packages.
func TestEveryFamilyOfTheRealProducersArrivesOverOTLPUnchanged(t *testing.T) {
	registry := health.NewRegistry(time.Second)
	if err := registry.RegisterRequired("postgres", func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := registry.RegisterRequired("clickhouse", func(context.Context) error { return context.DeadlineExceeded }); err != nil {
		t.Fatal(err)
	}
	registry.SetLive(true)
	registry.SetReady(true)

	jobs := []jobruntime.JobLabels{{Queue: "metrics", Kind: "daily"}, {Queue: "sync", Kind: "provider"}}
	collector, err := jobruntime.NewMetricsCollector(jobruntime.MetricDimensions{Jobs: jobs, DomainTypes: []string{"git"}})
	if err != nil {
		t.Fatalf("NewMetricsCollector: %v", err)
	}
	for index, wait := range []time.Duration{20 * time.Millisecond, 3 * time.Second, 40 * time.Second} {
		if err := collector.ObserveJobWait(jobs[index%2], wait); err != nil {
			t.Fatalf("ObserveJobWait: %v", err)
		}
	}

	providers := providerfoundation.NewMetrics()
	providers.RecordRequest("github", providerfoundation.ErrorRateLimited)
	providers.RecordRequest("github", providerfoundation.ErrorTransient)
	providers.RecordBudgetDenied("gitlab")

	guard := postureguard.New("dev-health-worker", nil, "digest")
	for name, source := range map[string]health.MetricsSource{
		"job_runtime":  collector,
		"providers":    providers,
		"legacyingest": legacyingest.NewMetrics(),
		"scope_intent": synccoverage.NewScopeIntentMetrics(),
		"folded_keys":  synccoverage.NewFoldedKeyResolutionMetrics(),
		"posture":      guard,
	} {
		if err := registry.RegisterMetrics(name, source); err != nil {
			t.Fatalf("RegisterMetrics(%s): %v", name, err)
		}
	}

	server, err := health.NewServer(health.ServerOptions{Address: "127.0.0.1:0", Registry: registry, Service: "dev-health-worker", Version: "v-test"})
	if err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("/metrics answered %d", response.Code)
	}
	want := parseExposition(t, response.Body.String())

	resources, stats := push(t, health.Scrape{Registry: registry, Service: "dev-health-worker", Version: "v-test"}, nil)
	families, seriesCount := compare(t, "producers", want, resources)
	// The job runtime collector alone declares dozens of families; a pass over
	// fewer means the registry was not what this test believes it is.
	if families < 40 || seriesCount < 100 {
		t.Fatalf("compared %d families and %d series: the real producers carry more, the test examined too little", families, seriesCount)
	}
	histograms := 0
	for _, f := range want {
		if f.typ == "histogram" && !f.empty() {
			histograms++
		}
	}
	if histograms == 0 {
		t.Fatal("no histogram family in the producers' output: the histogram path is not exercised")
	}
	if stats.ExportFailures() != 0 || len(stats.SourceFailures()) != 0 {
		t.Errorf("failures on a clean push: export=%d sources=%v", stats.ExportFailures(), stats.SourceFailures())
	}
	t.Logf("compared %d families, %d series, %d histograms", families, seriesCount, histograms)
}
