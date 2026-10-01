package otlpmetrics

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/health"
)

type fixedSource string

func (f fixedSource) WritePrometheus(w io.Writer) error {
	_, err := io.WriteString(w, string(f))
	return err
}

type brokenSource struct{}

func (brokenSource) WritePrometheus(io.Writer) error { return errors.New("dependency down") }

// TestEverythingTheScrapeServesIsPushed builds a real health.Registry with
// required checks and several sources (one of them failing), serves /metrics
// through the real operator handler, and pushes the same registry through the
// real Pipeline: every family the scrape body carries, the runtime block and
// the per-source failure gauge included, must arrive over OTLP unchanged. A
// bridge that reads fewer sources than the scrape serves is red.
func TestEverythingTheScrapeServesIsPushed(t *testing.T) {
	registry := health.NewRegistry(time.Second)
	if err := registry.RegisterRequired("postgres", func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := registry.RegisterRequired("clickhouse", func(context.Context) error { return errors.New("down") }); err != nil {
		t.Fatal(err)
	}
	registry.SetLive(true)
	registry.SetReady(true)
	for name, source := range map[string]health.MetricsSource{
		"pool":       fixedSource("# TYPE pool_in_use gauge\npool_in_use{pool=\"queue\"} 3\npool_in_use{pool=\"domain\"} 1\n"),
		"reconciler": fixedSource("# TYPE reconcile_total counter\nreconcile_total{outcome=\"ok\"} 41\nreconcile_total{outcome=\"error\"} 2\n# TYPE reconcile_seconds histogram\nreconcile_seconds_bucket{le=\"0.1\"} 4\nreconcile_seconds_bucket{le=\"1\"} 9\nreconcile_seconds_bucket{le=\"+Inf\"} 10\nreconcile_seconds_sum 3.5\nreconcile_seconds_count 10\n"),
		"flaky":      brokenSource{},
	} {
		if err := registry.RegisterMetrics(name, source); err != nil {
			t.Fatal(err)
		}
	}
	server, err := health.NewServer(health.ServerOptions{Address: "127.0.0.1:0", Registry: registry, Service: "dev-health-worker", Version: "v-test"})
	if err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := response.Body.String()
	for _, want := range []string{"dev_health_runtime_live", "dev_health_runtime_check_failed", "pool_in_use", "reconcile_seconds", "dev_health_runtime_metrics_source_failed"} {
		if !strings.Contains(body, want) {
			t.Fatalf("the scrape body does not carry %s; the fixture is wrong:\n%s", want, body)
		}
	}

	resources, _ := push(t, health.Scrape{Registry: registry, Service: "dev-health-worker", Version: "v-test"}, nil)
	families, seriesCount := compare(t, "scrape", parseExposition(t, body), resources)
	// runtime(live, ready, uptime, required, info, check_failed) + pool + counter + histogram + source_failed
	if families < 10 || seriesCount < 10 {
		t.Fatalf("compared %d families and %d series: fewer than the scrape serves", families, seriesCount)
	}
}

// TestASkippedSourceStillHasItsFailureStatusOverOTLP: the instruments' own
// fragment is excluded from the push, but the scrape's source-failure gauge
// lists every registered source, so the push must too, healthy or failing.
func TestASkippedSourceStillHasItsFailureStatusOverOTLP(t *testing.T) {
	registry := health.NewRegistry(time.Second)
	registry.SetLive(true)
	registry.SetReady(true)
	for name, source := range map[string]health.MetricsSource{
		"skipped_ok":     fixedSource("# TYPE skipped_ok_total counter\nskipped_ok_total 1\n"),
		"skipped_broken": brokenSource{},
		"kept":           fixedSource("# TYPE kept_total counter\nkept_total 2\n"),
	} {
		if err := registry.RegisterMetrics(name, source); err != nil {
			t.Fatal(err)
		}
	}
	skip := map[string]bool{"skipped_ok": true, "skipped_broken": true}
	resources, _ := push(t, health.Scrape{Registry: registry, Service: "svc", Version: "v"}, skip)
	received := collect(resources)
	failed := received["dev_health_runtime_metrics_source_failed"]
	if failed == nil {
		t.Fatal("the source-failure gauge did not arrive")
	}
	for source, want := range map[string]float64{"skipped_ok": 0, "skipped_broken": 1, "kept": 0} {
		got, ok := failed.numbers["source="+source]
		if !ok || got != want {
			t.Errorf("source_failed{source=%q} = %v (present %v) over OTLP, want %v: %v", source, got, ok, want, failed.numbers)
		}
	}
	// The skipped sources' own families are not pushed by the Scrape.
	if received["skipped_ok_total"] != nil {
		t.Error("a skipped source's family was pushed")
	}
	if received["kept_total"] == nil {
		t.Error("a kept source's family was not pushed")
	}
}
