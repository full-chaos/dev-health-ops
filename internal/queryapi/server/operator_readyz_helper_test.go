package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/full-chaos/dev-health-ops/internal/platform/health"
)

// NotConfiguredCheckProbe is NotConfiguredCheck as a probe, for operatorReadyz.
func NotConfiguredCheckProbe() ReadinessProbe {
	return ReadinessProbe{Name: "query_routes", Check: nil}
}

// operatorReadyz serves /readyz the way dho query-api does: a real health registry
// with one required check per probe (ObserveProbe, or NotConfiguredCheck for the
// "query_routes" probe with no Check), the readiness gate open, and the shell's own
// operator handler. It returns the status code and the exact body.
func operatorReadyz(t *testing.T, probes ...ReadinessProbe) (int, string) {
	t.Helper()
	registry := health.NewRegistry(5 * time.Second)
	for _, probe := range probes {
		check := NotConfiguredCheck()
		if probe.Check != nil {
			check = ObserveProbe(probe)
		}
		if err := registry.RegisterRequired(probe.Name, check); err != nil {
			t.Fatal(err)
		}
	}
	registry.SetReady(true)
	operator, err := health.NewServer(health.ServerOptions{Address: "127.0.0.1:0", Registry: registry, Service: "test", Version: "test"})
	if err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	operator.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	return recorder.Code, trimNewline(recorder.Body.String())
}

func trimNewline(text string) string {
	for len(text) > 0 && (text[len(text)-1] == '\n') {
		text = text[:len(text)-1]
	}
	return text
}

// readyzOutcomeTotal reads devhealth_query_api_readyz_total for one outcome and check
// from the test binary's one meter provider (registry_drift_telemetry_test.go).
func readyzOutcomeTotal(t *testing.T, outcome, check string) int64 {
	t.Helper()
	var collected metricdata.ResourceMetrics
	if err := driftMeterReader.Collect(context.Background(), &collected); err != nil {
		t.Fatal(err)
	}
	var total int64
	for _, scope := range collected.ScopeMetrics {
		for _, metric := range scope.Metrics {
			if metric.Name != "devhealth_query_api_readyz_total" {
				continue
			}
			sum, ok := metric.Data.(metricdata.Sum[int64])
			if !ok {
				continue
			}
			for _, point := range sum.DataPoints {
				gotOutcome, _ := point.Attributes.Value("outcome")
				gotCheck, _ := point.Attributes.Value("check")
				if gotOutcome.AsString() == outcome && gotCheck.AsString() == check {
					total += point.Value
				}
			}
		}
	}
	return total
}
