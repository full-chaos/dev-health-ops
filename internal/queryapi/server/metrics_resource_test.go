package server

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/metric"
)

// TestMetricsServiceName pins the service_name /metrics reports in
// target_info: otelServiceName when the environment names none, the
// environment's value when it does. Without an explicit resource the SDK
// reports "unknown_service:<executable>", so the name would follow the
// binary's file name.
func TestMetricsServiceName(t *testing.T) {
	for name, testCase := range map[string]struct {
		env  map[string]string
		want string
	}{
		"no environment":           {want: `service_name="` + otelServiceName + `"`},
		"OTEL_SERVICE_NAME":        {env: map[string]string{"OTEL_SERVICE_NAME": "from-env"}, want: `service_name="from-env"`},
		"OTEL_RESOURCE_ATTRIBUTES": {env: map[string]string{"OTEL_RESOURCE_ATTRIBUTES": "service.name=from-attrs"}, want: `service_name="from-attrs"`},
	} {
		t.Run(name, func(t *testing.T) {
			t.Setenv("OTEL_SERVICE_NAME", "")
			t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "")
			for key, value := range testCase.env {
				t.Setenv(key, value)
			}
			provider, registry, err := newPrometheusMeterProvider()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
			counter, err := provider.Meter("test").Int64Counter("probe")
			if err != nil {
				t.Fatal(err)
			}
			counter.Add(context.Background(), 1, metric.WithAttributes())

			recorder := httptest.NewRecorder()
			metricsHandler(registry).ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
			body, _ := io.ReadAll(recorder.Body)
			var targetInfo string
			for _, line := range strings.Split(string(body), "\n") {
				if strings.HasPrefix(line, "target_info{") {
					targetInfo = line
				}
			}
			if !strings.Contains(targetInfo, testCase.want) {
				t.Fatalf("target_info = %q, want it to contain %s", targetInfo, testCase.want)
			}
		})
	}
}
