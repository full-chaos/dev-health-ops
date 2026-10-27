package tracing

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

// The start-up line names the collector by host:port only; credentials, a
// path, a query or a fragment in the configured endpoint never reach the log.
func TestTheStartupLineNamesTheCollectorByHostAndPortOnly(t *testing.T) {
	for _, tc := range []struct{ endpoint, want string }{
		{"http://ops-user:c4n4ry-pw@127.0.0.1:4317/v1/traces?c4n4ry-q=1#c4n4ry-f", "127.0.0.1:4317"},
		{"https://collector.internal:4317/c4n4ry-path", "collector.internal:4317"},
		{"http://[::1]:4317", "[::1]:4317"},
		{"127.0.0.1:4317", "127.0.0.1:4317"},
		{"collector.internal:4317", "collector.internal:4317"},
	} {
		t.Run(tc.endpoint, func(t *testing.T) {
			t.Setenv("OTEL_ENABLED", "true")
			t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", tc.endpoint)
			t.Setenv("OTEL_SAMPLE_RATE", "0")
			t.Cleanup(func() {
				otel.SetTracerProvider(noop.NewTracerProvider())
				otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator())
			})
			var logs bytes.Buffer
			component := InitWithServiceName(slog.New(slog.NewJSONHandler(&logs, nil)), "auth-service")
			_ = component.Shutdown(context.Background())
			var line map[string]any
			for _, raw := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
				if strings.Contains(raw, "OpenTelemetry tracing initialised") {
					if err := json.Unmarshal([]byte(raw), &line); err != nil {
						t.Fatal(err)
					}
				}
			}
			if line == nil {
				t.Fatalf("no start-up line:\n%s", logs.String())
			}
			if line["otlp_endpoint"] != tc.want {
				t.Errorf("otlp_endpoint = %v, want %q", line["otlp_endpoint"], tc.want)
			}
			for _, leaked := range []string{"ops-user", "c4n4ry"} {
				if strings.Contains(logs.String(), leaked) {
					t.Errorf("the log carries %q:\n%s", leaked, logs.String())
				}
			}
		})
	}
}

// A malformed endpoint never reaches the exporter (whose URL parser logs the
// raw value outside the service logger): tracing stays off, the failure is
// counted and the log names no part of the value.
func TestAMalformedEndpointNeverReachesTheExporter(t *testing.T) {
	for _, endpoint := range []string{
		"http://ops-user:c4n4ry-pw@127.0.0.1:c4n4ry-port",
		"http://ops-user:c4n4ry-pw@[::1",
		"ops-user:c4n4ry-pw@",
		"ops-user:c4n4ry-pw@127.0.0.1:4317",
		"127.0.0.1:4317/c4n4ry-path",
		"127.0.0.1:4317?c4n4ry-q=1",
	} {
		t.Run(endpoint, func(t *testing.T) {
			t.Setenv("OTEL_ENABLED", "true")
			t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", endpoint)
			t.Setenv("OTEL_SAMPLE_RATE", "0")
			built := false
			previous := exporterFor
			exporterFor = func(ctx context.Context, endpoint string) (sdktrace.SpanExporter, error) {
				built = true
				return previous(ctx, endpoint)
			}
			t.Cleanup(func() {
				exporterFor = previous
				otel.SetTracerProvider(noop.NewTracerProvider())
				otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator())
			})
			before := initFailures.final.Load()
			var logs bytes.Buffer
			component := InitWithServiceName(slog.New(slog.NewJSONHandler(&logs, nil)), "auth-service")
			_ = component.Shutdown(context.Background())
			if built {
				t.Error("the exporter was built from a malformed endpoint")
			}
			if initFailures.final.Load() != before+1 {
				t.Errorf("init failures moved by %d, want 1", initFailures.final.Load()-before)
			}
			for _, leaked := range []string{"ops-user", "c4n4ry"} {
				if strings.Contains(logs.String(), leaked) {
					t.Errorf("the log carries %q:\n%s", leaked, logs.String())
				}
			}
		})
	}
}
