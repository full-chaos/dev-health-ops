package server

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// The query-api writes one request line and one per-route counter point for
// every non-probe request on each of its three listeners, whether or not the
// request's span was sampled (CHAOS-9106): the route is the mux's registered
// pattern, the peer is the connection's remote host, and no path, query
// string, header or token reaches the line. Probe paths write nothing.
func TestEveryQueryListenerWritesOneRequestLinePerRequest(t *testing.T) {
	var logs bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
	reader := sdkmetric.NewManualReader()
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))
	t.Cleanup(func() {
		slog.SetDefault(previousLogger)
		otel.SetMeterProvider(noop.NewMeterProvider())
	})

	// Sampling is off for this test: the line must not depend on the span.
	installSpanTracing(t, "0")
	handlers := realListenerHandlers(queryMux())
	const canary = "requestlinecanary"
	for listener, handler := range handlers {
		request := map[string]string{"Authorization": "Bearer " + canary, "X-Forwarded-For": canary}
		send(handler, http.MethodPost, "/query?token="+canary, request)
		send(handler, http.MethodPost, "/query", request)
		for _, probe := range []string{"/healthz", "/readyz", "/metrics"} {
			send(handler, http.MethodGet, probe, nil)
		}
		send(handler, http.MethodGet, "/no/such/route-"+canary+"-"+listener, nil)
	}

	perListener := map[string]int{}
	unmatched := map[string]int{}
	for _, raw := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
		var line map[string]any
		if raw == "" || json.Unmarshal([]byte(raw), &line) != nil || line["msg"] != "http request" {
			continue
		}
		listener, _ := line["listener"].(string)
		switch line["route"] {
		case "/query":
			perListener[listener]++
			if line["status"] != float64(200) || line["method"] != "POST" {
				t.Errorf("%s: line = %v, want POST /query 200", listener, line)
			}
		case "unmatched":
			unmatched[listener]++
		default:
			t.Errorf("%s: unexpected route %v (a probe path must write no line): %v", listener, line["route"], line)
		}
		if peer, _ := line["peer"].(string); peer != "192.0.2.1" {
			t.Errorf("%s: peer = %q, want the connection's host 192.0.2.1 (never a forwarded header)", listener, peer)
		}
		if _, ok := line["duration_ms"].(float64); !ok {
			t.Errorf("%s: no duration_ms: %v", listener, line)
		}
	}
	for listener := range handlers {
		if perListener[listener] != 2 || unmatched[listener] != 1 {
			t.Errorf("%s: %d /query lines and %d unmatched lines, want 2 and 1", listener, perListener[listener], unmatched[listener])
		}
	}
	for _, leaked := range []string{canary, "Bearer", "token="} {
		if strings.Contains(logs.String(), leaked) {
			t.Errorf("a request value (%q) reached the log:\n%s", leaked, logs.String())
		}
	}

	var collected metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &collected); err != nil {
		t.Fatal(err)
	}
	counts := map[string]int64{}
	for _, scope := range collected.ScopeMetrics {
		for _, m := range scope.Metrics {
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok || m.Name != "dev_health_api_http_requests_total" {
				continue
			}
			for _, point := range sum.DataPoints {
				route, _ := point.Attributes.Value(attribute.Key("route"))
				listener, _ := point.Attributes.Value(attribute.Key("listener"))
				counts[listener.AsString()+" "+route.AsString()] += point.Value
			}
		}
	}
	for listener := range handlers {
		if counts[listener+" /query"] != 2 || counts[listener+" unmatched"] != 1 {
			t.Errorf("%s: request counter = %v, want /query 2 and unmatched 1", listener, counts)
		}
	}
}
