package authruntime

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace/noop"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/grpc"

	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
)

// exportedSpan is one span as it crossed the wire, with its resource's
// service.name.
type exportedSpan struct {
	service string
	span    *tracepb.Span
}

// otlpReceiver is a real OTLP/gRPC TraceService receiver.
type otlpReceiver struct {
	coltracepb.UnimplementedTraceServiceServer
	mu    sync.Mutex
	spans []exportedSpan
}

func (c *otlpReceiver) Export(_ context.Context, req *coltracepb.ExportTraceServiceRequest) (*coltracepb.ExportTraceServiceResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, resource := range req.GetResourceSpans() {
		service := ""
		for _, kv := range resource.GetResource().GetAttributes() {
			if kv.GetKey() == "service.name" {
				service = kv.GetValue().GetStringValue()
			}
		}
		for _, scope := range resource.GetScopeSpans() {
			for _, span := range scope.GetSpans() {
				c.spans = append(c.spans, exportedSpan{service: service, span: span})
			}
		}
	}
	return &coltracepb.ExportTraceServiceResponse{}, nil
}

func (c *otlpReceiver) received() []exportedSpan {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]exportedSpan(nil), c.spans...)
}

// startReceiver serves a receiver on loopback and resets the process-wide
// provider and propagator when the test ends.
func startReceiver(t *testing.T) (*otlpReceiver, string) {
	t.Helper()
	receiver := &otlpReceiver{}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	server := grpc.NewServer()
	coltracepb.RegisterTraceServiceServer(server, receiver)
	go func() { _ = server.Serve(lis) }()
	t.Cleanup(func() {
		server.Stop()
		otel.SetTracerProvider(noop.NewTracerProvider())
		otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator())
	})
	return receiver, lis.Addr().String()
}

// unsetEnv removes name from the process environment for the test and puts
// back what was there.
func unsetEnv(t *testing.T, name string) {
	t.Helper()
	previous, had := os.LookupEnv(name)
	if err := os.Unsetenv(name); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if had {
			_ = os.Setenv(name, previous)
		} else {
			_ = os.Unsetenv(name)
		}
	})
}

// tracedService runs the service against a fresh receiver with every request
// sampled, sends requests to the API listener, scrapes the operator /metrics,
// stops the service and returns the exported spans, the scrape and the logs.
func tracedService(t *testing.T, env map[string]string, paths ...string) ([]exportedSpan, string, string) {
	t.Helper()
	receiver, address := startReceiver(t)
	t.Setenv("OTEL_ENABLED", "true")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", address)
	t.Setenv("OTEL_SAMPLE_RATE", "1")
	ports := reservePorts(t, 2)
	if env == nil {
		env = brokenEnvironment(t, ports[0], ports[1])
	}
	logs, stop := runService(t, env)
	client := &http.Client{Timeout: 10 * time.Second}
	defer client.CloseIdleConnections()
	api := "http://" + env[authconfig.EnvAPIAddress]
	operator := "http://" + env[authconfig.EnvOperatorAddress]
	awaitResponse(t, client, operator+"/healthz").Body.Close()
	for _, path := range paths {
		request, err := http.NewRequest(http.MethodGet, api+path, nil)
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Authorization", "Bearer c4n4ry-bearer")
		request.Header.Set("Cookie", "session=c4n4ry-cookie")
		request.Header.Set("traceparent", "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01")
		response, err := client.Do(request)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		_, _ = io.Copy(io.Discard, response.Body)
		_ = response.Body.Close()
	}
	scrape := awaitResponse(t, client, operator+"/metrics")
	body, _ := io.ReadAll(scrape.Body)
	_ = scrape.Body.Close()
	if code := stop(); code != 0 {
		t.Fatalf("Execute exited %d, want 0; logs:\n%s", code, logs.String())
	}
	return receiver.received(), string(body), logs.String()
}

func TestTheAPIListenerExportsItsServerSpansAsAuthService(t *testing.T) {
	unsetEnv(t, "OTEL_SERVICE_NAME")
	spans, _, logs := tracedService(t, nil, "/v1/c4n4ry-path-segment")
	if len(spans) == 0 {
		t.Fatalf("no span reached the receiver; logs:\n%s", logs)
	}
	found := false
	for _, s := range spans {
		if s.service != "auth-service" {
			t.Errorf("span %q has service.name %q, want auth-service", s.span.GetName(), s.service)
		}
		if s.span.GetName() == "GET unmatched" && s.span.GetKind() == tracepb.Span_SPAN_KIND_SERVER {
			found = true
		}
	}
	if !found {
		t.Errorf("no SERVER span named \"GET unmatched\" among %d spans", len(spans))
	}
}

// Every exported span carries only the bounded request facts: no URL, query,
// path text, header, cookie or caller trace context.
func TestAnExportedSpanCarriesOnlyBoundedAttributes(t *testing.T) {
	spans, _, logs := tracedService(t, nil, "/v1/c4n4ry-path-segment?code=c4n4ry-code&state=c4n4ry-state")
	if len(spans) == 0 {
		t.Fatalf("no span reached the receiver; logs:\n%s", logs)
	}
	allowed := map[string]bool{
		"http.request.method": true, "dev_health.listener": true, "http.route": true,
		"http.response.status_code": true, "dev_health.http.not_found_cause": true,
	}
	for _, s := range spans {
		for _, kv := range s.span.GetAttributes() {
			if !allowed[kv.GetKey()] {
				t.Errorf("span %q carries attribute %q", s.span.GetName(), kv.GetKey())
			}
		}
		raw, err := json.Marshal(s.span)
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Contains(raw, []byte("c4n4ry")) {
			t.Errorf("span %q carries request text: %s", s.span.GetName(), raw)
		}
		if hex.EncodeToString(s.span.GetTraceId()) == "0af7651916cd43dd8448eb211c80319c" || len(s.span.GetParentSpanId()) != 0 {
			t.Errorf("span %q joined the caller's trace context; want a new root", s.span.GetName())
		}
	}
}

func TestAProbePathOpensNoSpan(t *testing.T) {
	spans, _, logs := tracedService(t, nil, "/health", "/ready", "/health/workers")
	if len(spans) != 0 {
		names := make([]string, 0, len(spans))
		for _, s := range spans {
			names = append(names, s.span.GetName())
		}
		t.Fatalf("probe paths exported spans %v; logs:\n%s", names, logs)
	}
}

func TestOTELServiceNameWinsOverTheBinaryDefault(t *testing.T) {
	t.Setenv("OTEL_SERVICE_NAME", "auth-service-canary")
	spans, _, logs := tracedService(t, nil, "/v1/unmatched-probe")
	if len(spans) == 0 {
		t.Fatalf("no span reached the receiver; logs:\n%s", logs)
	}
	for _, s := range spans {
		if s.service != "auth-service-canary" {
			t.Errorf("service.name = %q, want the OTEL_SERVICE_NAME value", s.service)
		}
	}
}

func TestTheOperatorMetricsCarryTheSpanCounters(t *testing.T) {
	_, metrics, _ := tracedService(t, nil, "/v1/unmatched-probe")
	for _, line := range []string{
		"# TYPE dev_health_otel_spans_total counter",
		"# TYPE dev_health_otel_init_failures_total counter",
	} {
		if !strings.Contains(metrics, line) {
			t.Errorf("operator /metrics lacks %q", line)
		}
	}
}

func TestTracingOffExportsNothing(t *testing.T) {
	for _, value := range []string{"false", "0", "no", "FALSE"} {
		t.Run(value, func(t *testing.T) {
			receiver, address := startReceiver(t)
			t.Setenv("OTEL_ENABLED", value)
			t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", address)
			t.Setenv("OTEL_SAMPLE_RATE", "1")
			ports := reservePorts(t, 2)
			env := brokenEnvironment(t, ports[0], ports[1])
			env[authconfig.EnvLogLevel] = "info"
			logs, stop := runService(t, env)
			client := &http.Client{Timeout: 10 * time.Second}
			defer client.CloseIdleConnections()
			awaitResponse(t, client, "http://"+ports[1]+"/healthz").Body.Close()
			response := awaitResponse(t, client, "http://"+ports[0]+"/v1/unmatched-probe")
			_ = response.Body.Close()
			if code := stop(); code != 0 {
				t.Fatalf("Execute exited %d", code)
			}
			if got := receiver.received(); len(got) != 0 {
				t.Errorf("OTEL_ENABLED=%s exported %d spans", value, len(got))
			}
			if strings.Contains(logs.String(), "OpenTelemetry tracing initialised") {
				t.Errorf("OTEL_ENABLED=%s still initialised tracing:\n%s", value, logs.String())
			}
		})
	}
}

// startupLine is the "OpenTelemetry tracing initialised" log line, decoded.
func startupLine(t *testing.T, logs string) map[string]any {
	t.Helper()
	for _, raw := range strings.Split(logs, "\n") {
		if !strings.Contains(raw, "OpenTelemetry tracing initialised") {
			continue
		}
		var line map[string]any
		if err := json.Unmarshal([]byte(raw), &line); err != nil {
			t.Fatalf("log line %q is not JSON: %v", raw, err)
		}
		return line
	}
	t.Fatalf("no \"OpenTelemetry tracing initialised\" line:\n%s", logs)
	return nil
}

// The startup line names the collector by host:port only: credentials, a
// path or a query in the configured endpoint never reach the log.
func TestTheStartupLineNamesTheCollectorByHostAndPortOnly(t *testing.T) {
	for _, tc := range []struct{ endpoint, want string }{
		{"http://ops-user:c4n4ry-pw@127.0.0.1:4317/v1/traces?c4n4ry-q=1", "127.0.0.1:4317"},
		{"ops-user:c4n4ry-pw@127.0.0.1:4317", "127.0.0.1:4317"},
		{"https://collector.internal:4317/c4n4ry-path", "collector.internal:4317"},
		{"127.0.0.1:4317", "127.0.0.1:4317"},
	} {
		t.Run(tc.want, func(t *testing.T) {
			t.Setenv("OTEL_ENABLED", "true")
			t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", tc.endpoint)
			t.Setenv("OTEL_SAMPLE_RATE", "0")
			t.Cleanup(func() { otel.SetTracerProvider(noop.NewTracerProvider()) })
			ports := reservePorts(t, 2)
			env := brokenEnvironment(t, ports[0], ports[1])
			env[authconfig.EnvLogLevel] = "info"
			logs, stop := runService(t, env)
			client := &http.Client{Timeout: 10 * time.Second}
			defer client.CloseIdleConnections()
			awaitResponse(t, client, "http://"+ports[1]+"/healthz").Body.Close()
			if code := stop(); code != 0 {
				t.Fatalf("Execute exited %d", code)
			}
			line := startupLine(t, logs.String())
			if line["otlp_endpoint"] != tc.want {
				t.Errorf("otlp_endpoint = %v, want %q", line["otlp_endpoint"], tc.want)
			}
			if line["service_name"] != "auth-service" {
				t.Errorf("service_name = %v, want auth-service", line["service_name"])
			}
			for _, leaked := range []string{"ops-user", "c4n4ry"} {
				if strings.Contains(logs.String(), leaked) {
					t.Errorf("the log carries %q:\n%s", leaked, logs.String())
				}
			}
		})
	}
}

// With no OTEL_* variable at all (a hosted runner, `env -i`), the service
// starts, initialises tracing with the package defaults and names auth-service.
func TestWithNoOTELVariablesTheServiceStartsWithTheDefaults(t *testing.T) {
	for _, name := range []string{"OTEL_ENABLED", "OTEL_EXPORTER_OTLP_ENDPOINT", "OTEL_SAMPLE_RATE", "OTEL_SERVICE_NAME", "OTEL_ENVIRONMENT"} {
		unsetEnv(t, name)
	}
	t.Cleanup(func() { otel.SetTracerProvider(noop.NewTracerProvider()) })
	ports := reservePorts(t, 2)
	env := brokenEnvironment(t, ports[0], ports[1])
	env[authconfig.EnvLogLevel] = "info"
	logs, stop := runService(t, env)
	client := &http.Client{Timeout: 10 * time.Second}
	defer client.CloseIdleConnections()
	awaitResponse(t, client, "http://"+ports[1]+"/healthz").Body.Close()
	if code := stop(); code != 0 {
		t.Fatalf("Execute exited %d", code)
	}
	line := startupLine(t, logs.String())
	if line["otlp_endpoint"] != "localhost:4317" || line["service_name"] != "auth-service" {
		t.Errorf("startup line = %v, want localhost:4317 and auth-service", line)
	}
}
