package httpapi

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace/noop"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/grpc"

	"github.com/full-chaos/dev-health-ops/internal/platform/tracing"
)

// spanCollector is a real OTLP/gRPC TraceService receiver: the spans a test
// asserts on crossed the wire the way production spans cross to the host
// collector, they are not read out of an in-memory recorder.
type spanCollector struct {
	coltracepb.UnimplementedTraceServiceServer
	mu    sync.Mutex
	spans []*tracepb.Span
}

func (c *spanCollector) Export(_ context.Context, req *coltracepb.ExportTraceServiceRequest) (*coltracepb.ExportTraceServiceResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, resource := range req.GetResourceSpans() {
		for _, scope := range resource.GetScopeSpans() {
			c.spans = append(c.spans, scope.GetSpans()...)
		}
	}
	return &coltracepb.ExportTraceServiceResponse{}, nil
}

// tracedEnv installs the SDK provider the way the shell does
// (tracing.InitWithServiceName) against a fresh collector, at the given
// OTEL_SAMPLE_RATE, and returns the flush-and-read function.
func tracedEnv(t *testing.T, rate string) func() []*tracepb.Span {
	t.Helper()
	collector := &spanCollector{}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	server := grpc.NewServer()
	coltracepb.RegisterTraceServiceServer(server, collector)
	go func() { _ = server.Serve(lis) }()
	t.Setenv("OTEL_ENABLED", "true")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", lis.Addr().String())
	t.Setenv("OTEL_SAMPLE_RATE", rate)
	component := tracing.InitWithServiceName(slog.New(slog.NewTextHandler(io.Discard, nil)), "dev-health-api")
	flushed := false
	read := func() []*tracepb.Span {
		if !flushed {
			flushed = true
			if err := component.Shutdown(context.Background()); err != nil {
				t.Fatalf("flush spans: %v", err)
			}
		}
		collector.mu.Lock()
		defer collector.mu.Unlock()
		return append([]*tracepb.Span(nil), collector.spans...)
	}
	t.Cleanup(func() {
		_ = component.Shutdown(context.Background())
		server.Stop()
		otel.SetTracerProvider(noop.NewTracerProvider())
		otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator())
	})
	return read
}

func spanHandler(t *testing.T, listener string, trust bool, routes ...Route) http.Handler {
	t.Helper()
	options := testOptions(routes...)
	options.Listener = listener
	options.TrustRemoteSampling = trust
	options.StrictPaths = true
	return handlerFor(t, options)
}

func get(handler http.Handler, target string, headers map[string]string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(http.MethodGet, target, nil)
	for name, value := range headers {
		request.Header.Set(name, value)
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	return recorder
}

func attrString(span *tracepb.Span, key string) (string, bool) {
	for _, kv := range span.GetAttributes() {
		if kv.GetKey() == key {
			return kv.GetValue().GetStringValue(), true
		}
	}
	return "", false
}

func attrInt(span *tracepb.Span, key string) (int64, bool) {
	for _, kv := range span.GetAttributes() {
		if kv.GetKey() == key {
			if _, ok := kv.GetValue().GetValue().(*commonpb.AnyValue_IntValue); ok {
				return kv.GetValue().GetIntValue(), true
			}
		}
	}
	return 0, false
}

func sampledParent(traceID, spanID string) string {
	return "00-" + traceID + "-" + spanID + "-01"
}

func TestARoutedRequestMakesOneServerSpanNamedByThePatternAndCarryingNoRequestValue(t *testing.T) {
	read := tracedEnv(t, "1")
	handler := spanHandler(t, "public", false, itemsRoute())
	get(handler, "/v1/items/9183?token=querycanary", map[string]string{"Authorization": "Bearer headercanary", "X-Org-Id": "orgcanary"})
	spans := read()
	if len(spans) != 1 {
		t.Fatalf("got %d spans, want 1", len(spans))
	}
	span := spans[0]
	if span.GetName() != "GET /v1/items/{id}" {
		t.Errorf("name = %q, want the registered pattern", span.GetName())
	}
	if span.GetKind() != tracepb.Span_SPAN_KIND_SERVER {
		t.Errorf("kind = %v, want server", span.GetKind())
	}
	for key, want := range map[string]string{"http.route": "/v1/items/{id}", "http.request.method": "GET", listenerAttribute: "public"} {
		if got, _ := attrString(span, key); got != want {
			t.Errorf("attribute %s = %q, want %q", key, got, want)
		}
	}
	if status, _ := attrInt(span, "http.response.status_code"); status != http.StatusNoContent {
		t.Errorf("status attribute = %d, want 204", status)
	}
	dump := fmt.Sprint(span)
	for _, canary := range []string{"9183", "querycanary", "headercanary", "orgcanary", "token"} {
		if strings.Contains(dump, canary) {
			t.Errorf("the span carries %q, a request value: %s", canary, dump)
		}
	}
}

func TestAnUnmatchedRequestSpanIsNamedUnmatchedNeverByTheRawPath(t *testing.T) {
	read := tracedEnv(t, "1")
	get(spanHandler(t, "public", false, itemsRoute()), "/no/such/path-pathcanary", nil)
	spans := read()
	if len(spans) != 1 || spans[0].GetName() != "GET "+UnmatchedRoute {
		t.Fatalf("spans = %v, want one named %q", spans, "GET "+UnmatchedRoute)
	}
	if strings.Contains(fmt.Sprint(spans[0]), "pathcanary") {
		t.Errorf("the raw path reached the span: %v", spans[0])
	}
}

func TestProbePathsMakeNoSpanAndEveryOtherPathDoes(t *testing.T) {
	read := tracedEnv(t, "1")
	routes := []Route{okRoute(http.MethodGet, "/health/other"), okRoute(http.MethodGet, "/healthz")}
	for _, probe := range ProbePaths {
		routes = append(routes, okRoute(http.MethodGet, probe), okRoute(http.MethodHead, probe))
	}
	handler := spanHandler(t, "public", false, routes...)
	for _, probe := range ProbePaths {
		get(handler, probe, nil)
		head := httptest.NewRequest(http.MethodHead, probe, nil)
		handler.ServeHTTP(httptest.NewRecorder(), head)
	}
	if spans := read(); len(spans) != 0 {
		t.Fatalf("probe paths produced %d spans: %v", len(spans), spans)
	}
	// Exact match only: a sibling path is a real route and stays traced.
	read = tracedEnv(t, "1")
	handler = spanHandler(t, "public", false, routes...)
	get(handler, "/health/other", nil)
	get(handler, "/healthz", nil)
	if spans := read(); len(spans) != 2 {
		t.Fatalf("non-probe siblings produced %d spans, want 2", len(spans))
	}
}

func TestAPanickingHandlerEndsItsSpanInErrorWithThe500(t *testing.T) {
	read := tracedEnv(t, "1")
	boom := Route{Method: http.MethodGet, Pattern: "/v1/boom", Handler: http.HandlerFunc(func(http.ResponseWriter, *http.Request) { panic("kaboom") })}
	if code := get(spanHandler(t, "public", false, boom), "/v1/boom", nil).Code; code != http.StatusInternalServerError {
		t.Fatalf("panic answered %d, want 500", code)
	}
	spans := read()
	if len(spans) != 1 {
		t.Fatalf("got %d spans, want 1", len(spans))
	}
	if spans[0].GetStatus().GetCode() != tracepb.Status_STATUS_CODE_ERROR {
		t.Errorf("span status = %v, want error", spans[0].GetStatus())
	}
	if status, _ := attrInt(spans[0], "http.response.status_code"); status != 500 {
		t.Errorf("status attribute = %d, want 500", status)
	}
	if spans[0].GetName() != "GET /v1/boom" {
		t.Errorf("name = %q, want the pattern", spans[0].GetName())
	}
}

func TestA5xxResponseIsAnErrorSpanAnd4xxIsNot(t *testing.T) {
	read := tracedEnv(t, "1")
	handler := spanHandler(t, "public", false,
		Route{Method: http.MethodGet, Pattern: "/v1/five", Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusBadGateway) })},
		Route{Method: http.MethodGet, Pattern: "/v1/four", Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNotFound) })})
	get(handler, "/v1/five", nil)
	get(handler, "/v1/four", nil)
	byName := map[string]*tracepb.Span{}
	for _, span := range read() {
		byName[span.GetName()] = span
	}
	if byName["GET /v1/five"].GetStatus().GetCode() != tracepb.Status_STATUS_CODE_ERROR {
		t.Errorf("502 span status = %v, want error", byName["GET /v1/five"].GetStatus())
	}
	if byName["GET /v1/four"].GetStatus().GetCode() == tracepb.Status_STATUS_CODE_ERROR {
		t.Errorf("404 span is an error span; only 5xx should be")
	}
}

// TestACallerCannotForceRecordingOnAnOuterListener is the traceparent rule:
// on the public and billing-edge listeners (TrustRemoteSampling off) the
// caller keeps its trace id but its sampled flag is replaced by this process's
// own root-sampler decision.
func TestACallerCannotForceRecordingOnAnOuterListener(t *testing.T) {
	for _, listener := range []string{"public", "billing-edge"} {
		t.Run(listener+"/ratio 0 records nothing for N sampled parents", func(t *testing.T) {
			read := tracedEnv(t, "0")
			handler := spanHandler(t, listener, false, itemsRoute())
			for index := 0; index < 25; index++ {
				traceID := fmt.Sprintf("%032x", index+1)
				get(handler, "/v1/items/1", map[string]string{"traceparent": sampledParent(traceID, "00f067aa0ba902b7"), "tracestate": "vendor=forced"})
			}
			if spans := read(); len(spans) != 0 {
				t.Fatalf("%d spans recorded at ratio 0 for sampled traceparents: the caller forced recording", len(spans))
			}
		})
		t.Run(listener+"/ratio 1 records and keeps the caller's trace id", func(t *testing.T) {
			read := tracedEnv(t, "1")
			handler := spanHandler(t, listener, false, itemsRoute())
			const traceID = "4bf92f3577b34da6a3ce929d0e0e4736"
			get(handler, "/v1/items/1", map[string]string{"traceparent": sampledParent(traceID, "00f067aa0ba902b7"), "tracestate": "vendor=forced"})
			spans := read()
			if len(spans) != 1 {
				t.Fatalf("got %d spans, want 1", len(spans))
			}
			if got := hex.EncodeToString(spans[0].GetTraceId()); got != traceID {
				t.Errorf("trace id = %s, want the caller's %s", got, traceID)
			}
			if got := hex.EncodeToString(spans[0].GetParentSpanId()); got != "00f067aa0ba902b7" {
				t.Errorf("parent span id = %s, want the caller's span", got)
			}
			if spans[0].GetTraceState() != "" {
				t.Errorf("trace state = %q: the caller's tracestate must not be carried on an outer listener", spans[0].GetTraceState())
			}
		})
	}
}

func TestTheInternalListenerHonoursTheCallersSamplingDecision(t *testing.T) {
	const traceID = "4bf92f3577b34da6a3ce929d0e0e4736"
	t.Run("sampled parent is recorded at ratio 0", func(t *testing.T) {
		read := tracedEnv(t, "0")
		get(spanHandler(t, "internal", true, itemsRoute()), "/v1/items/1", map[string]string{"traceparent": sampledParent(traceID, "00f067aa0ba902b7")})
		spans := read()
		if len(spans) != 1 || hex.EncodeToString(spans[0].GetTraceId()) != traceID {
			t.Fatalf("spans = %v, want one under the caller's trace", spans)
		}
	})
	t.Run("unsampled parent is not recorded at ratio 1", func(t *testing.T) {
		read := tracedEnv(t, "1")
		get(spanHandler(t, "internal", true, itemsRoute()), "/v1/items/1", map[string]string{"traceparent": "00-" + traceID + "-00f067aa0ba902b7-00"})
		if spans := read(); len(spans) != 0 {
			t.Fatalf("an unsampled in-cluster parent still recorded %d spans", len(spans))
		}
	})
}

func TestAnOuterListenerWithoutATraceparentUsesTheRootSampler(t *testing.T) {
	read := tracedEnv(t, "1")
	get(spanHandler(t, "public", false, itemsRoute()), "/v1/items/1", nil)
	spans := read()
	if len(spans) != 1 || len(spans[0].GetParentSpanId()) != 0 {
		t.Fatalf("spans = %v, want one root span", spans)
	}
}
