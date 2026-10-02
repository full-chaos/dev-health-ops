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
	// The FIELDS a request value could ride on, never the span's own random
	// trace/span id bytes or timestamps (fmt.Sprint(span) holds them, and "9183"
	// appears in them by chance: CHAOS-7912).
	dump := spanFieldText(span)
	for _, canary := range []string{"9183", "querycanary", "headercanary", "orgcanary", "token"} {
		if strings.Contains(dump, canary) {
			t.Errorf("the span carries %q, a request value: %s", canary, dump)
		}
	}
}

// spanFieldText is every field of the span that can carry a request value: the
// name, the attributes (key and value), the events (name and attributes), the
// links' attributes, the trace state and the status message. It leaves out the
// trace id, span id, parent span id and the timestamps: random or clock
// bytes/numbers that can contain any digit string by chance.
func spanFieldText(span *tracepb.Span) string {
	parts := []string{span.GetName(), span.GetTraceState(), span.GetStatus().GetMessage()}
	addAttrs := func(attrs []*commonpb.KeyValue) {
		for _, kv := range attrs {
			parts = append(parts, kv.GetKey(), fmt.Sprint(kv.GetValue()))
		}
	}
	addAttrs(span.GetAttributes())
	for _, event := range span.GetEvents() {
		parts = append(parts, event.GetName())
		addAttrs(event.GetAttributes())
	}
	for _, link := range span.GetLinks() {
		parts = append(parts, link.GetTraceState())
		addAttrs(link.GetAttributes())
	}
	return strings.Join(parts, "\n")
}

// CHAOS-7912: the guard still sees a request value in any field, and is blind to
// the ids and timestamps that merely contain the digits.
func TestSpanFieldTextSeesEveryRequestCarryingFieldAndNoIDOrTime(t *testing.T) {
	str := func(v string) *commonpb.AnyValue {
		return &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: v}}
	}
	benign := func() *tracepb.Span {
		return &tracepb.Span{
			Name:              "GET /v1/items/{id}",
			TraceId:           []byte("9183918391839183"),
			SpanId:            []byte("91839183"),
			ParentSpanId:      []byte("91839183"),
			StartTimeUnixNano: 1791839183000000000,
			EndTimeUnixNano:   1791839183918300000,
			Attributes:        []*commonpb.KeyValue{{Key: "http.route", Value: str("/v1/items/{id}")}},
		}
	}
	if text := spanFieldText(benign()); strings.Contains(text, "9183") {
		t.Fatalf("ids/times leaked into the field text: %q", text)
	}
	if !strings.Contains(fmt.Sprint(benign()), "9183") {
		t.Fatal("the control no longer shows why fmt.Sprint(span) is the wrong haystack")
	}
	carriers := map[string]func(*tracepb.Span){
		"name": func(sp *tracepb.Span) { sp.Name = "GET /v1/items/9183" },
		"attribute value": func(sp *tracepb.Span) {
			sp.Attributes = append(sp.Attributes, &commonpb.KeyValue{Key: "url.path", Value: str("/v1/items/9183")})
		},
		"attribute key": func(sp *tracepb.Span) {
			sp.Attributes = append(sp.Attributes, &commonpb.KeyValue{Key: "item.9183", Value: str("x")})
		},
		"event name": func(sp *tracepb.Span) { sp.Events = []*tracepb.Span_Event{{Name: "saw 9183"}} },
		"event attribute": func(sp *tracepb.Span) {
			sp.Events = []*tracepb.Span_Event{{Name: "e", Attributes: []*commonpb.KeyValue{{Key: "k", Value: str("9183")}}}}
		},
		"link attribute": func(sp *tracepb.Span) {
			sp.Links = []*tracepb.Span_Link{{Attributes: []*commonpb.KeyValue{{Key: "k", Value: str("9183")}}}}
		},
		"trace state":    func(sp *tracepb.Span) { sp.TraceState = "vendor=9183" },
		"status message": func(sp *tracepb.Span) { sp.Status = &tracepb.Status{Message: "failed for 9183"} },
	}
	for name, set := range carriers {
		span := benign()
		set(span)
		if !strings.Contains(spanFieldText(span), "9183") {
			t.Errorf("a request value in the span's %s is not seen by the guard", name)
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

// TestAnOuterListenerIgnoresAnIncomingTraceparent is the traceparent rule: on
// the public and billing-edge listeners (TrustRemoteSampling off) the incoming
// traceparent is not extracted at all, so a caller chooses neither the trace id
// nor the sampling decision.
func TestAnOuterListenerIgnoresAnIncomingTraceparent(t *testing.T) {
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
		t.Run(listener+"/ratio 1 records roots under a local trace id, never the caller's", func(t *testing.T) {
			read := tracedEnv(t, "1")
			handler := spanHandler(t, listener, false, itemsRoute())
			callerIDs := map[string]bool{}
			for index := 0; index < 10; index++ {
				traceID := fmt.Sprintf("%032x", 0x4bf92f3577b34da6+index)
				callerIDs[traceID] = true
				get(handler, "/v1/items/1", map[string]string{"traceparent": sampledParent(traceID, "00f067aa0ba902b7"), "tracestate": "vendor=forced"})
			}
			spans := read()
			if len(spans) != 10 {
				t.Fatalf("got %d spans, want 10 roots", len(spans))
			}
			for _, span := range spans {
				if callerIDs[hex.EncodeToString(span.GetTraceId())] {
					t.Errorf("span joined the caller's trace %x: a caller can write spans into a trace id it names", span.GetTraceId())
				}
				if len(span.GetParentSpanId()) != 0 {
					t.Errorf("span has parent %x, want a root", span.GetParentSpanId())
				}
				if span.GetTraceState() != "" {
					t.Errorf("trace state = %q, want none", span.GetTraceState())
				}
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

// TestTheInternalListenerNeverExportsTheCallersTracestate: the in-cluster
// caller's trace id, parent span id and sampled flag are used, but its
// tracestate header value must not reach the exported span.
func TestTheInternalListenerNeverExportsTheCallersTracestate(t *testing.T) {
	read := tracedEnv(t, "1")
	const traceID = "4bf92f3577b34da6a3ce929d0e0e4736"
	get(spanHandler(t, "internal", true, itemsRoute()), "/v1/items/1", map[string]string{
		"traceparent": sampledParent(traceID, "00f067aa0ba902b7"),
		"tracestate":  "vendor=tracestatecanary",
	})
	spans := read()
	if len(spans) != 1 {
		t.Fatalf("got %d spans, want 1", len(spans))
	}
	if got := hex.EncodeToString(spans[0].GetTraceId()); got != traceID {
		t.Errorf("trace id = %s, want the caller's %s", got, traceID)
	}
	if spans[0].GetTraceState() != "" || strings.Contains(fmt.Sprint(spans[0]), "tracestatecanary") {
		t.Errorf("the caller's tracestate reached the exported span: %q", spans[0].GetTraceState())
	}
}

// TestAnAbortedHandlerEndsItsSpanInErrorWhateverWasCommitted: a handler that
// wrote a 200 and then aborted the response cut the client off, so its span is
// an error span even though the committed status is 200; the status that was
// committed stays on the span, as it stays on the metric.
func TestAnAbortedHandlerEndsItsSpanInErrorWhateverWasCommitted(t *testing.T) {
	read := tracedEnv(t, "1")
	abort := func(write bool) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			if write {
				_, _ = w.Write([]byte("partial"))
			}
			panic(http.ErrAbortHandler)
		})
	}
	handler := spanHandler(t, "public", false,
		Route{Method: http.MethodGet, Pattern: "/v1/abort-after-body", Handler: abort(true)},
		Route{Method: http.MethodGet, Pattern: "/v1/abort-before-body", Handler: abort(false)})
	for _, target := range []string{"/v1/abort-after-body", "/v1/abort-before-body"} {
		func() {
			defer func() {
				if recovered := recover(); recovered != http.ErrAbortHandler {
					t.Errorf("%s: recovered %v, want http.ErrAbortHandler re-panicked to net/http", target, recovered)
				}
			}()
			get(handler, target, nil)
		}()
	}
	byName := map[string]*tracepb.Span{}
	for _, span := range read() {
		byName[span.GetName()] = span
	}
	for _, name := range []string{"GET /v1/abort-after-body", "GET /v1/abort-before-body"} {
		span := byName[name]
		if span == nil {
			t.Fatalf("no span %q in %v", name, byName)
		}
		if span.GetStatus().GetCode() != tracepb.Status_STATUS_CODE_ERROR {
			t.Errorf("%s: span status = %v, want error", name, span.GetStatus())
		}
	}
	if status, _ := attrInt(byName["GET /v1/abort-after-body"], "http.response.status_code"); status != http.StatusOK {
		t.Errorf("committed status attribute = %d, want 200", status)
	}
	if _, has := attrInt(byName["GET /v1/abort-before-body"], "http.response.status_code"); has {
		t.Errorf("a response that committed nothing carries a status attribute")
	}
}
