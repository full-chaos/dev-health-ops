package server

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/vektah/gqlparser/v2/gqlerror"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace/noop"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/grpc"

	"github.com/99designs/gqlgen/graphql"

	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/platform/tracing"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph"
)

type listenerSpanCollector struct {
	coltracepb.UnimplementedTraceServiceServer
	mu    sync.Mutex
	spans []*tracepb.Span
}

func (c *listenerSpanCollector) Export(_ context.Context, req *coltracepb.ExportTraceServiceRequest) (*coltracepb.ExportTraceServiceResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, resource := range req.GetResourceSpans() {
		for _, scope := range resource.GetScopeSpans() {
			c.spans = append(c.spans, scope.GetSpans()...)
		}
	}
	return &coltracepb.ExportTraceServiceResponse{}, nil
}

// installSpanTracing installs tracing the way the shell does (the query-api's
// own default service name) against a real in-process OTLP collector at the
// given sample rate, and returns the flush-and-read function.
func installSpanTracing(t *testing.T, rate string) func() []*tracepb.Span {
	t.Helper()
	collector := &listenerSpanCollector{}
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
	t.Setenv("OTEL_SERVICE_NAME", "placeholder")
	_ = os.Unsetenv("OTEL_SERVICE_NAME")
	component := tracing.InitWithServiceName(slog.New(slog.NewTextHandler(io.Discard, nil)), "dev-health-query-api")
	flushed := false
	t.Cleanup(func() {
		_ = component.Shutdown(context.Background())
		server.Stop()
		otel.SetTracerProvider(noop.NewTracerProvider())
		otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator())
	})
	return func() []*tracepb.Span {
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
}

func spanString(span *tracepb.Span, key string) (string, bool) {
	for _, kv := range span.GetAttributes() {
		if kv.GetKey() == key {
			return kv.GetValue().GetStringValue(), true
		}
	}
	return "", false
}

func spanInt(span *tracepb.Span, key string) (int64, bool) {
	for _, kv := range span.GetAttributes() {
		if kv.GetKey() == key {
			if _, ok := kv.GetValue().GetValue().(*commonpb.AnyValue_IntValue); ok {
				return kv.GetValue().GetIntValue(), true
			}
		}
	}
	return 0, false
}

// queryMux is the shape of a query-api listener base: the routes by registered
// pattern, the probe paths the listeners also answer, and no catch-all.
func queryMux() *http.ServeMux {
	mux := http.NewServeMux()
	ok := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.Handle("/query", ok)
	mux.Handle("/healthz", ok)
	mux.Handle("/readyz", ok)
	mux.Handle("/metrics", ok)
	return mux
}

type listenerHandlers map[string]http.Handler

// realListenerHandlers returns the handlers of the three listeners as the
// query-api builds them (newListenerServers, newMCPListenerServer).
func realListenerHandlers(base http.Handler) listenerHandlers {
	public, internal := newListenerServers("127.0.0.1:0", "127.0.0.1:0", base, base)
	mcp := newMCPListenerServer("127.0.0.1:0", base)
	return listenerHandlers{"public": public.Handler, "internal": internal.Handler, "mcp": mcp.Handler}
}

func send(handler http.Handler, method, target string, headers map[string]string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(method, target, strings.NewReader(""))
	for name, value := range headers {
		request.Header.Set(name, value)
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	return recorder
}

// TestEveryQueryListenerExportsAServerSpanNamedByThePattern: each of the three
// real listeners delivers one server span for a routed request, named by the
// mux's registered pattern with the status code and its own listener
// attribute, none for the probe paths, "unmatched" for a path no route took,
// and no request value (path, query string, header) anywhere in the span.
func TestEveryQueryListenerExportsAServerSpanNamedByThePattern(t *testing.T) {
	read := installSpanTracing(t, "1")
	handlers := realListenerHandlers(queryMux())
	for listener, handler := range handlers {
		send(handler, http.MethodPost, "/query?token=querycanary", map[string]string{"Authorization": "Bearer headercanary"})
		// The literal paths the query listeners answer for the kubelet and the
		// scraper, not queryProbePaths: a path dropped from that variable must
		// fail here, not pass by being left out of both sides.
		for _, probe := range []string{"/healthz", "/readyz", "/metrics"} {
			send(handler, http.MethodGet, probe, nil)
		}
		send(handler, http.MethodGet, "/no/such/route-"+listener+"-pathcanary", nil)
	}
	spans := read()
	if want := 2 * len(handlers); len(spans) != want {
		t.Fatalf("got %d spans, want %d (a routed and an unmatched request per listener, none for /healthz /readyz /metrics): %v", len(spans), want, spans)
	}
	routed := map[string]bool{}
	for _, span := range spans {
		listener, _ := spanString(span, "dev_health.listener")
		switch span.GetName() {
		case "POST /query":
			routed[listener] = true
			if route, _ := spanString(span, "http.route"); route != "/query" {
				t.Errorf("%s: http.route = %q, want /query", listener, route)
			}
			if status, _ := spanInt(span, "http.response.status_code"); status != 200 {
				t.Errorf("%s: status = %d, want 200", listener, status)
			}
		case "GET unmatched":
		default:
			t.Errorf("unexpected span name %q", span.GetName())
		}
		if span.GetKind() != tracepb.Span_SPAN_KIND_SERVER {
			t.Errorf("%s: kind = %v, want server", span.GetName(), span.GetKind())
		}
		dump := fmt.Sprint(span)
		for _, canary := range []string{"querycanary", "headercanary", "pathcanary", "token"} {
			if strings.Contains(dump, canary) {
				t.Errorf("a request value (%q) reached the span: %s", canary, dump)
			}
		}
	}
	for listener := range handlers {
		if !routed[listener] {
			t.Errorf("no 'POST /query' span from the %s listener", listener)
		}
	}
}

// TestOnlyTheInternalQueryListenerHonoursACallersTraceparent: the public and mcp
// listeners ignore an incoming traceparent (a caller cannot force recording or
// join a trace it names); the internal listener, whose callers are in-cluster,
// keeps the caller's trace.
func TestOnlyTheInternalQueryListenerHonoursACallersTraceparent(t *testing.T) {
	const traceID = "4bf92f3577b34da6a3ce929d0e0e4736"
	parent := map[string]string{"traceparent": "00-" + traceID + "-00f067aa0ba902b7-01"}
	t.Run("ratio 0: only the internal listener records", func(t *testing.T) {
		read := installSpanTracing(t, "0")
		handlers := realListenerHandlers(queryMux())
		for _, handler := range handlers {
			send(handler, http.MethodPost, "/query", parent)
		}
		spans := read()
		if len(spans) != 1 {
			t.Fatalf("got %d spans at ratio 0, want exactly the internal listener's: %v", len(spans), spans)
		}
		if listener, _ := spanString(spans[0], "dev_health.listener"); listener != "internal" {
			t.Errorf("the recorded span is from %q, want internal", listener)
		}
		if got := hex.EncodeToString(spans[0].GetTraceId()); got != traceID {
			t.Errorf("trace id = %s, want the caller's %s", got, traceID)
		}
	})
	t.Run("ratio 1: public and mcp start their own trace", func(t *testing.T) {
		read := installSpanTracing(t, "1")
		handlers := realListenerHandlers(queryMux())
		send(handlers["public"], http.MethodPost, "/query", parent)
		send(handlers["mcp"], http.MethodPost, "/query", parent)
		for _, span := range read() {
			if hex.EncodeToString(span.GetTraceId()) == traceID || len(span.GetParentSpanId()) != 0 {
				t.Errorf("a span joined the caller's trace on an outer listener: %v", span)
			}
		}
	})
}

// TestAGraphQLResponseWithErrorsIsMarkedOnTheServerSpan: gqlgen answers a
// failing document with errors in the body; the server span carries their
// count (a number, never a message), so the error share is readable at the
// HTTP level. The real /query server (newGraphQLServer) is driven with a
// document its schema refuses.
func TestAGraphQLResponseWithErrorsIsMarkedOnTheServerSpan(t *testing.T) {
	read := installSpanTracing(t, "1")
	mux := http.NewServeMux()
	mux.Handle("/query", newGraphQLServer(&graph.Resolver{}))
	handler := httpapi.TraceHandler(mux, httpapi.TraceOptions{Listener: "public", ProbePaths: queryProbePaths})
	request := httptest.NewRequest(http.MethodPost, "/query", strings.NewReader(`{"query":"{ fieldThatDoesNotExistCanary }"}`))
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if !strings.Contains(recorder.Body.String(), "errors") {
		t.Fatalf("the document did not produce a GraphQL error response: %d %s", recorder.Code, recorder.Body.String())
	}
	spans := read()
	if len(spans) != 1 {
		t.Fatalf("got %d spans, want 1", len(spans))
	}
	count, ok := spanInt(spans[0], httpapi.GraphQLErrorCountAttribute)
	if !ok || count < 1 {
		t.Errorf("%s = %d (present %v), want >= 1", httpapi.GraphQLErrorCountAttribute, count, ok)
	}
	if strings.Contains(fmt.Sprint(spans[0]), "fieldThatDoesNotExistCanary") {
		t.Errorf("the document or the error message reached the span: %v", spans[0])
	}
}

// TestAGraphQLResponseWithoutErrorsCarriesAZeroCount: the attribute is a
// per-response count, so a clean response says zero rather than being silent.
func TestAGraphQLResponseWithoutErrorsCarriesAZeroCount(t *testing.T) {
	read := installSpanTracing(t, "1")
	probe := graphqlRecorderHandler(0)
	handler := httpapi.TraceHandler(probe, httpapi.TraceOptions{Listener: "public", ProbePaths: queryProbePaths})
	send(handler, http.MethodPost, "/query", nil)
	spans := read()
	if len(spans) != 1 {
		t.Fatalf("got %d spans, want 1", len(spans))
	}
	if count, ok := spanInt(spans[0], httpapi.GraphQLErrorCountAttribute); !ok || count != 0 {
		t.Errorf("%s = %d (present %v), want 0", httpapi.GraphQLErrorCountAttribute, count, ok)
	}
}

// graphqlRecorderHandler stands in for a GraphQL server whose response carried
// the given number of errors: it runs recordErrorCount exactly as gqlgen does.
func graphqlRecorderHandler(errorCount int) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/query", func(w http.ResponseWriter, r *http.Request) {
		response := &graphql.Response{}
		for i := 0; i < errorCount; i++ {
			response.Errors = append(response.Errors, &gqlerror.Error{Message: "x"})
		}
		_ = recordErrorCount(r.Context(), func(context.Context) *graphql.Response { return response })
		w.WriteHeader(http.StatusOK)
	})
	return mux
}

// TestTheMCPGraphQLServerMarksErrorsOnTheServerSpanToo: the MCP class has its
// own gqlgen server (newMCPGraphQLServer); its responses carry the error count
// the same way.
func TestTheMCPGraphQLServerMarksErrorsOnTheServerSpanToo(t *testing.T) {
	read := installSpanTracing(t, "1")
	schema := graph.NewExecutableSchema(graph.Config{Resolvers: &graph.Resolver{}})
	mux := http.NewServeMux()
	mux.Handle("/query", newMCPGraphQLServer(schema, mcpLimits{complexity: 1000, depth: 20}))
	handler := httpapi.TraceHandler(mux, httpapi.TraceOptions{Listener: "mcp", ProbePaths: queryProbePaths})
	request := httptest.NewRequest(http.MethodPost, "/query", strings.NewReader(`{"query":"{ fieldThatDoesNotExistCanary }"}`))
	request.Header.Set("Content-Type", "application/json")
	handler.ServeHTTP(httptest.NewRecorder(), request)
	spans := read()
	if len(spans) != 1 {
		t.Fatalf("got %d spans, want 1", len(spans))
	}
	if count, ok := spanInt(spans[0], httpapi.GraphQLErrorCountAttribute); !ok || count < 1 {
		t.Errorf("%s = %d (present %v), want >= 1", httpapi.GraphQLErrorCountAttribute, count, ok)
	}
}

// TestTheRealPlaneAndListenersNameTheSpanByTheInnermostPattern drives the
// chain the binary actually serves (Build, then Listeners with the operator
// "extra" handler on /healthz and /readyz, the response-model marker, the
// identity middleware) and checks the span carries the pattern of the
// innermost mux that took the request, not the catch-all "/" of an outer one.
// A layer that copied the request between the muxes would turn this red.
func TestTheRealPlaneAndListenersNameTheSpanByTheInnermostPattern(t *testing.T) {
	read := installSpanTracing(t, "1")
	plane, err := Build(func(string) string { return "" })
	if err != nil {
		t.Fatal(err)
	}
	defer plane.Close()
	extra := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	public, internal := Listeners("127.0.0.1:0", "127.0.0.1:0", plane, extra, nil)
	for _, listener := range []*Listener{public, internal} {
		send(listener.server.Handler, http.MethodGet, "/api/v1/not-a-route", nil)
		send(listener.server.Handler, http.MethodGet, "/healthz", nil)
	}
	spans := read()
	if len(spans) != 2 {
		t.Fatalf("got %d spans, want 2 (the catch-all request on each listener, none for /healthz): %v", len(spans), spans)
	}
	for _, span := range spans {
		if span.GetName() != "GET /api/v1/" {
			t.Errorf("span name = %q, want the innermost mux's pattern %q (not the outer catch-all %q)", span.GetName(), "GET /api/v1/", "GET /")
		}
	}
}
