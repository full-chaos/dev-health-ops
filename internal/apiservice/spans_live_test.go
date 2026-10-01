package apiservice

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace/noop"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/grpc"

	"github.com/full-chaos/dev-health-ops/internal/api/billing"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/tracing"
)

type liveCollector struct {
	coltracepb.UnimplementedTraceServiceServer
	mu       sync.Mutex
	spans    []*tracepb.Span
	services []string
}

func (c *liveCollector) Export(_ context.Context, req *coltracepb.ExportTraceServiceRequest) (*coltracepb.ExportTraceServiceResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, resource := range req.GetResourceSpans() {
		for _, kv := range resource.GetResource().GetAttributes() {
			if kv.GetKey() == "service.name" {
				c.services = append(c.services, kv.GetValue().GetStringValue())
			}
		}
		for _, scope := range resource.GetScopeSpans() {
			c.spans = append(c.spans, scope.GetSpans()...)
		}
	}
	return &coltracepb.ExportTraceServiceResponse{}, nil
}

func listenerOf(span *tracepb.Span) string {
	for _, kv := range span.GetAttributes() {
		if kv.GetKey() == "dev_health.listener" {
			return kv.GetValue().GetStringValue()
		}
	}
	return ""
}

// TestTheRealListenersExportServerSpansToACollector is the executed proof of
// the deployed claim: the env wiring of the go-api alone exported nothing
// (the review round of the chart change measured 0 spans) because no listener
// created a span. Built the way the api binary builds its three listeners
// (NewServer, NewEdgeServer, NewInternalServer) and installed the way the
// shell installs tracing (InitWithServiceName with the api's default name),
// each listener must deliver a server span to a real OTLP collector, under the
// api's own service name, while a probe path delivers none.
func TestTheRealListenersExportServerSpansToACollector(t *testing.T) {
	collector := installLiveTracing(t, "1")
	public, edge, internal := realListeners(t)
	for listener, handler := range map[string]http.Handler{"public": public, "billing-edge": edge, "internal": internal} {
		for _, path := range append(append([]string{}, httpapi.ProbePaths...), "/no/such/route-"+listener) {
			handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, path, nil))
		}
	}
	collector.flush(t)
	collector.mu.Lock()
	defer collector.mu.Unlock()
	if len(collector.spans) != 3 {
		t.Fatalf("collector received %d spans, want 3 (one per listener, none for the %d probe paths): %v", len(collector.spans), len(httpapi.ProbePaths), collector.spans)
	}
	seen := map[string]bool{}
	for _, span := range collector.spans {
		seen[listenerOf(span)] = true
		if span.GetName() != "GET unmatched" {
			t.Errorf("span name = %q, want %q", span.GetName(), "GET unmatched")
		}
	}
	for _, listener := range []string{"public", "billing-edge", "internal"} {
		if !seen[listener] {
			t.Errorf("no span from the %s listener: %v", listener, seen)
		}
	}
	for _, service := range collector.services {
		if service != config.APIServiceName {
			t.Errorf("resource service.name = %q, want %q", service, config.APIServiceName)
		}
	}
	if len(collector.services) == 0 {
		t.Error("no resource service.name reached the collector")
	}
}

// TestOnlyTheInternalListenerHonoursACallersSampledFlag pins the wiring the
// httpapi tests cannot see: which of the real listeners trusts a remote
// sampling decision. At a local ratio of 0, sampled traceparents recorded
// nothing on the public and billing-edge listeners and one span each on the
// internal one.
func TestOnlyTheInternalListenerHonoursACallersSampledFlag(t *testing.T) {
	collector := installLiveTracing(t, "0")
	public, edge, internal := realListeners(t)
	const requests = 10
	for listener, handler := range map[string]http.Handler{"public": public, "billing-edge": edge, "internal": internal} {
		for index := 0; index < requests; index++ {
			request := httptest.NewRequest(http.MethodGet, "/no/such/route-"+listener, nil)
			request.Header.Set("traceparent", fmt.Sprintf("00-%032x-00f067aa0ba902b7-01", index+1))
			handler.ServeHTTP(httptest.NewRecorder(), request)
		}
	}
	collector.flush(t)
	collector.mu.Lock()
	defer collector.mu.Unlock()
	count := map[string]int{}
	for _, span := range collector.spans {
		count[listenerOf(span)]++
	}
	if count["public"] != 0 || count["billing-edge"] != 0 {
		t.Errorf("an outside caller forced recording at ratio 0: %v", count)
	}
	if count["internal"] != requests {
		t.Errorf("internal listener recorded %d of %d sampled in-cluster parents at ratio 0", count["internal"], requests)
	}
}

func realListeners(t *testing.T) (public, edge, internal http.Handler) {
	t.Helper()
	cfg := config.Config{APIAddress: "127.0.0.1:0", APIBillingEdgeAddress: "127.0.0.1:0", APIInternalAddress: "127.0.0.1:0"}
	publicServer, err := NewServer(cfg, quietLog(), Routes(Deps{}, quietLog()))
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	edgeServer, err := NewEdgeServer(cfg, quietLog(), billing.EdgeRoutes(billing.Deps{}))
	if err != nil {
		t.Fatalf("NewEdgeServer: %v", err)
	}
	internalServer, err := NewInternalServer(cfg, quietLog(), InternalRoutes(Deps{}, quietLog()))
	if err != nil {
		t.Fatalf("NewInternalServer: %v", err)
	}
	return publicServer.Handler(), edgeServer.Handler(), internalServer.Handler()
}

type flushableCollector struct {
	*liveCollector
	component tracing.Component
}

func (c *flushableCollector) flush(t *testing.T) {
	t.Helper()
	if err := c.component.Shutdown(context.Background()); err != nil {
		t.Fatalf("flush spans: %v", err)
	}
}

// installLiveTracing starts a real OTLP collector and installs tracing the
// way the shell does, at the given OTEL_SAMPLE_RATE.
func installLiveTracing(t *testing.T, rate string) *flushableCollector {
	t.Helper()
	collector := &liveCollector{}
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
	// OTEL_SERVICE_NAME stays unset, as on the go-api container: the binary's
	// own default name is what the collector must see.
	t.Setenv("OTEL_SERVICE_NAME", "placeholder")
	_ = os.Unsetenv("OTEL_SERVICE_NAME")
	component := tracing.InitWithServiceName(quietLog(), config.APIServiceName)
	t.Cleanup(func() {
		_ = component.Shutdown(context.Background())
		server.Stop()
		otel.SetTracerProvider(noop.NewTracerProvider())
		otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator())
	})
	return &flushableCollector{liveCollector: collector, component: component}
}
