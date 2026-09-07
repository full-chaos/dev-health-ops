//go:build integration

package main

// CHAOS-5408 live proof, part (2) of team-lead's ask: chris's binding
// telemetry rule requires a rejection's telemetry to actually LAND
// somewhere collected, not just be constructed in memory -- the unit tests
// in internal/graph (org_scoping_span_sweep_telemetry_test.go,
// forecast_resolver_telemetry_test.go) already prove a span object with the
// right attributes gets built, using an in-memory tracer.SpanRecorder that
// never leaves the process. That is not the same claim as "this reaches a
// collector once wired the way main() wires it" -- this file is that
// second, stronger claim.
//
// No live collector was available to prove against directly: `docker ps`
// on this host found no otel-collector/signoz/tempo container running, and
// docker-compose.yml documents SigNoz as a SEPARATE stack an operator
// starts and network-connects manually (see that file's own "Observability
// -- SigNoz" comment block) -- it is not a service this repo's compose
// file brings up itself. So this test stands up a real, minimal
// OTLP/gRPC TraceService receiver in-process and speaks the actual wire
// protocol (go.opentelemetry.io/proto/otlp, the same package
// otlptracegrpc.New uses) to it -- a genuine collector, just not SigNoz
// specifically.

import (
	"context"
	"io"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/grpc"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/authctx"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graph"
	"github.com/full-chaos/dev-health-ops/internal/platform/tracing"
)

// stubTraceCollector implements coltracepb.TraceServiceServer -- the real
// OTLP/gRPC Export RPC, not a fake in-process shortcut -- and records every
// span it receives for the test to inspect.
type stubTraceCollector struct {
	coltracepb.UnimplementedTraceServiceServer
	mu    sync.Mutex
	spans []*tracepb.Span
}

func (s *stubTraceCollector) Export(_ context.Context, req *coltracepb.ExportTraceServiceRequest) (*coltracepb.ExportTraceServiceResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, rs := range req.GetResourceSpans() {
		for _, ss := range rs.GetScopeSpans() {
			s.spans = append(s.spans, ss.GetSpans()...)
		}
	}
	return &coltracepb.ExportTraceServiceResponse{}, nil
}

func (s *stubTraceCollector) received() []*tracepb.Span {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*tracepb.Span, len(s.spans))
	copy(out, s.spans)
	return out
}

func spanAttr(span *tracepb.Span, key string) (string, bool) {
	for _, kv := range span.GetAttributes() {
		if kv.GetKey() == key {
			return kv.GetValue().GetStringValue(), true
		}
	}
	return "", false
}

// TestOrgScopingDenialSpanReachesARealOTLPCollector installs the SDK
// TracerProvider the EXACT way main() does (tracing.InitWithServiceName,
// the identical call main() makes with the identical otelServiceName
// constant), fires one org-mismatch FeatureFlags call through the real
// graph.Resolver (nil ClickHouse -- the auth guard must short-circuit
// before it is ever touched, same discipline the unit tests already use),
// and asserts the denied span actually arrived at a real OTLP collector
// with its denial_reason attribute -- not that a span object was
// constructed, which the in-memory-recorder unit tests already cover.
func TestOrgScopingDenialSpanReachesARealOTLPCollector(t *testing.T) {
	collector := &stubTraceCollector{}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	server := grpc.NewServer()
	coltracepb.RegisterTraceServiceServer(server, collector)
	go func() { _ = server.Serve(lis) }()
	defer server.Stop()

	// OTEL_SERVICE_NAME deliberately left UNSET: proves InitWithServiceName's
	// fallback-name argument (otelServiceName, "dev-health-query-api") is what
	// actually takes effect when the env var is absent, matching production
	// today (deploy/go-api/compose-query-api.yml sets none of the three
	// OTEL_* vars at all -- see the PR body / context file for that finding).
	t.Setenv("OTEL_ENABLED", "true")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", lis.Addr().String()) // bare host:port form
	t.Setenv("OTEL_SAMPLE_RATE", "1")                            // AlwaysSample, deterministic

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	component := tracing.InitWithServiceName(logger, otelServiceName)
	t.Cleanup(func() { _ = component.Shutdown(context.Background()) })

	// Proof part (1) of team-lead's ask, inline: InitWithServiceName must
	// have actually installed the real SDK provider globally, not left the
	// no-op default in place -- every span internal/graph's resolvers start
	// depends on this exact assertion holding true in production.
	if _, ok := otel.GetTracerProvider().(*sdktrace.TracerProvider); !ok {
		t.Fatalf("otel.GetTracerProvider() is %T, want *sdktrace.TracerProvider -- tracing was not actually installed", otel.GetTracerProvider())
	}

	resolver := &graph.Resolver{}
	ctx := authctx.WithClaims(context.Background(), authctx.Claims{OrgID: "org-authorized"})
	if _, err := resolver.Query().FeatureFlags(ctx, "org-requested-different", nil, nil, nil, 10); err == nil {
		t.Fatal("expected an authorization rejection for a mismatched org id")
	}

	if err := component.Shutdown(context.Background()); err != nil {
		t.Fatalf("tracing shutdown (forces the final batch flush the test depends on): %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	var spans []*tracepb.Span
	for time.Now().Before(deadline) {
		spans = collector.received()
		if len(spans) > 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if len(spans) != 1 {
		t.Fatalf("collector received %d spans, want exactly 1", len(spans))
	}

	span := spans[0]
	if span.GetName() != "query-api.featureFlags" {
		t.Errorf("span name = %q, want query-api.featureFlags", span.GetName())
	}
	if outcome, _ := spanAttr(span, "outcome"); outcome != "denied" {
		t.Errorf("outcome attribute = %q, want denied", outcome)
	}
	if reason, _ := spanAttr(span, "denial_reason"); reason != "org_mismatch" {
		t.Errorf("denial_reason attribute = %q, want org_mismatch", reason)
	}
}
