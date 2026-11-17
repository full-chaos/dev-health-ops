package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/internalidentity"
)

// CHAOS-13950: the internal listener refuses the header carrier's elevated claims by the MCP listener's rule and
// reason (one predicate, carrierElevatedClaims), counts them by claim, and both listeners record the carrier's claims as
// bounded span attributes.

const elevatedClaimsMetric = "devhealth_query_api_internal_elevated_claims_total"

type carrierRow struct {
	name, role, superuser, impersonation string
	claims                               []string // "" = not elevated
}

var carrierRows = []carrierRow{
	{"acr's real header set", "viewer", "false", "false", nil},
	{"member", "member", "false", "false", nil},
	{"role admin", "admin", "false", "false", []string{"role_admin"}},
	{"role owner", "Owner", "false", "false", []string{"role_owner"}},
	{"role operator", " operator ", "false", "false", []string{"role_operator"}},
	{"superuser", "member", "true", "false", []string{"superuser"}},
	{"impersonation", "member", "false", "true", []string{"impersonation"}},
	{"all three", "admin", "true", "true", []string{"superuser", "impersonation", "role_admin"}},
}

func internalCarrierRequest(role, superuser, impersonation string) *http.Request {
	r := httptest.NewRequest(http.MethodPost, "/query/run-operation", nil)
	for key, values := range mcpHeaders(mcpTestOrg, role, superuser, impersonation) {
		r.Header[key] = values
	}
	var marked *http.Request
	internalidentity.Internal(http.HandlerFunc(func(_ http.ResponseWriter, in *http.Request) { marked = in })).ServeHTTP(httptest.NewRecorder(), r)
	return marked
}

func elevatedClaimDelta(before, after map[string]int64) map[string]int64 {
	out := map[string]int64{}
	for key, v := range after {
		if len(key) > len(elevatedClaimsMetric) && key[:len(elevatedClaimsMetric)+1] == elevatedClaimsMetric+"|" {
			if d := v - before[key]; d != 0 {
				out[key] = d
			}
		}
	}
	return out
}

func TestInternalCarrierRefusesElevatedClaims(t *testing.T) {
	for _, row := range carrierRows {
		t.Run(row.name, func(t *testing.T) {
			before := shadowCounters(t)
			w := httptest.NewRecorder()
			claims, ok := authenticateInternalRequest(w, internalCarrierRequest(row.role, row.superuser, row.impersonation), nil, nil, nil)
			delta := elevatedClaimDelta(before, shadowCounters(t))
			if row.claims == nil {
				if !ok || claims.OrgID != mcpTestOrg || len(delta) != 0 {
					t.Fatalf("accepted %v claims %+v status %d counted %v, want accepted for the header org and nothing counted", ok, claims, w.Code, delta)
				}
				return
			}
			if ok || w.Code != http.StatusForbidden {
				t.Fatalf("accepted %v status %d, want 403", ok, w.Code)
			}
			want := map[string]int64{}
			for _, claim := range row.claims {
				want[elevatedClaimsMetric+"|claim="+claim] = 1
			}
			if len(delta) != len(want) {
				t.Fatalf("counted %v, want %v", delta, want)
			}
			for key, n := range want {
				if delta[key] != n {
					t.Fatalf("counted %v, want %v", delta, want)
				}
			}
		})
	}
}

// The MCP listener and the internal listener refuse exactly the same claim sets.
func TestBothListenersShareTheElevatedClaimRule(t *testing.T) {
	for _, row := range carrierRows {
		w := httptest.NewRecorder()
		_, internalOK := authenticateInternalRequest(w, internalCarrierRequest(row.role, row.superuser, row.impersonation), nil, nil, nil)
		mcp := httptest.NewRequest(http.MethodPost, "/query", nil)
		mcp.Header = mcpHeaders(mcpTestOrg, row.role, row.superuser, row.impersonation)
		_, status, reason := mcpAuthenticate(mcp)
		mcpRefused := status == http.StatusForbidden && reason == mcpReasonElevatedClaim
		if internalOK == mcpRefused || (row.claims != nil) != mcpRefused {
			t.Errorf("%s: internal accepted %v, MCP refused %v (%d %s), want both to follow the one rule (elevated %v)",
				row.name, internalOK, mcpRefused, status, reason, row.claims != nil)
		}
	}
}

func carrierSpanAttributes(t *testing.T, run func(ctx context.Context)) map[string]string {
	t.Helper()
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	ctx, span := provider.Tracer("carrier-claims-test").Start(context.Background(), "request")
	run(ctx)
	span.End()
	spans := exporter.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("exported %d spans, want 1", len(spans))
	}
	out := map[string]string{}
	for _, kv := range spans[0].Attributes {
		out[string(kv.Key)] = kv.Value.Emit()
	}
	return out
}

// Both sites record dev_health.internal.role/.superuser/.impersonation, bounded, with no org value, for an accepted and
// for a refused carrier alike.
func TestCarrierClaimsAreBoundedSpanAttributes(t *testing.T) {
	for _, c := range []struct {
		role, superuser, impersonation string
		want                           map[string]string
	}{
		{"viewer", "false", "false", map[string]string{attrInternalRole: "viewer", attrInternalSuperuser: "false", attrInternalImpersonation: "false"}},
		{"Owner", "true", "true", map[string]string{attrInternalRole: "owner", attrInternalSuperuser: "true", attrInternalImpersonation: "true"}},
		{"", "false", "false", map[string]string{attrInternalRole: "none", attrInternalSuperuser: "false", attrInternalImpersonation: "false"}},
		{"billing-manager-of-" + mcpTestOrg, "false", "true", map[string]string{attrInternalRole: "other", attrInternalSuperuser: "false", attrInternalImpersonation: "true"}},
	} {
		sites := map[string]func(ctx context.Context){
			"internal": func(ctx context.Context) {
				authenticateInternalRequest(httptest.NewRecorder(), internalCarrierRequest(c.role, c.superuser, c.impersonation).WithContext(
					contextWithInternal(ctx)), nil, nil, nil)
			},
			"mcp": func(ctx context.Context) {
				r := httptest.NewRequest(http.MethodPost, "/query", nil).WithContext(ctx)
				r.Header = mcpHeaders(mcpTestOrg, c.role, c.superuser, c.impersonation)
				mcpAuthenticate(r)
			},
		}
		for site, run := range sites {
			got := carrierSpanAttributes(t, run)
			for key, want := range c.want {
				if got[key] != want {
					t.Errorf("%s role %q: %s = %q, want %q (all: %v)", site, c.role, key, got[key], want, got)
				}
			}
			for key, value := range got {
				if value == mcpTestOrg || (len(key) > 0 && attribute.Key(key) == "dev_health.internal.org") {
					t.Errorf("%s: the span carries the org: %s=%s", site, key, value)
				}
			}
		}
	}
}

// contextWithInternal marks ctx as arriving on the internal listener.
func contextWithInternal(ctx context.Context) context.Context {
	var marked context.Context
	internalidentity.Internal(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) { marked = r.Context() })).
		ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil).WithContext(ctx))
	return marked
}
