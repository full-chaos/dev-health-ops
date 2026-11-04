package server

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-go/pep"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/pdp"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pdp/pdptest"
)

type gatedREST struct {
	handler http.Handler
	reader  *sdkmetric.ManualReader
	gate    *pdp.Gate
	f       *restFixture
	minter  *accessMinter
}

// newGatedREST is the REST fixture (GET/POST /api/v1/home behind authenticateRESTRequest) served through the plane
// handler with a gate of mode over a PEP client of base.
func newGatedREST(t *testing.T, mode, base string, routed bool) *gatedREST {
	t.Helper()
	quietStdLog(t)
	m := newAccessMinter(t)
	setup := testAccess(m.keys, nil, accessTestNow)
	if !routed {
		setup = nil
	}
	f := newRESTFixture(t, newAccessStore(), setup, true)
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	cfg := pdp.Config{Mode: mode, Meter: provider.Meter("test"), Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	if base != "" {
		client, err := pep.New(pep.Config{BaseURL: base, AllowPlainHTTP: true, HTTPClient: &http.Client{}, ClientService: pdp.ClientService, Timeout: time.Second})
		if err != nil {
			t.Fatal(err)
		}
		cfg.Decider = client
	}
	gate := pdp.New(cfg)
	return &gatedREST{handler: planeHandler(f.handler.(*http.ServeMux), gate), reader: reader, gate: gate, f: f, minter: m}
}

func (g *gatedREST) userToken() string { return g.minter.mint(ecUser, nil, nil) }

func (g *gatedREST) edgeToken(t *testing.T) string {
	return ecMintEdgeToken(t, ecEdgeClaims{sub: ecUser.String(), orgID: ecOrg.String(), tokenVersion: 5})
}

func (g *gatedREST) do(method, bearer string) answer {
	req := httptest.NewRequest(method, "/api/v1/home", nil)
	req.Header.Set("Authorization", "Bearer "+bearer)
	rec := httptest.NewRecorder()
	g.handler.ServeHTTP(rec, req)
	g.gate.Wait()
	body, _ := io.ReadAll(rec.Result().Body)
	return answer{rec.Code, strings.TrimSpace(string(body))}
}

func (g *gatedREST) counter(t *testing.T, name string) map[string]int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := g.reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	out := map[string]int64{}
	for _, scope := range rm.ScopeMetrics {
		for _, metric := range scope.Metrics {
			sum, ok := metric.Data.(metricdata.Sum[int64])
			if metric.Name != name || !ok {
				continue
			}
			for _, p := range sum.DataPoints {
				var parts []string
				for _, kv := range p.Attributes.ToSlice() {
					parts = append(parts, string(kv.Key)+"="+kv.Value.Emit())
				}
				out[strings.Join(parts, ",")] += p.Value
			}
		}
	}
	return out
}

var (
	gatedOK          = answer{http.StatusOK, `{"org":"` + ecOrg.String() + `"}`}
	gatedForbidden   = answer{http.StatusForbidden, `{"detail":"Not authorized"}`}
	gatedUnavailable = answer{http.StatusServiceUnavailable, `{"detail":"Authorization unavailable"}`}
)

// Enforce: a PDP deny is 403 and the handler is never reached; an allow serves.
func TestRESTPDPEnforceDenyIs403(t *testing.T) {
	srv := pdptest.New(t)
	g := newGatedREST(t, pdp.ModeEnforce, srv.URL, true)
	if got := g.do(http.MethodGet, g.userToken()); got != gatedOK {
		t.Fatalf("allow: got %+v, want %+v", got, gatedOK)
	}
	srv.Deny("role_lacks_action")
	if got := g.do(http.MethodPost, g.userToken()); got != gatedForbidden {
		t.Fatalf("deny: got %+v, want %+v", got, gatedForbidden)
	}
	got := srv.Requests()
	if len(got) != 2 || got[1].Action != "analytics.read" || got[1].OrganizationID != ecOrg.String() {
		t.Fatalf("PDP calls %+v", got)
	}
}

// Enforce: PDP down is 503, never a pass.
func TestRESTPDPEnforceDownIs503(t *testing.T) {
	closed := httptest.NewServer(http.NotFoundHandler())
	base := closed.URL
	closed.Close()
	g := newGatedREST(t, pdp.ModeEnforce, base, true)
	if got := g.do(http.MethodGet, g.userToken()); got != gatedUnavailable {
		t.Fatalf("PDP down: got %+v, want %+v", got, gatedUnavailable)
	}
}

// Shadow asks and counts, and never denies.
func TestRESTPDPShadowNeverDenies(t *testing.T) {
	srv := pdptest.New(t)
	srv.Deny("no_membership")
	g := newGatedREST(t, pdp.ModeShadow, srv.URL, true)
	if got := g.do(http.MethodGet, g.userToken()); got != gatedOK {
		t.Fatalf("shadow deny: got %+v, want %+v", got, gatedOK)
	}
	if n := len(srv.Requests()); n != 1 {
		t.Fatalf("shadow asked %d times, want 1", n)
	}
	if got := g.counter(t, "dev_health_query_api_pdp_results_total"); got["action=analytics.read,mode=shadow,result=mismatch"] != 1 {
		t.Fatalf("results %v", got)
	}
}

// Off, and no gate at all, ask nothing.
func TestRESTPDPOffAsksNothing(t *testing.T) {
	srv := pdptest.New(t)
	srv.Deny("no_membership")
	g := newGatedREST(t, pdp.ModeOff, "", true)
	if got := g.do(http.MethodGet, g.userToken()); got != gatedOK {
		t.Fatalf("off: got %+v, want %+v", got, gatedOK)
	}
	if got := g.f.do(http.MethodGet, g.userToken()); got != gatedOK {
		t.Fatalf("no gate: got %+v, want %+v", got, gatedOK)
	}
	if n := len(srv.Requests()); n != 0 {
		t.Fatalf("off asked %d times", n)
	}
}

// Misconfigured (a bad mode, or a mode on with a missing input): the user token is refused 503 on the gated route,
// and the start did not fail.
func TestRESTPDPMisconfiguredIs503(t *testing.T) {
	for _, mode := range []string{"banana", pdp.ModeEnforce, pdp.ModeShadow} {
		t.Run(mode, func(t *testing.T) {
			g := newGatedREST(t, mode, "", true)
			if got := g.do(http.MethodGet, g.userToken()); got != gatedUnavailable {
				t.Fatalf("misconfigured %s: got %+v, want %+v", mode, got, gatedUnavailable)
			}
		})
	}
}

// The exempt callers (HS256 edge token, routed or not, and the envelope) bypass the PDP even in enforce with a PDP
// that denies everything, and each is counted with its class.
func TestRESTPDPExemptCallersBypassAndCount(t *testing.T) {
	srv := pdptest.New(t)
	srv.Deny("role_lacks_action")
	for name, routed := range map[string]bool{"routed": true, "unrouted": false} {
		t.Run(name, func(t *testing.T) {
			g := newGatedREST(t, pdp.ModeEnforce, srv.URL, routed)
			if got := g.do(http.MethodGet, g.edgeToken(t)); got != gatedOK {
				t.Fatalf("edge token: got %+v, want %+v", got, gatedOK)
			}
			if got := g.do(http.MethodGet, g.f.envelope); got.Status != http.StatusOK {
				t.Fatalf("envelope: got %+v, want 200", got)
			}
			got := g.counter(t, "dev_health_query_api_pdp_exempt_total")
			if len(got) != 2 || got["action=analytics.read,class=ops_access_token_hs256"] != 1 || got["action=analytics.read,class=effective_principal_envelope"] != 1 {
				t.Fatalf("exempt %v, want one per class", got)
			}
		})
	}
	if n := len(srv.Requests()); n != 0 {
		t.Fatalf("an exempt caller asked the PDP %d times", n)
	}
}

// The plane handler puts the gate on every request, so each REST route reads the process's one gate.
func TestPlaneHandlerCarriesTheGate(t *testing.T) {
	gate := pdp.New(pdp.Config{Mode: pdp.ModeOff})
	var seen *pdp.Gate
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/home", func(w http.ResponseWriter, r *http.Request) { seen = pdp.FromContext(r.Context()) })
	planeHandler(mux, gate).ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/api/v1/home", nil))
	if seen != gate {
		t.Fatal("the plane handler did not put the gate on the request")
	}
}

// Build reads QUERY_API_PDP_MODE and never fails the start on a bad value: it logs one ERROR naming the variable.
func TestBuildWithABadPDPModeStillStarts(t *testing.T) {
	quietStdLog(t)
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })
	plane, err := Build(func(name string) string {
		if name == pdp.EnvMode {
			return "banana"
		}
		return ""
	})
	if err != nil {
		t.Fatalf("a bad %s failed the start: %v", pdp.EnvMode, err)
	}
	plane.Close()
	if text := logs.String(); strings.Count(text, "level=ERROR") != 1 || !strings.Contains(text, "variable="+pdp.EnvMode) {
		t.Fatalf("log %q, want one ERROR naming %s", text, pdp.EnvMode)
	}
}
