package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/authctx"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/routeswitch"
)

// TestQuadrantSwitchFromEnvDefaultsDisabled mirrors
// TestInvestmentExplainSwitchFromEnvDefaultsDisabled: with no env var set,
// quadrantOperation is NOT enabled.
func TestQuadrantSwitchFromEnvDefaultsDisabled(t *testing.T) {
	sw := quadrantSwitchFromEnv()
	if sw.Enabled(quadrantOperation) {
		t.Fatal("expected quadrantOperation to be disabled with no env var set")
	}
}

// TestQuadrantSwitchFromEnvEnabledViaEnvVar is the other half.
func TestQuadrantSwitchFromEnvEnabledViaEnvVar(t *testing.T) {
	t.Setenv(quadrantEnabledEnvVar, "true")
	sw := quadrantSwitchFromEnv()
	if !sw.Enabled(quadrantOperation) {
		t.Fatal("expected quadrantOperation to be enabled with GO_API_QUADRANT_ENABLED=true")
	}
}

// TestQuadrantRouteUnreachableWhenSwitchDisabled mirrors
// TestInvestmentExplainRouteUnreachableWhenSwitchDisabled: a registered
// handler dispatched through a Mux whose Switch reports the operation
// disabled must never run and must answer 404.
func TestQuadrantRouteUnreachableWhenSwitchDisabled(t *testing.T) {
	sw := routeswitch.NewDynamicSwitch()
	mux := routeswitch.NewMux(sw)

	reached := false
	mux.Register(quadrantOperation, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reached = true
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/quadrant?type=wip_throughput", nil)
	rec := httptest.NewRecorder()
	mux.Dispatch(quadrantOperation, rec, req)

	if reached {
		t.Fatal("registered handler ran despite the switch being disabled")
	}
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}
}

// TestQuadrantRouteReachableWhenSwitchEnabled is the other half.
func TestQuadrantRouteReachableWhenSwitchEnabled(t *testing.T) {
	sw := routeswitch.NewDynamicSwitch()
	sw.Set(quadrantOperation, true)
	mux := routeswitch.NewMux(sw)

	reached := false
	mux.Register(quadrantOperation, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reached = true
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/quadrant?type=wip_throughput", nil)
	rec := httptest.NewRecorder()
	mux.Dispatch(quadrantOperation, rec, req)

	if !reached {
		t.Fatal("registered handler did not run despite the switch being enabled")
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
}

// emptyRowsQuadrantClient answers every ClickHouse call with zero rows --
// used where the test only cares about HTTP-layer plumbing (missing auth,
// missing/invalid params), never the resulting data.
type emptyRowsQuadrantClient struct{}

func (emptyRowsQuadrantClient) Query(context.Context, string, []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	return emptyRowsQuadrantScanner{}, nil
}

type emptyRowsQuadrantScanner struct{}

func (emptyRowsQuadrantScanner) Next() bool        { return false }
func (emptyRowsQuadrantScanner) Scan(...any) error { return nil }
func (emptyRowsQuadrantScanner) Err() error        { return nil }
func (emptyRowsQuadrantScanner) Close() error      { return nil }

// TestNewQuadrantWorkHandlerRequiresAuthContext pins that the work handler
// (reached only after buildQuadrantRoute's entryHandler already
// authenticated) itself still refuses a request with no claims attached --
// the same defensive check newInvestmentExplainWorkHandler makes.
func TestNewQuadrantWorkHandlerRequiresAuthContext(t *testing.T) {
	handler := newQuadrantWorkHandler(emptyRowsQuadrantClient{})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/quadrant?type=wip_throughput", nil)
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
}

// TestNewQuadrantWorkHandlerRequiresType pins the "type is required" 400.
func TestNewQuadrantWorkHandlerRequiresType(t *testing.T) {
	handler := newQuadrantWorkHandler(emptyRowsQuadrantClient{})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/quadrant", nil)
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

// TestNewQuadrantWorkHandlerUnknownTypeIs404 pins the resolver's 404
// surfacing through the HTTP layer via quadrant.AsRequestError.
func TestNewQuadrantWorkHandlerUnknownTypeIs404(t *testing.T) {
	handler := newQuadrantWorkHandler(emptyRowsQuadrantClient{})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/quadrant?type=not_a_real_type", nil)
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}
}

// TestNewQuadrantWorkHandlerHappyPathShape pins the response Content-Type
// and that the body decodes as a well-formed QuadrantResponse shape (axes/
// points/annotations present) for a supported, empty-data request.
func TestNewQuadrantWorkHandlerHappyPathShape(t *testing.T) {
	handler := newQuadrantWorkHandler(emptyRowsQuadrantClient{})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/quadrant?type=wip_throughput&scope_type=repo&bucket=week", nil)
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", ct)
	}
	var decoded struct {
		Axes        map[string]any   `json:"axes"`
		Points      []map[string]any `json:"points"`
		Annotations []map[string]any `json:"annotations"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &decoded); err != nil {
		t.Fatalf("decode body: %v (body=%s)", err, rec.Body.String())
	}
	if decoded.Axes == nil {
		t.Fatal("axes missing")
	}
	if decoded.Points == nil {
		t.Fatal("points must be [] (present), not omitted/null")
	}
	if decoded.Annotations == nil {
		t.Fatal("annotations must be [] (present), not omitted/null")
	}
}

// TestNewQuadrantWorkHandlerPersonScopeIsNotImplemented pins the
// documented scope gap surfacing through the HTTP layer as 501.
func TestNewQuadrantWorkHandlerPersonScopeIsNotImplemented(t *testing.T) {
	handler := newQuadrantWorkHandler(emptyRowsQuadrantClient{})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/quadrant?type=wip_throughput&scope_type=person", nil)
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusNotImplemented {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotImplemented)
	}
}
