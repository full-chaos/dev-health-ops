package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/routeswitch"
)

// TestFilterOptionsSwitchFromEnvDefaultsDisabled mirrors
// TestQuadrantSwitchFromEnvDefaultsDisabled: with no env var set,
// filterOptionsOperation is NOT enabled.
func TestFilterOptionsSwitchFromEnvDefaultsDisabled(t *testing.T) {
	sw := filterOptionsSwitchFromEnv(os.Getenv)
	if sw.Enabled(filterOptionsOperation) {
		t.Fatal("expected filterOptionsOperation to be disabled with no env var set")
	}
}

// TestFilterOptionsSwitchFromEnvEnabledViaEnvVar is the other half.
func TestFilterOptionsSwitchFromEnvEnabledViaEnvVar(t *testing.T) {
	t.Setenv(filterOptionsEnabledEnvVar, "true")
	sw := filterOptionsSwitchFromEnv(os.Getenv)
	if !sw.Enabled(filterOptionsOperation) {
		t.Fatal("expected filterOptionsOperation to be enabled with GO_API_FILTER_OPTIONS_ENABLED=true")
	}
}

// TestFilterOptionsRouteUnreachableWhenSwitchDisabled mirrors
// TestQuadrantRouteUnreachableWhenSwitchDisabled: a registered handler
// dispatched through a Mux whose Switch reports the operation disabled
// must never run and must answer 404.
func TestFilterOptionsRouteUnreachableWhenSwitchDisabled(t *testing.T) {
	sw := routeswitch.NewDynamicSwitch()
	mux := routeswitch.NewMux(sw)

	reached := false
	mux.Register(filterOptionsOperation, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reached = true
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/filters/options", nil)
	rec := httptest.NewRecorder()
	mux.Dispatch(filterOptionsOperation, rec, req)

	if reached {
		t.Fatal("registered handler ran despite the switch being disabled")
	}
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}
}

// TestFilterOptionsRouteReachableWhenSwitchEnabled is the other half.
func TestFilterOptionsRouteReachableWhenSwitchEnabled(t *testing.T) {
	sw := routeswitch.NewDynamicSwitch()
	sw.Set(filterOptionsOperation, true)
	mux := routeswitch.NewMux(sw)

	reached := false
	mux.Register(filterOptionsOperation, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reached = true
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/filters/options", nil)
	rec := httptest.NewRecorder()
	mux.Dispatch(filterOptionsOperation, rec, req)

	if !reached {
		t.Fatal("registered handler did not run despite the switch being enabled")
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
}

// emptyRowsFilterOptionsClient answers every ClickHouse call with zero
// rows -- used where the test only cares about HTTP-layer plumbing
// (missing auth).
type emptyRowsFilterOptionsClient struct{}

func (emptyRowsFilterOptionsClient) Query(context.Context, string, []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	return emptyRowsFilterOptionsScanner{}, nil
}

type emptyRowsFilterOptionsScanner struct{}

func (emptyRowsFilterOptionsScanner) Next() bool        { return false }
func (emptyRowsFilterOptionsScanner) Scan(...any) error { return nil }
func (emptyRowsFilterOptionsScanner) Err() error        { return nil }
func (emptyRowsFilterOptionsScanner) Close() error      { return nil }

type erroringFilterOptionsClient struct{}

func (erroringFilterOptionsClient) Query(context.Context, string, []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	return nil, errors.New("clickhouse unavailable")
}

// TestNewFilterOptionsWorkHandlerRequiresAuthContext pins that the work
// handler (reached only after buildFilterOptionsRoute's entryHandler has
// already authenticated) itself still refuses a request with no claims
// attached -- the same defensive check newQuadrantWorkHandler makes.
func TestNewFilterOptionsWorkHandlerRequiresAuthContext(t *testing.T) {
	handler := newFilterOptionsWorkHandler(emptyRowsFilterOptionsClient{})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/filters/options", nil)
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
	if got, want := rec.Body.String(), `{"detail":{"message":"Not authenticated"}}`+"\n"; got != want {
		t.Fatalf("body = %q, want %q", got, want)
	}
}

// TestNewFilterOptionsWorkHandlerHappyPathShape pins the response
// Content-Type and that the body decodes with every list field present
// (empty arrays, never null/omitted) for an empty-data request.
func TestNewFilterOptionsWorkHandlerHappyPathShape(t *testing.T) {
	handler := newFilterOptionsWorkHandler(emptyRowsFilterOptionsClient{})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/filters/options", nil)
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
		Teams        []string `json:"teams"`
		Repos        []string `json:"repos"`
		Services     []string `json:"services"`
		Developers   []string `json:"developers"`
		WorkCategory []string `json:"work_category"`
		IssueType    []string `json:"issue_type"`
		FlowStage    []string `json:"flow_stage"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &decoded); err != nil {
		t.Fatalf("decode body: %v (body=%s)", err, rec.Body.String())
	}
	if decoded.Teams == nil || decoded.Repos == nil || decoded.Services == nil ||
		decoded.Developers == nil || decoded.IssueType == nil || decoded.FlowStage == nil {
		t.Fatal("every list field must be [] (present), not omitted/null")
	}
	if len(decoded.WorkCategory) != 20 {
		t.Fatalf("work_category len = %d, want 20 (static taxonomy, independent of ClickHouse rows)", len(decoded.WorkCategory))
	}
}

// TestNewFilterOptionsWorkHandlerClickHouseFailureIs503 pins Python's
// blanket "except Exception: raise HTTPException(503, 'Data unavailable')"
// (main.py:1466-1467) surfacing through the HTTP layer.
func TestNewFilterOptionsWorkHandlerClickHouseFailureIs503(t *testing.T) {
	handler := newFilterOptionsWorkHandler(erroringFilterOptionsClient{})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/filters/options", nil)
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
	if got, want := rec.Body.String(), `{"detail":"Data unavailable"}`+"\n"; got != want {
		t.Fatalf("body = %q, want %q", got, want)
	}
}

// TestBuildFilterOptionsRouteEntryHandlerRejectsNonGET pins the
// entryHandler's own method guard: 405, not 404, with Starlette's own
// default {"detail": "Method Not Allowed"} body -- confirmed live against
// the real FastAPI app (see this route set's TEST-EVIDENCE). Every env
// var here is a placeholder value: construction is lazy (no live
// ClickHouse/JWKS dial happens for a request this entryHandler rejects
// before Dispatch).
func TestBuildFilterOptionsRouteEntryHandlerRejectsNonGET(t *testing.T) {
	t.Setenv("CLICKHOUSE_URI", "clickhouse://localhost:8123/default")
	t.Setenv("GO_API_ENVELOPE_JWKS_PATH", filepath.Join(t.TempDir(), "missing-jwks.json"))
	t.Setenv("GO_API_ENVELOPE_ISSUER", "test-issuer")
	t.Setenv("GO_API_ENVELOPE_AUDIENCE", "test-audience")

	handler, cleanup, ok, err := buildFilterOptionsRoute(os.Getenv)
	if err != nil {
		t.Fatalf("buildFilterOptionsRoute: %v", err)
	}
	if !ok {
		t.Fatal("buildFilterOptionsRoute: ok = false, want true with every dependency env var set")
	}
	defer cleanup()

	req := httptest.NewRequest(http.MethodPost, "/api/v1/filters/options", nil)
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusMethodNotAllowed)
	}
	if got, want := rec.Body.String(), `{"detail":"Method Not Allowed"}`+"\n"; got != want {
		t.Fatalf("body = %q, want %q", got, want)
	}
}
