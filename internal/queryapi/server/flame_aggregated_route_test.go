package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"testing"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/routeswitch"
)

// TestFlameAggregatedSwitchFromEnvDefaultsDisabled mirrors
// TestQuadrantSwitchFromEnvDefaultsDisabled: with no env var set,
// flameAggregatedOperation is NOT enabled.
func TestFlameAggregatedSwitchFromEnvDefaultsDisabled(t *testing.T) {
	sw := flameAggregatedSwitchFromEnv(os.Getenv)
	if sw.Enabled(flameAggregatedOperation) {
		t.Fatal("expected flameAggregatedOperation to be disabled with no env var set")
	}
}

// TestFlameAggregatedSwitchFromEnvEnabledViaEnvVar is the other half.
func TestFlameAggregatedSwitchFromEnvEnabledViaEnvVar(t *testing.T) {
	t.Setenv(flameAggregatedEnabledEnvVar, "true")
	sw := flameAggregatedSwitchFromEnv(os.Getenv)
	if !sw.Enabled(flameAggregatedOperation) {
		t.Fatal("expected flameAggregatedOperation to be enabled with GO_API_FLAME_AGGREGATED_ENABLED=true")
	}
}

// TestFlameAggregatedRouteUnreachableWhenSwitchDisabled mirrors
// TestQuadrantRouteUnreachableWhenSwitchDisabled.
func TestFlameAggregatedRouteUnreachableWhenSwitchDisabled(t *testing.T) {
	sw := routeswitch.NewDynamicSwitch()
	mux := routeswitch.NewMux(sw)

	reached := false
	mux.Register(flameAggregatedOperation, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reached = true
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/flame/aggregated?mode=throughput", nil)
	rec := httptest.NewRecorder()
	mux.Dispatch(flameAggregatedOperation, rec, req)

	if reached {
		t.Fatal("registered handler ran despite the switch being disabled")
	}
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}
}

// TestFlameAggregatedRouteReachableWhenSwitchEnabled is the other half.
func TestFlameAggregatedRouteReachableWhenSwitchEnabled(t *testing.T) {
	sw := routeswitch.NewDynamicSwitch()
	sw.Set(flameAggregatedOperation, true)
	mux := routeswitch.NewMux(sw)

	reached := false
	mux.Register(flameAggregatedOperation, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reached = true
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/flame/aggregated?mode=throughput", nil)
	rec := httptest.NewRecorder()
	mux.Dispatch(flameAggregatedOperation, rec, req)

	if !reached {
		t.Fatal("registered handler did not run despite the switch being enabled")
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
}

// emptyRowsFlameAggregatedClient answers every ClickHouse call with zero
// rows -- used where the test only cares about HTTP-layer plumbing
// (missing auth, missing/invalid params), never the resulting data.
type emptyRowsFlameAggregatedClient struct{}

func (emptyRowsFlameAggregatedClient) Query(context.Context, string, []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	return emptyRowsFlameAggregatedScanner{}, nil
}

type emptyRowsFlameAggregatedScanner struct{}

func (emptyRowsFlameAggregatedScanner) Next() bool        { return false }
func (emptyRowsFlameAggregatedScanner) Scan(...any) error { return nil }
func (emptyRowsFlameAggregatedScanner) Err() error        { return nil }
func (emptyRowsFlameAggregatedScanner) Close() error      { return nil }

// TestNewFlameAggregatedWorkHandlerRequiresAuthContext pins that the work
// handler (reached only after buildFlameAggregatedRoute's entryHandler
// already authenticated) itself still refuses a request with no claims
// attached.
func TestNewFlameAggregatedWorkHandlerRequiresAuthContext(t *testing.T) {
	handler := newFlameAggregatedWorkHandler(emptyRowsFlameAggregatedClient{})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/flame/aggregated?mode=throughput", nil)
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
	if got, want := rec.Body.String(), `{"detail":{"message":"Not authenticated"}}`+"\n"; got != want {
		t.Fatalf("body = %q, want %q", got, want)
	}
}

// TestNewFlameAggregatedWorkHandlerRequiresMode pins the missing-`mode`
// 422, matching FastAPI's own RequestValidationError body for a required
// query param with no value on the wire.
func TestNewFlameAggregatedWorkHandlerRequiresMode(t *testing.T) {
	handler := newFlameAggregatedWorkHandler(emptyRowsFlameAggregatedClient{})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/flame/aggregated", nil)
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusUnprocessableEntity)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", ct)
	}
	var body pydanticValidationErrorBody
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v (body=%s)", err, rec.Body.String())
	}
	want := pydanticValidationErrorBody{Detail: []pydanticErrorDetail{
		{Type: "missing", Loc: []any{"query", "mode"}, Msg: "Field required", Input: nil},
	}}
	if !reflect.DeepEqual(body, want) {
		t.Fatalf("body = %+v, want %+v", body, want)
	}
}

// TestNewFlameAggregatedWorkHandlerAggregatesMultipleValidationErrors pins
// that multiple simultaneously-invalid fields are reported together in
// ONE 422, in the same order main.py's flame_aggregated() signature
// declares them (mode, start_date, end_date, range_days, ..., limit,
// min_value) -- never a fail-fast single-error response.
func TestNewFlameAggregatedWorkHandlerAggregatesMultipleValidationErrors(t *testing.T) {
	handler := newFlameAggregatedWorkHandler(emptyRowsFlameAggregatedClient{})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/flame/aggregated?start_date=bad&range_days=abc&limit=xyz&min_value=nope", nil)
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusUnprocessableEntity)
	}
	var body pydanticValidationErrorBody
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v (body=%s)", err, rec.Body.String())
	}
	if len(body.Detail) != 5 {
		t.Fatalf("detail has %d entries, want 5: %+v", len(body.Detail), body.Detail)
	}
	wantLocs := [][]any{
		{"query", "mode"}, {"query", "start_date"}, {"query", "range_days"},
		{"query", "limit"}, {"query", "min_value"},
	}
	for i, want := range wantLocs {
		if !reflect.DeepEqual(body.Detail[i].Loc, want) {
			t.Fatalf("detail[%d].Loc = %v, want %v", i, body.Detail[i].Loc, want)
		}
	}
}

// TestNewFlameAggregatedWorkHandlerComparativeParamIs400 pins
// _reject_comparative_params's 400 (main.py:202-208, called at
// main.py:836) -- checked AFTER FastAPI's own required-field validation
// already passed.
func TestNewFlameAggregatedWorkHandlerComparativeParamIs400(t *testing.T) {
	handler := newFlameAggregatedWorkHandler(emptyRowsFlameAggregatedClient{})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/flame/aggregated?mode=throughput&rank=1", nil)
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
	if got, want := rec.Body.String(), `{"detail":"Comparative parameters are not supported."}`+"\n"; got != want {
		t.Fatalf("body = %q, want %q", got, want)
	}
}

// TestNewFlameAggregatedWorkHandlerUnknownModeIs400 pins the mode-enum
// 400 (main.py:838-842), surfaced through internal/aggflame.AsRequestError.
func TestNewFlameAggregatedWorkHandlerUnknownModeIs400(t *testing.T) {
	handler := newFlameAggregatedWorkHandler(emptyRowsFlameAggregatedClient{})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/flame/aggregated?mode=not_a_real_mode", nil)
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
	if got, want := rec.Body.String(), `{"detail":"mode must be 'cycle_breakdown', 'code_hotspots', or 'throughput'"}`+"\n"; got != want {
		t.Fatalf("body = %q, want %q", got, want)
	}
}

// TestNewFlameAggregatedWorkHandlerHappyPathShape pins the response
// Content-Type and that the body decodes as a well-formed Response shape
// for a supported, empty-data request.
func TestNewFlameAggregatedWorkHandlerHappyPathShape(t *testing.T) {
	handler := newFlameAggregatedWorkHandler(emptyRowsFlameAggregatedClient{})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/flame/aggregated?mode=throughput", nil)
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", ct)
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v (body=%s)", err, rec.Body.String())
	}
	for _, key := range []string{"mode", "unit", "root", "meta"} {
		if _, ok := body[key]; !ok {
			t.Fatalf("body missing key %q: %+v", key, body)
		}
	}
	if body["mode"] != "throughput" {
		t.Fatalf("mode = %v, want throughput", body["mode"])
	}
}

// TestNewFlameAggregatedWorkHandlerClampsLimitAndMinValue pins main.py's
// own `limit=min(max(limit, 1), 1000)` / `min_value=max(min_value, 0)`
// clamps (main.py:867-868) -- an out-of-range value never reaches
// internal/aggflame as a 422/400, it is silently clamped, same as Python.
func TestNewFlameAggregatedWorkHandlerClampsLimitAndMinValue(t *testing.T) {
	handler := newFlameAggregatedWorkHandler(emptyRowsFlameAggregatedClient{})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/flame/aggregated?mode=code_hotspots&limit=5000&min_value=-5", nil)
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
}
