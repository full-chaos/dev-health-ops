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

// TestHeatmapSwitchFromEnvDefaultsDisabled mirrors
// TestQuadrantSwitchFromEnvDefaultsDisabled.
func TestHeatmapSwitchFromEnvDefaultsDisabled(t *testing.T) {
	sw := heatmapSwitchFromEnv(os.Getenv)
	if sw.Enabled(heatmapOperation) {
		t.Fatal("expected heatmapOperation to be disabled with no env var set")
	}
}

func TestHeatmapSwitchFromEnvEnabledViaEnvVar(t *testing.T) {
	t.Setenv(heatmapEnabledEnvVar, "true")
	sw := heatmapSwitchFromEnv(os.Getenv)
	if !sw.Enabled(heatmapOperation) {
		t.Fatal("expected heatmapOperation to be enabled with GO_API_HEATMAP_ENABLED=true")
	}
}

func TestHeatmapRouteUnreachableWhenSwitchDisabled(t *testing.T) {
	sw := routeswitch.NewDynamicSwitch()
	mux := routeswitch.NewMux(sw)

	reached := false
	mux.Register(heatmapOperation, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reached = true
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/heatmap?type=temporal_load&metric=review_wait_density", nil)
	rec := httptest.NewRecorder()
	mux.Dispatch(heatmapOperation, rec, req)

	if reached {
		t.Fatal("registered handler ran despite the switch being disabled")
	}
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}
}

func TestHeatmapRouteReachableWhenSwitchEnabled(t *testing.T) {
	sw := routeswitch.NewDynamicSwitch()
	sw.Set(heatmapOperation, true)
	mux := routeswitch.NewMux(sw)

	reached := false
	mux.Register(heatmapOperation, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reached = true
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/heatmap?type=temporal_load&metric=review_wait_density", nil)
	rec := httptest.NewRecorder()
	mux.Dispatch(heatmapOperation, rec, req)

	if !reached {
		t.Fatal("registered handler did not run despite the switch being enabled")
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
}

// emptyRowsHeatmapClient answers every ClickHouse call with zero rows --
// used where the test only cares about HTTP-layer plumbing.
type emptyRowsHeatmapClient struct{}

func (emptyRowsHeatmapClient) Query(context.Context, string, []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	return emptyRowsHeatmapScanner{}, nil
}

type emptyRowsHeatmapScanner struct{}

func (emptyRowsHeatmapScanner) Next() bool        { return false }
func (emptyRowsHeatmapScanner) Scan(...any) error { return nil }
func (emptyRowsHeatmapScanner) Err() error        { return nil }
func (emptyRowsHeatmapScanner) Close() error      { return nil }

func TestNewHeatmapWorkHandlerRequiresAuthContext(t *testing.T) {
	handler := newHeatmapWorkHandler(emptyRowsHeatmapClient{})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/heatmap?type=temporal_load&metric=review_wait_density", nil)
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
	if got, want := rec.Body.String(), `{"detail":{"message":"Not authenticated"}}`+"\n"; got != want {
		t.Fatalf("body = %q, want %q", got, want)
	}
}

// TestNewHeatmapWorkHandlerRequiresTypeAndMetric pins the aggregated
// missing-type/missing-metric 422, in Python's own field-declaration
// order (type, then metric).
func TestNewHeatmapWorkHandlerRequiresTypeAndMetric(t *testing.T) {
	handler := newHeatmapWorkHandler(emptyRowsHeatmapClient{})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/heatmap", nil)
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
	want := pydanticValidationErrorBody{Detail: []pydanticErrorDetail{
		{Type: "missing", Loc: []any{"query", "type"}, Msg: "Field required", Input: nil},
		{Type: "missing", Loc: []any{"query", "metric"}, Msg: "Field required", Input: nil},
	}}
	if !reflect.DeepEqual(body, want) {
		t.Fatalf("body = %+v, want %+v", body, want)
	}
}

// TestNewHeatmapWorkHandlerAggregatesMultipleValidationErrors mirrors
// quadrant's own aggregation test, extended with limit.
func TestNewHeatmapWorkHandlerAggregatesMultipleValidationErrors(t *testing.T) {
	handler := newHeatmapWorkHandler(emptyRowsHeatmapClient{})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/heatmap?range_days=abc&start_date=bad&limit=abc", nil)
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
	wantLocs := [][]any{
		{"query", "type"}, {"query", "metric"}, {"query", "range_days"},
		{"query", "start_date"}, {"query", "limit"},
	}
	if len(body.Detail) != len(wantLocs) {
		t.Fatalf("detail has %d entries, want %d: %+v", len(body.Detail), len(wantLocs), body.Detail)
	}
	for i, want := range wantLocs {
		if !reflect.DeepEqual(body.Detail[i].Loc, want) {
			t.Fatalf("detail[%d].Loc = %v, want %v", i, body.Detail[i].Loc, want)
		}
	}
}

// TestNewHeatmapWorkHandlerComparativeParamRejected pins
// _reject_comparative_params's 400, and that it fires AFTER field-level
// 422 validation would have passed (a well-formed request here).
func TestNewHeatmapWorkHandlerComparativeParamRejected(t *testing.T) {
	handler := newHeatmapWorkHandler(emptyRowsHeatmapClient{})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/heatmap?type=temporal_load&metric=review_wait_density&rank=1", nil)
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

// TestNewHeatmapWorkHandlerValidationBeforeComparativeReject pins that a
// 422-worthy request short-circuits BEFORE _reject_comparative_params
// ever runs -- matching FastAPI's own dependency-solving-then-body order
// (heatmap_route.go's own doc comment).
func TestNewHeatmapWorkHandlerValidationBeforeComparativeReject(t *testing.T) {
	handler := newHeatmapWorkHandler(emptyRowsHeatmapClient{})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/heatmap?rank=1", nil)
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want %d (validation must win over the comparative-param reject)", rec.Code, http.StatusUnprocessableEntity)
	}
}

// TestNewHeatmapWorkHandlerUnknownMetricIs404 pins the resolver's 404
// surfacing through the HTTP layer via heatmap.AsRequestError.
func TestNewHeatmapWorkHandlerUnknownMetricIs404(t *testing.T) {
	handler := newHeatmapWorkHandler(emptyRowsHeatmapClient{})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/heatmap?type=temporal_load&metric=not_a_real_metric", nil)
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}
	if got, want := rec.Body.String(), `{"detail":"Unknown heatmap metric"}`+"\n"; got != want {
		t.Fatalf("body = %q, want %q", got, want)
	}
}

// TestNewHeatmapWorkHandlerInvalidScopeTypeIs400 pins ScopeFilter's
// Literal-set validation surfacing as 400 "Invalid scope filter" for a
// scope_type outside {org,team,repo,service,developer}.
func TestNewHeatmapWorkHandlerInvalidScopeTypeIs400(t *testing.T) {
	handler := newHeatmapWorkHandler(emptyRowsHeatmapClient{})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/heatmap?type=temporal_load&metric=review_wait_density&scope_type=bogus", nil)
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
	if got, want := rec.Body.String(), `{"detail":"Invalid scope filter"}`+"\n"; got != want {
		t.Fatalf("body = %q, want %q", got, want)
	}
}

// TestNewHeatmapWorkHandlerIndividualRequiresDeveloperScope pins the
// individual/developer-scope cross-checks.
func TestNewHeatmapWorkHandlerIndividualRequiresDeveloperScope(t *testing.T) {
	handler := newHeatmapWorkHandler(emptyRowsHeatmapClient{})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/heatmap?type=individual&metric=active_hours&scope_type=org", nil)
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
	if got, want := rec.Body.String(), `{"detail":"Individual heatmaps require developer scope"}`+"\n"; got != want {
		t.Fatalf("body = %q, want %q", got, want)
	}
}

// TestNewHeatmapWorkHandlerDeveloperScopeRequiresIndividual is the other
// half of the cross-check.
func TestNewHeatmapWorkHandlerDeveloperScopeRequiresIndividual(t *testing.T) {
	handler := newHeatmapWorkHandler(emptyRowsHeatmapClient{})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/heatmap?type=temporal_load&metric=review_wait_density&scope_type=developer", nil)
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
	if got, want := rec.Body.String(), `{"detail":"Developer scope is only supported for individual heatmaps"}`+"\n"; got != want {
		t.Fatalf("body = %q, want %q", got, want)
	}
}

// TestNewHeatmapWorkHandlerIndividualRequiresScopeID pins the missing-
// person-id 400.
func TestNewHeatmapWorkHandlerIndividualRequiresScopeID(t *testing.T) {
	handler := newHeatmapWorkHandler(emptyRowsHeatmapClient{})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/heatmap?type=individual&metric=active_hours&scope_type=developer", nil)
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
	if got, want := rec.Body.String(), `{"detail":"Individual heatmaps require a person id"}`+"\n"; got != want {
		t.Fatalf("body = %q, want %q", got, want)
	}
}

// TestNewHeatmapWorkHandlerHappyPathShape pins the response Content-Type
// and that the body decodes as a well-formed HeatmapResponse shape for a
// supported, empty-data request.
func TestNewHeatmapWorkHandlerHappyPathShape(t *testing.T) {
	handler := newHeatmapWorkHandler(emptyRowsHeatmapClient{})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/heatmap?type=context_switch&metric=repo_touchpoints&scope_type=repo", nil)
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d (body=%s)", rec.Code, http.StatusOK, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", ct)
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v (body=%s)", err, rec.Body.String())
	}
	for _, key := range []string{"axes", "cells", "legend", "evidence"} {
		if _, ok := body[key]; !ok {
			t.Fatalf("body missing key %q: %+v", key, body)
		}
	}
	if body["evidence"] != nil {
		t.Fatalf("evidence = %v, want null (repo_touchpoints never sets it)", body["evidence"])
	}
}
