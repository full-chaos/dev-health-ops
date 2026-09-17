package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/authctx"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/opportunities"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/routeswitch"
)

// TestOpportunitiesSwitchFromEnvDefaultsDisabled mirrors home's own
// switch test: with no env var set, neither operation is enabled.
func TestOpportunitiesSwitchFromEnvDefaultsDisabled(t *testing.T) {
	sw := opportunitiesSwitchFromEnv()
	if sw.Enabled(opportunitiesGetOperation) || sw.Enabled(opportunitiesPostOperation) {
		t.Fatal("expected both opportunities operations disabled with no env var set")
	}
}

func TestOpportunitiesSwitchFromEnvEnabledViaEnvVar(t *testing.T) {
	t.Setenv(opportunitiesEnabledEnvVar, "true")
	sw := opportunitiesSwitchFromEnv()
	if !sw.Enabled(opportunitiesGetOperation) || !sw.Enabled(opportunitiesPostOperation) {
		t.Fatal("expected both opportunities operations enabled with GO_API_OPPORTUNITIES_ENABLED=true")
	}
}

func TestOpportunitiesRouteUnreachableWhenSwitchDisabled(t *testing.T) {
	sw := routeswitch.NewDynamicSwitch()
	mux := routeswitch.NewMux(sw)
	reached := false
	mux.Register(opportunitiesGetOperation, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reached = true
		w.WriteHeader(http.StatusOK)
	}))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/opportunities", nil)
	rec := httptest.NewRecorder()
	mux.Dispatch(opportunitiesGetOperation, rec, req)
	if reached {
		t.Fatal("registered handler ran despite the switch being disabled")
	}
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}
}

// emptyRowsHomeClient (home_route_test.go) answers every ClickHouse call
// with zero rows -- reused here rather than re-declared, since this
// route composes home.BuildResponse against the same home.QueryClient
// interface.

func TestNewOpportunitiesGetHandlerRequiresAuthContext(t *testing.T) {
	handler := newOpportunitiesGetHandler(emptyRowsHomeClient{})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/opportunities", nil)
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
	if got, want := rec.Body.String(), `{"detail":{"message":"Not authenticated"}}`+"\n"; got != want {
		t.Fatalf("body = %q, want %q", got, want)
	}
}

func TestNewOpportunitiesPostHandlerRequiresAuthContext(t *testing.T) {
	handler := newOpportunitiesPostHandler(emptyRowsHomeClient{})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/opportunities", strings.NewReader(`{"filters":{}}`))
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
}

// TestNewOpportunitiesGetHandlerBadRangeDaysIs422 pins a non-numeric
// range_days, matching home_route_test.go's own live-captured shape for
// the same Pydantic int-coercion failure (shared _filters_from_query).
func TestNewOpportunitiesGetHandlerBadRangeDaysIs422(t *testing.T) {
	handler := newOpportunitiesGetHandler(emptyRowsHomeClient{})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/opportunities?range_days=abc", nil)
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
		{
			Type: "int_parsing", Loc: []any{"query", "range_days"},
			Msg: "Input should be a valid integer, unable to parse string as an integer", Input: "abc",
		},
	}}
	if !reflect.DeepEqual(body, want) {
		t.Fatalf("body = %+v, want %+v", body, want)
	}
}

// TestNewOpportunitiesGetHandlerBadCompareDaysIs422 is compare_days' own
// half -- the field _filters_from_query passes that range_days' sibling
// test does not cover.
func TestNewOpportunitiesGetHandlerBadCompareDaysIs422(t *testing.T) {
	handler := newOpportunitiesGetHandler(emptyRowsHomeClient{})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/opportunities?compare_days=xyz", nil)
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
	if len(body.Detail) != 1 || !reflect.DeepEqual(body.Detail[0].Loc, []any{"query", "compare_days"}) {
		t.Fatalf("detail = %+v, want one compare_days entry", body.Detail)
	}
}

// TestNewOpportunitiesGetHandlerInvalidScopeTypeIs503 pins
// _filters_from_query's own Pydantic ScopeFilter construction failure
// degrading to the generic 503 -- matching home_route_test.go's
// identical precedent for the same shared helper.
func TestNewOpportunitiesGetHandlerInvalidScopeTypeIs503(t *testing.T) {
	handler := newOpportunitiesGetHandler(emptyRowsHomeClient{})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/opportunities?scope_type=bogus", nil)
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

// TestNewOpportunitiesGetHandlerHappyPathShape pins the response
// Content-Type, the deprecation header, and that the body decodes as a
// well-formed opportunities.Response for a supported, empty-data
// request -- with zero ClickHouse rows, home.BuildResponse's own deltas
// are all zero-valued (delta_pct == 0), so this exercises the
// "Maintain steady flow" fallback card end to end through the real mux
// wiring.
func TestNewOpportunitiesGetHandlerHappyPathShape(t *testing.T) {
	handler := newOpportunitiesGetHandler(emptyRowsHomeClient{})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/opportunities?scope_type=org", nil)
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", ct)
	}
	if dep := rec.Header().Get("X-DevHealth-Deprecated"); dep != "use POST with filters" {
		t.Fatalf("X-DevHealth-Deprecated = %q, want %q", dep, "use POST with filters")
	}
	var resp opportunities.Response
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode body: %v (body=%s)", err, rec.Body.String())
	}
	if len(resp.Items) != 1 || resp.Items[0].ID != "opp-0" {
		t.Fatalf("Items = %+v, want the single opp-0 fallback card", resp.Items)
	}
}

// TestNewOpportunitiesPostHandlerMissingBodyIs422 pins an empty POST
// body's missing-"body" shape, matching home_route_test.go's own
// precedent for HomeRequest's single required field (opportunities_post
// carries the identical HomeRequest body).
func TestNewOpportunitiesPostHandlerMissingBodyIs422(t *testing.T) {
	handler := newOpportunitiesPostHandler(emptyRowsHomeClient{})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/opportunities", nil)
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
		{Type: "missing", Loc: []any{"body"}, Msg: "Field required", Input: nil},
	}}
	if !reflect.DeepEqual(body, want) {
		t.Fatalf("body = %+v, want %+v", body, want)
	}
}

// TestNewOpportunitiesPostHandlerMissingFiltersIs422 pins a
// present-but-empty body object missing HomeRequest's own required
// "filters" field.
func TestNewOpportunitiesPostHandlerMissingFiltersIs422(t *testing.T) {
	handler := newOpportunitiesPostHandler(emptyRowsHomeClient{})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/opportunities", strings.NewReader(`{}`))
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
		{Type: "missing", Loc: []any{"body", "filters"}, Msg: "Field required", Input: map[string]any{}},
	}}
	if !reflect.DeepEqual(body, want) {
		t.Fatalf("body = %+v, want %+v", body, want)
	}
}

// TestNewOpportunitiesPostHandlerInvalidScopeLevelIs422 pins
// MetricFilter's own nested scope.level Literal validation, reused
// verbatim from pydantic_metric_filter.go (validateMetricFilter).
func TestNewOpportunitiesPostHandlerInvalidScopeLevelIs422(t *testing.T) {
	handler := newOpportunitiesPostHandler(emptyRowsHomeClient{})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/opportunities", strings.NewReader(`{"filters":{"scope":{"level":"bogus"}}}`))
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want %d, body=%s", rec.Code, http.StatusUnprocessableEntity, rec.Body.String())
	}
	var body pydanticValidationErrorBody
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v (body=%s)", err, rec.Body.String())
	}
	if len(body.Detail) != 1 || !reflect.DeepEqual(body.Detail[0].Loc, []any{"body", "filters", "scope", "level"}) {
		t.Fatalf("detail = %+v, want one scope.level entry", body.Detail)
	}
}

// TestNewOpportunitiesPostHandlerBodyTooLargeIs413 pins the Go-side-only
// body size cap (opportunitiesMaxBodyBytes) -- Python's real endpoint
// has no equivalent limit (this route's own package doc comment).
func TestNewOpportunitiesPostHandlerBodyTooLargeIs413(t *testing.T) {
	handler := newOpportunitiesPostHandler(emptyRowsHomeClient{})
	oversized := strings.Repeat("a", opportunitiesMaxBodyBytes+1)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/opportunities", strings.NewReader(`{"filters":{"why":{"work_category":["`+oversized+`"]}}}`))
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusRequestEntityTooLarge)
	}
}

// TestNewOpportunitiesPostHandlerHappyPathShape mirrors the GET
// happy-path test for POST's own body-carrying request.
func TestNewOpportunitiesPostHandlerHappyPathShape(t *testing.T) {
	handler := newOpportunitiesPostHandler(emptyRowsHomeClient{})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/opportunities", strings.NewReader(`{"filters":{}}`))
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if dep := rec.Header().Get("X-DevHealth-Deprecated"); dep != "" {
		t.Fatalf("POST must never carry the GET-only deprecation header, got %q", dep)
	}
	var resp opportunities.Response
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode body: %v (body=%s)", err, rec.Body.String())
	}
}
