package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/authctx"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/home"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/routeswitch"
)

// TestHomeSwitchFromEnvDefaultsDisabled mirrors quadrant's own switch
// test: with no env var set, neither operation is enabled.
func TestHomeSwitchFromEnvDefaultsDisabled(t *testing.T) {
	sw := homeSwitchFromEnv()
	if sw.Enabled(homeGetOperation) || sw.Enabled(homePostOperation) {
		t.Fatal("expected both home operations disabled with no env var set")
	}
}

func TestHomeSwitchFromEnvEnabledViaEnvVar(t *testing.T) {
	t.Setenv(homeEnabledEnvVar, "true")
	sw := homeSwitchFromEnv()
	if !sw.Enabled(homeGetOperation) || !sw.Enabled(homePostOperation) {
		t.Fatal("expected both home operations enabled with GO_API_HOME_ENABLED=true")
	}
}

func TestHomeRouteUnreachableWhenSwitchDisabled(t *testing.T) {
	sw := routeswitch.NewDynamicSwitch()
	mux := routeswitch.NewMux(sw)
	reached := false
	mux.Register(homeGetOperation, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reached = true
		w.WriteHeader(http.StatusOK)
	}))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/home", nil)
	rec := httptest.NewRecorder()
	mux.Dispatch(homeGetOperation, rec, req)
	if reached {
		t.Fatal("registered handler ran despite the switch being disabled")
	}
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}
}

// emptyRowsHomeClient answers every ClickHouse call with zero rows --
// used where the test only cares about HTTP-layer plumbing (missing
// auth, missing/invalid params), never the resulting data, matching
// quadrant_route_test.go's own emptyRowsQuadrantClient precedent.
type emptyRowsHomeClient struct{}

func (emptyRowsHomeClient) Query(context.Context, string, []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	return emptyRowsHomeScanner{}, nil
}

type emptyRowsHomeScanner struct{}

func (emptyRowsHomeScanner) Next() bool        { return false }
func (emptyRowsHomeScanner) Scan(...any) error { return nil }
func (emptyRowsHomeScanner) Err() error        { return nil }
func (emptyRowsHomeScanner) Close() error      { return nil }

func TestNewHomeGetHandlerRequiresAuthContext(t *testing.T) {
	handler := newHomeGetHandler(emptyRowsHomeClient{}, nil)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/home", nil)
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
	if got, want := rec.Body.String(), `{"detail":{"message":"Not authenticated"}}`+"\n"; got != want {
		t.Fatalf("body = %q, want %q", got, want)
	}
}

func TestNewHomePostHandlerRequiresAuthContext(t *testing.T) {
	handler := newHomePostHandler(emptyRowsHomeClient{}, nil)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/home", strings.NewReader(`{"filters":{}}`))
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
}

// TestNewHomeGetHandlerBadRangeDaysIs422 pins a non-numeric range_days,
// matching quadrant_route_test.go's own live-captured shape for the
// same Pydantic int-coercion failure.
func TestNewHomeGetHandlerBadRangeDaysIs422(t *testing.T) {
	handler := newHomeGetHandler(emptyRowsHomeClient{}, nil)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/home?range_days=abc", nil)
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

// TestNewHomeGetHandlerBadCompareDaysIs422 is compare_days' own half --
// the field _filters_from_query passes that range_days' sibling test
// does not cover.
func TestNewHomeGetHandlerBadCompareDaysIs422(t *testing.T) {
	handler := newHomeGetHandler(emptyRowsHomeClient{}, nil)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/home?compare_days=xyz", nil)
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

// TestNewHomeGetHandlerInvalidScopeTypeIs503 pins _filters_from_query's
// own Pydantic ScopeFilter construction failure degrading to the
// generic 503 -- matching sankey_route.go's identical precedent for the
// same shared helper.
func TestNewHomeGetHandlerInvalidScopeTypeIs503(t *testing.T) {
	handler := newHomeGetHandler(emptyRowsHomeClient{}, nil)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/home?scope_type=bogus", nil)
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

// TestNewHomeGetHandlerHappyPathShape pins the response Content-Type,
// the deprecation header, and that the body decodes as a well-formed
// home.Response for a supported, empty-data request.
func TestNewHomeGetHandlerHappyPathShape(t *testing.T) {
	handler := newHomeGetHandler(emptyRowsHomeClient{}, nil)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/home?scope_type=org", nil)
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
	var resp home.Response
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode body: %v (body=%s)", err, rec.Body.String())
	}
}

// TestNewHomePostHandlerMissingBodyIs422 pins an empty POST body's
// missing-"body" shape, matching sankey_route.go's own precedent for
// HomeRequest's single required field.
func TestNewHomePostHandlerMissingBodyIs422(t *testing.T) {
	handler := newHomePostHandler(emptyRowsHomeClient{}, nil)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/home", nil)
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

// TestNewHomePostHandlerMissingFiltersIs422 pins a present-but-empty
// body object missing HomeRequest's own required "filters" field.
func TestNewHomePostHandlerMissingFiltersIs422(t *testing.T) {
	handler := newHomePostHandler(emptyRowsHomeClient{}, nil)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/home", strings.NewReader(`{}`))
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

// TestNewHomePostHandlerInvalidScopeLevelIs422 pins MetricFilter's own
// nested scope.level Literal validation, reused verbatim from
// pydantic_metric_filter.go (validateMetricFilter).
func TestNewHomePostHandlerInvalidScopeLevelIs422(t *testing.T) {
	handler := newHomePostHandler(emptyRowsHomeClient{}, nil)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/home", strings.NewReader(`{"filters":{"scope":{"level":"bogus"}}}`))
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

// TestNewHomePostHandlerBodyTooLargeIs413 pins the Go-side-only body
// size cap (homeMaxBodyBytes) -- Python's real endpoint has no
// equivalent limit (this route's own package doc comment).
func TestNewHomePostHandlerBodyTooLargeIs413(t *testing.T) {
	handler := newHomePostHandler(emptyRowsHomeClient{}, nil)
	oversized := strings.Repeat("a", homeMaxBodyBytes+1)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/home", strings.NewReader(`{"filters":{"why":{"work_category":["`+oversized+`"]}}}`))
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusRequestEntityTooLarge)
	}
}

// TestNewHomePostHandlerHappyPathShape mirrors the GET happy-path test
// for POST's own body-carrying request.
func TestNewHomePostHandlerHappyPathShape(t *testing.T) {
	handler := newHomePostHandler(emptyRowsHomeClient{}, nil)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/home", strings.NewReader(`{"filters":{}}`))
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if dep := rec.Header().Get("X-DevHealth-Deprecated"); dep != "" {
		t.Fatalf("POST must never carry the GET-only deprecation header, got %q", dep)
	}
	var resp home.Response
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode body: %v (body=%s)", err, rec.Body.String())
	}
}

// TestHomeFiltersFromMapAppliesTimeFilterDefaults pins TimeFilter/
// ScopeFilter's own Pydantic defaults for an absent field.
func TestHomeFiltersFromMapAppliesTimeFilterDefaults(t *testing.T) {
	f := homeFiltersFromMap(map[string]any{})
	if f.Time.RangeDays != 14 || f.Time.CompareDays != 14 || f.Scope.Level != "org" {
		t.Fatalf("homeFiltersFromMap({}) = %+v, want TimeFilter{14,14}/ScopeFilter{org}", f)
	}
}
