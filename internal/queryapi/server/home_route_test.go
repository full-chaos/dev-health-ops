package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"testing"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/home"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/routeswitch"
)

// TestHomeSwitchFromEnvDefaultsDisabled mirrors quadrant's own switch
// test: with no env var set, neither operation is enabled.
func TestHomeSwitchFromEnvDefaultsDisabled(t *testing.T) {
	sw := homeSwitchFromEnv(os.Getenv)
	if sw.Enabled(homeGetOperation) || sw.Enabled(homePostOperation) {
		t.Fatal("expected both home operations disabled with no env var set")
	}
}

func TestHomeSwitchFromEnvEnabledViaEnvVar(t *testing.T) {
	t.Setenv(homeEnabledEnvVar, "true")
	sw := homeSwitchFromEnv(os.Getenv)
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

// homeCoverageRowsClient supplies only the three aggregate reads that
// fetchCoverage performs. Every other Home reader sees no rows, so these
// tests exercise the real HTTP handler and JSON encoder while controlling
// each coverage denominator independently.
type homeCoverageRowsClient struct {
	reposCovered, reposTotal float64
	linked, workItems        float64
	withCycle, cycleItems    float64
}

func (c homeCoverageRowsClient) Query(_ context.Context, query string, _ []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	switch {
	case strings.Contains(query, "countDistinct(id)) AS total"):
		return &homeCoverageRowsScanner{values: []float64{c.reposTotal}}, nil
	case strings.Contains(query, "countDistinct(repo_id)) AS covered"):
		return &homeCoverageRowsScanner{values: []float64{c.reposCovered}}, nil
	case strings.Contains(query, "countIf(work_scope_id != '')"):
		return &homeCoverageRowsScanner{values: []float64{c.linked, c.workItems}}, nil
	case strings.Contains(query, "countIf(cycle_time_hours IS NOT NULL)"):
		return &homeCoverageRowsScanner{values: []float64{c.withCycle, c.cycleItems}}, nil
	default:
		return emptyRowsHomeScanner{}, nil
	}
}

type homeCoverageRowsScanner struct {
	values  []float64
	scanned bool
}

func (s *homeCoverageRowsScanner) Next() bool {
	if s.scanned {
		return false
	}
	s.scanned = true
	return true
}

func (s *homeCoverageRowsScanner) Scan(dest ...any) error {
	for index, destination := range dest {
		*destination.(*float64) = s.values[index]
	}
	return nil
}

func (*homeCoverageRowsScanner) Err() error   { return nil }
func (*homeCoverageRowsScanner) Close() error { return nil }

func TestNewHomeGetHandlerRequiresAuthContext(t *testing.T) {
	handler := newHomeGetHandler(emptyRowsHomeClient{}, nil)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/home", nil)
	rec := httptest.NewRecorder()
	serveRoute(t, handler, rec, req)
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
	serveRoute(t, handler, rec, req)
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
	serveRoute(t, handler, rec, req)
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
	serveRoute(t, handler, rec, req)
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
	serveRoute(t, handler, rec, req)
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
	serveRoute(t, handler, rec, req)
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
	var body struct {
		Deltas      []map[string]any `json:"deltas"`
		Constraint  map[string]any   `json:"constraint"`
		HealthState struct {
			Status string `json:"status"`
		} `json:"health_state"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode REST body: %v", err)
	}
	if body.HealthState.Status != "no_data" {
		t.Fatalf("health_state.status = %q, want no_data", body.HealthState.Status)
	}
	if body.Constraint == nil || body.Constraint["title"] != "" || body.Constraint["claim"] != "" {
		t.Fatalf("no-data REST constraint = %#v, want an empty legacy card", body.Constraint)
	}
	// CHAOS-9044: every REST delta carries the three Go-only fields after the
	// frozen ones, so a client can tell no data from a measured zero.
	for _, delta := range body.Deltas {
		for _, key := range []string{"has_data", "has_prior_data", "rate_state"} {
			if _, ok := delta[key]; !ok {
				t.Fatalf("REST delta lacks %s: %#v", key, delta)
			}
		}
		if delta["has_data"] != false || delta["has_prior_data"] != false || delta["delta_pct"] != nil {
			t.Fatalf("a delta of an empty organization = %#v, want has_data false, has_prior_data false and delta_pct null (a percent has no meaning without data; CHAOS-9111)", delta)
		}
	}
}

func TestNewHomeGetHandlerCoverageDistinguishesUnavailableFromObservedZero(t *testing.T) {
	tests := []struct {
		name   string
		client homeCoverageRowsClient
		expect map[string]*float64
	}{
		{
			name:   "each zero denominator is null",
			client: homeCoverageRowsClient{},
			expect: map[string]*float64{
				"repos_covered_pct": nil, "prs_linked_to_issues_pct": nil, "issues_with_cycle_states_pct": nil,
			},
		},
		{
			name:   "observed zero stays numeric zero",
			client: homeCoverageRowsClient{reposTotal: 2, workItems: 3, cycleItems: 3},
			expect: map[string]*float64{
				"repos_covered_pct": float64Pointer(0), "prs_linked_to_issues_pct": float64Pointer(0), "issues_with_cycle_states_pct": float64Pointer(0),
			},
		},
		{
			name:   "one unavailable denominator does not collapse the coverage object",
			client: homeCoverageRowsClient{workItems: 3, withCycle: 1, cycleItems: 2},
			expect: map[string]*float64{
				"repos_covered_pct": nil, "prs_linked_to_issues_pct": float64Pointer(0), "issues_with_cycle_states_pct": float64Pointer(50),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := newHomeGetHandler(tt.client, nil)
			req := httptest.NewRequest(http.MethodGet, "/api/v1/home?scope_type=org", nil)
			req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
			rec := httptest.NewRecorder()
			serveRoute(t, handler, rec, req)
			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
			}

			var body struct {
				Freshness struct {
					Coverage map[string]any `json:"coverage"`
				} `json:"freshness"`
			}
			if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
				t.Fatalf("decode body: %v; body=%s", err, rec.Body.String())
			}
			for field, want := range tt.expect {
				got, exists := body.Freshness.Coverage[field]
				if !exists {
					t.Errorf("freshness.coverage.%s is absent; want explicit JSON value", field)
					continue
				}
				if want == nil {
					if got != nil {
						t.Errorf("freshness.coverage.%s = %v, want null", field, got)
					}
					continue
				}
				value, ok := got.(float64)
				if !ok || value != *want {
					t.Errorf("freshness.coverage.%s = %v, want numeric %v", field, got, *want)
				}
			}
		})
	}
}

func float64Pointer(value float64) *float64 { return &value }

// TestNewHomePostHandlerMissingBodyIs422 pins an empty POST body's
// missing-"body" shape, matching sankey_route.go's own precedent for
// HomeRequest's single required field.
func TestNewHomePostHandlerMissingBodyIs422(t *testing.T) {
	handler := newHomePostHandler(emptyRowsHomeClient{}, nil)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/home", nil)
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
	rec := httptest.NewRecorder()
	serveRoute(t, handler, rec, req)
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
	serveRoute(t, handler, rec, req)
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
	serveRoute(t, handler, rec, req)
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
	serveRoute(t, handler, rec, req)
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
	serveRoute(t, handler, rec, req)
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
