package server

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
)

// TestReportWindowOverflowIsThePython503 drives each route family to a
// report window Python's time_window cannot hold (a day count past
// timedelta's 999999999, or a date past 9999-12-31) and checks the Python
// route's answer: each computes the window inside `except Exception:
// raise HTTPException(503, "Data unavailable")`.
// Before, the day counts were clamped to the Go int range and the dates
// wrapped, so these were 200s over a nonsense window.
func TestReportWindowOverflowIsThePython503(t *testing.T) {
	t.Setenv("IDENTITY_MAPPING_PATH", t.TempDir()+"/missing.yaml")
	cases := []struct {
		name    string
		handler http.HandlerFunc
		method  string
		target  string
		body    string
		path    map[string]string
	}{
		{name: "work-units POST range_days past the Go int range", handler: newWorkUnitsPostHandler(newTestWorkUnitsReader(t)),
			method: http.MethodPost, target: "/api/v1/work-units", body: `{"filters":{"time":{"range_days":9223372036854775808}}}`},
		{name: "work-units POST compare_days past timedelta", handler: newWorkUnitsPostHandler(newTestWorkUnitsReader(t)),
			method: http.MethodPost, target: "/api/v1/work-units", body: `{"filters":{"time":{"compare_days":1000000000}}}`},
		{name: "drilldown PRs GET end_date 9999-12-31", handler: newDrilldownPRsGetHandler(newEmptyRowsDrilldownReader(t)),
			method: http.MethodGet, target: "/api/v1/drilldown/prs?scope_type=repo&scope_id=repo-a&end_date=9999-12-31"},
		{name: "drilldown PRs GET range past year 1", handler: newDrilldownPRsGetHandler(newEmptyRowsDrilldownReader(t)),
			method: http.MethodGet, target: "/api/v1/drilldown/prs?scope_type=repo&scope_id=repo-a&range_days=3000000"},
		{name: "drilldown PRs GET range_days past int64 (a valid pydantic int)", handler: newDrilldownPRsGetHandler(newEmptyRowsDrilldownReader(t)),
			method: http.MethodGet, target: "/api/v1/drilldown/prs?scope_type=repo&scope_id=repo-a&range_days=99999999999999999999"},
		{name: "home GET compare_days past int64", handler: newHomeGetHandler(emptyRowsHomeClient{}, nil),
			method: http.MethodGet, target: "/api/v1/home?compare_days=99999999999999999999"},
		{name: "investment flow POST range_days past the Go int range", handler: newInvestmentFlowHandler(emptyRowsInvestmentFlowClient{}),
			method: http.MethodPost, target: "/api/v1/investment/flow", body: `{"filters":{"time":{"range_days":9223372036854775807}}}`},
		{name: "investment flow repo-team POST compare_days past timedelta", handler: newInvestmentFlowRepoTeamHandler(emptyRowsInvestmentFlowClient{}),
			method: http.MethodPost, target: "/api/v1/investment/flow/repo-team", body: `{"filters":{"time":{"compare_days":1000000000}}}`},
		{name: "explain GET compare window past year 1", handler: newExplainGetHandler(newEmptyRowsExplainReader(t)),
			method: http.MethodGet, target: "/api/v1/explain?metric=cycle_time&scope_type=repo&scope_id=repo-a&end_date=0001-01-10&range_days=5"},
		{name: "home POST range_days past timedelta", handler: newHomePostHandler(emptyRowsHomeClient{}, nil),
			method: http.MethodPost, target: "/api/v1/home", body: `{"filters":{"time":{"range_days":1000000000}}}`},
		{name: "quadrant GET end_date 9999-12-31", handler: newQuadrantWorkHandler(emptyRowsQuadrantClient{}),
			method: http.MethodGet, target: "/api/v1/quadrant?type=wip_throughput&scope_type=repo&bucket=week&end_date=9999-12-31"},
		{name: "heatmap GET end_date 9999-12-31", handler: newHeatmapWorkHandler(emptyRowsHeatmapClient{}),
			method: http.MethodGet, target: "/api/v1/heatmap?type=context_switch&metric=repo_touchpoints&scope_type=repo&end_date=9999-12-31"},
		{name: "people summary GET range past year 1", handler: newPeopleSummaryHandler(newPeopleDetailFoundReader(t, "alice@example.com")),
			method: http.MethodGet, target: "/api/v1/people/anything/summary?range_days=3000000", path: map[string]string{"person_id": "anything"}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			req := httptest.NewRequest(test.method, test.target, strings.NewReader(test.body))
			if test.body != "" {
				req.Header.Set("Content-Type", "application/json")
			}
			for key, value := range test.path {
				req.SetPathValue(key, value)
			}
			req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
			rec := httptest.NewRecorder()
			test.handler(rec, req)
			if rec.Code != http.StatusServiceUnavailable || strings.TrimSpace(rec.Body.String()) != `{"detail":"Data unavailable"}` {
				t.Fatalf("got %d %q, want 503 {\"detail\":\"Data unavailable\"}", rec.Code, rec.Body.String())
			}
		})
	}
}

// TestAggregatedFlameWindowOverflowIsTheGeneric500: Python's aggregated
// flame route computes end_day - timedelta(days=range_days) BEFORE its
// try block, so an overflow there is the api's unhandled-exception 500.
func TestAggregatedFlameWindowOverflowIsTheGeneric500(t *testing.T) {
	for _, target := range []string{
		"/api/v1/flame/aggregated?mode=throughput&range_days=9223372036854775808",
		"/api/v1/flame/aggregated?mode=throughput&range_days=-9223372036854775809",
		"/api/v1/flame/aggregated?mode=throughput&range_days=1000000000&end_date=2026-01-01",
		"/api/v1/flame/aggregated?mode=throughput&range_days=-1&end_date=9999-12-31",
	} {
		handler := newFlameAggregatedWorkHandler(emptyRowsFlameAggregatedClient{})
		req := httptest.NewRequest(http.MethodGet, target, nil)
		req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
		rec := httptest.NewRecorder()
		handler(rec, req)
		if rec.Code != http.StatusInternalServerError || strings.TrimSpace(rec.Body.String()) != `{"detail":"Internal Server Error"}` {
			t.Errorf("%s: got %d %q, want 500 {\"detail\":\"Internal Server Error\"}", target, rec.Code, rec.Body.String())
		}
	}
	// A start_date means Python never computes end_day - range_days.
	handler := newFlameAggregatedWorkHandler(emptyRowsFlameAggregatedClient{})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/flame/aggregated?mode=throughput&range_days=9223372036854775808&start_date=2026-01-01", nil)
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("with start_date: got %d, want 200", rec.Code)
	}
}

// TestEmptyDayCountIsA422: FastAPI parses an explicit empty int query
// value and refuses it (int_parsing); it does not fall back to the
// default.
func TestEmptyDayCountIsA422(t *testing.T) {
	for name, test := range map[string]struct {
		handler http.HandlerFunc
		target  string
		field   string
	}{
		"drilldown prs range_days": {newDrilldownPRsGetHandler(newEmptyRowsDrilldownReader(t)), "/api/v1/drilldown/prs?scope_type=repo&scope_id=repo-a&range_days=", "range_days"},
		"home compare_days":        {newHomeGetHandler(emptyRowsHomeClient{}, nil), "/api/v1/home?compare_days=", "compare_days"},
		"explain compare_days":     {newExplainGetHandler(newEmptyRowsExplainReader(t)), "/api/v1/explain?metric=cycle_time&scope_type=repo&scope_id=repo-a&compare_days=", "compare_days"},
	} {
		req := httptest.NewRequest(http.MethodGet, test.target, nil)
		req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
		rec := httptest.NewRecorder()
		test.handler(rec, req)
		want := `"type":"int_parsing","loc":["query","` + test.field + `"]`
		if rec.Code != http.StatusUnprocessableEntity || !strings.Contains(rec.Body.String(), want) || !strings.Contains(rec.Body.String(), `"input":""`) {
			t.Errorf("%s: got %d %s, want 422 with %s and input \"\"", name, rec.Code, rec.Body.String(), want)
		}
	}
}
