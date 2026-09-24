package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
)

// loadIssuesValidationErrorGolden mirrors loadValidationErrorGolden
// (drilldown_prs_422_test.go)'s own decode-with-DisallowUnknownFields
// technique, pointed at this route's own fixture directory -- captured
// from the REAL Python app the same way (FastAPI TestClient, auth
// dependency overridden), see this file's own TEST-EVIDENCE citation in
// the PR body for the exact capture script.
func loadIssuesValidationErrorGolden(t *testing.T, name string) pydanticValidationErrorBody {
	t.Helper()
	data, err := os.ReadFile("testdata/drilldown_issues_422/" + name)
	if err != nil {
		t.Fatalf("read golden %s: %v", name, err)
	}
	var body pydanticValidationErrorBody
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&body); err != nil {
		t.Fatalf("decode golden %s: %v", name, err)
	}
	return body
}

// TestGetIssuesValidationErrorsMatchPython replays
// testdata/drilldown_issues_422/get_*.json against
// newDrilldownIssuesGetHandler -- byte-for-field-exact against the SAME
// live Python capture technique drilldown_prs_422_test.go's own GET
// coverage documents. These fixtures are byte-identical to
// drilldown_prs_422's own get_*.json (both routes validate the same
// query-param shape via the same FastAPI/Pydantic machinery, confirmed by
// diffing the two live captures), but this route's own handler is a
// SEPARATE Go implementation, so this test exercises that implementation
// directly rather than assuming parity from the sibling route's coverage.
func TestGetIssuesValidationErrorsMatchPython(t *testing.T) {
	cases := []struct {
		name   string
		query  string
		golden string
	}{
		{"malformed start_date", "start_date=not-a-date", "get_malformed_start_date.json"},
		{"malformed end_date", "end_date=2024-13-40", "get_malformed_end_date.json"},
		{"non-numeric range_days", "range_days=not-a-number", "get_non_numeric_range_days.json"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			handler := newDrilldownIssuesGetHandler(newEmptyRowsDrilldownIssuesReader(t))
			req := httptest.NewRequest(http.MethodGet, "/api/v1/drilldown/issues?"+tc.query, nil)
			req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
			rec := httptest.NewRecorder()
			handler(rec, req)

			if rec.Code != http.StatusUnprocessableEntity {
				t.Fatalf("status = %d, want 422, body=%s", rec.Code, rec.Body.String())
			}
			got := decodeValidationErrorBody(t, rec.Body.Bytes())
			want := loadIssuesValidationErrorGolden(t, tc.golden)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("response mismatch\n got:  %+v\nwant: %+v", got, want)
			}
		})
	}
}

// TestPostIssuesValidationErrorsMatchPython is the POST route's own copy,
// replaying testdata/drilldown_issues_422/post_*.json.
func TestPostIssuesValidationErrorsMatchPython(t *testing.T) {
	cases := []struct {
		name   string
		body   []byte
		golden string
	}{
		{"empty body", []byte(``), "post_empty_body.json"},
		{"missing filters", []byte(`{}`), "post_missing_filters.json"},
		{"filters wrong type", []byte(`{"filters":"nope"}`), "post_filters_wrong_type.json"},
		{"limit wrong type string", []byte(`{"filters":{},"limit":"not-a-number"}`), "post_limit_wrong_type_string.json"},
		{"limit fractional float", []byte(`{"filters":{},"limit":3.7}`), "post_limit_fractional_float.json"},
		{"limit wrong json type", []byte(`{"filters":{},"limit":[1,2]}`), "post_limit_wrong_json_type.json"},
		{"sort wrong type", []byte(`{"filters":{},"sort":123}`), "post_sort_wrong_type.json"},
		{"body not object", []byte(`[1,2,3]`), "post_body_not_object.json"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			handler := newDrilldownIssuesPostHandler(newEmptyRowsDrilldownIssuesReader(t))
			req := httptest.NewRequest(http.MethodPost, "/api/v1/drilldown/issues", bytes.NewReader(tc.body))
			req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
			rec := httptest.NewRecorder()
			handler(rec, req)

			if rec.Code != http.StatusUnprocessableEntity {
				t.Fatalf("status = %d, want 422, body=%s", rec.Code, rec.Body.String())
			}
			got := decodeValidationErrorBody(t, rec.Body.Bytes())
			want := loadIssuesValidationErrorGolden(t, tc.golden)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("response mismatch\n got:  %+v\nwant: %+v", got, want)
			}
		})
	}
}

// TestNestedMetricFilterFieldsMatchPythonIssues is
// TestNestedMetricFilterFieldsMatchPython's (drilldown_prs_422_test.go)
// counterpart for this route -- same shared validateMetricFilter
// (pydantic_metric_filter.go), exercised through the issues POST handler.
func TestNestedMetricFilterFieldsMatchPythonIssues(t *testing.T) {
	handler := newDrilldownIssuesPostHandler(newEmptyRowsDrilldownIssuesReader(t))
	body := `{"filters":{"scope":{"level":"not_a_real_level"},"time":{"range_days":"not-a-number"}}}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/drilldown/issues", bytes.NewReader([]byte(body)))
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
	rec := httptest.NewRecorder()
	handler(rec, req)

	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422, body=%s", rec.Code, rec.Body.String())
	}
	got := decodeValidationErrorBody(t, rec.Body.Bytes())
	want := pydanticValidationErrorBody{Detail: []pydanticErrorDetail{
		{
			Type: "int_parsing", Loc: []any{"body", "filters", "time", "range_days"},
			Msg: "Input should be a valid integer, unable to parse string as an integer", Input: "not-a-number",
		},
		{
			Type: "literal_error", Loc: []any{"body", "filters", "scope", "level"},
			Msg: "Input should be 'org', 'team', 'repo', 'service' or 'developer'", Input: "not_a_real_level",
			Ctx: map[string]string{"expected": "'org', 'team', 'repo', 'service' or 'developer'"},
		},
	}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("response mismatch\n got:  %+v\nwant: %+v", got, want)
	}
}

// TestMalformedJSONSyntaxMatchesPythonIssues is
// TestMalformedJSONSyntaxMatchesPython's (drilldown_prs_422_test.go)
// counterpart for this route.
func TestMalformedJSONSyntaxMatchesPythonIssues(t *testing.T) {
	handler := newDrilldownIssuesPostHandler(newEmptyRowsDrilldownIssuesReader(t))
	req := httptest.NewRequest(http.MethodPost, "/api/v1/drilldown/issues", bytes.NewReader([]byte(`not json`)))
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
	rec := httptest.NewRecorder()
	handler(rec, req)

	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422, body=%s", rec.Code, rec.Body.String())
	}
	got := decodeValidationErrorBody(t, rec.Body.Bytes())
	want := pydanticValidationErrorBody{Detail: []pydanticErrorDetail{{
		Type:  "json_invalid",
		Loc:   []any{"body", float64(0)},
		Msg:   "JSON decode error",
		Input: map[string]any{},
		Ctx:   map[string]string{"error": "Expecting value"},
	}}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("response mismatch\n got:  %+v\nwant: %+v", got, want)
	}
}
