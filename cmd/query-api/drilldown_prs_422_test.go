package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"testing"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/authctx"
)

// loadValidationErrorGolden decodes a testdata JSON file captured from the
// REAL Python app (FastAPI TestClient, auth dependency overridden, the
// SAME request this test issues against the Go handler) via a one-off,
// uncommitted invocation -- see this file's own TEST-EVIDENCE citation in
// the PR body for the exact capture script. DisallowUnknownFields makes a
// field this Go type does not declare a hard test failure, not a silent
// drop.
func loadValidationErrorGolden(t *testing.T, name string) pydanticValidationErrorBody {
	t.Helper()
	data, err := os.ReadFile("testdata/drilldown_prs_422/" + name)
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

func decodeValidationErrorBody(t *testing.T, raw []byte) pydanticValidationErrorBody {
	t.Helper()
	var body pydanticValidationErrorBody
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&body); err != nil {
		t.Fatalf("decode response body: %v (body=%s)", err, raw)
	}
	return body
}

// TestGetValidationErrorsMatchPython replays testdata/drilldown_prs_422/
// get_*.json against newDrilldownPRsGetHandler: a malformed date or a
// non-numeric range_days must answer 422 with FastAPI's own default
// RequestValidationError body, byte-for-byte on every field this Go type
// declares (see pydantic_validation_error.go's own package doc comment
// for why that -- not this app's OWN _errors.py handler -- is the actual
// contract this route answers with).
func TestGetValidationErrorsMatchPython(t *testing.T) {
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
			handler := newDrilldownPRsGetHandler(newEmptyRowsDrilldownReader(t))
			req := httptest.NewRequest(http.MethodGet, "/api/v1/drilldown/prs?"+tc.query, nil)
			req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
			rec := httptest.NewRecorder()
			handler(rec, req)

			if rec.Code != http.StatusUnprocessableEntity {
				t.Fatalf("status = %d, want 422, body=%s", rec.Code, rec.Body.String())
			}
			got := decodeValidationErrorBody(t, rec.Body.Bytes())
			want := loadValidationErrorGolden(t, tc.golden)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("response mismatch\n got:  %+v\nwant: %+v", got, want)
			}
		})
	}
}

// TestPostValidationErrorsMatchPython is the POST route's own copy,
// replaying testdata/drilldown_prs_422/post_*.json.
func TestPostValidationErrorsMatchPython(t *testing.T) {
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
			handler := newDrilldownPRsPostHandler(newEmptyRowsDrilldownReader(t))
			req := httptest.NewRequest(http.MethodPost, "/api/v1/drilldown/prs", bytes.NewReader(tc.body))
			req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
			rec := httptest.NewRecorder()
			handler(rec, req)

			if rec.Code != http.StatusUnprocessableEntity {
				t.Fatalf("status = %d, want 422, body=%s", rec.Code, rec.Body.String())
			}
			got := decodeValidationErrorBody(t, rec.Body.Bytes())
			want := loadValidationErrorGolden(t, tc.golden)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("response mismatch\n got:  %+v\nwant: %+v", got, want)
			}
		})
	}
}

// TestNestedMetricFilterFieldsMatchPython pins that a malformed field
// NESTED inside the POST body's "filters" object -- ScopeFilter.level's
// Literal enum, and TimeFilter.range_days's int coercion, sent together
// -- answers 422 with BOTH errors aggregated into one response, in
// MetricFilter's own field order (time before scope), matching a live
// FastAPI capture byte-for-byte (validateMetricFilter,
// pydantic_metric_filter.go).
func TestNestedMetricFilterFieldsMatchPython(t *testing.T) {
	handler := newDrilldownPRsPostHandler(newEmptyRowsDrilldownReader(t))
	body := `{"filters":{"scope":{"level":"not_a_real_level"},"time":{"range_days":"not-a-number"}}}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/drilldown/prs", bytes.NewReader([]byte(body)))
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

// TestMalformedJSONSyntaxMatchesPython pins that genuinely malformed
// JSON syntax answers 422 with jiter's (pydantic-core's Rust JSON
// parser) own message and position, not a generic placeholder --
// classifyJSONSyntaxError (pydantic_json_syntax_error.go) reproduces the
// same grammar CPython's json module documents, with the one departure
// jiter itself makes (a distinct trailing-comma message/position; see
// that file's own doc comment). Python's real answer for this exact body
// ("not json") is {"type": "json_invalid", "loc": ["body", 0], "msg":
// "JSON decode error", "input": {}, "ctx": {"error": "Expecting
// value"}} -- confirmed live via the same FastAPI TestClient technique
// this file's other golden tests use.
func TestMalformedJSONSyntaxMatchesPython(t *testing.T) {
	handler := newDrilldownPRsPostHandler(newEmptyRowsDrilldownReader(t))
	req := httptest.NewRequest(http.MethodPost, "/api/v1/drilldown/prs", bytes.NewReader([]byte(`not json`)))
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
