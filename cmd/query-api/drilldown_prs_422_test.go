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

// declaredDivergence documents one accepted, ticketed gap between this
// route's Go validation and Python's Pydantic-driven one -- the same
// shape (a Ticket field, separate from prose) internal/goapiproof's own
// BaselineDefect declarations use. Never referenced by production code:
// its only job is to give each declared-divergence test below one place
// that carries the ticket id, so a source-diff grep for a bare ticket
// number in a comment (this repo's own hygiene check on every lane's
// diff) has exactly one accepted hit per declaration, not a citation
// scattered across prose.
type declaredDivergence struct {
	Ticket string
	Reason string
}

// TestDeclaredDivergenceNestedFilterFieldsNotValidated pins TODAY's Go
// behavior for the first of two gaps pydantic_validation_error.go's own
// package doc comment declares: a malformed field NESTED inside the POST
// body's "filters" object (its Literal-typed scope.level, or a
// wrong-typed time.range_days) is silently accepted, not rejected with
// 422 -- filters stays an untyped map in this route (no Go MetricFilter
// type exists in this binary), the same boundary
// investment_explain_route.go already established elsewhere. Python
// answers 422 for the SAME body (api/models/filters.py's ScopeFilter.level:
// Literal[...]; confirmed live via the same FastAPI TestClient technique
// this file's other golden tests use). This is a real, accepted
// divergence, not an oversight -- this test exists so a future change
// that starts silently MIScomputing on these inputs (instead of
// continuing to pass them through unvalidated, today's documented
// behavior) is caught.
func TestDeclaredDivergenceNestedFilterFieldsNotValidated(t *testing.T) {
	_ = declaredDivergence{
		Ticket: "CHAOS-5797",
		Reason: "nested MetricFilter fields (filters.scope.level's Literal enum, filters.time.range_days's type, and so on) are not validated by this Go port; a malformed value there is silently accepted rather than answering 422. Tracked for a shared validator alongside the malformed-JSON-message gap.",
	}

	handler := newDrilldownPRsPostHandler(newEmptyRowsDrilldownReader(t))
	body := `{"filters":{"scope":{"level":"not_a_real_level"},"time":{"range_days":"not-a-number"}}}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/drilldown/prs", bytes.NewReader([]byte(body)))
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
	rec := httptest.NewRecorder()
	handler(rec, req)

	if rec.Code == http.StatusUnprocessableEntity {
		t.Fatalf("got 422 for a malformed nested filters field -- this route now DOES validate nested filters; update the declared divergence this test pins (and pydantic_validation_error.go's own doc comment) to match, they no longer describe reality")
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (today's declared behavior: the malformed nested field is silently accepted, not rejected); body=%s", rec.Code, rec.Body.String())
	}
}

// TestDeclaredDivergenceMalformedJSONGenericMessage pins TODAY's Go
// behavior for the second of two gaps pydantic_validation_error.go's own
// package doc comment declares: genuinely malformed JSON syntax answers
// 422 with the correct envelope (type "json_invalid", loc ["body", 0])
// but a GENERIC message/ctx.error, not jiter's (pydantic-core's Rust
// JSON parser) exact per-error-class text and byte offset. Python's real
// answer for this exact body ("not json") is {"type": "json_invalid",
// "loc": ["body", 0], "msg": "JSON decode error", "input": {}, "ctx":
// {"error": "Expecting value"}} -- confirmed live via the same FastAPI
// TestClient technique this file's other golden tests use. ctx.error
// genuinely varies by WHERE and HOW the JSON is malformed (a missing
// closing brace produces "Expecting property name enclosed in double
// quotes", a trailing comma "Illegal trailing comma before end of
// object", and so on -- also confirmed live), so replicating jiter's
// exact taxonomy would mean reimplementing it; this route declares the
// gap instead of guessing at a message that would only be right for one
// specific kind of malformed input.
func TestDeclaredDivergenceMalformedJSONGenericMessage(t *testing.T) {
	_ = declaredDivergence{
		Ticket: "CHAOS-5797",
		Reason: "genuinely malformed JSON syntax (not just a wrong-shaped-but-valid body) answers 422 with the correct envelope/type but a generic ctx.error/msg, not jiter's exact per-error-class text and byte offset. Tracked for a shared validator alongside the nested-filters gap.",
	}

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
		Ctx:   map[string]string{"error": "Invalid JSON"},
	}}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("response mismatch (this pins the DECLARED, generic-message divergence -- it is not meant to equal Python's own \"Expecting value\" text)\n got:  %+v\nwant: %+v", got, want)
	}
}
