package server

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

// loadExplainValidationErrorGolden decodes a testdata JSON file captured
// from the REAL Python app (FastAPI TestClient, auth dependency
// overridden, the SAME request this test issues against the Go handler)
// via a one-off, uncommitted invocation -- see this PR's own
// TEST-EVIDENCE for the exact capture script. DisallowUnknownFields makes
// a field this Go type does not declare a hard test failure, not a
// silent drop -- same convention as drilldown_prs_422_test.go's own
// loadValidationErrorGolden.
func loadExplainValidationErrorGolden(t *testing.T, name string) pydanticValidationErrorBody {
	t.Helper()
	data, err := os.ReadFile("testdata/explain_422/" + name)
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

func decodeExplainValidationErrorBody(t *testing.T, raw []byte) pydanticValidationErrorBody {
	t.Helper()
	var body pydanticValidationErrorBody
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&body); err != nil {
		t.Fatalf("decode response body: %v (body=%s)", err, raw)
	}
	return body
}

// TestExplainGetValidationErrorsMatchPython replays
// testdata/explain_422/get_*.json against newExplainGetHandler.
func TestExplainGetValidationErrorsMatchPython(t *testing.T) {
	cases := []struct {
		name   string
		query  string
		golden string
	}{
		{"missing metric", "", "get_missing_metric.json"},
		{"malformed range_days", "metric=cycle_time&range_days=abc", "get_malformed_range_days.json"},
		{"malformed compare_days", "metric=cycle_time&compare_days=abc", "get_malformed_compare_days.json"},
		{"malformed start_date", "metric=cycle_time&start_date=not-a-date", "get_malformed_start_date.json"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			handler := newExplainGetHandler(newEmptyRowsExplainReader(t))
			req := httptest.NewRequest(http.MethodGet, "/api/v1/explain?"+tc.query, nil)
			req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
			rec := httptest.NewRecorder()
			handler(rec, req)

			if rec.Code != http.StatusUnprocessableEntity {
				t.Fatalf("status = %d, want 422, body=%s", rec.Code, rec.Body.String())
			}
			got := decodeExplainValidationErrorBody(t, rec.Body.Bytes())
			want := loadExplainValidationErrorGolden(t, tc.golden)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("response mismatch\n got:  %+v\nwant: %+v", got, want)
			}
		})
	}
}

// TestExplainPostValidationErrorsMatchPython replays
// testdata/explain_422/post_*.json against newExplainPostHandler.
func TestExplainPostValidationErrorsMatchPython(t *testing.T) {
	cases := []struct {
		name   string
		body   []byte
		golden string
	}{
		{"missing body", nil, "post_missing_body.json"},
		{"missing metric", []byte(`{"filters":{}}`), "post_missing_metric.json"},
		{"non-string metric", []byte(`{"metric":5,"filters":{}}`), "post_non_string_metric.json"},
		{"missing filters", []byte(`{"metric":"cycle_time"}`), "post_missing_filters.json"},
		{"invalid scope level", []byte(`{"metric":"cycle_time","filters":{"scope":{"level":"bogus"}}}`), "post_invalid_scope_level.json"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			handler := newExplainPostHandler(newEmptyRowsExplainReader(t))
			req := httptest.NewRequest(http.MethodPost, "/api/v1/explain", bytes.NewReader(tc.body))
			req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
			rec := httptest.NewRecorder()
			handler(rec, req)

			if rec.Code != http.StatusUnprocessableEntity {
				t.Fatalf("status = %d, want 422, body=%s", rec.Code, rec.Body.String())
			}
			got := decodeExplainValidationErrorBody(t, rec.Body.Bytes())
			want := loadExplainValidationErrorGolden(t, tc.golden)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("response mismatch\n got:  %+v\nwant: %+v", got, want)
			}
		})
	}
}
