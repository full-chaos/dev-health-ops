package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/authctx"
)

// loadPeopleDrilldownIssuesValidationErrorGolden decodes a testdata JSON
// file captured from the REAL Python app the same way
// loadPeopleDrilldownPRsValidationErrorGolden does -- see that function's
// own doc comment.
func loadPeopleDrilldownIssuesValidationErrorGolden(t *testing.T, name string) pydanticValidationErrorBody {
	t.Helper()
	data, err := os.ReadFile("testdata/people_drilldown_issues_422/" + name)
	if err != nil {
		t.Fatalf("read golden %s: %v", name, err)
	}
	return decodeValidationErrorBody(t, data)
}

func TestPeopleDrilldownIssuesSwitchFromEnvDefaultsDisabled(t *testing.T) {
	sw := peopleDrilldownIssuesSwitchFromEnv()
	if sw.Enabled(peopleDrilldownIssuesOperation) {
		t.Fatal("expected the people drilldown issues operation disabled with no env var set")
	}
}

func TestPeopleDrilldownIssuesSwitchFromEnvEnablesOperation(t *testing.T) {
	t.Setenv(peopleDrilldownIssuesEnabledEnvVar, "true")
	sw := peopleDrilldownIssuesSwitchFromEnv()
	if !sw.Enabled(peopleDrilldownIssuesOperation) {
		t.Fatal("expected the people drilldown issues operation enabled with GO_API_PEOPLE_DRILLDOWN_ISSUES_ENABLED=true")
	}
}

func TestNewPeopleDrilldownIssuesHandlerRequiresAuthContext(t *testing.T) {
	handler := newPeopleDrilldownIssuesHandler(newPeopleDetailNotFoundReader(t))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/people/abc/drilldown/issues", nil)
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
}

// TestNewPeopleDrilldownIssuesHandlerPersonNotFound pins the 404 branch
// (main.py:1175-1176's ValueError -> HTTPException(404, "Person not found")).
func TestNewPeopleDrilldownIssuesHandlerPersonNotFound(t *testing.T) {
	t.Setenv("IDENTITY_MAPPING_PATH", t.TempDir()+"/missing.yaml")
	handler := newPeopleDrilldownIssuesHandler(newPeopleDetailNotFoundReader(t))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/people/nobody/drilldown/issues", nil)
	req.SetPathValue("person_id", "nobody")
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d, body=%s", rec.Code, http.StatusNotFound, rec.Body.String())
	}
	var decoded map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &decoded); err != nil {
		t.Fatalf("decode body: %v (body=%s)", err, rec.Body.String())
	}
	if decoded["detail"] != "Person not found" {
		t.Fatalf("detail = %q, want %q", decoded["detail"], "Person not found")
	}
}

// TestNewPeopleDrilldownIssuesHandlerDataUnavailable pins the outer 503
// fallback (main.py:1177-1178).
func TestNewPeopleDrilldownIssuesHandlerDataUnavailable(t *testing.T) {
	handler := newPeopleDrilldownIssuesHandler(newPeopleDetailFailingReader(t))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/people/anyone/drilldown/issues", nil)
	req.SetPathValue("person_id", "anyone")
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
}

// TestNewPeopleDrilldownIssuesHandlerRejectsComparativeParams pins
// _reject_comparative_params for every key in _FORBIDDEN_QUERY_PARAMS.
func TestNewPeopleDrilldownIssuesHandlerRejectsComparativeParams(t *testing.T) {
	for _, key := range []string{"compare_to", "rank", "percentile", "score", "leaderboard", "top", "bottom"} {
		t.Run(key, func(t *testing.T) {
			handler := newPeopleDrilldownIssuesHandler(newPeopleDetailFailingReader(t))
			req := httptest.NewRequest(http.MethodGet, "/api/v1/people/anyone/drilldown/issues?"+key+"=x", nil)
			req.SetPathValue("person_id", "anyone")
			req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
			rec := httptest.NewRecorder()
			handler(rec, req)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d, body=%s", rec.Code, http.StatusBadRequest, rec.Body.String())
			}
		})
	}
}

// TestNewPeopleDrilldownIssuesHandlerValidationErrorsMatchPython replays
// testdata/people_drilldown_issues_422/ against
// newPeopleDrilldownIssuesHandler: a malformed range_days/cursor must
// answer 422 with FastAPI's own default RequestValidationError body,
// byte-for-byte.
func TestNewPeopleDrilldownIssuesHandlerValidationErrorsMatchPython(t *testing.T) {
	cases := []struct {
		name   string
		query  string
		golden string
	}{
		{"malformed cursor", "cursor=not-a-date", "get_malformed_cursor.json"},
		{"malformed range_days", "range_days=not-a-number", "get_malformed_range_days.json"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			handler := newPeopleDrilldownIssuesHandler(newPeopleDetailFailingReader(t))
			req := httptest.NewRequest(http.MethodGet, "/api/v1/people/anyone/drilldown/issues?"+tc.query, nil)
			req.SetPathValue("person_id", "anyone")
			req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
			rec := httptest.NewRecorder()
			handler(rec, req)

			if rec.Code != http.StatusUnprocessableEntity {
				t.Fatalf("status = %d, want 422, body=%s", rec.Code, rec.Body.String())
			}
			got := decodeValidationErrorBody(t, rec.Body.Bytes())
			want := loadPeopleDrilldownIssuesValidationErrorGolden(t, tc.golden)
			gotJSON, _ := json.Marshal(got)
			wantJSON, _ := json.Marshal(want)
			if string(gotJSON) != string(wantJSON) {
				t.Fatalf("response mismatch\n got:  %s\nwant: %s", gotJSON, wantJSON)
			}
		})
	}
}

// TestBuildPeopleDrilldownIssuesRouteEntryHandlerRejectsNonGET pins the
// entryHandler's own method guard.
func TestBuildPeopleDrilldownIssuesRouteEntryHandlerRejectsNonGET(t *testing.T) {
	t.Setenv("CLICKHOUSE_URI", "clickhouse://localhost:8123/default")
	t.Setenv("GO_API_ENVELOPE_JWKS_PATH", filepath.Join(t.TempDir(), "missing-jwks.json"))
	t.Setenv("GO_API_ENVELOPE_ISSUER", "test-issuer")
	t.Setenv("GO_API_ENVELOPE_AUDIENCE", "test-audience")

	handler, cleanup, ok, err := buildPeopleDrilldownIssuesRoute()
	if err != nil {
		t.Fatalf("buildPeopleDrilldownIssuesRoute: %v", err)
	}
	if !ok {
		t.Fatal("buildPeopleDrilldownIssuesRoute: ok = false, want true with every dependency env var set")
	}
	defer cleanup()

	req := httptest.NewRequest(http.MethodPost, "/api/v1/people/anyone/drilldown/issues", nil)
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusMethodNotAllowed)
	}
}
