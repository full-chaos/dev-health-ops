package server

import (
	"encoding/json"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/people"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
)

// loadPeopleDrilldownPRsValidationErrorGolden decodes a testdata JSON file
// captured from the REAL Python app (FastAPI TestClient, the SAME request
// this test issues against the Go handler) via a one-off, uncommitted
// invocation -- see this file's own TEST-EVIDENCE citation in the PR body
// for the exact capture script. DisallowUnknownFields makes a field this
// Go type does not declare a hard test failure, not a silent drop.
func loadPeopleDrilldownPRsValidationErrorGolden(t *testing.T, name string) pydanticValidationErrorBody {
	t.Helper()
	data, err := os.ReadFile("testdata/people_drilldown_prs_422/" + name)
	if err != nil {
		t.Fatalf("read golden %s: %v", name, err)
	}
	return decodeValidationErrorBody(t, data)
}

func TestPeopleDrilldownPRsSwitchFromEnvDefaultsDisabled(t *testing.T) {
	sw := peopleDrilldownPRsSwitchFromEnv(os.Getenv)
	if sw.Enabled(peopleDrilldownPRsOperation) {
		t.Fatal("expected the people drilldown prs operation disabled with no env var set")
	}
}

func TestPeopleDrilldownPRsSwitchFromEnvEnablesOperation(t *testing.T) {
	t.Setenv(peopleDrilldownPRsEnabledEnvVar, "true")
	sw := peopleDrilldownPRsSwitchFromEnv(os.Getenv)
	if !sw.Enabled(peopleDrilldownPRsOperation) {
		t.Fatal("expected the people drilldown prs operation enabled with GO_API_PEOPLE_DRILLDOWN_PRS_ENABLED=true")
	}
}

func TestNewPeopleDrilldownPRsHandlerRequiresAuthContext(t *testing.T) {
	handler := newPeopleDrilldownPRsHandler(newPeopleDetailNotFoundReader(t))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/people/abc/drilldown/prs", nil)
	rec := httptest.NewRecorder()
	serveRoute(t, handler, rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
}

// TestNewPeopleDrilldownPRsHandlerPersonNotFound pins the 404 branch
// (main.py:1146-1147's ValueError -> HTTPException(404, "Person not found")).
func TestNewPeopleDrilldownPRsHandlerPersonNotFound(t *testing.T) {
	t.Setenv("IDENTITY_MAPPING_PATH", t.TempDir()+"/missing.yaml")
	handler := newPeopleDrilldownPRsHandler(newPeopleDetailNotFoundReader(t))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/people/nobody/drilldown/prs", nil)
	req.SetPathValue("person_id", "nobody")
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
	rec := httptest.NewRecorder()
	serveRoute(t, handler, rec, req)
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

// TestNewPeopleDrilldownPRsHandlerDataUnavailable pins the outer 503
// fallback (main.py:1148-1149).
func TestNewPeopleDrilldownPRsHandlerDataUnavailable(t *testing.T) {
	handler := newPeopleDrilldownPRsHandler(newPeopleDetailFailingReader(t))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/people/anyone/drilldown/prs", nil)
	req.SetPathValue("person_id", "anyone")
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
	rec := httptest.NewRecorder()
	serveRoute(t, handler, rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
}

// TestNewPeopleDrilldownPRsHandlerRejectsComparativeParams pins
// _reject_comparative_params for every key in _FORBIDDEN_QUERY_PARAMS.
func TestNewPeopleDrilldownPRsHandlerRejectsComparativeParams(t *testing.T) {
	for _, key := range []string{"compare_to", "rank", "percentile", "score", "leaderboard", "top", "bottom"} {
		t.Run(key, func(t *testing.T) {
			handler := newPeopleDrilldownPRsHandler(newPeopleDetailFailingReader(t))
			req := httptest.NewRequest(http.MethodGet, "/api/v1/people/anyone/drilldown/prs?"+key+"=x", nil)
			req.SetPathValue("person_id", "anyone")
			req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
			rec := httptest.NewRecorder()
			serveRoute(t, handler, rec, req)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d, body=%s", rec.Code, http.StatusBadRequest, rec.Body.String())
			}
		})
	}
}

// TestNewPeopleDrilldownPRsHandlerValidationErrorsMatchPython replays
// testdata/people_drilldown_prs_422/ against newPeopleDrilldownPRsHandler:
// a malformed range_days/limit/cursor must answer 422 with FastAPI's own
// default RequestValidationError body, byte-for-byte.
func TestNewPeopleDrilldownPRsHandlerValidationErrorsMatchPython(t *testing.T) {
	cases := []struct {
		name   string
		query  string
		golden string
	}{
		{"malformed cursor", "cursor=not-a-date", "get_malformed_cursor.json"},
		{"malformed range_days", "range_days=not-a-number", "get_malformed_range_days.json"},
		{"malformed limit", "limit=not-a-number", "get_malformed_limit.json"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			handler := newPeopleDrilldownPRsHandler(newPeopleDetailFailingReader(t))
			req := httptest.NewRequest(http.MethodGet, "/api/v1/people/anyone/drilldown/prs?"+tc.query, nil)
			req.SetPathValue("person_id", "anyone")
			req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
			rec := httptest.NewRecorder()
			serveRoute(t, handler, rec, req)

			if rec.Code != http.StatusUnprocessableEntity {
				t.Fatalf("status = %d, want 422, body=%s", rec.Code, rec.Body.String())
			}
			got := decodeValidationErrorBody(t, rec.Body.Bytes())
			want := loadPeopleDrilldownPRsValidationErrorGolden(t, tc.golden)
			gotJSON, _ := json.Marshal(got)
			wantJSON, _ := json.Marshal(want)
			if string(gotJSON) != string(wantJSON) {
				t.Fatalf("response mismatch\n got:  %s\nwant: %s", gotJSON, wantJSON)
			}
		})
	}
}

// TestBuildPeopleDrilldownPRsRouteEntryHandlerRejectsNonGET pins the
// entryHandler's own method guard.
func TestBuildPeopleDrilldownPRsRouteEntryHandlerRejectsNonGET(t *testing.T) {
	t.Setenv("CLICKHOUSE_URI", "clickhouse://localhost:8123/default")
	t.Setenv("GO_API_ENVELOPE_JWKS_PATH", filepath.Join(t.TempDir(), "missing-jwks.json"))
	t.Setenv("GO_API_ENVELOPE_ISSUER", "test-issuer")
	t.Setenv("GO_API_ENVELOPE_AUDIENCE", "test-audience")

	handler, cleanup, ok, err := buildPeopleDrilldownPRsRoute(os.Getenv)
	if err != nil {
		t.Fatalf("buildPeopleDrilldownPRsRoute: %v", err)
	}
	if !ok {
		t.Fatal("buildPeopleDrilldownPRsRoute: ok = false, want true with every dependency env var set")
	}
	defer cleanup()

	req := httptest.NewRequest(http.MethodPost, "/api/v1/people/anyone/drilldown/prs", nil)
	rec := httptest.NewRecorder()
	serveRoute(t, handler, rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusMethodNotAllowed)
	}
}

// TestNewPeopleDrilldownPRsHandlerSuccessIsAResponseModelBody drives the
// route to a 200 through serveRoute, so a success body left off
// writeModelResponse fails here.
func TestNewPeopleDrilldownPRsHandlerSuccessIsAResponseModelBody(t *testing.T) {
	client := cursorRoundTripClient{identity: "alice@example.com", capture: &cursorRoundTripCapture{}}
	reader, err := people.NewReader(client)
	if err != nil {
		t.Fatalf("people.NewReader: %v", err)
	}
	handler := newPeopleDrilldownPRsHandler(reader)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/people/anyone/drilldown/prs", nil)
	req.SetPathValue("person_id", "anyone")
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
	rec := httptest.NewRecorder()
	serveRoute(t, handler, rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200, body=%s", rec.Code, rec.Body.String())
	}
}
