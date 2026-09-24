package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
)

func TestPeopleMetricSwitchFromEnvDefaultsDisabled(t *testing.T) {
	sw := peopleMetricSwitchFromEnv()
	if sw.Enabled(peopleMetricOperation) {
		t.Fatal("expected the people metric operation disabled with no env var set")
	}
}

func TestPeopleMetricSwitchFromEnvEnablesOperation(t *testing.T) {
	t.Setenv(peopleMetricEnabledEnvVar, "true")
	sw := peopleMetricSwitchFromEnv()
	if !sw.Enabled(peopleMetricOperation) {
		t.Fatal("expected the people metric operation enabled with GO_API_PEOPLE_METRIC_ENABLED=true")
	}
}

func TestNewPeopleMetricHandlerRequiresAuthContext(t *testing.T) {
	handler := newPeopleMetricHandler(newPeopleDetailNotFoundReader(t))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/people/abc/metric?metric=churn", nil)
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
}

// TestNewPeopleMetricHandlerMissingMetricValidationError pins the
// "missing"-type 422 detail for the REQUIRED `metric` query param
// (main.py:1093's `metric: str`, no default).
func TestNewPeopleMetricHandlerMissingMetricValidationError(t *testing.T) {
	handler := newPeopleMetricHandler(newPeopleDetailFailingReader(t))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/people/anyone/metric", nil)
	req.SetPathValue("person_id", "anyone")
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want %d, body=%s", rec.Code, http.StatusUnprocessableEntity, rec.Body.String())
	}
	var decoded pydanticValidationErrorBody
	if err := json.Unmarshal(rec.Body.Bytes(), &decoded); err != nil {
		t.Fatalf("decode body: %v (body=%s)", err, rec.Body.String())
	}
	if len(decoded.Detail) != 1 || decoded.Detail[0].Type != "missing" {
		t.Fatalf("detail = %+v, want one missing entry", decoded.Detail)
	}
	if len(decoded.Detail[0].Loc) != 2 || decoded.Detail[0].Loc[1] != "metric" {
		t.Fatalf("loc = %v, want [query metric]", decoded.Detail[0].Loc)
	}
}

// TestNewPeopleMetricHandlerHappyPath pins a 200 for a found person and
// a supported metric, with the underlying ClickHouse reads answering
// zero rows beyond the identity lookup.
func TestNewPeopleMetricHandlerHappyPath(t *testing.T) {
	t.Setenv("IDENTITY_MAPPING_PATH", t.TempDir()+"/missing.yaml")
	handler := newPeopleMetricHandler(newPeopleDetailFoundReader(t, "alice@example.com"))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/people/anything/metric?metric=churn", nil)
	req.SetPathValue("person_id", "anything")
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	var decoded map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &decoded); err != nil {
		t.Fatalf("decode body: %v (body=%s)", err, rec.Body.String())
	}
	if decoded["metric"] != "churn" || decoded["label"] != "Code Churn" {
		t.Fatalf("body = %v, want metric=churn label=Code Churn", decoded)
	}
}

// TestNewPeopleMetricHandlerUnsupportedMetric pins the 400 branch --
// build_person_metric_response's `metric not supported` ValueError ->
// main.py's own {"detail": "Metric not supported"} (main.py:1112-1118).
func TestNewPeopleMetricHandlerUnsupportedMetric(t *testing.T) {
	handler := newPeopleMetricHandler(newPeopleDetailFailingReader(t))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/people/anyone/metric?metric=not-a-real-metric", nil)
	req.SetPathValue("person_id", "anyone")
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d, body=%s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
	var decoded map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &decoded); err != nil {
		t.Fatalf("decode body: %v (body=%s)", err, rec.Body.String())
	}
	if decoded["detail"] != "Metric not supported" {
		t.Fatalf("detail = %q, want %q", decoded["detail"], "Metric not supported")
	}
}

// TestNewPeopleMetricHandlerPersonNotFound pins the 404 branch, reached
// only after the metric itself validates as supported.
func TestNewPeopleMetricHandlerPersonNotFound(t *testing.T) {
	t.Setenv("IDENTITY_MAPPING_PATH", t.TempDir()+"/missing.yaml")
	handler := newPeopleMetricHandler(newPeopleDetailNotFoundReader(t))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/people/nobody/metric?metric=churn", nil)
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

// TestNewPeopleMetricHandlerDataUnavailable pins the outer 503 fallback
// (main.py:1119-1120).
func TestNewPeopleMetricHandlerDataUnavailable(t *testing.T) {
	handler := newPeopleMetricHandler(newPeopleDetailFailingReader(t))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/people/anyone/metric?metric=churn", nil)
	req.SetPathValue("person_id", "anyone")
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
}

// TestNewPeopleMetricHandlerRejectsComparativeParams pins
// _reject_comparative_params for every key in _FORBIDDEN_QUERY_PARAMS.
func TestNewPeopleMetricHandlerRejectsComparativeParams(t *testing.T) {
	for _, key := range []string{"compare_to", "rank", "percentile", "score", "leaderboard", "top", "bottom"} {
		t.Run(key, func(t *testing.T) {
			handler := newPeopleMetricHandler(newPeopleDetailFailingReader(t))
			req := httptest.NewRequest(http.MethodGet, "/api/v1/people/anyone/metric?metric=churn&"+key+"=x", nil)
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

// TestNewPeopleMetricHandlerMissingMetricBeatsComparativeParamRejection
// pins the SAME precedence peopleSearchHandler's own doc comment
// establishes: the missing-field 422 for `metric` wins over
// _reject_comparative_params' 400, because Pydantic parameter validation
// happens at the framework level ahead of the route function body.
func TestNewPeopleMetricHandlerMissingMetricBeatsComparativeParamRejection(t *testing.T) {
	handler := newPeopleMetricHandler(newPeopleDetailFailingReader(t))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/people/anyone/metric?compare_to=x", nil)
	req.SetPathValue("person_id", "anyone")
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want %d, body=%s", rec.Code, http.StatusUnprocessableEntity, rec.Body.String())
	}
}

// TestBuildPeopleMetricRouteEntryHandlerRejectsNonGET pins the
// entryHandler's own method guard.
func TestBuildPeopleMetricRouteEntryHandlerRejectsNonGET(t *testing.T) {
	t.Setenv("CLICKHOUSE_URI", "clickhouse://localhost:8123/default")
	t.Setenv("GO_API_ENVELOPE_JWKS_PATH", filepath.Join(t.TempDir(), "missing-jwks.json"))
	t.Setenv("GO_API_ENVELOPE_ISSUER", "test-issuer")
	t.Setenv("GO_API_ENVELOPE_AUDIENCE", "test-audience")

	handler, cleanup, ok, err := buildPeopleMetricRoute()
	if err != nil {
		t.Fatalf("buildPeopleMetricRoute: %v", err)
	}
	if !ok {
		t.Fatal("buildPeopleMetricRoute: ok = false, want true with every dependency env var set")
	}
	defer cleanup()

	req := httptest.NewRequest(http.MethodPost, "/api/v1/people/anyone/metric", nil)
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusMethodNotAllowed)
	}
}
