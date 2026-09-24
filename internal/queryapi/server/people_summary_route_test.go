package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/people"
)

// emptyScanner answers Next() false unconditionally -- safe as a stand-in
// for ANY query's row shape, since Scan is only ever called after a true
// Next().
type emptyScanner struct{}

func (emptyScanner) Next() bool        { return false }
func (emptyScanner) Scan(...any) error { return nil }
func (emptyScanner) Err() error        { return nil }
func (emptyScanner) Close() error      { return nil }

// personFoundScanner replays a single string column once -- the shape
// resolvePersonIdentity's md5-lookup query scans into.
type personFoundScanner struct {
	identity string
	done     bool
}

func (s *personFoundScanner) Next() bool {
	if s.done {
		return false
	}
	s.done = true
	return true
}
func (s *personFoundScanner) Scan(dest ...any) error {
	*dest[0].(*string) = s.identity
	return nil
}
func (s *personFoundScanner) Err() error   { return nil }
func (s *personFoundScanner) Close() error { return nil }

// personLookupClient answers resolvePersonIdentity's own md5-lookup query
// (detected by its distinctive SQL fragment, same convention
// internal/queryapi/people's own golden tests use) with a fixed
// identity, and every other query with zero rows -- enough for
// BuildSummaryResponse/BuildMetricResponse to reach a 200 with empty-but-
// present sections, without this test package needing to replay every
// individual metric/coverage/breakdown query.
type personLookupClient struct {
	identity string
}

func (c personLookupClient) Query(_ context.Context, query string, _ []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	if strings.Contains(query, "lower(hex(MD5(identity)))") {
		return &personFoundScanner{identity: c.identity}, nil
	}
	return emptyScanner{}, nil
}

func newPeopleDetailFoundReader(t *testing.T, identity string) *people.Reader {
	t.Helper()
	reader, err := people.NewReader(personLookupClient{identity: identity})
	if err != nil {
		t.Fatalf("people.NewReader: %v", err)
	}
	return reader
}

func newPeopleDetailNotFoundReader(t *testing.T) *people.Reader {
	t.Helper()
	reader, err := people.NewReader(personLookupClient{identity: ""})
	if err != nil {
		t.Fatalf("people.NewReader: %v", err)
	}
	return reader
}

func newPeopleDetailFailingReader(t *testing.T) *people.Reader {
	t.Helper()
	reader, err := people.NewReader(failingPeopleClient{})
	if err != nil {
		t.Fatalf("people.NewReader: %v", err)
	}
	return reader
}

func TestPeopleSummarySwitchFromEnvDefaultsDisabled(t *testing.T) {
	sw := peopleSummarySwitchFromEnv(os.Getenv)
	if sw.Enabled(peopleSummaryOperation) {
		t.Fatal("expected the people summary operation disabled with no env var set")
	}
}

func TestPeopleSummarySwitchFromEnvEnablesOperation(t *testing.T) {
	t.Setenv(peopleSummaryEnabledEnvVar, "true")
	sw := peopleSummarySwitchFromEnv(os.Getenv)
	if !sw.Enabled(peopleSummaryOperation) {
		t.Fatal("expected the people summary operation enabled with GO_API_PEOPLE_SUMMARY_ENABLED=true")
	}
}

func TestNewPeopleSummaryHandlerRequiresAuthContext(t *testing.T) {
	handler := newPeopleSummaryHandler(newPeopleDetailNotFoundReader(t))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/people/abc/summary", nil)
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
	if got, want := rec.Body.String(), `{"detail":{"message":"Not authenticated"}}`+"\n"; got != want {
		t.Fatalf("body = %q, want %q", got, want)
	}
}

// TestNewPeopleSummaryHandlerHappyPath pins Content-Type and a 200 for a
// found person, with every ClickHouse read beyond the identity lookup
// answering zero rows -- deltas/work_mix/flow_breakdown/collaboration all
// present but empty/zero-valued.
func TestNewPeopleSummaryHandlerHappyPath(t *testing.T) {
	t.Setenv("IDENTITY_MAPPING_PATH", t.TempDir()+"/missing.yaml")
	handler := newPeopleSummaryHandler(newPeopleDetailFoundReader(t, "alice@example.com"))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/people/anything/summary", nil)
	req.SetPathValue("person_id", "anything")
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", ct)
	}
	var decoded map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &decoded); err != nil {
		t.Fatalf("decode body: %v (body=%s)", err, rec.Body.String())
	}
	person, ok := decoded["person"].(map[string]any)
	if !ok || person["display_name"] != "Alice" {
		t.Fatalf("person = %v, want display_name Alice", decoded["person"])
	}
}

// TestNewPeopleSummaryHandlerPersonNotFound pins the 404 branch --
// build_person_summary_response's ValueError -> main.py's own
// {"detail": "Person not found"} (main.py:1086-1087).
func TestNewPeopleSummaryHandlerPersonNotFound(t *testing.T) {
	t.Setenv("IDENTITY_MAPPING_PATH", t.TempDir()+"/missing.yaml")
	handler := newPeopleSummaryHandler(newPeopleDetailNotFoundReader(t))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/people/nobody/summary", nil)
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

// TestNewPeopleSummaryHandlerDataUnavailable pins the outer `except
// Exception: raise HTTPException(503, "Data unavailable")` fallback
// (main.py:1088-1089).
func TestNewPeopleSummaryHandlerDataUnavailable(t *testing.T) {
	handler := newPeopleSummaryHandler(newPeopleDetailFailingReader(t))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/people/anyone/summary", nil)
	req.SetPathValue("person_id", "anyone")
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
	var decoded map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &decoded); err != nil {
		t.Fatalf("decode body: %v (body=%s)", err, rec.Body.String())
	}
	if decoded["detail"] != "Data unavailable" {
		t.Fatalf("detail = %q, want %q", decoded["detail"], "Data unavailable")
	}
}

// TestNewPeopleSummaryHandlerRejectsComparativeParams pins
// _reject_comparative_params for every key in _FORBIDDEN_QUERY_PARAMS.
func TestNewPeopleSummaryHandlerRejectsComparativeParams(t *testing.T) {
	for _, key := range []string{"compare_to", "rank", "percentile", "score", "leaderboard", "top", "bottom"} {
		t.Run(key, func(t *testing.T) {
			handler := newPeopleSummaryHandler(newPeopleDetailFailingReader(t))
			req := httptest.NewRequest(http.MethodGet, "/api/v1/people/anyone/summary?"+key+"=x", nil)
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
			if decoded["detail"] != "Comparative parameters are not supported." {
				t.Fatalf("detail = %q, want %q", decoded["detail"], "Comparative parameters are not supported.")
			}
		})
	}
}

// TestNewPeopleSummaryHandlerNonNumericRangeDaysValidationError pins the
// int_parsing 422 detail for a malformed range_days, and that it is
// checked BEFORE _reject_comparative_params (same precedence
// peopleSearchHandler's own doc comment establishes for `limit`).
func TestNewPeopleSummaryHandlerNonNumericRangeDaysValidationError(t *testing.T) {
	handler := newPeopleSummaryHandler(newPeopleDetailFailingReader(t))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/people/anyone/summary?range_days=not-a-number&compare_to=x", nil)
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
	if len(decoded.Detail) != 1 || decoded.Detail[0].Type != "int_parsing" {
		t.Fatalf("detail = %+v, want one int_parsing entry", decoded.Detail)
	}
}

// TestBuildPeopleSummaryRouteEntryHandlerRejectsNonGET pins the
// entryHandler's own method guard, same shared-helper contract every
// sibling REST route's entryHandler uses.
func TestBuildPeopleSummaryRouteEntryHandlerRejectsNonGET(t *testing.T) {
	t.Setenv("CLICKHOUSE_URI", "clickhouse://localhost:8123/default")
	t.Setenv("GO_API_ENVELOPE_JWKS_PATH", filepath.Join(t.TempDir(), "missing-jwks.json"))
	t.Setenv("GO_API_ENVELOPE_ISSUER", "test-issuer")
	t.Setenv("GO_API_ENVELOPE_AUDIENCE", "test-audience")

	handler, cleanup, ok, err := buildPeopleSummaryRoute(os.Getenv)
	if err != nil {
		t.Fatalf("buildPeopleSummaryRoute: %v", err)
	}
	if !ok {
		t.Fatal("buildPeopleSummaryRoute: ok = false, want true with every dependency env var set")
	}
	defer cleanup()

	req := httptest.NewRequest(http.MethodPost, "/api/v1/people/anyone/summary", nil)
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusMethodNotAllowed)
	}
	if got, want := rec.Body.String(), `{"detail":"Method Not Allowed"}`+"\n"; got != want {
		t.Fatalf("body = %q, want %q", got, want)
	}
}
