package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/people"
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
	sw := peopleDrilldownIssuesSwitchFromEnv(os.Getenv)
	if sw.Enabled(peopleDrilldownIssuesOperation) {
		t.Fatal("expected the people drilldown issues operation disabled with no env var set")
	}
}

func TestPeopleDrilldownIssuesSwitchFromEnvEnablesOperation(t *testing.T) {
	t.Setenv(peopleDrilldownIssuesEnabledEnvVar, "true")
	sw := peopleDrilldownIssuesSwitchFromEnv(os.Getenv)
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

	handler, cleanup, ok, err := buildPeopleDrilldownIssuesRoute(os.Getenv)
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

// cursorRoundTripCapture holds the ONE query/bindings pair
// cursorRoundTripClient sees that is not the identity-lookup query --
// fetchPersonIssuesQuery, the cursor-bound one this test cares about.
type cursorRoundTripCapture struct {
	query    string
	bindings []dhclickhouse.Binding
}

// cursorRoundTripClient answers resolvePersonIdentity's own md5-lookup
// query the same way personLookupClient does (people_summary_route_test.go),
// and captures every OTHER query's statement/bindings instead of just
// discarding them -- this test needs to inspect exactly what
// fetchPersonIssuesQuery bound for its own cursor_filter branch.
type cursorRoundTripClient struct {
	identity string
	capture  *cursorRoundTripCapture
}

func (c cursorRoundTripClient) Query(_ context.Context, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	if strings.Contains(query, "lower(hex(MD5(identity)))") {
		return &personFoundScanner{identity: c.identity}, nil
	}
	c.capture.query = query
	c.capture.bindings = bindings
	return emptyScanner{}, nil
}

// TestPeopleDrilldownIssuesCursorRoundTripsThroughResponseMarshalling closes
// the gap the corpus's own valid_cursor entry (restcorpus.go) cannot close
// by itself: that entry binds a live `cursor` from a PRIOR response's own
// FIRST item's completed_at, but a status-only entry never decodes its own
// body, so nothing there proves the wire form this route's response
// actually emits for completed_at is a form its OWN cursor query param
// parser (parseISODateTimeQueryParam, pydantic_validation_error.go) accepts
// -- a mismatch there would make every real caller's own next_cursor
// round-trip fail live, undetected by a corpus that never round-trips a
// body through itself. This test does, in three steps, none of them a
// hand-typed literal:
//  1. Builds a real people.DrilldownIssuesResponse and lets its own
//     encoding/json marshalling produce the completed_at string exactly as
//     the route emits it on the wire.
//  2. Feeds that exact string to parseISODateTimeQueryParam, the same
//     function newPeopleDrilldownIssuesHandler's own `cursor` query param
//     goes through, and requires it to parse to the SAME instant.
//  3. Sends that exact string back as this route's own `?cursor=` query
//     param through the real HTTP handler, and requires
//     fetchPersonIssuesQuery's own cursor_filter branch (drilldownissues.go)
//     to fire and bind that SAME instant -- not merely that some cursor
//     filter appears, but that the round trip lost nothing.
func TestPeopleDrilldownIssuesCursorRoundTripsThroughResponseMarshalling(t *testing.T) {
	t.Setenv("IDENTITY_MAPPING_PATH", t.TempDir()+"/missing.yaml")

	completedAt := time.Date(2024, 6, 3, 10, 0, 0, 0, time.UTC)
	resp := people.DrilldownIssuesResponse{Items: []people.IssueRow{{CompletedAt: &completedAt}}}
	encoded, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("marshal response: %v", err)
	}
	var decoded struct {
		Items []struct {
			CompletedAt string `json:"completed_at"`
		} `json:"items"`
	}
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(decoded.Items) != 1 {
		t.Fatalf("decoded %d items, want 1", len(decoded.Items))
	}
	wireCursor := decoded.Items[0].CompletedAt

	parsed, present, ok := parseISODateTimeQueryParam(wireCursor)
	if !present || !ok {
		t.Fatalf("parseISODateTimeQueryParam(%q) = present=%v ok=%v, want true/true -- this route's own emitted completed_at/next_cursor form must round-trip through its own cursor parser", wireCursor, present, ok)
	}
	if !parsed.Equal(completedAt) {
		t.Fatalf("parsed cursor = %v, want %v", parsed, completedAt)
	}

	capture := &cursorRoundTripCapture{}
	client := cursorRoundTripClient{identity: "alice@example.com", capture: capture}
	reader, err := people.NewReader(client)
	if err != nil {
		t.Fatalf("people.NewReader: %v", err)
	}
	handler := newPeopleDrilldownIssuesHandler(reader)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/people/anyone/drilldown/issues?cursor="+url.QueryEscape(wireCursor), nil)
	req.SetPathValue("person_id", "anyone")
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200, body=%s", rec.Code, rec.Body.String())
	}

	if capture.query == "" {
		t.Fatal("fetchPersonIssuesQuery was never called")
	}
	if !strings.Contains(capture.query, "AND wct.completed_at < {cursor:DateTime64(3, 'UTC')}") {
		t.Fatalf("query missing cursor filter:\n%s", capture.query)
	}
	boundCursor, ok := bindingValue(capture.bindings, "cursor")
	if !ok {
		t.Fatalf("cursor binding not found: %+v", capture.bindings)
	}
	boundTime, isTime := boundCursor.(time.Time)
	if !isTime {
		t.Fatalf("cursor binding = %#v, want a time.Time", boundCursor)
	}
	if !boundTime.Equal(completedAt) {
		t.Fatalf("bound cursor = %v, want %v -- the round trip through the response's own wire form and back through this route's own query param lost precision", boundTime, completedAt)
	}
}
