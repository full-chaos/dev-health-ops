package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/authctx"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/drilldown"
)

// errUnexpectedScopeFilter is returned by a fake ClickHouse client when a
// test asserts NO team scope filter should have been added to the query
// (scope_filter_for_metric's own asymmetry, see scopeClauseTeam's doc
// comment) but one was present anyway.
var errUnexpectedScopeFilter = errors.New("unexpected scope filter in query")

func newEmptyRowsDrilldownIssuesReader(t *testing.T) *drilldown.Reader {
	t.Helper()
	reader, err := drilldown.NewReader(emptyRowsDrilldownClient{})
	if err != nil {
		t.Fatalf("drilldown.NewReader: %v", err)
	}
	return reader
}

func TestDrilldownIssuesSwitchFromEnvDefaultsDisabled(t *testing.T) {
	sw := drilldownIssuesSwitchFromEnv()
	if sw.Enabled(drilldownIssuesGetOperation) || sw.Enabled(drilldownIssuesPostOperation) {
		t.Fatal("expected both drilldown/issues operations disabled with no env var set")
	}
}

func TestDrilldownIssuesSwitchFromEnvEnablesBothOperations(t *testing.T) {
	t.Setenv(drilldownIssuesEnabledEnvVar, "true")
	sw := drilldownIssuesSwitchFromEnv()
	if !sw.Enabled(drilldownIssuesGetOperation) {
		t.Fatal("expected drilldownIssuesGetOperation enabled with GO_API_DRILLDOWN_ISSUES_ENABLED=true")
	}
	if !sw.Enabled(drilldownIssuesPostOperation) {
		t.Fatal("expected drilldownIssuesPostOperation enabled with GO_API_DRILLDOWN_ISSUES_ENABLED=true")
	}
}

// TestNewDrilldownIssuesGetHandlerRequiresAuthContext pins that the work
// handler (reached only after buildDrilldownIssuesRoute's entryHandler
// already authenticated) itself still refuses a request with no claims
// attached, same defensive check every sibling REST route's work handler
// makes.
func TestNewDrilldownIssuesGetHandlerRequiresAuthContext(t *testing.T) {
	handler := newDrilldownIssuesGetHandler(newEmptyRowsDrilldownIssuesReader(t))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/drilldown/issues", nil)
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
}

// TestNewDrilldownIssuesGetHandlerHappyPathSetsDeprecatedHeader pins the
// GET route's Content-Type, its Python-parity X-DevHealth-Deprecated
// response header (main.py:1041-1042), and the "items" shape ([] present,
// not omitted/null) for a supported, empty-data request.
func TestNewDrilldownIssuesGetHandlerHappyPathSetsDeprecatedHeader(t *testing.T) {
	handler := newDrilldownIssuesGetHandler(newEmptyRowsDrilldownIssuesReader(t))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/drilldown/issues?scope_type=team&scope_id=team-a&range_days=7", nil)
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", ct)
	}
	if got := rec.Header().Get("X-DevHealth-Deprecated"); got != "use POST with filters" {
		t.Fatalf("X-DevHealth-Deprecated = %q, want %q", got, "use POST with filters")
	}
	var decoded struct {
		Items []map[string]any `json:"items"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &decoded); err != nil {
		t.Fatalf("decode body: %v (body=%s)", err, rec.Body.String())
	}
	if decoded.Items == nil {
		t.Fatal("items must be [] (present), not omitted/null")
	}
}

// TestNewDrilldownIssuesGetHandlerDefaultOrgScopeNoTeamFilter pins that a
// GET request with no scope_type query param (defaulting to "org") issues
// a query with no team scope filter -- scope_filter_for_metric's own
// asymmetry (see scopeClauseTeam's doc comment).
func TestNewDrilldownIssuesGetHandlerDefaultOrgScopeNoTeamFilter(t *testing.T) {
	client := fakeQueryClientFunc(func(_ context.Context, query string, _ []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		if strings.Contains(query, "scope_ids") {
			return nil, errUnexpectedScopeFilter
		}
		return emptyRowsDrilldownScanner{}, nil
	})
	reader, err := drilldown.NewReader(client)
	if err != nil {
		t.Fatalf("drilldown.NewReader: %v", err)
	}
	handler := newDrilldownIssuesGetHandler(reader)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/drilldown/issues", nil)
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
}

// TestNewDrilldownIssuesPostHandlerRequiresAuthContext mirrors the GET
// handler's own auth-context guard.
func TestNewDrilldownIssuesPostHandlerRequiresAuthContext(t *testing.T) {
	handler := newDrilldownIssuesPostHandler(newEmptyRowsDrilldownIssuesReader(t))
	req := httptest.NewRequest(http.MethodPost, "/api/v1/drilldown/issues", bytes.NewReader([]byte(`{"filters":{}}`)))
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
}

// TestNewDrilldownIssuesPostHandlerHappyPathNoDeprecatedHeader pins that
// POST never sets the GET-only deprecation header, and accepts an ignored
// "sort" field without error (drilldown_issues_post parses payload.sort
// but never reads it, api/main.py:977-1005).
func TestNewDrilldownIssuesPostHandlerHappyPathNoDeprecatedHeader(t *testing.T) {
	handler := newDrilldownIssuesPostHandler(newEmptyRowsDrilldownIssuesReader(t))
	body := `{"filters":{"scope":{"level":"team","ids":["team-a"]},"time":{"range_days":7}},"sort":"completed_at","limit":10}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/drilldown/issues", bytes.NewReader([]byte(body)))
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if got := rec.Header().Get("X-DevHealth-Deprecated"); got != "" {
		t.Fatalf("X-DevHealth-Deprecated = %q, want unset on POST", got)
	}
}

// TestNewDrilldownIssuesPostHandlerLimitFallback pins "payload.limit or
// 50" (api/main.py:1000): an explicit 0 falls back to 50.
func TestNewDrilldownIssuesPostHandlerLimitFallback(t *testing.T) {
	var sawLimit any
	client := fakeQueryClientFunc(func(_ context.Context, _ string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		for _, b := range bindings {
			if b.Name == "limit" {
				sawLimit = b.Value
			}
		}
		return emptyRowsDrilldownScanner{}, nil
	})
	reader, err := drilldown.NewReader(client)
	if err != nil {
		t.Fatalf("drilldown.NewReader: %v", err)
	}
	handler := newDrilldownIssuesPostHandler(reader)
	body := `{"filters":{"scope":{"level":"org"}},"limit":0}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/drilldown/issues", bytes.NewReader([]byte(body)))
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if sawLimit != 50 {
		t.Fatalf("limit binding = %v, want 50", sawLimit)
	}
}

// TestNewDrilldownIssuesPostHandlerNonTeamScopeIgnoresIDs pins
// scope_filter_for_metric's own asymmetry on the POST path: a "repo" scope
// level with explicit ids still issues no team scope filter (see
// scopeClauseTeam's doc comment).
func TestNewDrilldownIssuesPostHandlerNonTeamScopeIgnoresIDs(t *testing.T) {
	client := fakeQueryClientFunc(func(_ context.Context, query string, _ []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		if strings.Contains(query, "scope_ids") {
			return nil, errUnexpectedScopeFilter
		}
		return emptyRowsDrilldownScanner{}, nil
	})
	reader, err := drilldown.NewReader(client)
	if err != nil {
		t.Fatalf("drilldown.NewReader: %v", err)
	}
	handler := newDrilldownIssuesPostHandler(reader)
	body := `{"filters":{"scope":{"level":"repo","ids":["some-repo"]}}}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/drilldown/issues", bytes.NewReader([]byte(body)))
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
}
