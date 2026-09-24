package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/drilldown"
)

// emptyRowsDrilldownClient answers every ClickHouse call with zero rows --
// same convention as quadrant_route_test.go's emptyRowsQuadrantClient.
type emptyRowsDrilldownClient struct{}

func (emptyRowsDrilldownClient) Query(context.Context, string, []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	return emptyRowsDrilldownScanner{}, nil
}

type emptyRowsDrilldownScanner struct{}

func (emptyRowsDrilldownScanner) Next() bool        { return false }
func (emptyRowsDrilldownScanner) Scan(...any) error { return nil }
func (emptyRowsDrilldownScanner) Err() error        { return nil }
func (emptyRowsDrilldownScanner) Close() error      { return nil }

func newEmptyRowsDrilldownReader(t *testing.T) *drilldown.Reader {
	t.Helper()
	reader, err := drilldown.NewReader(emptyRowsDrilldownClient{})
	if err != nil {
		t.Fatalf("drilldown.NewReader: %v", err)
	}
	return reader
}

// erroringDrilldownClient fails every ClickHouse call -- used to pin
// Python's blanket "except Exception: raise HTTPException(503, 'Data
// unavailable')" (main.py:973-974) surfacing through the HTTP layer.
type erroringDrilldownClient struct{}

func (erroringDrilldownClient) Query(context.Context, string, []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	return nil, errors.New("clickhouse unavailable")
}

func newErroringDrilldownReader(t *testing.T) *drilldown.Reader {
	t.Helper()
	reader, err := drilldown.NewReader(erroringDrilldownClient{})
	if err != nil {
		t.Fatalf("drilldown.NewReader: %v", err)
	}
	return reader
}

func TestDrilldownPRsSwitchFromEnvDefaultsDisabled(t *testing.T) {
	sw := drilldownPRsSwitchFromEnv(os.Getenv)
	if sw.Enabled(drilldownPRsGetOperation) || sw.Enabled(drilldownPRsPostOperation) {
		t.Fatal("expected both drilldown/prs operations disabled with no env var set")
	}
}

func TestDrilldownPRsSwitchFromEnvEnablesBothOperations(t *testing.T) {
	t.Setenv(drilldownPRsEnabledEnvVar, "true")
	sw := drilldownPRsSwitchFromEnv(os.Getenv)
	if !sw.Enabled(drilldownPRsGetOperation) {
		t.Fatal("expected drilldownPRsGetOperation enabled with GO_API_DRILLDOWN_PRS_ENABLED=true")
	}
	if !sw.Enabled(drilldownPRsPostOperation) {
		t.Fatal("expected drilldownPRsPostOperation enabled with GO_API_DRILLDOWN_PRS_ENABLED=true")
	}
}

// TestDrilldownUsesTheSharedISODateQueryParamParser pins that this
// route parses start_date/end_date via parseISODateQueryParam
// (pydantic_validation_error.go) -- the ONE shared implementation every
// REST route in this binary uses, not a per-route copy. The function's
// own behaviour is pinned once, by quadrant_route_test.go's equivalent
// coverage; this test only guards against a future edit reintroducing a
// second copy here.
func TestDrilldownUsesTheSharedISODateQueryParamParser(t *testing.T) {
	if _, present, ok := parseISODateQueryParam(""); present || !ok {
		t.Fatalf("parseISODateQueryParam(\"\") = present=%v ok=%v, want present=false ok=true", present, ok)
	}
	if got, present, ok := parseISODateQueryParam("2024-03-01"); !present || !ok || got.Year() != 2024 || got.Month() != 3 || got.Day() != 1 {
		t.Fatalf("parseISODateQueryParam(2024-03-01) = %v present=%v ok=%v, want 2024-03-01 true true", got, present, ok)
	}
	if _, present, ok := parseISODateQueryParam("not-a-date"); !present || ok {
		t.Fatalf("parseISODateQueryParam(malformed) = present=%v ok=%v, want present=true ok=false", present, ok)
	}
}

// TestNewDrilldownPRsGetHandlerRequiresAuthContext pins that the work
// handler (reached only after buildDrilldownPRsRoute's entryHandler
// already authenticated) itself still refuses a request with no claims
// attached, same defensive check every sibling REST route's work handler
// makes.
func TestNewDrilldownPRsGetHandlerRequiresAuthContext(t *testing.T) {
	handler := newDrilldownPRsGetHandler(newEmptyRowsDrilldownReader(t))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/drilldown/prs", nil)
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
	if got, want := rec.Body.String(), `{"detail":{"message":"Not authenticated"}}`+"\n"; got != want {
		t.Fatalf("body = %q, want %q", got, want)
	}
}

// TestNewDrilldownPRsGetHandlerHappyPathSetsDeprecatedHeader pins the
// GET route's Content-Type, its Python-parity X-DevHealth-Deprecated
// response header (main.py:970-971), and the "items" shape ([] present,
// not omitted/null) for a supported, empty-data request.
func TestNewDrilldownPRsGetHandlerHappyPathSetsDeprecatedHeader(t *testing.T) {
	handler := newDrilldownPRsGetHandler(newEmptyRowsDrilldownReader(t))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/drilldown/prs?scope_type=repo&scope_id=repo-a&range_days=7", nil)
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

// TestNewDrilldownPRsPostHandlerRequiresAuthContext mirrors the GET
// handler's own auth-context guard.
func TestNewDrilldownPRsPostHandlerRequiresAuthContext(t *testing.T) {
	handler := newDrilldownPRsPostHandler(newEmptyRowsDrilldownReader(t))
	req := httptest.NewRequest(http.MethodPost, "/api/v1/drilldown/prs", bytes.NewReader([]byte(`{"filters":{}}`)))
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
	if got, want := rec.Body.String(), `{"detail":{"message":"Not authenticated"}}`+"\n"; got != want {
		t.Fatalf("body = %q, want %q", got, want)
	}
}

// TestNewDrilldownPRsPostHandlerHappyPathNoDeprecatedHeader pins that
// POST never sets the GET-only deprecation header, and accepts an
// ignored "sort" field without error (drilldown_prs_post parses
// payload.sort but never reads it, api/main.py:910-935).
func TestNewDrilldownPRsPostHandlerHappyPathNoDeprecatedHeader(t *testing.T) {
	handler := newDrilldownPRsPostHandler(newEmptyRowsDrilldownReader(t))
	body := `{"filters":{"scope":{"level":"org"},"time":{"range_days":7}},"sort":"created_at","limit":10}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/drilldown/prs", bytes.NewReader([]byte(body)))
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

// TestNewDrilldownPRsPostHandlerLimitFallback pins "payload.limit or 50"
// (api/main.py:930): an explicit 0 falls back to 50.
func TestNewDrilldownPRsPostHandlerLimitFallback(t *testing.T) {
	var sawLimit any
	client := fakeQueryClientFunc(func(_ context.Context, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
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
	handler := newDrilldownPRsPostHandler(reader)
	body := `{"filters":{"scope":{"level":"org"}},"limit":0}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/drilldown/prs", bytes.NewReader([]byte(body)))
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

// fakeQueryClientFunc adapts a plain function to drilldown.QueryClient.
type fakeQueryClientFunc func(context.Context, string, []dhclickhouse.Binding) (dhclickhouse.RowScanner, error)

func (f fakeQueryClientFunc) Query(ctx context.Context, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	return f(ctx, query, bindings)
}

// TestNewDrilldownPRsGetHandlerClickHouseFailureIs503 pins Python's
// blanket "except Exception: raise HTTPException(503, 'Data unavailable')"
// (main.py:973-974) surfacing through the HTTP layer for the GET route.
func TestNewDrilldownPRsGetHandlerClickHouseFailureIs503(t *testing.T) {
	handler := newDrilldownPRsGetHandler(newErroringDrilldownReader(t))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/drilldown/prs", nil)
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
	if got, want := rec.Body.String(), `{"detail":"Data unavailable"}`+"\n"; got != want {
		t.Fatalf("body = %q, want %q", got, want)
	}
}

// TestNewDrilldownPRsPostHandlerClickHouseFailureIs503 mirrors the GET
// handler's own ClickHouse-failure case for POST.
func TestNewDrilldownPRsPostHandlerClickHouseFailureIs503(t *testing.T) {
	handler := newDrilldownPRsPostHandler(newErroringDrilldownReader(t))
	req := httptest.NewRequest(http.MethodPost, "/api/v1/drilldown/prs", bytes.NewReader([]byte(`{"filters":{}}`)))
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
	if got, want := rec.Body.String(), `{"detail":"Data unavailable"}`+"\n"; got != want {
		t.Fatalf("body = %q, want %q", got, want)
	}
}

// TestBuildDrilldownPRsRouteEntryHandlerRejectsUnknownMethod pins the
// entryHandler's own method guard: 405, not 404, with Starlette's own
// default {"detail": "Method Not Allowed"} body -- confirmed live against
// the real FastAPI app (see this route set's TEST-EVIDENCE). Every env
// var here is a placeholder value: construction is lazy (no live
// ClickHouse/JWKS dial happens for a request this entryHandler rejects
// before Dispatch).
func TestBuildDrilldownPRsRouteEntryHandlerRejectsUnknownMethod(t *testing.T) {
	t.Setenv("CLICKHOUSE_URI", "clickhouse://localhost:8123/default")
	t.Setenv("GO_API_ENVELOPE_JWKS_PATH", filepath.Join(t.TempDir(), "missing-jwks.json"))
	t.Setenv("GO_API_ENVELOPE_ISSUER", "test-issuer")
	t.Setenv("GO_API_ENVELOPE_AUDIENCE", "test-audience")

	handler, cleanup, ok, err := buildDrilldownPRsRoute(os.Getenv)
	if err != nil {
		t.Fatalf("buildDrilldownPRsRoute: %v", err)
	}
	if !ok {
		t.Fatal("buildDrilldownPRsRoute: ok = false, want true with every dependency env var set")
	}
	defer cleanup()

	req := httptest.NewRequest(http.MethodPut, "/api/v1/drilldown/prs", nil)
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusMethodNotAllowed)
	}
	if got, want := rec.Body.String(), `{"detail":"Method Not Allowed"}`+"\n"; got != want {
		t.Fatalf("body = %q, want %q", got, want)
	}
}
