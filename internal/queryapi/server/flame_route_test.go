package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
)

// emptyRowsFlameClient answers every ClickHouse call with zero rows --
// same convention as quadrant_route_test.go/people_route_test.go's own
// empty-rows fakes.
type emptyRowsFlameClient struct{}

func (emptyRowsFlameClient) Query(context.Context, string, []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	return emptyRowsFlameScanner{}, nil
}

type emptyRowsFlameScanner struct{}

func (emptyRowsFlameScanner) Next() bool        { return false }
func (emptyRowsFlameScanner) Scan(...any) error { return nil }
func (emptyRowsFlameScanner) Err() error        { return nil }
func (emptyRowsFlameScanner) Close() error      { return nil }

func TestFlameSwitchFromEnvDefaultsDisabled(t *testing.T) {
	sw := flameSwitchFromEnv(os.Getenv)
	if sw.Enabled(flameOperation) {
		t.Fatal("expected flameOperation to be disabled with no env var set")
	}
}

func TestFlameSwitchFromEnvEnabledViaEnvVar(t *testing.T) {
	t.Setenv(flameEnabledEnvVar, "true")
	sw := flameSwitchFromEnv(os.Getenv)
	if !sw.Enabled(flameOperation) {
		t.Fatal("expected flameOperation to be enabled with GO_API_FLAME_ENABLED=true")
	}
}

// TestNewFlameWorkHandlerRequiresAuthContext pins that the work handler
// (reached only after buildFlameRoute's entryHandler already
// authenticated) itself still refuses a request with no claims attached.
func TestNewFlameWorkHandlerRequiresAuthContext(t *testing.T) {
	handler := newFlameWorkHandler(emptyRowsFlameClient{})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/flame?entity_type=issue&entity_id=abc", nil)
	rec := httptest.NewRecorder()
	serveRoute(t, handler, rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
	if got, want := rec.Body.String(), `{"detail":{"message":"Not authenticated"}}`+"\n"; got != want {
		t.Fatalf("body = %q, want %q", got, want)
	}
}

// TestNewFlameWorkHandlerRequiresEntityTypeAndEntityID pins the missing-
// field 422, aggregating BOTH entity_type and entity_id in the order
// main.py's flame() signature declares them, matching FastAPI's own
// RequestValidationError body for two required query params with no
// value on the wire (the same shape quadrant_route_test.go's own
// TestNewQuadrantWorkHandlerAggregatesMultipleValidationErrors pins).
func TestNewFlameWorkHandlerRequiresEntityTypeAndEntityID(t *testing.T) {
	handler := newFlameWorkHandler(emptyRowsFlameClient{})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/flame", nil)
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
	rec := httptest.NewRecorder()
	serveRoute(t, handler, rec, req)
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusUnprocessableEntity)
	}
	var body pydanticValidationErrorBody
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v (body=%s)", err, rec.Body.String())
	}
	want := pydanticValidationErrorBody{Detail: []pydanticErrorDetail{
		{Type: "missing", Loc: []any{"query", "entity_type"}, Msg: "Field required", Input: nil},
		{Type: "missing", Loc: []any{"query", "entity_id"}, Msg: "Field required", Input: nil},
	}}
	if !reflect.DeepEqual(body, want) {
		t.Fatalf("body = %+v, want %+v", body, want)
	}
}

// TestNewFlameWorkHandlerMissingEntityTypeOnly pins the single-field case
// (entity_id present, entity_type absent).
func TestNewFlameWorkHandlerMissingEntityTypeOnly(t *testing.T) {
	handler := newFlameWorkHandler(emptyRowsFlameClient{})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/flame?entity_id=abc", nil)
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
	rec := httptest.NewRecorder()
	serveRoute(t, handler, rec, req)
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusUnprocessableEntity)
	}
	var body pydanticValidationErrorBody
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v (body=%s)", err, rec.Body.String())
	}
	want := pydanticValidationErrorBody{Detail: []pydanticErrorDetail{
		{Type: "missing", Loc: []any{"query", "entity_type"}, Msg: "Field required", Input: nil},
	}}
	if !reflect.DeepEqual(body, want) {
		t.Fatalf("body = %+v, want %+v", body, want)
	}
}

// TestNewFlameWorkHandlerValidationRunsBeforeComparativeParamCheck pins
// the ordering confirmed live for /api/v1/people (people_route.go's own
// doc comment): a request missing a required field AND carrying a
// forbidden comparative param answers 422 (validation), never 400
// (comparative rejection) -- FastAPI resolves typed query params before
// the route's own body-level _reject_comparative_params check ever runs.
func TestNewFlameWorkHandlerValidationRunsBeforeComparativeParamCheck(t *testing.T) {
	handler := newFlameWorkHandler(emptyRowsFlameClient{})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/flame?entity_id=abc&compare_to=1", nil)
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
	rec := httptest.NewRecorder()
	serveRoute(t, handler, rec, req)
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want %d (validation must win over the comparative-param check)", rec.Code, http.StatusUnprocessableEntity)
	}
}

// TestNewFlameWorkHandlerComparativeParamRejected pins
// _reject_comparative_params (main.py:202-208): a forbidden query-param
// KEY with both required fields present is a 400, matching the exact body
// shape restErrorBody produces.
func TestNewFlameWorkHandlerComparativeParamRejected(t *testing.T) {
	handler := newFlameWorkHandler(emptyRowsFlameClient{})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/flame?entity_type=issue&entity_id=abc&rank=1", nil)
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
	rec := httptest.NewRecorder()
	serveRoute(t, handler, rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
	if got, want := rec.Body.String(), `{"detail":"Comparative parameters are not supported."}`+"\n"; got != want {
		t.Fatalf("body = %q, want %q", got, want)
	}
}

// TestNewFlameWorkHandlerUnknownEntityTypeIs404 pins the resolver's 404
// surfacing through the HTTP layer via flame.AsRequestError.
func TestNewFlameWorkHandlerUnknownEntityTypeIs404(t *testing.T) {
	handler := newFlameWorkHandler(emptyRowsFlameClient{})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/flame?entity_type=bogus&entity_id=abc", nil)
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
	rec := httptest.NewRecorder()
	serveRoute(t, handler, rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}
	if got, want := rec.Body.String(), `{"detail":"Unknown entity type"}`+"\n"; got != want {
		t.Fatalf("body = %q, want %q", got, want)
	}
}

// TestNewFlameWorkHandlerInvalidRepoPrefixIs400 pins _parse_repo_entity's
// own 400 surfacing through the HTTP layer for the "pr" branch.
func TestNewFlameWorkHandlerInvalidRepoPrefixIs400(t *testing.T) {
	handler := newFlameWorkHandler(emptyRowsFlameClient{})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/flame?entity_type=pr&entity_id=not-prefixed", nil)
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
	rec := httptest.NewRecorder()
	serveRoute(t, handler, rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
	if got, want := rec.Body.String(), `{"detail":"Entity id must include repo_id prefix"}`+"\n"; got != want {
		t.Fatalf("body = %q, want %q", got, want)
	}
}

// TestNewFlameWorkHandlerIssueNotFoundIs404 pins the issue branch's own
// 404 surfacing through the HTTP layer for an unresolvable work item.
func TestNewFlameWorkHandlerIssueNotFoundIs404(t *testing.T) {
	handler := newFlameWorkHandler(emptyRowsFlameClient{})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/flame?entity_type=issue&entity_id=missing", nil)
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
	rec := httptest.NewRecorder()
	serveRoute(t, handler, rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}
	if got, want := rec.Body.String(), `{"detail":"Issue not found"}`+"\n"; got != want {
		t.Fatalf("body = %q, want %q", got, want)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", ct)
	}
}

// TestBuildFlameRouteEntryHandlerRejectsNonGET pins the entryHandler's own
// method guard: 405, not 404, with Starlette's own default
// {"detail": "Method Not Allowed"} body -- the same pattern
// TestBuildQuadrantRouteEntryHandlerRejectsNonGET already establishes.
// Every env var here is a placeholder value: construction is lazy (no
// live ClickHouse/JWKS dial happens for a request this entryHandler
// rejects before Dispatch).
func TestBuildFlameRouteEntryHandlerRejectsNonGET(t *testing.T) {
	t.Setenv("CLICKHOUSE_URI", "clickhouse://localhost:8123/default")
	t.Setenv("GO_API_ENVELOPE_JWKS_PATH", filepath.Join(t.TempDir(), "missing-jwks.json"))
	t.Setenv("GO_API_ENVELOPE_ISSUER", "test-issuer")
	t.Setenv("GO_API_ENVELOPE_AUDIENCE", "test-audience")

	handler, cleanup, ok, err := buildFlameRoute(os.Getenv)
	if err != nil {
		t.Fatalf("buildFlameRoute: %v", err)
	}
	if !ok {
		t.Fatal("buildFlameRoute: ok = false, want true with every dependency env var set")
	}
	defer cleanup()

	req := httptest.NewRequest(http.MethodPost, "/api/v1/flame", nil)
	rec := httptest.NewRecorder()
	serveRoute(t, handler, rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusMethodNotAllowed)
	}
	if got, want := rec.Body.String(), `{"detail":"Method Not Allowed"}`+"\n"; got != want {
		t.Fatalf("body = %q, want %q", got, want)
	}
}

// TestBuildFlameRouteMissingConfigStaysUnmounted pins the "stay
// unmounted, don't fail to build/start" contract for a missing dependency
// env var.
func TestBuildFlameRouteMissingConfigStaysUnmounted(t *testing.T) {
	handler, cleanup, ok, err := buildFlameRoute(os.Getenv)
	if err != nil {
		t.Fatalf("buildFlameRoute: %v", err)
	}
	if ok {
		t.Fatal("buildFlameRoute: ok = true, want false with no dependency env vars set")
	}
	if handler != nil || cleanup != nil {
		t.Fatal("buildFlameRoute: expected nil handler/cleanup when not configured")
	}
}
