package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/routeswitch"
)

func TestSankeySwitchFromEnvDefaultsDisabled(t *testing.T) {
	sw := sankeySwitchFromEnv()
	if sw.Enabled(sankeyGetOperation) || sw.Enabled(sankeyPostOperation) {
		t.Fatal("expected both sankey operations disabled with no env var set")
	}
}

func TestSankeySwitchFromEnvEnabledViaEnvVar(t *testing.T) {
	t.Setenv(sankeyEnabledEnvVar, "true")
	sw := sankeySwitchFromEnv()
	if !sw.Enabled(sankeyGetOperation) || !sw.Enabled(sankeyPostOperation) {
		t.Fatal("expected both sankey operations enabled with GO_API_SANKEY_ENABLED=true")
	}
}

func TestSankeyRouteUnreachableWhenSwitchDisabled(t *testing.T) {
	sw := routeswitch.NewDynamicSwitch()
	mux := routeswitch.NewMux(sw)
	reached := false
	mux.Register(sankeyGetOperation, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reached = true
		w.WriteHeader(http.StatusOK)
	}))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/sankey", nil)
	rec := httptest.NewRecorder()
	mux.Dispatch(sankeyGetOperation, rec, req)
	if reached {
		t.Fatal("registered handler ran despite the switch being disabled")
	}
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}
}

// emptyRowsSankeyClient answers every system.tables/system.columns probe
// as present, and every other query with zero rows -- enough for a
// simple 200-path plumbing test.
type emptyRowsSankeyClient struct{}

func (emptyRowsSankeyClient) Query(_ context.Context, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	for _, b := range bindings {
		if b.Name == "tables" || b.Name == "columns" {
			names, _ := b.Value.([]string)
			rows := make([][]any, 0, len(names))
			for _, n := range names {
				rows = append(rows, []any{n})
			}
			return &sankeyFixtureScanner{rows: rows}, nil
		}
	}
	return &sankeyFixtureScanner{}, nil
}

type sankeyFixtureScanner struct {
	rows  [][]any
	index int
}

func (s *sankeyFixtureScanner) Next() bool {
	if s.index >= len(s.rows) {
		return false
	}
	s.index++
	return true
}

func (s *sankeyFixtureScanner) Scan(dest ...any) error {
	row := s.rows[s.index-1]
	for i, d := range dest {
		if sp, ok := d.(*string); ok {
			v, _ := row[i].(string)
			*sp = v
		}
	}
	return nil
}
func (s *sankeyFixtureScanner) Err() error   { return nil }
func (s *sankeyFixtureScanner) Close() error { return nil }

func TestNewSankeyGetHandlerRequiresAuthContext(t *testing.T) {
	handler := newSankeyGetHandler(emptyRowsSankeyClient{})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/sankey", nil)
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
}

// TestNewSankeyGetHandlerRangeDaysNotAnInt pins the 422 for a
// non-numeric range_days query param.
func TestNewSankeyGetHandlerRangeDaysNotAnInt(t *testing.T) {
	handler := newSankeyGetHandler(emptyRowsSankeyClient{})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/sankey?range_days=nope", nil)
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want %d, body=%s", rec.Code, http.StatusUnprocessableEntity, rec.Body.String())
	}
	var body pydanticValidationErrorBody
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if len(body.Detail) != 1 || body.Detail[0].Loc[1] != "range_days" {
		t.Fatalf("Detail = %+v, want one range_days error", body.Detail)
	}
}

// TestNewSankeyGetHandlerInvalidScopeTypeIs503 pins the Python
// asymmetry: GET's scope_type reaches ScopeFilter's own Pydantic
// constructor with no dedicated 422 surface, so an invalid value falls
// through the generic except-Exception 503, never a 400/404.
func TestNewSankeyGetHandlerInvalidScopeTypeIs503(t *testing.T) {
	handler := newSankeyGetHandler(emptyRowsSankeyClient{})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/sankey?scope_type=not-a-level", nil)
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d, body=%s", rec.Code, http.StatusServiceUnavailable, rec.Body.String())
	}
	if rec.Header().Get("X-DevHealth-Deprecated") != "" {
		t.Fatalf("X-DevHealth-Deprecated header set on a 503, want unset")
	}
}

// TestNewSankeyGetHandlerSuccessSetsDeprecatedHeader pins the 200 path
// and the GET-only deprecation header.
func TestNewSankeyGetHandlerSuccessSetsDeprecatedHeader(t *testing.T) {
	handler := newSankeyGetHandler(emptyRowsSankeyClient{})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/sankey?mode=hotspot", nil)
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if got := rec.Header().Get("X-DevHealth-Deprecated"); got != "use POST with filters" {
		t.Fatalf("X-DevHealth-Deprecated = %q, want %q", got, "use POST with filters")
	}
	if !strings.Contains(rec.Body.String(), `"mode":"hotspot"`) {
		t.Fatalf("body = %s, want mode=hotspot", rec.Body.String())
	}
}

func TestNewSankeyPostHandlerRequiresAuthContext(t *testing.T) {
	handler := newSankeyPostHandler(emptyRowsSankeyClient{})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/sankey", bytes.NewReader([]byte(`{}`)))
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
}

// TestNewSankeyPostHandlerEmptyBodyIsMissing pins the whole-body-missing
// 422 for an empty/null body.
func TestNewSankeyPostHandlerEmptyBodyIsMissing(t *testing.T) {
	handler := newSankeyPostHandler(emptyRowsSankeyClient{})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/sankey", bytes.NewReader(nil))
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want %d, body=%s", rec.Code, http.StatusUnprocessableEntity, rec.Body.String())
	}
	var body pydanticValidationErrorBody
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if len(body.Detail) != 1 || body.Detail[0].Loc[0] != "body" || len(body.Detail[0].Loc) != 1 {
		t.Fatalf("Detail = %+v, want one whole-body-missing error", body.Detail)
	}
}

// TestNewSankeyPostHandlerMissingModeAndFilters pins the aggregated
// missing-mode/missing-filters 422, in SankeyRequest's own field order.
func TestNewSankeyPostHandlerMissingModeAndFilters(t *testing.T) {
	handler := newSankeyPostHandler(emptyRowsSankeyClient{})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/sankey", bytes.NewReader([]byte(`{}`)))
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want %d, body=%s", rec.Code, http.StatusUnprocessableEntity, rec.Body.String())
	}
	var body pydanticValidationErrorBody
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if len(body.Detail) != 2 || body.Detail[0].Loc[1] != "mode" || body.Detail[1].Loc[1] != "filters" {
		t.Fatalf("Detail = %+v, want mode then filters", body.Detail)
	}
}

// TestNewSankeyPostHandlerInvalidModeLiteral pins the 422 for a mode
// value outside the four-way Literal.
func TestNewSankeyPostHandlerInvalidModeLiteral(t *testing.T) {
	handler := newSankeyPostHandler(emptyRowsSankeyClient{})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/sankey", bytes.NewReader([]byte(`{"mode":"nope","filters":{}}`)))
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want %d, body=%s", rec.Code, http.StatusUnprocessableEntity, rec.Body.String())
	}
	var body pydanticValidationErrorBody
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if len(body.Detail) != 1 || body.Detail[0].Type != "literal_error" {
		t.Fatalf("Detail = %+v, want one literal_error", body.Detail)
	}
}

// TestNewSankeyPostHandlerSuccessNoDeprecatedHeader pins the 200 path
// and that POST never sets the deprecation header.
func TestNewSankeyPostHandlerSuccessNoDeprecatedHeader(t *testing.T) {
	handler := newSankeyPostHandler(emptyRowsSankeyClient{})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/sankey", bytes.NewReader([]byte(`{"mode":"state","filters":{}}`)))
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if rec.Header().Get("X-DevHealth-Deprecated") != "" {
		t.Fatalf("X-DevHealth-Deprecated header set on POST, want unset")
	}
	if !strings.Contains(rec.Body.String(), `"mode":"state"`) {
		t.Fatalf("body = %s, want mode=state", rec.Body.String())
	}
}
