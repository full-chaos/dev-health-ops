package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/investment"
)

// emptyRowsInvestmentClient answers every ClickHouse call (including the
// table/columns-present checks) with zero rows -- work_unit_investments
// then reads as absent, so BuildResponse/BuildSunburstResponse take
// their early "empty" return, matching
// build_investment_response/build_investment_sunburst's own
// _tables_present(False) branch. Sufficient for tests that only assert
// HTTP-layer behaviour (headers, status, validation), not response
// content.
type emptyRowsInvestmentClient struct{}

func (emptyRowsInvestmentClient) Query(context.Context, string, []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	return emptyRowsInvestmentScanner{}, nil
}

type emptyRowsInvestmentScanner struct{}

func (emptyRowsInvestmentScanner) Next() bool        { return false }
func (emptyRowsInvestmentScanner) Scan(...any) error { return nil }
func (emptyRowsInvestmentScanner) Err() error        { return nil }
func (emptyRowsInvestmentScanner) Close() error      { return nil }

func newEmptyRowsInvestmentReader(t *testing.T) *investment.Reader {
	t.Helper()
	reader, err := investment.NewReader(emptyRowsInvestmentClient{})
	if err != nil {
		t.Fatalf("investment.NewReader: %v", err)
	}
	return reader
}

// tablePresentInvestmentClient answers the table/columns-present checks
// truthfully and every other query with zero rows -- BuildResponse/
// BuildSunburstResponse reach their real ClickHouse reads (unlike
// emptyRowsInvestmentClient) and return a real, empty-data 200.
type tablePresentInvestmentClient struct{}

func (tablePresentInvestmentClient) Query(_ context.Context, query string, _ []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	switch {
	case strings.Contains(query, "FROM system.tables"):
		return &fixedRowsInvestmentScanner{rows: [][]any{{"work_unit_investments"}}}, nil
	case strings.Contains(query, "FROM system.columns"):
		return &fixedRowsInvestmentScanner{rows: [][]any{
			{"from_ts"}, {"to_ts"}, {"repo_id"}, {"effort_value"},
			{"theme_distribution_json"}, {"subcategory_distribution_json"},
		}}, nil
	default:
		return emptyRowsInvestmentScanner{}, nil
	}
}

type fixedRowsInvestmentScanner struct {
	rows  [][]any
	index int
}

func (s *fixedRowsInvestmentScanner) Next() bool {
	if s.index >= len(s.rows) {
		return false
	}
	s.index++
	return true
}

func (s *fixedRowsInvestmentScanner) Scan(dest ...any) error {
	row := s.rows[s.index-1]
	for i, d := range dest {
		if typed, ok := d.(*string); ok {
			*typed = row[i].(string)
		}
	}
	return nil
}
func (s *fixedRowsInvestmentScanner) Err() error   { return nil }
func (s *fixedRowsInvestmentScanner) Close() error { return nil }

func newTablePresentInvestmentReader(t *testing.T) *investment.Reader {
	t.Helper()
	reader, err := investment.NewReader(tablePresentInvestmentClient{})
	if err != nil {
		t.Fatalf("investment.NewReader: %v", err)
	}
	return reader
}

// erroringAfterTablePresentInvestmentClient answers the table/columns
// checks truthfully, then fails every subsequent query -- pins Python's
// blanket "except Exception: raise HTTPException(503, 'Data
// unavailable')" (main.py:1240-1241 &c) for a failure that happens AFTER
// work_unit_investments is confirmed present (a query failure DURING the
// presence check itself fails closed to the empty-response 200 branch
// instead -- see tableExists/columnsExist's own doc comments -- so it
// does not exercise this 503 path).
type erroringAfterTablePresentInvestmentClient struct{}

func (erroringAfterTablePresentInvestmentClient) Query(_ context.Context, query string, _ []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	switch {
	case strings.Contains(query, "FROM system.tables"):
		return &fixedRowsInvestmentScanner{rows: [][]any{{"work_unit_investments"}}}, nil
	case strings.Contains(query, "FROM system.columns"):
		return &fixedRowsInvestmentScanner{rows: [][]any{
			{"from_ts"}, {"to_ts"}, {"repo_id"}, {"effort_value"},
			{"theme_distribution_json"}, {"subcategory_distribution_json"},
		}}, nil
	default:
		return nil, errors.New("clickhouse unavailable")
	}
}

func newErroringInvestmentReader(t *testing.T) *investment.Reader {
	t.Helper()
	reader, err := investment.NewReader(erroringAfterTablePresentInvestmentClient{})
	if err != nil {
		t.Fatalf("investment.NewReader: %v", err)
	}
	return reader
}

func TestInvestmentSwitchFromEnvDefaultsDisabled(t *testing.T) {
	sw := investmentSwitchFromEnv(os.Getenv)
	if sw.Enabled(investmentGetOperation) || sw.Enabled(investmentPostOperation) {
		t.Fatal("expected both investment operations disabled with no env var set")
	}
	sunburstSW := investmentSunburstSwitchFromEnv(os.Getenv)
	if sunburstSW.Enabled(investmentSunburstGetOperation) {
		t.Fatal("expected investment/sunburst disabled with no env var set")
	}
}

func TestInvestmentSwitchFromEnvEnablesBothOperations(t *testing.T) {
	t.Setenv(investmentEnabledEnvVar, "true")
	sw := investmentSwitchFromEnv(os.Getenv)
	if !sw.Enabled(investmentGetOperation) || !sw.Enabled(investmentPostOperation) {
		t.Fatal("expected both investment operations enabled with GO_API_INVESTMENT_ENABLED=true")
	}
}

func TestInvestmentSunburstSwitchFromEnvEnablesOperation(t *testing.T) {
	t.Setenv(investmentSunburstEnabledEnvVar, "true")
	sw := investmentSunburstSwitchFromEnv(os.Getenv)
	if !sw.Enabled(investmentSunburstGetOperation) {
		t.Fatal("expected investment/sunburst enabled with GO_API_INVESTMENT_SUNBURST_ENABLED=true")
	}
}

func TestNewInvestmentGetHandlerRequiresAuthContext(t *testing.T) {
	handler := newInvestmentGetHandler(newEmptyRowsInvestmentReader(t))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/investment", nil)
	rec := httptest.NewRecorder()
	serveRoute(t, handler, rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
	if got, want := rec.Body.String(), `{"detail":{"message":"Not authenticated"}}`+"\n"; got != want {
		t.Fatalf("body = %q, want %q", got, want)
	}
}

func TestNewInvestmentPostHandlerRequiresAuthContext(t *testing.T) {
	handler := newInvestmentPostHandler(newEmptyRowsInvestmentReader(t))
	req := httptest.NewRequest(http.MethodPost, "/api/v1/investment", bytes.NewReader([]byte(`{"filters":{}}`)))
	rec := httptest.NewRecorder()
	serveRoute(t, handler, rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
}

func TestNewInvestmentSunburstGetHandlerRequiresAuthContext(t *testing.T) {
	handler := newInvestmentSunburstGetHandler(newEmptyRowsInvestmentReader(t))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/investment/sunburst", nil)
	rec := httptest.NewRecorder()
	serveRoute(t, handler, rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
}

// TestNewInvestmentGetHandlerHappyPathSetsDeprecatedHeader pins the GET
// route's Content-Type, its Python-parity X-DevHealth-Deprecated
// response header (main.py:1237-1238), and the always-present (never
// omitted/null) theme_distribution/subcategory_distribution/edges
// fields for a supported, empty-data request.
func TestNewInvestmentGetHandlerHappyPathSetsDeprecatedHeader(t *testing.T) {
	handler := newInvestmentGetHandler(newTablePresentInvestmentReader(t))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/investment?scope_type=repo&scope_id=repo-a&range_days=7", nil)
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
	rec := httptest.NewRecorder()
	serveRoute(t, handler, rec, req)
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
		ThemeDistribution       map[string]float64 `json:"theme_distribution"`
		SubcategoryDistribution map[string]float64 `json:"subcategory_distribution"`
		Edges                   []map[string]any   `json:"edges"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &decoded); err != nil {
		t.Fatalf("decode body: %v (body=%s)", err, rec.Body.String())
	}
	if decoded.ThemeDistribution == nil || decoded.SubcategoryDistribution == nil || decoded.Edges == nil {
		t.Fatalf("expected theme_distribution/subcategory_distribution/edges all present, not null; got body=%s", rec.Body.String())
	}
}

// TestNewInvestmentPostHandlerHappyPathNoDeprecatedHeader pins that POST
// never sets the GET-only deprecation header.
func TestNewInvestmentPostHandlerHappyPathNoDeprecatedHeader(t *testing.T) {
	handler := newInvestmentPostHandler(newTablePresentInvestmentReader(t))
	body := `{"filters":{"scope":{"level":"org"},"time":{"range_days":7}}}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/investment", bytes.NewReader([]byte(body)))
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
	rec := httptest.NewRecorder()
	serveRoute(t, handler, rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if got := rec.Header().Get("X-DevHealth-Deprecated"); got != "" {
		t.Fatalf("X-DevHealth-Deprecated = %q, want unset on POST", got)
	}
}

// TestNewInvestmentPostHandlerMissingFiltersIs422 pins the shared
// "missing filters key" validator every body-carrying REST route in this
// binary shares.
func TestNewInvestmentPostHandlerMissingFiltersIs422(t *testing.T) {
	handler := newInvestmentPostHandler(newEmptyRowsInvestmentReader(t))
	req := httptest.NewRequest(http.MethodPost, "/api/v1/investment", bytes.NewReader([]byte(`{}`)))
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
	rec := httptest.NewRecorder()
	serveRoute(t, handler, rec, req)
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want %d, body=%s", rec.Code, http.StatusUnprocessableEntity, rec.Body.String())
	}
}

// TestNewInvestmentPostHandlerInvalidScopeLevelIs422 pins the shared
// nested MetricFilter validator (pydantic_metric_filter.go).
func TestNewInvestmentPostHandlerInvalidScopeLevelIs422(t *testing.T) {
	handler := newInvestmentPostHandler(newEmptyRowsInvestmentReader(t))
	body := `{"filters":{"scope":{"level":"not-a-real-scope-level"}}}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/investment", bytes.NewReader([]byte(body)))
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
	rec := httptest.NewRecorder()
	serveRoute(t, handler, rec, req)
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want %d, body=%s", rec.Code, http.StatusUnprocessableEntity, rec.Body.String())
	}
}

// TestNewInvestmentGetHandlerInvalidRangeDaysIs422 pins the shared
// int_parsing validator every GET route with a range_days param shares.
func TestNewInvestmentGetHandlerInvalidRangeDaysIs422(t *testing.T) {
	handler := newInvestmentGetHandler(newEmptyRowsInvestmentReader(t))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/investment?range_days=not-a-number", nil)
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
	rec := httptest.NewRecorder()
	serveRoute(t, handler, rec, req)
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want %d, body=%s", rec.Code, http.StatusUnprocessableEntity, rec.Body.String())
	}
}

// TestNewInvestmentGetHandlerClickHouseFailureIs503 pins Python's
// blanket 503 fallback surfacing through the HTTP layer once the table
// presence check has already succeeded.
func TestNewInvestmentGetHandlerClickHouseFailureIs503(t *testing.T) {
	handler := newInvestmentGetHandler(newErroringInvestmentReader(t))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/investment", nil)
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
	rec := httptest.NewRecorder()
	serveRoute(t, handler, rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d, body=%s", rec.Code, http.StatusServiceUnavailable, rec.Body.String())
	}
	if got, want := rec.Body.String(), `{"detail":"Data unavailable"}`+"\n"; got != want {
		t.Fatalf("body = %q, want %q", got, want)
	}
}

// TestNewInvestmentSunburstGetHandlerHappyPathBareArray pins the sunburst
// route's bare-JSON-ARRAY wire shape ([] present, never an object
// wrapping one) and its own deprecation header.
func TestNewInvestmentSunburstGetHandlerHappyPathBareArray(t *testing.T) {
	handler := newInvestmentSunburstGetHandler(newTablePresentInvestmentReader(t))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/investment/sunburst", nil)
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
	rec := httptest.NewRecorder()
	serveRoute(t, handler, rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if got := rec.Header().Get("X-DevHealth-Deprecated"); got != "use POST with filters" {
		t.Fatalf("X-DevHealth-Deprecated = %q, want %q", got, "use POST with filters")
	}
	body := strings.TrimSpace(rec.Body.String())
	if !strings.HasPrefix(body, "[") {
		t.Fatalf("expected a bare JSON array body, got %q", body)
	}
	var decoded []map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &decoded); err != nil {
		t.Fatalf("decode body: %v (body=%s)", err, rec.Body.String())
	}
}

// TestNewInvestmentSunburstGetHandlerInvalidLimitIs422 pins that an
// unparseable limit query param is a 422, the same int_parsing shape
// every other int query parameter in this binary answers.
func TestNewInvestmentSunburstGetHandlerInvalidLimitIs422(t *testing.T) {
	handler := newInvestmentSunburstGetHandler(newEmptyRowsInvestmentReader(t))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/investment/sunburst?limit=not-a-number", nil)
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
	rec := httptest.NewRecorder()
	serveRoute(t, handler, rec, req)
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want %d, body=%s", rec.Code, http.StatusUnprocessableEntity, rec.Body.String())
	}
}

// TestBuildInvestmentRouteStaysUnmountedWithoutConfig pins the "stay
// unmounted, don't fail to build/start" contract every optionally-
// configured route in this binary follows when its dependency env vars
// are unset.
func TestBuildInvestmentRouteStaysUnmountedWithoutConfig(t *testing.T) {
	t.Setenv("CLICKHOUSE_URI", "")
	handler, cleanup, ok, err := buildInvestmentRoute(os.Getenv)
	if err != nil {
		t.Fatalf("buildInvestmentRoute: %v", err)
	}
	if ok {
		t.Fatal("expected buildInvestmentRoute to stay unmounted with no CLICKHOUSE_URI configured")
	}
	if handler != nil || cleanup != nil {
		t.Fatal("expected nil handler/cleanup when not configured")
	}
}
