package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/authctx"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/investmentexplain"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/routeswitch"
)

// TestWorkUnitsSwitchFromEnvDefaultsDisabled mirrors
// TestQuadrantSwitchFromEnvDefaultsDisabled: with no env var set, neither
// operation is enabled.
func TestWorkUnitsSwitchFromEnvDefaultsDisabled(t *testing.T) {
	sw := workUnitsSwitchFromEnv()
	if sw.Enabled(workUnitsGetOperation) || sw.Enabled(workUnitsPostOperation) {
		t.Fatal("expected both work-units operations to be disabled with no env var set")
	}
}

// TestWorkUnitsSwitchFromEnvEnabledViaEnvVar is the other half.
func TestWorkUnitsSwitchFromEnvEnabledViaEnvVar(t *testing.T) {
	t.Setenv(workUnitsEnabledEnvVar, "true")
	sw := workUnitsSwitchFromEnv()
	if !sw.Enabled(workUnitsGetOperation) || !sw.Enabled(workUnitsPostOperation) {
		t.Fatal("expected both work-units operations to be enabled with GO_API_WORK_UNITS_ENABLED=true")
	}
}

// TestWorkUnitsRouteUnreachableWhenSwitchDisabled mirrors
// TestQuadrantRouteUnreachableWhenSwitchDisabled.
func TestWorkUnitsRouteUnreachableWhenSwitchDisabled(t *testing.T) {
	sw := routeswitch.NewDynamicSwitch()
	mux := routeswitch.NewMux(sw)

	reached := false
	mux.Register(workUnitsGetOperation, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reached = true
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/work-units", nil)
	rec := httptest.NewRecorder()
	mux.Dispatch(workUnitsGetOperation, rec, req)

	if reached {
		t.Fatal("registered handler ran despite the switch being disabled")
	}
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}
}

// emptyRowsWorkUnitsClient answers every ClickHouse call with zero rows --
// used where the test only cares about HTTP-layer plumbing (missing auth,
// missing/invalid params, response shape for an empty result).
type emptyRowsWorkUnitsClient struct{}

func (emptyRowsWorkUnitsClient) Query(context.Context, string, []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	return emptyRowsWorkUnitsScanner{}, nil
}

type emptyRowsWorkUnitsScanner struct{}

func (emptyRowsWorkUnitsScanner) Next() bool        { return false }
func (emptyRowsWorkUnitsScanner) Scan(...any) error { return nil }
func (emptyRowsWorkUnitsScanner) Err() error        { return nil }
func (emptyRowsWorkUnitsScanner) Close() error      { return nil }

func newTestWorkUnitsReader(t *testing.T) *investmentexplain.Reader {
	t.Helper()
	reader, err := investmentexplain.NewReader(emptyRowsWorkUnitsClient{})
	if err != nil {
		t.Fatalf("investmentexplain.NewReader: %v", err)
	}
	return reader
}

// TestNewWorkUnitsGetHandlerRequiresAuthContext pins that the work handler
// (reached only after buildWorkUnitsRoute's entryHandler already
// authenticated) itself still refuses a request with no claims attached.
func TestNewWorkUnitsGetHandlerRequiresAuthContext(t *testing.T) {
	handler := newWorkUnitsGetHandler(newTestWorkUnitsReader(t))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/work-units", nil)
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
	if got, want := rec.Body.String(), `{"detail":{"message":"Not authenticated"}}`+"\n"; got != want {
		t.Fatalf("body = %q, want %q", got, want)
	}
}

// TestNewWorkUnitsGetHandlerBadRangeDaysIs422 pins a non-numeric
// range_days against the shared, already-live-confirmed int_parsing
// shape (pydantic_validation_error.go).
func TestNewWorkUnitsGetHandlerBadRangeDaysIs422(t *testing.T) {
	handler := newWorkUnitsGetHandler(newTestWorkUnitsReader(t))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/work-units?range_days=abc", nil)
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want %d, body=%s", rec.Code, http.StatusUnprocessableEntity, rec.Body.String())
	}
	var body pydanticValidationErrorBody
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v (body=%s)", err, rec.Body.String())
	}
	want := pydanticValidationErrorBody{Detail: []pydanticErrorDetail{
		{
			Type: "int_parsing", Loc: []any{"query", "range_days"},
			Msg: "Input should be a valid integer, unable to parse string as an integer", Input: "abc",
		},
	}}
	if !reflect.DeepEqual(body, want) {
		t.Fatalf("body = %+v, want %+v", body, want)
	}
}

// TestNewWorkUnitsGetHandlerEmptyRangeDaysIs422 pins that an explicit,
// present-but-EMPTY range_days value is ALSO rejected -- confirmed live
// against a real FastAPI app (this route's own TEST-EVIDENCE): FastAPI
// does not treat "" as "absent" for an int query param the way a bare
// string field would. query.Has (not a `raw != ""` check) is what lets
// this route's own strconv.Atoi see the empty value and fail here,
// instead of silently keeping the 14-day default.
func TestNewWorkUnitsGetHandlerEmptyRangeDaysIs422(t *testing.T) {
	handler := newWorkUnitsGetHandler(newTestWorkUnitsReader(t))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/work-units?range_days=", nil)
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want %d, body=%s", rec.Code, http.StatusUnprocessableEntity, rec.Body.String())
	}
}

// TestNewWorkUnitsGetHandlerBadIncludeTextualIs422 pins that an
// unrecognized include_textual value is a bool_parsing 422, confirmed
// live (this route's own TEST-EVIDENCE) to accept the identical
// case-insensitive string set FastAPI's own Query(bool) parameter type
// does.
func TestNewWorkUnitsGetHandlerBadIncludeTextualIs422(t *testing.T) {
	handler := newWorkUnitsGetHandler(newTestWorkUnitsReader(t))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/work-units?include_textual=maybe", nil)
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want %d, body=%s", rec.Code, http.StatusUnprocessableEntity, rec.Body.String())
	}
	var body pydanticValidationErrorBody
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v (body=%s)", err, rec.Body.String())
	}
	if len(body.Detail) != 1 || body.Detail[0].Type != "bool_parsing" {
		t.Fatalf("detail = %+v, want one bool_parsing entry", body.Detail)
	}
}

// TestNewWorkUnitsGetHandlerHappyPathShape pins the response Content-Type,
// the deprecation header, and that an empty result renders `[]`, never
// `null`.
func TestNewWorkUnitsGetHandlerHappyPathShape(t *testing.T) {
	handler := newWorkUnitsGetHandler(newTestWorkUnitsReader(t))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/work-units", nil)
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", ct)
	}
	if got, want := rec.Header().Get("X-DevHealth-Deprecated"), "use POST with filters"; got != want {
		t.Fatalf("X-DevHealth-Deprecated = %q, want %q", got, want)
	}
	if got, want := rec.Body.String(), "[]\n"; got != want {
		t.Fatalf("body = %q, want %q (empty result must render as a JSON array, not null)", got, want)
	}
}

// TestNewWorkUnitsPostHandlerRequiresAuthContext mirrors the GET handler's
// own defensive auth check.
func TestNewWorkUnitsPostHandlerRequiresAuthContext(t *testing.T) {
	handler := newWorkUnitsPostHandler(newTestWorkUnitsReader(t))
	req := httptest.NewRequest(http.MethodPost, "/api/v1/work-units", nil)
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
}

// TestNewWorkUnitsPostHandlerMissingFiltersIs422 pins the shared
// "missing required top-level MetricFilter field" shape drilldown/prs'
// own POST handler already exercises identically (WorkUnitRequest and
// DrilldownRequest share the same "filters" required field).
func TestNewWorkUnitsPostHandlerMissingFiltersIs422(t *testing.T) {
	handler := newWorkUnitsPostHandler(newTestWorkUnitsReader(t))
	req := httptest.NewRequest(http.MethodPost, "/api/v1/work-units", strings.NewReader(`{}`))
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want %d, body=%s", rec.Code, http.StatusUnprocessableEntity, rec.Body.String())
	}
	var body pydanticValidationErrorBody
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v (body=%s)", err, rec.Body.String())
	}
	want := pydanticValidationErrorBody{Detail: []pydanticErrorDetail{
		{Type: "missing", Loc: []any{"body", "filters"}, Msg: "Field required", Input: map[string]any{}},
	}}
	if !reflect.DeepEqual(body, want) {
		t.Fatalf("body = %+v, want %+v", body, want)
	}
}

// TestNewWorkUnitsPostHandlerEmptyBodyIs422 pins the "no body at all"
// shape -- Starlette's Request.json() treats a genuinely empty body as
// None, so Pydantic reports the whole model missing at loc ["body"], not
// ["body","filters"], the same distinction drilldown/prs' own POST
// handler already draws.
func TestNewWorkUnitsPostHandlerEmptyBodyIs422(t *testing.T) {
	handler := newWorkUnitsPostHandler(newTestWorkUnitsReader(t))
	req := httptest.NewRequest(http.MethodPost, "/api/v1/work-units", nil)
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want %d, body=%s", rec.Code, http.StatusUnprocessableEntity, rec.Body.String())
	}
	var body pydanticValidationErrorBody
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v (body=%s)", err, rec.Body.String())
	}
	if len(body.Detail) != 1 || body.Detail[0].Type != "missing" || len(body.Detail[0].Loc) != 1 || body.Detail[0].Loc[0] != "body" {
		t.Fatalf("detail = %+v, want one missing entry at loc [\"body\"]", body.Detail)
	}
}

// TestNewWorkUnitsPostHandlerHappyPathShape pins the response shape for a
// well-formed body with an empty result.
func TestNewWorkUnitsPostHandlerHappyPathShape(t *testing.T) {
	handler := newWorkUnitsPostHandler(newTestWorkUnitsReader(t))
	req := httptest.NewRequest(http.MethodPost, "/api/v1/work-units", strings.NewReader(`{"filters":{}}`))
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", ct)
	}
	if got := rec.Header().Get("X-DevHealth-Deprecated"); got != "" {
		t.Fatalf("X-DevHealth-Deprecated = %q, want empty (POST never sets it)", got)
	}
	if got, want := rec.Body.String(), "[]\n"; got != want {
		t.Fatalf("body = %q, want %q", got, want)
	}
}

// TestBuildWorkUnitsRouteEntryHandlerRejectsUnsupportedMethod pins the
// entryHandler's own method guard for a method neither GET nor POST.
func TestBuildWorkUnitsRouteEntryHandlerRejectsUnsupportedMethod(t *testing.T) {
	t.Setenv("CLICKHOUSE_URI", "clickhouse://localhost:8123/default")
	t.Setenv("GO_API_ENVELOPE_JWKS_PATH", t.TempDir()+"/missing-jwks.json")
	t.Setenv("GO_API_ENVELOPE_ISSUER", "test-issuer")
	t.Setenv("GO_API_ENVELOPE_AUDIENCE", "test-audience")

	handler, cleanup, ok, err := buildWorkUnitsRoute()
	if err != nil {
		t.Fatalf("buildWorkUnitsRoute: %v", err)
	}
	if !ok {
		t.Fatal("buildWorkUnitsRoute: ok = false, want true with every dependency env var set")
	}
	defer cleanup()

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/work-units", nil)
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusMethodNotAllowed)
	}
}

// TestBoundedWorkUnitsLimit pins _bounded_limit_param's own branches
// (api/main.py:211-214) exactly.
func TestBoundedWorkUnitsLimit(t *testing.T) {
	cases := []struct {
		limit int
		want  int
	}{
		{0, 50},
		{-5, 50},
		{1, 1},
		{200, 200},
		{1000, 1000},
		{1001, 1000},
		{5000, 1000},
	}
	for _, tc := range cases {
		if got := boundedWorkUnitsLimit(tc.limit); got != tc.want {
			t.Errorf("boundedWorkUnitsLimit(%d) = %d, want %d", tc.limit, got, tc.want)
		}
	}
}

// TestFormatWorkUnitTimestampMatchesPythonGolden pins Pydantic v2's own
// JSON-mode datetime encoding for every fractional-second shape this
// route's own TEST-EVIDENCE captured live: zero, sub-microsecond-rounding
// boundary values, and a full 6-digit fraction, all suffixed "Z", never
// "+00:00" -- confirmed against `fastapi.encoders.jsonable_encoder` over
// a real WorkUnitTimeRange instance.
func TestFormatWorkUnitTimestampMatchesPythonGolden(t *testing.T) {
	utc := time.UTC
	cases := []struct {
		t    time.Time
		want string
	}{
		{time.Date(2024, 1, 15, 0, 0, 0, 0, utc), "2024-01-15T00:00:00Z"},
		{time.Date(2024, 1, 15, 0, 0, 0, 1000, utc), "2024-01-15T00:00:00.000001Z"},
		{time.Date(2024, 1, 15, 0, 0, 0, 100000000, utc), "2024-01-15T00:00:00.100000Z"},
		{time.Date(2024, 1, 15, 0, 0, 0, 1000000, utc), "2024-01-15T00:00:00.001000Z"},
		{time.Date(2024, 1, 15, 0, 0, 0, 999999000, utc), "2024-01-15T00:00:00.999999Z"},
	}
	for _, tc := range cases {
		if got := formatWorkUnitTimestamp(tc.t); got != tc.want {
			t.Errorf("formatWorkUnitTimestamp(%v) = %q, want %q", tc.t, got, tc.want)
		}
	}
}

// --- end-to-end golden: reader.BuildWorkUnitInvestments + this route's
// --- own wire encoder, against a captured live Python response body.

// assignScanValue assigns value into *dest via reflection -- dest is
// always a pointer (as passed to RowScanner.Scan), and value is either
// nil (SQL NULL: zeroes the pointed-to value, matching a Nullable column
// scanning to a nil *T) or already the exact Go type the destination
// expects.
func assignScanValue(dest any, value any) error {
	dv := reflect.ValueOf(dest)
	if dv.Kind() != reflect.Ptr {
		return fmt.Errorf("scan destination %T is not a pointer", dest)
	}
	elem := dv.Elem()
	if value == nil {
		elem.Set(reflect.Zero(elem.Type()))
		return nil
	}
	vv := reflect.ValueOf(value)
	if !vv.Type().AssignableTo(elem.Type()) {
		return fmt.Errorf("cannot assign %T into %s", value, elem.Type())
	}
	elem.Set(vv)
	return nil
}

// fixtureWorkUnitRowScanner replays a fixed set of rows, column values
// given as a positional []any per row matching FetchWorkUnitInvestments'
// own SELECT column order exactly (workunitreader.go).
type fixtureWorkUnitRowScanner struct {
	rows [][]any
	idx  int
}

func (s *fixtureWorkUnitRowScanner) Next() bool { return s.idx < len(s.rows) }
func (s *fixtureWorkUnitRowScanner) Scan(dest ...any) error {
	row := s.rows[s.idx]
	s.idx++
	if len(dest) != len(row) {
		return fmt.Errorf("scan arity mismatch: dest has %d, fixture row has %d", len(dest), len(row))
	}
	for i := range dest {
		if err := assignScanValue(dest[i], row[i]); err != nil {
			return fmt.Errorf("column %d: %w", i, err)
		}
	}
	return nil
}
func (s *fixtureWorkUnitRowScanner) Err() error   { return nil }
func (s *fixtureWorkUnitRowScanner) Close() error { return nil }

// fixtureWorkUnitsClient dispatches by the query's OWN table reference:
// a query against work_unit_investment_quotes answers with quoteRows,
// every other query with fixtureRows on its first call and zero rows
// after -- FetchWorkUnitInvestments and FetchWorkUnitInvestmentQuotes
// never share a call slot, so table-text dispatch is exact here, not a
// heuristic. Repo-scope/identity/team-assignment lookups still answer
// empty (this file's fixture rows carry no repo_id and no issues/prs in
// their structural payload, so BuildWorkUnitInvestments' own
// short-circuits never issue those queries in the first place).
type fixtureWorkUnitsClient struct {
	fixtureRows     [][]any
	quoteRows       [][]any
	investmentCalls int
}

func (c *fixtureWorkUnitsClient) Query(_ context.Context, statement string, _ []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	if strings.Contains(statement, "FROM work_unit_investment_quotes") {
		return &fixtureWorkUnitRowScanner{rows: c.quoteRows}, nil
	}
	c.investmentCalls++
	if c.investmentCalls == 1 {
		return &fixtureWorkUnitRowScanner{rows: c.fixtureRows}, nil
	}
	return emptyRowsWorkUnitsScanner{}, nil
}

func strPtr(s string) *string     { return &s }
func floatPtr(f float64) *float64 { return &f }

// TestBuildWorkUnitsResponseMatchesPythonGolden is this route's own
// golden parity test, table-driven over every Python-side control-flow
// branch build_work_unit_investments (work_units.py) takes on already-
// fetched rows: FetchWorkUnitInvestments' fixed column order feeds each
// case's own fixture row(s) through the REAL
// (*investmentexplain.Reader).BuildWorkUnitInvestments and this file's
// own toWorkUnitInvestmentWire/writeWorkUnitsResponse encoding, compared
// against a committed JSON fixture under testdata/ -- never a Go string
// literal, so the golden can be diffed and regenerated on its own.
//
// Every fixture row leaves work_unit_type/repo_id/provider all NULL and
// a structural payload with no issues/prs keys, so team/repo resolution
// stay at their "unassigned" defaults without any further ClickHouse
// call; that seam (repo/team resolution against real data) is covered
// separately by this package's own seeded integration tests
// (workunits_seeded_integration_test.go), not here.
//
// Each testdata/workunits_golden_*.json fixture is captured via an
// uncommitted one-off invocation of build_work_unit_investments,
// monkeypatching its five ClickHouse-reading dependencies to return the
// SAME logical row(s) given to the fake Go client below for that case,
// everything else left real:
//
//	.venv/bin/python3 <<'PY'
//	import asyncio, json
//	from datetime import datetime, timezone
//	from unittest.mock import AsyncMock, patch
//	from fastapi.encoders import jsonable_encoder
//	from dev_health_ops.api.services import work_units as wu_mod
//	from dev_health_ops.api.models.filters import MetricFilter
//	BASE_ROW = {
//	    "work_unit_id": "abc-123", "work_unit_type": None,
//	    "work_unit_name": "Some Name",
//	    "from_ts": datetime(2024,1,15,0,0,0,tzinfo=timezone.utc),
//	    "to_ts": datetime(2024,1,20,12,30,45,123000,tzinfo=timezone.utc),
//	    "repo_id": None, "provider": None, "effort_metric": "churn_loc",
//	    "effort_value": 12.5,
//	    "theme_distribution_json": {"feature_delivery": 0.8, "maintenance": 0.2},
//	    "subcategory_distribution_json": {},
//	    "structural_evidence_json": json.dumps({"note": "x"}),
//	    "evidence_quality": 0.75, "evidence_quality_band": "high",
//	    "categorization_status": "ok", "categorization_model_version": "v1",
//	    "categorization_run_id": "run-1",
//	    "computed_at": datetime(2024,1,20,12,30,45,123000,tzinfo=timezone.utc),
//	}
//	QUOTE_ROW = {
//	    "work_unit_id": "abc-123", "quote": "Shipped the thing.",
//	    "source_type": "pr_body", "source_id": "pr-1",
//	    "categorization_run_id": "run-1",
//	}
//	class FakeSink:
//	    async def __aenter__(self): return self
//	    async def __aexit__(self, *a): return False
//	async def capture(rows, quote_rows, include_text, limit):
//	    with patch.object(wu_mod, "clickhouse_client", return_value=FakeSink()), \
//	         patch.object(wu_mod, "require_clickhouse_backend", return_value=None), \
//	         patch.object(wu_mod, "resolve_repo_filter_ids", new=AsyncMock(return_value=[])), \
//	         patch.object(wu_mod, "fetch_work_unit_investments", new=AsyncMock(return_value=rows)), \
//	         patch.object(wu_mod, "fetch_work_unit_investment_quotes", new=AsyncMock(return_value=quote_rows)), \
//	         patch.object(wu_mod, "fetch_repo_scopes", new=AsyncMock(return_value={})), \
//	         patch.object(wu_mod, "fetch_repo_identities", new=AsyncMock(return_value={})), \
//	         patch.object(wu_mod, "fetch_work_item_team_assignments", new=AsyncMock(return_value={})), \
//	         patch.object(wu_mod, "warn_once_for_mock_fixture_rows", return_value=None):
//	        result = await wu_mod.build_work_unit_investments(
//	            db_url="clickhouse://fake", filters=MetricFilter(), org_id="org-1",
//	            limit=limit, include_text=include_text)
//	    return jsonable_encoder(result)
//	async def main():
//	    # default: include_text=True, one row, one quote present.
//	    default = await capture([BASE_ROW], [QUOTE_ROW], True, 200)
//	    json.dump(default, open("cmd/query-api/testdata/workunits_golden_default.json", "w"), indent=2)
//	    # include_text=False: the SAME row/quote data available, but the
//	    # builder's own `if include_text:` branch must skip the quotes
//	    # fetch entirely -- textual must be empty despite a matching quote.
//	    include_false = await capture([BASE_ROW], [QUOTE_ROW], False, 200)
//	    json.dump(include_false, open("cmd/query-api/testdata/workunits_golden_include_text_false.json", "w"), indent=2)
//	    # limit path: two rows with different effort_value, sorted DESC by
//	    # the builder's own `results.sort(...)` then truncated by
//	    # `results[:limit]` -- limit=1 must keep only the higher-effort row.
//	    low_effort_row = dict(BASE_ROW, work_unit_id="abc-000", effort_value=1.0, categorization_run_id=None)
//	    limit_path = await capture([BASE_ROW, low_effort_row], [], True, 1)
//	    json.dump(limit_path, open("cmd/query-api/testdata/workunits_golden_limit_path.json", "w"), indent=2)
//	asyncio.run(main())
//	PY
func TestBuildWorkUnitsResponseMatchesPythonGolden(t *testing.T) {
	fromTS := time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)
	toTS := time.Date(2024, 1, 20, 12, 30, 45, 123000000, time.UTC)

	// baseRow mirrors BASE_ROW above exactly -- categorization_run_id
	// "run-1" (not nil) so the include_text branch has a real quote to
	// either fetch or skip.
	baseRow := func(unitID string, effortValue float64, runID *string) []any {
		return []any{
			unitID, (*string)(nil), strPtr("Some Name"), fromTS, toTS,
			(*string)(nil), (*string)(nil), strPtr("churn_loc"), floatPtr(effortValue),
			[]string{"feature_delivery", "maintenance"}, []float64{0.8, 0.2},
			[]string{}, []float64{},
			strPtr(`{"note":"x"}`), floatPtr(0.75), strPtr("high"),
			strPtr("ok"), strPtr("v1"), runID, toTS,
		}
	}
	quoteRow := []any{"abc-123", "Shipped the thing.", "pr_body", "pr-1", "run-1"}

	cases := []struct {
		name        string
		golden      string
		fixtureRows [][]any
		quoteRows   [][]any
		includeText bool
		limit       int
	}{
		{
			name:        "default",
			golden:      "testdata/workunits_golden_default.json",
			fixtureRows: [][]any{baseRow("abc-123", 12.5, strPtr("run-1"))},
			quoteRows:   [][]any{quoteRow},
			includeText: true,
			limit:       200,
		},
		{
			name:        "include_text_false",
			golden:      "testdata/workunits_golden_include_text_false.json",
			fixtureRows: [][]any{baseRow("abc-123", 12.5, strPtr("run-1"))},
			quoteRows:   [][]any{quoteRow},
			includeText: false,
			limit:       200,
		},
		{
			name:   "limit_path",
			golden: "testdata/workunits_golden_limit_path.json",
			fixtureRows: [][]any{
				baseRow("abc-123", 12.5, nil),
				baseRow("abc-000", 1.0, nil),
			},
			quoteRows:   nil,
			includeText: true,
			limit:       1,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			client := &fixtureWorkUnitsClient{fixtureRows: tc.fixtureRows, quoteRows: tc.quoteRows}
			reader, err := investmentexplain.NewReader(client)
			if err != nil {
				t.Fatalf("NewReader: %v", err)
			}

			investments, err := reader.BuildWorkUnitInvestments(t.Context(), investmentexplain.BuildWorkUnitInvestmentsOptions{
				OrgID:       "org-1",
				StartTS:     fromTS,
				EndTS:       toTS,
				Limit:       tc.limit,
				IncludeText: tc.includeText,
			})
			if err != nil {
				t.Fatalf("BuildWorkUnitInvestments: %v", err)
			}

			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, "/api/v1/work-units", nil)
			writeWorkUnitsResponse(rec, req, "org-1", investments)

			wantBytes, err := os.ReadFile(tc.golden)
			if err != nil {
				t.Fatalf("read golden %s: %v", tc.golden, err)
			}

			var got, want any
			if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
				t.Fatalf("decode got: %v (body=%s)", err, rec.Body.String())
			}
			if err := json.Unmarshal(wantBytes, &want); err != nil {
				t.Fatalf("decode want: %v", err)
			}
			// jsonAlmostEqual, not reflect.DeepEqual: span_days is computed
			// via toTS.Sub(fromTS).Hours()/24 (workunitassembly.go) on this
			// side and via (to_ts - from_ts).total_seconds()/86400.0 on
			// Python's -- mathematically the same quantity, but a different
			// floating-point operation ORDER, which can (and here does:
			// 5.521355590277778 vs 5.5213555902777784) disagree in the last
			// representable bit of a float64. This is ordinary float64
			// non-associativity, not a content difference -- the same class
			// of tolerance every genuine merged ClickHouse float aggregate
			// already gets elsewhere in this service's own corpus (Tier B),
			// applied here to a pure Go arithmetic path instead of a
			// ClickHouse one.
			if !jsonAlmostEqual(got, want) {
				gotPretty, _ := json.MarshalIndent(got, "", "  ")
				wantPretty, _ := json.MarshalIndent(want, "", "  ")
				t.Fatalf("wire encoding mismatch:\n--- got ---\n%s\n--- want (%s) ---\n%s", gotPretty, tc.golden, wantPretty)
			}
		})
	}
}

// jsonAlmostEqual recursively compares two values decoded from JSON via
// encoding/json's default any-typed decode (map[string]any/[]any/
// float64/string/bool/nil), treating float64 leaves as equal within a
// small relative+absolute epsilon rather than requiring bit-identical
// values -- see this test's own call site for why an exact
// reflect.DeepEqual is too strict for a span_days-shaped float.
func jsonAlmostEqual(a, b any) bool {
	switch av := a.(type) {
	case float64:
		bv, ok := b.(float64)
		if !ok {
			return false
		}
		diff := av - bv
		if diff < 0 {
			diff = -diff
		}
		const epsilon = 1e-9
		return diff <= epsilon*(1+absFloat(av))
	case map[string]any:
		bv, ok := b.(map[string]any)
		if !ok || len(av) != len(bv) {
			return false
		}
		for k, v := range av {
			bvv, present := bv[k]
			if !present || !jsonAlmostEqual(v, bvv) {
				return false
			}
		}
		return true
	case []any:
		bv, ok := b.([]any)
		if !ok || len(av) != len(bv) {
			return false
		}
		for i := range av {
			if !jsonAlmostEqual(av[i], bv[i]) {
				return false
			}
		}
		return true
	default:
		return reflect.DeepEqual(a, b)
	}
}

func absFloat(f float64) float64 {
	if f < 0 {
		return -f
	}
	return f
}

// capturingWorkUnitsClient is a fake analytics.QueryClient that records
// every query/bindings it is asked to run and answers each with zero
// rows -- used to prove a query PARAMETER (like a scope-resolution id)
// actually reaches the SQL this route composes, without a live
// ClickHouse.
type capturingWorkUnitsClient struct {
	queries  []string
	bindings [][]dhclickhouse.Binding
}

func (c *capturingWorkUnitsClient) Query(_ context.Context, statement string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	c.queries = append(c.queries, statement)
	c.bindings = append(c.bindings, bindings)
	return emptyRowsWorkUnitsScanner{}, nil
}

func bindingValue(bindings []dhclickhouse.Binding, name string) (any, bool) {
	for _, b := range bindings {
		if b.Name == name {
			return b.Value, true
		}
	}
	return nil, false
}

// TestNewWorkUnitsGetHandlerTeamScopeResolvesViaUserMetricsDaily proves
// scope_type=team&scope_id=<id> reaches ResolveRepoFilterIDs' own team
// branch (resolveRepoIDsForTeams, repofilter.go), which reads
// user_metrics_daily keyed by team_id -- the same corpus request this
// route's "team_scoped" GET entry (restcorpus.go) exercises against a
// live baseline.
func TestNewWorkUnitsGetHandlerTeamScopeResolvesViaUserMetricsDaily(t *testing.T) {
	client := &capturingWorkUnitsClient{}
	reader, err := investmentexplain.NewReader(client)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	handler := newWorkUnitsGetHandler(reader)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/work-units?scope_type=team&scope_id=team-42", nil)
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}

	found := false
	for i, q := range client.queries {
		if strings.Contains(q, "FROM user_metrics_daily") {
			found = true
			teamIDs, ok := bindingValue(client.bindings[i], "team_ids")
			if !ok {
				t.Fatalf("user_metrics_daily query carries no team_ids binding")
			}
			ids, ok := teamIDs.([]string)
			if !ok || len(ids) != 1 || ids[0] != "team-42" {
				t.Errorf("team_ids binding = %#v, want [\"team-42\"]", teamIDs)
			}
		}
	}
	if !found {
		t.Fatal("no query against user_metrics_daily -- scope_type=team never reached resolveRepoIDsForTeams")
	}
}

// TestDecodeWorkUnitsRequestBodyReadsLimitAndIncludeTextual pins the POST
// body decode's own field wiring: an explicit, non-zero limit and an
// explicit include_textual=false both come through unchanged, matching
// this route's own "explicit_limit"/"include_textual_false" corpus
// requests (restcorpus.go).
func TestDecodeWorkUnitsRequestBodyReadsLimitAndIncludeTextual(t *testing.T) {
	filters, limit, includeTextual, validationErrors, bodyErr := decodeWorkUnitsRequestBody([]byte(`{"filters":{},"limit":5,"include_textual":false}`))
	if bodyErr != nil {
		t.Fatalf("bodyErr = %v, want nil", bodyErr)
	}
	if len(validationErrors) != 0 {
		t.Fatalf("validationErrors = %+v, want none", validationErrors)
	}
	if filters == nil {
		t.Fatal("filters = nil, want an empty (present) map")
	}
	if limit != 5 {
		t.Errorf("limit = %d, want 5", limit)
	}
	if includeTextual {
		t.Error("includeTextual = true, want false")
	}
}

// TestDecodeWorkUnitsRequestBodyLimitZeroFallsBackToCallerDefault pins
// `payload.limit or 200`'s (api/main.py:620) own "falsy limit" half: an
// absent, null or explicit-zero limit all decode as limitSet=false (rawLimit
// 0 here), leaving the 200 fallback to newWorkUnitsPostHandler's own
// `if rawLimit == 0 { rawLimit = 200 }` -- decodeWorkUnitsRequestBody
// itself never applies that default, matching this file's own
// decodeWorkUnitsRequestBody doc comment.
func TestDecodeWorkUnitsRequestBodyLimitZeroFallsBackToCallerDefault(t *testing.T) {
	for _, body := range []string{
		`{"filters":{}}`,
		`{"filters":{},"limit":null}`,
		`{"filters":{},"limit":0}`,
	} {
		_, limit, _, validationErrors, bodyErr := decodeWorkUnitsRequestBody([]byte(body))
		if bodyErr != nil || len(validationErrors) != 0 {
			t.Fatalf("body=%s: bodyErr=%v validationErrors=%+v, want none", body, bodyErr, validationErrors)
		}
		if limit != 0 {
			t.Errorf("body=%s: limit = %d, want 0 (caller applies the 200 fallback)", body, limit)
		}
	}
}

// TestNewWorkUnitsPostHandlerWorkCategoryFilterNarrowsResults proves
// filters.why.work_category actually excludes a non-matching row --
// end-to-end through the real BuildWorkUnitInvestments and this route's
// own wire encoder, using the same fixtureWorkUnitsClient/
// fixtureWorkUnitRowScanner infrastructure as
// TestBuildWorkUnitsResponseMatchesPythonGolden.
func TestNewWorkUnitsPostHandlerWorkCategoryFilterNarrowsResults(t *testing.T) {
	fromTS := time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)
	toTS := time.Date(2024, 1, 20, 0, 0, 0, 0, time.UTC)
	matchingRow := []any{
		"wu-match", (*string)(nil), strPtr("Matches the filter"), fromTS, toTS, (*string)(nil), (*string)(nil),
		strPtr("churn_loc"), floatPtr(5.0),
		[]string{"feature_delivery"}, []float64{1.0}, []string{}, []float64{},
		strPtr(`{}`), floatPtr(0.5), strPtr("moderate"), strPtr("ok"), strPtr("v1"), (*string)(nil), toTS,
	}
	nonMatchingRow := []any{
		"wu-nomatch", (*string)(nil), strPtr("Does not match"), fromTS, toTS, (*string)(nil), (*string)(nil),
		strPtr("churn_loc"), floatPtr(3.0),
		[]string{"maintenance"}, []float64{1.0}, []string{}, []float64{},
		strPtr(`{}`), floatPtr(0.5), strPtr("moderate"), strPtr("ok"), strPtr("v1"), (*string)(nil), toTS,
	}
	client := &fixtureWorkUnitsClient{fixtureRows: [][]any{matchingRow, nonMatchingRow}}
	reader, err := investmentexplain.NewReader(client)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	handler := newWorkUnitsPostHandler(reader)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/work-units", strings.NewReader(`{"filters":{"why":{"work_category":["feature_delivery"]}}}`))
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}

	var decoded []map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &decoded); err != nil {
		t.Fatalf("decode body: %v (body=%s)", err, rec.Body.String())
	}
	if len(decoded) != 1 {
		t.Fatalf("len(results) = %d, want exactly 1 (work_category filter must exclude the non-matching row) -- got %+v", len(decoded), decoded)
	}
	if got := decoded[0]["work_unit_id"]; got != "wu-match" {
		t.Errorf("work_unit_id = %v, want \"wu-match\"", got)
	}
}
