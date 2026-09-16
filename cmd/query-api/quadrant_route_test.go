package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"testing"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/authctx"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/routeswitch"
)

// TestQuadrantSwitchFromEnvDefaultsDisabled mirrors
// TestInvestmentExplainSwitchFromEnvDefaultsDisabled: with no env var set,
// quadrantOperation is NOT enabled.
func TestQuadrantSwitchFromEnvDefaultsDisabled(t *testing.T) {
	sw := quadrantSwitchFromEnv()
	if sw.Enabled(quadrantOperation) {
		t.Fatal("expected quadrantOperation to be disabled with no env var set")
	}
}

// TestQuadrantSwitchFromEnvEnabledViaEnvVar is the other half.
func TestQuadrantSwitchFromEnvEnabledViaEnvVar(t *testing.T) {
	t.Setenv(quadrantEnabledEnvVar, "true")
	sw := quadrantSwitchFromEnv()
	if !sw.Enabled(quadrantOperation) {
		t.Fatal("expected quadrantOperation to be enabled with GO_API_QUADRANT_ENABLED=true")
	}
}

// TestQuadrantRouteUnreachableWhenSwitchDisabled mirrors
// TestInvestmentExplainRouteUnreachableWhenSwitchDisabled: a registered
// handler dispatched through a Mux whose Switch reports the operation
// disabled must never run and must answer 404.
func TestQuadrantRouteUnreachableWhenSwitchDisabled(t *testing.T) {
	sw := routeswitch.NewDynamicSwitch()
	mux := routeswitch.NewMux(sw)

	reached := false
	mux.Register(quadrantOperation, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reached = true
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/quadrant?type=wip_throughput", nil)
	rec := httptest.NewRecorder()
	mux.Dispatch(quadrantOperation, rec, req)

	if reached {
		t.Fatal("registered handler ran despite the switch being disabled")
	}
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}
}

// TestQuadrantRouteReachableWhenSwitchEnabled is the other half.
func TestQuadrantRouteReachableWhenSwitchEnabled(t *testing.T) {
	sw := routeswitch.NewDynamicSwitch()
	sw.Set(quadrantOperation, true)
	mux := routeswitch.NewMux(sw)

	reached := false
	mux.Register(quadrantOperation, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reached = true
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/quadrant?type=wip_throughput", nil)
	rec := httptest.NewRecorder()
	mux.Dispatch(quadrantOperation, rec, req)

	if !reached {
		t.Fatal("registered handler did not run despite the switch being enabled")
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
}

// emptyRowsQuadrantClient answers every ClickHouse call with zero rows --
// used where the test only cares about HTTP-layer plumbing (missing auth,
// missing/invalid params), never the resulting data.
type emptyRowsQuadrantClient struct{}

func (emptyRowsQuadrantClient) Query(context.Context, string, []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	return emptyRowsQuadrantScanner{}, nil
}

type emptyRowsQuadrantScanner struct{}

func (emptyRowsQuadrantScanner) Next() bool        { return false }
func (emptyRowsQuadrantScanner) Scan(...any) error { return nil }
func (emptyRowsQuadrantScanner) Err() error        { return nil }
func (emptyRowsQuadrantScanner) Close() error      { return nil }

// TestNewQuadrantWorkHandlerRequiresAuthContext pins that the work handler
// (reached only after buildQuadrantRoute's entryHandler already
// authenticated) itself still refuses a request with no claims attached --
// the same defensive check newInvestmentExplainWorkHandler makes.
func TestNewQuadrantWorkHandlerRequiresAuthContext(t *testing.T) {
	handler := newQuadrantWorkHandler(emptyRowsQuadrantClient{})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/quadrant?type=wip_throughput", nil)
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
}

// TestNewQuadrantWorkHandlerRequiresType pins the missing-`type` 422,
// matching FastAPI's own RequestValidationError body for a required
// query param with no value on the wire (live-captured, see this PR's
// TEST-EVIDENCE): {"detail":[{"type":"missing","loc":["query","type"],
// "msg":"Field required","input":null}]}.
func TestNewQuadrantWorkHandlerRequiresType(t *testing.T) {
	handler := newQuadrantWorkHandler(emptyRowsQuadrantClient{})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/quadrant", nil)
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusUnprocessableEntity)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", ct)
	}
	var body pydanticValidationErrorBody
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v (body=%s)", err, rec.Body.String())
	}
	want := pydanticValidationErrorBody{Detail: []pydanticErrorDetail{
		{Type: "missing", Loc: []any{"query", "type"}, Msg: "Field required", Input: nil},
	}}
	if !reflect.DeepEqual(body, want) {
		t.Fatalf("body = %+v, want %+v", body, want)
	}
}

// TestNewQuadrantWorkHandlerAggregatesMultipleValidationErrors pins that
// multiple simultaneously-invalid fields are reported together in ONE
// 422, in the same order Python's endpoint signature declares them --
// matching a live FastAPI capture of the same request shape (quoted in
// TEST-EVIDENCE), never a fail-fast single-error response.
func TestNewQuadrantWorkHandlerAggregatesMultipleValidationErrors(t *testing.T) {
	handler := newQuadrantWorkHandler(emptyRowsQuadrantClient{})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/quadrant?range_days=abc&start_date=bad", nil)
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusUnprocessableEntity)
	}
	var body pydanticValidationErrorBody
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v (body=%s)", err, rec.Body.String())
	}
	if len(body.Detail) != 3 {
		t.Fatalf("detail has %d entries, want 3: %+v", len(body.Detail), body.Detail)
	}
	wantLocs := [][]any{{"query", "type"}, {"query", "range_days"}, {"query", "start_date"}}
	for i, want := range wantLocs {
		if !reflect.DeepEqual(body.Detail[i].Loc, want) {
			t.Fatalf("detail[%d].Loc = %v, want %v", i, body.Detail[i].Loc, want)
		}
	}
}

// TestNewQuadrantWorkHandlerBadRangeDaysIs422 pins a non-numeric
// range_days against a live FastAPI capture (TEST-EVIDENCE):
// {"detail":[{"type":"int_parsing","loc":["query","range_days"],
// "msg":"Input should be a valid integer, unable to parse string as an
// integer","input":"abc"}]}. A non-numeric range_days must be rejected,
// never silently coerced to the default range.
func TestNewQuadrantWorkHandlerBadRangeDaysIs422(t *testing.T) {
	handler := newQuadrantWorkHandler(emptyRowsQuadrantClient{})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/quadrant?type=wip_throughput&range_days=abc", nil)
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusUnprocessableEntity)
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

// TestNewQuadrantWorkHandlerUnknownTypeIs404 pins the resolver's 404
// surfacing through the HTTP layer via quadrant.AsRequestError.
func TestNewQuadrantWorkHandlerUnknownTypeIs404(t *testing.T) {
	handler := newQuadrantWorkHandler(emptyRowsQuadrantClient{})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/quadrant?type=not_a_real_type", nil)
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}
}

// TestNewQuadrantWorkHandlerHappyPathShape pins the response Content-Type
// and that the body decodes as a well-formed QuadrantResponse shape (axes/
// points/annotations present) for a supported, empty-data request.
func TestNewQuadrantWorkHandlerHappyPathShape(t *testing.T) {
	handler := newQuadrantWorkHandler(emptyRowsQuadrantClient{})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/quadrant?type=wip_throughput&scope_type=repo&bucket=week", nil)
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", ct)
	}
	var decoded struct {
		Axes        map[string]any   `json:"axes"`
		Points      []map[string]any `json:"points"`
		Annotations []map[string]any `json:"annotations"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &decoded); err != nil {
		t.Fatalf("decode body: %v (body=%s)", err, rec.Body.String())
	}
	if decoded.Axes == nil {
		t.Fatal("axes missing")
	}
	if decoded.Points == nil {
		t.Fatal("points must be [] (present), not omitted/null")
	}
	if decoded.Annotations == nil {
		t.Fatal("annotations must be [] (present), not omitted/null")
	}
}

// TestNewQuadrantWorkHandlerPersonScopeRequiresScopeID pins the
// person/developer scope's own required-param check (quadrant.py:500-503)
// surfacing through the HTTP layer as 400 when scope_id is absent.
func TestNewQuadrantWorkHandlerPersonScopeRequiresScopeID(t *testing.T) {
	handler := newQuadrantWorkHandler(emptyRowsQuadrantClient{})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/quadrant?type=wip_throughput&scope_type=person", nil)
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

// TestNewQuadrantWorkHandlerPersonScopeIndividualNotFound pins the person
// scope's 404 (quadrant.py:536-537) surfacing through the HTTP layer for a
// scope_id that resolves to no identity -- emptyRowsQuadrantClient answers
// every ClickHouse call with zero rows, so resolve_person_identity's
// lookup finds nothing.
func TestNewQuadrantWorkHandlerPersonScopeIndividualNotFound(t *testing.T) {
	t.Setenv("IDENTITY_MAPPING_PATH", filepath.Join(t.TempDir(), "missing.yaml"))
	handler := newQuadrantWorkHandler(emptyRowsQuadrantClient{})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/quadrant?type=wip_throughput&scope_type=person&scope_id=deadbeef", nil)
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}
}
