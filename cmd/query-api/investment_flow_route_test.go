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

func TestInvestmentFlowSwitchFromEnvDefaultsDisabled(t *testing.T) {
	sw := investmentFlowSwitchFromEnv()
	if sw.Enabled(investmentFlowOperation) || sw.Enabled(investmentFlowRepoTeamOperation) {
		t.Fatal("expected both investment/flow operations disabled with no env var set")
	}
}

func TestInvestmentFlowSwitchFromEnvEnabledViaEnvVar(t *testing.T) {
	t.Setenv(investmentFlowEnabledEnvVar, "true")
	sw := investmentFlowSwitchFromEnv()
	if !sw.Enabled(investmentFlowOperation) || !sw.Enabled(investmentFlowRepoTeamOperation) {
		t.Fatal("expected both investment/flow operations enabled with GO_API_INVESTMENT_FLOW_ENABLED=true")
	}
}

func TestInvestmentFlowRouteUnreachableWhenSwitchDisabled(t *testing.T) {
	sw := routeswitch.NewDynamicSwitch()
	mux := routeswitch.NewMux(sw)
	reached := false
	mux.Register(investmentFlowOperation, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reached = true
		w.WriteHeader(http.StatusOK)
	}))
	req := httptest.NewRequest(http.MethodPost, "/api/v1/investment/flow", bytes.NewReader([]byte(`{}`)))
	rec := httptest.NewRecorder()
	mux.Dispatch(investmentFlowOperation, rec, req)
	if reached {
		t.Fatal("registered handler ran despite the switch being disabled")
	}
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}
}

// emptyRowsInvestmentFlowClient answers every system.tables/system.columns
// probe as present, and every other query with zero rows -- enough for a
// simple 200-path plumbing test, same convention as sankey_route_test.go's
// own emptyRowsSankeyClient.
type emptyRowsInvestmentFlowClient struct{}

func (emptyRowsInvestmentFlowClient) Query(_ context.Context, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	for _, b := range bindings {
		if b.Name == "tables" || b.Name == "columns" {
			names, _ := b.Value.([]string)
			rows := make([][]any, 0, len(names))
			for _, n := range names {
				rows = append(rows, []any{n})
			}
			return &investmentFlowFixtureScanner{rows: rows}, nil
		}
	}
	return &investmentFlowFixtureScanner{}, nil
}

type investmentFlowFixtureScanner struct {
	rows  [][]any
	index int
}

func (s *investmentFlowFixtureScanner) Next() bool {
	if s.index >= len(s.rows) {
		return false
	}
	s.index++
	return true
}

func (s *investmentFlowFixtureScanner) Scan(dest ...any) error {
	row := s.rows[s.index-1]
	for i, d := range dest {
		switch typed := d.(type) {
		case *string:
			v, _ := row[i].(string)
			*typed = v
		case *float64:
			v, _ := row[i].(float64)
			*typed = v
		case *int64:
			v, _ := row[i].(int64)
			*typed = v
		}
	}
	return nil
}
func (s *investmentFlowFixtureScanner) Err() error   { return nil }
func (s *investmentFlowFixtureScanner) Close() error { return nil }

func TestNewInvestmentFlowHandlerRequiresAuthContext(t *testing.T) {
	handler := newInvestmentFlowHandler(emptyRowsInvestmentFlowClient{})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/investment/flow", bytes.NewReader([]byte(`{}`)))
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
}

func TestNewInvestmentFlowRepoTeamHandlerRequiresAuthContext(t *testing.T) {
	handler := newInvestmentFlowRepoTeamHandler(emptyRowsInvestmentFlowClient{})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/investment/flow/repo-team", bytes.NewReader([]byte(`{}`)))
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
}

func withOrg1(req *http.Request) *http.Request {
	return req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
}

// TestNewInvestmentFlowHandlerEmptyBodyIsMissing pins the whole-body-
// missing 422 for a genuinely absent body -- confirmed live against the
// real InvestmentFlowRequest model (this file's own TEST-EVIDENCE) even
// though every one of that model's fields has a default.
func TestNewInvestmentFlowHandlerEmptyBodyIsMissing(t *testing.T) {
	handler := newInvestmentFlowHandler(emptyRowsInvestmentFlowClient{})
	req := withOrg1(httptest.NewRequest(http.MethodPost, "/api/v1/investment/flow", bytes.NewReader(nil)))
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want %d, body=%s", rec.Code, http.StatusUnprocessableEntity, rec.Body.String())
	}
	var body pydanticValidationErrorBody
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if len(body.Detail) != 1 || len(body.Detail[0].Loc) != 1 || body.Detail[0].Loc[0] != "body" {
		t.Fatalf("Detail = %+v, want one whole-body-missing error", body.Detail)
	}
}

// TestNewInvestmentFlowHandlerEmptyObjectIsOK pins that {} is a valid,
// all-defaults body -- every InvestmentFlowRequest field has its own
// default.
func TestNewInvestmentFlowHandlerEmptyObjectIsOK(t *testing.T) {
	handler := newInvestmentFlowHandler(emptyRowsInvestmentFlowClient{})
	req := withOrg1(httptest.NewRequest(http.MethodPost, "/api/v1/investment/flow", bytes.NewReader([]byte(`{}`))))
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"mode":"investment"`) {
		t.Fatalf("body = %s, want mode=investment", rec.Body.String())
	}
}

// TestNewInvestmentFlowHandlerFiltersNullIs422 pins that an explicit
// `"filters": null` is model_attributes_type, not a silent fall-back to
// the field's own default factory.
func TestNewInvestmentFlowHandlerFiltersNullIs422(t *testing.T) {
	handler := newInvestmentFlowHandler(emptyRowsInvestmentFlowClient{})
	req := withOrg1(httptest.NewRequest(http.MethodPost, "/api/v1/investment/flow", bytes.NewReader([]byte(`{"filters":null}`))))
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want %d, body=%s", rec.Code, http.StatusUnprocessableEntity, rec.Body.String())
	}
	var body pydanticValidationErrorBody
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if len(body.Detail) != 1 || body.Detail[0].Type != "model_attributes_type" {
		t.Fatalf("Detail = %+v, want one model_attributes_type error", body.Detail)
	}
}

// TestNewInvestmentFlowHandlerTopNReposNullIs422 pins that an explicit
// `"top_n_repos": null` is int_type, not a silent fall-back to 12.
func TestNewInvestmentFlowHandlerTopNReposNullIs422(t *testing.T) {
	handler := newInvestmentFlowHandler(emptyRowsInvestmentFlowClient{})
	req := withOrg1(httptest.NewRequest(http.MethodPost, "/api/v1/investment/flow", bytes.NewReader([]byte(`{"top_n_repos":null}`))))
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want %d, body=%s", rec.Code, http.StatusUnprocessableEntity, rec.Body.String())
	}
	var body pydanticValidationErrorBody
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if len(body.Detail) != 1 || body.Detail[0].Type != "int_type" {
		t.Fatalf("Detail = %+v, want one int_type error", body.Detail)
	}
}

// TestNewInvestmentFlowHandlerInvalidFlowModeLiteral pins the 422 for a
// flow_mode value outside the three-way Literal.
func TestNewInvestmentFlowHandlerInvalidFlowModeLiteral(t *testing.T) {
	handler := newInvestmentFlowHandler(emptyRowsInvestmentFlowClient{})
	req := withOrg1(httptest.NewRequest(http.MethodPost, "/api/v1/investment/flow", bytes.NewReader([]byte(`{"flow_mode":"nope"}`))))
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

// TestNewInvestmentFlowHandlerMissingDrillCategoryIs400 pins the ONE
// 400 this route ever answers -- build_investment_flow_response's own
// ValueError, mapped verbatim (str(exc) as the detail), never a 503.
func TestNewInvestmentFlowHandlerMissingDrillCategoryIs400(t *testing.T) {
	handler := newInvestmentFlowHandler(emptyRowsInvestmentFlowClient{})
	req := withOrg1(httptest.NewRequest(http.MethodPost, "/api/v1/investment/flow", bytes.NewReader([]byte(`{"flow_mode":"team_subcategory_repo"}`))))
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d, body=%s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
	var body restErrorBody
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if body.Detail != "drill_category is required for team_subcategory_repo" {
		t.Fatalf("Detail = %v, want the exact ValueError message", body.Detail)
	}
}

// TestNewInvestmentFlowHandlerTeamCategoryRepoOK pins the 200 path for a
// flow_mode that does not require drill_category.
func TestNewInvestmentFlowHandlerTeamCategoryRepoOK(t *testing.T) {
	handler := newInvestmentFlowHandler(emptyRowsInvestmentFlowClient{})
	req := withOrg1(httptest.NewRequest(http.MethodPost, "/api/v1/investment/flow", bytes.NewReader([]byte(`{"flow_mode":"team_category_repo"}`))))
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"flow_mode":"team_category_repo"`) {
		t.Fatalf("body = %s, want flow_mode=team_category_repo", rec.Body.String())
	}
}

func TestNewInvestmentFlowRepoTeamHandlerSuccess(t *testing.T) {
	handler := newInvestmentFlowRepoTeamHandler(emptyRowsInvestmentFlowClient{})
	req := withOrg1(httptest.NewRequest(http.MethodPost, "/api/v1/investment/flow/repo-team", bytes.NewReader([]byte(`{}`))))
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"chosen_mode":"repo_team"`) {
		t.Fatalf("body = %s, want chosen_mode=repo_team", rec.Body.String())
	}
}

// TestNewInvestmentFlowRepoTeamHandlerNoValueErrorBranch pins that
// investment_flow_repo_team's own body has no 400 branch at all -- even
// the SAME missing-drill_category input that answers 400 on the
// investment/flow sibling above is accepted here (drill_category is
// simply not read by this route's business logic) rather than erroring.
func TestNewInvestmentFlowRepoTeamHandlerNoValueErrorBranch(t *testing.T) {
	handler := newInvestmentFlowRepoTeamHandler(emptyRowsInvestmentFlowClient{})
	req := withOrg1(httptest.NewRequest(http.MethodPost, "/api/v1/investment/flow/repo-team", bytes.NewReader([]byte(`{"flow_mode":"team_subcategory_repo"}`))))
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
}

// nilRowsErrorClient answers system.tables/system.columns as present (so
// the schema-drift guard passes) and every other query with an error --
// exercising the 503 "Data unavailable" fallback both handlers share.
type nilRowsErrorClient struct{}

func (nilRowsErrorClient) Query(_ context.Context, _ string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	for _, b := range bindings {
		if b.Name == "tables" || b.Name == "columns" {
			names, _ := b.Value.([]string)
			rows := make([][]any, 0, len(names))
			for _, n := range names {
				rows = append(rows, []any{n})
			}
			return &investmentFlowFixtureScanner{rows: rows}, nil
		}
	}
	return nil, context.DeadlineExceeded
}

func TestNewInvestmentFlowHandlerDataUnavailableIs503(t *testing.T) {
	handler := newInvestmentFlowHandler(nilRowsErrorClient{})
	req := withOrg1(httptest.NewRequest(http.MethodPost, "/api/v1/investment/flow", bytes.NewReader([]byte(`{}`))))
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d, body=%s", rec.Code, http.StatusServiceUnavailable, rec.Body.String())
	}
	var body restErrorBody
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if body.Detail != "Data unavailable" {
		t.Fatalf("Detail = %v, want \"Data unavailable\"", body.Detail)
	}
}

func TestNewInvestmentFlowRepoTeamHandlerDataUnavailableIs503(t *testing.T) {
	handler := newInvestmentFlowRepoTeamHandler(nilRowsErrorClient{})
	req := withOrg1(httptest.NewRequest(http.MethodPost, "/api/v1/investment/flow/repo-team", bytes.NewReader([]byte(`{}`))))
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d, body=%s", rec.Code, http.StatusServiceUnavailable, rec.Body.String())
	}
}
