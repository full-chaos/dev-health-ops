package server

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/llmorgsettings"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/investmentexplain"
)

// testPlatformGate admits the platform model, for tests of everything but the gate.
type testPlatformGate struct{}

func (testPlatformGate) Decide(context.Context, string) llmorgsettings.Decision {
	return llmorgsettings.Decision{Kind: llmorgsettings.Platform}
}

type gateSource struct {
	in  llmorgsettings.Inputs
	err error
}

func (s gateSource) Inputs(context.Context, string) (llmorgsettings.Inputs, error) {
	return s.in, s.err
}

// countingResolver counts every provider-resolution call; a refused request must make none.
type countingResolver struct{ calls atomic.Int64 }

func (c *countingResolver) ResolveUsableProvider(context.Context, string) (string, error) {
	c.calls.Add(1)
	return "", nil
}

func (c *countingResolver) Credentials(context.Context, string, string) (llmorgsettings.Credentials, bool, error) {
	c.calls.Add(1)
	return llmorgsettings.Credentials{}, false, nil
}

func (c *countingResolver) Matches(context.Context, string, string) (bool, error) {
	c.calls.Add(1)
	return false, nil
}

func (c *countingResolver) Model(context.Context, string, string) (string, error) {
	c.calls.Add(1)
	return "", nil
}

func (c *countingResolver) RawBaseURL(context.Context, string, string) (string, bool, error) {
	c.calls.Add(1)
	return "", false, nil
}

// The D6107 rows (CHAOS-10269 B1-B5 plus B1-off, B1-unknown, B1').
var explainGateRows = []struct {
	name    string
	source  gateSource
	admit   bool
	status  int
	refusal string
}{
	{"B1 saved config, acr_service_model on: own model", gateSource{in: llmorgsettings.Inputs{Saved: true, ConfigUsable: true, ServiceModelOn: true}}, true, 0, ""},
	{"B1-off saved config, acr_service_model off", gateSource{in: llmorgsettings.Inputs{Saved: true, ConfigUsable: true}}, false, http.StatusForbidden, "ai_features_turned_off"},
	{"B1-unknown entitlement read fails", gateSource{err: errors.New("entitlement read failed")}, false, http.StatusServiceUnavailable, "Service unavailable"},
	{"B1' saved but unusable", gateSource{in: llmorgsettings.Inputs{Saved: true, ServiceModelOn: true}}, false, http.StatusServiceUnavailable, "Service unavailable"},
	{"flag off, saved config, gate closed", gateSource{in: llmorgsettings.Inputs{Saved: true, GateClosed: true, Fallback: true}}, false, http.StatusForbidden, "ai_features_turned_off"},
	{"flag off, saved and unusable", gateSource{in: llmorgsettings.Inputs{Saved: true}}, false, http.StatusForbidden, "ai_features_turned_off"},
	{"B2 saved behind a closed gate, with the grant", gateSource{in: llmorgsettings.Inputs{Saved: true, GateClosed: true, ServiceModelOn: true, Fallback: true}}, false, http.StatusForbidden, "own_model_gate_closed"},
	{"B3 no config, acr_service_model and platform_model_fallback: platform model", gateSource{in: llmorgsettings.Inputs{ServiceModelOn: true, Fallback: true}}, true, 0, ""},
	{"B4 no config, acr_service_model only", gateSource{in: llmorgsettings.Inputs{ServiceModelOn: true}}, false, http.StatusForbidden, "service_model_not_entitled"},
	{"B5 nothing", gateSource{}, false, http.StatusForbidden, llmorgsettings.CodeNothing},
}

func TestInvestmentExplainRunsTheModelGateBeforeAnyProvider(t *testing.T) {
	for _, row := range explainGateRows {
		t.Run(row.name, func(t *testing.T) {
			resolver := &countingResolver{}
			reader, err := investmentexplain.NewReader(emptyRowsQueryClient{})
			if row.status != 0 {
				reader, err = investmentexplain.NewReader(unusedQueryClient{})
			}
			if err != nil {
				t.Fatal(err)
			}
			handler := newInvestmentExplainWorkHandler(reader, nil, resolver, nil, llmorgsettings.Gate{Source: row.source})
			req := httptest.NewRequest(http.MethodPost, "/api/v1/investment/explain?llm_provider=mock", nil)
			req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
			rec := httptest.NewRecorder()
			handler(rec, req)
			assertGateOutcome(t, rec, resolver, row.admit, row.status, row.refusal)
		})
	}
}

func TestWorkUnitExplainRunsTheModelGateBeforeAnyProvider(t *testing.T) {
	for _, row := range explainGateRows {
		t.Run(row.name, func(t *testing.T) {
			resolver := &countingResolver{}
			handler := newWorkUnitExplainHandler(newTestWorkUnitsReader(t), nil, resolver, nil, llmorgsettings.Gate{Source: row.source})
			rec := httptest.NewRecorder()
			handler(rec, newWorkUnitExplainRequest(t, "wu-ABC-123", "llm_provider=mock"))
			assertGateOutcome(t, rec, resolver, row.admit, row.status, row.refusal)
		})
	}
}

func TestExplainRoutesRefuseWithoutAGate(t *testing.T) {
	reader, err := investmentexplain.NewReader(unusedQueryClient{})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/api/v1/investment/explain?llm_provider=mock", nil)
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
	rec := httptest.NewRecorder()
	newInvestmentExplainWorkHandler(reader, nil, nil, nil, nil)(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("a missing gate must refuse, got %d", rec.Code)
	}
}

func assertGateOutcome(t *testing.T, rec *httptest.ResponseRecorder, resolver *countingResolver, admit bool, status int, refusal string) {
	t.Helper()
	if admit {
		if rec.Code == http.StatusForbidden || rec.Code == http.StatusServiceUnavailable {
			t.Fatalf("an admitted request was refused: %d %s", rec.Code, rec.Body.String())
		}
		return
	}
	if rec.Code != status || !strings.Contains(rec.Body.String(), `"`+refusal+`"`) {
		t.Fatalf("got %d %s, want %d with detail %q", rec.Code, rec.Body.String(), status, refusal)
	}
	if n := resolver.calls.Load(); n != 0 {
		t.Fatalf("a refused request made %d provider-resolution calls; the platform model must not be reachable", n)
	}
}

// Every production builder of an explain route must hand its handler the real gate.
func TestExplainRouteBuildersWireTheModelGate(t *testing.T) {
	for _, file := range []string{"investment_explain_route.go", "workunit_explain_route.go"} {
		src, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(src), "newModelGate(orgSettingsPool, orgSettings)") {
			t.Errorf("%s: the route handler is built without newModelGate", file)
		}
	}
}
