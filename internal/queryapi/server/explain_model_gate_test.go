package server

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

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

func (c *countingResolver) StoredBaseURLCarriesKey(context.Context, string, string) (bool, error) {
	c.calls.Add(1)
	return false, nil
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
	{"B1' saved but unusable", gateSource{in: llmorgsettings.Inputs{Saved: true, ServiceModelOn: true, ConfigReason: llmorgsettings.ConfigMissingCredentials}}, false, http.StatusForbidden, "missing_credentials"},
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
			assertGateOutcome(t, rec, resolver, investmentAdmitted(row.source.in.Saved), row.admit, row.status, row.refusal)
		})
	}
}

func TestWorkUnitExplainRunsTheModelGateBeforeAnyProvider(t *testing.T) {
	for _, row := range explainGateRows {
		t.Run(row.name, func(t *testing.T) {
			resolver := &countingResolver{}
			reader := newTestWorkUnitsReader(t)
			if !row.admit {
				var err error
				if reader, err = investmentexplain.NewReader(unusedQueryClient{}); err != nil {
					t.Fatal(err)
				}
			}
			handler := newWorkUnitExplainHandler(reader, nil, resolver, nil, llmorgsettings.Gate{Source: row.source})
			rec := httptest.NewRecorder()
			handler(rec, newWorkUnitExplainRequest(t, "wu-ABC-123", "llm_provider=mock"))
			assertGateOutcome(t, rec, resolver, admittedOutcome{http.StatusNotFound, "not found"}, row.admit, row.status, row.refusal)
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

// investmentAdmitted: an own-model org that reaches the investment route gets no platform answer (its own
// model cannot be built here), a platform org gets the summary.
func investmentAdmitted(own bool) admittedOutcome {
	if own {
		return admittedOutcome{http.StatusOK, "Streaming error"}
	}
	return admittedOutcome{http.StatusOK, `"summary"`}
}

// admittedOutcome is what an admitted request must end as once the gate has let it through: the exact
// status and a marker of the body that proves which path ran.
type admittedOutcome struct {
	status int
	marker string
}

func assertGateOutcome(t *testing.T, rec *httptest.ResponseRecorder, resolver *countingResolver, admitted admittedOutcome, admit bool, status int, refusal string) {
	t.Helper()
	if admit {
		if rec.Code != admitted.status || !strings.Contains(rec.Body.String(), admitted.marker) {
			t.Fatalf("an admitted request ended as %d %s, want %d containing %q", rec.Code, rec.Body.String(), admitted.status, admitted.marker)
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

// countingSettings is the org settings of the production handlers, counting every resolution.
type countingSettings struct{ *countingResolver }

func (countingSettings) OrgConfig(context.Context, string) (llmorgsettings.OrgConfig, bool, error) {
	return llmorgsettings.OrgConfig{}, false, nil
}

type denyingGate struct{ asked atomic.Int64 }

func (g *denyingGate) Decide(context.Context, string) llmorgsettings.Decision {
	g.asked.Add(1)
	return llmorgsettings.Decision{Kind: llmorgsettings.Refuse, Code: "gate_denied_for_test", Status: http.StatusForbidden}
}

// The handlers the route builders mount consult the gate the factory returns and obey its refusal: no
// provider is resolved and no ClickHouse query runs.
func TestMountedExplainHandlersObeyTheModelGate(t *testing.T) {
	deny := &denyingGate{}
	previous := explainModelGate
	explainModelGate = func(*pgxpool.Pool, llmorgsettings.ConfigSource) modelGate { return deny }
	t.Cleanup(func() { explainModelGate = previous })

	reader, err := investmentexplain.NewReader(unusedQueryClient{})
	if err != nil {
		t.Fatal(err)
	}
	for name, run := range map[string]func(*countingSettings) *httptest.ResponseRecorder{
		"investment explain": func(settings *countingSettings) *httptest.ResponseRecorder {
			req := httptest.NewRequest(http.MethodPost, "/api/v1/investment/explain", nil)
			req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
			rec := httptest.NewRecorder()
			investmentExplainHandler(reader, nil, settings, nil)(rec, req)
			return rec
		},
		"work-unit explain": func(settings *countingSettings) *httptest.ResponseRecorder {
			rec := httptest.NewRecorder()
			workUnitExplainHandler(reader, nil, settings, nil)(rec, newWorkUnitExplainRequest(t, "wu-ABC-123", ""))
			return rec
		},
	} {
		t.Run(name, func(t *testing.T) {
			before := deny.asked.Load()
			settings := &countingSettings{&countingResolver{}}
			rec := run(settings)
			assertGateOutcome(t, rec, settings.countingResolver, admittedOutcome{}, false, http.StatusForbidden, "gate_denied_for_test")
			if deny.asked.Load() != before+1 {
				t.Fatalf("the gate was asked %d times, want 1", deny.asked.Load()-before)
			}
		})
	}
}
