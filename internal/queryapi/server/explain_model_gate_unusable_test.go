package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/api/licensing"
	"github.com/full-chaos/dev-health-ops/internal/llmorgsettings"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/investmentexplain"
)

type grantedLicensing struct{}

func (grantedLicensing) Decide(context.Context, string, string) (licensing.Decision, error) {
	return licensing.Decision{Allowed: true, Reason: licensing.ReasonEnabledByOrgOverride}, nil
}

type savedOpenAIRow struct{}

func (savedOpenAIRow) Scan(dest ...any) error {
	*(dest[0].(*[]byte)) = []byte(`["openai"]`)
	return nil
}

type savedOpenAI struct{}

func (savedOpenAI) QueryRow(context.Context, string, ...any) pgx.Row { return savedOpenAIRow{} }

type storedConfig struct{ cfg llmorgsettings.OrgConfig }

func (s storedConfig) OrgConfig(context.Context, string) (llmorgsettings.OrgConfig, bool, error) {
	return s.cfg, true, nil
}

// A saved provider that cannot serve a call is refused before any provider is resolved and before any
// ClickHouse read, on both explain routes, with the platform environment key present or not.
func TestExplainRoutesRefuseASavedConfigThatCannotServe(t *testing.T) {
	for name, c := range map[string]struct {
		cfg    llmorgsettings.OrgConfig
		reason string
	}{
		"no key":    {llmorgsettings.OrgConfig{Provider: "openai"}, "missing_credentials"},
		"bad url":   {llmorgsettings.OrgConfig{Provider: "openai", APIKey: secrets.NewHidden("k"), BaseURL: "http://169.254.169.254/v1"}, "invalid_base_url"},
		"bad kind":  {llmorgsettings.OrgConfig{Provider: "no-such-provider", APIKey: secrets.NewHidden("k")}, "unknown_provider"},
		"empty key": {llmorgsettings.OrgConfig{Provider: "anthropic", APIKey: secrets.NewHidden("")}, "missing_credentials"},
	} {
		for _, platformKey := range []string{"", "sk-platform"} {
			t.Run(name+"/platform key "+platformKey, func(t *testing.T) {
				t.Setenv("OPENAI_API_KEY", platformKey)
				t.Setenv("ANTHROPIC_API_KEY", platformKey)
				gate := llmorgsettings.Gate{Source: llmorgsettings.PgSource{Licensing: grantedLicensing{}, Queryer: savedOpenAI{}, Config: storedConfig{c.cfg}}}

				resolver := &countingResolver{}
				reader, err := investmentexplain.NewReader(unusedQueryClient{})
				if err != nil {
					t.Fatal(err)
				}
				req := httptest.NewRequest(http.MethodPost, "/api/v1/investment/explain", nil)
				req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
				rec := httptest.NewRecorder()
				newInvestmentExplainWorkHandler(reader, nil, resolver, nil, gate)(rec, req)
				assertGateOutcome(t, rec, resolver, admittedOutcome{}, false, http.StatusForbidden, c.reason)

				resolver = &countingResolver{}
				rec = httptest.NewRecorder()
				newWorkUnitExplainHandler(reader, nil, resolver, nil, gate)(rec, newWorkUnitExplainRequest(t, "wu-ABC-123", ""))
				assertGateOutcome(t, rec, resolver, admittedOutcome{}, false, http.StatusForbidden, c.reason)
			})
		}
	}
}
