package investmentexplain

import (
	"context"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"
	"github.com/full-chaos/dev-health-ops/internal/llmorgsettings"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

func TestCompleteForOrgReportsTheCredentialsThatMadeTheCall(t *testing.T) {
	calls := 0
	origCreds, origEnv := newProviderFromCredentials, newProviderFromEnv
	t.Cleanup(func() { newProviderFromCredentials, newProviderFromEnv = origCreds, origEnv })
	newProviderFromCredentials = func(categorize.ProviderKind, string, string, string) (categorize.Provider, error) {
		return spendingProvider{calls: &calls}, nil
	}
	newProviderFromEnv = func(categorize.ProviderKind, string) (categorize.Provider, error) {
		return spendingProvider{calls: &calls}, nil
	}
	byo := &fakeOrgResolver{
		usableProvider: "openai", matches: true,
		credentials: llmorgsettings.Credentials{APIKey: secrets.NewHidden("org-key")}, credentialsOK: true,
	}
	platform := &fakeOrgResolver{usableProvider: "openai"}
	for name, run := range map[string]func(resolver *fakeOrgResolver) (categorize.CompletionResult, error){
		"mix": func(r *fakeOrgResolver) (categorize.CompletionResult, error) {
			result, _, _, err := CompleteInvestmentMixExplanationForOrg(context.Background(), "openai", "", "org-1", r, nil, "p")
			return result, err
		},
		"work unit": func(r *fakeOrgResolver) (categorize.CompletionResult, error) {
			result, _, _, err := CompleteWorkUnitExplanationForOrg(context.Background(), "openai", "", "org-1", r, nil, "p")
			return result, err
		},
	} {
		t.Run(name, func(t *testing.T) {
			if result, err := run(byo); err != nil || !result.OrgCredentials {
				t.Errorf("org credentials call: OrgCredentials=%v err=%v", result.OrgCredentials, err)
			}
			if result, err := run(platform); err != nil || result.OrgCredentials {
				t.Errorf("platform fallback call: OrgCredentials=%v err=%v", result.OrgCredentials, err)
			}
		})
	}
}

func TestBuildLLMTokenUsageRecordCredentialSource(t *testing.T) {
	tokens := 3
	for orgCredentials, want := range map[bool]string{true: "org", false: "platform"} {
		record, ok := BuildLLMTokenUsageRecord(TokenUsageInput{
			OrgID: "o", Source: "work_unit_explain", Provider: "openai", InputTokens: &tokens, OrgCredentials: orgCredentials,
		}, fixedComputedAt)
		if !ok || record.CredentialSource != want {
			t.Errorf("org=%v: credential source %q, want %q", orgCredentials, record.CredentialSource, want)
		}
	}
}
