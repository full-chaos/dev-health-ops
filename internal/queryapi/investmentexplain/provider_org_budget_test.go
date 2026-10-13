package investmentexplain

import (
	"context"
	"errors"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"
	"github.com/full-chaos/dev-health-ops/internal/llmbudget"
	"github.com/full-chaos/dev-health-ops/internal/llmorgsettings"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

type refusingStore struct{ admits int }

func (s *refusingStore) Admit(context.Context, llmbudget.AdmitRequest) (llmbudget.Admission, error) {
	s.admits++
	return llmbudget.Admission{}, llmbudget.ErrBudgetExceeded
}
func (s *refusingStore) Settle(context.Context, llmbudget.SettleRequest) error { return nil }

type spendingProvider struct{ calls *int }

func (p spendingProvider) Complete(context.Context, categorize.CompletionRequest) (categorize.CompletionResult, error) {
	*p.calls++
	return categorize.CompletionResult{Text: "{}"}, nil
}
func (spendingProvider) Close() error  { return nil }
func (spendingProvider) Model() string { return "gpt-5-mini" }

// CHAOS-9451: an org's BYO provider is guarded by its budget; the platform's
// own provider (the env fallback) is not an org's spend and is not.
func TestNewProviderForOrgGuardsOnlyBYOCredentials(t *testing.T) {
	calls := 0
	origCreds, origEnv := newProviderFromCredentials, newProviderFromEnv
	t.Cleanup(func() { newProviderFromCredentials, newProviderFromEnv = origCreds, origEnv })
	newProviderFromCredentials = func(categorize.ProviderKind, string, string, string) (categorize.Provider, error) {
		return spendingProvider{calls: &calls}, nil
	}
	newProviderFromEnv = func(categorize.ProviderKind, string) (categorize.Provider, error) {
		return spendingProvider{calls: &calls}, nil
	}
	store := &refusingStore{}
	byo := &fakeOrgResolver{credentials: llmorgsettings.Credentials{APIKey: secrets.NewHidden("org-key")}, credentialsOK: true}

	provider, err := newProviderForOrg(context.Background(), categorize.ProviderKindOpenAI, "org-1", "gpt-5-mini", byo, store)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := provider.Complete(context.Background(), categorize.InvestmentMixExplanationRequest("p")); !errors.Is(err, llmbudget.ErrBudgetExceeded) {
		t.Fatalf("BYO call err = %v, want ErrBudgetExceeded", err)
	}
	if calls != 0 || store.admits != 1 {
		t.Fatalf("calls=%d admits=%d, want the BYO provider never called after one refused admission", calls, store.admits)
	}

	platform, err := newProviderForOrg(context.Background(), categorize.ProviderKindOpenAI, "org-1", "gpt-5-mini", &fakeOrgResolver{}, store)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := platform.Complete(context.Background(), categorize.InvestmentMixExplanationRequest("p")); err != nil {
		t.Fatalf("platform call err = %v, want it unguarded", err)
	}
	if calls != 1 || store.admits != 1 {
		t.Fatalf("calls=%d admits=%d, want the platform call made without admission", calls, store.admits)
	}
}
