// Package investmentexplain -- this file wires provider.Complete for the
// investment-mix-explanation LLM call, reusing
// internal/jobs/investment/categorize's already-ported Provider
// abstraction (Provider/CompletionRequest/CompletionResult,
// InvestmentMixExplanationRequest, MockProvider/OpenAIProvider/
// LocalProvider, ResolveProviderKind/NewProviderFromEnv) rather than a
// second, parallel port -- that package already anticipated this caller
// (InvestmentMixExplanationRequest's own doc comment: "No Go caller
// builds this prompt yet; this constructor exists so a Provider
// implementation has something concrete to be tested against today,
// ahead of that caller landing").
//
// SCOPE, matching categorize.ResolveProviderKind's own documented
// narrowing: this file resolves and constructs a provider for the
// platform-default path only (mock, none, openai, local) -- org BYO
// credential/model resolution (llm/credentials.py's
// resolve_llm_org_settings_model / org_byo_provider_matches /
// resolve_llm_credentials) and the anthropic/gemini/qwen backends have
// no Go port anywhere in this repo yet, categorize's included. Widening
// that is a separate, larger task than "wire provider.Complete with the
// mock provider."
package investmentexplain

import (
	"context"
	"fmt"
	"os"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"
)

// modelEnvByProvider ports llm/providers/__init__.py's
// _MODEL_ENV_BY_PROVIDER, narrowed to the providers this port can
// actually construct (categorize.IsProviderKindImplemented).
var modelEnvByProvider = map[categorize.ProviderKind][]string{
	categorize.ProviderKindOpenAI: {"LLM_MODEL_OPENAI"},
	categorize.ProviderKindLocal:  {"LLM_MODEL_LOCAL", "LOCAL_LLM_MODEL"},
}

// defaultModelByProvider ports llm/providers/base.py's
// DEFAULT_MODEL_BY_PROVIDER, narrowed the same way.
var defaultModelByProvider = map[categorize.ProviderKind]string{
	categorize.ProviderKindOpenAI: "gpt-5-mini",
	categorize.ProviderKindLocal:  "llama3.2",
}

// ResolveModelName ports resolve_model_name (llm/providers/__init__.py:
// 187-209), the org-BYO branch (org_byo_provider_matches /
// resolve_llm_org_settings_model) omitted -- see this file's package doc
// comment for why. found is false only for a kind with neither an
// explicit model, an env override, nor a platform default (matches
// Python returning None); ProviderKindNone always returns ("", false)
// and ProviderKindMock always returns ("mock", true), exactly as Python
// special-cases both before touching any env var.
func ResolveModelName(kind categorize.ProviderKind, model string) (resolved string, found bool) {
	if kind == categorize.ProviderKindMock {
		return "mock", true
	}
	if kind == categorize.ProviderKindNone {
		return "", false
	}
	if model != "" {
		return model, true
	}
	for _, envName := range modelEnvByProvider[kind] {
		if value := os.Getenv(envName); value != "" {
			return value, true
		}
	}
	if value := os.Getenv("LLM_MODEL"); value != "" {
		return value, true
	}
	if def, ok := defaultModelByProvider[kind]; ok {
		return def, true
	}
	return "", false
}

// IsLLMAvailable ports is_llm_available (llm/providers/__init__.py:
// 224-229), narrowed to categorize.ResolveProviderKind's own platform-
// default scope: org_id is accepted for signature parity with the
// eventual real caller but is not yet threaded into any BYO check here
// (no Go port of that resolution exists to call). A resolution error
// (no provider configured, matching Python's LLMAuthError branch) is
// "not available", never a propagated error -- mirroring
// is_llm_available's own try/except LLMAuthError: return False.
func IsLLMAvailable(requested string, _ string) bool {
	kind, err := categorize.ResolveProviderKind(requested)
	if err != nil {
		return false
	}
	return providerHasRequiredConfig(kind)
}

// providerHasRequiredConfig ports _provider_has_required_config
// (llm/providers/__init__.py:95-112), narrowed to the four kinds this
// port can construct: mock is always available, none is never
// available, openai/local match NewProviderFromEnv's own real
// construction attempt (the cheapest correct way to answer "would this
// succeed" is to actually try, matching resolve_llm_credentials'
// approach of attempting resolution and catching the auth error). Any
// other kind (a BYO provider this port doesn't implement) is refused,
// matching Python's `name not in _KNOWN_PROVIDERS` catch-all -- except
// Python's _KNOWN_PROVIDERS DOES include anthropic/gemini/qwen/ollama/
// lmstudio (they're known, just not necessarily configured); this port
// has no way to check THEIR credentials at all, so it reports them
// unavailable rather than guessing, a narrower answer than Python's for
// exactly the kinds this port cannot serve regardless.
func providerHasRequiredConfig(kind categorize.ProviderKind) bool {
	switch kind {
	case categorize.ProviderKindMock:
		return true
	case categorize.ProviderKindNone:
		return false
	case categorize.ProviderKindOpenAI, categorize.ProviderKindLocal:
		provider, err := categorize.NewProviderFromEnv(kind)
		if err != nil {
			return false
		}
		_ = provider.Close()
		return true
	default:
		return false
	}
}

// CompleteInvestmentMixExplanation resolves a provider for
// requestedProvider/requestedModel and calls Complete with the
// investment-mix-explanation request/response format
// (categorize.InvestmentMixExplanationRequest), matching
// explain_investment_mix's own:
//
//	resolved_llm_provider = resolve_provider_name(llm_provider, org_id=org_id)
//	resolved_llm_model = resolve_model_name(resolved_llm_provider, llm_model, org_id=org_id) or llm_model or resolved_llm_provider
//	provider = get_provider(llm_provider, model=llm_model, org_id=org_id)
//	completion = await provider.complete(full_prompt)
//
// resolvedProvider/resolvedModel are returned alongside the completion
// for the caller to pass to token-usage persistence (matching
// _persist_investment_mix_token_usage's provider=resolved_llm_provider,
// model=completion.model or llm_model).
func CompleteInvestmentMixExplanation(ctx context.Context, requestedProvider, requestedModel, fullPrompt string) (result categorize.CompletionResult, resolvedProvider string, resolvedModel string, err error) {
	kind, err := categorize.ResolveProviderKind(requestedProvider)
	if err != nil {
		return categorize.CompletionResult{}, "", "", err
	}
	resolvedProvider = string(kind)

	model, found := ResolveModelName(kind, requestedModel)
	resolvedModel = model
	if !found {
		resolvedModel = requestedModel
		if resolvedModel == "" {
			resolvedModel = resolvedProvider
		}
	}

	provider, err := categorize.NewProviderFromEnv(kind)
	if err != nil {
		return categorize.CompletionResult{}, resolvedProvider, resolvedModel, fmt.Errorf("construct llm provider: %w", err)
	}
	defer func() { _ = provider.Close() }()

	result, err = provider.Complete(ctx, categorize.InvestmentMixExplanationRequest(fullPrompt))
	if err != nil {
		return categorize.CompletionResult{}, resolvedProvider, resolvedModel, err
	}
	return result, resolvedProvider, resolvedModel, nil
}
