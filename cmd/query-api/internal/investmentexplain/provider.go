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
// default scope: org_id is accepted for signature parity but deliberately
// NOT threaded into any BYO check here -- CHAOS-5006 (#2223) added the org
// BYO resolution path (categorize.ResolveProviderKindForOrg), but this
// function is kept as the org-UNAWARE half of the seam on purpose (see
// availabilityFromIsLLMAvailable's own doc comment in provider_org.go for
// why an org-unaware entry point still needs to exist). The real request
// path uses IsLLMAvailableForOrg (provider_org.go), which DOES call the
// org-aware resolver -- confirmed wired at investment_explain_route.go's
// `available` closure. A resolution error (no provider configured,
// matching Python's LLMAuthError branch) is "not available", never a
// propagated error -- mirroring is_llm_available's own try/except
// LLMAuthError: return False.
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

// goUnsupportedButPythonKnownProviderKinds are provider kinds Python's
// providers/__init__.py genuinely knows and can SERVE (a real, configured
// credential completes a real request) but this Go port has no client
// for at all -- goImplementedProviderKinds's complement within Python's
// real known-provider set, MOCK/NONE excluded (those are legitimately
// "no completion possible" on both planes, not a Go scope gap).
//
// Once this route's routeswitch is live, silently answering
// "llm_unavailable" for one of these is a parity break, not an
// acceptable narrower answer: Python would have served the request for
// real. That is why this is a SEPARATE check from IsLLMAvailable, not a
// widening of it -- team-lead ruling, CHAOS-4977 codex round 1's #5.
//
// #2189 (native Go Ollama support) LANDED in this branch's base (merged to
// main as ce0d58d03) -- categorize.ProviderKindOllama moved into
// goImplementedProviderKinds, so it is REMOVED from this set below. This is
// the update this comment itself warned would be needed by hand.
//
// #2189 also REMOVED categorize.ProviderKindLMStudio entirely (chris's
// ruling, CHAOS-4978): Go has no typed kind for bare "lmstudio" anymore,
// deliberately -- LM Studio is reached via ProviderKindLocal instead. But
// Python's providers/__init__.py still serves a real "lmstudio" provider
// name verbatim (llm/providers/__init__.py:31,40,91,339 -- confirmed
// directly against src, not assumed from the removed constant's absence),
// and categorize.ResolveProviderKind still resolves and returns "lmstudio"
// unchanged for an explicit (non-"auto") request -- ResolveProviderKind has
// no special-case mapping from the STRING "lmstudio" to ProviderKindLocal,
// only NewProviderFromEnv's construction path for an explicit
// ProviderKindLocal request reaches an LM-Studio-shaped endpoint via the
// generic LLM_BASE_URL override. So a request naming "lmstudio" explicitly
// is exactly as Go-unimplemented as it was before #2189 -- named below as a
// raw string literal (providerKindLMStudio), the same shape as
// qwen-local/qwen-lmstudio just below, not resurrected as a categorize
// constant this package doesn't own.
//
// qwen-local/qwen-lmstudio are two MORE provider name strings Python's
// _KNOWN_PROVIDERS carries (llm/providers/__init__.py:33-34,47-48) beyond
// the categorize.ProviderKind constants -- they have no typed constant in
// the categorize package at all (a pre-existing gap in that package, not
// introduced here), so they're named as raw string literals rather than
// invented as new categorize.ProviderKind constants this package doesn't
// own. Missing them here reproduces exactly the same silent
// llm_unavailable regression as the typed kinds above, just for string
// values `categorize.ResolveProviderKind` still resolves and returns
// verbatim for an explicit (non-"auto") request. Caught by codex round 2.
const (
	providerKindLMStudio     categorize.ProviderKind = "lmstudio"
	providerKindQwenLocal    categorize.ProviderKind = "qwen-local"
	providerKindQwenLMStudio categorize.ProviderKind = "qwen-lmstudio"
)

var goUnsupportedButPythonKnownProviderKinds = map[categorize.ProviderKind]struct{}{
	categorize.ProviderKindAnthropic: {},
	categorize.ProviderKindGemini:    {},
	categorize.ProviderKindQwen:      {},
	providerKindLMStudio:             {},
	providerKindQwenLocal:            {},
	providerKindQwenLMStudio:         {},
}

// ResolveUnsupportedProviderKind resolves requestedProvider the same way
// IsLLMAvailable/CompleteInvestmentMixExplanation do
// (categorize.ResolveProviderKind) and reports whether the resolution is
// a kind Python can genuinely serve but this port cannot. The caller (the
// route handler) must answer with a distinct non-200 BEFORE any
// streaming begins in that case, so the Python REST forwarder's own
// non-200 fallback (investment_explain_dispatcher.py) routes the request
// to Python's real completion instead of a wrong Go llm_unavailable
// answer. A resolution error (nothing configured at all) is NOT this
// case -- that is the ordinary llm_unavailable path, unchanged.
func ResolveUnsupportedProviderKind(requestedProvider string) (categorize.ProviderKind, bool) {
	kind, err := categorize.ResolveProviderKind(requestedProvider)
	if err != nil {
		return "", false
	}
	_, unsupported := goUnsupportedButPythonKnownProviderKinds[kind]
	return kind, unsupported
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
