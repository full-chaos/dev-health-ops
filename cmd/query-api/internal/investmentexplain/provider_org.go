// This file is CHAOS-5006 PR3: the org-BYO-aware siblings of provider.go's
// IsLLMAvailable/ResolveUnsupportedProviderKind/CompleteInvestmentMixExplanation/
// ResolveModelName. provider.go's own functions are UNCHANGED (same
// signatures, same tests, same callers) -- every function here is
// additive, taking one more llmorgsettings.Resolver parameter that a
// caller with no org context (or no Postgres wiring) passes as nil,
// which every function below treats identically to "org has no usable
// BYO provider" -- so a nil-resolver call through the "ForOrg" entry
// points is behaviorally identical to the original, unwidened function.
//
// SCOPE: this closes CHAOS-5006's remaining gap over PR2's
// categorize.ResolveProviderKindForOrg (kind resolution only) -- an org's
// own decrypted api_key/base_url/model now also drive the actual
// provider CONSTRUCTION when its BYO provider matched (team-lead ruling:
// resolving the right kind but calling with platform credentials would
// bill/authenticate as the platform account, a worse bug than the one
// being fixed). Source-bound throughout (CHAOS-2550): an org-BYO
// construction never mixes an org credential with a platform env value,
// or vice versa -- see newProviderForOrg's own doc comment.
package investmentexplain

import (
	"context"
	"fmt"
	"os"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"
	"github.com/full-chaos/dev-health-ops/internal/llmorgsettings"
)

// orgResolverFunc adapts a possibly-nil llmorgsettings.Resolver to a
// possibly-nil categorize.OrgProviderResolver -- categorize never
// imports llmorgsettings (stays storage-agnostic; see providerkind.go's
// own doc comment), so every "ForOrg" function here narrows its
// llmorgsettings.Resolver down to this bare func type at the one call
// site that actually needs it.
func orgResolverFunc(orgSettings llmorgsettings.Resolver) categorize.OrgProviderResolver {
	if orgSettings == nil {
		return nil
	}
	return orgSettings.ResolveUsableProvider
}

// availabilityFromIsLLMAvailable adapts the org-unaware IsLLMAvailable to
// AvailabilityFunc's shape (explain.go), for a caller/test with no org
// context to thread through ExplainInvestmentMix -- IsLLMAvailable takes
// no ctx and AvailabilityFunc does, so a bare function reference cannot
// satisfy it directly.
func availabilityFromIsLLMAvailable(_ context.Context, requestedProvider, orgID string) bool {
	return IsLLMAvailable(requestedProvider, orgID)
}

// IsLLMAvailableForOrg is IsLLMAvailable widened with org BYO resolution
// -- is_llm_available's real org_id argument, finally consulted (it was
// reserved but discarded in the original function -- see that function's
// own doc comment).
func IsLLMAvailableForOrg(ctx context.Context, requested, orgID string, orgSettings llmorgsettings.Resolver) bool {
	kind, err := categorize.ResolveProviderKindForOrg(ctx, requested, orgID, orgResolverFunc(orgSettings))
	if err != nil {
		return false
	}
	if providerHasRequiredConfig(kind) {
		return true
	}
	// providerHasRequiredConfig only ever tries NewProviderFromEnv --
	// give an org-BYO match its own chance via its own credentials
	// before answering "not available".
	if orgSettings == nil {
		return false
	}
	_, ok, err := orgSettings.Credentials(ctx, orgID, string(kind))
	return err == nil && ok
}

// ResolveUnsupportedProviderKindForOrg is ResolveUnsupportedProviderKind
// widened the same way.
func ResolveUnsupportedProviderKindForOrg(
	ctx context.Context, requestedProvider, orgID string, orgSettings llmorgsettings.Resolver,
) (categorize.ProviderKind, bool) {
	kind, err := categorize.ResolveProviderKindForOrg(ctx, requestedProvider, orgID, orgResolverFunc(orgSettings))
	if err != nil {
		return "", false
	}
	_, unsupported := goUnsupportedButPythonKnownProviderKinds[kind]
	return kind, unsupported
}

// ResolveModelNameForOrg is ResolveModelName widened with resolve_model_
// name's own org-BYO branch (provider.go's ResolveModelName omits it --
// see that function's doc comment): when org BYO is the active
// credential source for kind (org_byo_provider_matches), the model comes
// from the org's OWN setting or the platform default, NEVER a platform
// env value -- checked in the SAME position Python checks it, after an
// explicit model (which always wins outright, checked first) but before
// any env var.
func ResolveModelNameForOrg(
	ctx context.Context, kind categorize.ProviderKind, model, orgID string, orgSettings llmorgsettings.Resolver,
) (resolved string, found bool) {
	if kind == categorize.ProviderKindMock {
		return "mock", true
	}
	if kind == categorize.ProviderKindNone {
		return "", false
	}
	if model != "" {
		return model, true
	}
	if orgSettings != nil {
		if matches, err := orgSettings.Matches(ctx, orgID, string(kind)); err == nil && matches {
			orgModel, err := orgSettings.Model(ctx, orgID, string(kind))
			if err == nil {
				if orgModel != "" {
					return orgModel, true
				}
				if def, ok := defaultModelByProvider[kind]; ok {
					return def, true
				}
				return "", false
			}
		}
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

// newProviderForOrg constructs a Provider for kind, sourced from the
// org's own BYO credentials when org BYO is the active source for kind,
// else from the platform environment (NewProviderFromEnv) -- exactly
// resolve_llm_credentials' own source-bound precedence (CHAOS-2550):
// never mix an org api_key with a platform base_url, or vice versa.
// orgSettings.Credentials' own ok=false return (unconfigured, mismatched
// provider, incomplete, or SSRF-rejected) IS "org BYO is not the active
// source for kind" -- the same signal org_byo_provider_matches/
// _resolve_org_byo_credentials collapse to in Python (see
// internal/llmorgsettings' own doc comment on the one documented gap:
// this narrows out only the SSRF-fallback AUDIT LOG, never the
// fallback decision itself).
func newProviderForOrg(
	ctx context.Context, kind categorize.ProviderKind, orgID string, orgSettings llmorgsettings.Resolver,
) (categorize.Provider, error) {
	if orgSettings != nil {
		creds, ok, err := orgSettings.Credentials(ctx, orgID, string(kind))
		if err != nil {
			return nil, err
		}
		if ok {
			model, err := orgSettings.Model(ctx, orgID, string(kind))
			if err != nil {
				return nil, err
			}
			return newProviderFromCredentials(kind, creds.APIKey, creds.BaseURL, model)
		}
	}
	return newProviderFromEnv(kind)
}

// newProviderFromCredentials/newProviderFromEnv are newProviderForOrg's
// own construction calls, indirected through package vars so a test can
// substitute a capturing fake and prove WHICH ONE fired and with what
// arguments -- categorize.NewProviderFromCredentials' own construction
// correctness (explicit values win, no ambient-env leakage) is already
// proven in categorize's own test suite
// (providerkind_credentials_test.go); what THIS package's tests need to
// prove is that its OWN wiring picks the right one and passes it the
// org's real values, not that the constructor itself is correct.
var (
	newProviderFromCredentials = categorize.NewProviderFromCredentials
	newProviderFromEnv         = categorize.NewProviderFromEnv
)

// CompleteInvestmentMixExplanationForOrg is CompleteInvestmentMixExplanation
// widened with org BYO kind resolution AND org BYO credential/model
// construction (both halves of CHAOS-5006's fix -- see this file's
// package doc comment). Never logs, wraps, or wraps-with-detail any
// credential value from orgSettings; the only error surface it adds over
// the platform-only path is newProviderForOrg's own construction error,
// which categorize.NewProviderFromCredentials guarantees never
// interpolates apiKey/baseURL (providerkind_credentials_test.go).
func CompleteInvestmentMixExplanationForOrg(
	ctx context.Context, requestedProvider, requestedModel, orgID string,
	orgSettings llmorgsettings.Resolver, fullPrompt string,
) (result categorize.CompletionResult, resolvedProvider string, resolvedModel string, err error) {
	kind, err := categorize.ResolveProviderKindForOrg(ctx, requestedProvider, orgID, orgResolverFunc(orgSettings))
	if err != nil {
		return categorize.CompletionResult{}, "", "", err
	}
	resolvedProvider = string(kind)

	model, found := ResolveModelNameForOrg(ctx, kind, requestedModel, orgID, orgSettings)
	resolvedModel = model
	if !found {
		resolvedModel = requestedModel
		if resolvedModel == "" {
			resolvedModel = resolvedProvider
		}
	}

	provider, err := newProviderForOrg(ctx, kind, orgID, orgSettings)
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
