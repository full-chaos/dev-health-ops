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
	"log"
	"net/url"
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
//
// model is the CompleteInvestmentMixExplanationForOrg's own already-fully-
// resolved model (ResolveModelNameForOrg's return value -- explicit
// request override, else the org's stored model, else that provider's
// documented default), passed through VERBATIM and used directly, never
// re-derived here.
//
// codex round 1 (#2234), P2 [FIXED then]: an earlier version ignored an
// explicit caller override entirely and always called orgSettings.Model
// itself. codex round 2 (#2234), P1 [fixed here]: the round-1 fix threaded
// through the RAW requestedModel (empty whenever the caller passed none),
// not the FULLY RESOLVED model -- so when an org had BYO credentials but
// NO stored model, this function's own orgSettings.Model call returned ""
// too, and construction fell through to categorize's OWN per-provider
// default (e.g. openaiprovider.go's "gpt-5-nano") instead of
// ResolveModelNameForOrg's already-computed default (e.g.
// defaultModelByProvider[openai] == "gpt-5-mini") -- two DIFFERENT
// hardcoded defaults disagreeing on which model a BYO request with no
// explicit/stored model actually gets. Passing the ALREADY-RESOLVED value
// straight through eliminates the second, independent derivation
// entirely, so there is only one source of truth for "what model does
// this request use."
func newProviderForOrg(
	ctx context.Context, kind categorize.ProviderKind, orgID, model string, orgSettings llmorgsettings.Resolver,
) (categorize.Provider, error) {
	if orgSettings != nil {
		creds, ok, err := orgSettings.Credentials(ctx, orgID, string(kind))
		if err != nil {
			return nil, err
		}
		if ok {
			return newProviderFromCredentials(kind, creds.APIKey, creds.BaseURL, model)
		}
		// Not usable via org BYO. team-lead ruling (codex round 1, P2 --
		// SSRF-fallback telemetry, the one finding this package's own
		// doc.go already flagged as a known, deferred gap): as the FIRST
		// production caller of this package, log the fallback -- but
		// ONLY when it happened because the org's own base_url
		// specifically failed the SSRF guard, not for the overwhelmingly
		// common "org configured no BYO for this provider at all" case
		// (that would be noise, not a security signal). Org id + URL
		// scheme/host only -- never the full URL, its path/query, or any
		// credential value.
		logOrgBaseURLSSRFFallback(ctx, orgSettings, orgID, string(kind))
	}
	return newProviderFromEnv(kind)
}

// logOrgBaseURLSSRFFallback re-checks the org's RAW (unvalidated)
// base_url against the SSRF guard specifically to decide whether THIS
// fallback (platform env instead of org credentials) has a security
// signal worth logging -- every other "not usable" reason (no BYO
// configured, incomplete api_key) is silent, matching Python's own
// fallback paths, none of which have an equivalent audit event either.
// Indirected through a package var so a test can capture what would
// have been logged without parsing real log output.
var logOrgBaseURLSSRFFallback = func(ctx context.Context, orgSettings llmorgsettings.Resolver, orgID, provider string) {
	rawURL, configured, err := orgSettings.RawBaseURL(ctx, orgID, provider)
	if err != nil || !configured || rawURL == "" {
		return
	}
	if ok, _ := llmorgsettings.ValidateBaseURL(ctx, rawURL); ok {
		return // not usable for some OTHER reason (e.g. incomplete api_key)
	}
	scheme, host := schemeAndHostForLogging(rawURL)
	log.Printf(
		"investment/explain: org %s configured a BYO base_url for provider %s that failed "+
			"the SSRF guard (scheme=%s host=%s); falling back to the platform default provider",
		orgID, provider, scheme, host,
	)
}

// schemeAndHostForLogging extracts ONLY the scheme and host from a
// base_url for a log line -- never the path, query, userinfo, or any
// other component, so a credential accidentally embedded in the URL
// (userinfo, a query-string API key) can never reach a log. An
// unparseable URL logs as "invalid"/"invalid" rather than the raw value.
func schemeAndHostForLogging(rawURL string) (scheme, host string) {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return "invalid", "invalid"
	}
	if parsed.Scheme == "" {
		scheme = "unknown"
	} else {
		scheme = parsed.Scheme
	}
	if parsed.Hostname() == "" {
		host = "unknown"
	} else {
		host = parsed.Hostname()
	}
	return scheme, host
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

	provider, err := newProviderForOrg(ctx, kind, orgID, model, orgSettings)
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
