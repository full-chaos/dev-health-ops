package llmorgsettings

import (
	"context"
	"strings"
)

// knownProviders mirrors llm/credentials.py's _is_known_llm_provider (the
// union of _API_KEY_ENV_BY_PROVIDER and _BASE_URL_ENV_BY_PROVIDER keys) --
// the set of provider name strings the ORG SETTINGS resolver recognizes as
// configurable at all. This is deliberately a WIDER set than any Go
// implementation registry (categorize.goImplementedProviderKinds, or
// cmd/query-api/internal/investmentexplain's
// goUnsupportedButPythonKnownProviderKinds): an org can configure
// "lmstudio"/"qwen-local"/"qwen-lmstudio" exactly as Python allows, even
// though today's Go port cannot construct a client for them -- that gap is
// ResolveUnsupportedProviderKind's job to catch downstream, not this
// package's job to hide.
var knownProviders = map[string]struct{}{
	"openai": {}, "anthropic": {}, "gemini": {}, "qwen": {},
	"local": {}, "ollama": {}, "lmstudio": {},
	"qwen-local": {}, "qwen-lmstudio": {},
}

// apiKeyRequiredProviders mirrors _API_KEY_REQUIRED_PROVIDERS: only these
// four can be "incomplete" for lacking an api_key -- every local/
// self-hosted provider is usable without one.
var apiKeyRequiredProviders = map[string]struct{}{
	"openai": {}, "anthropic": {}, "gemini": {}, "qwen": {},
}

// normalizeProvider ports both providers/__init__.py's
// _normalize_provider_name and credentials.py's _normalize_provider --
// identical bodies in Python, kept as two names there; one name here.
func normalizeProvider(name string) string {
	trimmed := strings.ToLower(strings.TrimSpace(name))
	if trimmed == "" {
		return "auto"
	}
	return trimmed
}

func isKnownProvider(provider string) bool {
	_, ok := knownProviders[provider]
	return ok
}

// credentialsComplete ports _llm_credentials_complete narrowed to the
// api_key half (base_url is never hard-required by any provider in
// Python either).
func credentialsComplete(provider, apiKey string) bool {
	if _, required := apiKeyRequiredProviders[provider]; required {
		return apiKey != ""
	}
	return true
}

// Credentials is an org's BYO api_key/base_url pair for one provider.
type Credentials struct {
	APIKey  string
	BaseURL string
}

// ResolveUsableProvider ports resolve_usable_org_llm_provider: the org's
// BYO provider name iff it is configured AND usable (known provider,
// credential material present, complete per-provider, base_url passes the
// SSRF guard). Returns "" -- never an error -- for every "not usable"
// case (unset, auto/mock/none, unrecognized, incomplete, SSRF-rejected):
// Python logs a warning and falls through to the platform default in
// those cases rather than raising (CHAOS-2550 decision #1,
// "silent-but-missing > crashing"); this package leaves that logging to
// its caller, which has the request/route context this pure resolver
// does not. An error return here is ONLY the fail-closed case: the
// byo_llm feature-flag state for a BYO-configured org could not be
// determined at all (see loadGatedSettings).
func (s Store) ResolveUsableProvider(ctx context.Context, orgID string) (string, error) {
	settings, err := s.loadGatedSettings(ctx, orgID)
	if err != nil {
		return "", err
	}
	provider := normalizeProvider(settings[keyProvider])
	if provider == "" || provider == "auto" || provider == "mock" || provider == "none" {
		return "", nil
	}
	if !isKnownProvider(provider) {
		return "", nil
	}
	creds := Credentials{APIKey: settings[keyAPIKey], BaseURL: settings[keyBaseURL]}
	if creds.APIKey == "" && creds.BaseURL == "" {
		return "", nil
	}
	if !credentialsComplete(provider, creds.APIKey) {
		return "", nil
	}
	if ok, _ := ValidateBaseURL(ctx, creds.BaseURL); !ok {
		return "", nil
	}
	return provider, nil
}

// Credentials ports _resolve_org_byo_credentials: the org's BYO
// api_key/base_url for `provider` when the org has configured exactly
// that provider (or configured none at all -- an empty stored "provider"
// key matches any request, same as Python's `if configured_provider and
// ...`) with a complete, SSRF-safe bundle. ok=false covers every
// not-usable case Python falls back on; err is non-nil ONLY for the
// fail-closed feature-flag-lookup failure, same contract as
// ResolveUsableProvider.
func (s Store) Credentials(ctx context.Context, orgID, provider string) (Credentials, bool, error) {
	settings, err := s.loadGatedSettings(ctx, orgID)
	if err != nil {
		return Credentials{}, false, err
	}
	configured := normalizeProvider(settings[keyProvider])
	requested := normalizeProvider(provider)
	// normalizeProvider never returns "" (an absent/empty stored provider
	// normalizes to "auto"), so Python's `if configured_provider and ...`
	// is always true -- the real gate is just this membership check.
	if requested != "auto" && requested != configured {
		return Credentials{}, false, nil
	}
	creds := Credentials{APIKey: settings[keyAPIKey], BaseURL: settings[keyBaseURL]}
	if creds.APIKey == "" && creds.BaseURL == "" {
		return Credentials{}, false, nil
	}
	if !credentialsComplete(requested, creds.APIKey) {
		return Credentials{}, false, nil
	}
	if ok, _ := ValidateBaseURL(ctx, creds.BaseURL); !ok {
		return Credentials{}, false, nil
	}
	return creds, true, nil
}

// Matches ports org_byo_provider_matches: true iff the org configured
// `provider` with a complete, SSRF-safe bundle -- used by a caller that
// needs to know whether org BYO is the active credential source (so
// model resolution stays source-bound: the org's own model, never a
// platform env model) without needing the credentials themselves.
func (s Store) Matches(ctx context.Context, orgID, provider string) (bool, error) {
	_, ok, err := s.Credentials(ctx, orgID, provider)
	return ok, err
}

// Model ports resolve_llm_org_settings_model: the org's configured model
// string for `provider`, iff the org's stored provider matches (or the
// org set no provider at all). Returns "" -- never an error for a
// mismatch or an unset model, matching Python's own `return ""` branches;
// err is only the fail-closed feature-flag case.
func (s Store) Model(ctx context.Context, orgID, provider string) (string, error) {
	settings, err := s.loadGatedSettings(ctx, orgID)
	if err != nil {
		return "", err
	}
	configured := normalizeProvider(settings[keyProvider])
	requested := normalizeProvider(provider)
	if requested != "auto" && requested != configured {
		return "", nil
	}
	return settings[keyModel], nil
}
