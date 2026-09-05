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
// identical bodies in Python, kept as two names there; one name here:
// `(name or "auto").strip().lower()`. The `or` tests RAW (pre-strip)
// falsiness -- only a truly EMPTY string substitutes to "auto"; a
// whitespace-only string is truthy in Python and is NOT substituted, so
// it strips down to "" (never reaching "auto"). codex round 2 (#2234),
// P2: this port previously trimmed FIRST and checked the TRIMMED result
// for emptiness, so a whitespace-only value ("   ") was wrongly
// substituted to "auto" here -- the exact same trim-order bug
// categorize.normalizeProviderKind's own doc comment already documents
// fixing (codex round 2, #2178) for its sibling function; this one was
// missed. Used for the CONFIGURED side (an org's own stored provider
// setting) below, where "empty (or whitespace) stored provider means
// auto" is the correct, intentional semantic -- see normalizeRequested
// below for why the REQUESTED side needs a DIFFERENT function entirely,
// not just this same fix.
func normalizeProvider(name string) string {
	if name == "" {
		name = "auto"
	}
	return strings.ToLower(strings.TrimSpace(name))
}

// normalizeRequestedProvider trims and lowercases ONLY -- no empty-to-
// "auto" substitution at all, unlike normalizeProvider. codex round 2
// (#2234), P2: every production caller of Credentials/Matches/Model/
// RawBaseURL passes an ALREADY-RESOLVED categorize.ProviderKind (from
// ResolveProviderKindForOrg), never a raw, unprocessed user string --
// and that resolver's own contract is that an empty kind means "the
// caller explicitly requested an invalid/malformed provider" (e.g. a
// whitespace-only `?llm_provider=+` query value, which
// categorize.normalizeProviderKind correctly treats as EXPLICIT and
// non-"auto", collapsing to ""), which must NEVER be treated as a
// wildcard "auto" match against whatever the org happens to have
// configured. Reusing normalizeProvider's own empty-to-"auto"
// substitution on this already-resolved value let a malformed request
// silently match ANY configured BYO provider (since "auto" matches
// unconditionally) instead of correctly refusing it -- a request that
// should fail the pre-stream availability check with `llm_unavailable`
// (matching Python) instead passed it, then failed LATE, mid-stream,
// with a generic construction error after the response had already
// started with a 200 status.
func normalizeRequestedProvider(name string) string {
	return strings.ToLower(strings.TrimSpace(name))
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
	requested := normalizeRequestedProvider(provider)
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
	requested := normalizeRequestedProvider(provider)
	if requested != "auto" && requested != configured {
		return "", nil
	}
	return settings[keyModel], nil
}

// RawBaseURL returns the org's raw, UNVALIDATED base_url for `provider`
// (empty when the org has not configured this provider at all) --
// deliberately bypassing the SSRF guard and completeness checks
// Credentials()/Matches() apply. CHAOS-5006 PR3 (this package's first
// production caller) uses it ONLY to distinguish, for its own fallback
// telemetry, "org never configured BYO for this provider" from "org
// configured BYO but its base_url specifically failed the SSRF guard" --
// Credentials()'s bare ok=false collapses both into one signal, which is
// deliberately not enough to decide whether an SSRF-fallback log line is
// warranted (this package's own doc.go still does not write that log
// itself -- see its documented, still-open gap: the Python audit-log
// write this mirrors is a separate, larger follow-up).
func (s Store) RawBaseURL(ctx context.Context, orgID, provider string) (baseURL string, configured bool, err error) {
	settings, err := s.loadGatedSettings(ctx, orgID)
	if err != nil {
		return "", false, err
	}
	configuredProvider := normalizeProvider(settings[keyProvider])
	requested := normalizeRequestedProvider(provider)
	if requested != "auto" && requested != configuredProvider {
		return "", false, nil
	}
	return settings[keyBaseURL], true, nil
}

// Resolver is the seam a caller outside this package (CHAOS-5006 PR3:
// cmd/query-api/internal/investmentexplain) depends on instead of the
// concrete Store type -- an interface, not internal/jobs/investment/
// categorize.OrgProviderResolver's bare func type, because a caller that
// needs credentials/model too (not just kind resolution) wants all three
// methods behind one value it can pass around and nil-check once. Store
// satisfies this exactly; a caller with no Postgres wiring yet passes a
// nil Resolver, which every method here treats identically to "org has
// no usable BYO provider" (see each method's own nil-orgSettings
// handling at its call sites).
type Resolver interface {
	// ResolveUsableProvider matches categorize.OrgProviderResolver's own
	// func-type signature exactly, so `store.ResolveUsableProvider` (a
	// method value) can be passed directly wherever an
	// OrgProviderResolver is expected -- no adapter needed.
	ResolveUsableProvider(ctx context.Context, orgID string) (string, error)
	Credentials(ctx context.Context, orgID, provider string) (Credentials, bool, error)
	Matches(ctx context.Context, orgID, provider string) (bool, error)
	Model(ctx context.Context, orgID, provider string) (string, error)
	RawBaseURL(ctx context.Context, orgID, provider string) (baseURL string, configured bool, err error)
}

var _ Resolver = Store{}
