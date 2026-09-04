package categorize

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
)

// ProviderKind is llm/providers/__init__.py's provider name string, typed.
// It names the BYO LLM backends Python supports (plus the "none" kill-switch
// and the "mock" test double) even though only four have a Go implementation
// today -- see NewProviderFromEnv. There is no ProviderKindLMStudio: see the
// comment on the const block below.
type ProviderKind string

const (
	ProviderKindOpenAI    ProviderKind = "openai"
	ProviderKindAnthropic ProviderKind = "anthropic"
	ProviderKindGemini    ProviderKind = "gemini"
	ProviderKindQwen      ProviderKind = "qwen"
	ProviderKindLocal     ProviderKind = "local"
	ProviderKindOllama    ProviderKind = "ollama"
	ProviderKindMock      ProviderKind = "mock"
	ProviderKindNone      ProviderKind = "none"

	// There is deliberately no ProviderKindLMStudio: chris's ruling
	// (CHAOS-4978, 2026-09-03 13:14) dropped the native LM Studio provider
	// kind and its LMSTUDIO_* env family -- LM Studio is reached as an
	// OpenAI-compatible endpoint via ProviderKindLocal, using the generic
	// LLM_BASE_URL/LLM_MODEL/LLM_API_KEY overrides below (or LOCAL_LLM_*).
	// Python's llm/providers/local.py still has a distinct LMStudioProvider
	// with its own LMSTUDIO_* env names -- this is a deliberate, RULED
	// divergence from the Python oracle for this one provider, not an
	// oversight; a follow-up ticket tracks reconciling it.

	providerKindAuto ProviderKind = "auto"
)

// normalizeProviderKind ports providers/__init__.py's
// _normalize_provider_name: `(name or "auto").strip().lower()`. The "auto"
// substitution is on Python's `or`, which tests RAW (pre-strip) falsiness --
// only a truly EMPTY string substitutes. codex round 2 (#2178, bigboy) P2:
// the first Go cut substituted on the TRIMMED result instead, so a
// whitespace-only value (non-empty, therefore not substituted in Python,
// but trims to "" and WOULD be substituted by a trim-first check) silently
// became "auto" here -- letting a malformed `LLM_PROVIDER=" "` fall through
// to auto-detection and select a live provider, rather than resolving to
// the same explicit, invalid "" Python returns (which fails loudly
// downstream instead of picking one).
func normalizeProviderKind(name string) ProviderKind {
	if name == "" {
		name = string(providerKindAuto)
	}
	return ProviderKind(strings.ToLower(strings.TrimSpace(name)))
}

// firstNonEmptyEnv returns the value of the first set environment variable
// among names, in priority order -- e.g. a provider-specific override
// before a generic BYO fallback. Only variable NAMES are ever hardcoded
// here (mirroring llm/credentials.py's own env-name tables); no value is
// ever read from or compared against ops/.env.
func firstNonEmptyEnv(names ...string) string {
	for _, name := range names {
		if value := os.Getenv(name); value != "" {
			return value
		}
	}
	return ""
}

// detectConfiguredProviderKind ports providers/__init__.py's
// _configured_provider: presence-only auto-detection over a fixed priority
// order of env KEY NAMES (never their values beyond a non-empty check).
func detectConfiguredProviderKind() (ProviderKind, bool) {
	switch {
	case os.Getenv("OPENAI_API_KEY") != "":
		return ProviderKindOpenAI, true
	case os.Getenv("ANTHROPIC_API_KEY") != "":
		return ProviderKindAnthropic, true
	case os.Getenv("GEMINI_API_KEY") != "":
		return ProviderKindGemini, true
	case os.Getenv("LOCAL_LLM_BASE_URL") != "":
		return ProviderKindLocal, true
	case os.Getenv("DASHSCOPE_API_KEY") != "" || os.Getenv("QWEN_API_KEY") != "":
		return ProviderKindQwen, true
	case os.Getenv("OLLAMA_MODEL") != "" || os.Getenv("OLLAMA_BASE_URL") != "":
		return ProviderKindOllama, true
	}
	return "", false
}

// OrgProviderResolver resolves an org's usable BYO provider kind as a raw
// provider name string, "" when the org has none usable (unconfigured,
// incomplete, SSRF-rejected, or feature-gated off -- see
// internal/llmorgsettings.Store.ResolveUsableProvider, the production
// implementation). A non-nil error is the ONLY case ResolveProviderKindForOrg
// treats as fatal: internal/llmorgsettings' own contract is that error
// return is reserved for a byo_llm feature-flag lookup that genuinely could
// not be answered for an org WITH BYO settings configured (fail closed,
// never silently reroute that org's traffic to the platform default -- see
// that package's doc comment). This package takes the resolver as a plain
// function type, not a llmorgsettings.Store parameter, so it never depends
// on Postgres directly -- callers pass a Store method value
// (store.ResolveUsableProvider satisfies this type exactly).
type OrgProviderResolver func(ctx context.Context, orgID string) (string, error)

// ResolveProviderKind ports providers/__init__.py's resolve_provider_name,
// narrowed to the platform-default path: org BYO resolution has no org_id
// or resolver to consult. Equivalent to
// ResolveProviderKindForOrg(context.Background(), requested, "", nil) --
// kept as a separate, stable-signature entry point for every existing
// caller that has no org context to offer (this package's own tests
// included). See ResolveProviderKindForOrg for the org-aware path
// CHAOS-5006 added.
func ResolveProviderKind(requested string) (ProviderKind, error) {
	return ResolveProviderKindForOrg(context.Background(), requested, "", nil)
}

// ResolveProviderKindForOrg ports providers/__init__.py's
// resolve_provider_name IN FULL, org BYO resolution included (CHAOS-5006:
// the "auto" branch previously always picked the platform provider,
// silently diverging from Python whenever an org's own BYO provider
// should have won -- a divergence #2197's unsupported-provider 501 guard
// could not catch, because both sides name Go-supported kinds). resolveOrg
// nil is treated exactly as "no org configured any BYO provider" (never
// consulted) -- a caller with no org context (or no Postgres wiring yet)
// gets IDENTICAL behavior to the pre-fix ResolveProviderKind.
//
// Precedence, matching Python's resolve_provider_name line for line:
// explicit non-"auto" request > LLM_PROVIDER=none/mock operator kill-switch
// > org BYO (checked BEFORE an explicit platform LLM_PROVIDER value, not
// after -- a tenant's BYO configuration must not be overridden by the
// platform default env) > explicit platform LLM_PROVIDER > env
// auto-detection > error.
func ResolveProviderKindForOrg(
	ctx context.Context, requested string, orgID string, resolveOrg OrgProviderResolver,
) (ProviderKind, error) {
	if normalized := normalizeProviderKind(requested); normalized != providerKindAuto {
		return normalized, nil
	}

	envKind := normalizeProviderKind(os.Getenv("LLM_PROVIDER"))
	// Operator kill-switch / explicit disable: LLM_PROVIDER=none or mock
	// must not be overridden by auto-detection OR by org BYO.
	if envKind == ProviderKindNone || envKind == ProviderKindMock {
		return envKind, nil
	}

	if resolveOrg != nil {
		orgProvider, err := resolveOrg(ctx, orgID)
		if err != nil {
			return "", err
		}
		if orgProvider != "" {
			// codex round 1 (#2223), P3: this branch is the whole point of
			// CHAOS-5006 -- an org's own BYO provider overriding the
			// platform's LLM_PROVIDER for "auto" -- and had no telemetry at
			// all, making it operationally indistinguishable from every
			// other resolution path. Org id only, never the resolved
			// provider's credentials (this function only ever receives a
			// provider NAME string here, never a secret).
			//
			// codex round 2, P3: normalize BEFORE logging, not after --
			// the earlier version logged the raw resolver output
			// (e.g. "  OLLAMA  ") while RETURNING its normalized form
			// ("ollama"), so telemetry never matched the value actually
			// selected, breaking any search/aggregation keyed on the
			// logged provider name.
			normalized := normalizeProviderKind(orgProvider)
			log.Printf("categorize: org %q BYO provider %q selected over platform default for auto resolution", orgID, normalized)
			return normalized, nil
		}
	}

	if envKind != providerKindAuto {
		return envKind, nil
	}

	if detected, ok := detectConfiguredProviderKind(); ok {
		return detected, nil
	}
	return "", fmt.Errorf(
		"no LLM provider is configured: set LLM_PROVIDER, or one of OPENAI_API_KEY, " +
			"ANTHROPIC_API_KEY, GEMINI_API_KEY, QWEN_API_KEY/DASHSCOPE_API_KEY, " +
			"LOCAL_LLM_BASE_URL, OLLAMA_BASE_URL, or OLLAMA_MODEL",
	)
}

// goImplementedProviderKinds are the kinds NewProviderFromEnv can actually
// construct a working client for, as opposed to an explicit-refusal stub.
var goImplementedProviderKinds = map[ProviderKind]struct{}{
	ProviderKindOpenAI: {},
	ProviderKindLocal:  {},
	ProviderKindOllama: {},
	ProviderKindMock:   {},
	ProviderKindNone:   {},
}

// IsProviderKindImplemented reports whether kind has a real Go client
// (openai, local, ollama, mock, none) as opposed to a BYO stub.
func IsProviderKindImplemented(kind ProviderKind) bool {
	_, ok := goImplementedProviderKinds[kind]
	return ok
}

// unimplementedProvider satisfies Provider for a kind Python supports that
// this port does not yet -- so a caller resolving "any of the six" always
// gets a Provider value back, with the refusal happening explicitly at the
// first real call rather than the factory failing before a Provider even
// exists. This is the shape a future Anthropic/Gemini/Qwen port fills in:
// implement Provider, register it in NewProviderFromEnv, delete the
// corresponding entry below.
type unimplementedProvider struct {
	kind ProviderKind
}

func (p unimplementedProvider) Complete(_ context.Context, _ CompletionRequest) (CompletionResult, error) {
	return CompletionResult{}, fmt.Errorf(
		"LLM provider kind %q is not yet implemented in Go -- implemented kinds: openai, local, ollama, mock, none",
		p.kind,
	)
}

func (p unimplementedProvider) Close() error { return nil }

var _ Provider = unimplementedProvider{}

// NewProviderFromEnv constructs a Provider for kind, reading configuration
// from environment variable NAMES only -- mirroring
// llm/credentials.py's _API_KEY_ENV_BY_PROVIDER/_BASE_URL_ENV_BY_PROVIDER
// tables for the two Go-ported real providers. It never reads ops/.env or
// any other file, and this source file never hardcodes a credential VALUE,
// only the variable NAMES Python's own config module already uses.
func NewProviderFromEnv(kind ProviderKind) (Provider, error) {
	return NewProviderFromEnvWithModel(kind, "")
}

// modelEnvByKind ports _MODEL_ENV_BY_PROVIDER (llm/providers/__init__.py:25-31),
// narrowed to the kinds this port can construct.
var modelEnvByKind = map[ProviderKind][]string{
	ProviderKindOpenAI: {"LLM_MODEL_OPENAI"},
	ProviderKindLocal:  {"LLM_MODEL_LOCAL", "LOCAL_LLM_MODEL"},
	ProviderKindOllama: {"LLM_MODEL_OLLAMA", "OLLAMA_MODEL"},
}

// defaultModelByKind ports DEFAULT_MODEL_BY_PROVIDER (llm/providers/base.py:36-46),
// narrowed the same way. Python falls back to this when no env var is set;
// without it a stamp would record an EMPTY model for a run that used the
// provider's built-in default.
var defaultModelByKind = map[ProviderKind]string{
	ProviderKindOpenAI: "gpt-5-mini",
	ProviderKindLocal:  "llama3.2",
	ProviderKindOllama: "llama3.2",
}

// ResolveModelName returns the model that WILL ACTUALLY BE USED for kind, and
// therefore the model that must be stamped into
// work_unit_investments.categorization_model_version.
//
// # ONE FUNCTION FOR BOTH, DELIBERATELY
//
// This is used by BOTH the provider construction below AND the durable stamp
// (materialize.go's resolvedModelName). That is the whole point: codex r2's P1
// was that the two disagreed -- the stamp fell back to the PROVIDER NAME while
// the provider used its env-configured model, so every native row recorded a
// model that never ran and the skip-existing lookup keyed on it. Two call sites
// computing "the model" independently is what produced that defect; a single
// function makes agreement structural rather than a thing to remember.
//
// # PRECEDENCE DIVERGES FROM PYTHON, AND THAT IS A RULING, NOT AN ACCIDENT
//
// Python checks the provider-specific env FIRST, then generic LLM_MODEL
// (llm/providers/__init__.py:204-208). This checks generic LLM_MODEL FIRST, per
// chris's CHAOS-4978 ruling (2026-09-03 13:14): the generic override must be
// able to force a model on ANY provider without touching each provider-specific
// var. The two orders differ ONLY when both are set to different values.
//
// Following Python's order here instead would re-open the very defect this
// fixes: the provider path is already ruled generic-first, so a stamp using
// Python's order would again record a model the run did not use. Matching what
// RUNS is the invariant that matters; matching Python's precedence is not
// available while CHAOS-4978 stands.
//
// The org-BYO branch (org_byo_provider_matches / resolve_llm_org_settings_model)
// is omitted -- no Go caller threads an org_id through this package yet
// (CHAOS-5006).
func ResolveModelName(kind ProviderKind, requested string) string {
	if kind == ProviderKindMock {
		return "mock"
	}
	if kind == ProviderKindNone {
		return ""
	}
	if requested != "" {
		return requested
	}
	// Generic first (CHAOS-4978), then the per-kind chain, then the default.
	names := append([]string{"LLM_MODEL"}, modelEnvByKind[kind]...)
	if resolved := firstNonEmptyEnv(names...); resolved != "" {
		return resolved
	}
	return defaultModelByKind[kind]
}

// NewProviderFromEnvWithModel is NewProviderFromEnv with an explicit model
// override that WINS over every environment variable.
//
// This exists because a caller that knows which model it wants -- the
// investment.materialize executor, which carries the request row's
// `model_ref` -- must be able to say so. Python does exactly this:
// materialize_investments passes `model=config.llm_model` into get_provider
// (materialize.py:1189-1195), and resolve_model_name treats an explicit model
// as winning over any env lookup (llm/providers/__init__.py's
// resolve_model_name, mirrored in cmd/query-api's ResolveModelName).
//
// Dropping the override is not a cosmetic loss (codex r1 P1-b): the executor
// stamps the REQUESTED model into work_unit_investments.categorization_model_version
// and into the skip-existing lookup key, so calling the env-default model
// while recording the requested one writes a row that the next run will treat
// as a valid cached result FOR A MODEL THAT NEVER RAN. The error compounds
// rather than self-correcting.
//
// An empty model means "no explicit request", which is the pre-existing
// env-only behaviour NewProviderFromEnv keeps.
func NewProviderFromEnvWithModel(kind ProviderKind, model string) (Provider, error) {
	// Every provider below takes its model from the SAME resolver the durable
	// stamp uses, so the two cannot drift apart (codex r2 P1).
	resolved := ResolveModelName(kind, model)
	switch kind {
	case ProviderKindMock:
		return MockProvider{}, nil

	case ProviderKindNone:
		return NoneProvider{}, nil

	case ProviderKindOpenAI:
		apiKey := firstNonEmptyEnv("LLM_API_KEY", "OPENAI_API_KEY")
		if apiKey == "" {
			return nil, fmt.Errorf("LLM provider %q is not configured: set OPENAI_API_KEY", kind)
		}
		return NewOpenAIProvider(OpenAIProviderConfig{
			APIKey:  apiKey,
			BaseURL: firstNonEmptyEnv("LLM_BASE_URL", "OPENAI_BASE_URL"),
			// Generic LLM_MODEL checked BEFORE the provider-specific
			// LLM_MODEL_OPENAI (chris's ruling, CHAOS-4978, 2026-09-03
			// 13:14): the generic override must be able to force a model
			// on ANY provider -- e.g. pointing everything at one
			// OpenAI-compatible endpoint/model without touching each
			// provider-specific var individually.
			Model: resolved,
		}), nil

	case ProviderKindLocal:
		// An unset LOCAL_LLM_BASE_URL is NOT an error for an explicitly
		// requested "local" kind (unlike auto-detection, which requires it
		// to even select "local") -- local.py's own LocalProvider falls
		// back to Ollama's default endpoint when base_url is empty, and
		// NewLocalProvider below applies that same fallback.
		return NewLocalProvider(LocalProviderConfig{
			BaseURL: firstNonEmptyEnv("LLM_BASE_URL", "LOCAL_LLM_BASE_URL"),
			// Generic-first, same ruling as openai above.
			Model:  resolved,
			APIKey: firstNonEmptyEnv("LLM_API_KEY", "LOCAL_LLM_API_KEY"),
		}), nil

	case ProviderKindOllama:
		// Native /api/chat client (ollamaprovider.go) -- distinct from
		// ProviderKindLocal, which speaks Ollama's OpenAI-compatible /v1
		// endpoint instead. Env names mirror credentials.py's
		// _API_KEY_ENV_BY_PROVIDER/_BASE_URL_ENV_BY_PROVIDER["ollama"]
		// tables; OLLAMA_MODEL is local.py's OllamaProvider's own model env.
		// Generic LLM_MODEL/LLM_API_KEY checked first, same ruling as above
		// -- OLLAMA_* stays as the native provider's own override tier.
		return NewOllamaProvider(OllamaProviderConfig{
			BaseURL: firstNonEmptyEnv("LLM_BASE_URL", "OLLAMA_BASE_URL"),
			Model:   resolved,
			APIKey:  firstNonEmptyEnv("LLM_API_KEY", "OLLAMA_API_KEY", "LOCAL_LLM_API_KEY"),
		}), nil

	// BYO LLM stubs: Python has a real client for each of these; this port
	// does not yet.
	case ProviderKindAnthropic, ProviderKindGemini, ProviderKindQwen:
		return unimplementedProvider{kind: kind}, nil

	default:
		return nil, fmt.Errorf("unknown LLM provider kind %q", kind)
	}
}
