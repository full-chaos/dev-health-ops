package categorize

import (
	"context"
	"fmt"
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

// ResolveProviderKind ports providers/__init__.py's resolve_provider_name,
// narrowed to the platform-default path: org BYO resolution
// (resolve_usable_org_llm_provider, backed by org settings storage) has no
// Go port, since no Go caller threads an org_id through this package yet.
// An explicit, non-"auto" request is honored as deliberate caller intent,
// exactly as the Python source comments it -- never silently overridden by
// LLM_PROVIDER or auto-detection.
func ResolveProviderKind(requested string) (ProviderKind, error) {
	if normalized := normalizeProviderKind(requested); normalized != providerKindAuto {
		return normalized, nil
	}

	envKind := normalizeProviderKind(os.Getenv("LLM_PROVIDER"))
	// Operator kill-switch / explicit disable: LLM_PROVIDER=none or mock
	// must not be overridden by auto-detection.
	if envKind == ProviderKindNone || envKind == ProviderKindMock {
		return envKind, nil
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
			Model: firstNonEmptyEnv("LLM_MODEL", "LLM_MODEL_OPENAI"),
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
			Model:  firstNonEmptyEnv("LLM_MODEL", "LLM_MODEL_LOCAL", "LOCAL_LLM_MODEL"),
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
			Model:   firstNonEmptyEnv("LLM_MODEL", "LLM_MODEL_OLLAMA", "OLLAMA_MODEL"),
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
