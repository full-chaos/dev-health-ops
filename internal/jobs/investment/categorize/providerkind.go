package categorize

import (
	"context"
	"fmt"
	"os"
	"strings"
)

// ProviderKind is llm/providers/__init__.py's provider name string, typed.
// It names all six BYO LLM backends Python supports (plus the "none"
// kill-switch and the "mock" test double) even though only three have a Go
// implementation today -- see NewProviderFromEnv.
type ProviderKind string

const (
	ProviderKindOpenAI    ProviderKind = "openai"
	ProviderKindAnthropic ProviderKind = "anthropic"
	ProviderKindGemini    ProviderKind = "gemini"
	ProviderKindQwen      ProviderKind = "qwen"
	ProviderKindLocal     ProviderKind = "local"
	ProviderKindOllama    ProviderKind = "ollama"
	ProviderKindLMStudio  ProviderKind = "lmstudio"
	ProviderKindMock      ProviderKind = "mock"
	ProviderKindNone      ProviderKind = "none"

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
	case os.Getenv("LMSTUDIO_MODEL") != "" || os.Getenv("LMSTUDIO_BASE_URL") != "":
		return ProviderKindLMStudio, true
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
	ProviderKindMock:   {},
	ProviderKindNone:   {},
}

// IsProviderKindImplemented reports whether kind has a real Go client
// (openai, local, mock, none) as opposed to a BYO stub.
func IsProviderKindImplemented(kind ProviderKind) bool {
	_, ok := goImplementedProviderKinds[kind]
	return ok
}

// unimplementedProvider satisfies Provider for a kind Python supports that
// this port does not yet -- so a caller resolving "any of the six" always
// gets a Provider value back, with the refusal happening explicitly at the
// first real call rather than the factory failing before a Provider even
// exists. This is the shape a future Anthropic/Gemini/Qwen/Ollama/LMStudio
// port fills in: implement Provider, register it in NewProviderFromEnv,
// delete the corresponding entry below.
type unimplementedProvider struct {
	kind ProviderKind
}

func (p unimplementedProvider) Complete(_ context.Context, _ CompletionRequest) (CompletionResult, error) {
	return CompletionResult{}, fmt.Errorf(
		"LLM provider kind %q is not yet implemented in Go -- implemented kinds: openai, local, mock, none",
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
			Model:   firstNonEmptyEnv("LLM_MODEL_OPENAI", "LLM_MODEL"),
		}), nil

	case ProviderKindLocal:
		// An unset LOCAL_LLM_BASE_URL is NOT an error for an explicitly
		// requested "local" kind (unlike auto-detection, which requires it
		// to even select "local") -- local.py's own LocalProvider falls
		// back to Ollama's default endpoint when base_url is empty, and
		// NewLocalProvider below applies that same fallback.
		return NewLocalProvider(LocalProviderConfig{
			BaseURL: firstNonEmptyEnv("LLM_BASE_URL", "LOCAL_LLM_BASE_URL"),
			Model:   firstNonEmptyEnv("LLM_MODEL_LOCAL", "LOCAL_LLM_MODEL", "LLM_MODEL"),
			APIKey:  firstNonEmptyEnv("LLM_API_KEY", "LOCAL_LLM_API_KEY"),
		}), nil

	// BYO LLM stubs: Python has a real client for each of these; this port
	// does not yet. Chris runs a local gemma model through LM Studio's
	// OpenAI-compatible endpoint today, which is ProviderKindLocal above
	// (LOCAL_LLM_BASE_URL) -- LMStudio/Ollama's own NATIVE provider names
	// (with their own env tables: LMSTUDIO_BASE_URL/OLLAMA_BASE_URL) are a
	// distinct, thinner variant with no Go port, ticketed separately.
	case ProviderKindAnthropic, ProviderKindGemini, ProviderKindQwen, ProviderKindOllama, ProviderKindLMStudio:
		return unimplementedProvider{kind: kind}, nil

	default:
		return nil, fmt.Errorf("unknown LLM provider kind %q", kind)
	}
}
