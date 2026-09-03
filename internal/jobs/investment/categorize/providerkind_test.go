package categorize

import (
	"context"
	"strings"
	"testing"
)

// clearProviderEnv ensures no leftover variable from the host environment
// leaks into a resolution test -- every name any code path in this file
// reads, cleared unconditionally before each test sets up its own scenario.
func clearProviderEnv(t *testing.T) {
	t.Helper()
	names := []string{
		"LLM_PROVIDER", "LLM_API_KEY", "LLM_BASE_URL", "LLM_MODEL",
		"LLM_MODEL_OPENAI", "LLM_MODEL_LOCAL",
		"OPENAI_API_KEY", "OPENAI_BASE_URL",
		"ANTHROPIC_API_KEY", "GEMINI_API_KEY",
		"QWEN_API_KEY", "DASHSCOPE_API_KEY",
		"LOCAL_LLM_BASE_URL", "LOCAL_LLM_MODEL", "LOCAL_LLM_API_KEY",
		"OLLAMA_MODEL", "OLLAMA_BASE_URL",
		"LMSTUDIO_MODEL", "LMSTUDIO_BASE_URL",
	}
	for _, name := range names {
		t.Setenv(name, "")
		_ = name
	}
}

func TestResolveProviderKindExplicitRequestNeverOverridden(t *testing.T) {
	clearProviderEnv(t)
	t.Setenv("LLM_PROVIDER", "anthropic")

	kind, err := ResolveProviderKind("mock")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if kind != ProviderKindMock {
		t.Fatalf("kind = %q, want mock (explicit request must win over LLM_PROVIDER)", kind)
	}
}

func TestResolveProviderKindEnvKillSwitchBeatsAutoDetection(t *testing.T) {
	clearProviderEnv(t)
	t.Setenv("LLM_PROVIDER", "none")
	t.Setenv("OPENAI_API_KEY", "sk-would-otherwise-auto-detect")

	kind, err := ResolveProviderKind("auto")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if kind != ProviderKindNone {
		t.Fatalf("kind = %q, want none (LLM_PROVIDER=none must not be overridden by a configured key)", kind)
	}
}

func TestResolveProviderKindEnvExplicitProviderWins(t *testing.T) {
	clearProviderEnv(t)
	t.Setenv("LLM_PROVIDER", "local")
	t.Setenv("OPENAI_API_KEY", "sk-present-but-not-selected")

	kind, err := ResolveProviderKind("auto")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if kind != ProviderKindLocal {
		t.Fatalf("kind = %q, want local (explicit LLM_PROVIDER wins over auto-detection)", kind)
	}
}

func TestResolveProviderKindAutoDetectionPriorityOrder(t *testing.T) {
	cases := []struct {
		name string
		env  map[string]string
		want ProviderKind
	}{
		{"openai first", map[string]string{"OPENAI_API_KEY": "x", "ANTHROPIC_API_KEY": "y"}, ProviderKindOpenAI},
		{"anthropic before gemini", map[string]string{"ANTHROPIC_API_KEY": "x", "GEMINI_API_KEY": "y"}, ProviderKindAnthropic},
		{"local via base url", map[string]string{"LOCAL_LLM_BASE_URL": "http://x"}, ProviderKindLocal},
		{"qwen via dashscope", map[string]string{"DASHSCOPE_API_KEY": "x"}, ProviderKindQwen},
		{"ollama via model", map[string]string{"OLLAMA_MODEL": "gemma3"}, ProviderKindOllama},
		{"lmstudio via base url", map[string]string{"LMSTUDIO_BASE_URL": "http://x"}, ProviderKindLMStudio},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			clearProviderEnv(t)
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			kind, err := ResolveProviderKind("auto")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if kind != tc.want {
				t.Fatalf("kind = %q, want %q", kind, tc.want)
			}
		})
	}
}

func TestResolveProviderKindWhitespaceLLMProviderDoesNotAutoDetect(t *testing.T) {
	// codex round 2 (#2178, bigboy) P2: LLM_PROVIDER=" " (whitespace, not
	// truly unset) trimmed to "" and was substituted back to "auto" by the
	// old normalizeProviderKind, silently auto-detecting -- and selecting
	// -- a live provider from OPENAI_API_KEY's mere presence. Python's own
	// `(name or "auto")` does not re-substitute here (a whitespace string
	// is truthy pre-strip), so it resolves to the literal, explicitly
	// invalid "" instead -- which fails loudly downstream rather than
	// silently placing a real API call.
	clearProviderEnv(t)
	t.Setenv("LLM_PROVIDER", "   ")
	t.Setenv("OPENAI_API_KEY", "sk-should-not-be-auto-selected")

	kind, err := ResolveProviderKind("auto")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if kind == ProviderKindOpenAI {
		t.Fatal("ResolveProviderKind silently selected \"openai\" from a whitespace LLM_PROVIDER")
	}
	if kind != ProviderKind("") {
		t.Fatalf("kind = %q, want the literal empty kind (matching Python's resolve_provider_name)", kind)
	}
}

func TestResolveProviderKindNothingConfiguredErrors(t *testing.T) {
	clearProviderEnv(t)
	_, err := ResolveProviderKind("auto")
	if err == nil {
		t.Fatal("expected an error when nothing is configured")
	}
}

func TestNewProviderFromEnvMock(t *testing.T) {
	provider, err := NewProviderFromEnv(ProviderKindMock)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := provider.(MockProvider); !ok {
		t.Fatalf("provider = %T, want MockProvider", provider)
	}
}

func TestNewProviderFromEnvNone(t *testing.T) {
	provider, err := NewProviderFromEnv(ProviderKindNone)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	result, err := provider.Complete(context.Background(), CategorizationRequest("prompt"))
	if err != nil {
		t.Fatalf("NoneProvider.Complete returned an error: %v", err)
	}
	if result.Text != "" {
		t.Fatalf("NoneProvider.Complete text = %q, want empty", result.Text)
	}
}

func TestNewProviderFromEnvOpenAIRequiresAPIKey(t *testing.T) {
	clearProviderEnv(t)
	if _, err := NewProviderFromEnv(ProviderKindOpenAI); err == nil {
		t.Fatal("expected an error constructing openai with no OPENAI_API_KEY set")
	}
}

func TestNewProviderFromEnvOpenAIReadsEnv(t *testing.T) {
	clearProviderEnv(t)
	t.Setenv("OPENAI_API_KEY", "sk-test-key-value")
	t.Setenv("LLM_MODEL_OPENAI", "gpt-5-nano-preview")

	provider, err := NewProviderFromEnv(ProviderKindOpenAI)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	openAIProvider, ok := provider.(*OpenAIProvider)
	if !ok {
		t.Fatalf("provider = %T, want *OpenAIProvider", provider)
	}
	if openAIProvider.cfg.APIKey != "sk-test-key-value" {
		t.Error("APIKey not read from OPENAI_API_KEY")
	}
	if openAIProvider.cfg.Model != "gpt-5-nano-preview" {
		t.Errorf("Model = %q, want value from LLM_MODEL_OPENAI", openAIProvider.cfg.Model)
	}
}

func TestNewProviderFromEnvLocalFallsBackToDefaultWithNoBaseURL(t *testing.T) {
	clearProviderEnv(t)
	provider, err := NewProviderFromEnv(ProviderKindLocal)
	if err != nil {
		t.Fatalf("unexpected error: local must fall back to a default endpoint, not require LOCAL_LLM_BASE_URL: %v", err)
	}
	localProvider, ok := provider.(*LocalProvider)
	if !ok {
		t.Fatalf("provider = %T, want *LocalProvider", provider)
	}
	if localProvider.cfg.BaseURL != defaultLocalBaseURL {
		t.Errorf("BaseURL = %q, want the package default %q", localProvider.cfg.BaseURL, defaultLocalBaseURL)
	}
}

func TestNewProviderFromEnvLocalReadsEnv(t *testing.T) {
	clearProviderEnv(t)
	t.Setenv("LOCAL_LLM_BASE_URL", "http://127.0.0.1:1234/v1")
	t.Setenv("LOCAL_LLM_MODEL", "gemma-3-4b")

	provider, err := NewProviderFromEnv(ProviderKindLocal)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	localProvider := provider.(*LocalProvider)
	if localProvider.cfg.BaseURL != "http://127.0.0.1:1234/v1" {
		t.Errorf("BaseURL = %q, want value from LOCAL_LLM_BASE_URL", localProvider.cfg.BaseURL)
	}
	if localProvider.cfg.Model != "gemma-3-4b" {
		t.Errorf("Model = %q, want value from LOCAL_LLM_MODEL", localProvider.cfg.Model)
	}
}

func TestNewProviderFromEnvOllamaReadsEnv(t *testing.T) {
	clearProviderEnv(t)
	t.Setenv("OLLAMA_BASE_URL", "http://127.0.0.1:11434")
	t.Setenv("OLLAMA_MODEL", "gemma3:4b")

	provider, err := NewProviderFromEnv(ProviderKindOllama)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	ollamaProvider, ok := provider.(*OllamaProvider)
	if !ok {
		t.Fatalf("provider = %T, want *OllamaProvider", provider)
	}
	if ollamaProvider.cfg.BaseURL != "http://127.0.0.1:11434" {
		t.Errorf("BaseURL = %q, want value from OLLAMA_BASE_URL", ollamaProvider.cfg.BaseURL)
	}
	if ollamaProvider.cfg.Model != "gemma3:4b" {
		t.Errorf("Model = %q, want value from OLLAMA_MODEL", ollamaProvider.cfg.Model)
	}
}

func TestNewProviderFromEnvOllamaFallsBackToDefaultWithNoEnv(t *testing.T) {
	clearProviderEnv(t)
	provider, err := NewProviderFromEnv(ProviderKindOllama)
	if err != nil {
		t.Fatalf("unexpected error: local must fall back to defaults, not require env: %v", err)
	}
	ollamaProvider := provider.(*OllamaProvider)
	if ollamaProvider.cfg.BaseURL != defaultOllamaBaseURL {
		t.Errorf("BaseURL = %q, want the package default %q", ollamaProvider.cfg.BaseURL, defaultOllamaBaseURL)
	}
	if ollamaProvider.cfg.Model != defaultOllamaModel {
		t.Errorf("Model = %q, want DEFAULT_MODEL_BY_PROVIDER[\"ollama\"] %q", ollamaProvider.cfg.Model, defaultOllamaModel)
	}
}

func TestNewProviderFromEnvLMStudioReadsEnv(t *testing.T) {
	clearProviderEnv(t)
	t.Setenv("LMSTUDIO_BASE_URL", "http://127.0.0.1:1234")
	t.Setenv("LMSTUDIO_MODEL", "gemma-3-4b")

	provider, err := NewProviderFromEnv(ProviderKindLMStudio)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	lmStudioProvider, ok := provider.(*LMStudioProvider)
	if !ok {
		t.Fatalf("provider = %T, want *LMStudioProvider", provider)
	}
	if lmStudioProvider.cfg.BaseURL != "http://127.0.0.1:1234" {
		t.Errorf("BaseURL = %q, want value from LMSTUDIO_BASE_URL", lmStudioProvider.cfg.BaseURL)
	}
	if lmStudioProvider.cfg.Model != "gemma-3-4b" {
		t.Errorf("Model = %q, want value from LMSTUDIO_MODEL", lmStudioProvider.cfg.Model)
	}
}

func TestNewProviderFromEnvLMStudioFallsBackToDefaultWithNoEnv(t *testing.T) {
	clearProviderEnv(t)
	provider, err := NewProviderFromEnv(ProviderKindLMStudio)
	if err != nil {
		t.Fatalf("unexpected error: lmstudio must fall back to defaults, not require env: %v", err)
	}
	lmStudioProvider := provider.(*LMStudioProvider)
	if lmStudioProvider.cfg.BaseURL != defaultLMStudioBaseURL {
		t.Errorf("BaseURL = %q, want the package default %q", lmStudioProvider.cfg.BaseURL, defaultLMStudioBaseURL)
	}
	if lmStudioProvider.cfg.Model != defaultLMStudioModel {
		t.Errorf("Model = %q, want DEFAULT_MODEL_BY_PROVIDER[\"lmstudio\"] %q", lmStudioProvider.cfg.Model, defaultLMStudioModel)
	}
}

func TestNewProviderFromEnvUnimplementedKindsRefuseExplicitly(t *testing.T) {
	for _, kind := range []ProviderKind{
		ProviderKindAnthropic, ProviderKindGemini, ProviderKindQwen,
	} {
		t.Run(string(kind), func(t *testing.T) {
			provider, err := NewProviderFromEnv(kind)
			if err != nil {
				t.Fatalf("NewProviderFromEnv(%q) returned an error, want a stub Provider: %v", kind, err)
			}
			if IsProviderKindImplemented(kind) {
				t.Fatalf("%q reported as implemented, but has no real client", kind)
			}
			_, completeErr := provider.Complete(context.Background(), CategorizationRequest("prompt"))
			if completeErr == nil {
				t.Fatalf("%q's stub Complete() must refuse, got a nil error", kind)
			}
			if !strings.Contains(completeErr.Error(), string(kind)) {
				t.Errorf("refusal error %q does not name the kind %q", completeErr, kind)
			}
			if closeErr := provider.Close(); closeErr != nil {
				t.Errorf("stub Close() returned an error: %v", closeErr)
			}
		})
	}
}

func TestNewProviderFromEnvUnknownKind(t *testing.T) {
	if _, err := NewProviderFromEnv(ProviderKind("not-a-real-provider")); err == nil {
		t.Fatal("expected an error for an unknown provider kind")
	}
}
