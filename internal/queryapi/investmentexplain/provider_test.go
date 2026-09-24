package investmentexplain

import (
	"context"
	"encoding/json"
	"os"
	"testing"
)

// TestCompleteInvestmentMixExplanationMockProviderEndToEnd proves the
// wiring actually reaches categorize.MockProvider's investment-mix-
// explanation branch (mockExplanation) and returns its
// canned, schema-shaped-but-NOT-parser-shaped JSON -- confirmed against
// investment_mix_validation.py's TOP_LEVEL_KEYS: the mock's real Python
// response ({"summary", "dominant_themes", "key_drivers",
// "operational_signals", "confidence_note"}) does NOT match what
// parse_investment_mix_response requires ({"summary", "top_findings",
// "confidence", "what_to_check_next", "anti_claims"}), which is why every
// llm_provider="mock" investment/explain call is expected to land on the
// invalid_llm_output fallback path in the real system, not "valid" --
// this is Python's actual, preserved behavior, not a Go-port defect.
func TestCompleteInvestmentMixExplanationMockProviderEndToEnd(t *testing.T) {
	result, resolvedProvider, resolvedModel, err := CompleteInvestmentMixExplanation(
		context.Background(), "mock", "", "some prompt text\nEvidence Quality: (high)\n  - velocity.feature: 62.50%\n",
	)
	if err != nil {
		t.Fatalf("CompleteInvestmentMixExplanation: %v", err)
	}
	if resolvedProvider != "mock" {
		t.Fatalf("resolvedProvider = %q, want %q", resolvedProvider, "mock")
	}
	if resolvedModel != "mock" {
		t.Fatalf("resolvedModel = %q, want %q", resolvedModel, "mock")
	}
	if result.Model != "mock" {
		t.Fatalf("result.Model = %q, want %q", result.Model, "mock")
	}

	var decoded map[string]any
	if err := json.Unmarshal([]byte(result.Text), &decoded); err != nil {
		t.Fatalf("mock response is not valid JSON: %v\ntext: %s", err, result.Text)
	}
	wantKeys := []string{"summary", "dominant_themes", "key_drivers", "operational_signals", "confidence_note"}
	for _, key := range wantKeys {
		if _, ok := decoded[key]; !ok {
			t.Fatalf("mock response missing expected key %q: %s", key, result.Text)
		}
	}
	if _, ok := decoded["top_findings"]; ok {
		t.Fatalf("mock response unexpectedly has a top_findings key -- the parser-shaped and mock-shaped responses should never collide: %s", result.Text)
	}

	parseStatus := ParseInvestmentMixResponse(result.Text, ParseOptions{FallbackLevel: "unknown"}).Status
	if parseStatus != ParseStatusInvalidLLMOutput {
		t.Fatalf("expected the mock provider's response to fail strict parsing with status %q, got %q -- if this now passes, the mock provider's shape changed and this test's own documentation is stale", ParseStatusInvalidLLMOutput, parseStatus)
	}
}

// TestResolveUnsupportedProviderKindCoversFullPythonKnownSet regresses
// codex round 2 (P1): Python's _KNOWN_PROVIDERS
// (llm/providers/__init__.py:37-48) has 11 names, more than the typed
// categorize.ProviderKind constants -- "qwen-local" and "qwen-lmstudio"
// are real, distinct provider name strings Python resolves and
// constructs (llm/providers/__init__.py:369/376) with no typed constant
// anywhere in this repo, and #2189 (CHAOS-4978) additionally removed the
// categorize.ProviderKindLMStudio constant entirely even though Python's
// bare "lmstudio" is still a real, served provider name -- see
// provider.go's goUnsupportedButPythonKnownProviderKinds doc comment. A
// request for any of these three must ALSO get the pre-stream 501, not
// the silent llm_unavailable regression #5's fix was meant to close
// entirely.
func TestResolveUnsupportedProviderKindCoversFullPythonKnownSet(t *testing.T) {
	for _, requested := range []string{
		"anthropic", "gemini", "qwen", "lmstudio",
		"qwen-local", "qwen-lmstudio",
	} {
		t.Run(requested, func(t *testing.T) {
			_, unsupported := ResolveUnsupportedProviderKind(requested)
			if !unsupported {
				t.Fatalf("ResolveUnsupportedProviderKind(%q) = unsupported=false, want true", requested)
			}
		})
	}

	// The discriminating half: a kind this port DOES implement must NOT
	// be flagged unsupported. "ollama" moved here from the unsupported
	// list above once #2189 (native Go Ollama support) landed in this
	// branch's base (merged to main as ce0d58d03) -- categorize.
	// ProviderKindOllama is now in goImplementedProviderKinds.
	for _, requested := range []string{"openai", "local", "ollama", "mock", "none"} {
		t.Run(requested, func(t *testing.T) {
			_, unsupported := ResolveUnsupportedProviderKind(requested)
			if unsupported {
				t.Fatalf("ResolveUnsupportedProviderKind(%q) = unsupported=true, want false (this port implements it)", requested)
			}
		})
	}
}

// TestProviderHasRequiredConfigCoversFullPythonKnownSet is the
// providerHasRequiredConfig/IsLLMAvailable sibling of
// TestResolveUnsupportedProviderKindCoversFullPythonKnownSet above: a table
// over every kind either function above resolves, so the next kind that
// moves from goUnsupportedButPythonKnownProviderKinds into
// goImplementedProviderKinds cannot silently skip the availability gate the
// way ollama did -- the availability check has its own switch, separate
// from the unsupported-kind check, and only ONE of the two switches was
// widened when ollama's native Go client landed. mock/none need no env
// control (their branches never read one); openai/local/ollama/the six
// BYO-only kinds each get their own subtest so env clearing stays scoped to
// the case that needs it, matching resolve_model_name_golden_test.go's own
// "clear before every subtest" idiom.
func TestProviderHasRequiredConfigCoversFullPythonKnownSet(t *testing.T) {
	envNames := []string{
		"OPENAI_API_KEY", "LLM_API_KEY", "LLM_BASE_URL", "OPENAI_BASE_URL",
		"LOCAL_LLM_BASE_URL", "LOCAL_LLM_API_KEY",
		"OLLAMA_BASE_URL", "OLLAMA_API_KEY", "OLLAMA_MODEL", "LLM_MODEL_OLLAMA",
	}
	clearEnv := func(t *testing.T) {
		t.Helper()
		for _, name := range envNames {
			t.Setenv(name, "")
			_ = os.Unsetenv(name)
		}
	}

	t.Run("mock", func(t *testing.T) {
		clearEnv(t)
		if !IsLLMAvailable("mock", "") {
			t.Fatal("IsLLMAvailable(mock) = false, want true (always available)")
		}
	})
	t.Run("none", func(t *testing.T) {
		clearEnv(t)
		if IsLLMAvailable("none", "") {
			t.Fatal("IsLLMAvailable(none) = true, want false (never available)")
		}
	})
	t.Run("local_with_nothing_configured", func(t *testing.T) {
		clearEnv(t)
		if !IsLLMAvailable("local", "") {
			t.Fatal("IsLLMAvailable(local) = false, want true (NewProviderFromEnv never errors for local)")
		}
	})
	t.Run("ollama_with_nothing_configured", func(t *testing.T) {
		clearEnv(t)
		// This is the regression: ollama's native Go client (categorize.
		// NewOllamaProvider) never errors -- no required field, same shape as
		// local -- so, exactly like Python's own _provider_has_required_config
		// (ollama is not in _API_KEY_REQUIRED_PROVIDERS), an explicit ollama
		// request is available with zero configuration. Before
		// providerHasRequiredConfig's switch carries ProviderKindOllama, this
		// falls to the default case and reports unavailable -- the bug this
		// test exists to pin shut.
		if !IsLLMAvailable("ollama", "") {
			t.Fatal("IsLLMAvailable(ollama) = false, want true (NewProviderFromEnv never errors for ollama)")
		}
	})
	t.Run("openai_unconfigured", func(t *testing.T) {
		clearEnv(t)
		if IsLLMAvailable("openai", "") {
			t.Fatal("IsLLMAvailable(openai) = true, want false (no OPENAI_API_KEY/LLM_API_KEY set)")
		}
	})
	t.Run("openai_configured", func(t *testing.T) {
		clearEnv(t)
		t.Setenv("OPENAI_API_KEY", "sk-test-not-real")
		if !IsLLMAvailable("openai", "") {
			t.Fatal("IsLLMAvailable(openai) = false, want true (OPENAI_API_KEY set)")
		}
	})

	for _, requested := range []string{
		"anthropic", "gemini", "qwen", "lmstudio", "qwen-local", "qwen-lmstudio",
	} {
		requested := requested
		t.Run(requested, func(t *testing.T) {
			clearEnv(t)
			if IsLLMAvailable(requested, "") {
				t.Fatalf("IsLLMAvailable(%q) = true, want false (this port has no client for it)", requested)
			}
		})
	}
}
