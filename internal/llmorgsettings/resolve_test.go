package llmorgsettings

import "testing"

func TestNormalizeProvider(t *testing.T) {
	cases := map[string]string{
		// codex round 2 (#2234), P2: "   " must NOT normalize to "auto" --
		// Python's `(name or "auto").strip().lower()` tests RAW (pre-strip)
		// falsiness; a whitespace-only string is truthy in Python, so the
		// `or` never fires and it strips down to "" instead. Only a truly
		// EMPTY string substitutes.
		"":           "auto",
		"   ":        "",
		"OpenAI":     "openai",
		"  Ollama  ": "ollama",
		"auto":       "auto",
		"LMSTUDIO":   "lmstudio",
		"Qwen-Local": "qwen-local",
	}
	for in, want := range cases {
		if got := normalizeProvider(in); got != want {
			t.Errorf("normalizeProvider(%q) = %q, want %q", in, got, want)
		}
	}
}

// TestNormalizeRequestedProvider is the codex round 2 (#2234), P2
// regression proper: unlike normalizeProvider, this function must NEVER
// substitute an empty (or whitespace-only) value to "auto" -- every
// production caller passes an ALREADY-RESOLVED categorize.ProviderKind,
// where an empty value means "the caller explicitly requested an
// invalid/malformed provider," never "no preference, match anything."
func TestNormalizeRequestedProvider(t *testing.T) {
	cases := map[string]string{
		"":           "",
		"   ":        "",
		"OpenAI":     "openai",
		"  Ollama  ": "ollama",
		"auto":       "auto",
	}
	for in, want := range cases {
		if got := normalizeRequestedProvider(in); got != want {
			t.Errorf("normalizeRequestedProvider(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestIsKnownProvider(t *testing.T) {
	for _, p := range []string{
		"openai", "anthropic", "gemini", "qwen", "local", "ollama",
		"lmstudio", "qwen-local", "qwen-lmstudio",
	} {
		if !isKnownProvider(p) {
			t.Errorf("isKnownProvider(%q) = false, want true", p)
		}
	}
	for _, p := range []string{"auto", "mock", "none", "bogus", ""} {
		if isKnownProvider(p) {
			t.Errorf("isKnownProvider(%q) = true, want false", p)
		}
	}
}

func TestCredentialsComplete(t *testing.T) {
	cases := []struct {
		provider string
		apiKey   string
		want     bool
	}{
		{"openai", "", false},
		{"openai", "sk-x", true},
		{"anthropic", "", false},
		{"gemini", "", false},
		{"qwen", "", false},
		{"local", "", true},
		{"ollama", "", true},
		{"lmstudio", "", true},
	}
	for _, tc := range cases {
		if got := credentialsComplete(tc.provider, tc.apiKey); got != tc.want {
			t.Errorf("credentialsComplete(%q, %q) = %v, want %v",
				tc.provider, tc.apiKey, got, tc.want)
		}
	}
}
