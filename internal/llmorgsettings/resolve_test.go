package llmorgsettings

import "testing"

func TestNormalizeProvider(t *testing.T) {
	cases := map[string]string{
		"":           "auto",
		"   ":        "auto",
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
