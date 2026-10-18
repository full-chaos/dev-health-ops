package llmbudget

import "testing"

func TestPlatformLookupNamesTheTextProviderACallRunsByDefault(t *testing.T) {
	for _, tc := range []struct {
		name string
		env  map[string]string
		want string
	}{
		{"named provider wins over keys", map[string]string{"LLM_PROVIDER": "anthropic", "OPENAI_API_KEY": "k"}, "anthropic"},
		{"unset falls to the key order", map[string]string{"OPENAI_API_KEY": "k", "ANTHROPIC_API_KEY": "k"}, "openai"},
		{"decision backend falls to the keys", map[string]string{"LLM_PROVIDER": "typesafe", "ANTHROPIC_API_KEY": "k"}, "anthropic"},
		{"auto falls to the keys", map[string]string{"LLM_PROVIDER": "auto", "GEMINI_API_KEY": "k"}, "gemini"},
		{"no keys keeps what is set", map[string]string{"LLM_PROVIDER": "typesafe"}, "typesafe"},
		{"no keys, nothing set", map[string]string{}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			lookup := PlatformLookup(func(name string) (string, bool) { v, ok := tc.env[name]; return v, ok })
			if got, _ := lookup("LLM_PROVIDER"); got != tc.want {
				t.Fatalf("LLM_PROVIDER = %q, want %q", got, tc.want)
			}
			if tc.env["OPENAI_API_KEY"] != "" {
				if got, ok := lookup("OPENAI_API_KEY"); !ok || got != "k" {
					t.Fatalf("other names pass through, got %q %v", got, ok)
				}
			}
		})
	}
}
