package authconfig

import (
	"strings"
	"testing"
)

func oauthLookup(values map[string]string) func(string) (string, bool) {
	return func(key string) (string, bool) {
		value, ok := values[key]
		return value, ok
	}
}

// Unset is valid and refuses at request time, so a dark deployment starts (D6465). A
// value of the wrong form is an operator error.
func TestOAuthSettings(t *testing.T) {
	var cfg Config
	if err := oauthSettings(oauthLookup(nil), &cfg); err != nil || cfg.OAuthWebOrigin != "" || cfg.OAuthResourceACRAPI != "" {
		t.Fatalf("unset: %v, %+v", err, cfg)
	}
	good := map[string]string{
		EnvOAuthResourceACRAPI: "https://acr.example.test",
		EnvOAuthResourceACRMCP: "https://mcp.example.test/mcp",
		EnvOAuthWebOrigin:      "https://www.example.test",
	}
	if err := oauthSettings(oauthLookup(good), &cfg); err != nil {
		t.Fatalf("positive control: %v", err)
	}
	if cfg.OAuthResourceACRMCP != "https://mcp.example.test/mcp" || cfg.OAuthWebOrigin != "https://www.example.test" || cfg.OAuthResourceACRAPI != "https://acr.example.test" {
		t.Fatalf("%+v", cfg)
	}
	for _, tc := range []struct{ name, value string }{
		{EnvOAuthWebOrigin, "http://www.example.test"},
		{EnvOAuthWebOrigin, "https://www.example.test/"},
		{EnvOAuthWebOrigin, "https://www.example.test/path"},
		{EnvOAuthWebOrigin, "https://user@www.example.test"},
		{EnvOAuthWebOrigin, "https://www.example.test?x=1"},
		{EnvOAuthWebOrigin, "https://WWW.example.test"},
		{EnvOAuthResourceACRAPI, "http://acr.example.test"},
		{EnvOAuthResourceACRAPI, "https://acr.example.test/"},
		{EnvOAuthResourceACRAPI, "https://acr.example.test/a?x=1"},
		{EnvOAuthResourceACRAPI, "https://acr.example.test/a#f"},
		{EnvOAuthResourceACRMCP, "acr.example.test"},
	} {
		var into Config
		err := oauthSettings(oauthLookup(map[string]string{tc.name: tc.value}), &into)
		if err == nil || !strings.Contains(err.Error(), tc.name) {
			t.Errorf("%s=%q: %v", tc.name, tc.value, err)
		}
		if strings.Contains(errText(err), tc.value) {
			t.Errorf("%s: the refusal echoes the value", tc.name)
		}
	}
}

func errText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
