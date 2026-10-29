package devhealth_test

import (
	"strings"
	"testing"
)

// query-api verifies the EdDSA access token with the same issuer value go-api
// does: rendered with class eddsa only, from authService.accessToken.issuerURL.
func TestQueryAPIAccessTokenIssuerRendersWithGoAPIs(t *testing.T) {
	envOf := func(docs []map[string]any, name, env string) (string, bool) {
		return containerEnvValue(t, authDoc(docs, "Deployment", name), env)
	}
	docs := renderPhase(t, 2, "--set", "authService.accessToken.class=hs256_compat")
	if authDoc(docs, "Deployment", "a-dev-health-query-api") == nil {
		t.Fatal("the phase render holds no query-api Deployment: the check would measure nothing")
	}
	if _, set := envOf(docs, "a-dev-health-query-api", "AUTH_ACCESS_TOKEN_ISSUER"); set {
		t.Error("hs256_compat renders query-api AUTH_ACCESS_TOKEN_ISSUER")
	}
	issuer := "https://auth.example/v1"
	docs = renderPhase(t, 2, "--set", "authService.accessToken.class=eddsa", "--set", "authService.accessToken.issuerURL="+issuer)
	for _, name := range []string{"a-dev-health-go-api", "a-dev-health-query-api"} {
		if v, _ := envOf(docs, name, "AUTH_ACCESS_TOKEN_ISSUER"); v != issuer {
			t.Errorf("%s AUTH_ACCESS_TOKEN_ISSUER = %q, want %q", name, v, issuer)
		}
	}
}

func TestQueryAPIExtraEnvRefusesTheAccessTokenIssuer(t *testing.T) {
	for _, name := range []string{"AUTH_ACCESS_TOKEN_ISSUER", "auth_access_token_issuer", " AUTH_ACCESS_TOKEN_ISSUER "} {
		t.Run(name, func(t *testing.T) {
			_, out, err := renderAuth(t, "--set", "queryApi.enabled=true", "--set-json", `queryApi.extraEnv=[{"name":"`+name+`","value":"https://x.example"}]`)
			if err == nil {
				t.Fatal("the render succeeded; it must refuse")
			}
			if want := "queryApi.extraEnv must not set AUTH_ACCESS_TOKEN_ISSUER"; !strings.Contains(out, want) {
				t.Fatalf("the refusal does not say %q: %.400s", want, out)
			}
		})
	}
}
