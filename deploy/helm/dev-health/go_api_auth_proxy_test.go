package devhealth_test

import (
	"strings"
	"testing"
)

// CHAOS-10231: the go-api /api/v1/auth/* proxy flags. Every flag is off by
// default, so the default render carries no AUTH_PROXY_* variable and
// auth-service admits no go-api pod.

var authProxyFlags = map[string]string{
	"sessions": "AUTH_PROXY_SESSIONS", "localLogin": "AUTH_PROXY_LOCAL_LOGIN", "credentials": "AUTH_PROXY_CREDENTIALS",
	"social": "AUTH_PROXY_SOCIAL", "oidc": "AUTH_PROXY_OIDC", "saml": "AUTH_PROXY_SAML",
}

func goAPIWithAuth(sets ...string) []string {
	base := append(append([]string{"--set", "goApi.enabled=true"}, authBase...), sessionReaders...)
	return append(append(base, sessionSuccessor...), sets...)
}

func proxyEnv(t *testing.T, docs []map[string]any) map[string]string {
	t.Helper()
	deployment := authDoc(docs, "Deployment", "a-dev-health-go-api")
	if deployment == nil {
		t.Fatal("no go-api Deployment in the render")
	}
	out := map[string]string{}
	for _, container := range at(deployment, "spec", "template", "spec", "containers").([]any) {
		env, _ := at(container, "env").([]any)
		for _, entry := range env {
			name, _ := at(entry, "name").(string)
			if strings.HasPrefix(name, "AUTH_PROXY_") {
				value, _ := at(entry, "value").(string)
				out[name] = value
			}
		}
	}
	return out
}

func authServiceAPIFrom(t *testing.T, docs []map[string]any) []any {
	t.Helper()
	policy := authDoc(docs, "NetworkPolicy", "a-dev-health-auth-service")
	if policy == nil {
		t.Fatal("no auth-service NetworkPolicy in the render")
	}
	ingress, _ := at(policy, "spec", "ingress").([]any)
	for _, rule := range ingress {
		ports, _ := at(rule, "ports").([]any)
		for _, port := range ports {
			if at(port, "port") == 8095 {
				from, _ := at(rule, "from").([]any)
				return from
			}
		}
	}
	return nil
}

func TestGoAPIAuthProxyIsOffByDefault(t *testing.T) {
	for name, args := range map[string][]string{
		"go-api alone":               {"--set", "goApi.enabled=true"},
		"go-api beside auth-service": goAPIWithAuth(),
	} {
		docs, out, err := renderAuth(t, args...)
		if err != nil {
			t.Fatalf("%s: render failed: %v\n%.400s", name, err, out)
		}
		if env := proxyEnv(t, docs); len(env) != 0 {
			t.Errorf("%s: the default render sets %v", name, env)
		}
		if strings.Contains(name, "auth-service") {
			if from := authServiceAPIFrom(t, docs); len(from) != 0 {
				t.Errorf("%s: auth-service admits %v with every proxy flag off", name, from)
			}
		}
	}
}

func TestGoAPIAuthProxyRendersOnlyTheFlagsThePhaseTurnsOn(t *testing.T) {
	for phase, want := range map[string]map[string]string{
		"2": {"AUTH_PROXY_URL": "http://a-dev-health-auth-service:8095", "AUTH_PROXY_SESSIONS": "true"},
		"3": {"AUTH_PROXY_URL": "http://a-dev-health-auth-service:8095", "AUTH_PROXY_SESSIONS": "true",
			"AUTH_PROXY_LOCAL_LOGIN": "true", "AUTH_PROXY_SOCIAL": "true"},
	} {
		docs, out, err := renderAuth(t, goAPIWithAuth(append([]string{"--set", "authService.phase=" + phase}, phase3Secrets...)...)...)
		if err != nil {
			t.Fatalf("phase %s: render failed: %v\n%.400s", phase, err, out)
		}
		got := proxyEnv(t, docs)
		if len(got) != len(want) {
			t.Fatalf("phase %s: AUTH_PROXY_* = %v, want %v", phase, got, want)
		}
		for name, value := range want {
			if got[name] != value {
				t.Errorf("phase %s: %s = %q, want %q", phase, name, got[name], value)
			}
		}
		from := authServiceAPIFrom(t, docs)
		if len(from) != 1 || at(from[0], "podSelector", "matchLabels", "app.kubernetes.io/component") != "go-api" {
			t.Fatalf("phase %s: auth-service API port admits %v, want only the go-api pods", phase, from)
		}
	}
}

func TestGoAPIAuthProxyRefusals(t *testing.T) {
	for name, tc := range map[string]struct {
		args []string
		want string
	}{
		"extraEnv flag": {goAPIWithAuth("--set", "goApi.extraEnv[0].name=AUTH_PROXY_SESSIONS", "--set", "goApi.extraEnv[0].value=true"),
			"goApi.extraEnv must not set AUTH_PROXY_*"},
		"extraEnv flag, lower case": {goAPIWithAuth("--set", "goApi.extraEnv[0].name=auth_proxy_sessions", "--set", "goApi.extraEnv[0].value=true"),
			"goApi.extraEnv must not set AUTH_PROXY_*"},
		"extraEnv issuer": {goAPIWithAuth("--set", "goApi.extraEnv[0].name=AUTH_ACCESS_TOKEN_ISSUER", "--set", "goApi.extraEnv[0].value=https://x.example"),
			"goApi.extraEnv must not set AUTH_ACCESS_TOKEN_ISSUER"},
	} {
		t.Run(name, func(t *testing.T) {
			_, out, err := renderAuth(t, tc.args...)
			if err == nil {
				t.Fatal("the render succeeded; it must refuse")
			}
			if !strings.Contains(out, tc.want) {
				t.Fatalf("the refusal does not say %q: %.400s", tc.want, out)
			}
		})
	}
}

// A hand-set goApi.authProxy flag is refused whatever its value or type: authService.phase owns every flag.
func TestGoAPIAuthProxyRefusesEveryHandSetFlag(t *testing.T) {
	for flag := range authProxyFlags {
		for _, set := range [][]string{
			{"--set", "goApi.authProxy." + flag + "=true"}, {"--set", "goApi.authProxy." + flag + "=false"},
			{"--set-string", "goApi.authProxy." + flag + "=false"}, {"--set-string", "goApi.authProxy." + flag + "=no"},
		} {
			docs, out, err := renderAuth(t, goAPIWithAuth(append([]string{"--set", "authService.phase=2"}, set...)...)...)
			if err == nil {
				t.Errorf("%v rendered %v; it must be refused", set, proxyEnv(t, docs))
				continue
			}
			if !strings.Contains(out, "goApi.authProxy is replaced by authService.phase") {
				t.Errorf("%v: the refusal does not name the replacement: %.300s", set, out)
			}
		}
	}
}
