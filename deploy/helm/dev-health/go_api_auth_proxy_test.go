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

func TestGoAPIAuthProxyRendersOnlyTheFlagsThatAreOn(t *testing.T) {
	docs, out, err := renderAuth(t, goAPIWithAuth("--set", "goApi.authProxy.sessions=true", "--set", "goApi.authProxy.saml=true")...)
	if err != nil {
		t.Fatalf("render failed: %v\n%.400s", err, out)
	}
	want := map[string]string{
		"AUTH_PROXY_URL": "http://a-dev-health-auth-service:8095", "AUTH_PROXY_SESSIONS": "true", "AUTH_PROXY_SAML": "true",
	}
	got := proxyEnv(t, docs)
	if len(got) != len(want) {
		t.Fatalf("AUTH_PROXY_* = %v, want %v", got, want)
	}
	for name, value := range want {
		if got[name] != value {
			t.Errorf("%s = %q, want %q", name, got[name], value)
		}
	}
	from := authServiceAPIFrom(t, docs)
	if len(from) != 1 || at(from[0], "podSelector", "matchLabels", "app.kubernetes.io/component") != "go-api" {
		t.Fatalf("auth-service API port admits %v, want only the go-api pods", from)
	}
	for flag, env := range authProxyFlags {
		args := goAPIWithAuth("--set", "goApi.authProxy.sessions=true", "--set", "goApi.authProxy."+flag+"=true")
		docs, out, err := renderAuth(t, args...)
		if err != nil {
			t.Fatalf("%s: render failed: %v\n%.400s", flag, err, out)
		}
		if proxyEnv(t, docs)[env] != "true" {
			t.Errorf("goApi.authProxy.%s does not render %s", flag, env)
		}
	}
}

func TestGoAPIAuthProxyRefusals(t *testing.T) {
	for name, tc := range map[string]struct {
		args []string
		want string
	}{
		"no auth-service": {[]string{"--set", "goApi.enabled=true", "--set", "goApi.authProxy.sessions=true"}, "goApi.authProxy flags need authService.enabled"},
		"login without sessions": {goAPIWithAuth("--set", "goApi.authProxy.localLogin=true"),
			"goApi.authProxy.sessions must be on"},
		"extraEnv flag": {goAPIWithAuth("--set", "goApi.extraEnv[0].name=AUTH_PROXY_SESSIONS", "--set", "goApi.extraEnv[0].value=true"),
			"goApi.extraEnv must not set AUTH_PROXY_*"},
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

// A flag is a YAML boolean: a string, "false" included, is refused, never
// read as on.
func TestGoAPIAuthProxyRefusesNonBooleanFlags(t *testing.T) {
	for flag := range authProxyFlags {
		for _, value := range []string{"false", "true", "no"} {
			docs, out, err := renderAuth(t, goAPIWithAuth("--set", "goApi.authProxy.sessions=true", "--set-string", "goApi.authProxy."+flag+"="+value)...)
			if err == nil {
				t.Errorf("goApi.authProxy.%s=%q (a string) rendered %v; it must be refused", flag, value, proxyEnv(t, docs))
				continue
			}
			if !strings.Contains(out, "authProxy") {
				t.Errorf("goApi.authProxy.%s=%q: the refusal does not name the field: %.300s", flag, value, out)
			}
		}
	}
}
