package devhealth_test

import (
	"strings"
	"testing"
)

const trustedEnvName = "AUTH_SERVICE_TRUSTED_PROXIES"

var authTrusted = []string{"--set-json", `authService.trustedProxies=["10.8.0.0/16","10.9.0.5/32"]`}

func envValue(container any, name string) (string, bool) {
	for _, e := range aList(at(container, "env")) {
		if at(e, "name") == name {
			return aStr(at(e, "value")), true
		}
	}
	return "", false
}

// With the go-api auth proxy on, auth-service believes the one forwarded address only from the listed peers, and
// admits no pod but go-api's: trust without that boundary would let any caller name its own address.
func TestAuthServiceTrustedProxiesRenderWithTheProxyOnAndTheBoundary(t *testing.T) {
	docs, out, err := renderAuth(t, goAPIWithAuth("--set", "goApi.authProxy.sessions=true")...)
	if err != nil {
		t.Fatalf("render: %v\n%.500s", err, out)
	}
	container := at(authDoc(docs, "Deployment", "a-dev-health-auth-service"), "spec", "template", "spec", "containers", 0)
	if got, ok := envValue(container, trustedEnvName); !ok || got != "10.8.0.0/16,10.9.0.5/32" {
		t.Fatalf("%s = %q (set %v), want the listed networks", trustedEnvName, got, ok)
	}
	if from := authServiceAPIFrom(t, docs); len(from) == 0 {
		t.Fatal("the proxy is on and trust is set, but the auth-service NetworkPolicy admits no pod on the API port")
	}
}

func TestAuthServiceProxyOnWithNoTrustedProxiesFailsTheRender(t *testing.T) {
	for _, flag := range []string{"sessions", "localLogin", "credentials", "social", "oidc", "saml"} {
		args := append(append([]string{"--set", "goApi.enabled=true"}, authBase...), sessionReaders...)
		args = append(args, "--set", "authService.sessionKeys.successor.secretName=auth-session")
		args = append(args, authValkey...)
		args = append(args, "--set", "goApi.authProxy.sessions=true", "--set", "goApi.authProxy."+flag+"=true")
		_, out, err := renderAuth(t, args...)
		if err == nil || !strings.Contains(out, "authService.trustedProxies is required") {
			t.Errorf("proxy flag %s with no trusted proxies: err=%v\n%.400s", flag, err, out)
		}
	}
}

func TestAuthServiceProxyOffRendersWithoutTrustedProxies(t *testing.T) {
	args := append(append([]string{"--set", "goApi.enabled=true"}, authBase...), sessionReaders...)
	docs, out, err := renderAuth(t, append(args, sessionSuccessor...)...)
	if err != nil {
		t.Fatalf("render: %v\n%.500s", err, out)
	}
	container := at(authDoc(docs, "Deployment", "a-dev-health-auth-service"), "spec", "template", "spec", "containers", 0)
	if _, ok := envValue(container, trustedEnvName); ok {
		t.Errorf("%s is set with the proxy off and no list", trustedEnvName)
	}
}

func TestAuthServiceTrustedProxiesRefuseTheWholeInternet(t *testing.T) {
	for _, entry := range []string{"0.0.0.0/0", "::/0", "::ffff:0.0.0.0/96", "::ffff:10.0.0.0/104", "10.0.0.0/8, 0.0.0.0/0", "*"} {
		args := append(goAPIWithAuth("--set", "goApi.authProxy.sessions=true"), "--set-json", `authService.trustedProxies=["`+entry+`"]`)
		_, out, err := renderAuth(t, args...)
		if err == nil || !strings.Contains(out, "authService.trustedProxies") {
			t.Errorf("entry %q rendered or named the wrong key: err=%v\n%.300s", entry, err, out)
		}
	}
}

func TestAuthServiceExtraEnvCannotSetTheTrustedProxies(t *testing.T) {
	args := append(append([]string{}, authBase...), "--set-json", `authService.extraEnv=[{"name":"`+trustedEnvName+`","value":"0.0.0.0/0"}]`)
	if _, out, err := renderAuth(t, args...); err == nil || !strings.Contains(out, trustedEnvName) {
		t.Errorf("extraEnv set the trusted proxies: err=%v\n%.300s", err, out)
	}
}

// Both ways on the switch that turns the session routes on (authService.sessionKeys.successor, which renders
// AUTH_JWT_SECRET_KEY and AUTH_SESSION_SUCCESSOR_KEY together, the two secrets authconfig's Session.Enabled needs): on
// with no store = fail, off = renders.
func TestAuthServiceStoreIsRequiredExactlyWhenTheSessionRoutesAreOn(t *testing.T) {
	_, out, err := renderAuth(t, append(append([]string{}, authBase...), "--set", "authService.sessionKeys.successor.secretName=auth-session")...)
	if err == nil || !strings.Contains(out, "authService.valkey.secretName is required") {
		t.Errorf("session routes on without the store: err=%v\n%.300s", err, out)
	}
	docs, out, err := renderAuth(t, authBase...)
	if err != nil {
		t.Fatalf("session routes off: %v\n%.300s", err, out)
	}
	container := at(authDoc(docs, "Deployment", "a-dev-health-auth-service"), "spec", "template", "spec", "containers", 0)
	if envNames(container)["AUTH_SESSION_SUCCESSOR_KEY"] || envNames(container)["AUTH_JWT_SECRET_KEY"] {
		t.Error("the session secrets render although the switch is off")
	}
}
