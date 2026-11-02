package devhealth_test

import (
	"strings"
	"testing"
)

const ssoJITEnv = "AUTH_SSO_JIT"

// Default and false render the switch nowhere.
func TestSSOJITOffRendersNothing(t *testing.T) {
	for _, extra := range [][]string{nil, {"--set", "authService.ssoJit=false"}} {
		docs, out, err := renderAuth(t, phaseArgs(4, extra...)...)
		if err != nil {
			t.Fatalf("render: %v\n%.400s", err, out)
		}
		doc := authDoc(docs, "Deployment", "a-dev-health-auth-service")
		if doc == nil {
			t.Fatal("no auth-service Deployment in the render")
		}
		if value, set := containerEnvValue(t, doc, ssoJITEnv); set {
			t.Errorf("auth-service sets %s=%q; want it unset", ssoJITEnv, value)
		}
	}
}

// True needs orgs owned: every phase below 5 refuses it.
func TestSSOJITTrueNeedsOrgsOwned(t *testing.T) {
	const on = "authService.ssoJit=true"
	for name, args := range map[string][]string{
		"phase 2":       phaseArgs(2, "--set", on),
		"phase 3":       phaseArgs(3, "--set", on),
		"phase 4 goApi": phaseArgs(4, "--set", "authService.usersOwnedStage=goApi", "--set", on),
		"phase 4 owned": phaseArgs(4, "--set", on),
	} {
		_, out, err := renderAuth(t, args...)
		if err == nil || !strings.Contains(out, "authService.ssoJit needs orgs owned") {
			t.Errorf("%s: rendered: err=%v out=%.300s", name, err, out)
		}
	}
}

// The check runs on every render, so it holds when auth-service does not render.
func TestSSOJITIsCheckedWithNoConsumerRendered(t *testing.T) {
	base := []string{"--set", "web.enabled=false", "--set", "authService.phase=0", "--set", "goApi.enabled=false", "--set", "queryApi.enabled=false"}
	for name, set := range map[string][]string{
		"true":     {"--set", "authService.ssoJit=true"},
		"null":     {"--set-json", "authService.ssoJit=null"},
		"a string": {"--set", "authService.ssoJit=yes"},
	} {
		_, out, err := renderAuth(t, append(append([]string{}, base...), set...)...)
		if err == nil || !strings.Contains(out, "ssoJit") {
			t.Errorf("%s: rendered with no consumer: err=%v out=%.300s", name, err, out)
		}
	}
	if _, out, err := renderAuth(t, base...); err != nil {
		t.Fatalf("the default render with no consumer failed: %v\n%.300s", err, out)
	}
}

// The switch has one source: extraEnv cannot set it past the guard.
func TestSSOJITCannotBeSetThroughExtraEnv(t *testing.T) {
	_, out, err := renderAuth(t, phaseArgs(4, "--set-json", `authService.extraEnv=[{"name":"AUTH_SSO_JIT","value":"true"}]`)...)
	if err == nil || !strings.Contains(out, "authService.extraEnv must not set AUTH_SSO_JIT") {
		t.Errorf("extraEnv rendered: err=%v out=%.300s", err, out)
	}
}
