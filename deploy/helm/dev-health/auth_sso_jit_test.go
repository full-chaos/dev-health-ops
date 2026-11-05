package devhealth_test

import (
	"os"
	"path/filepath"
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

// The env line is pinned by text as well as by render: only auth-service
// carries it, behind the one read of the switch.
func TestSSOJITLineSitsOnAuthServiceOnly(t *testing.T) {
	want := "{{- if eq (include \"dev-health.ssoJit\" .) \"true\" }}\n            - name: AUTH_SSO_JIT\n              value: \"true\"\n"
	entries, err := filepath.Glob("templates/*")
	if err != nil {
		t.Fatal(err)
	}
	carriers := 0
	for _, path := range entries {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(raw), "- name: AUTH_SSO_JIT") {
			carriers++
			if filepath.Base(path) != "auth-service.yaml" || !strings.Contains(string(raw), want) {
				t.Errorf("%s carries the env line in another form", path)
			}
		}
	}
	if carriers != 1 {
		t.Fatalf("%d templates carry the env line, want 1", carriers)
	}
}

// ssoJit=true renders only where users are owned: phase 4 with usersOwnedStage=owned.
func TestSSOJITRendersAtPhase4Owned(t *testing.T) {
	docs, out, err := renderAuth(t, phaseArgs(4, "--set", "authService.ssoJit=true")...)
	if err != nil {
		t.Fatalf("phase 4 owned: %v\n%.400s", err, out)
	}
	if value, set := containerEnvValue(t, authDoc(docs, "Deployment", "a-dev-health-auth-service"), ssoJITEnv); !set || value != "true" {
		t.Errorf("auth-service %s=%q set=%v; want true", ssoJITEnv, value, set)
	}
	for _, target := range []struct{ kind, name string }{
		{"Deployment", "a-dev-health-go-api"},
		{"Deployment", "a-dev-health-query-api"},
		{"Job", "a-dev-health-auth-migrate"},
		{"Job", "a-dev-health-auth-grants"},
		{"Job", "a-dev-health-migrate"},
	} {
		doc := authDoc(docs, target.kind, target.name)
		if doc == nil {
			continue
		}
		if value, set := containerEnvValue(t, doc, ssoJITEnv); set {
			t.Errorf("%s sets %s=%q; want it on auth-service only", target.name, ssoJITEnv, value)
		}
	}
}

// Below users owned the render is refused and the text names the stage that allows it.
func TestSSOJITRefusedBelowOwned(t *testing.T) {
	const on = "authService.ssoJit=true"
	for name, args := range map[string][]string{
		"phase 2":       phaseArgs(2, "--set", on),
		"phase 3":       phaseArgs(3, "--set", on),
		"phase 4 goApi": phaseArgs(4, "--set", "authService.usersOwnedStage=goApi", "--set", on),
	} {
		_, out, err := renderAuth(t, args...)
		if err == nil || !strings.Contains(out, "authService.usersOwnedStage=owned") {
			t.Errorf("%s: rendered or wrong refusal: err=%v out=%.300s", name, err, out)
		}
	}
}

// One writer: whenever the sign-in join runs in auth-service, go-api carries AUTH_USERS_OWNED, and its own join and
// account create refuse on it (internal/api/sso/provision.go createAccountAndMembership and membershipOrJoin).
func TestSSOJITGoApiSingleWriter(t *testing.T) {
	docs, out, err := renderAuth(t, phaseArgs(4, "--set", "authService.ssoJit=true")...)
	if err != nil {
		t.Fatalf("render: %v\n%.400s", err, out)
	}
	if value, set := containerEnvValue(t, authDoc(docs, "Deployment", "a-dev-health-go-api"), "AUTH_USERS_OWNED"); !set || value != "true" {
		t.Errorf("go-api AUTH_USERS_OWNED=%q set=%v with ssoJit on; want true", value, set)
	}
	if value, set := containerEnvValue(t, authDoc(docs, "Deployment", "a-dev-health-auth-service"), "AUTH_USERS_OWNED"); !set || value != "true" {
		t.Errorf("auth-service AUTH_USERS_OWNED=%q set=%v with ssoJit on; want true", value, set)
	}
}
