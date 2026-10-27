package devhealth_test

import (
	"strings"
	"testing"
)

// go-api reads AUTH_USERS_OWNED from authService.phase only, from phase 4 up; below it (the phase 3 rollback render
// included) the switch is unset and the credential routes are not forwarded.
func TestGoAPIUsersOwnedSwitchIsUnsetBelowPhase4(t *testing.T) {
	for _, phase := range []string{"0", "1", "2", "3"} {
		docs, out, err := renderAuth(t, goAPIWithAuth(append([]string{"--set", "authService.phase=" + phase}, phase3Secrets...)...)...)
		if err != nil {
			t.Fatalf("phase %s: render: %v\n%.400s", phase, err, out)
		}
		if value, set := containerEnvValue(t, authDoc(docs, "Deployment", "a-dev-health-go-api"), "AUTH_USERS_OWNED"); set {
			t.Errorf("phase %s: go-api AUTH_USERS_OWNED=%q; want it unset", phase, value)
		}
		if got := proxyEnv(t, docs)["AUTH_PROXY_CREDENTIALS"]; got == "true" {
			t.Errorf("phase %s: go-api AUTH_PROXY_CREDENTIALS=true; want it off below phase 4", phase)
		}
	}
}

// The switch has one source: go-api's extraEnv cannot set it.
func TestGoAPIUsersOwnedCannotBeSetThroughExtraEnv(t *testing.T) {
	_, out, err := renderAuth(t, goAPIWithAuth("--set-json", `goApi.extraEnv=[{"name":"AUTH_USERS_OWNED","value":"true"}]`)...)
	if err == nil || !strings.Contains(out, "goApi.extraEnv must not set AUTH_USERS_OWNED") {
		t.Fatalf("extraEnv AUTH_USERS_OWNED rendered: err=%v out=%.300s", err, out)
	}
}

// Phase 4: go-api owns the users switch and forwards the credential routes.
func TestGoAPIUsersOwnedAndCredentialsProxyRenderAtPhase4(t *testing.T) {
	docs, out, err := renderAuth(t, goAPIWithAuth(append([]string{"--set", "authService.phase=4"}, phase3Secrets...)...)...)
	if err != nil {
		t.Fatalf("phase 4: render: %v\n%.400s", err, out)
	}
	if value, set := containerEnvValue(t, authDoc(docs, "Deployment", "a-dev-health-go-api"), "AUTH_USERS_OWNED"); !set || value != "true" {
		t.Errorf("phase 4: go-api AUTH_USERS_OWNED=%q set=%v; want true", value, set)
	}
	if got := proxyEnv(t, docs)["AUTH_PROXY_CREDENTIALS"]; got != "true" {
		t.Errorf("phase 4: go-api AUTH_PROXY_CREDENTIALS=%q; want true", got)
	}
}
