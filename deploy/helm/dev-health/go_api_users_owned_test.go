package devhealth_test

import (
	"strings"
	"testing"
)

var goAPIProxyOn = []string{
	"--set", "goApi.authProxy.sessions=true", "--set", "goApi.authProxy.localLogin=true", "--set", "goApi.authProxy.credentials=true",
}

// go-api reads the same AUTH_USERS_OWNED as auth-service, from the one value
// authService.usersOwned. Off, nothing is rendered.
func TestGoAPIGetsTheUsersOwnedSwitchFromTheAuthServiceValue(t *testing.T) {
	docs, out, err := renderAuth(t, goAPIWithAuth()...)
	if err != nil {
		t.Fatalf("render: %v\n%.400s", err, out)
	}
	if value, set := containerEnvValue(t, authDoc(docs, "Deployment", "a-dev-health-go-api"), "AUTH_USERS_OWNED"); set {
		t.Errorf("the default render sets go-api AUTH_USERS_OWNED=%q; want it unset", value)
	}
	args := append(append(goAPIWithAuth("--set", "authService.usersOwned=true"), goAPIProxyOn...))
	docs, out, err = renderAuth(t, args...)
	if err != nil {
		t.Fatalf("render with the switch on: %v\n%.400s", err, out)
	}
	if value, _ := containerEnvValue(t, authDoc(docs, "Deployment", "a-dev-health-go-api"), "AUTH_USERS_OWNED"); value != "true" {
		t.Errorf("go-api AUTH_USERS_OWNED = %q, want true", value)
	}
}

// The render refuses what go-api would refuse at boot: the switch on without
// the three proxy aggregates that move the user-writing routes.
func TestUsersOwnedNeedsTheGoAPIProxyAggregates(t *testing.T) {
	for _, missing := range []string{"localLogin", "credentials"} {
		args := goAPIWithAuth("--set", "authService.usersOwned=true", "--set", "goApi.authProxy.sessions=true")
		for _, flag := range []string{"localLogin", "credentials"} {
			if flag != missing {
				args = append(args, "--set", "goApi.authProxy."+flag+"=true")
			}
		}
		_, out, err := renderAuth(t, args...)
		if err == nil || !strings.Contains(out, "goApi.authProxy."+missing) {
			t.Errorf("without %s: err=%v out=%.300s, want a refusal naming goApi.authProxy.%s", missing, err, out, missing)
		}
	}
	_, out, err := renderAuth(t, goAPIWithAuth("--set", "authService.usersOwned=true")...)
	if err == nil || !strings.Contains(out, "goApi.authProxy") {
		t.Errorf("no proxy flag at all: err=%v out=%.300s, want a refusal", err, out)
	}
}

// The switch has one source: go-api's extraEnv cannot set it.
func TestGoAPIUsersOwnedCannotBeSetThroughExtraEnv(t *testing.T) {
	_, out, err := renderAuth(t, goAPIWithAuth("--set-json", `goApi.extraEnv=[{"name":"AUTH_USERS_OWNED","value":"true"}]`)...)
	if err == nil || !strings.Contains(out, "authService.usersOwned") {
		t.Fatalf("extraEnv AUTH_USERS_OWNED rendered: err=%v out=%.300s", err, out)
	}
}
