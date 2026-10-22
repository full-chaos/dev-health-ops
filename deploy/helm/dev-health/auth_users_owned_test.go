package devhealth_test

import (
	"strings"
	"testing"
)

// authService.usersOwned reaches the service and both migration Jobs as
// AUTH_USERS_OWNED=true, so the write grant on public.users follows the
// switch. Off, the variable is not rendered anywhere.
func TestUsersOwnedSwitchReachesTheServiceAndTheGrantJobs(t *testing.T) {
	targets := []struct{ kind, name string }{
		{"Deployment", "a-dev-health-auth-service"},
		{"Job", "a-dev-health-auth-migrate"},
		{"Job", "a-dev-health-auth-grants"},
	}
	docs, out, err := renderAuth(t, authBase...)
	if err != nil {
		t.Fatalf("render: %v\n%.400s", err, out)
	}
	for _, target := range targets {
		if value, set := containerEnvValue(t, authDoc(docs, target.kind, target.name), "AUTH_USERS_OWNED"); set {
			t.Errorf("%s: the default render sets AUTH_USERS_OWNED=%q; want it unset", target.name, value)
		}
	}
	docs, out, err = renderAuth(t, append(append([]string{}, authBase...), "--set", "authService.usersOwned=true")...)
	if err != nil {
		t.Fatalf("render: %v\n%.400s", err, out)
	}
	for _, target := range targets {
		if value, _ := containerEnvValue(t, authDoc(docs, target.kind, target.name), "AUTH_USERS_OWNED"); value != "true" {
			t.Errorf("%s: AUTH_USERS_OWNED = %q, want true", target.name, value)
		}
	}
}

// The switch has one source: extraEnv cannot set it, or the Jobs and the
// service could disagree.
func TestUsersOwnedCannotBeSetThroughExtraEnv(t *testing.T) {
	_, out, err := renderAuth(t, append(append([]string{}, authBase...),
		"--set-json", `authService.extraEnv=[{"name":"AUTH_USERS_OWNED","value":"true"}]`)...)
	if err == nil || !strings.Contains(out, "authService.usersOwned") {
		t.Fatalf("extraEnv AUTH_USERS_OWNED rendered: err=%v out=%.300s", err, out)
	}
}
