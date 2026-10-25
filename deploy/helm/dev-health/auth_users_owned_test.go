package devhealth_test

import (
	"strings"
	"testing"
)

// AUTH_USERS_OWNED is rendered from authService.phase only (phase 4, refused until the public-table writer decision),
// so in every phase this chart renders it reaches no consumer.
func TestUsersOwnedSwitchIsUnsetInEveryRenderedPhase(t *testing.T) {
	targets := []struct{ kind, name string }{
		{"Deployment", "a-dev-health-auth-service"},
		{"Job", "a-dev-health-auth-migrate"},
		{"Job", "a-dev-health-auth-grants"},
	}
	for _, phase := range []string{"1", "2", "3"} {
		docs, out, err := renderAuth(t, goAPIWithAuth(append([]string{"--set", "authService.phase=" + phase}, phase3Secrets...)...)...)
		if err != nil {
			t.Fatalf("phase %s: render: %v\n%.400s", phase, err, out)
		}
		for _, target := range targets {
			doc := authDoc(docs, target.kind, target.name)
			if doc == nil {
				t.Fatalf("phase %s: no %s %s", phase, target.kind, target.name)
			}
			if value, set := containerEnvValue(t, doc, "AUTH_USERS_OWNED"); set {
				t.Errorf("phase %s: %s sets AUTH_USERS_OWNED=%q; want it unset", phase, target.name, value)
			}
		}
	}
}

// The switch has one source: extraEnv cannot set it, or the Jobs and the
// service could disagree.
func TestUsersOwnedCannotBeSetThroughExtraEnv(t *testing.T) {
	_, out, err := renderAuth(t, append(append([]string{}, authBase...),
		"--set-json", `authService.extraEnv=[{"name":"AUTH_USERS_OWNED","value":"true"}]`)...)
	if err == nil || !strings.Contains(out, "authService.extraEnv must not set AUTH_USERS_OWNED") {
		t.Fatalf("extraEnv AUTH_USERS_OWNED rendered: err=%v out=%.300s", err, out)
	}
}
