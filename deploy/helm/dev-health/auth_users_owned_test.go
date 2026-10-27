package devhealth_test

import (
	"strings"
	"testing"
)

// AUTH_USERS_OWNED is rendered from authService.phase only, from phase 4 up (D6358), so below phase 4 (the phase 3
// rollback render included) it reaches no consumer.
func TestUsersOwnedSwitchIsUnsetBelowPhase4(t *testing.T) {
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

// Phase 4 (users owned): auth-service, the auth-migrate and auth-grants hooks and the auth-backfill Job all carry
// AUTH_USERS_OWNED=true, and none carries AUTH_ORGS_OWNED.
func TestUsersOwnedRendersAtPhase4(t *testing.T) {
	docs, out, err := renderAuth(t, goAPIWithAuth(append([]string{"--set", "authService.phase=4"}, phase3Secrets...)...)...)
	if err != nil {
		t.Fatalf("phase 4: render: %v\n%.400s", err, out)
	}
	targets := []struct{ kind, name string }{
		{"Deployment", "a-dev-health-auth-service"},
		{"Job", "a-dev-health-auth-migrate"},
		{"Job", "a-dev-health-auth-grants"},
	}
	for _, target := range targets {
		doc := authDoc(docs, target.kind, target.name)
		if doc == nil {
			t.Fatalf("phase 4: no %s %s", target.kind, target.name)
		}
		if value, set := containerEnvValue(t, doc, "AUTH_USERS_OWNED"); !set || value != "true" {
			t.Errorf("phase 4: %s AUTH_USERS_OWNED=%q set=%v; want true", target.name, value, set)
		}
		if value, set := containerEnvValue(t, doc, "AUTH_ORGS_OWNED"); set {
			t.Errorf("phase 4: %s sets AUTH_ORGS_OWNED=%q; want it unset", target.name, value)
		}
	}
}

func TestUsersOwnedRendersOnBackfillJobAtPhase4(t *testing.T) {
	args := withSets(append(append([]string{}, backfillOn...), phase3Secrets...), "authService.backfill.verb=users-backfill", "authService.phase=4")
	args = append(append(args, sessionReaders...), sessionSuccessor...)
	_, job := renderBackfill(t, args)
	if value, set := containerEnvValue(t, job, "AUTH_USERS_OWNED"); !set || value != "true" {
		t.Errorf("phase 4: auth-backfill Job AUTH_USERS_OWNED=%q set=%v; want true", value, set)
	}
	if value, set := containerEnvValue(t, job, "AUTH_ORGS_OWNED"); set {
		t.Errorf("phase 4: auth-backfill Job sets AUTH_ORGS_OWNED=%q; want it unset", value)
	}
}
