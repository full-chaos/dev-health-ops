package devhealth_test

import "testing"

// At phase 5 the auth-migrate and auth-grants Jobs carry AUTH_ORGS_OWNED=true, byte-equal to auth-service's, so the
// grants pass gives the runtime role the organization write grant the service needs; at phase 4 none carries it.
func TestOrgsOwnedRendersOnTheGrantsAndMigrateJobsAtPhase5(t *testing.T) {
	targets := []struct{ kind, name string }{
		{"Deployment", "a-dev-health-auth-service"},
		{"Job", "a-dev-health-auth-migrate"},
		{"Job", "a-dev-health-auth-grants"},
	}
	for phase, want := range map[int]bool{4: false, 5: true} {
		args := phaseArgs(phase)
		if phase == 5 {
			args = phase5Args()
		}
		docs, out, err := renderAuth(t, args...)
		if err != nil {
			t.Fatalf("phase %d: render: %v\n%.400s", phase, err, out)
		}
		for _, target := range targets {
			doc := authDoc(docs, target.kind, target.name)
			if doc == nil {
				t.Fatalf("phase %d: no %s %s", phase, target.kind, target.name)
			}
			value, set := containerEnvValue(t, doc, "AUTH_ORGS_OWNED")
			if set != want || (want && value != "true") {
				t.Errorf("phase %d: %s AUTH_ORGS_OWNED=%q set=%v; want set=%v (true)", phase, target.name, value, set, want)
			}
		}
	}
}
