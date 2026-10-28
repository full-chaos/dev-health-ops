package devhealth_test

import (
	"strings"
	"testing"
)

const usersOwnedLabel = "dev-health.io/auth-users-owned"

func authServicePodLabels(t *testing.T, phase string, sets ...string) map[string]any {
	t.Helper()
	docs, out, err := renderAuth(t, goAPIWithAuth(append(append([]string{"--set", "authService.phase=" + phase}, sets...), phase3Secrets...)...)...)
	if err != nil {
		t.Fatalf("phase %s: render: %v\n%.400s", phase, err, out)
	}
	doc := authDoc(docs, "Deployment", "a-dev-health-auth-service")
	if doc == nil {
		t.Fatalf("phase %s: no auth-service Deployment", phase)
	}
	labels, _ := at(doc, "spec", "template", "metadata", "labels").(map[string]any)
	return labels
}

// The auth-service pods say whether users are owned, so an operator or a probe can tell the owned pods from the
// not-owned pods in a mixed window. The label follows real ownership: stage owned only, never stage goApi.
func TestAuthServicePodCarriesTheUsersOwnedLabelAtStageOwnedOnly(t *testing.T) {
	for _, phase := range []string{"1", "2", "3"} {
		if value, set := authServicePodLabels(t, phase)[usersOwnedLabel]; set {
			t.Errorf("phase %s: pod label %s=%v; want it absent", phase, usersOwnedLabel, value)
		}
	}
	if value, set := authServicePodLabels(t, "4", "--set", "authService.usersOwnedStage=goApi")[usersOwnedLabel]; set {
		t.Errorf("phase 4 stage goApi: pod label %s=%v; want it absent", usersOwnedLabel, value)
	}
	if value := authServicePodLabels(t, "4", "--set", "authService.usersOwnedStage=owned")[usersOwnedLabel]; value != "true" {
		t.Errorf("phase 4 stage owned: pod label %s=%v; want \"true\"", usersOwnedLabel, value)
	}
}

// A flip-back narrowing is not a phase 4 setting at any stage: main's guard (dev-health.authNarrowAfterFlipBackGuard)
// refuses it and names the value, with or without the acknowledgement.
func TestNarrowAfterFlipBackIsRefusedFromPhase4AtEveryStage(t *testing.T) {
	cases := map[string]struct {
		extra []string
		want  string
	}{
		"with the acknowledgement": {
			[]string{"--set", "authService.migrate.acknowledgeGrantNarrowing=" + ackDigest},
			"authService.migrate.narrowAfterFlipBack is refused at authService.phase 4 or higher and while auth-service owns users",
		},
		"without the acknowledgement": {nil, "authService.migrate.narrowAfterFlipBack=true needs authService.migrate.acknowledgeGrantNarrowing"},
	}
	for _, stage := range []string{"goApi", "owned"} {
		for name, tc := range cases {
			args := append([]string{"--set", "authService.phase=4", "--set", "authService.usersOwnedStage=" + stage, "--set", "authService.migrate.narrowAfterFlipBack=true"}, tc.extra...)
			_, out, err := renderAuth(t, goAPIWithAuth(append(args, phase3Secrets...)...)...)
			if err == nil || !strings.Contains(out, tc.want) {
				t.Errorf("stage %s, %s: want a render failure containing %q: err=%v out=%.300s", stage, name, tc.want, err, out)
			}
		}
	}
}
