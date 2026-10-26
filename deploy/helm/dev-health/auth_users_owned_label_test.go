package devhealth_test

import (
	"strings"
	"testing"
)

const usersOwnedLabel = "dev-health.io/auth-users-owned"

func authServicePodLabels(t *testing.T, phase string) map[string]any {
	t.Helper()
	docs, out, err := renderAuth(t, goAPIWithAuth(append([]string{"--set", "authService.phase=" + phase}, phase3Secrets...)...)...)
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

// The auth-service pods say whether users are owned, so an operator or a probe can tell the phase 4 pods from the
// phase 3 pods in a mixed window.
func TestAuthServicePodCarriesTheUsersOwnedLabelFromPhase4Only(t *testing.T) {
	for _, phase := range []string{"1", "2", "3"} {
		if value, set := authServicePodLabels(t, phase)[usersOwnedLabel]; set {
			t.Errorf("phase %s: pod label %s=%v; want it absent", phase, usersOwnedLabel, value)
		}
	}
	if value := authServicePodLabels(t, "4")[usersOwnedLabel]; value != "true" {
		t.Errorf("phase 4: pod label %s=%v; want \"true\"", usersOwnedLabel, value)
	}
}

// A flip-back narrowing is not a phase 4 setting: the render refuses it and names the value.
func TestNarrowAfterFlipBackIsRefusedFromPhase4(t *testing.T) {
	_, out, err := renderAuth(t, goAPIWithAuth(append([]string{"--set", "authService.phase=4", "--set", "authService.migrate.narrowAfterFlipBack=true"}, phase3Secrets...)...)...)
	if err == nil || !strings.Contains(out, "authService.migrate.narrowAfterFlipBack") {
		t.Errorf("phase 4: narrowAfterFlipBack rendered: err=%v out=%.300s", err, out)
	}
}
