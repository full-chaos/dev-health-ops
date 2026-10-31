package apiservice

import (
	"os"
	"strings"
	"testing"
)

// The scope middlewares read the caller from the Scope.Principal decision
// only. The prose of their reachable-validator entries must say so, and the
// code it describes must still hold.
func TestScopeReachableValidatorProseNamesThePrincipalDecision(t *testing.T) {
	staleWording := []string{"reads the first Bearer", "authenticates the Bearer", "authenticates it as"}
	checked := 0
	for _, row := range loadGoAPIRows(t) {
		reachable, _ := row["reachable_validators"].([]any)
		for _, item := range reachable {
			entry := item.(map[string]any)
			anchor, _ := entry["anchor"].(map[string]any)
			note, _ := anchor["note"].(string)
			if note != "func (s *Scope) OrgScope" && note != "func (s *Scope) Impersonation" {
				continue
			}
			description, _ := entry["description"].(string)
			if !strings.Contains(description, "Scope.Principal decision") {
				t.Errorf("%s: %q does not name the Scope.Principal decision", row["id"], note)
			}
			for _, stale := range staleWording {
				if strings.Contains(description, stale) {
					t.Errorf("%s: %q still says %q", row["id"], note, stale)
				}
			}
			checked++
		}
	}
	if checked < 400 {
		t.Fatalf("only %d scope entries were checked", checked)
	}
	raw, err := os.ReadFile("../api/policy/principalonce.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), "func (s *Scope) Principal(") {
		t.Fatal("Scope.Principal is gone: the rows' prose is stale")
	}
	for _, file := range []string{"../api/policy/scope.go"} {
		scope, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(scope), `Header.Get("Authorization")`) || strings.Contains(string(scope), `Header.Values("Authorization")`) {
			t.Errorf("%s reads the Authorization header: OrgScope and Impersonation no longer take the caller from Principal only", file)
		}
	}
}
