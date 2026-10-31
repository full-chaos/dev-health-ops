package contracts

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The workload_token class has exactly two validators, and both own the class
// on their routes. A flipped flag, a new validator or a moved declaration
// fails here. Line numbers are not pinned: the anchor tests own them.
func TestWorkloadTokenClassPinsItsValidators(t *testing.T) {
	want := map[string]string{
		"internal/apiservice/acr/workloadauth.go": "func (w *WorkloadAuth) decide(",
		"internal/auth/authapi/issue_workload.go": "func (s *SessionIssueWorkload) authorize(",
	}
	var validators []any
	for _, class := range registryDocument(t) {
		if class["class_id"] == "workload_token" {
			validators, _ = class["validators"].([]any)
		}
	}
	if len(validators) != len(want) {
		t.Fatalf("workload_token lists %d validators, want %d", len(validators), len(want))
	}
	root := testRoot(t)
	seen := map[string]bool{}
	for _, item := range validators {
		validator := item.(map[string]any)
		anchor := validator["anchor"].(map[string]any)
		path, _ := anchor["path"].(string)
		declaration, ok := want[path]
		if !ok || seen[path] {
			t.Errorf("unexpected or repeated workload_token validator at %q", path)
			continue
		}
		seen[path] = true
		if validator["is_intended_validator"] != true {
			t.Errorf("%s: is_intended_validator = %v, want true: it verifies the workload token on its routes", path, validator["is_intended_validator"])
		}
		if _, flagged := validator["reachable_but_not_owner"]; flagged {
			t.Errorf("%s: reachable_but_not_owner is set on an owning validator", path)
		}
		source, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			t.Fatal(err)
		}
		lines := strings.Split(string(source), "\n")
		first, last := int(anchor["line"].(float64)), int(anchor["line_end"].(float64))
		if first < 1 || last < first || last > len(lines) {
			t.Errorf("%s: anchor %d-%d is outside the file", path, first, last)
			continue
		}
		found := false
		for _, line := range lines[first-1 : last] {
			found = found || strings.HasPrefix(line, declaration)
		}
		if !found {
			t.Errorf("%s: anchor %d-%d does not hold %q", path, first, last, declaration)
		}
	}
}
