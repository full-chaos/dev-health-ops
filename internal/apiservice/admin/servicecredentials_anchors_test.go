package admin

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
)

// credential-classes.json pins lines of servicecredentials.go; a change above them moves a declaration off its anchor.
func TestAnchoredServiceCredentialLinesDidNotMove(t *testing.T) {
	raw, err := os.ReadFile("../../../contracts/auth/v1/credential-classes.json")
	if err != nil {
		t.Fatal(err)
	}
	source, err := os.ReadFile("servicecredentials.go")
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(string(source), "\n")
	var document any
	if err := json.Unmarshal(raw, &document); err != nil {
		t.Fatal(err)
	}
	want := map[int]string{143: "func generateServiceToken(", 217: ") IssueServiceCredential(", 296: ") RotateServiceCredential(", 333: ") RevokeServiceCredential("}
	seen := map[int]bool{}
	var walk func(node any)
	walk = func(node any) {
		switch value := node.(type) {
		case map[string]any:
			if value["path"] == "internal/apiservice/admin/servicecredentials.go" {
				line := int(value["line"].(float64))
				seen[line] = true
				if prefix, ok := want[line]; !ok || line > len(lines) || !strings.Contains(lines[line-1], prefix) {
					t.Errorf("anchor line %d does not hold %q", line, prefix)
				}
			}
			for _, child := range value {
				walk(child)
			}
		case []any:
			for _, child := range value {
				walk(child)
			}
		}
	}
	walk(document)
	for line := range want {
		if !seen[line] {
			t.Errorf("no anchor at line %d any more", line)
		}
	}
}
