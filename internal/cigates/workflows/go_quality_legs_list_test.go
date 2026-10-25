package workflows_test

import (
	"encoding/json"
	"path/filepath"
	"testing"
)

// goQualityLegList reads the go-quality-leg matrix from ci/go_quality_legs.json (CHAOS-11418): the workflow takes its legs
// from go-quality-plan, which filters this file with ci/go_quality_legs.sh.
func goQualityLegList(t *testing.T, root string) []map[string]any {
	t.Helper()
	var legs []map[string]any
	if err := json.Unmarshal([]byte(readFile(t, filepath.Join(root, "ci", "go_quality_legs.json"))), &legs); err != nil {
		t.Fatalf("ci/go_quality_legs.json: %v", err)
	}
	return legs
}
