package daily

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestFamilyRegistryIsCompleteAndRoutesCorePortFirst(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("families.json"))
	if err != nil {
		t.Fatal(err)
	}
	var registry struct {
		SchemaVersion int `json:"schema_version"`
		Families      []struct {
			Name   string   `json:"name"`
			Python string   `json:"python"`
			Writes []string `json:"writes"`
			Port   string   `json:"port"`
			Golden string   `json:"golden"`
		} `json:"families"`
	}
	if err := json.Unmarshal(data, &registry); err != nil {
		t.Fatal(err)
	}
	if registry.SchemaVersion != 1 || len(registry.Families) != 24 {
		t.Fatalf("invalid family registry: %#v", registry)
	}
	// The port enum is closed: "pending" (still Python-only), "next_core"
	// (a Wave-1 reference-implementation family not yet cut over), and "go"
	// (a native Go executor is registered and Python's compute path for it
	// is dormant behind PartitionHandler.SetNativeFamilies -- see
	// TeamWellbeingExecutor, CHAOS-4276). A typo in this field silently
	// leaves a family stuck, so it is validated here rather than only at
	// the Go-executor construction site.
	validPorts := map[string]bool{"pending": true, "next_core": true, "go": true}
	seen := map[string]bool{}
	for _, family := range registry.Families {
		if family.Name == "" || family.Python == "" || len(family.Writes) == 0 || family.Golden != "required" || seen[family.Name] || !validPorts[family.Port] {
			t.Fatalf("invalid family entry: %#v", family)
		}
		seen[family.Name] = true
	}
	expected := []string{
		"repo_user_commit", "team_wellbeing", "file_hotspots", "file_risk_hotspots", "work_item", "work_item_estimate", "work_item_attribution", "work_item_state", "review_edges", "cicd", "testops_pipeline", "testops_test", "testops_coverage", "deploy", "incident", "ai_governance", "ai_impact", "ai_workflow", "work_graph_edges", "compounding_risk", "testops_risk", "benchmarking", "ic_finalize",
	}
	for _, core := range expected {
		if !seen[core] {
			t.Fatalf("daily family %s is absent", core)
		}
	}
	byName := make(map[string]string, len(registry.Families))
	for _, family := range registry.Families {
		byName[family.Name] = family.Port
	}
	// team_wellbeing is CHAOS-4276's reference cutover: it must be "go", not
	// "next_core" or "pending" -- catches a families.json edit that flips
	// the flag back (or forgets to) without touching the Go registration.
	if got := byName["team_wellbeing"]; got != "go" {
		t.Fatalf("team_wellbeing must be port=go, got %q", got)
	}
	// repo_user_commit is Wave 1's OTHER reference implementation
	// (lane-4275): its own PR flips this to "go", registering
	// RepoUserCommitExecutor exactly as team_wellbeing did above.
	if got := byName["repo_user_commit"]; got != "go" {
		t.Fatalf("repo_user_commit must be port=go, got %q", got)
	}
	// incident (CHAOS-4269/CHAOS-4295): registers IncidentExecutor, WITH the
	// NULL-OK valid_from guard fix -- Python's compute path for this family
	// was always zero-yield (CHAOS-4269), so this flag flipping back without
	// the Go registration would silently resurrect a permanently-broken
	// family, not just a working-but-slower one.
	if got := byName["incident"]; got != "go" {
		t.Fatalf("incident must be port=go, got %q", got)
	}
}
