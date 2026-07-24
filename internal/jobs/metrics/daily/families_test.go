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
	if registry.SchemaVersion != 1 || len(registry.Families) != 23 {
		t.Fatalf("invalid family registry: %#v", registry)
	}
	seen := map[string]bool{}
	for _, family := range registry.Families {
		if family.Name == "" || family.Python == "" || len(family.Writes) == 0 || family.Golden != "required" || seen[family.Name] {
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
	for _, family := range registry.Families {
		if (family.Name == "repo_user_commit" || family.Name == "team_wellbeing") && family.Port != "next_core" {
			t.Fatalf("daily core family %s must be next_core, got %s", family.Name, family.Port)
		}
	}
}
