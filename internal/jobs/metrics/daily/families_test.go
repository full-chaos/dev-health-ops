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
	if registry.SchemaVersion != 1 || len(registry.Families) < 20 {
		t.Fatalf("invalid family registry: %#v", registry)
	}
	seen := map[string]bool{}
	for _, family := range registry.Families {
		if family.Name == "" || family.Python == "" || len(family.Writes) == 0 || family.Golden != "required" || seen[family.Name] {
			t.Fatalf("invalid family entry: %#v", family)
		}
		seen[family.Name] = true
	}
	for _, core := range []string{"repo_user_commit", "team_wellbeing"} {
		if !seen[core] {
			t.Fatalf("daily core family %s is absent", core)
		}
	}
}
