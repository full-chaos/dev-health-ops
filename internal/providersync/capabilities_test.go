package providersync

import (
	"fmt"
	"testing"
)

func TestCapabilitiesMatchPythonGitHubAndGitLabRegistry(t *testing.T) {
	t.Parallel()
	want := map[string][]string{
		"github": {
			"blame", "cicd", "commit-stats", "commits", "deployments", "files",
			"pr-comments", "pr-reviews", "prs", "repo-metadata", "security", "tests",
			"work-item-comments", "work-item-history", "work-item-labels",
			"work-item-projects", "work-items",
		},
		"gitlab": {
			"blame", "cicd", "commit-stats", "commits", "deployments",
			"feature-flags", "files", "incidents", "pr-comments", "pr-reviews",
			"prs", "repo-metadata", "security", "tests", "work-item-comments",
			"work-item-history", "work-item-labels", "work-item-projects", "work-items",
		},
	}
	for provider, datasets := range want {
		capabilities := Capabilities(provider)
		got := make([]string, 0, len(capabilities))
		for _, capability := range capabilities {
			got = append(got, capability.Dataset)
			if capability.Provider != provider {
				t.Fatalf("%s capability provider=%q", capability.Dataset, capability.Provider)
			}
		}
		if fmt.Sprint(got) != fmt.Sprint(datasets) {
			t.Fatalf("%s datasets=%v want=%v", provider, got, datasets)
		}
	}
}

func TestCapabilityCostWatermarkAndFlagsAreExact(t *testing.T) {
	t.Parallel()
	tests := []struct {
		provider, dataset string
		cost              CostClass
		watermark         WatermarkBehavior
		flags             map[string]bool
	}{
		{"github", "repo-metadata", CostLight, WatermarkNone, map[string]bool{}},
		{"github", "commits", CostMedium, WatermarkIncremental, map[string]bool{"sync_git": true, "sync_commits": true}},
		{"github", "commit-stats", CostHeavy, WatermarkIncremental, map[string]bool{"sync_git": true, "sync_commit_stats": true}},
		{"gitlab", "incidents", CostMedium, WatermarkIncremental, map[string]bool{"sync_incidents": true}},
		{"gitlab", "feature-flags", CostMedium, WatermarkIncremental, map[string]bool{}},
	}
	for _, test := range tests {
		capability, ok := Capability(test.provider, test.dataset)
		if !ok || capability.CostClass != test.cost || capability.Watermark != test.watermark ||
			fmt.Sprint(capability.ProcessorFlags) != fmt.Sprint(test.flags) {
			t.Fatalf("%s/%s=%+v", test.provider, test.dataset, capability)
		}
	}
}

func TestCapabilityReturnsDefensiveCopies(t *testing.T) {
	t.Parallel()
	first, _ := Capability("github", "commits")
	first.ProcessorFlags["mutated"] = true
	first.LegacyTargets[0] = "mutated"
	second, _ := Capability("github", "commits")
	if second.ProcessorFlags["mutated"] || second.LegacyTargets[0] != "git" {
		t.Fatalf("registry mutation escaped: %+v", second)
	}
}
