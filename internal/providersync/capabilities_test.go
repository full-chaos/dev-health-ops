package providersync

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"testing"
)

func TestCapabilitiesMatchPythonProviderRegistry(t *testing.T) {
	python := pythonExecutable(t)
	_, currentFile, _, _ := runtime.Caller(0)
	packageDir := filepath.Dir(currentFile)
	datasetsSource := filepath.Join(packageDir, "..", "..", "src", "dev_health_ops", "sync", "datasets.py")
	oracleScript := filepath.Join(packageDir, "testdata", "python_registry_oracle.py")
	output, err := exec.Command(python, oracleScript, datasetsSource).CombinedOutput()
	if err != nil {
		t.Fatalf("execute Python registry oracle: %v: %s", err, output)
	}
	var want map[string][]registryEntry
	if err := json.Unmarshal(output, &want); err != nil {
		t.Fatalf("decode Python registry oracle: %v: %s", err, output)
	}
	got := map[string][]registryEntry{}
	for _, provider := range []string{"github", "gitlab", "jira", "linear", "launchdarkly"} {
		sort.Slice(want[provider], func(left, right int) bool {
			return want[provider][left].Dataset < want[provider][right].Dataset
		})
		for _, capability := range Capabilities(provider) {
			targets := append([]string(nil), capability.LegacyTargets...)
			sort.Strings(targets)
			got[provider] = append(got[provider], registryEntry{
				Provider:       capability.Provider,
				Dataset:        capability.Dataset,
				CostClass:      string(capability.CostClass),
				Watermark:      string(capability.Watermark),
				LegacyTargets:  targets,
				ProcessorFlags: capability.ProcessorFlags,
			})
		}
	}
	if !reflect.DeepEqual(got, want) {
		gotJSON, _ := json.Marshal(got)
		t.Fatalf("Go registry drifted from live Python registry:\ngot  %s\nwant %s", gotJSON, output)
	}
}

type registryEntry struct {
	Provider       string          `json:"provider"`
	Dataset        string          `json:"dataset"`
	CostClass      string          `json:"cost_class"`
	Watermark      string          `json:"watermark"`
	LegacyTargets  []string        `json:"legacy_targets"`
	ProcessorFlags map[string]bool `json:"processor_flags"`
}

func pythonExecutable(t *testing.T) string {
	t.Helper()
	if configured := os.Getenv("PYTHON"); configured != "" {
		return configured
	}
	if path, err := exec.LookPath("python3"); err == nil {
		return path
	}
	t.Fatal("python3 is required for the cross-language dataset registry freshness check")
	return ""
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
		{"gitlab", "incidents", CostLight, WatermarkIncremental, map[string]bool{"sync_incidents": true}},
		{"gitlab", "feature-flags", CostMedium, WatermarkIncremental, map[string]bool{}},
		{"jira", "incidents", CostMedium, WatermarkIncremental, map[string]bool{}},
		{"jira", "work-item-labels", CostLight, WatermarkIncremental, map[string]bool{}},
		{"linear", "work-items", CostMedium, WatermarkIncremental, map[string]bool{}},
		{"launchdarkly", "feature-flags", CostMedium, WatermarkIncremental, map[string]bool{}},
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
