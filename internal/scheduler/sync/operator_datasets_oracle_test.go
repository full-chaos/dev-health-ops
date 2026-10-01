package sync

import (
	"encoding/json"
	"slices"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/programoracle"
)

// The operator dataset mapping is compared with the frozen answers of the real
// sync/datasets.py over every provider spelling and every subset of the legacy
// targets (plus one target no provider knows).
var (
	oracleProviders = []string{"github", "gitlab", "jira", "linear", "launchdarkly", "pagerduty", "GitHub", "PAGERDUTY", "unknown", ""}
	oracleTargets   = []string{"git", "prs", "blame", "cicd", "deployments", "incidents", "security", "tests", "work-items", "feature-flags", "operational", "bogus"}
)

// oracleEntry is one recorded answer: the keys, or the error text.
type oracleEntry struct {
	Targets []string `json:"targets"`
	Keys    []string `json:"keys,omitempty"`
	Error   string   `json:"error,omitempty"`
}

type oracleProvider struct {
	Supported []string      `json:"supported"`
	Entries   []oracleEntry `json:"entries"`
}

// subsets is every subset of oracleTargets in bitmask order.
func subsets() [][]string {
	out := make([][]string, 0, 1<<len(oracleTargets))
	for mask := 0; mask < 1<<len(oracleTargets); mask++ {
		subset := []string{}
		for bit, target := range oracleTargets {
			if mask&(1<<bit) != 0 {
				subset = append(subset, target)
			}
		}
		out = append(out, subset)
	}
	return out
}

func goProvider(provider string, all [][]string) oracleProvider {
	result := oracleProvider{Supported: SupportedLegacyTargets(provider)}
	for _, subset := range all {
		entry := oracleEntry{Targets: subset}
		keys, err := PlannerDatasetKeys(provider, subset)
		if err != nil {
			entry.Error = err.Error()
		} else {
			entry.Keys = keys
		}
		result.Entries = append(result.Entries, entry)
	}
	return result
}

const operatorDatasetsPython = `
import json, sys
from dev_health_ops.sync.datasets import planner_dataset_keys, supported_legacy_targets
spec = json.load(sys.stdin)
out = {}
for provider in spec["providers"]:
    entries = []
    for subset in spec["subsets"]:
        entry = {"targets": subset}
        try:
            entry["keys"] = planner_dataset_keys(provider, subset)
        except Exception as error:
            entry["error"] = str(error)
        entries.append(entry)
    out[provider] = {"supported": supported_legacy_targets(provider), "entries": entries}
json.dump(out, sys.stdout)
`

// pagerDutyRefusal is in the text Python gives for a PagerDuty selection that
// is not the operational target alone.
const pagerDutyRefusal = "PagerDuty sync target must be operational"

// TestOperatorDatasetsMatchFrozenPythonOnEverySubset compares PlannerDatasetKeys
// and SupportedLegacyTargets with the frozen answers of planner_dataset_keys
// and supported_legacy_targets for every provider spelling and every subset.
// The answers must select a dataset often and must hold the PagerDuty refusal:
// a frozen file without them would measure nothing.
func TestOperatorDatasetsMatchFrozenPythonOnEverySubset(t *testing.T) {
	all := subsets()
	input, err := json.Marshal(map[string]any{"providers": oracleProviders, "subsets": all})
	if err != nil {
		t.Fatal(err)
	}
	output := frozenPython(t, "operator-datasets.golden.json", programoracle.Program{
		Name: "operator datasets", Text: operatorDatasetsPython, Stdin: input, Env: map[string]string{"OTEL_ENABLED": "false"},
	})[0]
	var python2 map[string]oracleProvider
	if err := json.Unmarshal([]byte(output), &python2); err != nil {
		t.Fatal(err)
	}
	if len(python2) != len(oracleProviders) {
		t.Fatalf("the frozen answer holds %d providers, want %d", len(python2), len(oracleProviders))
	}
	nonEmpty, refusals := 0, 0
	for _, provider := range oracleProviders {
		want, got := python2[provider], goProvider(provider, all)
		if !slices.Equal(want.Supported, got.Supported) {
			t.Fatalf("SupportedLegacyTargets(%q) = %v, Python %v", provider, got.Supported, want.Supported)
		}
		if len(want.Entries) != len(got.Entries) {
			t.Fatalf("the frozen answer holds %d subsets for %q, want %d", len(want.Entries), provider, len(got.Entries))
		}
		for index := range want.Entries {
			w, g := want.Entries[index], got.Entries[index]
			if !slices.Equal(w.Targets, g.Targets) {
				t.Fatalf("answer %d of %q is for the targets %v, want %v", index, provider, w.Targets, g.Targets)
			}
			if !slices.Equal(w.Keys, g.Keys) || w.Error != g.Error {
				t.Fatalf("PlannerDatasetKeys(%q, %v) = %v %q, Python %v %q", provider, w.Targets, g.Keys, g.Error, w.Keys, w.Error)
			}
			if len(w.Keys) > 0 {
				nonEmpty++
			}
			if strings.Contains(w.Error, pagerDutyRefusal) {
				refusals++
			}
		}
	}
	if nonEmpty < 1000 {
		t.Fatalf("only %d of the compared answers selected a dataset: the comparison would measure nothing", nonEmpty)
	}
	if refusals == 0 {
		t.Fatalf("no frozen answer holds the PagerDuty refusal (%q)", pagerDutyRefusal)
	}
}
