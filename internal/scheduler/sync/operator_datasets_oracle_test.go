package sync

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// The operator dataset mapping is compared with the real sync/datasets.py over
// every provider spelling and every subset of the legacy targets (plus one
// target no provider knows). goldenSHA256 pins the frozen subset in testdata
// (R24): the producer is deleted with the Python CLI, so this is a rot guard,
// not a freshness check. The file is only rewritten by the venue oracle with
// DHO_OPERATOR_DATASETS_GOLDEN_UPDATE=1, then this digest is updated.
const operatorDatasetsGoldenSHA256 = "8210d5c6419796e2092e375518052d419653dc778f41c7a5312233a4de9d13c6"

const operatorDatasetsGolden = "testdata/operator_datasets_golden.json"

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

func keep(entry oracleEntry) bool {
	return len(entry.Targets) <= 2 || len(entry.Targets) >= len(oracleTargets)-1
}

func TestOperatorDatasetsVenueOracleMatchesThePythonProducer(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("the live Python producer runs only with DEV_HEALTH_LIVE_PYTHON_ORACLES=1 and the full project Python environment")
	}
	root, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	python := pyoracle.Resolve(t, root)
	all := subsets()
	input, err := json.Marshal(map[string]any{"providers": oracleProviders, "subsets": all})
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(python, "-c", operatorDatasetsPython)
	command.Stdin = bytes.NewReader(input)
	command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(root, "src"), "OTEL_ENABLED=false")
	var stderr bytes.Buffer
	command.Stderr = &stderr
	output, err := command.Output()
	if err != nil {
		t.Fatalf("the Python producer failed: %v", pyoracle.RunError(python, err, []byte(stderr.String())))
	}
	var python2 map[string]oracleProvider
	if err := json.Unmarshal(output, &python2); err != nil {
		t.Fatal(err)
	}
	frozen := map[string]oracleProvider{}
	nonEmpty := 0
	for _, provider := range oracleProviders {
		want, got := python2[provider], goProvider(provider, all)
		if !slices.Equal(want.Supported, got.Supported) {
			t.Fatalf("SupportedLegacyTargets(%q) = %v, Python %v", provider, got.Supported, want.Supported)
		}
		for index := range want.Entries {
			w, g := want.Entries[index], got.Entries[index]
			if !slices.Equal(w.Keys, g.Keys) || w.Error != g.Error {
				t.Fatalf("PlannerDatasetKeys(%q, %v) = %v %q, Python %v %q", provider, w.Targets, g.Keys, g.Error, w.Keys, w.Error)
			}
			if len(w.Keys) > 0 {
				nonEmpty++
			}
		}
		kept := oracleProvider{Supported: want.Supported}
		for _, entry := range want.Entries {
			if keep(entry) {
				kept.Entries = append(kept.Entries, entry)
			}
		}
		frozen[provider] = kept
	}
	if nonEmpty < 1000 {
		t.Fatalf("only %d of the compared answers selected a dataset: the comparison would measure nothing", nonEmpty)
	}
	if os.Getenv("DHO_OPERATOR_DATASETS_GOLDEN_UPDATE") == "1" {
		raw, err := json.MarshalIndent(frozen, "", " ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll("testdata", 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(operatorDatasetsGolden, append(raw, '\n'), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	venueoracle.WriteProof(t)
}

func TestOperatorDatasetsGoldenIsTheFileTheDigestPins(t *testing.T) {
	raw, err := os.ReadFile(operatorDatasetsGolden)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	if got := hex.EncodeToString(sum[:]); got != operatorDatasetsGoldenSHA256 {
		t.Fatalf("%s digest = %s, want %s: the golden changed without its digest", operatorDatasetsGolden, got, operatorDatasetsGoldenSHA256)
	}
}

// TestOperatorDatasetsMatchTheFrozenPythonAnswers checks the frozen answers of
// the real Python functions without Python.
func TestOperatorDatasetsMatchTheFrozenPythonAnswers(t *testing.T) {
	raw, err := os.ReadFile(operatorDatasetsGolden)
	if err != nil {
		t.Fatal(err)
	}
	var frozen map[string]oracleProvider
	if err := json.Unmarshal(raw, &frozen); err != nil {
		t.Fatal(err)
	}
	if len(frozen) != len(oracleProviders) {
		t.Fatalf("golden has %d providers, want %d", len(frozen), len(oracleProviders))
	}
	answers := 0
	for provider, want := range frozen {
		if !slices.Equal(SupportedLegacyTargets(provider), want.Supported) {
			t.Errorf("SupportedLegacyTargets(%q) = %v, frozen Python %v", provider, SupportedLegacyTargets(provider), want.Supported)
		}
		for _, entry := range want.Entries {
			keys, err := PlannerDatasetKeys(provider, entry.Targets)
			message := ""
			if err != nil {
				message = err.Error()
			}
			if !slices.Equal(keys, entry.Keys) || message != entry.Error {
				t.Errorf("PlannerDatasetKeys(%q, %v) = %v %q, frozen Python %v %q", provider, entry.Targets, keys, message, entry.Keys, entry.Error)
			}
			if len(entry.Keys) > 0 {
				answers++
			}
		}
	}
	if answers < 100 {
		t.Fatalf("the golden selects a dataset in only %d answers: it measures nothing", answers)
	}
	if !strings.Contains(string(raw), "PagerDuty sync target must be operational") {
		t.Fatal("the golden records no PagerDuty refusal")
	}
}
