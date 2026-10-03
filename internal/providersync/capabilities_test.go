package providersync

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/programoracle"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
)

const (
	livePythonOraclesEnv     = "DEV_HEALTH_LIVE_PYTHON_ORACLES"
	livePythonOracleProofDir = "DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR"
)

func requireLivePythonOracles(t *testing.T) {
	t.Helper()
	if os.Getenv(livePythonOraclesEnv) != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
	}
	if os.Getenv(livePythonOracleProofDir) == "" {
		t.Fatal("live Python oracle opt-in requires a proof directory from ci/check_go.sh")
	}
}

// capabilityGoldens is the set of this package's frozen Python registry answers: the dataset registry of
// the pinned build, read by its own supported_datasets, executed once and frozen. The producer is the
// standard library and dev_health_ops.sync.datasets only. A golden recorded by another producer is refused.
var capabilityGoldens = programoracle.Set{
	Package:  "./internal/providersync/",
	Build:    "a4847c5e93607451a0c987b314d37e02fc43ce85",
	Identity: "python 3.14.7\nunicodedata 16.0.0",
	// The goldenrecord verb writes each digest when it promotes a recording; a new golden starts as
	// "PIN:" + its file name without ".json".
	Pins: map[string]string{
		"capabilities-registry.golden.json": "b5a505262fb16113e7fe3bcd7ad2b37e68db4dddfd5acdcb11a0a0d9b64b14d4",
	},
}

// TestCapabilitiesMatchFrozenPythonProviderRegistry compares the Go capability registry with the frozen
// answer of the Python registry (testdata/python_registry_oracle.py on src/dev_health_ops/sync/datasets.py of
// the pinned build).
func TestCapabilitiesMatchFrozenPythonProviderRegistry(t *testing.T) {
	_, currentFile, _, _ := moduleroot.Caller(0)
	packageDir := filepath.Dir(currentFile)
	root := filepath.Clean(filepath.Join(packageDir, "..", ".."))
	script, err := os.ReadFile(filepath.Join(packageDir, "testdata", "python_registry_oracle.py"))
	if err != nil {
		t.Fatal(err)
	}
	// The program runs in the pinned checkout: the datasets module is read from there.
	const future = "from __future__ import annotations\n"
	if !strings.Contains(string(script), future) {
		t.Fatal("the registry oracle no longer starts with its __future__ import: place the argument line after it")
	}
	program := strings.Replace(string(script), future, future+"import sys as _argument_holder\n_argument_holder.argv = ['python_registry_oracle', 'src/dev_health_ops/sync/datasets.py']\n", 1)
	output := []byte(capabilityGoldens.Outputs(t, root, "capabilities-registry.golden.json", programoracle.Program{Name: "dataset registry", Text: program})[0])
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
		t.Fatalf("Go registry drifted from the frozen Python registry:\ngot  %s\nwant %s", gotJSON, output)
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
	requireLivePythonOracles(t)
	_, currentFile, _, _ := moduleroot.Caller(0)
	root := filepath.Dir(filepath.Dir(filepath.Dir(currentFile)))
	resolved := pyoracle.Resolve(t, root)
	assertPythonProducerIsThisWorktree(t, resolved)
	return resolved
}

// assertPythonProducerIsThisWorktree fails unless the interpreter about to be
// executed resolves `dev_health_ops` INSIDE this checkout.
//
// Every live-Python oracle in this package compares Go against "the real
// production function". WHICH production function that is depends entirely on
// sys.path, and nothing here previously checked it. On a machine with more than
// one worktree -- the normal case for this repo -- an ambient interpreter (a
// pyenv virtualenv editable-installed against a DIFFERENT checkout, say)
// silently supplies another worktree's `dev_health_ops`, while the oracle pairs,
// which resolve their own paths from `__file__`, keep reading THIS worktree's
// config and testdata. The comparison then runs half against one checkout and
// half against another, and reports a clean pass.
//
// That was reproduced directly on this machine, not hypothesised: with
// PYTHONPATH unset, the investment classifier came from
// ops-worktrees/chaos-3219-compose-env while its config came from here, and the
// whole suite went green.
//
// ci/check_go.sh sets PYTHONPATH="${ROOT}/src" and is therefore safe, but a bare
// `go test` with the opt-in env vars is not -- and a green run is exactly the
// output that stops anyone looking. This turns that silent mis-measurement into
// a hard failure naming both paths.
//
// find_spec LOCATES the package without executing it, so this can neither be
// affected by nor disturb the stub namespace python_oracle_loader.py installs.
func assertPythonProducerIsThisWorktree(t *testing.T, python string) {
	t.Helper()
	pythonProducerOnce.Do(func() {
		output, err := exec.Command(python, "-c",
			"import importlib.util;s=importlib.util.find_spec('dev_health_ops');"+
				"print(s.origin if s else '')").CombinedOutput()
		pythonProducerOrigin = strings.TrimSpace(string(output))
		pythonProducerErr = err
	})
	if pythonProducerErr != nil {
		t.Fatalf("cannot determine which dev_health_ops %s would import: %v: %s",
			python, pythonProducerErr, pythonProducerOrigin)
	}
	if pythonProducerOrigin == "" {
		t.Fatalf("%s cannot import dev_health_ops at all -- the live-Python oracles "+
			"would compare against nothing. Set PYTHONPATH to this worktree's src/, "+
			"or run through ci/check_go.sh, which does it for you", python)
	}
	_, currentFile, _, _ := moduleroot.Caller(0)
	root := filepath.Dir(filepath.Dir(filepath.Dir(currentFile)))
	if !strings.HasPrefix(pythonProducerOrigin, root+string(filepath.Separator)) {
		t.Fatalf("live-Python oracle producer is NOT in this worktree:\n"+
			"  interpreter    %s\n"+
			"  dev_health_ops %s\n"+
			"  this worktree  %s\n"+
			"Every oracle here would compare Go against ANOTHER checkout's Python "+
			"while reading this one's config and testdata -- a mixed-source "+
			"comparison that passes. Set PYTHONPATH=%s/src (ci/check_go.sh does it "+
			"for you), or point PYTHON at this worktree's .venv.",
			python, pythonProducerOrigin, root, root)
	}
}

var (
	pythonProducerOnce   sync.Once
	pythonProducerOrigin string
	pythonProducerErr    error
)

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

// TestDatasetKeyOrderCoversTheRegistry pins that PlannerDatasetKeys can
// list every registered dataset: a key missing from datasetKeyOrder would
// be silently dropped from a planner-managed config's datasets.
func TestDatasetKeyOrderCoversTheRegistry(t *testing.T) {
	ordered := map[string]bool{}
	for _, key := range datasetKeyOrder {
		if ordered[key] {
			t.Fatalf("datasetKeyOrder lists %q twice", key)
		}
		ordered[key] = true
	}
	for provider, datasets := range datasetCapabilities {
		for dataset := range datasets {
			if !ordered[dataset] {
				t.Errorf("%s dataset %q is not in datasetKeyOrder", provider, dataset)
			}
		}
	}
}
