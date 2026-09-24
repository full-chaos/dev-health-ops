package synccoverage

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// pythonEffectiveKeysProgram runs each (dataset, stored flags text) through
// the api's own _effective_dataset_keys after json.loads, as the ORM reads
// the column; an exception is recorded by type.
const pythonEffectiveKeysProgram = `
import json, sys
from dev_health_ops.api.services.sync_coverage import _effective_dataset_keys
out = []
for dataset, text in json.loads(sys.stdin.read()):
    flags = None if text is None else json.loads(text)
    try:
        out.append({"keys": _effective_dataset_keys(dataset, flags)})
    except Exception as exc:
        out.append({"error": type(exc).__name__})
print(json.dumps(out))
`

// effectiveKeyFlagShapes are stored processor_flags texts: every JSON type,
// falsy and truthy, and objects whose family flags are true, truthy, false
// or mixed with non-bool values.
var effectiveKeyFlagShapes = []any{
	nil, "null", "{}", "[]", `""`, "0", "0.0", "-0.0", "false", "true", "1", "2.5", `"x"`, `["a"]`, `[0]`, `{"a": 1}`,
	`{"family_dataset_work_item_labels": true}`,
	`{"family_dataset_work_item_labels": true, "sync_x": 1}`,
	`{"family_dataset_work_item_labels": 1, "family_dataset_work_item_history": true}`,
	`{"family_dataset_work_item_labels": "true", "family_dataset_work_item_comments": true, "other": [1, {"b": null}]}`,
	`{"family_dataset_work_item_labels": true, "family_dataset_work_item_labels": false}`,
	`{"family_dataset_work_item_labels": false, "family_dataset_work_item_labels": true}`,
	`{"family_dataset_pr_comments": true, "family_dataset_pr_reviews": true, "family_dataset_prs": true, "x": "y"}`,
	`{"family_dataset_tests": true, "family_dataset_cicd": 1.0}`,
	`{"family_dataset_cicd": true, "family_dataset_tests": true, "n": 2.5}`,
	`{"family_dataset_work_items": true, "family_dataset_work_item_projects": true}`,
	`{"family_dataset_tests": null}`,
}

var effectiveKeyDatasets = []string{"work-items", "prs", "cicd", "commits", "work-item-labels", "tests", "unknown"}

// TestEffectiveDatasetKeysVenueOracleMatchesLivePython requires
// effectiveDatasetKeys to answer every (dataset, flags) pair as the api's
// _effective_dataset_keys does: the same keys, or an error where Python
// raises. It imports the api's service module, so it runs in the
// venue-oracles job, which discovers it by name.
func TestEffectiveDatasetKeysVenueOracleMatchesLivePython(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("the effective keys oracle needs the full project Python environment; ci/check_go.sh venue-oracles runs it")
	}
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
	python := pyoracle.Resolve(t, root)
	var cases [][2]any
	for _, dataset := range effectiveKeyDatasets {
		for _, flags := range effectiveKeyFlagShapes {
			cases = append(cases, [2]any{dataset, flags})
		}
	}
	input, _ := json.Marshal(cases)
	command := exec.Command(python, "-c", pythonEffectiveKeysProgram)
	command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(root, "src"))
	command.Stdin = strings.NewReader(string(input))
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("live python: %v", pyoracle.RunError(python, err, output))
	}
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	var want []struct {
		Keys  []string
		Error string
	}
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &want); err != nil {
		t.Fatalf("decode: %v\n%s", err, output)
	}
	if len(want) != len(cases) || len(cases) != 7*27 {
		t.Fatalf("python answered %d of %d cases, want %d", len(want), len(cases), 7*27)
	}
	raised := 0
	for index, c := range cases {
		var flags json.RawMessage
		if text, ok := c[1].(string); ok {
			flags = json.RawMessage(text)
		}
		got, err := effectiveDatasetKeys(c[0].(string), flags)
		expected := want[index]
		if expected.Error != "" {
			raised++
			if err == nil {
				t.Errorf("%v: go %v, python raised %s", c, got, expected.Error)
			}
			continue
		}
		if err != nil || strings.Join(got, ",") != strings.Join(expected.Keys, ",") {
			t.Errorf("%v: go %v %v, python %v", c, got, err, expected.Keys)
		}
	}
	if raised == 0 {
		t.Fatal("no case raised in Python: the error path is not compared")
	}
	t.Logf("%d cases, %d raising in Python", len(cases), raised)
	venueoracle.WriteProof(t)
}
