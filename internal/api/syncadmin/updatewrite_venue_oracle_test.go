package syncadmin

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/providersync"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// pythonControlledKeysProgram prints sync/datasets.operator_controlled_dataset_keys
// for every provider, sorted.
const pythonControlledKeysProgram = `
import json, sys
from dev_health_ops.sync.datasets import operator_controlled_dataset_keys
print(json.dumps([sorted(operator_controlled_dataset_keys(p)) for p in json.loads(sys.stdin.read())]))
`

// TestOperatorControlledDatasetKeysVenueOracleMatchesLivePython holds
// providersync.OperatorControlledDatasetKeys to the api's own
// operator_controlled_dataset_keys for every registered provider, in other
// cases and spacing, and for unknown ones.
func TestOperatorControlledDatasetKeysVenueOracleMatchesLivePython(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("the controlled-keys oracle needs the full project Python environment; ci/check_go.sh venue-oracles runs it")
	}
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
	python := pyoracle.Resolve(t, root)
	providers := []string{"github", "gitlab", "jira", "linear", "launchdarkly", "pagerduty", "GitHub", "GITLAB", "PagerDuty",
		" jira", "jira ", "bogus", "", "local", "synthetic"}
	input, _ := json.Marshal(providers)
	command := exec.Command(python, "-c", pythonControlledKeysProgram)
	command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(root, "src"))
	command.Stdin = strings.NewReader(string(input))
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("live python: %v", pyoracle.RunError(python, err, output))
	}
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	var want [][]string
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &want); err != nil || len(want) != len(providers) {
		t.Fatalf("decode: %v\n%s", err, output)
	}
	nonEmpty := 0
	for index, provider := range providers {
		got := append([]string{}, providersync.OperatorControlledDatasetKeys(provider)...)
		sort.Strings(got)
		if strings.Join(got, ",") != strings.Join(want[index], ",") {
			t.Errorf("provider %q: go %v, python %v", provider, got, want[index])
		}
		if len(got) > 0 {
			nonEmpty++
		}
	}
	if nonEmpty == 0 {
		t.Fatal("no provider has controlled keys; the comparison is vacuous")
	}
	t.Logf("%d providers compared, %d with controlled keys", len(providers), nonEmpty)
	venueoracle.WriteProof(t)
}
