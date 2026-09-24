package providersync

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
)

// TestLinearWorkItemsOraclePrepExecutesLiveProducer runs the live Linear
// work-items producer probe beside the generic pair tests (which hold the
// whole-row comparison): the producer still emits a complete work item and
// its transitions. It runs in ci/check_go.sh live-python-oracles with the
// rest of this package, and its proof file is required there. This test
// remains provider-only: it does not inspect or activate registry, matrix,
// scheduler, or route wiring.
func TestLinearWorkItemsOraclePrepExecutesLiveProducer(t *testing.T) {
	requireLivePythonOracles(t)

	_, currentFile, _, _ := runtime.Caller(0)
	packageDir := filepath.Dir(currentFile)
	root := filepath.Join(packageDir, "..", "..")
	script := filepath.Join(
		packageDir, "testdata", "linear_work_items_oracle_prep.py",
	)
	python := pyoracle.Resolve(t, root)
	command := exec.Command(python, script)
	command.Dir = root
	command.Env = make([]string, 0, len(os.Environ())+2)
	for _, value := range os.Environ() {
		if strings.HasPrefix(value, "PYTHONPATH=") ||
			strings.HasPrefix(value, "OTEL_ENABLED=") {
			continue
		}
		command.Env = append(command.Env, value)
	}
	command.Env = append(command.Env,
		"PYTHONPATH="+filepath.Join(root, "src"),
		"OTEL_ENABLED=false",
	)
	output, err := command.Output()
	if err != nil {
		t.Fatalf("execute live Linear producer probe: %v", pyoracle.RunError(python, err, nil))
	}
	var result struct {
		Producer       string           `json:"producer"`
		WorkItem       map[string]any   `json:"work_item"`
		WorkItemFields []string         `json:"work_item_fields"`
		Transitions    []map[string]any `json:"transitions"`
	}
	if err := json.Unmarshal(output, &result); err != nil {
		t.Fatalf("decode live Linear producer probe: %v: %s", err, output)
	}
	if result.Producer == "" || len(result.WorkItem) == 0 ||
		len(result.WorkItemFields) != len(result.WorkItem) || len(result.Transitions) == 0 {
		t.Fatalf("live Linear producer probe was empty or incomplete: %+v", result)
	}
	if result.WorkItem["work_item_id"] != "linear:ENG-42" {
		t.Fatalf("live Linear producer emitted unexpected id: %#v", result.WorkItem["work_item_id"])
	}

	if strings.TrimSpace(result.WorkItem["provider"].(string)) != "linear" {
		t.Fatalf("live Linear producer emitted wrong provider: %#v", result.WorkItem["provider"])
	}
	proof := filepath.Join(os.Getenv(livePythonOracleProofDir), "linear-work-items-oracle-prep")
	if err := os.WriteFile(proof, []byte("executed"), 0o600); err != nil {
		t.Fatalf("write live Python oracle proof: %v", err)
	}
}
