package providersync

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// TestLinearWorkItemsOraclePrepExecutesLiveProducer is intentionally opt-in.
// It keeps the original producer probe available for focused debugging while
// the generic pair tests provide the required whole-row comparison. This test
// remains provider-only: it does not inspect or activate registry, matrix,
// scheduler, or route wiring.
func TestLinearWorkItemsOraclePrepExecutesLiveProducer(t *testing.T) {
	if os.Getenv("LINEAR_ORACLE_PREP") != "1" {
		t.Skip("Linear oracle preparation is opt-in until the provider handler exists")
	}

	_, currentFile, _, _ := runtime.Caller(0)
	packageDir := filepath.Dir(currentFile)
	root := filepath.Join(packageDir, "..", "..")
	script := filepath.Join(
		packageDir, "testdata", "linear_work_items_oracle_prep.py",
	)
	command := exec.Command("python3", script)
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
		t.Fatalf("execute live Linear producer probe: %v", err)
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
}
