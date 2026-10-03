package providersync

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/programoracle"
)

// producerProbeGoldens is the set of this package's frozen producer-probe answers: the real Python producers
// of the pinned build, executed once and frozen. The producers are the standard library and dev_health_ops
// (and what it imports). A golden recorded by another producer is refused.
var producerProbeGoldens = programoracle.Set{
	Package:  "./internal/providersync/",
	Build:    "a4847c5e93607451a0c987b314d37e02fc43ce85",
	Identity: "python 3.14.7\nunicodedata 16.0.0",
	// The goldenrecord verb writes each digest when it promotes a recording; a new golden starts as
	// "PIN:" + its file name without ".json".
	Pins: map[string]string{
		"linear-work-items-prep.golden.json": "e9bbaf23161e43b6e9b255363cd7d88523558ba94bb89a307c54fda8f4bb357e",
	},
}

// TestLinearWorkItemsOraclePrepMatchesFrozenProducerProbe checks the frozen answer of the Linear work-items
// producer probe (testdata/linear_work_items_oracle_prep.py, run in the pinned checkout): the producer emitted a
// complete work item and its transitions. The probe is provider-only and compares nothing in Go: the generic pair
// tests hold the whole-row comparison of Go with Python (CHAOS-7338); this test pins that the recorded Python
// producer answer itself is complete, so the pair cases it feeds are not empty.
func TestLinearWorkItemsOraclePrepMatchesFrozenProducerProbe(t *testing.T) {
	_, currentFile, _, _ := moduleroot.Caller(0)
	packageDir := filepath.Dir(currentFile)
	root := filepath.Clean(filepath.Join(packageDir, "..", ".."))
	script, err := os.ReadFile(filepath.Join(packageDir, "testdata", "linear_work_items_oracle_prep.py"))
	if err != nil {
		t.Fatal(err)
	}
	// The probe finds the checkout from its own path; the program runs in the pinned checkout, so the root is
	// its working directory.
	const fromFile = "REPO_ROOT = Path(__file__).resolve().parents[3]"
	if !strings.Contains(string(script), fromFile) {
		t.Fatal("the Linear probe no longer finds its checkout through __file__: update the program text")
	}
	program := strings.Replace(string(script), fromFile, "REPO_ROOT = Path.cwd()", 1)
	output := []byte(producerProbeGoldens.Outputs(t, root, "linear-work-items-prep.golden.json",
		programoracle.Program{Name: "linear work items probe", Text: program})[0])
	var result struct {
		Producer       string           `json:"producer"`
		WorkItem       map[string]any   `json:"work_item"`
		WorkItemFields []string         `json:"work_item_fields"`
		Transitions    []map[string]any `json:"transitions"`
	}
	if err := json.Unmarshal(output, &result); err != nil {
		t.Fatalf("decode the frozen Linear producer probe: %v: %s", err, output)
	}
	if result.Producer == "" || len(result.WorkItem) == 0 ||
		len(result.WorkItemFields) != len(result.WorkItem) || len(result.Transitions) == 0 {
		t.Fatalf("the frozen Linear producer probe is empty or incomplete: %+v", result)
	}
	if result.WorkItem["work_item_id"] != "linear:ENG-42" {
		t.Fatalf("the Linear producer emitted an unexpected id: %#v", result.WorkItem["work_item_id"])
	}
	if provider, _ := result.WorkItem["provider"].(string); strings.TrimSpace(provider) != "linear" {
		t.Fatalf("the Linear producer emitted the wrong provider: %#v", result.WorkItem["provider"])
	}
}
