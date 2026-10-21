package providersync

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/programoracle"
)

// producerProbeGoldens is the set of this package's frozen producer-probe answers: the real Python producers
// of the pinned build, executed once and frozen. The producers are the standard library and dev_health_ops
// (and what it imports). A golden recorded by another producer is refused.
var producerProbeGoldens = programoracle.Set{
	Build:    "a4847c5e93607451a0c987b314d37e02fc43ce85",
	Identity: "python 3.14.7\nunicodedata 16.0.0",
	Pins: map[string]string{
		"linear-work-items-prep.golden.json": "e9bbaf23161e43b6e9b255363cd7d88523558ba94bb89a307c54fda8f4bb357e",
	},
}

// TestLinearWorkItemsOraclePrepMatchesFrozenProducerProbe checks the frozen answer of the Linear work-items
// producer probe (run once in the pinned checkout): the producer emitted a
// complete work item and its transitions. The probe is provider-only and compares nothing in Go: the generic pair
// tests hold the whole-row comparison of Go with Python (CHAOS-7338); this test pins that the recorded Python
// producer answer itself is complete, so the pair cases it feeds are not empty.
func TestLinearWorkItemsOraclePrepMatchesFrozenProducerProbe(t *testing.T) {
	output := []byte(producerProbeGoldens.Outputs(t, "linear-work-items-prep.golden.json",
		programoracle.Program{Name: "linear work items probe"})[0])
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
