package providersync

import (
	"reflect"
	"testing"
)

// TestOracleDeclaredDivergenceGoKeepsTheInheritableEdgeOnATie: the frozen
// Python collapse (compute_work_items.py latest_edge) kept the lower
// relationship_type on an equal last_synced, so a blocks row beside the
// inheritable row of the same pair dropped the donor. Go keeps the inheritable
// row (CHAOS-9821: a lower tier never displaces a native link; that rule came
// after the Python choice): the frozen case with a same-timestamp blocks row gives the frozen
// case's answer. Python is deleted, so its answer for this input is not
// recorded (testdata/oracle_frozen/README.md).
func TestOracleDeclaredDivergenceGoKeepsTheInheritableEdgeOnATie(t *testing.T) {
	var input map[string]any
	for _, testCase := range githubWorkItemDerivationOracleCases() {
		if testCase.ID == "actual_extkey_link_inherits_linear_donor" {
			input = testCase.Input
		}
	}
	if input == nil {
		t.Fatal("frozen case actual_extkey_link_inherits_linear_donor is gone")
	}
	want := buildGitHubWorkItemDerivationOracleCandidates(t, input)
	inheritable := input["Dependencies"].([]any)[0].(map[string]any)
	blocks := map[string]any{}
	for key, value := range inheritable {
		blocks[key] = value
	}
	blocks["relationship_type"], blocks["relationship_type_raw"] = "blocks", "blocks"
	tied := map[string]any{}
	for key, value := range input {
		tied[key] = value
	}
	tied["Dependencies"] = []any{blocks, inheritable}
	if got := buildGitHubWorkItemDerivationOracleCandidates(t, tied); !reflect.DeepEqual(got, want) {
		t.Errorf("with a same-timestamp blocks row: sources %v teams %v, want sources %v teams %v", got.Source, got.TeamID, want.Source, want.TeamID)
	}
}
