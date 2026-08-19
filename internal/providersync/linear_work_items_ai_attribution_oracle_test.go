package providersync

import (
	"reflect"
	"testing"
)

func linearAIAttributionOracleInput() map[string]any {
	return map[string]any{
		"org_id":        "77777777-7777-4777-8777-777777777777",
		"signal_source": "issue_label",
		"history":       []any{},
		"raw_issue": map[string]any{
			"id": "lin-issue-3717", "identifier": "ENG-3717",
			"title":       "Codex is discussed, but only the label is attribution",
			"description": "Generated with Codex is ordinary issue text.",
			"priority":    2, "createdAt": "2026-08-01T08:00:00Z",
			"updatedAt": "2026-08-03T09:30:00Z",
			"state":     map[string]any{"name": "In Progress", "type": "started"},
			"labels": map[string]any{"nodes": []any{
				map[string]any{"name": "codex"}, map[string]any{"name": "bug"},
			}},
			"assignee": nil, "creator": nil,
			"team":    map[string]any{"id": "team-eng", "key": "ENG", "name": "Engineering"},
			"project": nil, "cycle": nil, "parent": nil,
		},
	}
}

func TestLinearAIAttributionMatchesLivePythonProductionRow(t *testing.T) {
	compareRowsAgainstPythonOracle(
		t, "linear/work-items/ai-attribution",
		[]oracleCase{{ID: "explicit_codex_issue_label", Input: linearAIAttributionOracleInput()}},
		buildLinearAIAttributionOracleRow, nil,
	)
}

func TestLinearAIAttributionIsRetryStable(t *testing.T) {
	input := linearAIAttributionOracleInput()
	first := buildLinearAIAttributionOracleRow(t, input)
	second := buildLinearAIAttributionOracleRow(t, input)
	if !reflect.DeepEqual(first, second) || first.RecordID == [16]byte{} {
		t.Fatalf("attribution changed across retry:\nfirst=%+v\nsecond=%+v", first, second)
	}
}

func buildLinearAIAttributionOracleRow(
	t *testing.T,
	input map[string]any,
) githubAIAttributionRow {
	t.Helper()
	claim := linearOracleClaim(input)
	item, _, err := normalizeLinearWorkItem(
		claim, linearOraclePayload(t, input), linearWorkItemOracleNormalizedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := normalizeLinearWorkItemAIAttributions(
		claim, linearWorkItemRows{WorkItems: []linearWorkItemRow{item}},
		linearWorkItemOracleNormalizedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("attribution rows=%d want 1: %+v", len(rows), rows)
	}
	return rows[0]
}
