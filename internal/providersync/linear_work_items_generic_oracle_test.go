package providersync

import (
	"encoding/json"
	"testing"
	"time"
)

var linearWorkItemOracleNormalizedAt = time.Date(
	2026, 8, 3, 12, 0, 0, 987654321, time.UTC,
)

func TestLinearIssueWorkItemMatchesLivePythonProductionRow(t *testing.T) {
	compareRowsAgainstPythonOracle(
		t,
		"linear/work-items/issue",
		linearWorkItemOracleCases(),
		buildLinearWorkItemOracleRow,
		linearWorkItemWriteStampGoOnly,
	)
}

func TestLinearStatusTransitionMatchesLivePythonProductionRow(t *testing.T) {
	compareRowsAgainstPythonOracle(
		t,
		"linear/work-items/status-transition",
		linearWorkItemOracleCases(),
		buildLinearStatusTransitionOracleRow,
		linearWorkItemWriteStampGoOnly,
	)
}

var linearWorkItemWriteStampGoOnly = map[string]string{
	"last_synced": "Python's semantic WorkItem and WorkItemStatusTransition " +
		"dataclasses do not contain this persistence stamp. Go carries the unit's " +
		"deterministic normalizedAt so recovery snapshots replay byte-identically " +
		"instead of stamping wall-clock time during a retry.",
}

func linearWorkItemOracleCases() []oracleCase {
	return []oracleCase{
		{
			ID: "complete_issue_with_reopen",
			Input: map[string]any{
				"org_id":  "org-acme",
				"history": linearOracleHistory(),
				"raw_issue": map[string]any{
					"id": "lin-issue-42", "identifier": "ENG-42",
					"title":       "Preserve the Linear work-item contract",
					"description": "A non-empty issue exercises the canonical normalizer.",
					"priority":    2, "estimate": 5,
					"createdAt":   "2026-07-25T09:00:00Z",
					"updatedAt":   "2026-07-28T16:30:00Z",
					"startedAt":   "2026-07-26T10:00:00Z",
					"completedAt": "2026-07-31T09:00:00Z", "canceledAt": "2026-08-01T09:00:00Z",
					"dueDate": "2026-08-01T00:00:00Z",
					"url":     "https://linear.app/fullchaos/issue/ENG-42",
					"state":   map[string]any{"name": "In Progress", "type": "started"},
					"labels": map[string]any{"nodes": []any{
						map[string]any{"name": "bug"},
						map[string]any{"name": "priority::high"},
					}},
					"assignee": map[string]any{"email": "alice@example.com", "name": "Alice"},
					"creator":  map[string]any{"email": "bob@example.com", "name": "Bob"},
					"team":     map[string]any{"id": "team-eng", "key": "ENG", "name": "Engineering"},
					"project":  map[string]any{"id": "project-platform", "name": "Platform"},
					"cycle":    map[string]any{"id": "cycle-7", "name": "Sprint 7", "number": 7},
					"parent":   map[string]any{"identifier": "ENG-1"},
				},
			},
		},
		{
			ID: "optional_values_and_cycle_number",
			Input: map[string]any{
				"org_id": "org-acme",
				"history": []any{map[string]any{
					"createdAt": "2026-07-30T11:00:00.123456Z",
					"fromState": nil,
					"toState":   map[string]any{"name": "Backlog", "type": "backlog"},
					"actor":     nil,
				}},
				"raw_issue": map[string]any{
					"id": "lin-issue-43", "identifier": "ENG-43", "title": "Unscoped issue",
					"description": "", "priority": 7, "estimate": 2.5,
					"createdAt": "2026-07-29T09:00:00.123000Z",
					"updatedAt": "2026-07-30T11:00:00.654000Z",
					"startedAt": nil, "completedAt": nil, "canceledAt": nil,
					"dueDate": nil, "url": nil,
					"state":    map[string]any{"name": "Backlog", "type": "backlog"},
					"labels":   map[string]any{"nodes": []any{}},
					"assignee": nil, "creator": nil,
					"team":    map[string]any{"id": "team-eng", "key": "ENG", "name": "Engineering"},
					"project": nil,
					"cycle":   map[string]any{"id": "cycle-8", "name": "", "number": 8},
					"parent":  nil,
				},
			},
		},
	}
}

func linearOracleHistory() []any {
	return []any{
		map[string]any{
			"createdAt": "2026-07-26T10:00:00Z",
			"fromState": map[string]any{"name": "Todo", "type": "unstarted"},
			"toState":   map[string]any{"name": "In Progress", "type": "started"},
			"actor":     map[string]any{"email": "alice@example.com", "name": "Alice"},
		},
		map[string]any{
			"createdAt": "2026-07-27T11:00:00Z",
			"fromState": map[string]any{"name": "Done", "type": "completed"},
			"toState":   map[string]any{"name": "In Progress", "type": "started"},
			"actor":     map[string]any{"email": "bob@example.com", "name": "Bob"},
		},
	}
}

func buildLinearWorkItemOracleRow(t *testing.T, input map[string]any) linearWorkItemRow {
	t.Helper()
	payload := linearOraclePayload(t, input)
	item, _, err := normalizeLinearWorkItem(
		linearOracleClaim(input), payload, linearWorkItemOracleNormalizedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	return item
}

func buildLinearStatusTransitionOracleRow(t *testing.T, input map[string]any) linearWorkItemTransitionRow {
	t.Helper()
	payload := linearOraclePayload(t, input)
	_, transitions, err := normalizeLinearWorkItem(
		linearOracleClaim(input), payload, linearWorkItemOracleNormalizedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	index, ok := input["row_index"]
	if !ok {
		index = 0
	}
	rowIndex, ok := index.(int)
	if !ok {
		rowIndex = int(index.(float64))
	}
	if rowIndex < 0 || rowIndex >= len(transitions) {
		t.Fatalf("transition index=%d rows=%d", rowIndex, len(transitions))

	}
	return transitions[rowIndex]
}

func linearOracleClaim(input map[string]any) Claim {
	claim := nativeTestClaim("linear", "work-items")
	claim.SourceExternalID = "ENG"
	claim.OrgID = input["org_id"].(string)
	return claim
}

func linearOraclePayload(t *testing.T, input map[string]any) linearWorkItemPayload {
	t.Helper()
	issue := make(map[string]any, len(input["raw_issue"].(map[string]any))+1)
	for key, value := range input["raw_issue"].(map[string]any) {
		issue[key] = value
	}
	issue["history"] = map[string]any{"nodes": input["history"]}
	raw, err := json.Marshal(issue)
	if err != nil {
		t.Fatal(err)
	}
	var payload linearWorkItemPayload
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatal(err)
	}
	return payload
}
