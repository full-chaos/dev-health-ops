package providersync

import (
	"encoding/json"
	"testing"
)

func TestLinearReferenceTeamMatchesLivePythonProducer(t *testing.T) {
	compareRowsAgainstPythonOracle(
		t, "linear/work-items/team", []oracleCase{
			{
				ID: "project_key_candidate",
				Input: map[string]any{
					"team_key": "ENG",
					"reference": map[string]any{
						"provider": "", "id": "team-eng", "name": "Engineering",
						"native_team_key": "", "project_keys": []any{"ENG"},
					},
				},
			},
			{
				ID: "native_key_candidate",
				Input: map[string]any{
					"team_key": "PLAT",
					"reference": map[string]any{
						"provider": "linear", "id": "", "name": "",
						"native_team_key": "PLAT", "project_keys": []any{},
					},
				},
			},
		},
		buildLinearReferenceTeamOracleRow,
		nil,
	)
}

func buildLinearReferenceTeamOracleRow(t *testing.T, input map[string]any) linearTeamPayload {
	t.Helper()
	raw, err := json.Marshal(input["reference"])
	if err != nil {
		t.Fatal(err)
	}
	var reference struct {
		Provider      string   `json:"provider"`
		ID            string   `json:"id"`
		Name          string   `json:"name"`
		NativeTeamKey string   `json:"native_team_key"`
		ProjectKeys   []string `json:"project_keys"`
	}
	if err := json.Unmarshal(raw, &reference); err != nil {
		t.Fatal(err)
	}
	team, ok := linearReferenceTeamPayload([]LinearReferenceTeam{{
		Provider: reference.Provider, ID: reference.ID, Name: reference.Name,
		NativeTeamKey: reference.NativeTeamKey, ProjectKeys: reference.ProjectKeys,
	}}, input["team_key"].(string))
	if !ok {
		t.Fatalf("reference team did not resolve: %+v", reference)
	}
	return team
}

func TestLinearDependencyMatchesLivePythonProductionRows(t *testing.T) {
	compareRowsAgainstPythonOracle(
		t, "linear/work-items/dependency", []oracleCase{
			{
				ID: "attachment_and_native_relation",
				Input: map[string]any{
					"org_id":       "org-acme",
					"work_item_id": "linear:ENG-42",
					"raw_issue": linearRawIssue(map[string]any{
						"attachments": map[string]any{"nodes": []any{
							map[string]any{"url": "https://github.com/acme/repo/pull/9", "sourceType": "github"},
							map[string]any{"url": "https://github.com/acme/repo/pull/9", "sourceType": "github"},
							map[string]any{"url": "https://evil.example/acme/repo/pull/10", "sourceType": "github"},
						}},
						"relations": map[string]any{"nodes": []any{
							map[string]any{"type": "blocked_by", "issue": map[string]any{"identifier": "ENG-42"}, "relatedIssue": map[string]any{"identifier": "ENG-1"}},
						}},
						"inverseRelations": map[string]any{"nodes": []any{
							map[string]any{"type": "blocked_by", "issue": map[string]any{"identifier": "ENG-42"}, "relatedIssue": map[string]any{"identifier": "ENG-1"}},
						}},
					}),
					"row_index": 1,
				},
			},
			{
				ID: "gitlab_nested_project_attachment",
				Input: map[string]any{
					"org_id":       "org-acme",
					"work_item_id": "linear:ENG-42",
					"raw_issue": linearRawIssue(map[string]any{
						"attachments": map[string]any{"nodes": []any{
							map[string]any{"url": "https://gitlab.com/group/subgroup/project/-/merge_requests/17", "sourceType": "gitlab"},
						}},
					}),
				},
			},
		},
		buildLinearDependencyOracleRow,
		nil,
	)
}

func TestLinearReopenMatchesLivePythonProductionRows(t *testing.T) {
	compareRowsAgainstPythonOracle(
		t, "linear/work-items/reopen", []oracleCase{{
			ID: "completed_to_started",
			Input: map[string]any{
				"org_id": "org-acme", "work_item_id": "linear:ENG-42",
				"history": []any{map[string]any{
					"createdAt": "2026-07-27T11:00:00Z",
					"fromState": map[string]any{"name": "Done", "type": "completed"},
					"toState":   map[string]any{"name": "In Progress", "type": "started"},
					"actor":     map[string]any{"email": "bob@example.com", "name": "Bob"},
				}},
			},
		}},
		buildLinearReopenOracleRow,
		nil,
	)
}

func TestLinearInteractionMatchesLivePythonProductionRows(t *testing.T) {
	compareRowsAgainstPythonOracle(
		t, "linear/work-items/interaction", []oracleCase{{
			ID: "unicode_comment",
			Input: map[string]any{
				"org_id": "org-acme", "work_item_id": "linear:ENG-42",
				"raw_comment": map[string]any{
					"body": "hello 🌍", "createdAt": "2026-07-27T12:00:00Z",
					"user": map[string]any{"email": "alice@example.com", "name": "Alice"},
				},
			},
		}},
		buildLinearInteractionOracleRow,
		nil,
	)
}

func TestLinearSprintMatchesLivePythonProductionRows(t *testing.T) {
	compareRowsAgainstPythonOracle(
		t, "linear/work-items/sprint", []oracleCase{
			{
				ID: "closed_cycle",
				Input: map[string]any{
					"org_id": "org-acme", "native_team_key": "ENG",
					"raw_cycle": map[string]any{
						"id": "cycle-7", "number": 7, "name": "",
						"startsAt": "2026-07-25T09:00:00Z", "endsAt": "2026-08-01T09:00:00Z",
						"completedAt": "2026-08-01T09:00:00Z", "progress": 1,
					},
				},
			},
			{
				ID: "active_cycle",
				Input: map[string]any{
					"org_id": "org-acme", "native_team_key": "ENG",
					"raw_cycle": map[string]any{
						"id": "cycle-8", "number": 8, "name": "Sprint 8",
						"startsAt": nil, "endsAt": nil, "completedAt": nil, "progress": 0.25,
					},
				},
			},
		},
		buildLinearSprintOracleRow,
		nil,
	)
}

func linearRawIssue(fields map[string]any) map[string]any {
	issue := map[string]any{
		"id": "lin-issue-42", "identifier": "ENG-42", "title": "Raw Linear issue",
		"createdAt": "2026-07-25T09:00:00Z", "updatedAt": "2026-07-28T16:30:00Z",
		"state": map[string]any{"name": "In Progress", "type": "started"},
	}
	for key, value := range fields {
		issue[key] = value
	}
	return issue
}

func linearRawPayload(t *testing.T, input map[string]any) linearWorkItemPayload {
	t.Helper()
	raw, err := json.Marshal(input["raw_issue"])
	if err != nil {
		t.Fatal(err)
	}
	var payload linearWorkItemPayload
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatal(err)
	}
	return payload
}

func buildLinearDependencyOracleRow(t *testing.T, input map[string]any) linearWorkItemDependencyRow {
	payload := linearRawPayload(t, input)
	rows := normalizeLinearDependencies(
		linearOracleClaim(input), payload, input["work_item_id"].(string), linearWorkItemOracleNormalizedAt,
	)
	index := oracleRowIndex(input)
	if index < 0 || index >= len(rows) {
		t.Fatalf("dependency index=%d rows=%d", index, len(rows))
	}
	return rows[index]
}

func buildLinearReopenOracleRow(t *testing.T, input map[string]any) linearWorkItemReopenRow {
	history := linearHistoryEntries(t, input["history"])
	rows := normalizeLinearReopens(
		linearOracleClaim(input), input["work_item_id"].(string), history, linearWorkItemOracleNormalizedAt,
	)
	index := oracleRowIndex(input)
	if index < 0 || index >= len(rows) {
		t.Fatalf("reopen index=%d rows=%d", index, len(rows))
	}
	return rows[index]
}

func buildLinearInteractionOracleRow(t *testing.T, input map[string]any) linearWorkItemInteractionRow {
	commentRaw, err := json.Marshal(input["raw_comment"])
	if err != nil {
		t.Fatal(err)
	}
	var comment linearCommentPayload
	if err := json.Unmarshal(commentRaw, &comment); err != nil {
		t.Fatal(err)
	}
	rows := normalizeLinearInteractions(
		linearOracleClaim(input), input["work_item_id"].(string), []linearCommentPayload{comment}, linearWorkItemOracleNormalizedAt,
	)
	if len(rows) != 1 {
		t.Fatalf("interaction rows=%d", len(rows))
	}
	return rows[0]
}

func buildLinearSprintOracleRow(t *testing.T, input map[string]any) linearSprintRow {
	raw, err := json.Marshal(input["raw_cycle"])
	if err != nil {
		t.Fatal(err)
	}
	var cycle linearCyclePayload
	if err := json.Unmarshal(raw, &cycle); err != nil {
		t.Fatal(err)
	}
	row, err := normalizeLinearSprint(linearOracleClaim(input), cycle, linearWorkItemOracleNormalizedAt)
	if err != nil {
		t.Fatal(err)
	}
	return row
}

func linearHistoryEntries(t *testing.T, value any) []linearHistoryEntry {
	t.Helper()
	raw, err := json.Marshal(map[string]any{"nodes": value})
	if err != nil {
		t.Fatal(err)
	}
	var history linearHistoryPayload
	if err := json.Unmarshal(raw, &history); err != nil {
		t.Fatal(err)
	}
	return history.Nodes
}

func oracleRowIndex(input map[string]any) int {
	if raw, ok := input["row_index"]; ok {
		switch value := raw.(type) {
		case int:
			return value
		case float64:
			return int(value)
		}
	}
	return 0
}
