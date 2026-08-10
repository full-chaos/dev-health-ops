package providersync

import (
	"encoding/json"
	"testing"
	"time"
)

func TestLinearReferenceProjectMatchesLivePythonProducer(t *testing.T) {
	compareRowsAgainstPythonOracle(
		t,
		"linear/work-items/reference-project",
		[]oracleCase{
			{
				ID: "active_with_lead_and_teams",
				Input: map[string]any{
					"org_id":        "org-acme",
					"normalized_at": "2026-08-10T12:34:56.789Z",
					"node": map[string]any{
						"id": "project-42", "name": "Platform", "trashed": false,
						"targetDate": "2026-09-30", "archivedAt": nil,
						"url":    "https://linear.app/project-42",
						"status": map[string]any{"id": "status-1", "name": "Completed", "type": "completed"},
						"lead":   map[string]any{"id": "user-7", "name": "Alice", "email": "alice@example.com"},
						"teams": map[string]any{"nodes": []any{
							map[string]any{"id": "team-2", "key": "OPS"},
							map[string]any{"id": "team-1", "key": "ENG"},
						}},
					},
				},
			},
			{
				ID: "trashed_without_lead",
				Input: map[string]any{
					"org_id":        "org-acme",
					"normalized_at": "2026-08-10T12:34:56.789Z",
					"node": map[string]any{
						"id": "project-43", "name": "Retired", "trashed": true,
						"targetDate": "not-a-date", "archivedAt": nil, "url": "",
						"status": map[string]any{"id": "status-2", "name": "Canceled", "type": "canceled"},
						"lead":   nil, "teams": map[string]any{"nodes": []any{}},
					},
				},
			},
		},
		buildLinearReferenceProjectOracleRow,
		nil,
	)
}

func buildLinearReferenceProjectOracleRow(t *testing.T, input map[string]any) linearReferenceProjectRow {
	t.Helper()
	raw, err := json.Marshal(input["node"])
	if err != nil {
		t.Fatal(err)
	}
	var payload linearReferenceProjectPayload
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatal(err)
	}
	normalizedAt, err := time.Parse(time.RFC3339Nano, input["normalized_at"].(string))
	if err != nil {
		t.Fatal(err)
	}
	claim := linearOracleClaim(input)
	row, err := normalizeLinearReferenceProject(claim, payload, normalizedAt)
	if err != nil {
		t.Fatal(err)
	}
	return row
}
