package providersync

import (
	"encoding/json"
	"testing"
	"time"
)

func TestLinearReferenceMemberMatchesLivePythonProducer(t *testing.T) {
	compareRowsAgainstPythonOracle(
		t,
		"linear/work-items/reference-member",
		[]oracleCase{
			{
				ID: "email_identity",
				Input: map[string]any{
					"org_id":        "org-acme",
					"team_id":       "ENG",
					"normalized_at": "2026-08-10T12:34:56.789Z",
					"member": map[string]any{
						"id": "user-7", "name": "Alice", "email": "alice@example.com",
					},
				},
			},
			{
				ID: "id_fallback_without_email",
				Input: map[string]any{
					"org_id":        "org-acme",
					"team_id":       "ENG",
					"normalized_at": "2026-08-10T12:34:56.789Z",
					"member": map[string]any{
						"id": "user-8", "name": "Bob", "email": nil,
					},
				},
			},
		},
		buildLinearReferenceMemberOracleRow,
		nil,
	)
}

func buildLinearReferenceMemberOracleRow(t *testing.T, input map[string]any) linearReferenceMemberRow {
	t.Helper()
	raw, err := json.Marshal(input["member"])
	if err != nil {
		t.Fatal(err)
	}
	var payload linearReferenceCatalogMemberPayload
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatal(err)
	}
	normalizedAt, err := time.Parse(time.RFC3339Nano, input["normalized_at"].(string))
	if err != nil {
		t.Fatal(err)
	}
	member, _, _, err := normalizeLinearReferenceMember(
		linearOracleClaim(input), input["team_id"].(string), payload, normalizedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	return member
}
