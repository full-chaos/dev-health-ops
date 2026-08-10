package providersync

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func buildPagerDutyTeamRowForOracle(
	t *testing.T, input map[string]any,
) pagerDutyTeamRow {
	t.Helper()
	encoded, err := json.Marshal(input["payload"])
	if err != nil {
		t.Fatal(err)
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var payload pagerDutyTeamPayload
	if err := decoder.Decode(&payload); err != nil {
		t.Fatal(err)
	}
	observedAt, err := time.Parse(time.RFC3339Nano, input["observed_at"].(string))
	if err != nil {
		t.Fatal(err)
	}
	claim := nativeTestClaim("pagerduty", "teams")
	claim.OrgID = input["org_id"].(string)
	row, err := normalizePagerDutyTeam(
		claim, strings.ToLower(input["provider_instance_id"].(string)), payload,
		observedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	return row
}

func oraclePagerDutyTeamCases() []oracleCase {
	return []oracleCase{
		{ID: "updated_html_url", Input: map[string]any{
			"org_id": "org-acme", "provider_instance_id": "Acme",
			"observed_at": "2026-08-09T19:00:00.987654Z",
			"payload": map[string]any{
				"id": "PT1", "type": "team", "name": "Platform",
				"description": "Platform response", "summary": "Ignored summary",
				"updated_at": "2026-08-01T10:00:00.123456Z",
				"html_url":   "https://acme.pagerduty.com/teams/PT1",
			},
		}},
		{ID: "created_self_url", Input: map[string]any{
			"org_id": "org-acme", "provider_instance_id": "ACME",
			"observed_at": "2026-08-09T19:00:00.987654Z",
			"payload": map[string]any{
				"id": "PT2", "type": "team", "summary": "Support",
				"created_at": "2026-07-31T09:00:00Z", "self": "/teams/PT2",
			},
		}},
		{ID: "observed_time_fallback", Input: map[string]any{
			"org_id": "org-acme", "provider_instance_id": "acme",
			"observed_at": "2026-08-09T19:00:00.987654Z",
			"payload": map[string]any{
				"id": "PT3", "type": "team", "name": "Tertiary",
			},
		}},
	}
}

func TestGenericOracleMatchesLivePythonForPagerDutyTeamRow(t *testing.T) {
	compareRowsAgainstPythonOracle(
		t, "pagerduty/teams/row", oraclePagerDutyTeamCases(),
		buildPagerDutyTeamRowForOracle, nil,
	)
}
