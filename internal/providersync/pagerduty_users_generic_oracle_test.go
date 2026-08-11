package providersync

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func buildPagerDutyUserRowForOracle(
	t *testing.T, input map[string]any,
) pagerDutyUserRow {
	t.Helper()
	encoded, err := json.Marshal(input["payload"])
	if err != nil {
		t.Fatal(err)
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var payload pagerDutyUserPayload
	if err := decoder.Decode(&payload); err != nil {
		t.Fatal(err)
	}
	observedAt, err := time.Parse(time.RFC3339Nano, input["observed_at"].(string))
	if err != nil {
		t.Fatal(err)
	}
	claim := nativeTestClaim("pagerduty", "users")
	claim.OrgID = input["org_id"].(string)
	row, err := normalizePagerDutyUser(
		claim, strings.ToLower(input["provider_instance_id"].(string)), payload,
		observedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	return row
}

func oraclePagerDutyUserCases() []oracleCase {
	return []oracleCase{
		{ID: "updated_html_url", Input: map[string]any{
			"org_id": "org-acme", "provider_instance_id": "Acme",
			"observed_at": "2026-08-09T19:00:00.987654Z",
			"payload": map[string]any{
				"id": "PU1", "type": "user", "name": "Alice",
				"email": "alice@example.com", "summary": "Ignored summary",
				"updated_at": "2026-08-01T10:00:00.123456Z",
				"html_url":   "https://acme.pagerduty.com/users/PU1",
			},
		}},
		{ID: "created_self_url", Input: map[string]any{
			"org_id": "org-acme", "provider_instance_id": "ACME",
			"observed_at": "2026-08-09T19:00:00.987654Z",
			"payload": map[string]any{
				"id": "PU2", "type": "user", "summary": "Bob",
				"created_at": "2026-07-31T09:00:00Z", "self": "/users/PU2",
			},
		}},
		{ID: "observed_time_fallback", Input: map[string]any{
			"org_id": "org-acme", "provider_instance_id": "acme",
			"observed_at": "2026-08-09T19:00:00.987654Z",
			"payload": map[string]any{
				"id": "PU3", "type": "user",
			},
		}},
	}
}

func TestGenericOracleMatchesLivePythonForPagerDutyUserRow(t *testing.T) {
	compareRowsAgainstPythonOracle(
		t, "pagerduty/users/row", oraclePagerDutyUserCases(),
		buildPagerDutyUserRowForOracle, nil,
	)
}
