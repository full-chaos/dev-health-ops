package providersync

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func buildPagerDutyScheduleRowForOracle(
	t *testing.T, input map[string]any,
) pagerDutyScheduleRow {
	t.Helper()
	encoded, err := json.Marshal(input["payload"])
	if err != nil {
		t.Fatal(err)
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var payload pagerDutySchedulePayload
	if err := decoder.Decode(&payload); err != nil {
		t.Fatal(err)
	}
	observedAt, err := time.Parse(time.RFC3339Nano, input["observed_at"].(string))
	if err != nil {
		t.Fatal(err)
	}
	claim := nativeTestClaim("pagerduty", "schedules")
	claim.OrgID = input["org_id"].(string)
	row, err := normalizePagerDutySchedule(
		claim, strings.ToLower(input["provider_instance_id"].(string)), payload,
		observedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	return row
}

func oraclePagerDutyScheduleCases() []oracleCase {
	return []oracleCase{
		{ID: "updated_html_url_timezone", Input: map[string]any{
			"org_id": "org-acme", "provider_instance_id": "Acme",
			"observed_at": "2026-08-09T19:00:00.987654Z",
			"payload": map[string]any{
				"id": "PS1", "type": "schedule", "name": "Primary",
				"summary": "Ignored summary", "time_zone": "America/Los_Angeles",
				"updated_at": "2026-08-01T10:00:00.123456Z",
				"html_url":   "https://acme.pagerduty.com/schedules/PS1",
			},
		}},
		{ID: "created_self_url_summary", Input: map[string]any{
			"org_id": "org-acme", "provider_instance_id": "ACME",
			"observed_at": "2026-08-09T19:00:00.987654Z",
			"payload": map[string]any{
				"id": "PS2", "type": "schedule", "summary": "Support",
				"created_at": "2026-07-31T09:00:00Z", "self": "/schedules/PS2",
			},
		}},
		{ID: "observed_time_fallback", Input: map[string]any{
			"org_id": "org-acme", "provider_instance_id": "acme",
			"observed_at": "2026-08-09T19:00:00.987654Z",
			"payload": map[string]any{
				"id": "PS3", "type": "schedule", "name": "Operations",
			},
		}},
	}
}

func TestGenericOracleMatchesLivePythonForPagerDutyScheduleRow(t *testing.T) {
	compareRowsAgainstPythonOracle(
		t, "pagerduty/schedules/row", oraclePagerDutyScheduleCases(),
		buildPagerDutyScheduleRowForOracle, nil,
	)
}
