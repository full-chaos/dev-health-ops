package providersync

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func buildPagerDutyServiceRowForOracle(t *testing.T, input map[string]any) pagerDutyServiceRow {
	t.Helper()
	encoded, err := json.Marshal(input["payload"])
	if err != nil {
		t.Fatal(err)
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var payload pagerDutyServicePayload
	if err := decoder.Decode(&payload); err != nil {
		t.Fatal(err)
	}
	observedAt, err := time.Parse(time.RFC3339Nano, input["observed_at"].(string))
	if err != nil {
		t.Fatal(err)
	}
	claim := nativeTestClaim("pagerduty", "services")
	claim.OrgID = input["org_id"].(string)
	row, err := normalizePagerDutyService(
		claim, strings.ToLower(input["provider_instance_id"].(string)), payload, observedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	return row
}

func oraclePagerDutyServiceCases() []oracleCase {
	return []oracleCase{
		{ID: "updated_html_url_policy", Input: map[string]any{
			"org_id": "org-acme", "provider_instance_id": "Acme",
			"observed_at": "2026-08-09T19:00:00.987654Z",
			"payload": map[string]any{
				"id": "PS1", "type": "service", "name": "Payments", "summary": "Ignored",
				"updated_at":        "2026-08-01T10:00:00.123456Z",
				"html_url":          "https://acme.pagerduty.com/services/PS1",
				"escalation_policy": map[string]any{"id": "PE1", "type": "escalation_policy"},
			},
		}},
		{ID: "created_self_url_summary", Input: map[string]any{
			"org_id": "org-acme", "provider_instance_id": "ACME",
			"observed_at": "2026-08-09T19:00:00.987654Z",
			"payload": map[string]any{
				"id": "PS2", "type": "service", "summary": "Support",
				"created_at": "2026-07-31T09:00:00Z", "self": "/services/PS2",
			},
		}},
		{ID: "observed_time_fallback", Input: map[string]any{
			"org_id": "org-acme", "provider_instance_id": "acme",
			"observed_at": "2026-08-09T19:00:00.987654Z",
			"payload":     map[string]any{"id": "PS3", "type": "service", "name": "Operations"},
		}},
	}
}

func TestGenericOracleMatchesLivePythonForPagerDutyServiceRow(t *testing.T) {
	compareRowsAgainstPythonOracle(
		t, "pagerduty/services/row", oraclePagerDutyServiceCases(),
		buildPagerDutyServiceRowForOracle, nil,
	)
}
