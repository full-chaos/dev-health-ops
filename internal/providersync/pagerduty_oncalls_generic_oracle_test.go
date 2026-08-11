package providersync

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func buildPagerDutyOnCallRowForOracle(
	t *testing.T, input map[string]any,
) pagerDutyOnCallRow {
	t.Helper()
	encoded, err := json.Marshal(input["payload"])
	if err != nil {
		t.Fatal(err)
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var payload pagerDutyOnCallPayload
	if err := decoder.Decode(&payload); err != nil {
		t.Fatal(err)
	}
	observedAt, err := time.Parse(time.RFC3339Nano, input["observed_at"].(string))
	if err != nil {
		t.Fatal(err)
	}
	claim := nativeTestClaim("pagerduty", "on-calls")
	claim.OrgID = input["org_id"].(string)
	row, err := normalizePagerDutyOnCall(
		claim, strings.ToLower(input["provider_instance_id"].(string)), payload,
		observedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	return row
}

func oraclePagerDutyOnCallCases() []oracleCase {
	return []oracleCase{
		{ID: "explicit_id_updated_html_url", Input: map[string]any{
			"org_id": "org-acme", "provider_instance_id": "Acme",
			"observed_at": "2026-08-09T19:00:00.987654Z",
			"payload": map[string]any{
				"id": "OC1", "type": "oncall",
				"start": "2026-08-01T10:00:00.123456Z",
				"end":   "2026-08-01T18:00:00.123456Z", "escalation_level": 1,
				"user":              map[string]any{"id": "PU1"},
				"schedule":          map[string]any{"id": "PS1"},
				"escalation_policy": map[string]any{"id": "PE1"},
				"updated_at":        "2026-08-01T10:00:00.123456Z",
				"html_url":          "https://acme.pagerduty.com/oncalls/OC1", "self": "/oncalls/OC1",
			},
		}},
		{ID: "composite_id_created_self_url", Input: map[string]any{
			"org_id": "org-acme", "provider_instance_id": "ACME",
			"observed_at": "2026-08-09T19:00:00.987654Z",
			"payload": map[string]any{
				"type": "oncall", "start": "2026-08-02T10:00:00Z",
				"end": "2026-08-02T18:00:00Z", "escalation_level": 2,
				"user":              map[string]any{"id": "PU2"},
				"schedule":          map[string]any{"id": "PS2"},
				"escalation_policy": map[string]any{"id": "PE2"},
				"created_at":        "2026-07-31T09:00:00Z", "self": "/oncalls/composite",
			},
		}},
		{ID: "permanent_composite_observed_fallback", Input: map[string]any{
			"org_id": "org-acme", "provider_instance_id": "acme",
			"observed_at": "2026-08-09T19:00:00.987654Z",
			"payload": map[string]any{
				"type": "oncall", "escalation_level": 3,
				"user":              map[string]any{"id": "PU3"},
				"escalation_policy": map[string]any{"id": "PE3"},
			},
		}},
	}
}

func TestGenericOracleMatchesLivePythonForPagerDutyOnCallRow(t *testing.T) {
	compareRowsAgainstPythonOracle(
		t, "pagerduty/on-calls/row", oraclePagerDutyOnCallCases(),
		buildPagerDutyOnCallRowForOracle, nil,
	)
}
