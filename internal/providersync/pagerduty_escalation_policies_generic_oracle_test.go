package providersync

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func buildPagerDutyEscalationPolicyRowForOracle(
	t *testing.T, input map[string]any,
) pagerDutyEscalationPolicyRow {
	t.Helper()
	encoded, err := json.Marshal(input["payload"])
	if err != nil {
		t.Fatal(err)
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var payload pagerDutyEscalationPolicyPayload
	if err := decoder.Decode(&payload); err != nil {
		t.Fatal(err)
	}
	observedAt, err := time.Parse(time.RFC3339Nano, input["observed_at"].(string))
	if err != nil {
		t.Fatal(err)
	}
	claim := nativeTestClaim("pagerduty", "escalation-policies")
	claim.OrgID = input["org_id"].(string)
	row, err := normalizePagerDutyEscalationPolicy(
		claim, strings.ToLower(input["provider_instance_id"].(string)), payload,
		observedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	return row
}

func oraclePagerDutyEscalationPolicyCases() []oracleCase {
	return []oracleCase{
		{ID: "updated_html_url", Input: map[string]any{
			"org_id": "org-acme", "provider_instance_id": "Acme",
			"observed_at": "2026-08-09T19:00:00.987654Z",
			"payload": map[string]any{
				"id": "PESCAL1", "type": "escalation_policy", "name": "Primary",
				"summary": "Ignored summary", "updated_at": "2026-08-01T10:00:00.123456Z",
				"html_url": "https://acme.pagerduty.com/escalation_policies/PESCAL1",
			},
		}},
		{ID: "created_self_url", Input: map[string]any{
			"org_id": "org-acme", "provider_instance_id": "ACME",
			"observed_at": "2026-08-09T19:00:00.987654Z",
			"payload": map[string]any{
				"id": "PESCAL2", "type": "escalation_policy", "summary": "Secondary",
				"created_at": "2026-07-31T09:00:00Z", "self": "/escalation_policies/PESCAL2",
			},
		}},
		{ID: "observed_time_fallback", Input: map[string]any{
			"org_id": "org-acme", "provider_instance_id": "acme",
			"observed_at": "2026-08-09T19:00:00.987654Z",
			"payload": map[string]any{
				"id": "PESCAL3", "type": "escalation_policy", "name": "Tertiary",
			},
		}},
	}
}

func TestGenericOracleMatchesLivePythonForPagerDutyEscalationPolicyRow(t *testing.T) {
	compareRowsAgainstPythonOracle(
		t, "pagerduty/escalation-policies/row", oraclePagerDutyEscalationPolicyCases(),
		buildPagerDutyEscalationPolicyRowForOracle, nil,
	)
}
