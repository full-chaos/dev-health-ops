package providersync

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func buildPagerDutyBusinessServiceRowForOracle(
	t *testing.T, input map[string]any,
) pagerDutyBusinessServiceRow {
	t.Helper()
	encoded, err := json.Marshal(input["payload"])
	if err != nil {
		t.Fatal(err)
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var payload pagerDutyBusinessServicePayload
	if err := decoder.Decode(&payload); err != nil {
		t.Fatal(err)
	}
	observedAt, err := time.Parse(time.RFC3339Nano, input["observed_at"].(string))
	if err != nil {
		t.Fatal(err)
	}
	claim := nativeTestClaim("pagerduty", "business-services")
	claim.OrgID = input["org_id"].(string)
	row, err := normalizePagerDutyBusinessService(
		claim, strings.ToLower(input["provider_instance_id"].(string)), payload,
		observedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	return row
}

func oraclePagerDutyBusinessServiceCases() []oracleCase {
	return []oracleCase{
		{ID: "updated_html_url_description", Input: map[string]any{
			"org_id": "org-acme", "provider_instance_id": "Acme",
			"observed_at": "2026-08-09T19:00:00.987654Z",
			"payload": map[string]any{
				"id": "PBS1", "type": "business_service", "name": "Payments",
				"summary": "Ignored summary", "description": "Checkout path",
				"updated_at": "2026-08-01T10:00:00.123456Z",
				"html_url":   "https://acme.pagerduty.com/business_services/PBS1",
			},
		}},
		{ID: "created_self_url_summary", Input: map[string]any{
			"org_id": "org-acme", "provider_instance_id": "ACME",
			"observed_at": "2026-08-09T19:00:00.987654Z",
			"payload": map[string]any{
				"id": "PBS2", "type": "business_service", "summary": "Support",
				"created_at": "2026-07-31T09:00:00Z",
				"self":       "/business_services/PBS2",
			},
		}},
		{ID: "observed_time_fallback", Input: map[string]any{
			"org_id": "org-acme", "provider_instance_id": "acme",
			"observed_at": "2026-08-09T19:00:00.987654Z",
			"payload": map[string]any{
				"id": "PBS3", "type": "business_service", "name": "Operations",
			},
		}},
	}
}

func TestGenericOracleMatchesLivePythonForPagerDutyBusinessServiceRow(t *testing.T) {
	compareRowsAgainstPythonOracle(
		t, "pagerduty/business-services/row", oraclePagerDutyBusinessServiceCases(),
		buildPagerDutyBusinessServiceRowForOracle, nil,
	)
}
