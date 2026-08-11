package providersync

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func buildPagerDutyIncidentRowForOracle(t *testing.T, input map[string]any) pagerDutyIncidentRow {
	t.Helper()
	var payload pagerDutyIncidentPayload
	encoded, err := json.Marshal(input["payload"])
	if err != nil {
		t.Fatal(err)
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	if err := decoder.Decode(&payload); err != nil {
		t.Fatal(err)
	}
	observedAt, err := time.Parse(time.RFC3339Nano, input["observed_at"].(string))
	if err != nil {
		t.Fatal(err)
	}
	claim := nativeTestClaim("pagerduty", "incidents")
	claim.OrgID = input["org_id"].(string)
	row, err := normalizePagerDutyIncident(claim, strings.ToLower(input["provider_instance_id"].(string)), payload, observedAt)
	if err != nil {
		t.Fatal(err)
	}
	return row
}

func buildPagerDutyAlertRowForOracle(t *testing.T, input map[string]any) pagerDutyAlertRow {
	t.Helper()
	var payload pagerDutyAlertPayload
	var incident pagerDutyIncidentPayload
	encoded, err := json.Marshal(input["payload"])
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(encoded, &payload); err != nil {
		t.Fatal(err)
	}
	encoded, err = json.Marshal(input["incident"])
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(encoded, &incident); err != nil {
		t.Fatal(err)
	}
	observedAt, err := time.Parse(time.RFC3339Nano, input["observed_at"].(string))
	if err != nil {
		t.Fatal(err)
	}
	claim := nativeTestClaim("pagerduty", "incident-alerts")
	claim.OrgID = input["org_id"].(string)
	providerInstance := strings.ToLower(input["provider_instance_id"].(string))
	parent, err := normalizePagerDutyIncident(claim, providerInstance, incident, observedAt)
	if err != nil {
		t.Fatal(err)
	}
	row, err := normalizePagerDutyAlert(claim, providerInstance, payload, parent.ID, observedAt)
	if err != nil {
		t.Fatal(err)
	}
	return row
}

func buildPagerDutyLogEntryRowForOracle(t *testing.T, input map[string]any) pagerDutyLogEntryRow {
	t.Helper()
	var payload pagerDutyLogEntryPayload
	var incident pagerDutyIncidentPayload
	encoded, err := json.Marshal(input["payload"])
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(encoded, &payload); err != nil {
		t.Fatal(err)
	}
	encoded, err = json.Marshal(input["incident"])
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(encoded, &incident); err != nil {
		t.Fatal(err)
	}
	observedAt, err := time.Parse(time.RFC3339Nano, input["observed_at"].(string))
	if err != nil {
		t.Fatal(err)
	}
	claim := nativeTestClaim("pagerduty", "incident-log-entries")
	claim.OrgID = input["org_id"].(string)
	providerInstance := strings.ToLower(input["provider_instance_id"].(string))
	parent, err := normalizePagerDutyIncident(claim, providerInstance, incident, observedAt)
	if err != nil {
		t.Fatal(err)
	}
	row, err := normalizePagerDutyLogEntry(claim, providerInstance, payload, parent.ID, observedAt)
	if err != nil {
		t.Fatal(err)
	}
	return row
}

func buildPagerDutyNoteRowForOracle(t *testing.T, input map[string]any) pagerDutyNoteRow {
	t.Helper()
	var payload pagerDutyNotePayload
	var incident pagerDutyIncidentPayload
	encoded, err := json.Marshal(input["payload"])
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(encoded, &payload); err != nil {
		t.Fatal(err)
	}
	encoded, err = json.Marshal(input["incident"])
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(encoded, &incident); err != nil {
		t.Fatal(err)
	}
	observedAt, err := time.Parse(time.RFC3339Nano, input["observed_at"].(string))
	if err != nil {
		t.Fatal(err)
	}
	claim := nativeTestClaim("pagerduty", "incident-notes")
	claim.OrgID = input["org_id"].(string)
	providerInstance := strings.ToLower(input["provider_instance_id"].(string))
	parent, err := normalizePagerDutyIncident(claim, providerInstance, incident, observedAt)
	if err != nil {
		t.Fatal(err)
	}
	row, err := normalizePagerDutyNote(claim, providerInstance, payload, parent.ID, observedAt)
	if err != nil {
		t.Fatal(err)
	}
	return row
}

func pagerDutyIncidentOracleBase() map[string]any {
	return map[string]any{
		"org_id": "org-acme", "provider_instance_id": "Acme",
		"observed_at": "2026-07-19T19:00:00.987654Z",
	}
}

func pagerDutyIncidentOracleParent() map[string]any {
	return map[string]any{
		"id": "PI-PARENT", "type": "incident", "incident_number": 7,
		"title": "Parent", "status": "triggered", "urgency": "high",
		"created_at": "2026-07-17T12:00:00.123456Z", "updated_at": "2026-07-17T12:01:00.654321Z",
		"service": map[string]any{"id": "PSVC1"}, "priority": map[string]any{"id": "P1", "summary": "P1"},
		"html_url": "https://acme.pagerduty.com/incidents/PI-PARENT",
	}
}

func TestGenericOracleMatchesLivePythonForPagerDutyIncidentRows(t *testing.T) {
	base := pagerDutyIncidentOracleBase()
	cases := []oracleCase{
		{ID: "resolved_full", Input: map[string]any{
			"org_id": base["org_id"], "provider_instance_id": base["provider_instance_id"], "observed_at": base["observed_at"],
			"payload": map[string]any{
				"id": "PI1", "type": "incident", "incident_number": 42, "title": "Database outage", "status": "resolved", "urgency": "high",
				"created_at": "2026-07-17T12:00:00.123456Z", "updated_at": "2026-07-18T10:00:00.654321Z", "resolved_at": "2026-07-18T09:00:00Z",
				"last_status_change_at": "2026-07-18T09:01:00Z", "service": map[string]any{"id": "PSVC1"}, "priority": map[string]any{"id": "P1", "summary": "P1"},
				"html_url": "https://acme.pagerduty.com/incidents/PI1",
			},
		}},
		{ID: "acknowledged_created_self", Input: map[string]any{
			"org_id": "org-acme", "provider_instance_id": "ACME", "observed_at": base["observed_at"],
			"payload": map[string]any{"id": "PI2", "type": "incident", "summary": "Support", "status": "acknowledged", "urgency": "low", "created_at": "2026-07-17T13:00:00Z", "self": "/incidents/PI2"},
		}},
		{ID: "observed_fallback", Input: map[string]any{
			"org_id": "org-acme", "provider_instance_id": "acme", "observed_at": base["observed_at"],
			"payload": map[string]any{"id": "PI3", "type": "incident", "title": "Fallback", "status": "triggered", "urgency": "unknown", "priority": map[string]any{"id": "P9"}},
		}},
	}
	compareRowsAgainstPythonOracle(t, "pagerduty/incidents/row", cases, buildPagerDutyIncidentRowForOracle, nil)
}

func TestGenericOracleMatchesLivePythonForPagerDutyAlertRows(t *testing.T) {
	cases := []oracleCase{
		{ID: "resolved_critical", Input: map[string]any{
			"org_id": "org-acme", "provider_instance_id": "Acme", "observed_at": "2026-07-19T19:00:00.987654Z", "incident": pagerDutyIncidentOracleParent(),
			"payload": map[string]any{"id": "PA1", "type": "alert", "summary": "Disk alert", "status": "resolved", "severity": "critical", "created_at": "2026-07-17T12:02:00Z", "updated_at": "2026-07-17T12:04:00Z", "self": "/incidents/PI-PARENT/alerts/PA1"},
		}},
		{ID: "triggered_warning", Input: map[string]any{
			"org_id": "org-acme", "provider_instance_id": "acme", "observed_at": "2026-07-19T19:00:00.987654Z", "incident": pagerDutyIncidentOracleParent(),
			"payload": map[string]any{"id": "PA2", "summary": "Warning", "status": "triggered", "severity": "warning", "created_at": "2026-07-17T12:05:00Z"},
		}},
	}
	compareRowsAgainstPythonOracle(t, "pagerduty/incident-alerts/row", cases, buildPagerDutyAlertRowForOracle, nil)
}

func TestGenericOracleMatchesLivePythonForPagerDutyLogEntryRows(t *testing.T) {
	cases := []oracleCase{
		{ID: "typed_summary", Input: map[string]any{
			"org_id": "org-acme", "provider_instance_id": "Acme", "observed_at": "2026-07-19T19:00:00.987654Z", "incident": pagerDutyIncidentOracleParent(),
			"payload": map[string]any{"id": "PL1", "type": "status_change", "summary": "Triggered", "created_at": "2026-07-17T12:03:00Z", "updated_at": "2026-07-17T12:04:00Z", "html_url": "https://acme.pagerduty.com/log_entries/PL1"},
		}},
		{ID: "message_default_type", Input: map[string]any{
			"org_id": "org-acme", "provider_instance_id": "acme", "observed_at": "2026-07-19T19:00:00.987654Z", "incident": pagerDutyIncidentOracleParent(),
			"payload": map[string]any{"id": "PL2", "message": "Fallback message", "created_at": "2026-07-17T12:05:00Z"},
		}},
	}
	compareRowsAgainstPythonOracle(t, "pagerduty/incident-log-entries/row", cases, buildPagerDutyLogEntryRowForOracle, nil)
}

func TestGenericOracleMatchesLivePythonForPagerDutyNoteRows(t *testing.T) {
	cases := []oracleCase{
		{ID: "content_with_user", Input: map[string]any{
			"org_id": "org-acme", "provider_instance_id": "Acme", "observed_at": "2026-07-19T19:00:00.987654Z", "incident": pagerDutyIncidentOracleParent(),
			"payload": map[string]any{"id": "PN1", "content": "Investigating", "created_at": "2026-07-17T12:06:00Z", "updated_at": "2026-07-17T12:07:00Z", "user": map[string]any{"id": "PU1"}},
		}},
		{ID: "empty_content", Input: map[string]any{
			"org_id": "org-acme", "provider_instance_id": "acme", "observed_at": "2026-07-19T19:00:00.987654Z", "incident": pagerDutyIncidentOracleParent(),
			"payload": map[string]any{"id": "PN2", "content": nil, "created_at": "2026-07-17T12:08:00Z"},
		}},
	}
	compareRowsAgainstPythonOracle(t, "pagerduty/incident-notes/row", cases, buildPagerDutyNoteRowForOracle, nil)
}
