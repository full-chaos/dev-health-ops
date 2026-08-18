package providersync

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"
)

var oracleJiraIncidentObservedAt = time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC)

func buildJiraIncidentRowForOracle(t *testing.T, input map[string]any) jiraIncidentRow {
	t.Helper()
	encoded, err := json.Marshal(input["raw_issue"])
	if err != nil {
		t.Fatal(err)
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var issue jiraIncidentPayload
	if err := decoder.Decode(&issue); err != nil {
		t.Fatal(err)
	}
	row, err := normalizeJiraIncident(
		nativeTestClaim("jira", "incidents"), input["cloud_id"].(string),
		input["base_url"].(string), issue, oracleJiraIncidentObservedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	return row
}

func oracleJiraIncidentCases() []oracleCase {
	return []oracleCase{
		{ID: "active_with_priority", Input: map[string]any{
			"cloud_id": "cloud-123", "base_url": "https://acme.atlassian.net",
			"raw_issue": map[string]any{"id": "10001", "key": "JSM-1", "fields": map[string]any{
				"summary": "API down", "created": "2026-07-22T10:00:00Z", "updated": "2026-07-22T10:05:00Z", "resolutiondate": nil,
				"status":   map[string]any{"name": "Investigating", "statusCategory": map[string]any{"key": "indeterminate"}},
				"priority": map[string]any{"name": "Highest"},
			}},
		}},
		// CHAOS-3869: the shapes Jira Cloud REST actually returns -- a numeric
		// "+0000" offset with millisecond precision, which strict RFC3339
		// parsing rejected. Every other case here is Z-suffixed, which is why
		// the oracle could not catch it either.
		{ID: "jira_cloud_numeric_offset", Input: map[string]any{
			"cloud_id": "cloud-123", "base_url": "https://acme.atlassian.net",
			"raw_issue": map[string]any{"id": "10003", "key": "JSM-7", "fields": map[string]any{
				"summary": "Checkout latency", "created": "2026-07-22T10:00:00.000+0000",
				"updated": "2026-07-22T10:05:00.000+0000", "resolutiondate": "2026-07-22T10:30:00.000+0000",
				"status":   map[string]any{"name": "Done", "statusCategory": map[string]any{"key": "done"}},
				"priority": map[string]any{"name": "High"},
			}},
		}},
		{ID: "jira_cloud_non_utc_offset", Input: map[string]any{
			"cloud_id": "cloud-123", "base_url": "https://acme.atlassian.net",
			"raw_issue": map[string]any{"id": "10004", "key": "JSM-8", "fields": map[string]any{
				"summary": "Region failover", "created": "2026-07-22T12:00:00.000+0200",
				"updated": "2026-07-22T12:05:00.000+0200", "resolutiondate": nil,
				"status":   map[string]any{"name": "Investigating", "statusCategory": map[string]any{"key": "indeterminate"}},
				"priority": nil,
			}},
		}},
		{ID: "resolved_without_priority", Input: map[string]any{
			"cloud_id": "CLOUD-XYZ", "base_url": "https://team.atlassian.net/",
			"raw_issue": map[string]any{"id": "10002", "key": "OPS-9", "fields": map[string]any{
				"summary": "Recovered", "created": "2026-07-22T11:00:00.123456Z", "updated": "2026-07-22T11:05:00.654321Z", "resolutiondate": "2026-07-22T11:04:00Z",
				"status": map[string]any{"name": "Done", "statusCategory": map[string]any{"key": "done"}}, "priority": nil,
			}},
		}},
	}
}

func TestGenericOracleMatchesLivePythonForJiraIncidentRow(t *testing.T) {
	compareRowsAgainstPythonOracle(
		t, "jira/incidents/row", oracleJiraIncidentCases(), buildJiraIncidentRowForOracle, nil,
	)
}
