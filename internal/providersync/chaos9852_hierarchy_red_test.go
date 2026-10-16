package providersync

import (
	"testing"
	"time"
)

func chaos9852JiraRow(t *testing.T, parent any) jiraWorkItemRow {
	t.Helper()
	input := jiraWorkItemOraclePrepCases()[0].Input
	raw := input["raw_issue"].(map[string]any)
	fields := map[string]any{}
	for key, value := range raw["fields"].(map[string]any) {
		fields[key] = value
	}
	fields["parent"] = parent
	claim := nativeTestClaim("jira", "work-items")
	claim.OrgID = input["org_id"].(string)
	row, _, err := normalizeJiraWorkItem(
		claim,
		jiraWorkItemFixtureInput{Raw: map[string]any{"key": raw["key"], "self": raw["self"], "fields": fields}, AtlassianShape: true},
		loadRealStatusMapping(t), jiraOracleIdentity, time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatal(err)
	}
	return row
}

func TestChaos9852RedJiraParentKeyOnLiveShape(t *testing.T) {
	row := chaos9852JiraRow(t, map[string]any{"key": "OPS-1", "fields": map[string]any{
		"issuetype": map[string]any{"name": "Story", "hierarchyLevel": 0},
	}})
	if row.ParentID == nil || *row.ParentID != "jira:OPS-1" {
		t.Fatalf("parent_id=%q want jira:OPS-1", valueOrNil(row.ParentID))
	}
}
