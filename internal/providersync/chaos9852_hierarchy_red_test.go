package providersync

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
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

func TestChaos9852RedGitLabIssueEpic(t *testing.T) {
	issue := map[string]any{
		"iid": 42, "title": "Issue in an epic", "state": "opened",
		"created_at": "2026-10-01T00:00:00Z", "updated_at": "2026-10-02T00:00:00Z",
		"labels": []string{"feature"},
		"epic":   map[string]any{"id": 900, "iid": 4, "group_id": 12, "title": "Epic four"},
	}
	encoded, err := json.Marshal(issue)
	if err != nil {
		t.Fatal(err)
	}
	var payload gitlabIssueWorkItemPayload
	if err := json.Unmarshal(encoded, &payload); err != nil {
		t.Fatal(err)
	}
	claim := nativeTestClaim("gitlab", "work-items")
	row, _, err := normalizeGitLabIssueWorkItem(
		claim, "acme/api", uuid.MustParse("0b7e3d4e-1111-4222-8333-944455556666"), payload, nil,
		loadRealStatusMapping(t), nil, time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatal(err)
	}
	// Python build_epic_id_for_issue (providers/gitlab/normalize.py:786-800).
	if row.EpicID == nil || *row.EpicID != "gitlab:12:epic:4" {
		t.Fatalf("epic_id=%q want gitlab:12:epic:4", valueOrNil(row.EpicID))
	}
}
