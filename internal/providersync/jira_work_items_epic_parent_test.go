package providersync

import (
	"encoding/json"
	"testing"
	"time"
)

func jiraEpicParentRow(t *testing.T, options map[string]any, extra map[string]any, parent any) jiraWorkItemRow {
	t.Helper()
	input := jiraWorkItemOraclePrepCases()[0].Input
	raw := input["raw_issue"].(map[string]any)
	fields := map[string]any{}
	for key, value := range raw["fields"].(map[string]any) {
		fields[key] = value
	}
	delete(fields, "customfield_10014")
	for key, value := range extra {
		fields[key] = value
	}
	if parent == nil {
		delete(fields, "parent")
	} else {
		fields["parent"] = parent
	}
	claim := nativeTestClaim("jira", "work-items")
	claim.OrgID = input["org_id"].(string)
	claim.DatasetOptions = options
	row, _, err := normalizeJiraWorkItem(
		claim,
		jiraWorkItemFixtureInput{Raw: map[string]any{"key": raw["key"], "self": raw["self"], "fields": fields}, AtlassianShape: true},
		loadRealStatusMapping(t), jiraOracleIdentity, time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatal(err)
	}
	return row
}

func jiraWantText(want string) string {
	if want == "" {
		return "<nil>"
	}
	return want
}

func jiraParentOf(key string, level any) map[string]any {
	issuetype := map[string]any{"name": "Anything"}
	if level != nil {
		issuetype["hierarchyLevel"] = level
	}
	return map[string]any{"key": key, "fields": map[string]any{"issuetype": issuetype}}
}

// Jira Cloud links an issue to its epic through fields.parent; the parent's issuetype.hierarchyLevel is 1 for an epic.
// The Epic Link custom field of the dataset option, when it holds a value, wins. This is a Go-only rule: the Python
// producer (providers/jira/normalize.py, deleted in 7537adba6f) set epic_id from epic_link_field alone.
func TestJiraEpicIDFromAnEpicLevelParent(t *testing.T) {
	epicOption := map[string]any{"epic_link_field": "customfield_10014"}
	for _, tc := range []struct {
		name       string
		options    map[string]any
		extra      map[string]any
		parent     any
		wantEpic   string
		wantParent string
	}{
		{"an Epic-level parent gives the epic", nil, nil, jiraParentOf("OPS-7", json.Number("1")), "jira:OPS-7", "jira:OPS-7"},
		{"the level as a float", nil, nil, jiraParentOf("OPS-7", float64(1)), "jira:OPS-7", "jira:OPS-7"},
		{"the level as an int", nil, nil, jiraParentOf("OPS-7", 1), "jira:OPS-7", "jira:OPS-7"},
		{"the option wins over the parent", epicOption, map[string]any{"customfield_10014": "OPS-9"}, jiraParentOf("OPS-7", json.Number("1")), "jira:OPS-9", "jira:OPS-7"},
		{"an option without a value falls back to the parent", epicOption, nil, jiraParentOf("OPS-7", json.Number("1")), "jira:OPS-7", "jira:OPS-7"},
		{"a subtask parent (level 0) is no epic", nil, nil, jiraParentOf("OPS-7", json.Number("0")), "", "jira:OPS-7"},
		{"an initiative parent (level 2) is no epic", nil, nil, jiraParentOf("OPS-7", json.Number("2")), "", "jira:OPS-7"},
		{"a parent without a level is no epic", nil, nil, jiraParentOf("OPS-7", nil), "", "jira:OPS-7"},
		{"a numeric string level is no number, so no epic", nil, nil, jiraParentOf("OPS-7", "1"), "", "jira:OPS-7"},
		{"a level that is no number is no epic", nil, nil, jiraParentOf("OPS-7", "epic"), "", "jira:OPS-7"},
		{"an Epic-level parent without a key gives nothing", nil, nil, jiraParentOf("", json.Number("1")), "", ""},
		{"no parent", nil, nil, nil, "", ""},
	} {
		row := jiraEpicParentRow(t, tc.options, tc.extra, tc.parent)
		if got := valueOrNil(row.EpicID); got != jiraWantText(tc.wantEpic) {
			t.Errorf("%s: epic_id = %v, want %v", tc.name, got, jiraWantText(tc.wantEpic))
		}
		if got := valueOrNil(row.ParentID); got != jiraWantText(tc.wantParent) {
			t.Errorf("%s: parent_id = %v, want %v", tc.name, got, jiraWantText(tc.wantParent))
		}
	}
}
