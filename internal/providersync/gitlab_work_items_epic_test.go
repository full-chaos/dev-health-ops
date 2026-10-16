package providersync

import (
	"testing"

	"github.com/google/uuid"
)

// gitlabIssueEpicCase is one GitLab issue with an epic object, for both the
// Go table below and the Python provider oracle.
func gitlabIssueEpicCase(id, repoFullName string, epic any) oracleCase {
	issue := map[string]any{
		"iid": 42, "title": "Issue in an epic", "state": "opened",
		"created_at": "2026-08-01T08:00:00Z", "updated_at": "2026-08-02T08:00:00Z",
		"labels": []any{"feature", "priority::low"}, "assignees": []any{},
	}
	if epic != nil {
		issue["epic"] = epic
	}
	return oracleCase{ID: id, Input: map[string]any{
		"repo_full_name": repoFullName,
		"repo_id":        "c7198fbc-1945-3717-05d8-eb78866b4e79",
		"org_id":         "org-acme",
		"since":          "2026-08-01T00:00:00Z",
		"raw_issue":      issue,
	}}
}

// gitlabIssueEpicCases covers the input domain of the epic link: the epic
// object, its iid and group_id (absent, null, zero, empty, string, float),
// and a project with and without a group.
func gitlabIssueEpicCases() []struct {
	oracleCase
	want string
} {
	return []struct {
		oracleCase
		want string
	}{
		{gitlabIssueEpicCase("epic_with_group_id", "acme/api", map[string]any{"id": 900, "iid": 4, "group_id": 12, "title": "Epic four"}), "gitlab:12:epic:4"},
		{gitlabIssueEpicCase("epic_without_group_id", "acme/api", map[string]any{"id": 900, "iid": 4}), "gitlab:acme:epic:4"},
		{gitlabIssueEpicCase("epic_null_group_id", "acme/api", map[string]any{"iid": 4, "group_id": nil}), "gitlab:acme:epic:4"},
		{gitlabIssueEpicCase("epic_zero_group_id", "acme/api", map[string]any{"iid": 4, "group_id": 0}), "gitlab:acme:epic:4"},
		{gitlabIssueEpicCase("epic_empty_group_id", "acme/api", map[string]any{"iid": 4, "group_id": ""}), "gitlab:acme:epic:4"},
		{gitlabIssueEpicCase("epic_string_group_id", "acme/api", map[string]any{"iid": 4, "group_id": "acme-group"}), "gitlab:acme-group:epic:4"},
		{gitlabIssueEpicCase("epic_nested_group", "acme/platform/api", map[string]any{"iid": 9}), "gitlab:acme/platform:epic:9"},
		{gitlabIssueEpicCase("epic_string_iid", "acme/api", map[string]any{"iid": "7", "group_id": 12}), "gitlab:12:epic:7"},
		{gitlabIssueEpicCase("epic_zero_iid", "acme/api", map[string]any{"iid": 0, "group_id": 12}), "gitlab:12:epic:0"},
		{gitlabIssueEpicCase("epic_float_iid", "acme/api", map[string]any{"iid": 4.5, "group_id": 12}), "gitlab:12:epic:4.5"},
		{gitlabIssueEpicCase("epic_null_iid", "acme/api", map[string]any{"iid": nil, "group_id": 12}), ""},
		{gitlabIssueEpicCase("epic_without_iid", "acme/api", map[string]any{"id": 900, "group_id": 12}), ""},
		{gitlabIssueEpicCase("epic_null", "acme/api", nil), ""},
		{gitlabIssueEpicCase("project_without_group", "api", map[string]any{"iid": 4, "group_id": 12}), ""},
	}
}

func buildGitLabIssueProviderOracleRow(t *testing.T, input map[string]any) gitlabWorkItemRow {
	t.Helper()
	repoID, err := uuid.Parse(input["repo_id"].(string))
	if err != nil {
		t.Fatal(err)
	}
	claim := nativeTestClaim("gitlab", "work-items")
	claim.OrgID = input["org_id"].(string)
	row, _, err := normalizeGitLabIssueWorkItem(
		claim, input["repo_full_name"].(string), repoID, gitlabIssueWorkItemPayloadFromOracle(t, input),
		gitlabLabelEventsFromOracle(t, input), loadRealStatusMapping(t), nil, gitlabWorkItemsOracleNormalizedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	return row
}

// TestGitLabIssueEpicIDFollowsPythonProvider is the Go side of each epic
// cell (CHAOS-9854); the oracle test below proves the same cells against
// Python's provider.
func TestGitLabIssueEpicIDFollowsPythonProvider(t *testing.T) {
	for _, tc := range gitlabIssueEpicCases() {
		t.Run(tc.ID, func(t *testing.T) {
			got := buildGitLabIssueProviderOracleRow(t, tc.Input).EpicID
			if tc.want == "" {
				if got != nil {
					t.Fatalf("epic_id = %q, want none", *got)
				}
				return
			}
			if got == nil || *got != tc.want {
				t.Fatalf("epic_id = %s, want %q", valueOrNil(got), tc.want)
			}
		})
	}
}

// TestGitLabIssueMatchesFrozenPythonProvider compares the whole GitLab issue
// row (label priority and epic included) with the row of Python's canonical
// GitLabProvider batch.
func TestGitLabIssueMatchesFrozenPythonProvider(t *testing.T) {
	cases := gitlabWorkItemsOracleCases()
	for _, tc := range gitlabIssueEpicCases() {
		cases = append(cases, tc.oracleCase)
	}
	compareRowsAgainstPythonOracle(
		t,
		"gitlab/work-items/issue-provider",
		cases,
		buildGitLabIssueProviderOracleRow,
		gitlabWorkItemsFetchGoOnlyFields,
	)
}
