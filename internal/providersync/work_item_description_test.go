package providersync

import (
	"encoding/json"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/full-chaos/dev-health-ops/internal/storedversion"
)

func TestBoundedWorkItemDescriptionCutsOnARuneBoundary(t *testing.T) {
	multibyte := strings.Repeat("é", workItemDescriptionMaxRunes+5)
	exact := strings.Repeat("a", workItemDescriptionMaxRunes)
	empty := ""
	cases := []struct {
		name  string
		in    *string
		want  *string
		runes int
	}{
		{"nil", nil, nil, 0},
		{"empty is no description", &empty, nil, 0},
		{"exactly the bound is kept whole", &exact, &exact, workItemDescriptionMaxRunes},
		{"multibyte is cut at the bound in runes", &multibyte, nil, workItemDescriptionMaxRunes},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := boundedWorkItemDescription(tc.in)
			if tc.want == nil && tc.runes == 0 {
				if got != nil {
					t.Fatalf("got %q, want nil", *got)
				}
				return
			}
			if got == nil || !utf8.ValidString(*got) || utf8.RuneCountInString(*got) != tc.runes {
				t.Fatalf("got %v, want %d valid runes", got, tc.runes)
			}
			if tc.want != nil && *got != *tc.want {
				t.Fatal("a description within the bound changed")
			}
		})
	}
}

func TestEveryWorkItemProjectionBoundsTheDescription(t *testing.T) {
	long := strings.Repeat("x", workItemDescriptionMaxRunes+300)
	github := projectWorkItem(githubWorkItemRow{WorkItemID: "gh:acme/api#1", Provider: "github", Description: &long})
	linear := projectLinearWorkItem(linearWorkItemRow{WorkItemID: "lin:1", Provider: "linear", Description: &long})
	for name, row := range map[string]workItemStoredRow{"github/gitlab/jira": github, "linear": linear} {
		if row.Description == nil || utf8.RuneCountInString(*row.Description) != workItemDescriptionMaxRunes {
			t.Errorf("%s: stored description not cut to %d runes: %v", name, workItemDescriptionMaxRunes, row.Description)
		}
	}
}

func heldWorkItemVersion(description string) func([]any) (map[string]any, bool) {
	return func([]any) (map[string]any, bool) {
		return map[string]any{"description": description, "priority_raw": "p0", "service_class": "expedite", "due_at": nil}, true
	}
}

// A sync row that states a description overwrites the held one; a row with
// none keeps the one a push wrote. Driven through the production row builder.
func TestWorkItemSyncKeepsAHeldDescriptionOnlyWhenTheRowStatesNone(t *testing.T) {
	stated := "synced body"
	for _, tc := range []struct {
		name string
		in   *string
		want any
	}{
		{"stated overwrites", &stated, "synced body"},
		{"nil keeps the push-written value", nil, "pushed body"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			row := workItemStoredRow{WorkItemID: "gh:acme/api#1", Provider: "github", Description: tc.in}
			rows := workItemStoredVersionRows([]workItemStoredRow{row})
			if _, err := workItemsContract.Fold(withHeldWorkItemColumns(gitHubWorkItemsInsert), rows, heldWorkItemVersion("pushed body")); err != nil {
				t.Fatal(err)
			}
			positions, err := storedversion.Positions(withHeldWorkItemColumns(gitHubWorkItemsInsert))
			if err != nil {
				t.Fatal(err)
			}
			got := rows[0].Values[positions["description"]]
			if p, ok := got.(*string); ok && p != nil {
				got = *p
			}
			if got != tc.want {
				t.Fatalf("description = %v, want %v", got, tc.want)
			}
			if rows[0].Values[positions["priority_raw"]] != "p0" {
				t.Fatalf("priority_raw not carried: %v", rows[0].Values[positions["priority_raw"]])
			}
		})
	}
}

// Each provider's real normalizer keeps the whole text; the stored-row
// projection of its output cuts at the bound on a rune boundary.
func TestEveryProviderNormalizerOutputIsCutAtTheProjection(t *testing.T) {
	long := strings.Repeat("é日", 800)
	want := strings.Repeat("é日", workItemDescriptionMaxRunes/2)

	github := buildGitHubIssueWorkItemOracleRow(t, map[string]any{
		"repo_full_name": "acme/api", "repo_id": "c7198fbc-1945-3717-05d8-eb78866b4e79", "org_id": "org-acme",
		"raw_issue": map[string]any{
			"number": 9, "title": "Long body", "body": long, "state": "open",
			"created_at": "2026-07-04T00:00:00Z", "updated_at": "2026-07-05T00:00:00Z",
			"labels": []any{}, "assignees": []any{},
		},
	})

	gitlabRow := buildGitLabFetchWorkItemOracleRow(t, map[string]any{
		"repo_full_name": "acme/api", "repo_id": "c7198fbc-1945-3717-05d8-eb78866b4e79", "org_id": "org-acme",
		"since": "2026-07-01T00:00:00Z",
		"raw_issue": map[string]any{
			"iid": 9, "title": "Long body", "description": long, "state": "opened",
			"created_at": "2026-07-04T00:00:00Z", "updated_at": "2026-07-05T00:00:00Z", "labels": []any{}, "assignees": []any{},
		},
	})
	encoded, err := json.Marshal(gitlabRow)
	if err != nil {
		t.Fatal(err)
	}
	var gitlab githubWorkItemRow
	if err := json.Unmarshal(encoded, &gitlab); err != nil {
		t.Fatal(err)
	}

	jiraCase := jiraWorkItemOraclePrepCases()[0].Input
	raw := jiraCase["raw_issue"].(map[string]any)
	fields := map[string]any{}
	for key, value := range raw["fields"].(map[string]any) {
		fields[key] = value
	}
	fields["description"] = long
	jiraInput := map[string]any{}
	for key, value := range jiraCase {
		jiraInput[key] = value
	}
	jiraInput["raw_issue"] = map[string]any{"key": raw["key"], "self": raw["self"], "fields": fields}
	jiraPrep := buildJiraWorkItemOraclePrepRow(t, jiraInput)
	jira := githubWorkItemRow{WorkItemID: jiraPrep.WorkItemID, Provider: jiraPrep.Provider, Description: jiraPrep.Description}

	linear := buildLinearWorkItemOracleRow(t, linearLongDescriptionInput(long))

	for name, kept := range map[string]*string{"github": github.Description, "gitlab": gitlab.Description, "jira": jira.Description, "linear": linear.Description} {
		if kept == nil || *kept != long {
			t.Errorf("%s: the normalizer row does not keep the whole description", name)
		}
	}
	for name, stored := range map[string]*string{
		"github": projectWorkItem(github).Description, "gitlab": projectWorkItem(gitlab).Description,
		"jira": projectWorkItem(jira).Description, "linear": projectLinearWorkItem(linear).Description,
	} {
		if stored == nil || *stored != want {
			t.Errorf("%s: stored description is %d runes, want the first %d", name, utf8.RuneCountInString(derefString(stored)), workItemDescriptionMaxRunes)
		}
	}
}

func linearLongDescriptionInput(description string) map[string]any {
	return map[string]any{
		"org_id": "org-acme", "history": []any{},
		"raw_issue": map[string]any{
			"id": "lin-issue-44", "identifier": "ENG-44", "title": "Long body",
			"description": description, "priority": 0, "estimate": nil,
			"createdAt": "2026-07-29T09:00:00Z", "updatedAt": "2026-07-30T11:00:00Z",
			"startedAt": nil, "completedAt": nil, "canceledAt": nil, "dueDate": nil, "url": nil,
			"state":    map[string]any{"name": "Backlog", "type": "backlog"},
			"labels":   map[string]any{"nodes": []any{}},
			"assignee": nil, "creator": nil,
			"team":    map[string]any{"id": "team-eng", "key": "ENG", "name": "Engineering"},
			"project": nil, "cycle": nil, "parent": nil,
		},
	}
}
