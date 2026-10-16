package teamattribution

import (
	"fmt"
	"testing"
)

// A GitLab ownership row is keyed by the project's catalog id and carries the
// path as project_key; a GitLab work item names the project by its path. The
// item resolves to the same team, with the same evidence, as when the row was
// keyed by the path.
func TestGitLabWorkItemResolvesACatalogKeyedOwnershipRowLikeAPathKeyedOne(t *testing.T) {
	path := "acme/api"
	subject := GithubWorkItemDerivationSubject{
		WorkItemID: "gitlab:acme/api#42", Provider: "gitlab", ProjectID: &path, OrgID: "org",
	}
	resolve := func(projectID string) (string, string) {
		t.Helper()
		key := path
		derived := NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{
			Projects: []GithubWorkItemDerivationProjectFact{
				{Provider: "gitlab", TeamID: "gl:acme", TeamName: "Acme", ProjectID: projectID, ProjectKey: &key, Specificity: 100, Priority: 1},
			},
		})
		teamID, _, candidates := derived.Resolve(subject)
		return GithubWorkItemDerivationStringValue(teamID), fmt.Sprint(rowsByTeam(candidates)["gl:acme"])
	}
	pathTeam, pathRows := resolve(path)
	catalogTeam, catalogRows := resolve("org:gitlab:4711")
	if pathTeam != "gl:acme" {
		t.Fatalf("path-keyed row: team %q, want gl:acme", pathTeam)
	}
	if catalogTeam != pathTeam || catalogRows != pathRows {
		t.Fatalf("catalog-keyed row: team %q rows %s, want the path-keyed result %q rows %s", catalogTeam, catalogRows, pathTeam, pathRows)
	}
}

// Only a GitLab row in the catalog form joins on its project_key: a row of
// another provider, or a GitLab row still keyed by the path, joins on its
// project_id as before.
func TestProjectOwnershipJoinID(t *testing.T) {
	key := "acme/api"
	blank := " "
	for _, tc := range []struct {
		provider, projectID string
		projectKey          *string
		want                string
	}{
		{"gitlab", "org:gitlab:4711", &key, "acme/api"},
		{"gitlab", "acme/api", &key, "acme/api"},
		{"gitlab", "org:gitlab:4711", nil, "org:gitlab:4711"},
		{"gitlab", "org:gitlab:4711", &blank, "org:gitlab:4711"},
		{"jira", "10001", &key, "10001"},
		{"linear", "org:linear:ENG", &key, "org:linear:ENG"},
	} {
		if got := ProjectOwnershipJoinID(tc.provider, tc.projectID, tc.projectKey); got != tc.want {
			t.Errorf("ProjectOwnershipJoinID(%q, %q, %v) = %q, want %q", tc.provider, tc.projectID, tc.projectKey, got, tc.want)
		}
	}
}
