package teamattribution

import "testing"

func gitlabOwnershipContext(ownerProjectID, ownerPath string) GithubWorkItemDerivationContext {
	return NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{
		Teams: []GithubWorkItemDerivationTeamFact{{Provider: "gitlab", TeamID: "owner-team"}},
		Projects: []GithubWorkItemDerivationProjectFact{{
			Provider: "gitlab", TeamID: "owner-team", TeamName: "Owner", ProjectID: ownerProjectID, ProjectKey: &ownerPath,
			IsPrimary: 1, Specificity: 110, Priority: 10,
		}},
	})
}

func gitlabSubject(projectID, path string) GithubWorkItemDerivationSubject {
	return GithubWorkItemDerivationSubject{
		WorkItemID: "gitlab:" + path + "#42", Provider: "gitlab", ProjectID: &projectID, ProjectKey: &path, OrgID: "org",
	}
}

func primaryTeam(derived GithubWorkItemDerivationContext, subject GithubWorkItemDerivationSubject) string {
	teamID, _, _ := derived.Resolve(subject)
	return GithubWorkItemDerivationStringValue(teamID)
}

func TestAGitLabItemJoinsItsProjectOwnershipByTheCatalogId(t *testing.T) {
	derived := gitlabOwnershipContext("org:gitlab:123", "platform/api-v2")
	if got := primaryTeam(derived, gitlabSubject("org:gitlab:123", "platform/api-v2")); got != "owner-team" {
		t.Fatalf("an item of project 123 joined %q, want owner-team", got)
	}
	// After a rename the item's key is the old path; the grant still follows the project.
	if got := primaryTeam(derived, gitlabSubject("org:gitlab:123", "acme/api")); got != "owner-team" {
		t.Fatalf("an item of project 123 under its old path joined %q, want owner-team", got)
	}
}

// LEAK: project 123 gave up acme/api and project 456 took it. A stale item of
// 123 at acme/api carries 123's catalog id and the path; the ownership of 456
// names the same path and must not claim it.
func TestAGitLabItemAtAReusedPathJoinsNothingOfTheNewOwner(t *testing.T) {
	derived := gitlabOwnershipContext("org:gitlab:456", "acme/api")
	if got := primaryTeam(derived, gitlabSubject("org:gitlab:123", "acme/api")); got != "" {
		t.Fatalf("a stale item of project 123 joined %q through the path of project 456", got)
	}
	if got := primaryTeam(derived, gitlabSubject("org:gitlab:456", "acme/api")); got != "owner-team" {
		t.Fatalf("an item of project 456 joined %q, want owner-team", got)
	}
}

func TestAGitLabItemWithACatalogIdAnswersRepoNameJoinsByItsPath(t *testing.T) {
	subject := gitlabSubject("org:gitlab:123", "acme/api")
	if got := GitLabProjectName(subject); got != "acme/api" {
		t.Fatalf("name = %q, want the project_key", got)
	}
	legacy := gitlabSubject("acme/api", "")
	legacy.ProjectKey = nil
	if got := GitLabProjectName(legacy); got != "acme/api" {
		t.Fatalf("name of an item that holds the path in project_id = %q, want the path", got)
	}
	other := subject
	other.Provider = "github"
	if got := GitLabProjectName(other); got != "org:gitlab:123" {
		t.Fatalf("name of a non-GitLab item = %q, want its project_id", got)
	}
}

func TestAGitLabItemScopeIsItsPathNotItsCatalogId(t *testing.T) {
	if got := WorkItemDerivationScope(gitlabSubject("org:gitlab:123", "acme/api")); got != "acme/api" {
		t.Fatalf("scope = %q, want the path", got)
	}
}
