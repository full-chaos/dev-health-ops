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

func TestAGitLabItemAnswersNameJoinsByEveryPathOfItsRepo(t *testing.T) {
	subject := gitlabSubject("org:gitlab:123", "acme/api")
	subject.GitLabPaths = []string{"platform/api-v2", "acme/api"}
	got := GitLabProjectNames(subject)
	if len(got) != 2 || got[0] != "platform/api-v2" || got[1] != "acme/api" {
		t.Fatalf("names = %v, want the current and the minted path once each", got)
	}
	legacy := gitlabSubject("acme/api", "")
	legacy.ProjectKey = nil
	if got := GitLabProjectNames(legacy); len(got) != 1 || got[0] != "acme/api" {
		t.Fatalf("names of an item that holds the path in project_id = %v, want the path", got)
	}
	other := subject
	other.Provider = "github"
	if got := GitLabProjectNames(other); len(got) != 1 || got[0] != "org:gitlab:123" {
		t.Fatalf("names of a non-GitLab item = %v, want its project_id only", got)
	}
}

func manualRepoRuleTeam(provider, scope string, subject GithubWorkItemDerivationSubject) string {
	derived := NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{
		Teams:           []GithubWorkItemDerivationTeamFact{{Provider: provider, TeamID: "rule-team"}},
		ManualFallbacks: []GithubWorkItemDerivationManualFallback{{Provider: provider, ScopeType: "repo", ScopeID: scope, TeamID: "rule-team", Priority: 1}},
	})
	for _, candidate := range derived.ManualCandidates(subject) {
		return GithubWorkItemDerivationStringValue(candidate.TeamID)
	}
	return ""
}

// A manual repo rule written with the new path and one written with the old
// path both match a renamed project; a GitHub repo of the same slug matches
// neither.
func TestAManualRepoRuleMatchesARenamedGitLabProjectByEitherPath(t *testing.T) {
	renamed := gitlabSubject("org:gitlab:123", "acme/api")
	renamed.GitLabPaths = []string{"platform/api-v2", "acme/api"}
	for _, scope := range []string{"platform/api-v2", "acme/api"} {
		if got := manualRepoRuleTeam("gitlab", scope, renamed); got != "rule-team" {
			t.Errorf("rule on %q: team = %q, want rule-team", scope, got)
		}
	}
	if got := manualRepoRuleTeam("gitlab", "acme/other", renamed); got != "" {
		t.Errorf("rule on another path matched %q", got)
	}
	github := GithubWorkItemDerivationSubject{WorkItemID: "github:acme/api#1", Provider: "github", ProjectID: strPtrForTest("acme/api"), OrgID: "org"}
	if got := manualRepoRuleTeam("gitlab", "acme/api", github); got != "" {
		t.Errorf("a gitlab rule matched a GitHub repo of the same slug: %q", got)
	}
}

func strPtrForTest(value string) *string { return &value }

func TestAGitLabItemScopeIsItsPathNotItsCatalogId(t *testing.T) {
	if got := WorkItemDerivationScope(gitlabSubject("org:gitlab:123", "acme/api")); got != "acme/api" {
		t.Fatalf("scope = %q, want the path", got)
	}
}

func TestGitLabRowPathsNeverOfferAPathAnotherRowHoldsNow(t *testing.T) {
	renamed := GitLabRepoRow{Repo: "platform/api-v2", IdentityPath: "acme/api"}
	holders := map[string][]string{"acme/api": {"repo-456"}}
	if got := GitLabRowPaths(renamed, "repo-123", holders); len(got) != 1 || got[0] != "platform/api-v2" {
		t.Fatalf("reused path: paths = %v, want only the current path", got)
	}
	if got := GitLabRowPaths(renamed, "repo-123", nil); len(got) != 2 || got[0] != "platform/api-v2" || got[1] != "acme/api" {
		t.Fatalf("renamed, path free: paths = %v, want current and identity path", got)
	}
	reusing := GitLabRepoRow{Repo: "acme/api", IdentityPath: "acme/api@456"}
	if got := GitLabRowPaths(reusing, "repo-456", holders); len(got) != 2 || got[0] != "acme/api" || got[1] != "acme/api@456" {
		t.Fatalf("project at the reused path: paths = %v, want both", got)
	}
	same := GitLabRepoRow{Repo: "acme/api", IdentityPath: "ACME/api"}
	if got := GitLabRowPaths(same, "repo-1", map[string][]string{"acme/api": {"repo-1"}}); len(got) != 1 {
		t.Fatalf("never renamed: paths = %v, want one", got)
	}
}

// Project 123 left acme/api (now platform/api-v2) and project 456 took it. The
// repo-ownership-by-name rows name each project by its current path.
func TestAReusedPathAttributesEachProjectsItemsToItsOwnOwner(t *testing.T) {
	oldRepo, newRepo := "repo-123", "repo-456"
	derived := NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{
		Teams: []GithubWorkItemDerivationTeamFact{
			{Provider: "gitlab", TeamID: "old-owner"}, {Provider: "gitlab", TeamID: "new-owner"},
		},
		Repos: []GithubWorkItemDerivationRepoFact{
			{Provider: "gitlab", TeamID: "old-owner", TeamName: "Old", RepoFullName: "platform/api-v2", IsPrimary: 1, Specificity: 100},
			{Provider: "gitlab", TeamID: "new-owner", TeamName: "New", RepoFullName: "acme/api", IsPrimary: 1, Specificity: 100},
		},
	})
	holders := map[string][]string{"acme/api": {newRepo}}
	stale := gitlabSubject("org:gitlab:123", "acme/api")
	stale.RepoID = &oldRepo
	stale.GitLabPaths = GitLabRowPaths(GitLabRepoRow{Repo: "platform/api-v2", IdentityPath: "acme/api"}, oldRepo, holders)
	current := gitlabSubject("org:gitlab:456", "acme/api@456")
	current.RepoID = &newRepo
	current.GitLabPaths = GitLabRowPaths(GitLabRepoRow{Repo: "acme/api", IdentityPath: "acme/api@456"}, newRepo, holders)
	if got := primaryTeam(derived, stale); got != "old-owner" {
		t.Fatalf("items of project 123 went to %q, want old-owner", got)
	}
	if got := primaryTeam(derived, current); got != "new-owner" {
		t.Fatalf("items of project 456 went to %q, want new-owner", got)
	}
}

func TestAReusedPathIsNotAnIssueProjectKeyOfTheOldProject(t *testing.T) {
	derived := NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{Teams: []GithubWorkItemDerivationTeamFact{
		{Provider: "gitlab", TeamID: "new-team", ProjectKeys: []string{"acme/api"}},
	}})
	stale := gitlabSubject("org:gitlab:123", "acme/api")
	stale.GitLabPaths = []string{"platform/api-v2"}
	if got := derived.IssueProjectCandidates(stale); len(got) != 0 {
		t.Fatalf("a stale item of project 123 matched the team that holds the reused path: %+v", got)
	}
	current := gitlabSubject("org:gitlab:456", "acme/api@456")
	current.GitLabPaths = []string{"acme/api", "acme/api@456"}
	if got := derived.IssueProjectCandidates(current); len(got) != 1 || GithubWorkItemDerivationStringValue(got[0].TeamID) != "new-team" {
		t.Fatalf("project 456 candidates = %+v, want new-team", got)
	}
}
