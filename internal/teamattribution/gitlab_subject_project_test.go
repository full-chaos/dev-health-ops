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
	if len(got) != 3 || got[0] != "platform/api-v2" || got[1] != "acme/api" || got[2] != "org:gitlab:123" {
		t.Fatalf("names = %v, want the current path, the minted path and the catalog id once each", got)
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

// A GitLab ownership row written before CHAOS-8883 holds the path. After the
// remap it lands on the catalog id, so an anchored item gets the owner the
// legacy item got.
func TestLegacyGitLabOwnershipJoinsAnAnchoredItem(t *testing.T) {
	path := "acme/api"
	legacy := []GithubWorkItemDerivationProjectFact{{
		Provider: "gitlab", TeamID: "owner-team", TeamName: "Owner", ProjectID: path, IsPrimary: 1, Specificity: 110, Priority: 10,
	}}
	teams := []GithubWorkItemDerivationTeamFact{{Provider: "gitlab", TeamID: "owner-team"}}
	before := NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{Teams: teams, Projects: legacy})
	oldItem := GithubWorkItemDerivationSubject{WorkItemID: "gitlab:acme/api#1", Provider: "gitlab", ProjectID: &path, OrgID: "org"}
	if got := primaryTeam(before, oldItem); got != "owner-team" {
		t.Fatalf("setup: the legacy item got %q", got)
	}
	anchored := gitlabSubject("org:gitlab:123", "acme/api")
	if got := primaryTeam(before, anchored); got != "" {
		t.Fatalf("setup: without the remap the anchored item got %q (the reviewed defect)", got)
	}
	facts, remapped, dropped := RemapLegacyGitLabProjectFacts("org", legacy, map[string]string{"acme/api": "123"}, nil)
	if remapped != 1 || dropped != 0 || facts[0].ProjectID != "org:gitlab:123" {
		t.Fatalf("remap = %+v remapped=%d dropped=%d", facts, remapped, dropped)
	}
	after := NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{Teams: teams, Projects: facts})
	if got := primaryTeam(after, anchored); got != "owner-team" {
		t.Fatalf("anchored item got %q after the remap, want owner-team", got)
	}
}

func TestLegacyGitLabOwnershipRemapKeepsWhatItCannotProve(t *testing.T) {
	facts := []GithubWorkItemDerivationProjectFact{
		{Provider: "gitlab", TeamID: "a", ProjectID: "x/dup"},
		{Provider: "gitlab", TeamID: "b", ProjectID: "x/unseen"},
		{Provider: "gitlab", TeamID: "c", ProjectID: "org:gitlab:7"},
		{Provider: "jira", TeamID: "d", ProjectID: "x/dup"},
	}
	out, remapped, dropped := RemapLegacyGitLabProjectFacts("org", facts, map[string]string{}, map[string]struct{}{"x/dup": {}})
	if remapped != 0 || dropped != 1 || len(out) != 4 {
		t.Fatalf("remapped=%d dropped=%d out=%+v", remapped, dropped, out)
	}
	if !out[0].Unjoinable || out[0].TeamID != "a" {
		t.Fatalf("a path two projects have used stayed joinable: %+v", out[0])
	}
	out = out[1:]
	if out[0].ProjectID != "x/unseen" || out[1].ProjectID != "org:gitlab:7" || out[2].Provider != "jira" || out[2].ProjectID != "x/dup" {
		t.Fatalf("unprovable or foreign rows changed: %+v", out)
	}
}

// A repo-ownership row keyed by full name (no repo id) joins the item anchored
// to the one project its path names; a path two projects have used joins no item
// by name, and the team still holds the ownership.
func TestNameKeyedGitLabRepoOwnershipFollowsTheOneProjectRule(t *testing.T) {
	teams := []GithubWorkItemDerivationTeamFact{{Provider: "gitlab", TeamID: "owner-team"}}
	repoFact := func(name string) []GithubWorkItemDerivationRepoFact {
		return []GithubWorkItemDerivationRepoFact{{Provider: "gitlab", TeamID: "owner-team", TeamName: "Owner", RepoFullName: name, IsPrimary: 1, Specificity: 100}}
	}
	item := gitlabSubject("org:gitlab:123", "acme/api")
	repoID := "repo-123"
	item.RepoID = &repoID
	item.GitLabPaths = []string{"acme/api"}

	unique, remapped, marked := RemapLegacyGitLabRepoFacts("org", repoFact("acme/api"), map[string]string{"acme/api": "123"}, nil)
	if remapped != 1 || marked != 0 || unique[0].NameKey != "org:gitlab:123" || unique[0].RepoFullName != "acme/api" {
		t.Fatalf("unique path: %+v remapped=%d marked=%d", unique, remapped, marked)
	}
	if got := primaryTeam(NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{Teams: teams, Repos: unique}), item); got != "owner-team" {
		t.Fatalf("the one project's item got %q, want owner-team", got)
	}

	// The same grant keyed by a path project 456 holds: project 123's item must not take it.
	other := gitlabSubject("org:gitlab:456", "acme/api@456")
	other.GitLabPaths = []string{"acme/api", "acme/api@456"}
	ownedBy456, _, _ := RemapLegacyGitLabRepoFacts("org", repoFact("acme/api"), map[string]string{"acme/api": "456"}, nil)
	if got := primaryTeam(NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{Teams: teams, Repos: ownedBy456}), item); got != "" {
		t.Fatalf("project 123's item took the name-keyed grant of project 456: %q", got)
	}

	dup, remapped, marked := RemapLegacyGitLabRepoFacts("org", repoFact("acme/api"), map[string]string{}, map[string]struct{}{"acme/api": {}})
	if remapped != 0 || marked != 1 || !dup[0].Unjoinable {
		t.Fatalf("ambiguous path: %+v", dup)
	}
	ambiguousCtx := NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{Teams: teams, Repos: dup})
	if got := primaryTeam(ambiguousCtx, item); got != "" {
		t.Fatalf("a reused path's name-keyed grant joined %q", got)
	}
	if got := primaryTeam(ambiguousCtx, other); got != "" {
		t.Fatalf("a reused path's name-keyed grant joined project 456's item: %q", got)
	}

	// The repo_id join of an ambiguous-name grant stays.
	withID := repoFact("acme/api")
	withID[0].RepoID = &repoID
	marked2, _, _ := RemapLegacyGitLabRepoFacts("org", withID, map[string]string{}, map[string]struct{}{"acme/api": {}})
	if got := primaryTeam(NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{Teams: teams, Repos: marked2}), item); got != "owner-team" {
		t.Fatalf("the repo_id join of an ambiguous-name grant was lost: %q", got)
	}

	unseen, remapped, marked := RemapLegacyGitLabRepoFacts("org", repoFact("x/unseen"), map[string]string{}, nil)
	if remapped != 0 || marked != 0 || unseen[0].NameKey != "" || unseen[0].Unjoinable {
		t.Fatalf("unseen path changed: %+v", unseen)
	}
	foreign := []GithubWorkItemDerivationRepoFact{{Provider: "github", TeamID: "g", RepoFullName: "acme/api"}}
	if out, r, m := RemapLegacyGitLabRepoFacts("org", foreign, map[string]string{"acme/api": "1"}, nil); r != 0 || m != 0 || out[0].NameKey != "" {
		t.Fatalf("a GitHub row was remapped: %+v", out)
	}
}

func manualProjectRuleTeam(scope string, subject GithubWorkItemDerivationSubject) string {
	derived := NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{
		Teams:           []GithubWorkItemDerivationTeamFact{{Provider: "gitlab", TeamID: "rule-team"}},
		ManualFallbacks: []GithubWorkItemDerivationManualFallback{{Provider: "gitlab", ScopeType: "project", ScopeID: scope, TeamID: "rule-team", Priority: 1}},
	})
	for _, candidate := range derived.ManualCandidates(subject) {
		return GithubWorkItemDerivationStringValue(candidate.TeamID)
	}
	return ""
}

// A catalog-keyed item with no repos row read has no path that is provably its
// own, so a manual project rule written with the path does not claim it.
func TestAManualProjectRuleByPathDoesNotClaimACatalogKeyedGitLabItemWithNoReposRow(t *testing.T) {
	subject := gitlabSubject("org:gitlab:123", "acme/api")
	if got := manualProjectRuleTeam("acme/api", subject); got != "" {
		t.Fatalf("path rule claimed an item with no repos row: %q", got)
	}
	if got := manualProjectRuleTeam("org:gitlab:123", subject); got != "rule-team" {
		t.Fatalf("catalog-id rule: team = %q, want rule-team", got)
	}
	subject.GitLabPaths = []string{"acme/api"}
	if got := manualProjectRuleTeam("acme/api", subject); got != "rule-team" {
		t.Fatalf("path rule with a repos row: team = %q, want rule-team", got)
	}
	legacy := gitlabSubject("acme/api", "")
	legacy.ProjectKey = nil
	if got := manualProjectRuleTeam("acme/api", legacy); got != "rule-team" {
		t.Fatalf("legacy path-in-project_id item: team = %q, want rule-team", got)
	}
}
