package teamattribution

import (
	"testing"
	"time"
)

// TestResolvePrefersRepoOwnershipOverLowerSources is a self-contained smoke
// test for the extracted cascade (CHAOS-3092 PR-A): it exercises Resolve
// directly, through this package's own exported API only, with no
// dependency on providersync's row-shape adapters or orchestrator. The
// fixture mirrors providersync's
// TestGitHubWorkItemDerivationLoadsSourceScopedProvenance (unchanged by the
// move), so a regression here would also fail there -- this test exists so
// the package has coverage of its own rather than borrowing all of it.
func TestResolvePrefersRepoOwnershipOverLowerSources(t *testing.T) {
	now := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	repoID := "c7198fbc-1945-3717-05d8-eb78866b4e79"

	facts := GithubWorkItemDerivationFacts{
		Repos: []GithubWorkItemDerivationRepoFact{{
			Provider: "github", TeamID: "team-repo", TeamName: "Repository Team",
			RepoID: &repoID, RepoFullName: "acme/api", IsPrimary: 1,
			Specificity: 70, UpdatedAt: now,
		}},
		Members: []GithubWorkItemDerivationMemberFact{{
			Provider: "github", TeamID: "team-member", TeamName: "Member Team",
			MemberID: "dev@example.com", IsPrimary: 1, Specificity: 50, UpdatedAt: now,
		}},
		ManualFallbacks: []GithubWorkItemDerivationManualFallback{{
			Provider: "github", ScopeType: "repo", ScopeID: repoID,
			TeamID: "team-manual", TeamName: "Manual Team", Priority: 100,
		}},
	}
	derived := NewGitHubWorkItemDerivationContext(facts)

	subject := GithubWorkItemDerivationSubject{
		WorkItemID: "gh:acme/api#7", Provider: "github", RepoID: &repoID,
		Assignees: []string{"dev@example.com"}, OrgID: "org-acme",
	}
	teamID, teamName, candidates := derived.Resolve(subject)
	if got := GithubWorkItemDerivationStringValue(teamID); got != "team-repo" {
		t.Fatalf("primary team id = %q, want team-repo", got)
	}
	if got := GithubWorkItemDerivationStringValue(teamName); got != "Repository Team" {
		t.Fatalf("primary team name = %q, want Repository Team", got)
	}

	bySource := map[string]GithubWorkItemDerivationCandidate{}
	for _, candidate := range candidates {
		bySource[candidate.Source] = candidate
	}
	repo := bySource["repo_ownership"]
	if repo.IsPrimary != 1 || repo.Confidence != "high" || repo.Evidence != "repo_ownership="+repoID {
		t.Fatalf("repo provenance = %+v", repo)
	}
	for _, lower := range []string{"assignee_membership", "manual_fallback"} {
		candidate, exists := bySource[lower]
		if !exists || candidate.IsPrimary != 0 {
			t.Fatalf("lower-precedence %s candidate = %+v exists=%t", lower, candidate, exists)
		}
	}
}

// TestBuildLinkedIssueIndexInheritsFromAttributedDonor exercises the
// linked-issue donor path directly on this package's own types: a
// team-less item with an "external_issue_key" edge to an item that DOES
// resolve inherits that donor's team once the index is wired back onto
// LinkedIssue and Resolve runs again.
func TestBuildLinkedIssueIndexInheritsFromAttributedDonor(t *testing.T) {
	now := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)

	facts := GithubWorkItemDerivationFacts{
		Projects: []GithubWorkItemDerivationProjectFact{{
			Provider: "linear", TeamID: "team-linked", TeamName: "Linked Team",
			ProjectID: "linear-project-1", IsPrimary: 1, Specificity: 80, UpdatedAt: now,
		}},
	}
	derived := NewGitHubWorkItemDerivationContext(facts)

	donorProject := "linear-project-1"
	donor := GithubWorkItemDerivationSubject{
		WorkItemID: "linear:CHAOS-42", Provider: "linear", ProjectID: &donorProject,
		OrgID: "org-acme",
	}
	dependent := GithubWorkItemDerivationSubject{
		WorkItemID: "gh:acme/api#7", Provider: "github", OrgID: "org-acme",
	}
	subjects := map[string]GithubWorkItemDerivationSubject{
		donor.WorkItemID: donor, dependent.WorkItemID: dependent,
	}
	dependencies := []GithubWorkItemDerivationDependencyEdge{{
		SourceWorkItemID: dependent.WorkItemID, TargetWorkItemID: "extkey:CHAOS-42",
		RelationshipType: "external_issue_key", LastSynced: now,
	}}

	linkedIssue, rescues, crossProviderRescues := derived.BuildLinkedIssueIndex(
		"github", subjects, dependencies, nil,
	)
	if rescues != 0 || crossProviderRescues != 0 {
		t.Fatalf("rescues = %d, crossProviderRescues = %d, want 0/0 (no stored-only edges here)", rescues, crossProviderRescues)
	}
	derived.LinkedIssue = linkedIssue

	teamID, teamName, candidates := derived.Resolve(dependent)
	if got := GithubWorkItemDerivationStringValue(teamID); got != "team-linked" {
		t.Fatalf("primary team id = %q, want team-linked", got)
	}
	if got := GithubWorkItemDerivationStringValue(teamName); got != "Linked Team" {
		t.Fatalf("primary team name = %q, want Linked Team", got)
	}
	var linked *GithubWorkItemDerivationCandidate
	for index := range candidates {
		if candidates[index].Source == "linked_issue" {
			linked = &candidates[index]
		}
	}
	if linked == nil || linked.Confidence != "medium" || linked.Evidence != "linked_issue="+dependent.WorkItemID {
		t.Fatalf("linked_issue candidate = %+v", linked)
	}
}
