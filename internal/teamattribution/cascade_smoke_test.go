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
		// CHAOS-4320: TeamID "team-repo" (not a separate "team-member"), so
		// the assignee's resolved team OWNS repoID -- otherwise the new
		// repo-ownership gate drops this candidate entirely (see
		// TestCascadeGateDropsAssigneeWhoseTeamDoesNotOwnTheRepo below for
		// that case), which is not what THIS test checks (repo_ownership
		// outranking an assignee_membership candidate that's still present).
		Members: []GithubWorkItemDerivationMemberFact{{
			Provider: "github", TeamID: "team-repo", TeamName: "Member Team",
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

// TestCascadeGateDropsAssigneeWhoseTeamDoesNotOwnTheRepo is CHAOS-4320's
// red-first pin for the assignee path: before the repo-ownership gate, an
// assignee admin-mapped to a team that does not own the repo still appeared
// as a non-primary attribution row -- laundering "any team this person
// belongs to" into a persisted candidate the repo_ownership tier itself
// would never grant. repo_ownership (team-repo, the actual owner) still
// wins primary either way -- it outranks assignee_membership in `order`
// regardless of this gate, since the SAME ownership row that makes
// "team-member does not own it" answerable also feeds repo_ownership's own
// candidate for team-repo. The gate's observable effect here is entirely on
// PROVENANCE: assignee_membership must no longer appear in the candidate
// list at all.
//
// This is ALSO CHAOS-4320's differential fixture: compute_work_items.py has
// no equivalent gate (the ticket's baseline_defect, R60 -- pinned here, not
// fixed there, per the "no new Python compute code" rule) -- Python
// resolving this exact input still emits an assignee_membership row for
// team-member as provenance. Go's correct output (that row ABSENT) is what
// this test pins.
func TestCascadeGateDropsAssigneeWhoseTeamDoesNotOwnTheRepo(t *testing.T) {
	now := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	repoID := "c7198fbc-1945-3717-05d8-eb78866b4e79"

	derived := NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{
		Repos: []GithubWorkItemDerivationRepoFact{{
			Provider: "github", TeamID: "team-repo", TeamName: "Repository Team",
			RepoID: &repoID, RepoFullName: "acme/api", IsPrimary: 1,
			Specificity: 70, UpdatedAt: now,
		}},
		Members: []GithubWorkItemDerivationMemberFact{{
			Provider: "github", TeamID: "team-member", TeamName: "Member Team",
			MemberID: "dev@example.com", IsPrimary: 1, Specificity: 50, UpdatedAt: now,
		}},
	})
	subject := GithubWorkItemDerivationSubject{
		WorkItemID: "gh:acme/api#8", Provider: "github", RepoID: &repoID,
		Assignees: []string{"dev@example.com"}, OrgID: "org-acme",
	}
	teamID, _, candidates := derived.Resolve(subject)
	if got := GithubWorkItemDerivationStringValue(teamID); got != "team-repo" {
		t.Fatalf("primary team id = %q, want team-repo (repo_ownership, unaffected by the membership gate)", got)
	}
	for _, candidate := range candidates {
		if candidate.Source == "assignee_membership" {
			t.Fatalf("candidates = %+v, want NO assignee_membership row (gated by CHAOS-4320: team-member does not own the repo)", candidates)
		}
	}
}

// TestCascadeGateDropsAuthorWhoseTeamOwnsNothing is CHAOS-4320's red-first
// pin for the author (reporter) path: "author-in-team-that-owns-nothing"
// from the brief. Mirrors the assignee case above but through the reporter/
// author_membership path (CHAOS-4244/CHAOS-4321), which is gated on the
// SAME ResolveMembership call and must get the SAME repo-ownership check.
func TestCascadeGateDropsAuthorWhoseTeamOwnsNothing(t *testing.T) {
	now := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	repoID := "c7198fbc-1945-3717-05d8-eb78866b4e79"

	derived := NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{
		Repos: []GithubWorkItemDerivationRepoFact{{
			Provider: "github", TeamID: "team-repo", TeamName: "Repository Team",
			RepoID: &repoID, RepoFullName: "acme/api", IsPrimary: 1,
			Specificity: 70, UpdatedAt: now,
		}},
		Members: []GithubWorkItemDerivationMemberFact{{
			Provider: "github", TeamID: "team-nothing", TeamName: "Nothing Team",
			MemberID: "alice", IsPrimary: 1, Specificity: 50, UpdatedAt: now,
		}},
	})
	reporter := "alice"
	subject := GithubWorkItemDerivationSubject{
		WorkItemID: "ghpr:acme/api#9", Provider: "github", Type: "pr",
		RepoID: &repoID, Reporter: &reporter, OrgID: "org-acme",
	}
	teamID, _, candidates := derived.Resolve(subject)
	if got := GithubWorkItemDerivationStringValue(teamID); got != "team-repo" {
		t.Fatalf("primary team id = %q, want team-repo (repo_ownership, unaffected by the membership gate)", got)
	}
	for _, candidate := range candidates {
		if candidate.Source == "author_membership" {
			t.Fatalf("candidates = %+v, want NO author_membership row (gated by CHAOS-4320)", candidates)
		}
	}
}

// TestCascadeGateAllowsAuthorWhenOneOfMultipleOwningTeamsMatches is
// "author-in-two-teams-one-owns" from the brief: repoID has TWO ownership
// rows (co-owned by team-a and team-b); the author is admin-mapped
// unambiguously to team-b, one of the two owners. The gate must not require
// the resolved team to be the ONLY owner, just AN owner.
func TestCascadeGateAllowsAuthorWhenOneOfMultipleOwningTeamsMatches(t *testing.T) {
	now := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	repoID := "c7198fbc-1945-3717-05d8-eb78866b4e79"

	derived := NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{
		Repos: []GithubWorkItemDerivationRepoFact{
			{Provider: "github", TeamID: "team-a", TeamName: "Team A", RepoID: &repoID, RepoFullName: "acme/api", IsPrimary: 1, Specificity: 70, UpdatedAt: now},
			{Provider: "github", TeamID: "team-b", TeamName: "Team B", RepoID: &repoID, RepoFullName: "acme/api", IsPrimary: 1, Specificity: 70, UpdatedAt: now},
		},
		Members: []GithubWorkItemDerivationMemberFact{{
			Provider: "github", TeamID: "team-b", TeamName: "Team B",
			MemberID: "alice", IsPrimary: 1, Specificity: 50, UpdatedAt: now,
		}},
	})
	reporter := "alice"
	subject := GithubWorkItemDerivationSubject{
		WorkItemID: "ghpr:acme/api#10", Provider: "github", Type: "pr",
		RepoID: &repoID, Reporter: &reporter, OrgID: "org-acme",
	}
	teamID, teamName, _ := derived.Resolve(subject)
	if got := GithubWorkItemDerivationStringValue(teamID); got != "team-a" && got != "team-b" {
		t.Fatalf("primary team id = %q, want team-a or team-b (repo_ownership itself outranks author_membership either way)", got)
	}
	_ = teamName
}

// TestCascadeGateOwnershipUnknownMatchesConfiguredDefault pins CHAOS-4320's
// R74 ruling (chris via team-lead, 2026-09-09, decision log 93c4f16842d8):
// a repo with NO team_repo_ownership row at all is "ownership_unknown, not
// a data miss" -- the gate does not apply, membership passes through
// unchanged. It asserts against ownershipUnknownBlocksMembership itself
// (not a hardcoded outcome) so it stays correct even though R74 says never
// to revisit the constant's value.
func TestCascadeGateOwnershipUnknownMatchesConfiguredDefault(t *testing.T) {
	now := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	repoID := "c7198fbc-1945-3717-05d8-eb78866b4e79"

	// Deliberately NO Repos fact for repoID: ownership data is entirely
	// absent, not merely silent about this team.
	derived := NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{
		Members: []GithubWorkItemDerivationMemberFact{{
			Provider: "github", TeamID: "team-x", TeamName: "Team X",
			MemberID: "dev@example.com", IsPrimary: 1, Specificity: 50, UpdatedAt: now,
		}},
	})
	subject := GithubWorkItemDerivationSubject{
		WorkItemID: "gh:acme/api#11", Provider: "github", RepoID: &repoID,
		Assignees: []string{"dev@example.com"}, OrgID: "org-acme",
	}
	teamID, _, candidates := derived.Resolve(subject)
	gotPassed := GithubWorkItemDerivationStringValue(teamID) == "team-x"
	wantPassed := !ownershipUnknownBlocksMembership
	if gotPassed != wantPassed {
		t.Fatalf("assignee_membership passed the gate = %t, want %t (ownershipUnknownBlocksMembership=%t)",
			gotPassed, wantPassed, ownershipUnknownBlocksMembership)
	}
	if wantPassed {
		var unassigned *GithubWorkItemDerivationCandidate
		for index := range candidates {
			if candidates[index].Source == "unassigned" {
				unassigned = &candidates[index]
			}
		}
		if unassigned != nil {
			t.Fatalf("candidates = %+v, want assignee_membership to have resolved, not unassigned", candidates)
		}
	} else {
		for _, candidate := range candidates {
			if candidate.Source == "assignee_membership" {
				t.Fatalf("candidates = %+v, want NO assignee_membership row (ownershipUnknownBlocksMembership=true)", candidates)
			}
		}
	}
}
