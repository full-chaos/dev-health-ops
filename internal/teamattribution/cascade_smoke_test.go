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
// This test executes ONLY Go -- it is not a differential fixture and does
// not itself run compute_work_items.py against this input. The Python gap
// this pins Go's correct behavior against (compute_work_items.py has no
// equivalent ownership gate -- the ticket's baseline_defect, R60, pinned
// here and not fixed there, per the "no new Python compute code" rule) was
// established separately, via the live 14-day ClickHouse before/after
// measurement, not by executing Python in this file. Go's correct output
// for this input (the assignee_membership row for team-member ABSENT) is
// what this test pins.
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
	// CHAOS-4320's own gate (teamOwnsSubjectRepo) is verified directly
	// below, unaffected by CHAOS-5649: team-b is one of two owners of
	// repoID, so the gate must still say owns=true for it.
	if reason, owns := derived.teamOwnsSubjectRepo(subject, "team-b"); !owns {
		t.Fatalf("teamOwnsSubjectRepo(team-b) = (%q, false), want owns=true (team-b co-owns repoID)", reason)
	}
	teamID, _, candidates := derived.Resolve(subject)
	if got := GithubWorkItemDerivationStringValue(teamID); got != "team-a" && got != "team-b" {
		t.Fatalf("primary team id = %q, want team-a or team-b (repo_ownership itself outranks author_membership either way)", got)
	}
	// CHAOS-5649 (R179 rule 1, chris 2026-09-12) supersedes this test's
	// ORIGINAL assertion (an author_membership row present for the
	// gate-passing team-b): repo_ownership already set `primary` before
	// the author_membership tier of `order` is even reached here, so rule
	// 1 now drops author_membership from `all` entirely regardless of
	// whether it would have passed CHAOS-4320's ownership gate -- the gate
	// itself still passes it (asserted above via teamOwnsSubjectRepo
	// directly), it is simply never RECORDED once a higher-ranked primary
	// already exists.
	for _, candidate := range candidates {
		if candidate.Source == "author_membership" {
			t.Fatalf("candidates = %+v, want NO author_membership row (CHAOS-5649 R179 rule 1: repo_ownership already set primary)", candidates)
		}
	}
}

// TestCascadeGateOwnershipUnknownMatchesConfiguredDefault pins CHAOS-4320's
// R74 ruling (chris via team-lead, 2026-09-09, decision log 93c4f16842d8):
// a repo with NO team_repo_ownership row at all is "ownership_unknown, not
// a data miss" -- the gate does not apply, membership passes through
// unchanged.
//
// codex round 2, P3: this test PREVIOUSLY derived its expectation from
// `!ownershipUnknownBlocksMembership` instead of a hardcoded value, so
// flipping that constant (an accidental one, since R74 says the value is
// never revisited) would silently redefine "correct" instead of failing --
// zero regression protection against exactly the mistake this test exists
// to catch. Now hardcoded to R74's actual decided outcome (membership
// passes through), with a SEPARATE assertion that the constant itself still
// reads false -- a flip of either one alone now fails this test.
func TestCascadeGateOwnershipUnknownMatchesConfiguredDefault(t *testing.T) {
	if ownershipUnknownBlocksMembership {
		t.Fatal("ownershipUnknownBlocksMembership = true, want false (R74's decided value -- never revisit, see project_ops_team_mapped_to_nothing.md)")
	}

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
	if got := GithubWorkItemDerivationStringValue(teamID); got != "team-x" {
		t.Fatalf("primary team id = %q, want team-x (R74: ownership-unknown passes membership through)", got)
	}
	var unassigned *GithubWorkItemDerivationCandidate
	for index := range candidates {
		if candidates[index].Source == "unassigned" {
			unassigned = &candidates[index]
		}
	}
	if unassigned != nil {
		t.Fatalf("candidates = %+v, want assignee_membership to have resolved, not unassigned", candidates)
	}
}

// TestCascadeGateDropsBlankTeamIDMembershipWhenRepoHasAnOwner is CHAOS-4320's
// red-first pin for codex round 1's F1 (P1, BLOCK): team_memberships.team_id
// is a plain String column with no non-empty constraint, so
// ResolveMembership's exactly-one-team gate can legitimately resolve to a
// single "" team the same way it resolves to any real one (an admin/provider
// fact with a blank TeamID still produces exactly one distinct team-id key:
// ""). teamOwnsSubjectRepo used to short-circuit `teamID == ""` straight to
// owns=true, laundering that degenerate membership row past the gate
// regardless of what team_repo_ownership actually said -- exactly the
// bypass this ticket exists to close, just with an empty team_id standing
// in for a real non-owning one. Fixed: "" is now compared against the
// repo's real owners like any other team_id and loses unless some
// ownership row itself names team_id "".
func TestCascadeGateDropsBlankTeamIDMembershipWhenRepoHasAnOwner(t *testing.T) {
	now := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	repoID := "c7198fbc-1945-3717-05d8-eb78866b4e79"

	derived := NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{
		Repos: []GithubWorkItemDerivationRepoFact{{
			Provider: "github", TeamID: "team-repo", TeamName: "Repository Team",
			RepoID: &repoID, RepoFullName: "acme/api", IsPrimary: 1,
			Specificity: 70, UpdatedAt: now,
		}},
		// A real, reachable shape: an identities/teams admin fact whose
		// TeamID is blank (no non-empty constraint on the column) --
		// ResolveMembership still resolves this to exactly one team ("").
		Members: []GithubWorkItemDerivationMemberFact{{
			Provider: "github", TeamID: "", TeamName: "",
			MemberID: "dev@example.com", IsPrimary: 1, Specificity: 60, UpdatedAt: now,
		}},
	})
	subject := GithubWorkItemDerivationSubject{
		WorkItemID: "gh:acme/api#12", Provider: "github", RepoID: &repoID,
		Assignees: []string{"dev@example.com"}, OrgID: "org-acme",
	}
	teamID, _, candidates := derived.Resolve(subject)
	if got := GithubWorkItemDerivationStringValue(teamID); got != "team-repo" {
		t.Fatalf("primary team id = %q, want team-repo (repo_ownership, unaffected by the membership gate)", got)
	}
	for _, candidate := range candidates {
		if candidate.Source == "assignee_membership" {
			t.Fatalf("candidates = %+v, want NO assignee_membership row (blank team_id must not bypass the ownership gate)", candidates)
		}
	}
}

// TestCandidateOwnershipReasonDistinguishesOwnedFromUnknownPassthrough is
// CHAOS-4320's red-first pin for codex round 1's F1 (P1, BLOCK): before this
// fix, a winning assignee_membership/author_membership candidate carried no
// signal distinguishing "the resolved team genuinely owns the repo" from
// "the repo has no ownership data at all, R74 passed it through anyway" --
// both cases reached WriteGitHubWorkItemEffect looking identical, so the
// ownership_checked counter collapsed both to "owned" and lost exactly the
// distinction it exists to make visible. candidate.OwnershipReason now
// carries that distinction from Resolve() through to the write boundary
// (mirrors candidate.Priority's carry-not-persist pattern).
func TestCandidateOwnershipReasonDistinguishesOwnedFromUnknownPassthrough(t *testing.T) {
	now := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	repoID := "c7198fbc-1945-3717-05d8-eb78866b4e79"

	// Case 1: team-repo genuinely owns repoID.
	owned := NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{
		Repos: []GithubWorkItemDerivationRepoFact{{
			Provider: "github", TeamID: "team-repo", TeamName: "Repository Team",
			RepoID: &repoID, RepoFullName: "acme/api", IsPrimary: 1,
			Specificity: 70, UpdatedAt: now,
		}},
		Members: []GithubWorkItemDerivationMemberFact{{
			Provider: "github", TeamID: "team-repo", TeamName: "Repository Team",
			MemberID: "dev@example.com", IsPrimary: 1, Specificity: 60, UpdatedAt: now,
		}},
	})
	ownedSubject := GithubWorkItemDerivationSubject{
		WorkItemID: "gh:acme/api#13", Provider: "github", RepoID: &repoID,
		Assignees: []string{"dev@example.com"}, OrgID: "org-acme",
	}
	_, _, ownedCandidates := owned.Resolve(ownedSubject)
	ownedReason := ""
	for _, candidate := range ownedCandidates {
		if candidate.Source == "assignee_membership" {
			ownedReason = candidate.OwnershipReason
		}
	}
	if ownedReason != "owned" {
		t.Fatalf("genuinely-owned assignee_membership candidate.OwnershipReason = %q, want owned", ownedReason)
	}

	// Case 2: no ownership row at all for repoID -- R74 pass-through.
	unknown := NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{
		Members: []GithubWorkItemDerivationMemberFact{{
			Provider: "github", TeamID: "team-x", TeamName: "Team X",
			MemberID: "dev@example.com", IsPrimary: 1, Specificity: 60, UpdatedAt: now,
		}},
	})
	unknownSubject := GithubWorkItemDerivationSubject{
		WorkItemID: "gh:acme/api#14", Provider: "github", RepoID: &repoID,
		Assignees: []string{"dev@example.com"}, OrgID: "org-acme",
	}
	_, _, unknownCandidates := unknown.Resolve(unknownSubject)
	unknownReason := ""
	for _, candidate := range unknownCandidates {
		if candidate.Source == "assignee_membership" {
			unknownReason = candidate.OwnershipReason
		}
	}
	if !ownershipUnknownBlocksMembership && unknownReason != MembershipOwnershipReasonUnknown {
		t.Fatalf("R74 pass-through assignee_membership candidate.OwnershipReason = %q, want %s", unknownReason, MembershipOwnershipReasonUnknown)
	}
	if ownedReason == unknownReason {
		t.Fatalf("owned and ownership_unknown candidates must carry DIFFERENT OwnershipReason values, both got %q", ownedReason)
	}
}

// TestCascadeGateChecksOwnershipByRepositoryNameWhenNoRepoIDMatches is
// CHAOS-4320's mutation-resistant pin for codex round 4's P3: every other
// gate test in this file sets subject.RepoID, so teamOwnsSubjectRepo's
// repoByID lookup alone accounts for every one of them -- removing ONLY the
// repoByName lookup (leaving repoByID untouched) passed the complete
// teamattribution AND providersync suites (executed, round 4). This test
// gives the subject NO RepoID at all, matching a repo ownership fact by
// ProjectID/RepoFullName alone -- the SAME repoByName lookup repo_ownership
// itself already depends on for a name-only subject -- so only THAT lookup
// can produce the result either assertion below checks.
func TestCascadeGateChecksOwnershipByRepositoryNameWhenNoRepoIDMatches(t *testing.T) {
	now := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)

	facts := GithubWorkItemDerivationFacts{
		Repos: []GithubWorkItemDerivationRepoFact{{
			Provider: "github", TeamID: "team-repo", TeamName: "Repository Team",
			RepoFullName: "acme/api", IsPrimary: 1, Specificity: 70, UpdatedAt: now,
		}},
		Members: []GithubWorkItemDerivationMemberFact{
			{
				Provider: "github", TeamID: "team-repo", TeamName: "Repository Team",
				MemberID: "owner-dev@example.com", IsPrimary: 1, Specificity: 50, UpdatedAt: now,
			},
			{
				Provider: "github", TeamID: "team-other", TeamName: "Other Team",
				MemberID: "other-dev@example.com", IsPrimary: 1, Specificity: 50, UpdatedAt: now,
			},
		},
	}
	derived := NewGitHubWorkItemDerivationContext(facts)

	// Case 1: the assignee's team MATCHES the name-resolved owner -- granted.
	granted := GithubWorkItemDerivationSubject{
		WorkItemID: "gh:acme/api#20", Provider: "github", ProjectID: GithubWorkItemDerivationStringPointer("acme/api"),
		Assignees: []string{"owner-dev@example.com"}, OrgID: "org-acme",
	}
	_, _, grantedCandidates := derived.Resolve(granted)
	var grantedMembership *GithubWorkItemDerivationCandidate
	for index := range grantedCandidates {
		if grantedCandidates[index].Source == "assignee_membership" {
			grantedMembership = &grantedCandidates[index]
		}
	}
	if grantedMembership == nil {
		t.Fatalf("candidates = %+v, want an assignee_membership row (team-repo owns acme/api by NAME)", grantedCandidates)
	}

	// Case 2: the assignee's team does NOT match the name-resolved owner --
	// rejected, exactly like the RepoID-keyed gate tests above.
	rejected := GithubWorkItemDerivationSubject{
		WorkItemID: "gh:acme/api#21", Provider: "github", ProjectID: GithubWorkItemDerivationStringPointer("acme/api"),
		Assignees: []string{"other-dev@example.com"}, OrgID: "org-acme",
	}
	_, _, rejectedCandidates := derived.Resolve(rejected)
	for _, candidate := range rejectedCandidates {
		if candidate.Source == "assignee_membership" {
			t.Fatalf("candidates = %+v, want NO assignee_membership row (team-other does not own acme/api by NAME)", rejectedCandidates)
		}
	}
}

// TestAuthorMembershipNeverStacksASecondTeamOntoAHigherRankedPrimary is
// CHAOS-5649's (R179 rule 1, chris 2026-09-12) red-first pin: on prod, 834
// author_membership rows were non-primary duplicates recorded on work items
// that ALREADY had a higher-ranked primary attribution -- a straight double
// count in the table. team-third here independently co-owns repoID (so
// CHAOS-4320's teamOwnsSubjectRepo gate itself would pass it, verified
// directly below), isolating rule 1 (stacking suppression) from CHAOS-4320's
// separate ownership gate.
func TestAuthorMembershipNeverStacksASecondTeamOntoAHigherRankedPrimary(t *testing.T) {
	now := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	repoID := "c7198fbc-1945-3717-05d8-eb78866b4e79"

	derived := NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{
		Repos: []GithubWorkItemDerivationRepoFact{
			{Provider: "github", TeamID: "team-primary", TeamName: "Primary Team", RepoID: &repoID, RepoFullName: "acme/api", IsPrimary: 1, Specificity: 70, UpdatedAt: now},
			{Provider: "github", TeamID: "team-third", TeamName: "Third Team", RepoID: &repoID, RepoFullName: "acme/api", IsPrimary: 1, Specificity: 70, UpdatedAt: now},
		},
		Members: []GithubWorkItemDerivationMemberFact{{
			Provider: "github", TeamID: "team-third", TeamName: "Third Team",
			MemberID: "alice", IsPrimary: 1, Specificity: 50, UpdatedAt: now,
		}},
	})
	reporter := "alice"
	subject := GithubWorkItemDerivationSubject{
		WorkItemID: "ghpr:acme/api#30", Provider: "github", Type: "pr",
		RepoID: &repoID, Reporter: &reporter, OrgID: "org-acme",
	}
	if reason, owns := derived.teamOwnsSubjectRepo(subject, "team-third"); !owns {
		t.Fatalf("teamOwnsSubjectRepo(team-third) = (%q, false), want owns=true -- this test isolates rule 1 from the CHAOS-4320 gate", reason)
	}
	teamID, _, candidates := derived.Resolve(subject)
	if got := GithubWorkItemDerivationStringValue(teamID); got != "team-primary" {
		t.Fatalf("primary team id = %q, want team-primary (repo_ownership, first-inserted owner)", got)
	}
	for _, candidate := range candidates {
		if candidate.Source == "author_membership" {
			t.Fatalf("candidates = %+v, want NO author_membership row (CHAOS-5649 R179 rule 1: repo_ownership already primary)", candidates)
		}
	}
}

// TestAssigneeMembershipStillStacksOntoAHigherRankedPrimary is the contrast
// case for the test above: CHAOS-5649's ruling names author_membership
// specifically (rank 6, the lowest-ranked person signal); every OTHER
// source in `order`, assignee_membership included, keeps recording its
// non-primary provenance rows exactly as before -- the "no team_id collapse
// here, deliberately" contract above this loop still applies to them. Same
// fixture shape as the author test, through Assignees instead of Reporter.
func TestAssigneeMembershipStillStacksOntoAHigherRankedPrimary(t *testing.T) {
	now := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	repoID := "c7198fbc-1945-3717-05d8-eb78866b4e79"

	derived := NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{
		Repos: []GithubWorkItemDerivationRepoFact{
			{Provider: "github", TeamID: "team-primary", TeamName: "Primary Team", RepoID: &repoID, RepoFullName: "acme/api", IsPrimary: 1, Specificity: 70, UpdatedAt: now},
			{Provider: "github", TeamID: "team-third", TeamName: "Third Team", RepoID: &repoID, RepoFullName: "acme/api", IsPrimary: 1, Specificity: 70, UpdatedAt: now},
		},
		Members: []GithubWorkItemDerivationMemberFact{{
			Provider: "github", TeamID: "team-third", TeamName: "Third Team",
			MemberID: "alice", IsPrimary: 1, Specificity: 50, UpdatedAt: now,
		}},
	})
	subject := GithubWorkItemDerivationSubject{
		WorkItemID: "gh:acme/api#31", Provider: "github",
		RepoID: &repoID, Assignees: []string{"alice"}, OrgID: "org-acme",
	}
	teamID, _, candidates := derived.Resolve(subject)
	if got := GithubWorkItemDerivationStringValue(teamID); got != "team-primary" {
		t.Fatalf("primary team id = %q, want team-primary (repo_ownership, first-inserted owner)", got)
	}
	var assignee *GithubWorkItemDerivationCandidate
	for index := range candidates {
		if candidates[index].Source == "assignee_membership" {
			assignee = &candidates[index]
		}
	}
	if assignee == nil {
		t.Fatalf("candidates = %+v, want an assignee_membership row present (rule 1 names author_membership only)", candidates)
	}
	if got := GithubWorkItemDerivationStringValue(assignee.TeamID); got != "team-third" || assignee.IsPrimary != 0 {
		t.Fatalf("assignee_membership candidate = %+v, want team-third, IsPrimary=0", assignee)
	}
}

// TestNullCarryingProviderTeamExcludedFromAuthorCascade is CHAOS-5649's
// (R179 rule 2, chris 2026-09-12) red-first pin for the concrete prod
// shape: gh:ops-team is a real Teams-catalog row (github-provider-synced,
// no project_keys, per the fixture below) with no repo_ownership or
// project_ownership row anywhere -- null-carrying. Its one member
// (chrisgeo, mirroring github:chrisgeo on prod) authors a PR on a repo with
// NO ownership data at all. Before this fix, R74's unknown-repo pass-through
// let the author_membership candidate through regardless, giving gh:ops-team
// 232 primary attributions on prod that belong to nobody. After this fix,
// the team-level null-carrying gate rejects it BEFORE the repo-level R74
// check ever runs, and the item falls through to unassigned.
func TestNullCarryingProviderTeamExcludedFromAuthorCascade(t *testing.T) {
	now := time.Date(2026, 9, 12, 12, 0, 0, 0, time.UTC)
	repoID := "d41d8cd9-8f00-3204-a980-0998ecf8427e"

	derived := NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{
		Teams: []GithubWorkItemDerivationTeamFact{{
			Provider: "github", TeamID: "gh:ops-team", TeamName: "Ops Team",
			ProjectKeys: nil, UpdatedAt: now,
		}},
		// Deliberately NO Repos/Projects fact naming gh:ops-team anywhere --
		// the null-carrying shape.
		Members: []GithubWorkItemDerivationMemberFact{{
			Provider: "github", TeamID: "gh:ops-team", TeamName: "Ops Team",
			MemberID: "chrisgeo", IsPrimary: 1, Specificity: 50, UpdatedAt: now,
		}},
	})
	if !derived.teamIsNullCarrying("gh:ops-team") {
		t.Fatal("teamIsNullCarrying(gh:ops-team) = false, want true (catalogued, zero ownership signal)")
	}
	reporter := "chrisgeo"
	subject := GithubWorkItemDerivationSubject{
		WorkItemID: "ghpr:acme/tools#5", Provider: "github", Type: "pr",
		RepoID: &repoID, Reporter: &reporter, OrgID: "org-acme",
	}
	teamID, _, candidates := derived.Resolve(subject)
	if got := GithubWorkItemDerivationStringValue(teamID); got != "" {
		t.Fatalf("primary team id = %q, want \"\" (unassigned -- gh:ops-team is null-carrying)", got)
	}
	var unassigned *GithubWorkItemDerivationCandidate
	for index := range candidates {
		if candidates[index].Source == "author_membership" {
			t.Fatalf("candidates = %+v, want NO author_membership row (gh:ops-team is null-carrying)", candidates)
		}
		if candidates[index].Source == "unassigned" {
			unassigned = &candidates[index]
		}
	}
	if unassigned == nil {
		t.Fatalf("candidates = %+v, want an unassigned row", candidates)
	}
	if got := unassigned.Evidence; got != "no_candidate:team_null_carrying" {
		t.Fatalf("unassigned evidence = %q, want \"no_candidate:team_null_carrying\"", got)
	}
}

// TestNullCarryingProviderTeamExcludedFromAssigneeCascade mirrors the test
// above through the assignee path, confirming CHAOS-5649's rule 2 gates
// BOTH membership paths teamOwnsSubjectRepo already gates for CHAOS-4320,
// not just author_membership.
func TestNullCarryingProviderTeamExcludedFromAssigneeCascade(t *testing.T) {
	now := time.Date(2026, 9, 12, 12, 0, 0, 0, time.UTC)
	repoID := "d41d8cd9-8f00-3204-a980-0998ecf8427e"

	derived := NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{
		Teams: []GithubWorkItemDerivationTeamFact{{
			Provider: "github", TeamID: "gh:ops-team", TeamName: "Ops Team",
			ProjectKeys: nil, UpdatedAt: now,
		}},
		Members: []GithubWorkItemDerivationMemberFact{{
			Provider: "github", TeamID: "gh:ops-team", TeamName: "Ops Team",
			MemberID: "chrisgeo", IsPrimary: 1, Specificity: 50, UpdatedAt: now,
		}},
	})
	subject := GithubWorkItemDerivationSubject{
		WorkItemID: "gh:acme/tools#6", Provider: "github",
		RepoID: &repoID, Assignees: []string{"chrisgeo"}, OrgID: "org-acme",
	}
	teamID, _, candidates := derived.Resolve(subject)
	if got := GithubWorkItemDerivationStringValue(teamID); got != "" {
		t.Fatalf("primary team id = %q, want \"\" (unassigned -- gh:ops-team is null-carrying)", got)
	}
	for _, candidate := range candidates {
		if candidate.Source == "assignee_membership" {
			t.Fatalf("candidates = %+v, want NO assignee_membership row (gh:ops-team is null-carrying)", candidates)
		}
	}
}

// TestUncatalogedTeamStaysOnR74UnknownPassThrough guards R74 (chris via
// team-lead, 2026-09-09, decision log 93c4f16842d8): CHAOS-5649's rule 2
// gate must NOT re-litigate it. A team this context never saw a Teams
// catalog row for at all (only a Members fact, exactly like
// TestCascadeGateOwnershipUnknownMatchesConfiguredDefault's fixture) is
// UNKNOWN, not proven null-carrying, and still passes through.
func TestUncatalogedTeamStaysOnR74UnknownPassThrough(t *testing.T) {
	derived := NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{})
	if derived.teamIsNullCarrying("team-never-catalogued") {
		t.Fatal("teamIsNullCarrying(team-never-catalogued) = true, want false (no Teams catalog row seen at all -- unknown, not null-carrying)")
	}
}
