package providersync

import (
	"context"
	"testing"
)

// github_team_catalog_guards_test.go pins the semantics (team-lead ruling,
// 2026-08-28) for GitHub's own wrappers of the two fail-safe guards Linear
// already has -- githubMembershipConflictsWithManualState (CHAOS-4431 codex
// review finding #6, ROUND 2 corrected shape: member-scoped, not
// pair-scoped -- see team_membership_conflict_guard.go's doc comment) and
// applyGitHubTeamSyncPolicyGuard (finding #3). Plain unit tests: no build
// tag, no I/O, no containers.

// TestGitHubMembershipConflictsWithManualStateConfirmsExactManualPair pins
// the round-2 correction: a manual membership to the SAME team as this
// native row is a CONFIRMATION, not a conflict.
func TestGitHubMembershipConflictsWithManualStateConfirmsExactManualPair(t *testing.T) {
	row := githubMembershipRow{MemberID: "github:octocat", TeamID: "gh:platform"}
	manualTeamsByMember := map[string]map[string]struct{}{
		"github:octocat": {"gh:platform": {}},
	}
	if githubMembershipConflictsWithManualState(row, manualTeamsByMember, nil) {
		t.Fatal("a manual membership to the SAME team is a confirmation, not a conflict")
	}
}

// TestGitHubMembershipConflictsWithManualStateRejectsDifferentTeamManualPin
// is the corrected positive case: octocat has an active manual pin to team
// A, but GitHub now reports them on a DIFFERENT team B -- B's native row
// must be rejected.
func TestGitHubMembershipConflictsWithManualStateRejectsDifferentTeamManualPin(t *testing.T) {
	row := githubMembershipRow{MemberID: "github:octocat", TeamID: "gh:other-team"}
	manualTeamsByMember := map[string]map[string]struct{}{
		"github:octocat": {"gh:platform": {}},
	}
	if !githubMembershipConflictsWithManualState(row, manualTeamsByMember, nil) {
		t.Fatal("want conflict: member has an active manual pin to a DIFFERENT team than this native row")
	}
}

// TestGitHubMembershipConflictsWithManualStateAllowsMemberWithNoManualMembership
// pins the negative case for the manual-membership source: a member with no
// active manual membership at all is never flagged by this source.
func TestGitHubMembershipConflictsWithManualStateAllowsMemberWithNoManualMembership(t *testing.T) {
	row := githubMembershipRow{MemberID: "github:monalisa", TeamID: "gh:platform"}
	manualTeamsByMember := map[string]map[string]struct{}{
		"github:octocat": {"gh:platform": {}},
	}
	if githubMembershipConflictsWithManualState(row, manualTeamsByMember, nil) {
		t.Fatal("a member with no manual membership at all must not be flagged")
	}
}

// TestGitHubMembershipConflictsWithManualStateFlagsMemberScopedFallbackAsConflict
// pins the guard's second source: an active member-scoped
// manual_attribution_fallbacks match blocks the write regardless of team --
// unaffected by the round-2 correction, already team-agnostic.
func TestGitHubMembershipConflictsWithManualStateFlagsMemberScopedFallbackAsConflict(t *testing.T) {
	row := githubMembershipRow{
		MemberID: "github:octocat", TeamID: "gh:platform",
		RawEmail: linearReferenceStringPtr("Octocat@Example.com"), // mixed case, must normalize-match
	}
	fallbacks := map[string]struct{}{"octocat@example.com": {}}
	if !githubMembershipConflictsWithManualState(row, nil, fallbacks) {
		t.Fatal("an active member-scoped manual_attribution_fallbacks match must block the write, " +
			"regardless of which team it names")
	}
}

// TestGitHubMembershipConflictsWithManualStateAllowsCleanRow is the fully
// negative case: no manual membership for this member, no matching
// fallback -- a genuinely clean native row must never be skipped.
func TestGitHubMembershipConflictsWithManualStateAllowsCleanRow(t *testing.T) {
	row := githubMembershipRow{
		MemberID: "github:octocat", TeamID: "gh:platform",
		RawEmail:       linearReferenceStringPtr("octocat@example.com"),
		IdentityFacets: []string{"github:octocat", "octocat@example.com"},
	}
	manualTeamsByMember := map[string]map[string]struct{}{
		"github:someone-else": {"gh:platform": {}},
	}
	fallbacks := map[string]struct{}{"unrelated@example.com": {}}
	if githubMembershipConflictsWithManualState(row, manualTeamsByMember, fallbacks) {
		t.Fatal("a clean row with no matching manual membership or fallback must not be skipped")
	}
}

// TestApplyGitHubTeamSyncPolicyGuardShortCircuitsOnEmptyTeams pins that the
// wrapper (mirroring Linear's applyTeamSyncPolicyGuard) must return before
// ever touching its conn argument when there is nothing to filter -- proven
// here by passing a nil conn: a version that queried unconditionally would
// nil-pointer-dereference/panic instead of returning cleanly.
func TestApplyGitHubTeamSyncPolicyGuardShortCircuitsOnEmptyTeams(t *testing.T) {
	kept, skipped, err := applyGitHubTeamSyncPolicyGuard(context.Background(), nil, "org-1", nil)
	if err != nil {
		t.Fatalf("empty input must short-circuit before ever touching conn: err=%v", err)
	}
	if len(kept) != 0 || len(skipped) != 0 {
		t.Fatalf("kept=%+v skipped=%+v want both empty", kept, skipped)
	}
}
