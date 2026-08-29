package providersync

import (
	"context"
	"testing"
	"time"
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

// TestGitHubMembershipConflictsWithManualStateConfirmsSameTeamFallback pins
// round 3's correction (codex round 1 P2 on this collector, already fixed
// upstream in e4bb50e74): a member-scoped manual_attribution_fallbacks row
// for the SAME team as the incoming native row is a CONFIRMATION, not a
// conflict -- fallbacks compare team_id exactly like manual memberships do,
// they are NOT team-agnostic (an earlier revision of this file, and of
// team_membership_conflict_guard.go, got this wrong).
func TestGitHubMembershipConflictsWithManualStateConfirmsSameTeamFallback(t *testing.T) {
	row := githubMembershipRow{
		MemberID: "github:octocat", TeamID: "gh:platform",
		RawEmail: linearReferenceStringPtr("Octocat@Example.com"), // mixed case, must normalize-match
	}
	fallbackTeamsByIdentity := map[string]map[string]struct{}{
		"octocat@example.com": {"gh:platform": {}},
	}
	if githubMembershipConflictsWithManualState(row, nil, fallbackTeamsByIdentity) {
		t.Fatal("a member-scoped fallback for the SAME team is a confirmation, not a conflict")
	}
}

// TestGitHubMembershipConflictsWithManualStateFlagsDifferentTeamFallbackAsConflict
// is the fallback source's corrected positive case: an active fallback
// naming a DIFFERENT team than the incoming native row blocks the write.
func TestGitHubMembershipConflictsWithManualStateFlagsDifferentTeamFallbackAsConflict(t *testing.T) {
	row := githubMembershipRow{
		MemberID: "github:octocat", TeamID: "gh:platform",
		RawEmail: linearReferenceStringPtr("octocat@example.com"),
	}
	fallbackTeamsByIdentity := map[string]map[string]struct{}{
		"octocat@example.com": {"gh:other-team": {}},
	}
	if !githubMembershipConflictsWithManualState(row, nil, fallbackTeamsByIdentity) {
		t.Fatal("want conflict: fallback names a DIFFERENT team than this native row")
	}
}

// TestGitHubMembershipConflictsWithManualStateFlagsConflictEvenWithASameTeamRowPresent
// pins the multi-team case (round 3, P2): a member/identity with active rows
// for BOTH the incoming team AND another team is STILL a conflict --
// "the incoming team is confirmed somewhere in the set" is not sufficient on
// its own. clickhouse_identity_drift.py's _conflict_for checks every
// candidate row and conflicts on the first one that differs, it does not
// stop once it finds one that matches.
func TestGitHubMembershipConflictsWithManualStateFlagsConflictEvenWithASameTeamRowPresent(t *testing.T) {
	row := githubMembershipRow{MemberID: "github:octocat", TeamID: "gh:platform"}
	manualTeamsByMember := map[string]map[string]struct{}{
		// octocat has an active manual pin to BOTH the incoming team
		// (platform) and a different one (other-team).
		"github:octocat": {"gh:platform": {}, "gh:other-team": {}},
	}
	if !githubMembershipConflictsWithManualState(row, manualTeamsByMember, nil) {
		t.Fatal("want conflict: a same-team manual pin does not clear a DIFFERENT-team pin also present " +
			"for this member")
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
	fallbackTeamsByIdentity := map[string]map[string]struct{}{
		"unrelated@example.com": {"gh:platform": {}},
	}
	if githubMembershipConflictsWithManualState(row, manualTeamsByMember, fallbackTeamsByIdentity) {
		t.Fatal("a clean row with no matching manual membership or fallback must not be skipped")
	}
}

// TestApplyGitHubTeamSyncPolicyGuardShortCircuitsOnEmptyTeams pins that the
// wrapper (mirroring Linear's applyTeamSyncPolicyGuard) must return before
// ever touching its conn argument when there is nothing to filter -- proven
// here by passing a nil conn: a version that queried unconditionally would
// nil-pointer-dereference/panic instead of returning cleanly.
func TestApplyGitHubTeamSyncPolicyGuardShortCircuitsOnEmptyTeams(t *testing.T) {
	kept, skipped, staged, superseded, err := applyGitHubTeamSyncPolicyGuard(context.Background(), nil, "org-1", nil, time.Now())
	if err != nil {
		t.Fatalf("empty input must short-circuit before ever touching conn: err=%v", err)
	}
	if len(kept) != 0 || len(skipped) != 0 || staged != 0 || superseded != 0 {
		t.Fatalf("kept=%+v skipped=%+v staged=%d superseded=%d want all empty/zero", kept, skipped, staged, superseded)
	}
}
