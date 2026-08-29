package providersync

import (
	"context"
	"testing"
)

// This file pins GitLab's own fail-safe guard wrappers for CHAOS-4431 codex
// review findings #3 (drift projector bypass, sync_policy) and #6
// (membership conflict review bypass) -- team-lead ruling 2026-08-28,
// mirroring what LinearTeamCatalogCollector already wires
// (linear_team_catalog_collector.go, team_sync_policy_guard.go,
// team_membership_conflict_guard.go). RED on this tip: gitlabMembershipConflictsWithManualState
// and applyGitLabTeamSyncPolicyGuard do not exist yet -- deliberately, per
// team-lead's instruction to hold the implementation for the rebase onto
// 4431's next base push (corrected membership-conflict semantics +
// ResolveSelections' new `strict bool` param land there). GREEN comes in the
// follow-up commit on that rebased tip.
//
// Corrected membership-conflict semantics (team-lead relay, 2026-08-28,
// INVERTED from 804ba3522's Linear implementation which this test explicitly
// does NOT mirror): an active manual team_memberships row for the EXACT SAME
// (member_id, team_id) pair as a native row CONFIRMS that native row (an
// admin agreeing with discovery), not a conflict -- keep it. An active
// manual row for the SAME MEMBER but a DIFFERENT team is the real conflict
// (an admin pinned this member elsewhere; a native row asserting THIS team
// would contradict that pin) -- skip it. A member-scoped manual_attribution_
// fallbacks match is a conflict regardless of team, unchanged.

// TestGitLabMembershipConflictsWithManualStateTreatsSamePairAsConfirmation
// pins the corrected semantics' headline change: an exact (member_id,
// team_id) match against an active manual membership is NOT a conflict.
func TestGitLabMembershipConflictsWithManualStateTreatsSamePairAsConfirmation(t *testing.T) {
	row := gitlabTeamCatalogMembershipRow{MemberID: "gitlab:alice", TeamID: "gl:org"}
	manualPairs := map[membershipConflictPair]struct{}{
		{MemberID: "gitlab:alice", TeamID: "gl:org"}: {},
	}
	if gitlabMembershipConflictsWithManualState(row, manualPairs, nil) {
		t.Fatal("an exact (member_id, team_id) manual match is a CONFIRMATION, not a conflict -- must not be skipped")
	}
}

// TestGitLabMembershipConflictsWithManualStateTreatsDifferentTeamAsConflict
// pins the corrected semantics' other half: an active manual membership for
// the SAME member but a DIFFERENT team is the real conflict.
func TestGitLabMembershipConflictsWithManualStateTreatsDifferentTeamAsConflict(t *testing.T) {
	row := gitlabTeamCatalogMembershipRow{MemberID: "gitlab:alice", TeamID: "gl:org/team-a"}
	manualPairs := map[membershipConflictPair]struct{}{
		{MemberID: "gitlab:alice", TeamID: "gl:org"}: {},
	}
	if !gitlabMembershipConflictsWithManualState(row, manualPairs, nil) {
		t.Fatal("a manual pin to a DIFFERENT team for the same member must be a conflict -- must be skipped")
	}
}

// TestGitLabMembershipConflictsWithManualStateSkipsMemberScopedFallback
// proves the member-scoped manual_attribution_fallbacks check is unchanged
// by the pair-semantics correction: it still blocks regardless of team.
func TestGitLabMembershipConflictsWithManualStateSkipsMemberScopedFallback(t *testing.T) {
	row := gitlabTeamCatalogMembershipRow{
		MemberID: "gitlab:alice", TeamID: "gl:org",
		IdentityFacets: []string{"gitlab:alice", "alice@example.com"},
	}
	fallbacks := map[string]struct{}{"alice@example.com": {}}
	if !gitlabMembershipConflictsWithManualState(row, nil, fallbacks) {
		t.Fatal("want conflict for a normalized-matching member-scoped fallback")
	}
}

// TestGitLabMembershipConflictsWithManualStateAllowsCleanRow is the negative
// case: no manual pair (same or different team) and no matching fallback --
// a genuinely clean native row must never be skipped.
func TestGitLabMembershipConflictsWithManualStateAllowsCleanRow(t *testing.T) {
	row := gitlabTeamCatalogMembershipRow{
		MemberID: "gitlab:carol", TeamID: "gl:org",
		IdentityFacets: []string{"gitlab:carol", "carol@example.com"},
	}
	manualPairs := map[membershipConflictPair]struct{}{
		{MemberID: "gitlab:someone-else", TeamID: "gl:org"}: {},
	}
	fallbacks := map[string]struct{}{"someone-else@example.com": {}}
	if gitlabMembershipConflictsWithManualState(row, manualPairs, fallbacks) {
		t.Fatal("a clean row with no matching manual pair or fallback must not be skipped")
	}
}

// TestGitLabApplyTeamSyncPolicyGuardNoopsOnEmptyInput pins the same
// zero-input short-circuit applyTeamSyncPolicyGuard already guarantees
// (team_sync_policy_guard.go) -- no DB round trip for an empty team slice,
// so this is runnable without a real ClickHouse connection.
func TestGitLabApplyTeamSyncPolicyGuardNoopsOnEmptyInput(t *testing.T) {
	kept, skipped, err := applyGitLabTeamSyncPolicyGuard(context.Background(), nil, "org-1", nil)
	if err != nil {
		t.Fatalf("unexpected error on empty input: %v", err)
	}
	if len(kept) != 0 || len(skipped) != 0 {
		t.Fatalf("kept=%v skipped=%v, want both empty", kept, skipped)
	}
}
