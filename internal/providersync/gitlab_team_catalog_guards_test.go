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
// team_membership_conflict_guard.go).
//
// Conflict semantics (codex review round 3 -- team-lead relay, 2026-08-28):
// both sources (manual team_memberships and manual_attribution_fallbacks)
// are team-scoped. A row (manual or fallback) to the EXACT SAME team as the
// incoming native row CONFIRMS it. A row to ANY OTHER team is a conflict --
// checked across EVERY active row for that member/identity, not just
// whether the incoming team is present in the set: a member with active
// rows for BOTH the incoming team and another team is still a conflict.

// TestGitLabMembershipConflictsWithManualStateTreatsSamePairAsConfirmation
// pins the base case: a manual membership to the EXACT SAME team as the
// incoming row, and nothing else, is a confirmation, not a conflict.
func TestGitLabMembershipConflictsWithManualStateTreatsSamePairAsConfirmation(t *testing.T) {
	row := gitlabTeamCatalogMembershipRow{MemberID: "gitlab:alice", TeamID: "gl:org"}
	manualTeamsByMember := map[string]map[string]struct{}{
		"gitlab:alice": {"gl:org": {}},
	}
	if gitlabMembershipConflictsWithManualState(row, manualTeamsByMember, nil) {
		t.Fatal("a manual membership to the SAME team, and nothing else, is a CONFIRMATION -- must not be skipped")
	}
}

// TestGitLabMembershipConflictsWithManualStateTreatsDifferentTeamAsConflict
// pins the conflict case: a manual membership to a DIFFERENT team than the
// incoming row.
func TestGitLabMembershipConflictsWithManualStateTreatsDifferentTeamAsConflict(t *testing.T) {
	row := gitlabTeamCatalogMembershipRow{MemberID: "gitlab:alice", TeamID: "gl:org/team-a"}
	manualTeamsByMember := map[string]map[string]struct{}{
		"gitlab:alice": {"gl:org": {}},
	}
	if !gitlabMembershipConflictsWithManualState(row, manualTeamsByMember, nil) {
		t.Fatal("a manual pin to a DIFFERENT team for the same member must be a conflict -- must be skipped")
	}
}

// TestGitLabMembershipConflictsWithManualStateConflictsWhenAnyOtherTeamPresent
// is the codex round-3 correction's own regression proof: a member with
// active manual memberships to BOTH the incoming team AND another team is
// STILL a conflict -- the incoming team being present in the set does not
// clear it, because Python's loop checks every row, not just the first
// match.
func TestGitLabMembershipConflictsWithManualStateConflictsWhenAnyOtherTeamPresent(t *testing.T) {
	row := gitlabTeamCatalogMembershipRow{MemberID: "gitlab:alice", TeamID: "gl:org"}
	manualTeamsByMember := map[string]map[string]struct{}{
		"gitlab:alice": {"gl:org": {}, "gl:org/team-a": {}},
	}
	if !gitlabMembershipConflictsWithManualState(row, manualTeamsByMember, nil) {
		t.Fatal("an active manual membership to ANOTHER team must still be a conflict, even though the incoming team is ALSO present")
	}
}

// TestGitLabMembershipConflictsWithManualStateFallbackSameTeamIsConfirmation
// proves the round-3 correction's other half: an active member-scoped
// manual_attribution_fallbacks row is now team-scoped too -- a fallback to
// the SAME team as the incoming row is a confirmation, not a conflict.
func TestGitLabMembershipConflictsWithManualStateFallbackSameTeamIsConfirmation(t *testing.T) {
	row := gitlabTeamCatalogMembershipRow{
		MemberID: "gitlab:alice", TeamID: "gl:org",
		IdentityFacets: []string{"gitlab:alice", "alice@example.com"},
	}
	fallbackTeamsByIdentity := map[string]map[string]struct{}{
		"alice@example.com": {"gl:org": {}},
	}
	if gitlabMembershipConflictsWithManualState(row, nil, fallbackTeamsByIdentity) {
		t.Fatal("a fallback row to the SAME team, and nothing else, is a CONFIRMATION -- must not be skipped")
	}
}

// TestGitLabMembershipConflictsWithManualStateFallbackDifferentTeamIsConflict
// proves a fallback row to a DIFFERENT team is still a conflict, matching
// the manual-membership source's shape exactly.
func TestGitLabMembershipConflictsWithManualStateFallbackDifferentTeamIsConflict(t *testing.T) {
	row := gitlabTeamCatalogMembershipRow{
		MemberID: "gitlab:alice", TeamID: "gl:org/team-a",
		IdentityFacets: []string{"gitlab:alice", "alice@example.com"},
	}
	fallbackTeamsByIdentity := map[string]map[string]struct{}{
		"alice@example.com": {"gl:org": {}},
	}
	if !gitlabMembershipConflictsWithManualState(row, nil, fallbackTeamsByIdentity) {
		t.Fatal("want conflict for a fallback row pointing at a DIFFERENT team")
	}
}

// TestGitLabMembershipConflictsWithManualStateAllowsCleanRow is the negative
// case: no manual or fallback row at all for this member/identity -- a
// genuinely clean native row must never be skipped.
func TestGitLabMembershipConflictsWithManualStateAllowsCleanRow(t *testing.T) {
	row := gitlabTeamCatalogMembershipRow{
		MemberID: "gitlab:carol", TeamID: "gl:org",
		IdentityFacets: []string{"gitlab:carol", "carol@example.com"},
	}
	manualTeamsByMember := map[string]map[string]struct{}{
		"gitlab:someone-else": {"gl:org": {}},
	}
	fallbackTeamsByIdentity := map[string]map[string]struct{}{
		"someone-else@example.com": {"gl:org": {}},
	}
	if gitlabMembershipConflictsWithManualState(row, manualTeamsByMember, fallbackTeamsByIdentity) {
		t.Fatal("a clean row with no matching manual or fallback row must not be skipped")
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

// TestGitLabApplyTeamMembershipConflictGuardNoopsOnEmptyInput mirrors the
// same zero-input short-circuit for the membership-conflict guard.
func TestGitLabApplyTeamMembershipConflictGuardNoopsOnEmptyInput(t *testing.T) {
	kept, skipped, err := applyGitLabTeamMembershipConflictGuard(context.Background(), nil, "org-1", "gitlab", nil)
	if err != nil {
		t.Fatalf("unexpected error on empty input: %v", err)
	}
	if len(kept) != 0 || skipped != 0 {
		t.Fatalf("kept=%v skipped=%d, want both empty/zero", kept, skipped)
	}
}
