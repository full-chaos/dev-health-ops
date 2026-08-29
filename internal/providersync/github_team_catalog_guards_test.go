package providersync

import (
	"context"
	"testing"
)

// github_team_catalog_guards_test.go pins the CORRECTED semantics (team-lead
// ruling, 2026-08-28) for the two fail-safe guards GitHub's collector still
// needs -- membershipConflictsWithManualState (CHAOS-4431 codex review
// finding #6) and the sync_policy guard (finding #3) -- BEFORE either
// wrapper is implemented, so the behavior is pinned ahead of the integration
// tests in github_team_catalog_collector_integration_test.go (which need a
// real ClickHouse and only run in-slot). These are plain unit tests (no
// build tag, no I/O): they are RED right now because
// githubMembershipConflictsWithManualState and applyGitHubTeamSyncPolicyGuard
// do not exist yet -- the whole package fails to compile under a plain
// `go test ./internal/providersync/...`, which is the point: the next
// rebase's implementation must satisfy these exact cases to go green.
//
// Corrected semantics (differs from Linear's PRE-fix behavior, which this
// same ruling also corrects in 4431's next base): an exact (member_id,
// team_id) match against active manual data is a CONFIRMATION (keep, do NOT
// skip) -- the native write agrees with the manual pin. Only a manual pin to
// a DIFFERENT team, or a member-scoped manual_attribution_fallbacks match
// (team-agnostic), is a real conflict (skip).

func TestGitHubMembershipConflictsWithManualStateTreatsExactPairAsConfirmation(t *testing.T) {
	row := githubMembershipRow{MemberID: "github:octocat", TeamID: "gh:platform"}
	manualPairs := map[membershipConflictPair]struct{}{
		{MemberID: "github:octocat", TeamID: "gh:platform"}: {},
	}
	if githubMembershipConflictsWithManualState(row, manualPairs, nil) {
		t.Fatal("an exact (member_id, team_id) match against active manual data is a CONFIRMATION, " +
			"not a conflict -- corrected CHAOS-4431 semantics, team-lead ruling 2026-08-28")
	}
}

func TestGitHubMembershipConflictsWithManualStateFlagsDifferentTeamPinAsConflict(t *testing.T) {
	row := githubMembershipRow{MemberID: "github:octocat", TeamID: "gh:platform"}
	manualPairs := map[membershipConflictPair]struct{}{
		{MemberID: "github:octocat", TeamID: "gh:other-team"}: {},
	}
	if !githubMembershipConflictsWithManualState(row, manualPairs, nil) {
		t.Fatal("a manual pin to a DIFFERENT team must block this team's native write")
	}
}

func TestGitHubMembershipConflictsWithManualStateFlagsMemberScopedFallbackAsConflict(t *testing.T) {
	row := githubMembershipRow{
		MemberID: "github:octocat", TeamID: "gh:platform",
		RawEmail: linearReferenceStringPtr("Octocat@Example.com"), // mixed case, must normalize-match
	}
	fallbacks := map[string]struct{}{"octocat@example.com": {}}
	if !githubMembershipConflictsWithManualState(row, nil, fallbacks) {
		t.Fatal("an active member-scoped manual_attribution_fallbacks match must block the write, " +
			"regardless of which team it names (fallbacks are member-scoped, not team-scoped)")
	}
}

func TestGitHubMembershipConflictsWithManualStateAllowsCleanRow(t *testing.T) {
	row := githubMembershipRow{
		MemberID: "github:octocat", TeamID: "gh:platform",
		RawEmail:       linearReferenceStringPtr("octocat@example.com"),
		IdentityFacets: []string{"github:octocat", "octocat@example.com"},
	}
	manualPairs := map[membershipConflictPair]struct{}{
		{MemberID: "github:someone-else", TeamID: "gh:platform"}: {},
	}
	fallbacks := map[string]struct{}{"unrelated@example.com": {}}
	if githubMembershipConflictsWithManualState(row, manualPairs, fallbacks) {
		t.Fatal("a clean row with no matching manual pair or fallback must not be skipped")
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
