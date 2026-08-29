package providersync

import "testing"

// TestMembershipConflictsWithManualStateSkipsExactManualPair pins the
// CHAOS-4431 codex review finding #6 fail-safe guard's first source (team-
// lead ruling, 2026-08-28): a native membership row whose EXACT (member_id,
// team_id) pair already has an active manual membership must be skipped.
func TestMembershipConflictsWithManualStateSkipsExactManualPair(t *testing.T) {
	row := linearReferenceMembershipRow{MemberID: "linear:alice@example.com", TeamID: "ENG"}
	manualPairs := map[membershipConflictPair]struct{}{
		{MemberID: "linear:alice@example.com", TeamID: "ENG"}: {},
	}
	if !membershipConflictsWithManualState(row, manualPairs, nil) {
		t.Fatal("want conflict for an exact manual (member_id, team_id) match")
	}
}

// TestMembershipConflictsWithManualStateIgnoresOtherTeamsManualPairs proves
// the guard is scoped to the EXACT pair, not "this member has any manual
// membership anywhere" -- a manual pin to a DIFFERENT team must not block an
// unrelated team's native write for the same member.
func TestMembershipConflictsWithManualStateIgnoresOtherTeamsManualPairs(t *testing.T) {
	row := linearReferenceMembershipRow{MemberID: "linear:alice@example.com", TeamID: "ENG"}
	manualPairs := map[membershipConflictPair]struct{}{
		{MemberID: "linear:alice@example.com", TeamID: "OPS"}: {},
	}
	if membershipConflictsWithManualState(row, manualPairs, nil) {
		t.Fatal("a manual pin to a different team must not block this team's native write")
	}
}

// TestMembershipConflictsWithManualStateSkipsMemberScopedFallback pins the
// guard's second source: an active manual_attribution_fallbacks(scope_type=
// 'member') row matching the member's identity (by provider id, email, or
// any resolved facet) blocks the write REGARDLESS of team, mirroring
// clickhouse_identity_drift.py's own fallback check (team-agnostic).
func TestMembershipConflictsWithManualStateSkipsMemberScopedFallback(t *testing.T) {
	row := linearReferenceMembershipRow{
		MemberID: "linear:alice@example.com", TeamID: "ENG",
		RawEmail: linearReferenceStringPtr("Alice@Example.com"), // mixed case, must normalize-match
	}
	fallbacks := map[string]struct{}{"alice@example.com": {}}
	if !membershipConflictsWithManualState(row, nil, fallbacks) {
		t.Fatal("want conflict for a normalized-matching member-scoped fallback")
	}
}

// TestMembershipConflictsWithManualStateMatchesViaIdentityFacets proves the
// fallback check also matches against identity_facets, not just raw_email/
// raw_provider_user_id -- the facet list is the authoritative identity set
// the attribution ladder itself resolves against.
func TestMembershipConflictsWithManualStateMatchesViaIdentityFacets(t *testing.T) {
	row := linearReferenceMembershipRow{
		MemberID:       "linear:bob@example.com",
		TeamID:         "ENG",
		IdentityFacets: []string{"linear:bob@example.com", "bob@example.com"},
	}
	fallbacks := map[string]struct{}{"bob@example.com": {}}
	if !membershipConflictsWithManualState(row, nil, fallbacks) {
		t.Fatal("want conflict via an identity_facets match")
	}
}

// TestMembershipConflictsWithManualStateAllowsCleanRow is the negative case:
// no manual pair, no matching fallback -- a genuinely clean native row must
// never be skipped.
func TestMembershipConflictsWithManualStateAllowsCleanRow(t *testing.T) {
	row := linearReferenceMembershipRow{
		MemberID: "linear:carol@example.com", TeamID: "ENG",
		RawEmail:       linearReferenceStringPtr("carol@example.com"),
		IdentityFacets: []string{"linear:carol@example.com", "carol@example.com"},
	}
	manualPairs := map[membershipConflictPair]struct{}{
		{MemberID: "linear:alice@example.com", TeamID: "ENG"}: {},
	}
	fallbacks := map[string]struct{}{"someone-else@example.com": {}}
	if membershipConflictsWithManualState(row, manualPairs, fallbacks) {
		t.Fatal("a clean row with no matching manual pair or fallback must not be skipped")
	}
}

// TestApplyTeamMembershipConflictGuardCountsAndFiltersSkippedRows pins the
// aggregate behavior applyTeamMembershipConflictGuard's callers depend on:
// given a resolved conflict set, it must both FILTER the conflicting row out
// of the returned batch and COUNT it, never just one or the other (a count
// with no filter would still write the conflicting row; a filter with no
// count would make "skipped for a real conflict" indistinguishable from
// "nothing to write" in telemetry).
func TestApplyTeamMembershipConflictGuardCountsAndFiltersSkippedRows(t *testing.T) {
	rows := []linearReferenceMembershipRow{
		{MemberID: "linear:alice@example.com", TeamID: "ENG"},
		{MemberID: "linear:bob@example.com", TeamID: "ENG"},
	}
	manualPairs := map[membershipConflictPair]struct{}{
		{MemberID: "linear:alice@example.com", TeamID: "ENG"}: {},
	}
	kept := make([]linearReferenceMembershipRow, 0, len(rows))
	skipped := 0
	for _, row := range rows {
		if membershipConflictsWithManualState(row, manualPairs, nil) {
			skipped++
			continue
		}
		kept = append(kept, row)
	}
	if skipped != 1 {
		t.Fatalf("skipped=%d want=1", skipped)
	}
	if len(kept) != 1 || kept[0].MemberID != "linear:bob@example.com" {
		t.Fatalf("kept=%+v want only bob's row", kept)
	}
}
