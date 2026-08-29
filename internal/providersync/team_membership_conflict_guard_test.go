package providersync

import "testing"

// TestMembershipConflictsWithManualStateConfirmsExactManualPair pins the
// CHAOS-4431 codex review round 2 correction (P1): a native membership row
// whose EXACT (member_id, team_id) matches an active manual membership is a
// CONFIRMATION, not a conflict -- mirroring clickhouse_identity_drift.py's
// _conflict_for (line 259: manual.team_id == team_id -> continue, not
// flagged). Writing the native row alongside a manual pin to the SAME team
// must proceed normally.
func TestMembershipConflictsWithManualStateConfirmsExactManualPair(t *testing.T) {
	row := linearReferenceMembershipRow{MemberID: "linear:alice@example.com", TeamID: "ENG"}
	manualTeamsByMember := map[string]map[string]struct{}{
		"linear:alice@example.com": {"ENG": {}},
	}
	if membershipConflictsWithManualState(row, manualTeamsByMember, nil) {
		t.Fatal("a manual membership to the SAME team is a confirmation, not a conflict")
	}
}

// TestMembershipConflictsWithManualStateRejectsDifferentTeamManualPin is the
// corrected positive case: a member with an active manual membership to team
// A, but Linear now reports them on a DIFFERENT team B -- the native row for
// B must be rejected, since writing it would contradict the manual pin.
func TestMembershipConflictsWithManualStateRejectsDifferentTeamManualPin(t *testing.T) {
	row := linearReferenceMembershipRow{MemberID: "linear:alice@example.com", TeamID: "OPS"}
	manualTeamsByMember := map[string]map[string]struct{}{
		"linear:alice@example.com": {"ENG": {}},
	}
	if !membershipConflictsWithManualState(row, manualTeamsByMember, nil) {
		t.Fatal("want conflict: member has an active manual pin to a DIFFERENT team than this native row")
	}
}

// TestMembershipConflictsWithManualStateAllowsMemberWithNoManualMembership
// pins the negative case for the manual-membership source specifically: a
// member with NO active manual membership at all is never flagged by this
// source, regardless of which team the native row targets.
func TestMembershipConflictsWithManualStateAllowsMemberWithNoManualMembership(t *testing.T) {
	row := linearReferenceMembershipRow{MemberID: "linear:dave@example.com", TeamID: "ENG"}
	manualTeamsByMember := map[string]map[string]struct{}{
		"linear:alice@example.com": {"ENG": {}},
	}
	if membershipConflictsWithManualState(row, manualTeamsByMember, nil) {
		t.Fatal("a member with no manual membership at all must not be flagged")
	}
}

// TestMembershipConflictsWithManualStateSkipsMemberScopedFallback pins the
// guard's second source: an active manual_attribution_fallbacks(scope_type=
// 'member') row matching the member's identity (by provider id, email, or
// any resolved facet) blocks the write REGARDLESS of team, mirroring
// clickhouse_identity_drift.py's own fallback check (team-agnostic). This
// source is unaffected by the round-2 correction -- it was already
// team-agnostic.
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
// no manual membership conflict, no matching fallback -- a genuinely clean
// native row must never be skipped.
func TestMembershipConflictsWithManualStateAllowsCleanRow(t *testing.T) {
	row := linearReferenceMembershipRow{
		MemberID: "linear:carol@example.com", TeamID: "ENG",
		RawEmail:       linearReferenceStringPtr("carol@example.com"),
		IdentityFacets: []string{"linear:carol@example.com", "carol@example.com"},
	}
	manualTeamsByMember := map[string]map[string]struct{}{
		"linear:alice@example.com": {"ENG": {}},
	}
	fallbacks := map[string]struct{}{"someone-else@example.com": {}}
	if membershipConflictsWithManualState(row, manualTeamsByMember, fallbacks) {
		t.Fatal("a clean row with no matching manual conflict or fallback must not be skipped")
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
		// alice: manual pin to a DIFFERENT team (OPS) -> conflict, skip.
		{MemberID: "linear:alice@example.com", TeamID: "ENG"},
		// bob: no manual membership at all -> kept.
		{MemberID: "linear:bob@example.com", TeamID: "ENG"},
	}
	manualTeamsByMember := map[string]map[string]struct{}{
		"linear:alice@example.com": {"OPS": {}},
	}
	kept := make([]linearReferenceMembershipRow, 0, len(rows))
	skipped := 0
	for _, row := range rows {
		if membershipConflictsWithManualState(row, manualTeamsByMember, nil) {
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
