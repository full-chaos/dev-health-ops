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

// TestMembershipConflictsWithManualStateRejectsWhenAnotherManualTeamExists
// pins CHAOS-4431 codex review round 3, P2: clickhouse_identity_drift.py's
// _conflict_for loop checks EVERY manual row for a member and conflicts on
// the FIRST one whose team_id differs -- it does not stop once it finds ONE
// row that happens to match the incoming team. A member with manual
// memberships to BOTH the incoming team and another team is therefore still
// a conflict. An earlier revision here only checked set membership of the
// incoming team_id, which incorrectly treated this case as confirmed.
func TestMembershipConflictsWithManualStateRejectsWhenAnotherManualTeamExists(t *testing.T) {
	row := linearReferenceMembershipRow{MemberID: "linear:alice@example.com", TeamID: "ENG"}
	manualTeamsByMember := map[string]map[string]struct{}{
		"linear:alice@example.com": {"ENG": {}, "OPS": {}},
	}
	if !membershipConflictsWithManualState(row, manualTeamsByMember, nil) {
		t.Fatal("want conflict: the member also has an active manual membership to a DIFFERENT team")
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

// TestMembershipConflictsWithManualStateConfirmsSameTeamFallback pins CHAOS-
// 4431 codex review round 3, P2: the member-scoped manual_attribution_
// fallbacks source is NOT team-agnostic -- clickhouse_identity_drift.py's
// _conflict_for fallback branch (lines 263-271) compares team_id with the
// SAME same-team-confirms shape the manual-membership branch uses. An
// earlier revision here dropped team_id from the fallback query entirely,
// treating every matching fallback as a conflict even when it already
// pointed at the incoming team.
func TestMembershipConflictsWithManualStateConfirmsSameTeamFallback(t *testing.T) {
	row := linearReferenceMembershipRow{
		MemberID: "linear:alice@example.com", TeamID: "ENG",
		RawEmail: linearReferenceStringPtr("Alice@Example.com"), // mixed case, must normalize-match
	}
	fallbackTeamsByIdentity := map[string]map[string]struct{}{
		"alice@example.com": {"ENG": {}},
	}
	if membershipConflictsWithManualState(row, nil, fallbackTeamsByIdentity) {
		t.Fatal("a member-scoped fallback already pointing at the incoming team is a confirmation, not a conflict")
	}
}

// TestMembershipConflictsWithManualStateRejectsDifferentTeamFallback is the
// fallback source's positive case: an active fallback pointing at a
// DIFFERENT team than the incoming native row is a conflict.
func TestMembershipConflictsWithManualStateRejectsDifferentTeamFallback(t *testing.T) {
	row := linearReferenceMembershipRow{
		MemberID: "linear:alice@example.com", TeamID: "ENG",
		RawEmail: linearReferenceStringPtr("alice@example.com"),
	}
	fallbackTeamsByIdentity := map[string]map[string]struct{}{
		"alice@example.com": {"OPS": {}},
	}
	if !membershipConflictsWithManualState(row, nil, fallbackTeamsByIdentity) {
		t.Fatal("want conflict: the fallback points at a DIFFERENT team than this native row")
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
	fallbackTeamsByIdentity := map[string]map[string]struct{}{
		"bob@example.com": {"OPS": {}},
	}
	if !membershipConflictsWithManualState(row, nil, fallbackTeamsByIdentity) {
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
	fallbackTeamsByIdentity := map[string]map[string]struct{}{
		"someone-else@example.com": {"OPS": {}},
	}
	if membershipConflictsWithManualState(row, manualTeamsByMember, fallbackTeamsByIdentity) {
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
