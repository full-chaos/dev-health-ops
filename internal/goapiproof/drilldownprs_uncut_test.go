package goapiproof

import (
	"fmt"
	"testing"
)

// This file exercises drilldownPRsTeamScopeUncutDefect (CHAOS-5988,
// restcorpus.go) and drilldownPRsTeamScopedParityWithLimit: the request-
// shape strategies (an explicit high limit, a short window) that keep a
// team's own true population strictly under the request's own effective
// limit, so DuplicateCollapseLengthShape's exact-collapse rule content-
// verifies every field of every row instead of CHAOS-5968's own page-cut
// admission.

// drilldownPRsUncutTicket reads the ticket of
// drilldownPRsTeamScopedParity's own drilldownPRsTeamScopeUncutDefect
// entry, rather than a literal copy that could drift out of sync with
// restcorpus.go.
func drilldownPRsUncutTicket(t *testing.T) string {
	t.Helper()
	for _, d := range drilldownPRsTeamScopedParity.BaselineDefects {
		if d.Ticket == "CHAOS-5988" {
			return d.Ticket
		}
	}
	t.Fatal("drilldownPRsTeamScopedParity declares no CHAOS-5988 entry")
	return ""
}

// TestDrilldownPRsTeamScopedParityWithLimit_RealCaptureAdmitsUnderCHAOS5988
// runs the real captured exact-collapse pair (drilldown/prs team_scoped,
// 42 baseline items collapsing to 36 candidate items -- the same real
// capture TestDuplicateCollapseLengthShape_RealTeamScopedCaptureCollapses
// Exactly already pins against CHAOS-5959) through
// drilldownPRsTeamScopedParityWithLimit(500): a DERIVED use of that same
// real data, standing in for a request whose own effective limit is 500
// instead of the route's own default -- 42 is comfortably under either,
// so this is a genuine, if reused, positive case for the request-shape
// strategy CHAOS-5988 declares, not a fixture engineered for this test.
func TestDrilldownPRsTeamScopedParityWithLimit_RealCaptureAdmitsUnderCHAOS5988(t *testing.T) {
	baseline := drilldownPRsSnapshotFromFile(t, "testdata/drilldownprs_lengthcollapse_baseline_a5c794d5.json")
	candidate := drilldownPRsSnapshotFromFile(t, "testdata/drilldownprs_lengthcollapse_candidate_fa4ebe78.json")

	result := Compare(baseline, candidate, drilldownPRsTeamScopedParityWithLimit(500))
	if result.TerminalState != TerminalStateMismatch {
		t.Fatalf("terminal = %q, want mismatch", result.TerminalState)
	}
	if result.DifferencesOutsideBaselineDefect != 0 {
		t.Fatalf("outside = %d, want 0 -- findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
	wantTicket := drilldownPRsUncutTicket(t)
	foundMatched := false
	for _, ticket := range result.BaselineDefectsMatched {
		if ticket == wantTicket {
			foundMatched = true
		}
	}
	if !foundMatched {
		t.Fatalf("matched = %v, want %s among them: idle %v stale %v", result.BaselineDefectsMatched, wantTicket, result.IdleIntermittentBaselineDefects, result.StaleBaselineDefects)
	}
}

// drilldownPRsUncutLimitFixture builds a synthetic team-scoped
// drilldown/prs body with n distinct ids in created_at DESC order, every
// one duplicated exactly once on the baseline side (byte-identical
// copies), and an exact collapse on the candidate side.
func drilldownPRsUncutLimitFixture(t *testing.T, n int) (baseline, candidate Snapshot) {
	t.Helper()
	var baseItems, candItems []string
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("id-%02d", i)
		baseItems = append(baseItems, pageCutItem(id, "x", i), pageCutItem(id, "x", i))
		candItems = append(candItems, pageCutItem(id, "x", i))
	}
	base, err := DecodeRESTSnapshot([]byte(dedupCollapseBody(baseItems)))
	if err != nil {
		t.Fatal(err)
	}
	cand, err := DecodeRESTSnapshot([]byte(dedupCollapseBody(candItems)))
	if err != nil {
		t.Fatal(err)
	}
	return base, cand
}

// TestDrilldownPRsTeamScopedParity_LimitReachedGoesIdleNotMatched
// pins the exact behavior team-lead's own ruling requires: a synthetic
// baseline whose own raw length (50) reaches drilldownPRsTeamScopedParity's
// own default RequestLimit (50, CHAOS-5988's own baseline configuration)
// exactly, with an otherwise-clean exact collapse (25 distinct ids, each
// duplicated once). CHAOS-5988 must go IDLE (not matched) -- "page
// possibly cut". The list's own length finding is still covered: a
// baseline at the limit with every duplicate collapsed onto the
// candidate's own prefix is exactly the page cut the page-cut entry
// names, and the exact-collapse entry admits the same structure on its
// own terms; neither is a fallback for the uncut entry's own refusal.
func TestDrilldownPRsTeamScopedParity_LimitReachedGoesIdleNotMatched(t *testing.T) {
	baseline, candidate := drilldownPRsUncutLimitFixture(t, 25)

	result := Compare(baseline, candidate, drilldownPRsTeamScopedParity)
	wantTicket := drilldownPRsUncutTicket(t)
	for _, ticket := range result.BaselineDefectsMatched {
		if ticket == wantTicket {
			t.Fatalf("matched = %v, must not include %s: baseline's own raw length (50) reaches the request's own effective limit (50), an unverifiable page boundary", result.BaselineDefectsMatched, wantTicket)
		}
	}
	foundIdle := false
	for _, ticket := range result.IdleIntermittentBaselineDefects {
		if ticket == wantTicket {
			foundIdle = true
		}
	}
	if !foundIdle {
		t.Fatalf("idle = %v, want %s among them -- a limit-reaching baseline goes idle by name, not silently unmentioned", result.IdleIntermittentBaselineDefects, wantTicket)
	}
	if result.DifferencesOutsideBaselineDefect != 0 {
		t.Fatalf("outside = %d, want 0: the length finding is a page cut at the limit, which the page-cut entry covers -- findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
}

// TestDrilldownPRsTeamScopedParity_LimitJustBelowAdmitsUnderCHAOS5988 is
// the ceiling-1 cell's own end-to-end counterpart: baseline's own raw
// length (48, from 24 duplicated ids) sits strictly below the request's
// own effective limit (50) -- CHAOS-5988 must match.
func TestDrilldownPRsTeamScopedParity_LimitJustBelowAdmitsUnderCHAOS5988(t *testing.T) {
	baseline, candidate := drilldownPRsUncutLimitFixture(t, 24)

	result := Compare(baseline, candidate, drilldownPRsTeamScopedParity)
	if result.DifferencesOutsideBaselineDefect != 0 {
		t.Fatalf("outside = %d, want 0 -- findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
	wantTicket := drilldownPRsUncutTicket(t)
	foundMatched := false
	for _, ticket := range result.BaselineDefectsMatched {
		if ticket == wantTicket {
			foundMatched = true
		}
	}
	if !foundMatched {
		t.Fatalf("matched = %v, want %s among them: idle %v", result.BaselineDefectsMatched, wantTicket, result.IdleIntermittentBaselineDefects)
	}
}

// TestDrilldownPRsTeamScopedParityWithLimit_OnlySetsRequestLimitOnCHAOS5988
// pins the ticket guard in drilldownPRsTeamScopedParityWithLimit's own
// wiring loop: CHAOS-5959 (this table's own pre-existing
// DuplicateCollapseLengthShape entry, no RequestLimit) and CHAOS-5988
// (this ticket's own entry) share the SAME shape TYPE, so the loop must
// disambiguate by ticket, not merely by type, or CHAOS-5959 would gain a
// RequestLimit this design deliberately leaves it without.
func TestDrilldownPRsTeamScopedParityWithLimit_OnlySetsRequestLimitOnCHAOS5988(t *testing.T) {
	opts := drilldownPRsTeamScopedParityWithLimit(500)
	found5959, found5988 := false, false
	for _, defect := range opts.BaselineDefects {
		if defect.DuplicateCollapseLengthShape == nil {
			continue
		}
		switch defect.Ticket {
		case "CHAOS-5959":
			found5959 = true
			if defect.DuplicateCollapseLengthShape.RequestLimit != 0 {
				t.Fatalf("CHAOS-5959's own RequestLimit = %d, want 0 (untouched)", defect.DuplicateCollapseLengthShape.RequestLimit)
			}
		case "CHAOS-5988":
			found5988 = true
			if defect.DuplicateCollapseLengthShape.RequestLimit != 500 {
				t.Fatalf("CHAOS-5988's own RequestLimit = %d, want 500", defect.DuplicateCollapseLengthShape.RequestLimit)
			}
		}
	}
	if !found5959 || !found5988 {
		t.Fatalf("found5959=%v found5988=%v, want both present", found5959, found5988)
	}
}
