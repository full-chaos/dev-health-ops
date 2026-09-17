package goapiproof

import "testing"

// This file proves investmentSunburstBaselineDefects' own repos-join
// fan-out entry (restcorpus.go): the KeyedDirectionShape mechanism
// itself is already proven generically by keyeddirection_test.go's own
// 2-key-field (heatmap x/y) fixtures and a real capture; this file
// proves the SAME mechanism fires correctly over the actual 3-field
// composite key (theme, subcategory, scope) this route's own
// OrderInsensitiveList/KeyedDirectionShape pair declares --
// orderInsensitiveKey joins however many KeyFields are given, so a
// 3-field key is not a new code path, but the declaration itself is
// specific to this route and deserves its own proof rather than an
// inference from a 2-field sibling.
//
// NO REAL CAPTURE SHOWS THIS MECHANISM FIRING (see
// investmentSunburstBaselineDefects' own doc comment for the three runs
// checked) -- proven here by injected fault only.

func sunburstBody(rows string) string {
	return `{"data":[` + rows + `]}`
}

func sunburstRow(theme, subcategory, scope string, value float64) string {
	return `{"theme":"` + theme + `","subcategory":"` + subcategory + `","scope":"` + scope + `","value":` + jsonFloat(value) + `}`
}

// A repo row fanned out by an unmerged physical version is admitted at
// its own (theme, subcategory, scope) key, no magnitude bound.
func TestInvestmentSunburstFanout_BaselineGreaterIsAdmitted(t *testing.T) {
	baseline := snapshotFromJSON(t, sunburstBody(sunburstRow("feature_delivery", "feature_delivery.customer", "full-chaos/script-manifest", 54496)))
	candidate := snapshotFromJSON(t, sunburstBody(sunburstRow("feature_delivery", "feature_delivery.customer", "full-chaos/script-manifest", 27248)))

	result := Compare(baseline, candidate, investmentSunburstParity)
	if result.DifferencesOutsideBaselineDefect != 0 {
		t.Fatalf("outside = %d, want 0: findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
	// The fan-out entry is the second-to-last of investmentSunburstBaselineDefects:
	// investmentBaselineDefects' five inherited entries, then the
	// KeyedDirectionShape fan-out entry this test proves, then the
	// LimitDisplacementShape entry (a single row has nothing to displace
	// a limit boundary with, so that one stays idle here, which is
	// expected, not stale).
	fanoutTicket := investmentSunburstBaselineDefects[len(investmentSunburstBaselineDefects)-2].Ticket
	matched := false
	for _, ticket := range result.BaselineDefectsMatched {
		if ticket == fanoutTicket {
			matched = true
		}
	}
	if !matched {
		t.Fatalf("matched = %v, want the fan-out entry's own ticket (%q) present -- idle %v stale %v", result.BaselineDefectsMatched, fanoutTicket, result.IdleIntermittentBaselineDefects, result.StaleBaselineDefects)
	}
}

// Blind-spot pin: a candidate value ABOVE baseline at the same key is
// never admitted -- a Go-side regression that under-counts stays
// uncovered, by design.
func TestInvestmentSunburstFanout_CandidateGreaterIsNotAdmitted(t *testing.T) {
	baseline := snapshotFromJSON(t, sunburstBody(sunburstRow("feature_delivery", "feature_delivery.customer", "full-chaos/script-manifest", 5)))
	candidate := snapshotFromJSON(t, sunburstBody(sunburstRow("feature_delivery", "feature_delivery.customer", "full-chaos/script-manifest", 9)))

	result := Compare(baseline, candidate, investmentSunburstParity)
	if result.DifferencesOutsideBaselineDefect != 1 {
		t.Fatalf("outside = %d, want 1: findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
}

// A genuine repo rename splitting one repos row's contribution across
// two (theme, subcategory, scope) keys is a structural extra/missing key
// (compareListByKey's own "key present on only one side" case), never a
// leaf value difference -- outside every shape in this package, and this
// citation is no exception. Pinned directly rather than inferred.
func TestInvestmentSunburstFanout_RenamedScopeIsStructuralNeverAdmitted(t *testing.T) {
	baseline := snapshotFromJSON(t, sunburstBody(sunburstRow("feature_delivery", "feature_delivery.customer", "full-chaos/old-name", 100)))
	candidate := snapshotFromJSON(t, sunburstBody(sunburstRow("feature_delivery", "feature_delivery.customer", "full-chaos/new-name", 100)))

	result := Compare(baseline, candidate, investmentSunburstParity)
	if result.DifferencesOutsideBaselineDefect != 2 {
		t.Fatalf("outside = %d, want 2 (one key missing from candidate, one extra) -- a renamed scope must never be read as a fan-out: findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
	for _, f := range result.Findings {
		if f.Kind == FindingMismatch && f.Shape != ShapePresence {
			t.Fatalf("finding %+v has shape %q, want presence (structural)", f, f.Shape)
		}
	}
}
