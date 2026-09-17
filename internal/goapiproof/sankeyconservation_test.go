package goapiproof

import "testing"

// This file exercises ConservationShape (sankeyconservation.go) through
// hand-authored, self-contained link lists ONLY -- see the type's own
// doc comment: this mechanism (sankeyInvestmentParity's own argMax null-skip relabelling
// argMax null-skip relabelling) has never been observed firing on a real
// production capture, and every real capture checked while building this
// shape (a production deployed-vs-deployed prove run) shows the whole observed divergence
// explained by SankeyRepoFanoutShape's mechanism alone. Proving it
// honestly means an injected fault, not a fabricated capture. Every key
// below is a NEUTRAL synthetic token, never a real ticket id.

func sankeyConservationOptions() Options {
	return Options{
		OrderInsensitiveLists: []OrderInsensitiveList{
			{Path: "data.links", KeyFields: []string{"source", "target"}, Reason: "test fixture", Ticket: "CHAOS-TEST-ORDER"},
		},
		BaselineDefects: []BaselineDefect{{
			Ticket: "CHAOS-TEST-CONSERVE", Reason: "test fixture",
			Paths:        []string{"data.links"},
			Intermittent: true, IntermittentReason: "test fixture",
			ConservationShape: &ConservationShape{
				ListPath:   "data.links",
				ValueField: "value",
				ValuePath:  "data.links.value",
			},
		}},
	}
}

// A relabelling that moves value between two edges while leaving the
// list's own total unchanged -- a work unit's effort re-grouped onto a
// different target, never created or destroyed -- is admitted on both
// affected edges.
func TestConservationShape_TotalConservedRedistributionIsAdmitted(t *testing.T) {
	baseline := snapshotFromJSON(t, sankeyBody("", sankeyLink("theme", "ABC-123", 700)+","+sankeyLink("theme", "Other", 300)))
	candidate := snapshotFromJSON(t, sankeyBody("", sankeyLink("theme", "ABC-123", 900)+","+sankeyLink("theme", "Other", 100)))

	result := Compare(baseline, candidate, sankeyConservationOptions())
	if result.DifferencesOutsideBaselineDefect != 0 {
		t.Fatalf("outside = %d, want 0 -- total is conserved (1000 == 1000), a pure redistribution: findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
	if !equalStrings(result.BaselineDefectsMatched, []string{"CHAOS-TEST-CONSERVE"}) {
		t.Fatalf("matched = %v, want [CHAOS-TEST-CONSERVE]", result.BaselineDefectsMatched)
	}
}

// Blind-spot pin: the same two-edge shape, but the total itself also
// moves (1000 -> 1050) -- effort was added or dropped, not just
// re-grouped, which this mechanism cannot produce. Neither edge is
// admitted: the shape admits a redistribution, never a change in the
// total.
func TestConservationShape_TotalNotConservedStaysUncovered(t *testing.T) {
	baseline := snapshotFromJSON(t, sankeyBody("", sankeyLink("theme", "ABC-123", 700)+","+sankeyLink("theme", "Other", 300)))
	candidate := snapshotFromJSON(t, sankeyBody("", sankeyLink("theme", "ABC-123", 900)+","+sankeyLink("theme", "Other", 150)))

	result := Compare(baseline, candidate, sankeyConservationOptions())
	if result.DifferencesOutsideBaselineDefect != 2 {
		t.Fatalf("outside = %d, want 2 -- total moved (1000 != 1050), so neither edge may be admitted: findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
}

// Rule 1: a mismatch surfacing ANYWHERE outside the declared list (here,
// a sibling field this Options also compares) means something outside
// this mechanism is also in play, and the whole plan admits nothing --
// even the two edges that, on their own, would conserve the total.
func TestConservationShape_DifferenceOutsideListStaysUncovered(t *testing.T) {
	opts := sankeyConservationOptions()
	body := func(coverage float64, links string) string {
		return `{"data":{"nodes":[],"links":[` + links + `],"repo_coverage":` + jsonFloat(coverage) + `}}`
	}
	baseline := snapshotFromJSON(t, body(0.90, sankeyLink("theme", "ABC-123", 700)+","+sankeyLink("theme", "Other", 300)))
	candidate := snapshotFromJSON(t, body(0.80, sankeyLink("theme", "ABC-123", 900)+","+sankeyLink("theme", "Other", 100)))

	result := Compare(baseline, candidate, opts)
	if result.DifferencesOutsideBaselineDefect != 3 {
		t.Fatalf("outside = %d, want 3 -- repo_coverage sits outside data.links, so nothing under the list may be admitted either: findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
}

// Blind-spot pin: a relabelling that moves a work unit onto a
// (source, target) combination that did NOT previously exist on one
// plane surfaces as a structural extra/missing key -- rule 1 still lets
// the whole plan stay valid (the presence finding sits under ListPath),
// but compare.go's leafDifference gate never lets ANY shape admit a
// structural finding. This shape only ever reaches the leaf value
// findings on keys both planes already carry.
func TestConservationShape_NewKeyStructuralFindingNeverAdmitted(t *testing.T) {
	baseline := snapshotFromJSON(t, sankeyBody("", sankeyLink("theme", "ABC-123", 1000)))
	candidate := snapshotFromJSON(t, sankeyBody("", sankeyLink("theme", "ABC-123", 700)+","+sankeyLink("theme", "Other", 300)))

	result := Compare(baseline, candidate, sankeyConservationOptions())
	if result.DifferencesOutsideBaselineDefect != 1 {
		t.Fatalf("outside = %d, want 1 -- the new (theme,Other) key is a structural finding no shape may admit: findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
}
