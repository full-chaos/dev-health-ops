package goapiproof

import "testing"

// This file exercises DictKeyDirectionShape (dictkeydirection.go)
// directly through small, self-contained JSON objects -- the per-key
// direction admission for the work_unit_supersessions exclusion over a
// Go map field (theme_distribution, subcategory_distribution,
// evidence_quality_distribution, evidence_quality_stats.band_counts,
// unassigned_reasons), where KeyedDirectionShape cannot apply because
// those fields are JSON OBJECTS, not order-insensitive-paired arrays.
//
// NO REAL CAPTURE SHOWS THIS MECHANISM FIRING: every one of the three
// available production deployed-vs-deployed prove runs (_records/
// deploy-71/r78/step73, deploy-70/r77/step72, deploy-69/r76/step71) shows
// GET/POST /api/v1/investment and investment/flow(-repo-team) as a clean
// match (or a divergence already explained by something else -- step73's
// evidence_quality_stats.mean rounding is FloatTierB, not this shape) on
// every admissible request -- the exclusion has not yet been
// observed live on the fields this shape covers. Proven here only by a
// deliberately injected fixture fault, exactly as sankeyInvestmentParity's
// own argMax-null-skip/ConservationShape entry already is (restcorpus.go).

func themeDistributionOptions(ticket string) Options {
	return Options{
		BaselineDefects: []BaselineDefect{{
			Ticket: ticket, Reason: "test fixture",
			Paths:        []string{"data.theme_distribution"},
			Intermittent: true, IntermittentReason: "test fixture",
			DictKeyDirectionShape: &DictKeyDirectionShape{
				DictPath:  "data.theme_distribution",
				ValuePath: "data.theme_distribution",
			},
		}},
	}
}

func themeDistBody(pairs string) string {
	return `{"data":{"theme_distribution":{` + pairs + `}}}`
}

func themePair(key string, value float64) string {
	return `"` + key + `":` + jsonFloat(value)
}

// The mechanism's own direction -- baseline greater than or equal to
// candidate, a strictly subtractive exclusion can only drop rows, never
// add them -- is admitted with no magnitude bound, mirroring
// KeyedDirectionShape's own direction test.
func TestDictKeyDirectionShape_BaselineGreaterIsAdmitted(t *testing.T) {
	baseline := snapshotFromJSON(t, themeDistBody(themePair("frontend", 1000000)))
	candidate := snapshotFromJSON(t, themeDistBody(themePair("frontend", 1)))

	result := Compare(baseline, candidate, themeDistributionOptions("CHAOS-TEST-DICTDIR"))
	if result.DifferencesOutsideBaselineDefect != 0 {
		t.Fatalf("outside = %d, want 0 -- a large shift is explained the same as a small one: findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
	if !equalStrings(result.BaselineDefectsMatched, []string{"CHAOS-TEST-DICTDIR"}) {
		t.Fatalf("matched = %v, want [CHAOS-TEST-DICTDIR]", result.BaselineDefectsMatched)
	}
}

// Blind-spot pin: a candidate value ABOVE baseline at the same key is
// the one shape this direction cannot produce -- a Go-side regression
// that over-counts stays uncovered, by design, exactly as
// KeyedDirectionShape's own analogous test documents.
func TestDictKeyDirectionShape_CandidateGreaterIsNotAdmitted(t *testing.T) {
	baseline := snapshotFromJSON(t, themeDistBody(themePair("frontend", 5)))
	candidate := snapshotFromJSON(t, themeDistBody(themePair("frontend", 9)))

	result := Compare(baseline, candidate, themeDistributionOptions("CHAOS-TEST-DICTDIR"))
	if result.DifferencesOutsideBaselineDefect != 1 {
		t.Fatalf("outside = %d, want 1 -- a candidate-greater shift must never be admitted by a direction-only shape: findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
}

// PINNED, not inherited: a key present in the candidate map but ABSENT
// from the baseline's contradicts the shape's own subset claim (Go's
// strict subset cannot contain a key the superset lacks) and must never
// be admitted. This is enforced one layer up today (compareDict reports
// it as ShapePresence, and compare.go's classifyBaselineDefects never
// asks any shape's admits() about a non-leaf finding) -- this test
// asserts the OUTCOME directly, as its own rule, rather than trusting
// that upstream routing to keep holding.
func TestDictKeyDirectionShape_CandidateOnlyKeyNeverAdmitted(t *testing.T) {
	baseline := snapshotFromJSON(t, themeDistBody(themePair("frontend", 100)))
	candidate := snapshotFromJSON(t, themeDistBody(themePair("frontend", 100)+","+themePair("backend", 50)))

	result := Compare(baseline, candidate, themeDistributionOptions("CHAOS-TEST-DICTDIR"))
	if result.DifferencesOutsideBaselineDefect != 1 {
		t.Fatalf("outside = %d, want 1 -- a candidate-only key must surface as an ordinary uncovered finding, never an admission: findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
	if len(result.BaselineDefectsMatched) != 0 {
		t.Fatalf("matched = %v, want none -- the shape's own citation must not read a structural candidate-only key as covering anything", result.BaselineDefectsMatched)
	}
	found := false
	for _, f := range result.Findings {
		if f.Kind == FindingMismatch && f.Shape == ShapePresence {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected a presence finding for the candidate-only key, got %+v", result.Findings)
	}
}

// Multiple keys, only some of which satisfy the direction claim: each
// key's own admission is independent, exactly as DictKeyDirectionShape's
// own doc comment claims (a per-key verdict, not a whole-object one).
func TestDictKeyDirectionShape_PerKeyIndependentAdmission(t *testing.T) {
	baseline := snapshotFromJSON(t, themeDistBody(themePair("frontend", 200)+","+themePair("backend", 50)))
	candidate := snapshotFromJSON(t, themeDistBody(themePair("frontend", 100)+","+themePair("backend", 90)))

	result := Compare(baseline, candidate, themeDistributionOptions("CHAOS-TEST-DICTDIR"))
	// frontend: baseline(200) > candidate(100) -- admitted.
	// backend: baseline(50) < candidate(90) -- NOT admitted (wrong direction).
	if result.DifferencesOutsideBaselineDefect != 1 {
		t.Fatalf("outside = %d, want 1 (backend stays uncovered, frontend is admitted): findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
}
