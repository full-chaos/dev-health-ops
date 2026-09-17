package goapiproof

import "testing"

// This file exercises ScalarDirectionShape (scalardirection.go) directly
// through small, self-contained response bodies -- the direction-only
// admission for a single named scalar leaf, with an OPTIONAL
// whole-comparison gate (ContestedPaths) that stays unset in the normal
// case (see the type's own doc comment for when it is needed).
//
// NO REAL CAPTURE SHOWS THIS MECHANISM FIRING: every one of the three
// available production deployed-vs-deployed prove runs shows
// investment/flow(-repo-team) and GET/POST /api/v1/investment's own
// team_coverage/repo_coverage/distinct_team_targets/distinct_repo_targets/
// evidence_quality_stats.total fields as a clean match on every
// admissible request. Proven here only by a deliberately injected
// fixture fault, exactly as sankeyInvestmentParity's own argMax-null-skip/
// ConservationShape entry already is (restcorpus.go).

func scalarDirectionOptions(baselineMustBeGreater bool, contested []string) Options {
	return Options{
		BaselineDefects: []BaselineDefect{{
			Ticket: "CHAOS-TEST-SCALARDIR", Reason: "test fixture",
			Paths:        []string{"data.repo_coverage", "data.links"},
			Intermittent: true, IntermittentReason: "test fixture",
			ScalarDirectionShape: &ScalarDirectionShape{
				Path:                  "data.repo_coverage",
				BaselineMustBeGreater: baselineMustBeGreater,
				ContestedPaths:        contested,
			},
		}},
	}
}

func scalarDirBody(repoCoverage float64, links string) string {
	return `{"data":{"repo_coverage":` + jsonFloat(repoCoverage) + `,"links":[` + links + `]}}`
}

// BaselineMustBeGreater == true admits only baseline > candidate --
// modelling the structural count/sum-over-a-strict-subset claim
// (distinct_team_targets/distinct_repo_targets, evidence_quality_stats.
// total).
func TestScalarDirectionShape_BaselineGreaterAdmittedWhenDirectionUp(t *testing.T) {
	baseline := snapshotFromJSON(t, scalarDirBody(0.90, ""))
	candidate := snapshotFromJSON(t, scalarDirBody(0.70, ""))

	result := Compare(baseline, candidate, scalarDirectionOptions(true, nil))
	if result.DifferencesOutsideBaselineDefect != 0 {
		t.Fatalf("outside = %d, want 0: findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
	if !equalStrings(result.BaselineDefectsMatched, []string{"CHAOS-TEST-SCALARDIR"}) {
		t.Fatalf("matched = %v, want [CHAOS-TEST-SCALARDIR]", result.BaselineDefectsMatched)
	}
}

// BaselineMustBeGreater == false admits only baseline < candidate --
// modelling the coverage-ratio skew claim (team_coverage/repo_coverage:
// retired units typically skew toward unassigned, pulling the reference
// plane's ratio DOWN).
func TestScalarDirectionShape_BaselineLessAdmittedWhenDirectionDown(t *testing.T) {
	baseline := snapshotFromJSON(t, scalarDirBody(0.70, ""))
	candidate := snapshotFromJSON(t, scalarDirBody(0.90, ""))

	result := Compare(baseline, candidate, scalarDirectionOptions(false, nil))
	if result.DifferencesOutsideBaselineDefect != 0 {
		t.Fatalf("outside = %d, want 0: findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
	if !equalStrings(result.BaselineDefectsMatched, []string{"CHAOS-TEST-SCALARDIR"}) {
		t.Fatalf("matched = %v, want [CHAOS-TEST-SCALARDIR]", result.BaselineDefectsMatched)
	}
}

// Blind-spot pin: a value moving on the WRONG side of candidate for the
// declared direction is never admitted -- only the sign is checked,
// never disproved, the same known limit every other direction shape in
// this package documents.
func TestScalarDirectionShape_WrongDirectionNeverAdmitted(t *testing.T) {
	baseline := snapshotFromJSON(t, scalarDirBody(0.60, ""))
	candidate := snapshotFromJSON(t, scalarDirBody(0.90, ""))

	// Declared BaselineMustBeGreater == true, but baseline is LESS here.
	result := Compare(baseline, candidate, scalarDirectionOptions(true, nil))
	if result.DifferencesOutsideBaselineDefect != 1 {
		t.Fatalf("outside = %d, want 1 -- a wrong-direction value must never be admitted: findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
}

// With NO gate declared (ContestedPaths nil, the normal case per the
// type's own doc comment), an unrelated sibling finding elsewhere in the
// SAME comparison must NOT block admission of this shape's own scalar --
// the whole point of the escalated ruling: the gate is per-declaration
// scoping for a genuine competing citation, never this shape's default.
func TestScalarDirectionShape_NoGateAdmitsDespiteSiblingFinding(t *testing.T) {
	baseline := snapshotFromJSON(t, scalarDirBody(0.90, sankeyLink("a", "b", 200)))
	candidate := snapshotFromJSON(t, scalarDirBody(0.70, sankeyLink("a", "b", 100)))

	result := Compare(baseline, candidate, scalarDirectionOptions(true, nil))
	// repo_coverage is admitted; the links[0].value finding stays outside
	// this defect entirely (no shape declared for it in this fixture).
	if result.DifferencesOutsideBaselineDefect != 1 {
		t.Fatalf("outside = %d, want 1 (only the sibling link finding, not repo_coverage): findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
	admittedRepoCoverage := false
	for i, f := range result.Findings {
		if f.Path == "$.data.repo_coverage" {
			_ = i
			admittedRepoCoverage = true
		}
	}
	if !admittedRepoCoverage {
		t.Fatalf("expected a repo_coverage finding to exist and be admitted, findings %+v", result.Findings)
	}
	if len(result.BaselineDefectsMatched) != 1 {
		t.Fatalf("matched = %v, want exactly one hit (repo_coverage admitted despite the sibling link finding)", result.BaselineDefectsMatched)
	}
}

// With a gate declared (ContestedPaths set), the SAME sibling finding
// that rule 1's absence tolerated above now blocks admission entirely --
// modelling the genuine-collision case (CoverageShiftShape/
// SupersessionSkewShape's own precedent) this field exists for.
func TestScalarDirectionShape_GateBlocksWhenSiblingFindingExists(t *testing.T) {
	baseline := snapshotFromJSON(t, scalarDirBody(0.90, sankeyLink("a", "b", 200)))
	candidate := snapshotFromJSON(t, scalarDirBody(0.70, sankeyLink("a", "b", 100)))

	result := Compare(baseline, candidate, scalarDirectionOptions(true, []string{"data.repo_coverage"}))
	if result.DifferencesOutsideBaselineDefect != 2 {
		t.Fatalf("outside = %d, want 2 -- the gate must decline BOTH findings when anything outside ContestedPaths also differs: findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
	if len(result.BaselineDefectsMatched) != 0 {
		t.Fatalf("matched = %v, want none", result.BaselineDefectsMatched)
	}
}

// With the SAME gate declared, a comparison whose ONLY mismatch is the
// gated scalar itself still admits -- the gate restricts, it does not
// disable, the shape's own claim.
func TestScalarDirectionShape_GateAdmitsWhenOnlyGatedPathDiffers(t *testing.T) {
	baseline := snapshotFromJSON(t, scalarDirBody(0.90, sankeyLink("a", "b", 100)))
	candidate := snapshotFromJSON(t, scalarDirBody(0.70, sankeyLink("a", "b", 100)))

	result := Compare(baseline, candidate, scalarDirectionOptions(true, []string{"data.repo_coverage"}))
	if result.DifferencesOutsideBaselineDefect != 0 {
		t.Fatalf("outside = %d, want 0: findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
	if !equalStrings(result.BaselineDefectsMatched, []string{"CHAOS-TEST-SCALARDIR"}) {
		t.Fatalf("matched = %v, want [CHAOS-TEST-SCALARDIR]", result.BaselineDefectsMatched)
	}
}
