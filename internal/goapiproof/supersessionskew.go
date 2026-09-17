package goapiproof

// SupersessionSkewShape, set on a BaselineDefect, narrows that defect's
// blanket "any leaf difference under Paths is covered" rule to the ONE
// transform the work_unit_supersessions exclusion can actually produce
// on investmentFull's two coverage ratios, instead of admitting any
// value difference under the cited coverage paths.
//
// The mechanism, briefly (full citation lives on the BaselineDefect
// itself, not here): LatestWorkUnitInvestmentsSource unconditionally
// excludes a work unit a later regrouping run retired
// (investmentsupersessions.go), on every reader that composes it,
// including sankeycoverage.go's own untruncated, whole-population
// coverage aggregate. The reference plane has no knowledge of
// work_unit_supersessions at all, so it keeps counting a retired work
// unit under whatever repo/team it carried before being superseded --
// and a unit is typically retired BECAUSE that earlier grouping was
// wrong, so the rows the reference plane keeps and Go excludes skew
// toward unassigned relative to the rest of the population.
//
// That gives this shape a DIRECTION to check, not a MAGNITUDE: a
// regrouping run can retire anywhere from one work unit to a great many,
// so nothing about this mechanism bounds how far it can move either
// ratio. A numeric tolerance would therefore admit an unrelated
// regression of the same size and call it this citation -- the same
// blind spot a bound always has, just relabelled. What the mechanism DOES
// guarantee, unconditionally, is which way it points: the reference
// plane's value can only be pulled DOWN by counting extra
// disproportionately-unassigned rows Go excludes, never up, so a real
// instance always leaves the reference plane's ratio strictly below Go's
// candidate ratio on both leaves. That is the rule this shape checks.
//
// The shape this type verifies, over the two DECODED response bodies:
//
//  1. The ONLY mismatch findings anywhere in the comparison are the two
//     cited coverage leaves. This is a WHOLE-COMPARISON rule, exactly as
//     CoverageShiftShape's own rule 1 (coverageshift.go): a difference
//     surfacing anywhere else -- including sankey.nodes/.edges, which
//     compose the SAME shared dedup source but are top-N truncated, so a
//     genuine supersession skew usually lands on tail nodes that never
//     reach the compared page -- means something outside this mechanism
//     is also in play, and this shape admits nothing at all.
//  2. Both coverage ratios lie in [0,1], and on BOTH leaves the baseline
//     (reference-plane) value is STRICTLY LESS than the candidate (Go)
//     value. Equal values never reach here (Compare would report no
//     difference, hence no Finding to classify); a baseline value at or
//     above the candidate's is the one shape this direction cannot
//     produce, and is therefore never admitted.
//
// What this shape CANNOT catch: a real Go regression that happens to
// move a coverage ratio in the SAME direction (candidate above baseline)
// while touching no other path -- for example a Go-side bug that
// over-counts assigned rows. Rule 1 gives no protection here, since a
// coverage-only regression is exactly what this mechanism also looks
// like from the response bodies alone; only the DIRECTION is checked,
// never disproved. This is a known, accepted limit of what a two-body
// comparison can tell apart, not an oversight.
type SupersessionSkewShape struct {
	// TeamCoveragePath/RepoCoveragePath are the coverage leaf paths --
	// must equal the defect's own Paths entries, the same form
	// CoverageShiftShape's fields use.
	TeamCoveragePath, RepoCoveragePath string
}

// supersessionSkewPlan is one comparison's fully-evaluated admission
// decision, built once per defect (not per finding) from the whole
// comparison's mismatch paths plus the two decoded coverage leaves.
type supersessionSkewPlan struct {
	// valid is a WHOLE-COMPARISON verdict: true only when every rule the
	// type doc comment states holds. false admits NOTHING -- the safe
	// default, same as coverageShiftPlan.valid and repoFanoutPlan.valid.
	// There is no per-id grain here: a coverage ratio is one scalar per
	// leaf, not a list of independently-verifiable elements.
	valid bool
}

// buildSupersessionSkewPlan evaluates every rule SupersessionSkewShape
// documents against one comparison's tiered mismatch paths and its
// decoded baseline/candidate `data` values.
func buildSupersessionSkewPlan(shape *SupersessionSkewShape, baselineData, candidateData any, mismatches []string) *supersessionSkewPlan {
	plan := &supersessionSkewPlan{}

	// Rule 1.
	for _, path := range mismatches {
		if path != shape.TeamCoveragePath && path != shape.RepoCoveragePath {
			return plan
		}
	}

	// Rule 2.
	repoCovBase, okRB := floatAtDottedPath(baselineData, shape.RepoCoveragePath)
	repoCovCand, okRC := floatAtDottedPath(candidateData, shape.RepoCoveragePath)
	teamCovBase, okTB := floatAtDottedPath(baselineData, shape.TeamCoveragePath)
	teamCovCand, okTC := floatAtDottedPath(candidateData, shape.TeamCoveragePath)
	if !okRB || !okRC || !okTB || !okTC {
		return plan
	}
	if !validCoverageRatio(repoCovBase) || !validCoverageRatio(repoCovCand) ||
		!validCoverageRatio(teamCovBase) || !validCoverageRatio(teamCovCand) {
		return plan
	}
	if !(repoCovBase < repoCovCand) || !(teamCovBase < teamCovCand) {
		return plan
	}

	plan.valid = true
	return plan
}

// admits reports whether one Finding is covered by this plan. Same
// whole-comparison verdict for every finding it is asked about as
// coverageShiftPlan.admits -- rule 1 already restricts which findings
// can ever reach here (defectCovers filters to the defect's own Paths
// before this is called).
func (p *supersessionSkewPlan) admits(Finding) bool {
	return p != nil && p.valid
}

// validCoverageRatio reports whether a coverage value is a legal ratio.
func validCoverageRatio(v float64) bool {
	return v >= 0 && v <= 1
}
