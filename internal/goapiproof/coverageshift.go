package goapiproof

import "math"

// CoverageShiftShape, set on a BaselineDefect, narrows that defect's
// blanket "any leaf difference under Paths is covered" rule to the ONE
// transform the argMax null-transition mechanism (investment.go's
// LatestWorkUnitInvestmentsSource, buildUnitTeamSubquery) can actually
// produce on investmentFull's two coverage ratios, instead of admitting
// any value difference under the cited coverage paths.
//
// Why the blanket form was not enough, measured: a blanket citation on
// data.analytics.sankey.coverage.teamCoverage/.repoCoverage also matches
// the repos-join fan-out's own citation (CHAOS-4773, RepoFanoutShape,
// repofanout.go) -- both defects cite the SAME two leaves, so a fanned-out
// baseline (sankey nodes and edges multiplied by a duplicated repos row's
// version count, which also moves coverage) reported BOTH tickets as
// covering the one coverage difference, and TestInvestmentFullRepoJoinFanout_FannedOutBaselineIsACoveredMismatch
// caught it (matched = [CHAOS-4547 CHAOS-4773], want [CHAOS-4773]). Worse,
// the blanket form admits ANY coverage difference at all, including a
// real Go regression with nothing to do with an argMax null-transition
// (TestCoverageShiftShape_ShiftBeyondBoundIsNotAdmitted's 20% shift).
//
// The shape this type verifies, over the two DECODED response bodies:
//
//  1. The ONLY mismatch findings anywhere in the comparison are the two
//     cited coverage leaves. This is a WHOLE-COMPARISON rule, not a
//     per-finding one -- unlike RepoFanoutShape, which can admit a subset
//     of nodes/edges while leaving an unrelated one outside, this
//     mechanism (a single work unit's newest generation clearing one
//     Nullable attribution column, investment.go's own CHAOS-4547 doc
//     comment) never touches anything outside the coverage query's own
//     read, so a real instance never coexists with a difference
//     anywhere else in the response -- including the repos-join fan-out,
//     which always also moves at least one sankey node or edge.
//  2. sankey.nodes and sankey.edges carry byte-identical decoded values
//     on both sides. Checked directly here, not merely inferred from
//     rule 1's absence of a Finding: nodes/edges values are declared
//     FloatTierB (CHAOS-5451) for this operation, so a difference small
//     enough to sit inside that merged-aggregate tolerance produces no
//     Finding at all, and this shape's own bound (rule 3) is tighter
//     than that tolerance is meant to excuse -- rule 1 alone cannot see
//     a sub-tolerance node/edge drift, so rule 2 reads the raw values
//     independently.
//  3. Both coverage ratios lie in [0,1] and differ from each other by no
//     more than coverageShiftMaxRelativeDelta. A single work unit
//     transitioning moves one row between assigned and unassigned out of
//     an org's whole population, so a real instance shifts either ratio
//     by a small amount: the production instance this shape was built
//     for measured relative shifts of ~0.2% (repoCoverage 0.8924689846789948
//     vs 0.8906755621478929) and ~0.3% (teamCoverage 0.8527558256115781
//     vs 0.8503000595320582). coverageShiftMaxRelativeDelta gives that
//     roughly 15-20x headroom while staying far under a bulk-scale
//     defect (a repos-join fan-out moves coverage by tens of percent, as
//     the k=2 fixture in investmentfull_repojoin_fanout_test.go shows).
//
// Anything outside these three rules is NOT covered: classifyBaselineDefects
// reports it as an ordinary difference outside the citation, exactly like
// any other uncited mismatch.
type CoverageShiftShape struct {
	// NodesListPath/EdgesListPath are the dotted, index-free paths to the
	// sankey nodes/edges LISTS themselves (no trailing ".value"), the
	// same form RepoFanoutShape's own fields use, e.g.
	// "data.analytics.sankey.nodes".
	NodesListPath, EdgesListPath string
	// TeamCoveragePath/RepoCoveragePath are the coverage leaf paths --
	// must equal the defect's own Paths entries.
	TeamCoveragePath, RepoCoveragePath string
}

// coverageShiftMaxRelativeDelta is rule 3's bound -- see the type doc
// comment for the measured instance it was sized against.
const coverageShiftMaxRelativeDelta = 0.05

// coverageShiftPlan is one comparison's fully-evaluated admission
// decision, built once per defect (not per finding) from the whole
// comparison's mismatch paths plus the two decoded `sankey` subtrees.
type coverageShiftPlan struct {
	// valid is a WHOLE-COMPARISON verdict: true only when every one of
	// the three rules the type doc comment states holds. false admits
	// NOTHING -- the safe default, same as repoFanoutPlan.valid.
	valid bool
}

// buildCoverageShiftPlan evaluates every rule CoverageShiftShape
// documents against one comparison's tiered mismatch paths and its
// decoded baseline/candidate `data` values.
func buildCoverageShiftPlan(shape *CoverageShiftShape, baselineData, candidateData any, mismatches []string) *coverageShiftPlan {
	plan := &coverageShiftPlan{}

	// Rule 1.
	for _, path := range mismatches {
		if path != shape.TeamCoveragePath && path != shape.RepoCoveragePath {
			return plan
		}
	}

	// Rule 2.
	baseNodes, ok1 := sankeyNodeValues(baselineData, shape.NodesListPath)
	candNodes, ok2 := sankeyNodeValues(candidateData, shape.NodesListPath)
	baseEdges, ok3 := sankeyEdgeInfoMap(baselineData, shape.EdgesListPath)
	candEdges, ok4 := sankeyEdgeInfoMap(candidateData, shape.EdgesListPath)
	if !ok1 || !ok2 || !ok3 || !ok4 {
		return plan
	}
	if !nodeValuesExactlyEqual(baseNodes, candNodes) || !edgeValuesExactlyEqual(baseEdges, candEdges) {
		return plan
	}

	// Rule 3.
	repoCovBase, okRB := floatAtDottedPath(baselineData, shape.RepoCoveragePath)
	repoCovCand, okRC := floatAtDottedPath(candidateData, shape.RepoCoveragePath)
	teamCovBase, okTB := floatAtDottedPath(baselineData, shape.TeamCoveragePath)
	teamCovCand, okTC := floatAtDottedPath(candidateData, shape.TeamCoveragePath)
	if !okRB || !okRC || !okTB || !okTC {
		return plan
	}
	if !coverageShiftWithinBound(repoCovBase, repoCovCand) || !coverageShiftWithinBound(teamCovBase, teamCovCand) {
		return plan
	}

	plan.valid = true
	return plan
}

// admits reports whether one Finding is covered by this plan. Unlike
// repoFanoutPlan.admits, this shape's admission is the SAME
// whole-comparison verdict for every finding it is asked about -- rule 1
// already restricts which findings can ever reach here (defectCovers
// filters to the defect's own Paths before this is called).
func (p *coverageShiftPlan) admits(Finding) bool {
	return p != nil && p.valid
}

// coverageShiftWithinBound reports whether two legal ([0,1]) coverage
// ratios differ by no more than coverageShiftMaxRelativeDelta, relative
// to the larger of the two (max(abs, abs) mirrors compareNumber's own
// tolerance shape).
func coverageShiftWithinBound(base, candidate float64) bool {
	if base < 0 || base > 1 || candidate < 0 || candidate > 1 {
		return false
	}
	denominator := math.Max(math.Abs(base), math.Abs(candidate))
	if denominator == 0 {
		return true
	}
	return math.Abs(base-candidate)/denominator <= coverageShiftMaxRelativeDelta
}

// nodeValuesExactlyEqual reports whether two id->value maps (as decoded
// by sankeyNodeValues) carry the same ids and the same values. Keyed by
// id rather than position, so this is unaffected by CHAOS-5546's
// nodes/edges ordering nondeterminism the same way sankeyNodeValues
// itself is.
func nodeValuesExactlyEqual(a, b map[string]float64) bool {
	if len(a) != len(b) {
		return false
	}
	for id, value := range a {
		other, ok := b[id]
		if !ok || value != other {
			return false
		}
	}
	return true
}

// edgeValuesExactlyEqual is nodeValuesExactlyEqual's edge-side twin, over
// the source\x1ftarget-keyed maps sankeyEdgeInfoMap decodes.
func edgeValuesExactlyEqual(a, b map[string]repoFanoutEdge) bool {
	if len(a) != len(b) {
		return false
	}
	for key, edge := range a {
		other, ok := b[key]
		if !ok || edge.value != other.value {
			return false
		}
	}
	return true
}
