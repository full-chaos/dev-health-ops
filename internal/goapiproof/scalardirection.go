package goapiproof

import "math"

// ScalarDirectionShape, set on a BaselineDefect, narrows that defect's
// blanket "any leaf difference under Paths is covered" rule to a
// DIRECTION-ONLY claim on ONE named scalar leaf: baseline is admitted
// only when it lies on the declared side of candidate (strictly greater,
// or strictly less -- see BaselineMustBeGreater), never by magnitude.
//
// Two DIFFERENT mechanism classes both need exactly this claim, on
// different fields of the SAME work_unit_supersessions-exclusion citation
// (investmentFlowRepoDedupParity, GET/POST /api/v1/investment's own
// investmentBaselineDefects):
//
//   - team_coverage/repo_coverage (investment/flow, investment/flow/
//     repo-team): the SAME empirical "retired units typically skew
//     toward unassigned" reasoning SupersessionSkewShape's own doc
//     comment already establishes for investmentFull's coverage leaves
//     -- an assumption about WHICH way a ratio moves, not a structural
//     guarantee, hence BaselineMustBeGreater == false (baseline pulled
//     DOWN).
//   - distinct_team_targets/distinct_repo_targets (investment/flow,
//     investment/flow/repo-team) and evidence_quality_stats.total
//     (GET/POST /api/v1/investment): a PLAIN STRUCTURAL FACT, the same
//     "candidate's row population is baseline's minus a subset" argument
//     DictKeyDirectionShape's own doc comment states -- a count over a
//     strict subset can only read less than or equal to the same count
//     over the full population, hence BaselineMustBeGreater == true
//     (baseline pulled UP).
//
// WHY NOT SupersessionSkewShape: that type's own rule 1 hardcodes "the
// ONLY mismatch findings anywhere in the whole comparison are these two
// NAMED coverage paths" -- built specifically to let the comparator tell
// TWO COMPETING citations apart when they cite the EXACT SAME leaf
// (investmentFull's teamCoverage/repoCoverage are ALSO cited by
// CoverageShiftShape, CoverageShiftShape's own doc comment: "a blanket
// citation... also matches the repos-join fan-out's own citation").
// Reused as-is here, that gate would almost never admit: this exclusion's
// own citation on investment/flow legitimately moves data.links/unassigned_reasons
// alongside team_coverage/repo_coverage under the SAME root cause, so a
// gate requiring NOTHING ELSE in the whole response to differ would read
// a real instance as unexplained whenever its own sibling paths (which
// the SAME BaselineDefect ALSO cites) also carry a live difference --
// exactly the false negative the whole-comparison discipline exists to
// prevent, not produce. The gate is scoping for a genuine collision
// between two DIFFERENT citations at the SAME path, not this shape's
// default posture -- see ContestedPaths' own doc comment.
//
// The shape this type verifies, over the two DECODED response bodies:
//
//  1. If ContestedPaths is non-empty, EVERY mismatch finding anywhere in
//     the whole comparison lies under one of ContestedPaths (checked
//     exactly as CoverageShiftShape/SupersessionSkewShape's own rule 1)
//     -- see ContestedPaths' own doc comment for when to set this.
//  2. Both baseline and candidate are readable as numbers at Path, and
//     baseline is on the declared side of candidate: strictly greater
//     when BaselineMustBeGreater, strictly less otherwise. Equal values
//     never reach here (Compare reports no difference, hence no Finding
//     to classify); a baseline value on the WRONG side of candidate is
//     the one shape this direction cannot produce, and is therefore
//     never admitted.
//
// What this shape CANNOT catch: a real Go regression that happens to
// move Path's OWN value in the SAME direction the declared mechanism
// does (e.g. a Go bug that under-counts, when BaselineMustBeGreater is
// true) is indistinguishable from a genuine instance -- only the sign is
// checked, never disproved. The same known, accepted limit
// SupersessionSkewShape/KeyedDirectionShape/DictKeyDirectionShape's own
// doc comments each state for their own direction claim.
type ScalarDirectionShape struct {
	// Path is the leaf path findings carry for this field -- must equal
	// one of the defect's own Paths entries, e.g. "data.team_coverage".
	Path string
	// BaselineMustBeGreater selects which side of candidate baseline must
	// fall on to be admitted: true admits only baseline > candidate (a
	// strict-subset COUNT/SUM claim, see the type doc comment's second
	// bullet), false admits only baseline < candidate (a ratio-skew
	// claim, the type doc comment's first bullet).
	BaselineMustBeGreater bool
	// ContestedPaths, when non-empty, restricts this shape's admission to
	// comparisons whose ONLY mismatch findings anywhere are under these
	// exact paths. Set this ONLY when a SIBLING BaselineDefect's shape
	// cites the SAME path Path names -- the gate exists to let the
	// comparator tell two competing mechanisms apart when path proximity
	// alone cannot (CoverageShiftShape's own doc comment states the
	// precedent this mirrors). Its ABSENCE (nil/empty) is the NORMAL
	// case, not a relaxation: most ScalarDirectionShape uses have no
	// competing citation at their own Path, and gating them anyway would
	// make an otherwise-valid instance spuriously fail to admit merely
	// because some UNRELATED, correctly-explained finding exists
	// elsewhere in the same response (investment/flow's own supersession-
	// exclusion entry is exactly this: team_coverage/repo_coverage,
	// distinct_team_targets/distinct_repo_targets, data.links and
	// unassigned_reasons all move under the ONE root cause, and neither
	// of investment/flow's other declared defects -- the repos-join
	// fan-out or the argMax null-skip relabelling -- cites
	// team_coverage/repo_coverage/distinct_*_targets at all, so nothing
	// here needs disambiguating and ContestedPaths stays unset).
	ContestedPaths []string
}

// scalarDirectionPlan is one comparison's fully-evaluated admission
// decision, built once per defect (not per finding) from the whole
// comparison's mismatch paths plus the two decoded `data` values.
type scalarDirectionPlan struct {
	shape *ScalarDirectionShape
	// valid is true only when every rule the type doc comment states
	// holds. false admits NOTHING -- the safe default, same as every
	// other plan in this package.
	valid bool
}

// buildScalarDirectionPlan evaluates every rule ScalarDirectionShape
// documents against one comparison's tiered mismatch paths and its
// decoded baseline/candidate `data` values.
func buildScalarDirectionPlan(shape *ScalarDirectionShape, baselineData, candidateData any, mismatches []string) *scalarDirectionPlan {
	plan := &scalarDirectionPlan{shape: shape}

	// Rule 1, only when a gate is declared.
	if len(shape.ContestedPaths) > 0 {
		allowed := make(map[string]bool, len(shape.ContestedPaths))
		for _, p := range shape.ContestedPaths {
			allowed[p] = true
		}
		for _, path := range mismatches {
			if !allowed[path] {
				return plan
			}
		}
	}

	// Rule 2.
	baseValue, okB := floatAtDottedPath(baselineData, shape.Path)
	candValue, okC := floatAtDottedPath(candidateData, shape.Path)
	if !okB || !okC {
		return plan
	}
	if math.IsNaN(baseValue) || math.IsNaN(candValue) || math.IsInf(baseValue, 0) || math.IsInf(candValue, 0) {
		return plan
	}

	if shape.BaselineMustBeGreater {
		plan.valid = baseValue > candValue
	} else {
		plan.valid = baseValue < candValue
	}
	return plan
}

// admits reports whether one Finding is covered by this plan. Unlike a
// per-key shape, this is the SAME whole-defect verdict for every finding
// it is asked about -- defectCovers already restricts what reaches here
// to findings under the defect's own Paths, and this shape only ever
// declares one.
func (p *scalarDirectionPlan) admits(finding Finding) bool {
	if p == nil || !p.valid {
		return false
	}
	return tieredPath(finding.Path) == p.shape.Path
}
