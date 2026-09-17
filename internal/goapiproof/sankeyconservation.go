package goapiproof

import "math"

// ConservationShape, set on a BaselineDefect, narrows that defect's
// blanket "any leaf difference under Paths is covered" rule to the ONE
// transform a RELABELLING mechanism can actually produce: the total of a
// list's own numeric field is UNCHANGED between the two planes, even
// though individual elements may move value between each other.
//
// This is the sankeyInvestmentParity argMax null-skip relabelling mechanism: work_unit_
// investments.repo_id/work_unit_type/work_unit_name/provider are
// Nullable, and the reference plane's own LATEST_WORK_UNIT_INVESTMENTS_CTE
// dedups them via a bare argMax(col, computed_at), which SKIPS a row
// whose column is NULL when picking the newest version and so returns a
// STALE non-null value from an earlier generation instead of the TRUE
// latest (possibly-null) one this port reads via a tuple-wrapped argMax.
// A work unit whose newest generation cleared one of those columns moves
// which node/edge its effort lands on -- it does NOT create or destroy
// effort, so the population's total is the SAME sum, just grouped
// differently. That gives this shape a CONSERVED quantity to check, not
// a direction: unlike SankeyRepoFanoutShape's mechanism (which can only
// ever ADD rows, so baseline is always >= candidate) or
// KeyedDirectionShape's (same), a relabelling can push ONE edge's value
// either up or down relative to candidate, so no per-edge sign holds --
// only the TOTAL is invariant.
//
// HAS THIS MECHANISM EVER FIRED ON A REAL CAPTURE? No. The declaration
// is Intermittent (present only while at least one work unit's newest
// generation actually differs from an earlier one on a Nullable
// attribution column), and every production capture checked while
// building this shape (a production deployed-vs-deployed prove run) shows the whole observed
// divergence explained by SankeyRepoFanoutShape's repos-join mechanism
// alone -- the totals in that capture already agreed before this shape
// was even needed. This shape is proven only by a deliberately injected
// fixture fault (see sankeyconservation_test.go); its silence in every
// capture checked so far is not evidence it cannot fire, only that it
// has not yet been observed to.
//
// The shape this type verifies, over the two DECODED response bodies:
//
//  1. The ONLY mismatch findings anywhere in the comparison lie under
//     ListPath itself -- its own leaf ValuePath, or a presence/length
//     finding at an element of the list (compareListByKey's own "key
//     present on only one side" case, which a relabelling can also
//     produce when a combination that did not exist on one plane before
//     now does). A difference surfacing anywhere else means something
//     outside this mechanism is also in play, and this shape admits
//     nothing at all -- the same whole-comparison discipline
//     CoverageShiftShape and SupersessionSkewShape already use.
//  2. sum(ValueField) over EVERY element of the list, baseline against
//     candidate, agrees within floatTolerance (this package's own
//     established Tier-B epsilon, max(abs, rel) -- the SAME constant
//     every other float comparison here uses, not a bound chosen for
//     this shape). A redistribution never changes this sum; a real
//     defect that ALSO drops or adds effort does.
//
// Anything outside these two rules is NOT covered: classifyBaselineDefects
// reports it as an ordinary difference outside the citation, exactly like
// any other uncited mismatch. In particular: a relabelling that moves a
// work unit onto a (source, target) COMBINATION that did not previously
// exist on either plane surfaces as a structural extra/missing key
// (rule 1 still lets the WHOLE plan stay valid, since that finding sits
// under ListPath, but compare.go's leafDifference gate never lets ANY
// shape admit a structural finding) -- this shape only ever reaches the
// leaf value findings on keys BOTH planes already carry.
type ConservationShape struct {
	// ListPath is the dotted, index-free path to the list itself, e.g.
	// "data.links".
	ListPath string
	// ValueField is the element's own numeric field name, e.g. "value".
	ValueField string
	// ValuePath is the leaf path findings carry for that field -- must
	// equal one of the defect's own Paths entries, e.g. "data.links.value".
	ValuePath string
}

// conservationPlan is one comparison's fully-evaluated admission
// decision, built once per defect (not per finding) from the whole
// comparison's mismatch paths plus the two decoded `data` values.
type conservationPlan struct {
	// valid is a WHOLE-COMPARISON verdict: true only when both rules the
	// type doc comment states hold. false admits NOTHING -- the safe
	// default, same as coverageShiftPlan.valid and supersessionSkewPlan.valid.
	valid bool
}

// buildConservationPlan evaluates every rule ConservationShape documents
// against one comparison's tiered mismatch paths and its decoded
// baseline/candidate `data` values.
func buildConservationPlan(shape *ConservationShape, baselineData, candidateData any, mismatches []string) *conservationPlan {
	plan := &conservationPlan{}

	// Rule 1.
	for _, path := range mismatches {
		if path != shape.ValuePath && path != shape.ListPath {
			return plan
		}
	}

	// Rule 2.
	baseList, ok1 := listAtDottedPath(baselineData, shape.ListPath)
	candList, ok2 := listAtDottedPath(candidateData, shape.ListPath)
	if !ok1 || !ok2 {
		return plan
	}
	baseSum, ok3 := sumField(baseList, shape.ValueField)
	candSum, ok4 := sumField(candList, shape.ValueField)
	if !ok3 || !ok4 {
		return plan
	}
	tolerance := math.Max(floatTolerance, floatTolerance*math.Max(math.Abs(baseSum), math.Abs(candSum)))
	if math.Abs(baseSum-candSum) > tolerance {
		return plan
	}

	plan.valid = true
	return plan
}

// admits reports whether one Finding is covered by this plan. Same
// whole-comparison verdict for every finding it is asked about as
// coverageShiftPlan.admits/supersessionSkewPlan.admits -- rule 1 already
// restricts which findings can ever reach here, and compare.go's
// leafDifference gate already restricts this to leaf value findings
// before it is called.
func (p *conservationPlan) admits(Finding) bool {
	return p != nil && p.valid
}

// sumField sums one numeric field across every element of a decoded
// list. ok is false when any element is not an object, or does not carry
// a numeric ValueField -- the caller treats that as "cannot evaluate the
// shape at all" rather than summing a partial, silently wrong total.
func sumField(list []any, field string) (float64, bool) {
	var total float64
	for _, element := range list {
		object, ok := element.(map[string]any)
		if !ok {
			return 0, false
		}
		value, ok := asFloat(object[field])
		if !ok {
			return 0, false
		}
		total += value
	}
	return total, true
}
