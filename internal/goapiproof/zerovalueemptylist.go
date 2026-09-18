package goapiproof

import "strings"

// ZeroValueEmptyListShape, set on a BaselineDefect, admits the FULL
// CONSEQUENCE a value-defect declared under the SAME Ticket's OWN blanket
// leaf Paths produces on ONE ranked list this route derives FROM that
// value: when the candidate's headline collapses to zero under the
// declared mechanism, the candidate's own list derives nothing and comes
// back empty, while the baseline (computing the value from a WIDER
// population) still ranks at least one entry -- a PRESENCE difference at
// every baseline-only element of that list, categorically outside every
// leaf-only Paths citation (leafDifference, compare.go).
//
// Established from source: for blocked_work, fetchMetricContributors/
// fetchMetricDriverDelta (cmd/query-api/internal/explain/metrics.go) GROUP
// BY over the SAME status-filtered FROM clause metricValueProjection's own
// headline read uses (response.go:158-176) -- zero matching rows for the
// window leaves the headline value 0 AND the list empty, by the SAME
// cause, not two independent ones (this shape's own sibling blanket leaf
// declaration, restcorpus.go, states the mechanism in words; this shape
// only verifies the two-body CONSEQUENCE of it, never the underlying row
// count, which is not on the wire). One instance covers ONE list -- the
// same one-shape-one-list granularity every KeyedDirectionShape pair in
// this package already uses for drivers versus contributors
// (restcorpus.go).
//
// The shape this type verifies, over the two DECODED response bodies:
//
//  1. The baseline's own value at ValuePath is non-zero and the
//     candidate's is exactly zero. Both zero (a metric already at zero on
//     both planes for an unrelated reason) refuses the WHOLE plan -- there
//     is then no declared value collapse for a list difference to be a
//     consequence OF, so any list difference under this defect's own
//     ListPath stays outside, correctly.
//  2. ListPath resolves to a JSON array on both sides, the candidate's is
//     EMPTY, and the baseline's is NOT. Either side failing this refuses
//     the whole plan -- a non-empty candidate list, or an already-empty
//     baseline list, means the value collapse did NOT fully explain what
//     changed under this list, so nothing here is admitted by
//     coincidence.
//
// Admission is granted ONLY for a ShapePresence finding at ListPath,
// baseline-only direction ("absent in candidate") -- never the reverse:
// this shape's own mechanism (a value collapsing to zero) can only ever
// make a candidate list NARROWER than baseline, never wider, so a
// "present in candidate, absent in baseline" finding is left outside,
// unconditionally.
//
// WHY NOT TeamRepoSubsetShape: that shape explicitly refuses an EMPTY
// candidate list (teamreposubset.go: "an empty candidate list proves
// nothing about the claimed scope") -- exactly the case this shape exists
// to cover, gated instead on the value-collapse condition that shape has
// no notion of.
type ZeroValueEmptyListShape struct {
	// ValuePath is the dotted, index-free path to the headline numeric
	// leaf whose collapse to zero drives this list's own consequence,
	// e.g. "data.value".
	ValuePath string
	// ListPath is the dotted, index-free path to the ONE ranked list this
	// same collapse empties, e.g. "data.drivers".
	ListPath string
}

// zeroValueEmptyListPlan is one comparison's fully-evaluated admission
// decision, built once per defect from the two decoded response bodies.
type zeroValueEmptyListPlan struct {
	shape *ZeroValueEmptyListShape
	// valid reports whether both rules held. false admits NOTHING -- the
	// safe default, same as every other plan in this package.
	valid bool
}

// buildZeroValueEmptyListPlan evaluates both rules ZeroValueEmptyListShape
// documents against one comparison's decoded baseline/candidate bodies.
func buildZeroValueEmptyListPlan(shape *ZeroValueEmptyListShape, baselineData, candidateData any) *zeroValueEmptyListPlan {
	plan := &zeroValueEmptyListPlan{shape: shape}

	baselineValue, ok1 := floatAtDottedPath(baselineData, shape.ValuePath)
	candidateValue, ok2 := floatAtDottedPath(candidateData, shape.ValuePath)
	if !ok1 || !ok2 || baselineValue == 0 || candidateValue != 0 {
		return plan
	}

	baseList, ok3 := listAtDottedPath(baselineData, shape.ListPath)
	candList, ok4 := listAtDottedPath(candidateData, shape.ListPath)
	if !ok3 || !ok4 || len(baseList) == 0 || len(candList) != 0 {
		return plan
	}

	plan.valid = true
	return plan
}

// admits reports whether one Finding is covered by this plan: a
// ShapePresence finding at ListPath, baseline-only direction only.
func (p *zeroValueEmptyListPlan) admits(finding Finding) bool {
	if p == nil || !p.valid || finding.Shape != ShapePresence {
		return false
	}
	if !strings.Contains(finding.Detail, "absent in candidate") {
		return false
	}
	return tieredPath(finding.Path) == p.shape.ListPath
}
