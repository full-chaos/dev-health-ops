package goapiproof

import "sort"

// HeatmapAxisTieGroupShape, set on a BaselineDefect, narrows that
// defect's blanket "any leaf difference under Paths is covered" rule to
// the ONE consequence a genuine total-VALUE tie produces on a heatmap
// metric's y-axis (repo_touchpoints: y_field="repo"; hotspot_risk:
// y_field="file_key"), the generic branch of `_axis_order`
// (services/heatmap.py:122-139) / `axisOrder` (heatmap/response.go:
// 157-222): both are a STABLE sort keyed on total value descending, and
// among names sharing the exact same total, Python's own tie-break is
// row-ENCOUNTER order (unverifiable from the two response bodies alone),
// where this port breaks the SAME tie by name ascending -- a
// deterministic choice Python never makes. Two names whose totals
// genuinely tie can therefore occupy either order on the reference
// plane and only the name-ascending order on this plane, with no
// regression anywhere in play.
//
// This is deliberately narrow, and composes with -- never replaces --
// every existing fan-out shape that also reaches data.axes.y
// (HeatmapAxisRepoOrderShape, HeatmapCellBoundaryShape's own rule 6): a
// name whose total genuinely differs between the two legs (a repos-join
// fan-out, a team-scope subset, or a real Go regression) can never
// satisfy this shape's own rule 2 below, so this shape stays INERT on
// it -- admits nothing -- and leaves it to whichever shape's own
// premises actually hold for that name. A defect this shape is wired
// onto reaches ONLY data.axes.y; it carries no opinion about data.cells
// at all.
//
// The shape this type verifies, over the two DECODED `data.cells`/
// `data.axes.y` values:
//
//  1. Neither leg's own `data.axes.y` list carries a repeated name (a
//     list this shape's model does not describe, refused outright
//     before the set comparison below -- a repeat silently collapses
//     into one set entry, which would otherwise hide that some OTHER
//     name is missing from the same list). The two legs' `data.axes.y`
//     name lists are then the EXACT SAME SET (no entrant, no leaver --
//     that boundary-crossing case belongs to HeatmapAxisRepoOrderShape/
//     HeatmapCellBoundaryShape, not here), and each leg's own list is a
//     valid descending-by-its-own-total order (ties permitted, a strict
//     increase anywhere is not) -- a list this shape's model of
//     `_axis_order`/`axisOrder` does not actually describe is refused
//     outright rather than guessed at.
//  2. `data.cells` is reconstructed into per-name totals on EACH leg
//     independently (summing every cell's own value across the buckets
//     it appears in -- the SAME total the axis sort key uses). A
//     contiguous run of `data.axes.y` positions in the CANDIDATE list
//     sharing one candidate-side total value is a tie-group CANDIDATE
//     only when: the SAME position range in the BASELINE list holds the
//     exact same name set (a pure reorder, not a level shift), and every
//     one of those names carries the exact SAME total on both legs (its
//     own baseline total equals its own candidate total, byte-for-byte
//     -- the cross-leg equality that keeps this shape inert wherever a
//     fan-out or a real regression has actually moved a name's total).
//  3. The candidate's own order inside an admitted tie-group is name
//     ascending, exactly. A tie-group whose candidate order is anything
//     else is refused, not admitted: this shape only certifies the ONE
//     deterministic tiebreak this port actually applies, never "any
//     permutation of a tied group is fine."
//  4. Every `data.axes.y[N]` position inside an admitted tie-group is
//     admitted, whether or not it happens to differ from the baseline at
//     that exact position -- the group, not the single position, is
//     what this shape's own premises are checked against.
//
// What this shape CANNOT, and does not try to, certify: a name whose
// total differs at all between the two legs (rule 2's own cross-leg
// equality refuses it immediately, deferring to a fan-out shape or
// leaving it genuinely unexplained), and any case where the two legs'
// own name sets differ (rule 1 refuses the whole plan).
type HeatmapAxisTieGroupShape struct {
	// CellsListPath is the dotted, index-free path to the cells LIST
	// itself, e.g. "data.cells".
	CellsListPath string
	// CellKeyFields are the SAME KeyFields the paired OrderInsensitiveList
	// declaration for CellsListPath uses, in the SAME order, e.g.
	// []string{"x", "y"}.
	CellKeyFields []string
	// NameField names which of CellKeyFields' own object fields carries
	// the axis identity AxisListPath itself lists, e.g. "y".
	NameField string
	// AxisListPath is the dotted, index-free path to the axis name LIST
	// itself, e.g. "data.axes.y".
	AxisListPath string
}

// heatmapAxisTieGroupPlan is one comparison's fully-evaluated admission
// decision, built once per defect from the two decoded `data.cells`/
// `data.axes.y` values.
type heatmapAxisTieGroupPlan struct {
	shape *HeatmapAxisTieGroupShape
	// valid is false only when the shape's own structural premise (rule
	// 1: decodable, same set, both legs a valid descending order) does
	// not hold at all -- never when it holds but no tie-group happens to
	// verify (that is the ordinary "inert" case, valid=true with an
	// empty admittedPositions).
	valid bool
	// admittedPositions holds every axis index (shared by both legs --
	// rule 1 already pins them to the same length) inside a verified
	// tie-group (rule 4).
	admittedPositions map[int]bool
}

// heatmapNameSet builds a set from a name list.
func heatmapNameSet(names []string) map[string]bool {
	set := make(map[string]bool, len(names))
	for _, name := range names {
		set[name] = true
	}
	return set
}

// heatmapNameSetsEqual reports whether a and b hold the exact same set
// of names (length first, then membership -- a cheap reject before the
// membership scan). Two sets of the same, ZERO length are equal with
// nothing left to scan.
func heatmapNameSetsEqual(a, b map[string]bool) bool {
	if len(a) != len(b) {
		return false
	}
	if len(a) == 0 {
		return true
	}
	for name := range a {
		if !b[name] {
			return false
		}
	}
	return true
}

// heatmapIsWeaklyDescendingByTotal reports whether names is sorted by
// totals[name] descending, TIES PERMITTED (a strict increase anywhere is
// not) -- the shape every valid `_axis_order`/`axisOrder` generic-branch
// list satisfies regardless of which name a stable sort's own tie-break
// happened to place first.
func heatmapIsWeaklyDescendingByTotal(names []string, totals map[string]float64) bool {
	for i := 1; i < len(names); i++ {
		if totals[names[i]] > totals[names[i-1]] {
			return false
		}
	}
	return true
}

// heatmapNamesSortedAscending reports whether names is already sorted
// name-ascending.
func heatmapNamesSortedAscending(names []string) bool {
	return sort.StringsAreSorted(names)
}

// heatmapSumTotalsByName sums every decoded cell's own value into its
// own name (the SAME total the axis sort key uses). An empty cells map
// carries nothing to sum, and returns the empty totals map with no
// iteration at all. Cells are summed in the cells' own sorted-key order
// (heatmapCellRowsSortedByKey, shared with HeatmapCellBoundaryShape's
// own rule 6) rather than Go's randomized map iteration order: float64
// addition is not associative, so summing the same cells in a different
// order can change the total's own last bit run to run, which this
// shape's own exact float64 equality checks (the contiguous equal-total
// run walk and heatmapAxisTieGroupAdmits, both below) are sensitive to.
func heatmapSumTotalsByName(cells map[string]heatmapCellRow) map[string]float64 {
	totals := map[string]float64{}
	if len(cells) == 0 {
		return totals
	}
	for _, row := range heatmapCellRowsSortedByKey(cells) {
		totals[row.file] = totals[row.file] + row.value
	}
	return totals
}

// buildHeatmapAxisTieGroupPlan evaluates every rule
// HeatmapAxisTieGroupShape documents against one comparison's decoded
// baseline/candidate `data` values.
func buildHeatmapAxisTieGroupPlan(shape *HeatmapAxisTieGroupShape, baselineData, candidateData any) *heatmapAxisTieGroupPlan {
	plan := &heatmapAxisTieGroupPlan{shape: shape, admittedPositions: map[int]bool{}}

	cellShape := &HeatmapCellBoundaryShape{
		CellsListPath: shape.CellsListPath,
		CellKeyFields: shape.CellKeyFields,
		FileField:     shape.NameField,
	}
	baseCells, ok1 := decodeHeatmapCells(baselineData, cellShape)
	candCells, ok2 := decodeHeatmapCells(candidateData, cellShape)
	axisBase, ok3 := stringListAtDottedPath(baselineData, shape.AxisListPath)
	axisCand, ok4 := stringListAtDottedPath(candidateData, shape.AxisListPath)
	if !ok1 || !ok2 || !ok3 || !ok4 {
		return plan
	}

	baseTotal := heatmapSumTotalsByName(baseCells)
	candTotal := heatmapSumTotalsByName(candCells)

	// Rule 1: identical name sets on data.axes.y, and both legs' own
	// axis a valid (tie-permitting) descending order of their own
	// reconstructed totals.
	if len(axisBase) == 0 || len(axisBase) != len(axisCand) {
		return plan
	}
	baseAxisSet := heatmapNameSet(axisBase)
	candAxisSet := heatmapNameSet(axisCand)
	// A name repeated within one leg's own axis list collapses into a
	// single entry once turned into a set, silently losing the
	// information that some OTHER name is missing from that same list --
	// two distinct malformations (a repeat, an omission) that heatmap
	// NameSetsEqual's own set-vs-set comparison below cannot tell apart
	// from a genuine, well-formed match. Refuse outright: a leg's own
	// axis list carrying a repeated name is not a list this shape's
	// model of `_axis_order`/`axisOrder` (one entry per distinct file)
	// actually describes.
	if len(axisBase) != len(baseAxisSet) || len(axisCand) != len(candAxisSet) {
		return plan
	}
	if !heatmapNameSetsEqual(baseAxisSet, candAxisSet) {
		return plan
	}
	if len(baseTotal) != len(axisBase) || len(candTotal) != len(axisCand) {
		return plan
	}
	if len(baseAxisSet) == 0 {
		return plan
	}
	for name := range baseAxisSet {
		if _, ok := baseTotal[name]; !ok {
			return plan
		}
		if _, ok := candTotal[name]; !ok {
			return plan
		}
	}
	if !heatmapIsWeaklyDescendingByTotal(axisBase, baseTotal) {
		return plan
	}
	if !heatmapIsWeaklyDescendingByTotal(axisCand, candTotal) {
		return plan
	}
	plan.valid = true

	// Rule 2/3: walk the candidate list's own contiguous equal-total
	// runs; each is a tie-group CANDIDATE.
	for lo := 0; lo < len(axisCand); {
		hi := lo + 1
		for hi < len(axisCand) && candTotal[axisCand[hi]] == candTotal[axisCand[lo]] {
			hi++
		}
		if hi-lo > 1 && heatmapAxisTieGroupAdmits(axisBase, axisCand, baseTotal, candTotal, lo, hi) {
			for i := lo; i < hi; i++ {
				plan.admittedPositions[i] = true
			}
		}
		lo = hi
	}

	return plan
}

// heatmapAxisTieGroupAdmits evaluates rules 2/3 for one candidate
// contiguous equal-total run [lo, hi): the same name set occupies [lo,
// hi) on the baseline leg, every one of those names' own total is
// byte-identical across both legs, and the candidate's own order inside
// [lo, hi) is name ascending.
func heatmapAxisTieGroupAdmits(axisBase, axisCand []string, baseTotal, candTotal map[string]float64, lo, hi int) bool {
	if hi > len(axisBase) {
		return false
	}
	candGroup := axisCand[lo:hi]
	baseGroup := axisBase[lo:hi]
	if !heatmapNameSetsEqual(heatmapNameSet(candGroup), heatmapNameSet(baseGroup)) {
		return false
	}
	for _, name := range candGroup {
		if baseTotal[name] != candTotal[name] {
			return false
		}
	}
	return heatmapNamesSortedAscending(candGroup)
}

// admits reports whether one Finding is covered by this plan.
func (p *heatmapAxisTieGroupPlan) admits(finding Finding) bool {
	if p == nil || !p.valid {
		return false
	}
	if tieredPath(finding.Path) != p.shape.AxisListPath {
		return false
	}
	idx, ok := edgeListIndex(finding.Path, p.shape.AxisListPath)
	if !ok {
		return false
	}
	return p.admittedPositions[idx]
}
