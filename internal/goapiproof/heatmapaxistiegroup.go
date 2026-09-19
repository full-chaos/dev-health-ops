package goapiproof

// HeatmapAxisTieGroupShape, set on a BaselineDefect, narrows that
// defect's blanket "any leaf difference under Paths is covered" rule to
// a data.axes.y difference between two legs that list the SAME names,
// each leg's axis being a valid ordering of that leg's OWN data.cells
// totals (repo_touchpoints: y_field="repo"; hotspot_risk:
// y_field="file_key"). Both routes build the axis with the generic
// branch of `_axis_order` (services/heatmap.py:122-139) / `axisOrder`
// (heatmap/response.go): a sort keyed on total value descending. Among
// names sharing one total, Python keeps row-ENCOUNTER order (not
// derivable from a response body), where this port breaks the tie by
// full name ascending.
//
// The shape verifies, over the two DECODED data.cells/data.axes.y
// values, ONE whole-axis invariant; it admits every data.axes.y
// position or none:
//
//  1. Neither leg's axis repeats a name, the two axes list the exact
//     same set of names, and each leg's data.cells names exactly that
//     set (no entrant, no leaver -- a name-set difference belongs to
//     HeatmapCellBoundaryShape / HeatmapAxisRepoOrderShape and refuses
//     here).
//  2. The BASELINE axis is weakly descending by the baseline's own
//     per-name totals; ties may sit in any order.
//  3. The CANDIDATE axis equals, position for position, the
//     deterministic order of the candidate's own per-name totals: total
//     descending, full name ascending.
//
// A total that differs between the two legs does not change this
// verdict: a value difference is judged by the data.cells declarations
// (KeyedDirectionShape, TeamRepoSubsetShape, ...) and stays outside
// where none admits it. The axis check only proves each axis is a
// consequence of its own leg's cells. A candidate axis that is not the
// deterministic order of its own cells is a Go defect and refuses.
//
// PREMISE: an axis is recomputable from its own leg's cells only when the
// cells present are the complete inputs of the axis order. On both planes
// (services/heatmap.py build_heatmap_response, heatmap/heatmap.go) the y
// axis and data.cells are built from the SAME row set; the top-N query
// only chooses which names the detail rows cover. The one cut between the
// rows and the cells is a row whose x or y label is empty. An empty y
// label leaves an axis name with no cell, which rule 1 refuses; an empty
// x label needs a NULL date or week bucket, which the routes' own
// expressions (toStartOfWeek(day), the commit day) never produce. A route
// whose axis totals include rows absent from data.cells would not satisfy
// this premise and must not carry this shape.
//
// Totals are summed in the cells' own sorted-key order
// (heatmapCellRowsSortedByKey), the order the route sums them in;
// float64 addition is not associative, and rules 2/3 compare totals
// exactly. A one-ULP difference between a reconstructed total and the
// route's own therefore refuses (a stated limit, never an admission).
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
// decision, built once per defect from the two decoded data.cells/
// data.axes.y values.
type heatmapAxisTieGroupPlan struct {
	shape *HeatmapAxisTieGroupShape
	// valid is true only when the whole-axis invariant holds; a valid
	// plan admits every data.axes.y position.
	valid bool
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

// heatmapSumTotalsByName sums every decoded cell's own value into its
// own name (the SAME total the axis sort key uses). An empty cells map
// carries nothing to sum, and returns the empty totals map with no
// iteration at all. Cells are summed in the cells' own sorted-key order
// (heatmapCellRowsSortedByKey, shared with HeatmapCellBoundaryShape's
// own rule 6) rather than Go's randomized map iteration order: float64
// addition is not associative, so summing the same cells in a different
// order can change the total's own last bit run to run, which this
// shape's own exact float64 comparisons (the weak-descent and
// deterministic-order checks below) are sensitive to.
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
	plan := &heatmapAxisTieGroupPlan{shape: shape}

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

	// Rule 1: identical, repeat-free name sets on data.axes.y, each equal
	// to its own leg's data.cells name set.
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
	// Rule 2: the baseline axis is a weakly descending order of its own
	// totals. Rule 3 (the candidate's deterministic order) follows.
	if !heatmapIsWeaklyDescendingByTotal(axisBase, baseTotal) {
		return plan
	}
	if !heatmapNamesAreDeterministicOrder(axisCand, candTotal) {
		return plan
	}
	plan.valid = true
	return plan
}

// heatmapNamesAreDeterministicOrder reports whether names is exactly the
// order axisOrder's generic branch produces from totals: total
// descending, full name ascending. names carries no repeated name and
// every name has a total (the caller checks both).
func heatmapNamesAreDeterministicOrder(names []string, totals map[string]float64) bool {
	for i := 1; i < len(names); i++ {
		prev, cur := names[i-1], names[i]
		if totals[cur] > totals[prev] {
			return false
		}
		if totals[cur] == totals[prev] && cur < prev {
			return false
		}
	}
	return true
}

// admits reports whether one Finding is covered by this plan: any
// data.axes.y position, when the whole-axis invariant holds.
func (p *heatmapAxisTieGroupPlan) admits(finding Finding) bool {
	if p == nil || !p.valid {
		return false
	}
	_, ok := edgeListIndex(finding.Path, p.shape.AxisListPath)
	return ok
}
