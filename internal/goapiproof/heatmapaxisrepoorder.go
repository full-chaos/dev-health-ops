package goapiproof

import "sort"

// HeatmapAxisRepoOrderShape, set on a BaselineDefect, narrows that
// defect's blanket "any leaf difference under Paths is covered" rule to
// the axis-reordering consequence a verified per-repo integer fan-out
// multiplier produces on a heatmap metric whose y-axis is sorted by
// per-name total value descending, the generic branch of `_axis_order`
// (heatmap.py:122-139) / `axisOrder` (heatmap/response.go:157-222). The
// axis name is either the repository itself (repo_touchpoints:
// y_field="repo") or a "<repo>:<path>" file key whose multiplier is its
// repository's (hotspot_risk: y_field="file_key", FileKeyNames).
//
// This is DELIBERATELY narrower than HeatmapCellBoundaryShape
// (heatmapcellboundary.go), which exists for hotspot_risk's own
// LIMIT-bounded file list crossing its boundary: HeatmapCellBoundaryShape's
// rule 3 requires both reconstructed totals maps to be EXACTLY the
// route's fixed Limit long, which a request never reaches when the scope
// holds fewer names than the cap (confirmed live: a 6-repository
// capture against Limit=20) -- there is no LIMIT boundary to reason
// about when nothing entered or left either plane's list, only a
// value-driven REORDER among the SAME names both legs already agree on.
// This shape never admits a presence (entrant/leaver) finding; that
// stays HeatmapCellBoundaryShape's own job where a boundary is in play.
//
// The shape this type verifies, over the two DECODED `data.cells`/
// `data.axes.y` values:
//
//  1. The two legs' `data.cells` reconstruct to the EXACT SAME set of
//     axis names (summing each name's own cell values), and
//     `data.axes.y` on each leg is exactly that same name set, no more
//     and no fewer. Anything else -- an entrant, a leaver, an axis name
//     absent from the reconstructed set -- refuses the WHOLE plan: this
//     shape has no boundary-crossing rule to fall back on.
//  2. A repository's multiplier k is the ONE INTEGER >= 2
//     (repoFanoutIntegerMultiplier) that AT LEAST TWO of its own shared
//     (present-on-both-legs) cells independently agree on
//     (intsAgree) -- the SAME discipline HeatmapCellBoundaryShape's own
//     rule 1 and SankeyRepoFanoutShape's own rule 1 already apply. A
//     repository with no verified k defaults to k=1 (no known fan-out)
//     for rule 4 below. At least one repository must carry a verified k,
//     or there is nothing this shape's own mechanism could have caused.
//  3. The candidate's own axis must equal, position for position, a
//     descending sort of its own per-repo totals with ties broken by
//     name ascending -- `axisOrder`'s (heatmap/response.go) own
//     deterministic rule, a function of (name, total) alone.
//  4. Every cell is present on both legs, and every cell's baseline
//     value is its candidate value under its repository's verified k
//     (heatmapCellExplained; k=1 where unverified) -- the fan-out
//     multiplies every cell of a repository and changes nothing else,
//     so an order agreement never stands in for a value agreement. The
//     baseline's own axis is then its own totals sorted descending and
//     split into runs of exactly equal total: the SAME position range of
//     the observed baseline axis must hold exactly that run's name set,
//     any order inside a run. `_axis_order`'s (services/heatmap.py) own
//     default branch breaks a tie on the reference plane's row encounter
//     order, which the response body does not carry -- the same rule
//     HeatmapAxisTieGroupShape and HeatmapCellBoundaryShape's rule 6b
//     apply.
//
// What this shape CANNOT, and does not try to, certify: an axis reorder
// caused by anything other than a verified integer per-repo multiplier
// (a genuine Go regression on an unrelated repository fails rule 3's own
// whole-list re-derivation and stays outside), and any case where the
// two legs' own repository sets differ at all (rule 1 refuses the whole
// plan -- a LIMIT-boundary case belongs to a future extension of
// HeatmapCellBoundaryShape, not here).
type HeatmapAxisRepoOrderShape struct {
	// CellsListPath is the dotted, index-free path to the cells LIST
	// itself, e.g. "data.cells".
	CellsListPath string
	// CellKeyFields are the SAME KeyFields the paired OrderInsensitiveList
	// declaration for CellsListPath uses, in the SAME order, e.g.
	// []string{"x", "y"}.
	CellKeyFields []string
	// RepoField names which of CellKeyFields' own object fields carries
	// the axis name, e.g. "y" (repo_touchpoints: y_field="repo";
	// hotspot_risk: y_field="file_key").
	RepoField string
	// FileKeyNames marks axis names as "<repo>:<path>" file keys
	// (hotspot_risk): a name's multiplier is its repository's
	// (heatmapFileRepo), verified across every file of that repository.
	// When false the name IS the repository (repo_touchpoints).
	FileKeyNames bool
	// AxisListPath is the dotted, index-free path to the repository-name
	// axis LIST itself, e.g. "data.axes.y".
	AxisListPath string
}

// repoOf is the repository an axis name belongs to.
func (shape *HeatmapAxisRepoOrderShape) repoOf(name string) string {
	if shape.FileKeyNames {
		return heatmapFileRepo(name)
	}
	return name
}

// heatmapAxisRepoOrderPlan is one comparison's fully-evaluated admission
// decision, built once per defect from the two decoded `data.cells`/
// `data.axes.y` values.
type heatmapAxisRepoOrderPlan struct {
	shape *HeatmapAxisRepoOrderShape
	valid bool
}

// heatmapCellExplained reports whether a shared cell's baseline value is
// its candidate value under its repository's verified multiplier k: for
// k <= 1 equal within the comparator's own Tier B float tolerance (exact
// for integer counts), for k >= 2 the same integer ratio
// repoFanoutIntegerMultiplier verified k from, or both zero.
func heatmapCellExplained(baseline, candidate float64, k int) bool {
	if k <= 1 {
		return withinFloatTolerance(baseline, candidate)
	}
	if baseline == 0 && candidate == 0 {
		return true
	}
	got, ok := repoFanoutIntegerMultiplier(baseline, candidate)
	return ok && got == k
}

// heatmapSharedCellsExplained reports whether every cell present on both
// legs is explained by its own repository's verified multiplier.
func heatmapSharedCellsExplained(baseCells, candCells map[string]heatmapCellRow, repoOf func(string) string, repoMultiplier map[string]int) bool {
	if len(candCells) == 0 {
		return true
	}
	for key, candRow := range candCells {
		baseRow, shared := baseCells[key]
		if !shared {
			continue
		}
		if !heatmapCellExplained(baseRow.value, candRow.value, repoMultiplier[repoOf(candRow.file)]) {
			return false
		}
	}
	return true
}

// heatmapAxisMatchesTieRuns reports whether axis lists exactly totals'
// own names, descending by total, where each run of exactly equal total
// occupies the same position range on axis as a name set (any order
// inside the run).
func heatmapAxisMatchesTieRuns(axis []string, totals map[string]float64) bool {
	names := heatmapSortDescByTotal(totals)
	if len(names) != len(axis) {
		return false
	}
	for lo := 0; lo < len(names); {
		hi := lo + 1
		for hi < len(names) && totals[names[hi]] == totals[names[lo]] {
			hi++
		}
		if !heatmapNameSetsEqual(heatmapNameSet(axis[lo:hi]), heatmapNameSet(names[lo:hi])) {
			return false
		}
		lo = hi
	}
	return true
}

// heatmapSortDescByTotal returns totals' own keys sorted by value
// descending, ties broken by name ascending -- axisOrder's own rule, and
// a deterministic result whatever Go's map iteration order.
func heatmapSortDescByTotal(totals map[string]float64) []string {
	names := make([]string, 0, len(totals))
	for name := range totals {
		names = append(names, name)
	}
	sort.Slice(names, func(i, j int) bool {
		if totals[names[i]] != totals[names[j]] {
			return totals[names[i]] > totals[names[j]]
		}
		return names[i] < names[j]
	})
	return names
}

// stringSlicesEqual reports whether a and b hold the same strings in the
// same order.
func stringSlicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// buildHeatmapAxisRepoOrderPlan evaluates every rule
// HeatmapAxisRepoOrderShape documents against one comparison's decoded
// baseline/candidate `data` values.
func buildHeatmapAxisRepoOrderPlan(shape *HeatmapAxisRepoOrderShape, baselineData, candidateData any) *heatmapAxisRepoOrderPlan {
	plan := &heatmapAxisRepoOrderPlan{shape: shape}

	cellShape := &HeatmapCellBoundaryShape{
		CellsListPath: shape.CellsListPath,
		CellKeyFields: shape.CellKeyFields,
		FileField:     shape.RepoField,
	}
	baseCells, ok1 := decodeHeatmapCells(baselineData, cellShape)
	candCells, ok2 := decodeHeatmapCells(candidateData, cellShape)
	axisBase, ok3 := stringListAtDottedPath(baselineData, shape.AxisListPath)
	axisCand, ok4 := stringListAtDottedPath(candidateData, shape.AxisListPath)
	if !ok1 || !ok2 || !ok3 || !ok4 {
		return plan
	}

	baseRepoTotal := heatmapSumTotalsByName(baseCells)
	candRepoTotal := heatmapSumTotalsByName(candCells)

	// Rule 1: identical repository sets, no entrant/leaver, on both
	// data.cells and data.axes.y.
	if len(baseRepoTotal) == 0 || len(baseRepoTotal) != len(candRepoTotal) {
		return plan
	}
	// baseRepoTotal is non-empty here (the length check directly above),
	// but that fact is several statements away from EITHER range below --
	// each one restates it immediately adjacent to its own loop, the same
	// explicit, adjacent-to-the-loop invariant heatmapFileSetDifference's
	// own doc comment states for its own map range
	// (heatmapcellboundary.go).
	if len(baseRepoTotal) == 0 {
		return plan
	}
	for repo := range baseRepoTotal {
		if _, ok := candRepoTotal[repo]; !ok {
			return plan
		}
	}
	if len(axisBase) != len(baseRepoTotal) || len(axisCand) != len(baseRepoTotal) {
		return plan
	}
	axisBaseSet := make(map[string]bool, len(axisBase))
	for _, name := range axisBase {
		axisBaseSet[name] = true
	}
	for _, name := range axisCand {
		if !axisBaseSet[name] {
			return plan
		}
	}
	if len(baseRepoTotal) == 0 {
		return plan
	}
	for repo := range baseRepoTotal {
		if !axisBaseSet[repo] {
			return plan
		}
	}

	// Rule 2: verified per-repository integer multiplier, >=2 agreeing
	// shared cells.
	repoKs := map[string][]int{}
	for key, candRow := range candCells {
		baseRow, ok := baseCells[key]
		if !ok || baseRow.value == candRow.value {
			continue
		}
		if k, ok := repoFanoutIntegerMultiplier(baseRow.value, candRow.value); ok {
			repo := shape.repoOf(candRow.file)
			repoKs[repo] = append(repoKs[repo], k)
		}
	}
	repoMultiplier := map[string]int{}
	for repo, ks := range repoKs {
		if len(ks) < 2 {
			continue
		}
		if agree, k := intsAgree(ks); agree {
			repoMultiplier[repo] = k
		}
	}
	if len(repoMultiplier) == 0 {
		return plan
	}

	// Rule 4, values: every cell is present on both legs and explained by its
	// repository's own verified k (k=1 where none) -- a name whose value
	// moved for any other reason refuses the plan, whatever the axis
	// order looks like.
	if len(baseCells) != len(candCells) {
		return plan
	}
	for key := range baseCells {
		if _, ok := candCells[key]; !ok {
			return plan
		}
	}
	if !heatmapSharedCellsExplained(baseCells, candCells, shape.repoOf, repoMultiplier) {
		return plan
	}

	// Rule 3: the candidate's own axis is axisOrder's own deterministic
	// order of its own totals, whole list -- the axis admission gate.
	if !heatmapCandidateAxisInGoOrder(axisCand, candCells) {
		return plan
	}
	// Rule 4, order: the baseline's own axis matches its own totals run
	// by run as name sets; the values rule above already pinned those
	// totals to the candidate's scaled by k.
	if !heatmapAxisMatchesTieRuns(axisBase, baseRepoTotal) {
		return plan
	}

	plan.valid = true
	return plan
}

// admits reports whether one Finding is covered by this plan. Every rule
// above is checked whole-list, so a valid plan admits every
// `data.axes.y[N]` finding in this comparison -- never a per-position
// guess.
func (p *heatmapAxisRepoOrderPlan) admits(finding Finding) bool {
	if p == nil || !p.valid {
		return false
	}
	return tieredPath(finding.Path) == p.shape.AxisListPath
}
