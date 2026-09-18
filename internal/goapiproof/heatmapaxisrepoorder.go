package goapiproof

import "sort"

// HeatmapAxisRepoOrderShape, set on a BaselineDefect, narrows that
// defect's blanket "any leaf difference under Paths is covered" rule to
// the axis-reordering consequence a verified per-repo integer fan-out
// multiplier produces on a heatmap metric whose own y-axis IS the
// repository identity directly (repo_touchpoints: y_field="repo",
// services/heatmap.py's HEATMAP_METRICS entry), sorted by per-repo total
// value descending, the generic branch of `_axis_order` (heatmap.py:
// 122-139) / `axisOrder` (heatmap/response.go:157-222).
//
// This is DELIBERATELY narrower than HeatmapCellBoundaryShape
// (heatmapcellboundary.go), which exists for hotspot_risk's own
// LIMIT-bounded file list: HeatmapCellBoundaryShape's rule 3 requires
// both reconstructed totals maps to be EXACTLY the route's fixed Limit
// long, which a team-scoped repo_touchpoints request never reaches when
// the team owns fewer repositories than the cap (confirmed live: a
// 6-repository capture against Limit=20) -- there is no LIMIT boundary
// to reason about at all when nothing entered or left either plane's
// list, only a value-driven REORDER among the SAME repositories both
// legs already agree on. This shape never admits a presence
// (entrant/leaver) finding; that stays HeatmapCellBoundaryShape's own
// job for the metrics/requests where a boundary is actually in play.
//
// The shape this type verifies, over the two DECODED `data.cells`/
// `data.axes.y` values:
//
//  1. The two legs' `data.cells` reconstruct to the EXACT SAME set of
//     repository names (summing each repo's own cell values), and
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
//     for rule 3 below. At least one repository must carry a verified k,
//     or there is nothing this shape's own mechanism could have caused.
//  3. The baseline's own axis is re-derived from the CANDIDATE's own
//     per-repo totals, scaled by each repository's own verified k (k=1
//     where unverified) -- never a fresh recomputation of the baseline's
//     real totals, which is what proves the reorder is EXACTLY what the
//     verified multiplier(s) alone produce, nothing more. This
//     re-derived order must equal the OBSERVED baseline `data.axes.y`
//     EXACTLY, position for position, whole list -- never a per-position
//     "these two names happen to be admitted elsewhere" guess. The
//     candidate's own observed axis, and the baseline's own observed
//     axis against its own TRUE (unscaled) totals, are both checked the
//     same way first, as a sanity precondition: a route whose own axis
//     does not match a plain descending sort of its own totals is not
//     one this shape's model of `_axis_order`/`axisOrder` actually
//     describes, and is refused rather than guessed at.
//  4. Sort ties are refused, not guessed. `_axis_order`'s default branch
//     and `axisOrder`'s own default branch are both a STABLE sort keyed
//     on total value descending -- ties break on each plane's own row
//     ENCOUNTER order, which this shape cannot independently verify from
//     the two response bodies alone (ClickHouse's own row order for an
//     un-ORDER-BY'd GROUP BY is not guaranteed to agree between the two
//     planes even absent any fan-out). So: a tie anywhere in the
//     baseline's own totals, the candidate's own totals, OR the
//     candidate-scaled-by-k expected-baseline totals refuses the WHOLE
//     plan outright -- the descending-by-total order this shape computes
//     is a well-defined TOTAL order (unique, tie-break-independent) only
//     when no such tie exists.
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
	// the repository identity DIRECTLY -- no "<repo>:<path>" parsing, e.g.
	// "y" for repo_touchpoints (y_field="repo").
	RepoField string
	// AxisListPath is the dotted, index-free path to the repository-name
	// axis LIST itself, e.g. "data.axes.y".
	AxisListPath string
}

// heatmapAxisRepoOrderPlan is one comparison's fully-evaluated admission
// decision, built once per defect from the two decoded `data.cells`/
// `data.axes.y` values.
type heatmapAxisRepoOrderPlan struct {
	shape *HeatmapAxisRepoOrderShape
	valid bool
}

// heatmapHasTie reports whether two distinct keys of totals share the
// exact same value -- see HeatmapAxisRepoOrderShape's own rule 4.
func heatmapHasTie(totals map[string]float64) bool {
	seen := make(map[float64]bool, len(totals))
	for _, value := range totals {
		if seen[value] {
			return true
		}
		seen[value] = true
	}
	return false
}

// heatmapSortDescByTotal returns totals' own keys sorted by value
// descending, ties (should any survive the caller's own heatmapHasTie
// check) broken by name ascending purely for a DETERMINISTIC result --
// Go's own map iteration order is randomized per run, so an unbroken tie
// fed through a non-stable sort would otherwise make this function's own
// result (and any caller comparing it) flaky rather than reliably wrong.
// Only meaningful as a TOTAL (unique, tie-break-independent) order when
// totals carries no tie at all -- callers check heatmapHasTie first.
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

	baseRepoTotal := map[string]float64{}
	for _, row := range baseCells {
		baseRepoTotal[row.file] = baseRepoTotal[row.file] + row.value
	}
	candRepoTotal := map[string]float64{}
	for _, row := range candCells {
		candRepoTotal[row.file] = candRepoTotal[row.file] + row.value
	}

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
			repoKs[candRow.file] = append(repoKs[candRow.file], k)
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

	// Rule 3: expected baseline totals, scaled from the candidate's own
	// totals by each repository's own verified k (k=1 where unverified).
	expectedBaseTotal := make(map[string]float64, len(candRepoTotal))
	for repo, total := range candRepoTotal {
		k := 1
		if kk, ok := repoMultiplier[repo]; ok {
			k = kk
		}
		expectedBaseTotal[repo] = total * float64(k)
	}

	// Rule 4: refuse on any tie -- see this shape's own doc comment.
	if heatmapHasTie(baseRepoTotal) || heatmapHasTie(candRepoTotal) || heatmapHasTie(expectedBaseTotal) {
		return plan
	}

	// Rule 3, continued: sanity precondition (both legs' own observed
	// axis matches a plain descending sort of their own real totals),
	// then the actual cross-leg re-derivation.
	if !stringSlicesEqual(heatmapSortDescByTotal(baseRepoTotal), axisBase) {
		return plan
	}
	if !stringSlicesEqual(heatmapSortDescByTotal(candRepoTotal), axisCand) {
		return plan
	}
	if !stringSlicesEqual(heatmapSortDescByTotal(expectedBaseTotal), axisBase) {
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
