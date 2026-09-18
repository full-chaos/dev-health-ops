package goapiproof

import (
	"fmt"
	"sort"
	"strings"
)

// HeatmapCellBoundaryShape, set on a BaselineDefect, narrows that
// defect's blanket "any leaf difference under Paths is covered" rule to
// the FULL consequence set the repos-join fan-out (heatmapDedupParity's
// own KeyedDirectionShape entry, same class of mechanism) produces on
// GET /api/v1/heatmap's hotspot_risk metric: a bounded, value-DESC top-N
// FILE selection (fetchHotspotRisk's own top_query, api/queries/
// heatmap.py:161-178 / cmd/query-api/internal/heatmap/queries.go:
// 244-257, LIMIT 20 on both planes -- services/heatmap.py:367,
// heatmap.go:322) feeding a flat, (week, file_key)-keyed `data.cells`
// list PLUS a SEPARATE, positional `data.axes.y` file-name list derived
// from the SAME per-file totals (services/heatmap.py's own
// `_axis_order` default branch, sorted by total value descending;
// heatmap/response.go's own `axisOrder` port of it).
//
// A physically-duplicated `repos` row for ONE repository inflates every
// file that repository owns by the SAME integer factor k (the repos row
// is joined once per file_metrics_daily row that repository owns,
// regardless of file or week -- confirmed against a real production
// capture: every one of one repository's own shared cells differed by
// EXACTLY 2.0x, and that repository alone supplied every baseline-only
// AND every candidate-only cell). This can (a) cross fetchHotspotRisk's
// own fixed top-20 boundary, entering one plane's list at the cost of
// whichever file currently ranks last on the other plane -- the same
// LIMIT-boundary consequence LimitDisplacementShape/
// HotspotListBoundaryShape already admit for their own routes -- and (b)
// reorder `data.axes.y`, whose own sort key is exactly the per-file
// total the same fan-out inflates.
//
// The shape this type verifies, over the two DECODED `data.cells` lists
// (paired by CellKeyFields, the SAME KeyFields the paired
// OrderInsensitiveList declaration for this path uses) and the two
// `data.axes.y` lists:
//
//  1. A repository's multiplier k is the ONE INTEGER >= 2
//     (repoFanoutIntegerMultiplier) that AT LEAST TWO of its own shared
//     (both-sides) cells independently agree on. A SINGLE shared cell
//     never establishes k on its own: a lone ratio that happens to be a
//     whole number could be an unrelated undercount, and admitting it
//     would paper over a genuine Go regression as if it were the
//     declared fan-out. A repository whose shared cells disagree, or
//     whose only shared-cell ratio is non-integer, has NO verified k and
//     explains nothing below.
//
//  2. A shared-key (present on both sides) cell VALUE difference is
//     admitted only when the baseline value equals the candidate value
//     times its OWN repository's verified k, exactly (within
//     repoFanoutFloatsWithinTolerance).
//
//  3. Both `data.cells` lists are reconstructed into per-FILE totals
//     (summing every cell's own value across the weeks it appears in --
//     the SAME total `data.axes.y`'s own sort key uses) and required to
//     name EXACTLY Limit distinct files each. Anything else refuses the
//     WHOLE plan, never a partial guess -- the same discipline
//     HotspotListBoundaryShape's own rule 1 applies to the sankey
//     route's file list.
//
//  4. A baseline-only (leaving) file is an ENTRANT admission candidate
//     when its own repository carries a verified k>1 and its total
//     divided by k -- its true, undoubled total -- sits at or under the
//     candidate list's own minimum file total. A candidate-only
//     (entering) file is admitted one-for-one against however many rule
//     4's baseline side actually admitted, LOWEST-VALUED first, each
//     still required to sit at or under baseline's own stable floor (its
//     minimum excluding every baseline-only file) -- identical to
//     HotspotListBoundaryShape's own rules 2/3, applied to a
//     reconstructed FILE list instead of a sankey graph.
//
//  5. Once rule 4 decides the file sets: every one of an admitted file's
//     own cells (every week it appears in, on its own leg) is admitted
//     as a presence finding.
//
//  6. A `data.axes.y[N]` difference is admitted when BOTH the baseline
//     name and the candidate name occupying position N are names this
//     SAME plan already admitted -- as a rule-4 entrant/leaver, or as a
//     rule-2 shared-value admission -- OR when the TWO-SIDED whole-list
//     identity below holds for the whole comparison. Never a bare "any
//     axis diff is fine": a swap between two names this plan has no
//     opinion about (a genuine Go regression, or an unrelated ordering
//     artifact) stays outside either way.
//
//     TWO-SIDED WHOLE-LIST IDENTITY (rule 6b): rules 1-5 above admit
//     every axis position EXCEPT one whose own file is untouched by any
//     repository's fan-out but whose RANK still moves because its
//     touched neighbours' own ranks move around it -- neither name at
//     such a position is ever a rule-4/rule-2 admission, so rule 6 alone
//     leaves it outside. This identity recovers that residual, over the
//     UNION of baseFileTotal's and candFileTotal's own file sets (every
//     file either page names):
//
//     - per file f, the true total t(f) is: candFileTotal[f] when f is
//     on the candidate page (present in candFileTotal, whether
//     shared or rule-4 entrant); otherwise baseFileTotal[f] divided
//     by f's own repository's verified k (a baseline-only file of a
//     corroborated repository); otherwise baseFileTotal[f] itself (a
//     baseline-only file of a repository with no verified k --
//     untouched, so its baseline value already IS its true value).
//     - b(f) is t(f) times f's own repository's verified k (t(f)
//     unchanged for an uncorroborated repository) -- re-inflating t(f)
//     by the SAME verified multiplier rule 1 already established.
//
//     TIE-GROUP AWARE, per the route's own deterministic tiebreak: this
//     port's own axisOrder breaks a total-value tie name-ascending
//     (heatmap/response.go); the reference plane's own tie-break is
//     unverifiable row-encounter order. This identity therefore never
//     demands a strict order across a genuine tie -- it groups the union
//     by EQUAL t(f) into "true groups" (identity (i)'s own key) and,
//     SEPARATELY, by EQUAL b(f) into "base groups" (identity (ii)'s own
//     key), and checks:
//
//     NOT REQUIRED: the two groupings need never be the identical
//     partition. Identity (i) checks the candidate's own axis against
//     ONLY the t(f) grouping; identity (ii) checks the baseline's own
//     axis against ONLY the b(f) grouping -- each is a complete,
//     self-contained claim about its own target axis, and nothing in
//     either claim reads what the OTHER key's own grouping looks like.
//     A file whose own repository is untouched (no verified k) tying
//     under t(f) with an unrelated untouched file at the exact value a
//     TOUCHED file's own b(f) (t(f) times its verified k) coincidentally
//     lands on is not a genuine cross-plane ambiguity -- it is simply
//     two DIFFERENT, independently well-defined rankings that happen to
//     share a number: a corroborated repository's own leaver file's true
//     total, divided out by its verified k, can equal an unrelated
//     untouched file's own total purely by coincidence of the two
//     underlying quantities, with no shared mechanism linking them and
//     no ambiguity for either identity to resolve on its own terms.
//
//     - identity (i): concatenating the true groups in t(f)-descending
//     order, each group's own names sorted ASCENDING internally (the
//     route's own deterministic tiebreak), and cutting at Limit,
//     reproduces the candidate's own axis EXACTLY, position for
//     position.
//     - identity (ii): concatenating the base groups in b(f)-descending
//     order and cutting at Limit reproduces the SAME axis positions
//     the baseline's own axis occupies over that same contiguous
//     range, group for group, as SETS -- the reference's own order
//     INSIDE one tied group is never constrained, only which
//     contiguous range of positions that group's own names occupy.
//
//     A group whose own members straddle the Limit cut -- the cut falls
//     strictly inside it under that identity's own group order, not on
//     a group boundary -- refuses that identity outright: the listed
//     subset of a straddling group is not determined by anything this
//     shape can verify.
//
//     Both identities must hold, over the WHOLE axis, for rule 6b to
//     admit anything -- it is an all-or-nothing property of this ONE
//     comparison, never evaluated position by position. When it holds,
//     every `data.axes.y[N]` finding is admitted, including the
//     residual positions rules 1-5 alone could not reach. When either
//     identity fails to reproduce its own target exactly, rule 6b
//     admits NOTHING and axis admission falls back to rule 6's own
//     per-position rule unchanged.
//
// What this shape CANNOT catch: a repository whose shared cells carry a
// non-integer or non-uniform ratio (the same kind of miss
// HotspotListBoundaryShape's own doc comment states for its sibling
// sankey route), and a `data.axes.y` swap between two names neither of
// which this plan's own file-level admission reaches. It also never
// composes with TeamRepoSubsetShape the way SankeyRepoFanoutShape/
// HotspotListBoundaryShape do (compare.go's own repoMultiplierCovered):
// a team-scoped hotspot_risk request where BOTH this mechanism and the
// team-repo-subset mechanism touch the SAME cell in the SAME comparison
// is outside this shape's current scope.
//
// PROVENANCE LIMIT, shared with rule 1 (repoFanoutIntegerMultiplier) and
// rule 4, both pre-existing: an integer ratio corroborated by two
// independent shared cells, and a leaver/entrant pair each sitting at or
// under the other side's own floor, are STRUCTURAL evidence consistent
// with the declared repos-join mechanism -- they are not, and cannot be,
// proof that THIS SPECIFIC candidate value was produced by that
// mechanism rather than by an unrelated regression that happens to
// produce the identical numbers. Rule 6b never raises or lowers this
// bar: it only ever propagates a comparison rule 1/4 has ALREADY
// admitted to the one further axis-position consequence that
// admission's own arithmetic implies (an untouched neighbour's own rank
// moving because the admitted leaver/entrant's rank moved past it) --
// see TestHeatmapCellBoundaryShape_TwoSidedAxisIdentityAdmitsACorroboratedLeaverEntrantRankShift.
// A comparison rule 1/4 does not admit gets no help from rule 6b either;
// this shape's only defense against an unrelated regression sharing the
// same numbers is rule 1's own two-independent-cells requirement, the
// same defense every shape built on repoFanoutIntegerMultiplier in this
// package already carries.
type HeatmapCellBoundaryShape struct {
	// CellsListPath is the dotted, index-free path to the cells LIST
	// itself, e.g. "data.cells".
	CellsListPath string
	// CellKeyFields are the SAME KeyFields the paired OrderInsensitiveList
	// declaration for CellsListPath uses, in the SAME order, e.g.
	// []string{"x", "y"}.
	CellKeyFields []string
	// FileField names which of CellKeyFields' own object fields carries
	// the file identity ("<repo>:<path>") a repository name is read from
	// -- declared here, not inferred from position, e.g. "y".
	FileField string
	// CellValuePath is the leaf path findings carry for a cell value
	// difference -- must equal one of the defect's own Paths entries,
	// e.g. "data.cells.value".
	CellValuePath string
	// AxisListPath is the dotted, index-free path to the file-name axis
	// LIST itself, e.g. "data.axes.y".
	AxisListPath string
	// Limit is the route's own fixed top-N file count (never a
	// per-request override -- see the type doc comment's rule 3).
	Limit int
}

// heatmapCellRow is one decoded cell's projection: its own pairing key
// (CellKeyFields joined, the SAME format compareListByKey embeds in a
// Finding's own Detail), the file it belongs to, and its own value.
type heatmapCellRow struct {
	key   string
	file  string
	value float64
}

// heatmapCellBoundaryPlan is one comparison's fully-evaluated admission
// decision, built once per defect from the two decoded `data.cells`/
// `data.axes.y` values.
type heatmapCellBoundaryPlan struct {
	shape *HeatmapCellBoundaryShape
	valid bool
	// admittedBaselineOnlyCells/admittedCandidateOnlyCells hold every
	// admitted PRESENCE cell key (rule 5).
	admittedBaselineOnlyCells  map[string]bool
	admittedCandidateOnlyCells map[string]bool
	// admittedSharedValueCells holds every shared-key cell whose own
	// value difference equals its repository's verified k exactly (rule
	// 2).
	admittedSharedValueCells map[string]bool
	// admittedFileNames is the set of file names (the y-part of a cell
	// key) rule 6's axis admission reads: every file rule 4 or rule 2
	// admitted, regardless of which side it belongs to.
	admittedFileNames map[string]bool
	// axisBaseline/axisCandidate are the decoded data.axes.y arrays
	// themselves, kept on the plan so admits() can resolve a positional
	// axis finding's own two names directly, rather than re-parsing them
	// out of the finding's own Detail string.
	axisBaseline, axisCandidate []string
	// axisIdentityHolds is rule 6b's own whole-comparison verdict: the
	// union's t(f)/b(f) groupings are the identical partition, and both
	// tie-group-aware identities reproduced their own target axis EXACTLY.
	// true admits every data.axes.y[N] finding unconditionally; false
	// (the default) leaves axis admission on rule 6's own per-position
	// rule alone.
	axisIdentityHolds bool
}

// heatmapCellRowsSortedByKey returns cells' own rows ordered by their
// own pairing key (heatmapCellRow.key), ascending -- a deterministic,
// run-to-run reproducible iteration order over a map whose own Go
// iteration order is randomized per process.
func heatmapCellRowsSortedByKey(cells map[string]heatmapCellRow) []heatmapCellRow {
	rows := make([]heatmapCellRow, 0, len(cells))
	for _, row := range cells {
		rows = append(rows, row)
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].key < rows[j].key })
	return rows
}

// heatmapFileRepo reads a file_key's own repository ("<repo>:<path>" ->
// "<repo>"). A file_key carrying no ":" is its own (unrecognised)
// repository, which never agrees with anything below.
func heatmapFileRepo(file string) string {
	if idx := strings.IndexByte(file, ':'); idx >= 0 {
		return file[:idx]
	}
	return file
}

// decodeHeatmapCells reads shape.CellsListPath into a key -> row map, key
// built the SAME way orderInsensitiveKey does for shape.CellKeyFields, so
// a key computed here lines up directly with parseOrderInsensitiveDetailKey/
// parsePresenceDetailKey's own output. ok is false when the path does not
// resolve to a list of objects each carrying every declared key field
// plus FileField -- the caller treats that as "cannot evaluate the shape
// at all" rather than guessing a partial map.
func decodeHeatmapCells(root any, shape *HeatmapCellBoundaryShape) (map[string]heatmapCellRow, bool) {
	list, ok := listAtDottedPath(root, shape.CellsListPath)
	if !ok {
		return nil, false
	}
	out := make(map[string]heatmapCellRow, len(list))
	for _, element := range list {
		object, ok := element.(map[string]any)
		if !ok {
			return nil, false
		}
		parts := make([]string, len(shape.CellKeyFields))
		for i, field := range shape.CellKeyFields {
			raw, ok := object[field]
			if !ok {
				return nil, false
			}
			parts[i] = fmt.Sprintf("%v", raw)
		}
		file, ok := object[shape.FileField].(string)
		if !ok {
			return nil, false
		}
		value, ok := asFloat(object["value"])
		if !ok {
			continue
		}
		key := strings.Join(parts, "\x1f")
		out[key] = heatmapCellRow{key: key, file: file, value: value}
	}
	return out, true
}

// heatmapMinFileTotal returns the smallest total among files, excluding
// any name in exclude. ok is false when nothing is left to measure -- the
// same "no samples, no claim" default hotspotMin (hotspotlistboundary.go)
// applies for its own sankey route.
func heatmapMinFileTotal(totals map[string]float64, exclude []string) (float64, bool) {
	excluded := make(map[string]bool, len(exclude))
	for _, name := range exclude {
		excluded[name] = true
	}
	found := false
	var min float64
	for name, v := range totals {
		if excluded[name] {
			continue
		}
		if !found || v < min {
			min = v
			found = true
		}
	}
	return min, found
}

// heatmapFileSetDifference returns the set of file names present in a but
// not in b -- rule 3's own baseline-only/candidate-only file sets, one
// call per direction. ok is guarded directly against a's own length,
// immediately before the range that reads it: an empty a returns nil
// with no iteration at all, the same explicit, adjacent-to-the-loop
// invariant every map range in this file states rather than leaving it
// to a caller's own, more distant guard.
func heatmapFileSetDifference(a, b map[string]float64) []string {
	if len(a) == 0 {
		return nil
	}
	var out []string
	for file := range a {
		if _, ok := b[file]; !ok {
			out = append(out, file)
		}
	}
	return out
}

// buildHeatmapCellBoundaryPlan evaluates every rule HeatmapCellBoundaryShape
// documents against one comparison's decoded baseline/candidate `data`
// values.
func buildHeatmapCellBoundaryPlan(shape *HeatmapCellBoundaryShape, baselineData, candidateData any) *heatmapCellBoundaryPlan {
	plan := &heatmapCellBoundaryPlan{
		shape:                      shape,
		admittedBaselineOnlyCells:  map[string]bool{},
		admittedCandidateOnlyCells: map[string]bool{},
		admittedSharedValueCells:   map[string]bool{},
		admittedFileNames:          map[string]bool{},
	}

	baseCells, ok1 := decodeHeatmapCells(baselineData, shape)
	candCells, ok2 := decodeHeatmapCells(candidateData, shape)
	axisBase, ok3 := stringListAtDottedPath(baselineData, shape.AxisListPath)
	axisCand, ok4 := stringListAtDottedPath(candidateData, shape.AxisListPath)
	if !ok1 || !ok2 || !ok3 || !ok4 {
		return plan
	}
	plan.axisBaseline = axisBase
	plan.axisCandidate = axisCand

	// Both totals accumulate over baseCells/candCells' own KEY-SORTED
	// order, never raw map iteration: Go's own map iteration order is
	// randomized per run, and floating-point addition is not
	// associative, so summing the SAME cell values in two different
	// orders can differ in the LAST bit. Rules 1-5 below only ever
	// compare these totals with a tolerance (repoFanoutFloatsWithinTolerance),
	// which absorbs that noise invisibly, but rule 6b's own tie-group
	// partitioning needs an EXACT, run-to-run REPRODUCIBLE total -- the
	// same premise HeatmapAxisTieGroupShape's own cross-leg equality
	// check already carries (heatmapaxistiegroup.go's own
	// heatmapSumTotalsByName has the identical latent exposure, unfixed
	// here since this file owns baseFileTotal/candFileTotal, not that
	// one's own re-derivation).
	baseFileTotal := map[string]float64{}
	baseFileRepo := map[string]string{}
	for _, row := range heatmapCellRowsSortedByKey(baseCells) {
		baseFileTotal[row.file] += row.value
		baseFileRepo[row.file] = heatmapFileRepo(row.file)
	}
	candFileTotal := map[string]float64{}
	for _, row := range heatmapCellRowsSortedByKey(candCells) {
		candFileTotal[row.file] += row.value
	}

	// Rule 3: both reconstructed file lists exactly Limit long, and Limit
	// itself positive -- the route's own top-N bound is never zero or
	// negative for any admissible request this package declares, and
	// stating that directly here (rather than leaving it implicit in
	// Limit's own declared value, many lines away from where it is used)
	// is what tells a reader, human or automated, that
	// heatmapFileSetDifference's own loop below can never range over an
	// empty map: the equality check just above already pins each map's
	// cardinality to Limit, and Limit is pinned positive right here.
	if shape.Limit <= 0 || len(baseFileTotal) != shape.Limit || len(candFileTotal) != shape.Limit {
		return plan
	}

	baselineOnlyFiles := heatmapFileSetDifference(baseFileTotal, candFileTotal)
	candidateOnlyFiles := heatmapFileSetDifference(candFileTotal, baseFileTotal)
	if len(baselineOnlyFiles) == 0 || len(baselineOnlyFiles) != len(candidateOnlyFiles) {
		return plan
	}
	sort.Strings(baselineOnlyFiles)
	sort.Strings(candidateOnlyFiles)
	plan.valid = true

	// Rule 1: per-repository k, from AT LEAST TWO agreeing shared cells.
	repoKs := map[string][]int{}
	for key, candRow := range candCells {
		baseRow, ok := baseCells[key]
		if !ok || baseRow.value == candRow.value {
			continue
		}
		if k, ok := repoFanoutIntegerMultiplier(baseRow.value, candRow.value); ok {
			repo := heatmapFileRepo(candRow.file)
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

	// Rule 2: shared-key value admission.
	for key, candRow := range candCells {
		baseRow, ok := baseCells[key]
		if !ok || baseRow.value == candRow.value {
			continue
		}
		k, ok := repoMultiplier[heatmapFileRepo(candRow.file)]
		if !ok {
			continue
		}
		if repoFanoutFloatsWithinTolerance(baseRow.value, candRow.value*float64(k)) {
			plan.admittedSharedValueCells[key] = true
			plan.admittedFileNames[candRow.file] = true
		}
	}

	// Rule 4: entrant/leaver by FILE total.
	candMin, hasCandMin := heatmapMinFileTotal(candFileTotal, nil)
	admittedLeavers := map[string]bool{}
	if hasCandMin {
		for _, file := range baselineOnlyFiles {
			k, ok := repoMultiplier[baseFileRepo[file]]
			if !ok {
				continue
			}
			if baseFileTotal[file]/float64(k) <= candMin {
				admittedLeavers[file] = true
			}
		}
	}
	if len(admittedLeavers) > 0 {
		floor, hasFloor := heatmapMinFileTotal(baseFileTotal, baselineOnlyFiles)
		if hasFloor {
			sortedCandidateOnly := append([]string(nil), candidateOnlyFiles...)
			sort.Slice(sortedCandidateOnly, func(i, j int) bool {
				return candFileTotal[sortedCandidateOnly[i]] < candFileTotal[sortedCandidateOnly[j]]
			})
			admitted := 0
			for _, file := range sortedCandidateOnly {
				if admitted >= len(admittedLeavers) {
					break
				}
				if candFileTotal[file] > floor {
					break
				}
				plan.admittedFileNames[file] = true
				for key, row := range candCells {
					if row.file == file {
						plan.admittedCandidateOnlyCells[key] = true
					}
				}
				admitted++
			}
		}
	}
	for file := range admittedLeavers {
		plan.admittedFileNames[file] = true
		for key, row := range baseCells {
			if row.file == file {
				plan.admittedBaselineOnlyCells[key] = true
			}
		}
	}

	// Rule 6b: the two-sided whole-list identity -- see the type doc
	// comment's own derivation. Built over the UNION of baseFileTotal's
	// and candFileTotal's own file sets, independent of which files rules
	// 3/4 happened to admit: a tie or a reconstruction mismatch here
	// refuses rule 6b outright and axis admission falls back to rule 6's
	// own per-position rule, already fully evaluated above.
	plan.axisIdentityHolds = heatmapAxisTwoSidedIdentityHolds(
		baseFileTotal, candFileTotal, repoMultiplier, axisBase, axisCand, shape.Limit,
	)

	return plan
}

// heatmapAxisRow is one union file's own true total (identity (i)'s own
// grouping/sort key) alongside its own repository-reinflated total
// (identity (ii)'s own grouping/sort key) -- computed once per file, so
// both identities group and sort from the SAME pair of numbers rather
// than two independent passes that could drift apart under
// floating-point rounding.
type heatmapAxisRow struct {
	file    string
	trueKey float64 // identity (i)'s own key: t(f).
	baseKey float64 // identity (ii)'s own key: t(f) * k (or t(f)).
}

// heatmapAxisTwoSidedIdentityHolds evaluates rule 6b in full: it builds
// t(f)/b(f) for the union of baseFileTotal's and candFileTotal's own
// file sets (see the type doc comment's own definitions) and reports
// whether identity (i) (candidate, grouped and sorted by t(f)) and
// identity (ii) (baseline, grouped and sorted by b(f)) EACH
// independently hold -- see the type doc comment's own F5 note for why
// the two groupings are never required to be the identical partition:
// each identity checks its OWN target axis against its OWN key's own
// grouping, and nothing about that check depends on what the OTHER
// key's own grouping happens to look like.
func heatmapAxisTwoSidedIdentityHolds(
	baseFileTotal, candFileTotal map[string]float64,
	repoMultiplier map[string]int,
	axisBaseline, axisCandidate []string,
	limit int,
) bool {
	// limit <= 0 is already refused by this plan's own rule 3 (the
	// shape.Limit <= 0 branch, evaluated before this function is ever
	// called from buildHeatmapCellBoundaryPlan) -- kept here anyway as
	// this function's own explicit precondition, since it is a
	// substantial, independently-reasoned algorithm, not a trivial
	// private helper whose every precondition is safe to assume from a
	// single call site. len(axisBaseline)/len(axisCandidate) != limit is
	// NOT similarly shadowed: nothing else in this file requires the
	// route's own data.axes.y array to carry exactly Limit entries, so a
	// response whose axis array length disagrees with its own cell-total
	// count is a genuine, reachable case this check refuses.
	if limit <= 0 || len(axisBaseline) != limit || len(axisCandidate) != limit {
		return false
	}

	union := make(map[string]bool, len(baseFileTotal)+len(candFileTotal))
	if len(baseFileTotal) > 0 {
		for file := range baseFileTotal {
			union[file] = true
		}
	}
	if len(candFileTotal) > 0 {
		for file := range candFileTotal {
			union[file] = true
		}
	}

	rows := make([]heatmapAxisRow, 0, len(union))
	if len(union) > 0 {
		for file := range union {
			var trueValue float64
			if value, onCandidate := candFileTotal[file]; onCandidate {
				trueValue = value
			} else if k, ok := repoMultiplier[heatmapFileRepo(file)]; ok {
				trueValue = baseFileTotal[file] / float64(k)
			} else {
				trueValue = baseFileTotal[file]
			}
			baseValue := trueValue
			if k, ok := repoMultiplier[heatmapFileRepo(file)]; ok {
				baseValue = trueValue * float64(k)
			}
			rows = append(rows, heatmapAxisRow{file: file, trueKey: trueValue, baseKey: baseValue})
		}
	}
	// This is already shadowed by the SAME rule-3 guarantee the top-level
	// limit check above states: len(baseFileTotal) == len(candFileTotal)
	// == limit, so the union (rows) can never be SHORTER than limit --
	// kept for the same explicit-precondition reason.
	if len(rows) < limit {
		return false
	}

	trueGroups := heatmapAxisGroupByKeyDesc(rows, func(r heatmapAxisRow) float64 { return r.trueKey })
	if !heatmapAxisIdentityICandidateMatches(trueGroups, axisCandidate, limit) {
		return false
	}

	baseGroups := heatmapAxisGroupByKeyDesc(rows, func(r heatmapAxisRow) float64 { return r.baseKey })
	return heatmapAxisIdentityIIBaselineMatches(baseGroups, axisBaseline, limit)
}

// heatmapAxisGroupByKeyDesc sorts rows by key descending (stable) and
// splits the result into contiguous runs of EXACTLY equal key value --
// each run is one tie group, in descending-key order.
func heatmapAxisGroupByKeyDesc(rows []heatmapAxisRow, key func(heatmapAxisRow) float64) [][]string {
	sorted := append([]heatmapAxisRow(nil), rows...)
	sort.SliceStable(sorted, func(i, j int) bool { return key(sorted[i]) > key(sorted[j]) })
	var groups [][]string
	for i := 0; i < len(sorted); {
		j := i + 1
		for j < len(sorted) && key(sorted[j]) == key(sorted[i]) {
			j++
		}
		group := make([]string, 0, j-i)
		for _, row := range sorted[i:j] {
			group = append(group, row.file)
		}
		groups = append(groups, group)
		i = j
	}
	return groups
}

// heatmapAxisIdentityICandidateMatches walks trueGroups (already in
// t(f)-descending order) accumulating position count; a group straddling
// the Limit cut refuses outright. Otherwise the groups fully inside the
// cut, each sorted name-ascending internally (this port's own
// deterministic tiebreak), concatenate to EXACTLY Limit names, compared
// byte-for-byte to axisCandidate.
func heatmapAxisIdentityICandidateMatches(trueGroups [][]string, axisCandidate []string, limit int) bool {
	// A group straddling the Limit cut is NOT refused with its own early
	// exit here: this loop always accumulates a group's own members
	// WHOLE whenever pos < limit, so a straddling group (pos < limit,
	// end > limit) always overshoots -- len(prefix) ends up STRICTLY
	// GREATER than limit, caught by the length check right below the
	// loop. No input reaches an early exit at this exact point that the
	// length check does not already reach -- proven by direct mutation.
	prefix := make([]string, 0, limit)
	pos := 0
	for _, group := range trueGroups {
		if pos < limit {
			sortedGroup := append([]string(nil), group...)
			sort.Strings(sortedGroup)
			prefix = append(prefix, sortedGroup...)
		}
		pos += len(group)
		if pos >= limit {
			break
		}
	}
	if len(prefix) != limit {
		return false
	}
	for i := 0; i < limit; i++ {
		if prefix[i] != axisCandidate[i] {
			return false
		}
	}
	return true
}

// heatmapAxisIdentityIIBaselineMatches walks baseGroups (already in
// b(f)-descending order) the same way: a straddling group refuses
// outright. Otherwise, for every group fully inside the cut, the SAME
// contiguous position range in axisBaseline must hold EXACTLY that
// group's own file set -- any order inside the range, since the
// reference's own tiebreak among a genuine tie is unverifiable.
func heatmapAxisIdentityIIBaselineMatches(baseGroups [][]string, axisBaseline []string, limit int) bool {
	// A group straddling the Limit cut is NOT refused with its own early
	// exit here either: the caller already guarantees len(axisBaseline)
	// == limit, so "end > limit" (a straddle) and "end > len(axisBaseline)"
	// right below are the IDENTICAL condition -- proven by direct
	// mutation, the same discipline identity (i)'s own sibling check
	// states.
	pos := 0
	for _, group := range baseGroups {
		end := pos + len(group)
		if pos < limit {
			if end > len(axisBaseline) {
				return false
			}
			if !heatmapNameSetsEqual(heatmapNameSet(axisBaseline[pos:end]), heatmapNameSet(group)) {
				return false
			}
		}
		pos = end
		if pos >= limit {
			break
		}
	}
	return pos >= limit
}

// admits reports whether one Finding is covered by this plan.
func (p *heatmapCellBoundaryPlan) admits(finding Finding) bool {
	if p == nil || !p.valid {
		return false
	}
	switch tieredPath(finding.Path) {
	case p.shape.CellValuePath:
		key, ok := parseOrderInsensitiveDetailKey(finding.Detail)
		if !ok {
			return false
		}
		return p.admittedSharedValueCells[key]
	case p.shape.CellsListPath:
		key, ok := parsePresenceDetailKey(finding.Detail)
		if !ok {
			return false
		}
		return p.admittedBaselineOnlyCells[key] || p.admittedCandidateOnlyCells[key]
	case p.shape.AxisListPath:
		idx, ok := edgeListIndex(finding.Path, p.shape.AxisListPath)
		if !ok || idx < 0 || idx >= len(p.axisBaseline) || idx >= len(p.axisCandidate) {
			return false
		}
		if p.axisIdentityHolds {
			return true
		}
		return p.admittedFileNames[p.axisBaseline[idx]] && p.admittedFileNames[p.axisCandidate[idx]]
	}
	return false
}

// stringListAtDottedPath reads a JSON list of plain strings at a dotted,
// index-free path under `data`. ok is false when the path does not
// resolve to a list of strings -- the caller treats that as "cannot
// evaluate the shape at all" rather than guessing a partial list.
func stringListAtDottedPath(root any, dottedPath string) ([]string, bool) {
	list, ok := listAtDottedPath(root, dottedPath)
	if !ok {
		return nil, false
	}
	out := make([]string, len(list))
	for i, element := range list {
		s, ok := element.(string)
		if !ok {
			return nil, false
		}
		out[i] = s
	}
	return out, true
}
