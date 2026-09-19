package goapiproof

import (
	"fmt"
	"sort"
	"testing"
)

// This file pins HeatmapAxisTieGroupShape's whole-axis invariant
// (heatmapaxistiegroup.go): an axis difference is admitted only when the
// two legs list the same names, the baseline axis is a weakly descending
// order of its own totals, and the candidate axis is the deterministic
// order (total descending, name ascending) of its own totals.

// heatmapAxisOrderRealPairs are production captures, one per admission
// path the invariant opens beyond a pure tie.
var heatmapAxisOrderRealPairs = []struct {
	name         string
	opts         Options
	baselinePath string
	candPath     string
}{
	{"repo_touchpoints team: repos-join fan-out moves the tied totals", heatmapRepoTouchpointsTeamScopedParity,
		"testdata/heatmap_axis_order_repotouchpoints_fanouttie_baseline_234f5d43.json",
		"testdata/heatmap_axis_order_repotouchpoints_fanouttie_candidate_a0c7c3ee.json"},
	{"repo_touchpoints team: fan-out swap next to an unrelated baseline tie", heatmapRepoTouchpointsTeamScopedParity,
		"testdata/heatmap_axis_order_repotouchpoints_fanoutbaselinetie_baseline_bf0891c4.json",
		"testdata/heatmap_axis_order_repotouchpoints_fanoutbaselinetie_candidate_f1e02a20.json"},
	{"hotspot_risk team: tied files whose totals both shift by the same amount", heatmapHotspotRiskTeamScopedParity,
		"testdata/heatmap_axis_order_hotspot_shiftedtie_baseline_6b39597a.json",
		"testdata/heatmap_axis_order_hotspot_shiftedtie_candidate_cc963236.json"},
}

func heatmapAxisOrderWithoutAxisDefects(opts Options) Options {
	out := opts
	out.BaselineDefects = nil
	for _, d := range opts.BaselineDefects {
		if d.HeatmapAxisTieGroupShape == nil && d.HeatmapAxisRepoOrderShape == nil && d.HeatmapCellBoundaryShape == nil {
			out.BaselineDefects = append(out.BaselineDefects, d)
		}
	}
	return out
}

func TestHeatmapAxisTieGroupShape_RealCapturedPairsAdmitEveryAxisPosition(t *testing.T) {
	for _, tc := range heatmapAxisOrderRealPairs {
		t.Run(tc.name, func(t *testing.T) {
			baseline := heatmapAxisOrderSnapshotFromFile(t, tc.baselinePath)
			candidate := heatmapAxisOrderSnapshotFromFile(t, tc.candPath)

			got := Compare(baseline, candidate, tc.opts)
			if got.DifferencesOutsideBaselineDefect != 0 {
				t.Fatalf("outside = %d, want 0: %+v", got.DifferencesOutsideBaselineDefect, got.Findings)
			}
			// The axis findings exist and only the axis declarations admit
			// them: without those declarations the same pair is outside.
			without := Compare(baseline, candidate, heatmapAxisOrderWithoutAxisDefects(tc.opts))
			if without.DifferencesOutsideBaselineDefect == 0 {
				t.Fatal("outside without the axis declarations = 0: the pair does not exercise them")
			}
		})
	}
}

// TestHeatmapAxisTieGroupShape_ACellDifferenceStaysJudgedByTheCellDeclarations:
// an axis reorder that follows from a candidate value ABOVE the baseline
// value (the direction the cell declaration never admits) is admitted on
// the axis and refused on the cell.
func TestHeatmapAxisTieGroupShape_ACellDifferenceStaysJudgedByTheCellDeclarations(t *testing.T) {
	baseCells := heatmapBoundaryCell("w1", "repoA", 4) + "," + heatmapBoundaryCell("w1", "repoB", 3)
	candCells := heatmapBoundaryCell("w1", "repoA", 4) + "," + heatmapBoundaryCell("w1", "repoB", 5)
	baseline := heatmapAxisTieGroupSnapshot(t, baseCells, []string{"repoA", "repoB"})
	candidate := heatmapAxisTieGroupSnapshot(t, candCells, []string{"repoB", "repoA"})

	got := Compare(baseline, candidate, heatmapRepoTouchpointsParity)
	if got.DifferencesOutsideBaselineDefect != 1 {
		t.Fatalf("outside = %d, want exactly 1 (the candidate-above-baseline cell): %+v", got.DifferencesOutsideBaselineDefect, got.Findings)
	}
	for _, f := range got.Findings {
		if f.Path == "$.data.axes.y[0]" || f.Path == "$.data.axes.y[1]" {
			continue
		}
		if f.Shape != ShapeValue {
			t.Fatalf("unexpected finding %+v", f)
		}
	}
}

// heatmapAxisOrderOracle restates the invariant without sharing code with
// the shape: sort.Slice by (total desc, name asc) for the candidate, a
// pairwise scan for the baseline.
func heatmapAxisOrderOracle(names []string, baseTotal, candTotal map[string]float64, axisBase, axisCand []string) bool {
	want := append([]string{}, names...)
	sort.Slice(want, func(i, j int) bool {
		if candTotal[want[i]] != candTotal[want[j]] {
			return candTotal[want[i]] > candTotal[want[j]]
		}
		return want[i] < want[j]
	})
	for i := range want {
		if axisCand[i] != want[i] {
			return false
		}
	}
	for i := 0; i < len(axisBase); i++ {
		for j := i + 1; j < len(axisBase); j++ {
			if baseTotal[axisBase[j]] > baseTotal[axisBase[i]] {
				return false
			}
		}
	}
	return true
}

func heatmapAxisOrderPermutations(names []string) [][]string {
	if len(names) <= 1 {
		return [][]string{append([]string{}, names...)}
	}
	var out [][]string
	for i := range names {
		rest := append(append([]string{}, names[:i]...), names[i+1:]...)
		for _, p := range heatmapAxisOrderPermutations(rest) {
			out = append(out, append([]string{names[i]}, p...))
		}
	}
	return out
}

// TestHeatmapAxisTieGroupShape_EnumerationMatchesTheInvariant runs every
// combination of: three names, each with a baseline and a candidate total
// drawn from {1, 2}, and every permutation of the three names as each
// leg's axis (8 x 8 x 6 x 6 = 2304 comparisons), and requires the plan's
// verdict to equal the independent oracle's.
func TestHeatmapAxisTieGroupShape_EnumerationMatchesTheInvariant(t *testing.T) {
	names := []string{"repoA", "repoB", "repoC"}
	values := []float64{1, 2}
	perms := heatmapAxisOrderPermutations(names)
	cellsFor := func(tot map[string]float64) string {
		out := ""
		for i, n := range names {
			if i > 0 {
				out += ","
			}
			out += heatmapBoundaryCell("w1", n, tot[n])
		}
		return out
	}
	total, valid := 0, 0
	for bm := 0; bm < 8; bm++ {
		for cm := 0; cm < 8; cm++ {
			baseTotal, candTotal := map[string]float64{}, map[string]float64{}
			for i, n := range names {
				baseTotal[n] = values[(bm>>i)&1]
				candTotal[n] = values[(cm>>i)&1]
			}
			baseCells, candCells := cellsFor(baseTotal), cellsFor(candTotal)
			for _, ab := range perms {
				for _, ac := range perms {
					plan := buildHeatmapAxisTieGroupPlan(heatmapAxisTieGroupTestShape(),
						heatmapAxisTieGroupBody(t, baseCells, ab), heatmapAxisTieGroupBody(t, candCells, ac))
					want := heatmapAxisOrderOracle(names, baseTotal, candTotal, ab, ac)
					if plan.valid != want {
						t.Fatalf("base %v axis %v, cand %v axis %v: plan.valid = %v, oracle = %v", baseTotal, ab, candTotal, ac, plan.valid, want)
					}
					total++
					if want {
						valid++
					}
				}
			}
		}
	}
	if total != 2304 {
		t.Fatalf("enumerated %d comparisons, want 2304", total)
	}
	if valid == 0 || valid == total {
		t.Fatalf("valid = %d of %d: the enumeration must contain both admitted and refused cells", valid, total)
	}
	t.Logf("enumerated %d comparisons, %d admitted, %d refused", total, valid, total-valid)
}

// TestHeatmapAxisTieGroupShape_StructuralRefusalsByName pins the
// structural cells of the decision table: each refuses the whole plan.
func TestHeatmapAxisTieGroupShape_StructuralRefusalsByName(t *testing.T) {
	two := heatmapBoundaryCell("w1", "repoA", 4) + "," + heatmapBoundaryCell("w1", "repoB", 4)
	three := two + "," + heatmapBoundaryCell("w1", "repoC", 1)
	cases := []struct {
		name                 string
		baseCells, candCells string
		axisBase, axisCand   []string
	}{
		{"empty axes", "", "", []string{}, []string{}},
		{"axis lengths differ", three, two, []string{"repoA", "repoB", "repoC"}, []string{"repoA", "repoB"}},
		{"name sets differ", three, three, []string{"repoA", "repoB", "repoC"}, []string{"repoA", "repoB", "repoZ"}},
		{"repeated name on baseline", two, two, []string{"repoA", "repoA"}, []string{"repoA", "repoB"}},
		{"repeated name on candidate", two, two, []string{"repoA", "repoB"}, []string{"repoB", "repoB"}},
		{"cells carry a name the axis omits", three, three, []string{"repoA", "repoB"}, []string{"repoA", "repoB"}},
		{"axis carries an empty name no cell can carry", two, two, []string{"repoA", ""}, []string{"repoA", ""}},
		{"axis names a repo with no cells", two, two, []string{"repoA", "repoZ"}, []string{"repoA", "repoZ"}},
		{"candidate cells name a repo the axis omits, same count", two, heatmapBoundaryCell("w1", "repoA", 4) + "," + heatmapBoundaryCell("w1", "repoZ", 4), []string{"repoA", "repoB"}, []string{"repoA", "repoB"}},
		{"baseline cells name a repo the axis omits, same count", heatmapBoundaryCell("w1", "repoA", 4) + "," + heatmapBoundaryCell("w1", "repoZ", 4), two, []string{"repoA", "repoB"}, []string{"repoA", "repoB"}},
	}
	for _, tc := range cases {
		t.Run(fmt.Sprint(tc.name), func(t *testing.T) {
			plan := buildHeatmapAxisTieGroupPlan(heatmapAxisTieGroupTestShape(),
				heatmapAxisTieGroupBody(t, tc.baseCells, tc.axisBase), heatmapAxisTieGroupBody(t, tc.candCells, tc.axisCand))
			if plan.valid {
				t.Fatal("plan must refuse")
			}
		})
	}
}

// TestHeatmapAxisTieGroupShape_AdmitsOnlyTheAxisPath: a valid plan covers
// data.axes.y positions and nothing else, whatever index the finding names.
func TestHeatmapAxisTieGroupShape_AdmitsOnlyTheAxisPath(t *testing.T) {
	cells := heatmapBoundaryCell("w1", "repoA", 4) + "," + heatmapBoundaryCell("w1", "repoB", 4)
	plan := buildHeatmapAxisTieGroupPlan(heatmapAxisTieGroupTestShape(),
		heatmapAxisTieGroupBody(t, cells, []string{"repoB", "repoA"}), heatmapAxisTieGroupBody(t, cells, []string{"repoA", "repoB"}))
	if !plan.valid {
		t.Fatal("plan must be valid")
	}
	for _, path := range []string{"$.data.axes.y[0]", "$.data.axes.y[1]"} {
		if !plan.admits(Finding{Path: path, Shape: ShapeValue}) {
			t.Errorf("%s must be admitted", path)
		}
	}
	for _, path := range []string{"$.data.cells[0].value", "$.data.axes.x[0]", "$.data.axes.y", "$.data.legend.unit"} {
		if plan.admits(Finding{Path: path, Shape: ShapeValue}) {
			t.Errorf("%s must stay outside: not a data.axes.y position", path)
		}
	}
}

// TestHeatmapAxisTieGroupShape_AnOutsideCellKeepsTheComparisonOutside runs
// the same 2304-comparison enumeration through Compare with the registered
// repo_touchpoints options: whenever any name's candidate value is above
// its baseline value (a direction no cell declaration admits), outside is
// at least 1 whatever the axis verdict is; and outside is 0 only when no
// candidate value is above its baseline value.
func TestHeatmapAxisTieGroupShape_AnOutsideCellKeepsTheComparisonOutside(t *testing.T) {
	names := []string{"repoA", "repoB", "repoC"}
	values := []float64{1, 2}
	perms := heatmapAxisOrderPermutations(names)
	cellsFor := func(tot map[string]float64) string {
		out := ""
		for i, n := range names {
			if i > 0 {
				out += ","
			}
			out += heatmapBoundaryCell("w1", n, tot[n])
		}
		return out
	}
	kept, total := 0, 0
	for bm := 0; bm < 8; bm++ {
		for cm := 0; cm < 8; cm++ {
			baseTotal, candTotal := map[string]float64{}, map[string]float64{}
			candAbove := false
			for i, n := range names {
				baseTotal[n] = values[(bm>>i)&1]
				candTotal[n] = values[(cm>>i)&1]
				if candTotal[n] > baseTotal[n] {
					candAbove = true
				}
			}
			for _, ab := range perms {
				for _, ac := range perms {
					got := Compare(heatmapAxisTieGroupSnapshot(t, cellsFor(baseTotal), ab),
						heatmapAxisTieGroupSnapshot(t, cellsFor(candTotal), ac), heatmapRepoTouchpointsParity)
					total++
					if candAbove {
						if got.DifferencesOutsideBaselineDefect == 0 {
							t.Fatalf("base %v %v cand %v %v: outside = 0 with a candidate value above its baseline", baseTotal, ab, candTotal, ac)
						}
						kept++
					}
				}
			}
		}
	}
	if total != 2304 || kept == 0 {
		t.Fatalf("enumerated %d comparisons, %d with an outside cell", total, kept)
	}
}
