package goapiproof

import (
	"fmt"
	"sort"
	"testing"
)

// The heatmap axis admission invariant, enumerated through the real
// production Options and checked in BOTH directions at every differing
// axis position: a position is admitted exactly when (1) the candidate's
// whole axis is Go's own order of its totals (descending, ties by name;
// for a Limit list, the listed N are the top N of the true totals and
// sorted), (2) per-name values agree, or differ only by the declared
// mechanism, and (3) every position difference between the legs is
// explained by that mechanism.
//
// The oracle below is written from those three parts and the two
// mechanisms of record, from the cell values alone; it shares no code
// with the shapes or their gate (its own sort, ratio and tie-run
// helpers, prefixed invariant*):
//   - fan-out: every cell of every name is its candidate value times its
//     repository's integer k (constant per repository, some k >= 2), and
//     the baseline axis is a descending order of the baseline's own
//     totals, any order inside a tie; then every position is admitted;
//   - genuine tie: the candidate tie run holding the position has two or
//     more names, each untouched (every cell equal on both legs), and the
//     baseline holds the same name set at those positions; then that
//     run's positions are admitted.
func TestHeatmapAxisAdmissionInvariantEnumerated(t *testing.T) {
	for _, tc := range []struct {
		name  string
		opts  Options
		names func(n int) []string
	}{
		{"repo_touchpoints", heatmapRepoTouchpointsParity, invariantRepoNames},
		{"repo_touchpoints team scope", heatmapRepoTouchpointsTeamScopedParity, invariantRepoNames},
		{"hotspot_risk", heatmapHotspotRiskParity, invariantFileNames},
		{"hotspot_risk team scope", heatmapHotspotRiskTeamScopedParity, invariantFileNames},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cases, positions, admitted := 0, 0, 0
			check := func(names []string, cand, base map[string][2]float64, allBaselines bool) {
				candTotal, baseTotal := map[string]float64{}, map[string]float64{}
				for _, name := range names {
					candTotal[name] = cand[name][0] + cand[name][1]
					baseTotal[name] = base[name][0] + base[name][1]
				}
				goAxis := invariantGoOrder(names, candTotal)
				fanout := invariantFanoutExplained(names, cand, base)
				untouched := func(name string) bool { return cand[name] == base[name] }
				baselines := invariantPermutations(names)
				if !allBaselines {
					var python [][]string
					for _, order := range baselines {
						if invariantPythonOrder(order, baseTotal) {
							python = append(python, order)
						}
					}
					reversed := make([]string, len(goAxis))
					for i := range goAxis {
						reversed[i] = goAxis[len(goAxis)-1-i]
					}
					baselines = append(python, reversed)
				}
				var baseCells, candCells []string
				for _, name := range names {
					for w, week := range []string{"2026-08-02", "2026-08-09"} {
						baseCells = append(baseCells, heatmapBoundaryCell(week, name, base[name][w]))
						candCells = append(candCells, heatmapBoundaryCell(week, name, cand[name][w]))
					}
				}
				for _, candAxis := range invariantPermutations(names) {
					for _, baseAxis := range baselines {
						cases++
						orderOK := invariantStringsEqual(candAxis, goAxis) && invariantPythonOrder(baseAxis, baseTotal)
						baseline, candidate := heatmapAxisSnapshotsFromCells(t, baseCells, candCells, baseAxis, candAxis)
						for idx := range candAxis {
							if candAxis[idx] == baseAxis[idx] {
								continue
							}
							positions++
							want := orderOK && (fanout || invariantTieRunExplained(candAxis, baseAxis, candTotal, untouched, idx))
							got := len(heatmapAxisAdmitters(tc.opts, baseline, candidate, idx)) > 0
							if got {
								admitted++
							}
							if got != want {
								t.Fatalf("cand=%v base=%v cand cells %v base cells %v: position %d admitted=%t, invariant says %t (admitters %v)",
									candAxis, baseAxis, cand, base, idx, got, want, heatmapAxisAdmitters(tc.opts, baseline, candidate, idx))
							}
						}
					}
				}
			}
			three, four := tc.names(3), tc.names(4)
			// Three names x two cells: candidate cells in {1, 2}, k in {1, 2}
			// per name, and a change on the first name's baseline cells:
			// none, +1 on one cell (total moves), or its two cells swapped
			// (total kept, cells unexplained when they differ).
			for cellBits := 0; cellBits < 64; cellBits++ {
				for kBits := 0; kBits < 8; kBits++ {
					for _, change := range []string{"none", "plus", "swap"} {
						cand, base := map[string][2]float64{}, map[string][2]float64{}
						for i, name := range three {
							k := 1 + float64((kBits>>i)&1)
							var c [2]float64
							for w := 0; w < 2; w++ {
								c[w] = 1 + float64((cellBits>>(2*i+w))&1)
							}
							cand[name], base[name] = c, [2]float64{c[0] * k, c[1] * k}
						}
						b := base[three[0]]
						switch change {
						case "plus":
							b[0]++
						case "swap":
							b[0], b[1] = b[1], b[0]
						}
						base[three[0]] = b
						check(three, cand, base, true)
					}
				}
			}
			// Four names x two cells (several tie runs at once): candidate
			// values in {1, 2} per cell pair, k all 1 or 2 on the first name,
			// the same three changes; every candidate permutation against
			// every baseline order Python can produce plus the reverse of
			// Go's order.
			for valueBits := 0; valueBits < 16; valueBits++ {
				for _, firstFanned := range []bool{false, true} {
					for _, change := range []string{"none", "plus", "swap"} {
						cand, base := map[string][2]float64{}, map[string][2]float64{}
						for i, name := range four {
							v := 1 + float64((valueBits>>i)&1)
							c := [2]float64{v, 3 - v}
							k := 1.0
							if i == 0 && firstFanned {
								k = 2
							}
							cand[name], base[name] = c, [2]float64{c[0] * k, c[1] * k}
						}
						b := base[four[0]]
						switch change {
						case "plus":
							b[0]++
						case "swap":
							b[0], b[1] = b[1], b[0]
						}
						base[four[0]] = b
						check(four, cand, base, false)
					}
				}
			}
			t.Logf("%d cases, %d differing positions, %d admitted", cases, positions, admitted)
			if cases < 64*8*3*36 {
				t.Fatalf("enumerated %d cases, want at least %d", cases, 64*8*3*36)
			}
		})
	}
}

// A Limit-bounded hotspot_risk list crossing its boundary, through both
// hotspot_risk Options, every differing position checked against the
// oracle in both directions: 17 untouched padding files; repo-a's
// one.go/two.go fanned at k=2 (or one of them changed off the ratio);
// a repo-a leaver and a repo-x entrant at several true totals, including
// ones that tie the cut or belong above it; every candidate order and
// every baseline order of the bottom three names.
func TestHeatmapAxisAdmissionInvariantOnALimitCrossing(t *testing.T) {
	const limit = 20
	var pads []heatmapAxisEntry
	var padAxis []string
	for i := 0; i < 17; i++ {
		name := fmt.Sprintf("ops:f%02d.go", i)
		pads = append(pads, heatmapAxisEntry{name, float64(100 - i), float64(100 - i)})
		padAxis = append(padAxis, name)
	}
	cases, positions, admitted := 0, 0, 0
	for _, f1 := range []float64{20, 22} {
		for _, f2 := range []float64{20, 22} {
			for _, offRatio := range []bool{false, true} {
				for _, leaverBase := range []float64{6, 40, 44, 50} {
					for _, entrant := range []float64{3, 11, 20, 22} {
						oneBase := 2 * f1
						if offRatio {
							oneBase = 2*f1 + 1
						}
						shared := append(append([]heatmapAxisEntry{}, pads...),
							heatmapAxisEntry{"repo-a:one.go", oneBase, f1}, heatmapAxisEntry{"repo-a:two.go", 2 * f2, f2})
						// The oracle's own view of the two legs.
						k := 2.0
						if offRatio {
							k = 0
						}
						trueTotal := map[string]float64{"repo-a:one.go": f1, "repo-a:two.go": f2, "repo-x:entrant.go": entrant}
						baseTotal := map[string]float64{"repo-a:one.go": oneBase, "repo-a:two.go": 2 * f2, "repo-a:leaver.go": leaverBase, "repo-x:entrant.go": entrant}
						if k > 0 {
							trueTotal["repo-a:leaver.go"] = leaverBase / k
						}
						for _, pad := range pads {
							trueTotal[pad.file], baseTotal[pad.file] = pad.cand, pad.base
						}
						goTop, goOK := invariantTopN(trueTotal, limit, true)
						pyTop, pyOK := invariantTopN(baseTotal, limit, false)
						bottom := []string{"repo-a:one.go", "repo-a:two.go", "repo-x:entrant.go"}
						baseBottom := []string{"repo-a:one.go", "repo-a:two.go", "repo-a:leaver.go"}
						for _, candOrder := range invariantPermutations(bottom) {
							for _, baseOrder := range invariantPermutations(baseBottom) {
								candAxis := append(append([]string{}, padAxis...), candOrder...)
								baseAxis := append(append([]string{}, padAxis...), baseOrder...)
								baseline, candidate := heatmapAxisEntrantLeaverFixture(t, shared, "repo-a:leaver.go", leaverBase, "repo-x:entrant.go", entrant, baseAxis, candAxis)
								cases++
								want := k > 0 && goOK && pyOK &&
									invariantStringsEqual(candAxis, goTop) &&
									invariantSameSet(baseAxis, pyTop) && invariantPythonOrder(baseAxis, baseTotal)
								for idx := range candAxis {
									if candAxis[idx] == baseAxis[idx] {
										continue
									}
									positions++
									for _, opts := range []Options{heatmapHotspotRiskParity, heatmapHotspotRiskTeamScopedParity} {
										got := len(heatmapAxisAdmitters(opts, baseline, candidate, idx)) > 0
										if got && opts.BaselineDefects != nil {
											admitted++
										}
										if got != want {
											t.Fatalf("one=%v/%v two=%v leaver base=%v entrant=%v cand bottom %v base bottom %v: position %d admitted=%t, invariant says %t (Go top ok=%t, Python top ok=%t)",
												oneBase, f1, f2, leaverBase, entrant, candOrder, baseOrder, idx, got, want, goOK, pyOK)
										}
									}
								}
							}
						}
					}
				}
			}
		}
	}
	t.Logf("%d cases, %d differing positions, %d admissions", cases, positions, admitted)
	if cases != 2*2*2*4*4*36 {
		t.Fatalf("enumerated %d cases, want %d", cases, 2*2*2*4*4*36)
	}
}

func invariantRepoNames(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("org/%c", 'a'+i)
	}
	return out
}

func invariantFileNames(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("o/r%c:%c.go", 'a'+i, 'a'+i)
	}
	return out
}

func invariantPermutations(names []string) [][]string {
	if len(names) <= 1 {
		return [][]string{append([]string(nil), names...)}
	}
	var out [][]string
	for i := range names {
		rest := append(append([]string{}, names[:i]...), names[i+1:]...)
		for _, tail := range invariantPermutations(rest) {
			out = append(out, append([]string{names[i]}, tail...))
		}
	}
	return out
}

func invariantStringsEqual(a, b []string) bool {
	return fmt.Sprint(a) == fmt.Sprint(b)
}

func invariantSameSet(a, b []string) bool {
	x, y := append([]string(nil), a...), append([]string(nil), b...)
	sort.Strings(x)
	sort.Strings(y)
	return invariantStringsEqual(x, y)
}

// invariantGoOrder is axisOrder's rule restated: total descending, ties
// by name ascending.
func invariantGoOrder(names []string, totals map[string]float64) []string {
	out := append([]string(nil), names...)
	sort.Slice(out, func(i, j int) bool {
		if totals[out[i]] != totals[out[j]] {
			return totals[out[i]] > totals[out[j]]
		}
		return out[i] < out[j]
	})
	return out
}

// invariantTopN is a Limit query's top N of totals: Go's order when
// byName, and ok=false when a tie straddles the cut (the reference
// plane's own choice inside such a tie is not visible, and this port's
// candidate is not claimed there).
func invariantTopN(totals map[string]float64, n int, byName bool) ([]string, bool) {
	var names []string
	for name := range totals {
		names = append(names, name)
	}
	ordered := invariantGoOrder(names, totals)
	if len(ordered) > n && totals[ordered[n-1]] == totals[ordered[n]] {
		return nil, false
	}
	if len(ordered) > n {
		ordered = ordered[:n]
	}
	return ordered, true
}

// invariantFanoutExplained reports whether every name's baseline cells
// are its candidate cells times one integer k >= 1 per name, with some
// k >= 2.
func invariantFanoutExplained(names []string, cand, base map[string][2]float64) bool {
	any := false
	for _, name := range names {
		c, b := cand[name], base[name]
		if c[0] == 0 || c[1] == 0 {
			return false
		}
		k0, k1 := b[0]/c[0], b[1]/c[1]
		if k0 != k1 || k0 < 1 || k0 != float64(int(k0)) {
			return false
		}
		if k0 >= 2 {
			any = true
		}
	}
	return any
}

// invariantTieRunExplained reports whether the candidate tie run holding
// idx has two or more names, each untouched, at the same baseline
// positions as a set.
func invariantTieRunExplained(candAxis, baseAxis []string, candTotal map[string]float64, untouched func(string) bool, idx int) bool {
	lo, hi := idx, idx+1
	for lo > 0 && candTotal[candAxis[lo-1]] == candTotal[candAxis[idx]] {
		lo--
	}
	for hi < len(candAxis) && candTotal[candAxis[hi]] == candTotal[candAxis[idx]] {
		hi++
	}
	if hi-lo < 2 {
		return false
	}
	set := map[string]bool{}
	for i := lo; i < hi; i++ {
		if !untouched(candAxis[i]) {
			return false
		}
		set[candAxis[i]] = true
	}
	for i := lo; i < hi; i++ {
		if !set[baseAxis[i]] {
			return false
		}
	}
	return true
}

// invariantPythonOrder reports whether axis is a descending order of
// totals with any order inside a tie.
func invariantPythonOrder(axis []string, totals map[string]float64) bool {
	for i := 1; i < len(axis); i++ {
		if totals[axis[i]] > totals[axis[i-1]] {
			return false
		}
	}
	return true
}

// A name is untouched only when its cell keys match on both legs and
// every value agrees; an extra zero-valued cell keeps the total and still
// touches the name.
func TestHeatmapUntouchedNames(t *testing.T) {
	cells := func(entries ...heatmapCellRow) map[string]heatmapCellRow {
		out := map[string]heatmapCellRow{}
		for _, row := range entries {
			out[row.key] = row
		}
		return out
	}
	row := func(key, file string, value float64) heatmapCellRow {
		return heatmapCellRow{key: key, file: file, value: value}
	}
	base := cells(row("w1|a", "a", 2), row("w1|b", "b", 1), row("w2|b", "b", 1), row("w1|c", "c", 3), row("w1|d", "d", 4))
	cand := cells(row("w1|a", "a", 2), row("w1|b", "b", 2), row("w1|c", "c", 3), row("w2|c", "c", 0), row("w1|d", "d", 4.000000000001))
	got := heatmapUntouchedNames(base, cand)
	for name, want := range map[string]bool{"a": true, "b": false, "c": false, "d": true} {
		if got[name] != want {
			t.Errorf("untouched[%s] = %t, want %t", name, got[name], want)
		}
	}
	if len(heatmapUntouchedNames(map[string]heatmapCellRow{}, cand)) != 0 {
		t.Error("an empty baseline leaves no untouched name")
	}
}
