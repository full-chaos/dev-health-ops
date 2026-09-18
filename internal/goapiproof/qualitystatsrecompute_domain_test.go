package goapiproof

import (
	"bytes"
	"encoding/json"
	"math"
	"os"
	"sort"
	"strings"
	"testing"
)

// TestQualityStatsShapes_InputDomain runs every input of both
// evidence-quality shapes across its domain against the real GET capture
// (large population) and pins, per cell, whether each shape admits the
// pair. A cell names the leg and the field it changes.
func TestQualityStatsShapes_InputDomain(t *testing.T) {
	type cell struct {
		name             string
		mutate           func(base, cand map[string]any)
		moments, drivers bool
	}
	set := func(key string, v any) func(_, cand map[string]any) {
		return func(_, cand map[string]any) { cand[key] = v }
	}
	del := func(key string) func(_, cand map[string]any) {
		return func(_, cand map[string]any) { delete(cand, key) }
	}
	band := func(key string, v any) func(_, cand map[string]any) {
		return func(_, cand map[string]any) { cand["band_counts"].(map[string]any)[key] = v }
	}
	n := func(s string) json.Number { return json.Number(s) }
	cells := []cell{
		{"canonical", func(_, _ map[string]any) {}, true, true},
		{"total absent", del("total"), false, false},
		{"total null", set("total", nil), false, false},
		{"total string", set("total", "1029"), false, false},
		{"total fractional", set("total", n("1029.5")), false, false},
		{"total -1", set("total", n("-1")), false, false},
		{"total 0 with bands", set("total", n("0")), false, false},
		{"total +1", set("total", n("1030")), false, true},
		{"band_counts absent", del("band_counts"), false, false},
		{"band_counts null", set("band_counts", nil), false, false},
		{"band_counts empty", set("band_counts", map[string]any{}), false, false},
		{"band_counts list", set("band_counts", []any{}), false, false},
		{"band key missing", func(_, cand map[string]any) { delete(cand["band_counts"].(map[string]any), "high") }, false, false},
		{"band key out of vocabulary", band("other", n("0")), false, false},
		{"band count string", band("high", "9"), false, false},
		{"band count fractional", band("high", n("9.5")), false, false},
		{"band count -1", band("high", n("-1")), false, false},
		{"band count +1 (sum above total)", band("high", n("10")), false, true},
		{"band count above baseline", func(_, cand map[string]any) {
			b := cand["band_counts"].(map[string]any)
			b["high"], b["moderate"] = n("12"), n("642")
		}, false, true},
		{"unknown 1", func(_, cand map[string]any) {
			b := cand["band_counts"].(map[string]any)
			b["unknown"], b["very_low"] = n("1"), n("333")
		}, false, true},
		{"mean absent", del("mean"), false, true},
		{"mean null", set("mean", nil), false, true},
		{"mean string", set("mean", "0.54"), false, false},
		{"mean above 1", set("mean", n("1.5")), false, true},
		{"mean below 0", set("mean", n("-0.1")), false, false},
		{"mean at the band interval's top", set("mean", n("0.6639")), false, true},
		{"stddev absent", del("stddev"), false, true},
		{"stddev null", set("stddev", nil), false, true},
		{"stddev string", set("stddev", "0.2"), false, false},
		{"stddev -0.2", set("stddev", n("-0.2")), false, true},
		{"stddev 0", set("stddev", n("0")), false, true},
		{"stddev 0.3 (crosses the 0.25 driver threshold)", set("stddev", n("0.3")), false, false},
		{"quality_drivers absent", del("quality_drivers"), true, false},
		{"quality_drivers null", set("quality_drivers", nil), false, false},
		{"quality_drivers string", set("quality_drivers", "weak_cross_links"), false, false},
		{"quality_drivers non-string element", set("quality_drivers", []any{n("1")}), false, false},
		{"quality_drivers out of vocabulary", set("quality_drivers", []any{"bogus"}), true, false},
		{"baseline driver duplicated", func(base, _ map[string]any) {
			base["quality_drivers"] = []any{"weak_cross_links", "weak_cross_links"}
		}, true, false},
		{"stats object absent", func(_, cand map[string]any) {
			for k := range cand {
				delete(cand, k)
			}
		}, false, false},
	}
	shapeM := &BandMomentSubsetShape{StatsPath: "data.evidence_quality_stats", MeanPath: "data.evidence_quality_stats.mean", StddevPath: "data.evidence_quality_stats.stddev"}
	shapeD := &QualityDriversRecomputeShape{StatsPath: "data.evidence_quality_stats", DriversPath: "data.evidence_quality_stats.quality_drivers"}
	for _, c := range cells {
		base, cand := qualityStatsCaptureData(t, c.mutate)
		m := buildBandMomentSubsetPlan(shapeM, base, cand)
		d := buildQualityDriversRecomputePlan(shapeD, base, cand)
		t.Logf("%-50s moments=%-5v (%s) drivers=%v", c.name, m.valid, m.refusal, d.valid)
		if m.valid != c.moments || d.valid != c.drivers {
			t.Errorf("%s: moments = %v, drivers = %v; want %v, %v", c.name, m.valid, d.valid, c.moments, c.drivers)
		}
	}
	// The stats object itself null or of the wrong type on the candidate.
	for _, v := range []any{nil, []any{}, "x"} {
		base, cand := qualityStatsCaptureData(t, func(_, _ map[string]any) {})
		cand.(map[string]any)["evidence_quality_stats"] = v
		m := buildBandMomentSubsetPlan(shapeM, base, cand)
		d := buildQualityDriversRecomputePlan(shapeD, base, cand)
		t.Logf("%-50s moments=%-5v (%s) drivers=%v", "stats object "+jsonKind(v), m.valid, m.refusal, d.valid)
		if m.valid || d.valid {
			t.Errorf("stats object %v admitted", v)
		}
	}
}

// qualityStatsCaptureData decodes the real GET capture, applies mutate to
// the two evidence_quality_stats objects, and returns the decoded bodies.
func qualityStatsCaptureData(t *testing.T, mutate func(base, cand map[string]any)) (any, any) {
	t.Helper()
	fx := qualityStatsTeamScopedFixtures[0]
	decode := func(path string) map[string]any {
		body, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		dec := json.NewDecoder(bytes.NewReader(body))
		dec.UseNumber()
		var out map[string]any
		if err := dec.Decode(&out); err != nil {
			t.Fatalf("decode %s: %v", path, err)
		}
		return out
	}
	base, cand := decode(fx.baseline), decode(fx.candidate)
	mutate(base["evidence_quality_stats"].(map[string]any), cand["evidence_quality_stats"].(map[string]any))
	return any(base), any(cand)
}

// TestBandMomentSubsetPlan_PopulationDomain pins the moment shape over
// population sizes 0, 1, 2, 3 and large, one band, zero variance and band
// edges: a correct pair (stats computed the way the planes compute them)
// is admitted, and the same pair with a wrong stddev is admitted only
// where the wrong value still lies inside the exact range.
func TestBandMomentSubsetPlan_PopulationDomain(t *testing.T) {
	large := make([]float64, 1029)
	for i := range large {
		large[i] = float64(i%97) / 97
	}
	largeExcluded := make([]float64, 703)
	for i := range largeExcluded {
		largeExcluded[i] = float64(i%41) / 100
	}
	cases := []struct {
		name           string
		cand, excluded []float64
		wrongSD        func(sd float64, n int) float64
		wrongAdmitted  bool
	}{
		{"candidate 1 row", []float64{0.5}, []float64{0.1, 0.9}, func(sd float64, _ int) float64 { return sd + 0.1 }, false},
		{"candidate 2 rows", []float64{0.1, 0.3}, []float64{0.7}, func(sd float64, n int) float64 { return sd * math.Sqrt(float64(n)/float64(n-1)) }, false},
		{"candidate 3 rows", []float64{0, 0.19, 0.38}, []float64{0.62, 0.78}, func(sd float64, n int) float64 { return sd * math.Sqrt(float64(n)/float64(n-1)) }, false},
		{"candidate large, small excluded set", large, []float64{0.2, 0.3}, func(sd float64, n int) float64 { return sd * math.Sqrt(float64(n)/float64(n-1)) }, false},
		{"candidate large, large excluded set", large, largeExcluded, func(sd float64, n int) float64 { return sd * math.Sqrt(float64(n)/float64(n-1)) }, true},
		{"candidate one band", []float64{0.61, 0.65, 0.7, 0.79}, []float64{0.1}, func(sd float64, _ int) float64 { return sd + 0.2 }, false},
		{"candidate zero variance", []float64{0.5, 0.5, 0.5}, []float64{0.9}, func(sd float64, _ int) float64 { return sd + 0.01 }, false},
		{"candidate all at band Lows", []float64{0.4, 0.6, 0.8}, []float64{0.1}, func(sd float64, _ int) float64 { return sd + 0.05 }, false},
		{"candidate all at 1.0", []float64{1, 1, 1, 1}, []float64{0.3}, func(sd float64, _ int) float64 { return sd + 0.01 }, false},
		{"excluded 1 row", []float64{0.2, 0.4, 0.6, 0.8}, []float64{0.33}, nil, false},
		{"excluded zero variance", []float64{0.2, 0.4, 0.6, 0.8}, []float64{0.33, 0.33, 0.33}, nil, false},
	}
	for _, tc := range cases {
		base := append(append([]float64{}, tc.cand...), tc.excluded...)
		cm, cs := reportedStats(tc.cand)
		bm, bs := reportedStats(base)
		baseBody := qualityStatsBody(t, bandCountsOf(base), len(base), &bm, &bs, nil)
		plan := buildBandMomentSubsetPlan(bandMomentShapeUnderTest, baseBody, qualityStatsBody(t, bandCountsOf(tc.cand), len(tc.cand), &cm, &cs, nil))
		line := tc.name + ": correct pair admitted=" + boolWord(plan.valid)
		if !plan.valid {
			t.Errorf("%s: correct pair refused: %s", tc.name, plan.refusal)
		}
		if tc.wrongSD != nil {
			wrong := tc.wrongSD(cs, len(tc.cand))
			wp := buildBandMomentSubsetPlan(bandMomentShapeUnderTest, baseBody, qualityStatsBody(t, bandCountsOf(tc.cand), len(tc.cand), &cm, &wrong, nil))
			line += " wrong stddev admitted=" + boolWord(wp.valid) + " (" + wp.refusal + ")"
			if wp.valid != tc.wrongAdmitted {
				t.Errorf("%s: wrong stddev admitted = %v, want %v (%s)", tc.name, wp.valid, tc.wrongAdmitted, wp.refusal)
			}
		}
		t.Log(line)
	}
	// Population size 0: an empty candidate.
	bm, bs := reportedStats([]float64{0.2, 0.7})
	baseBody := qualityStatsBody(t, bandCountsOf([]float64{0.2, 0.7}), 2, &bm, &bs, nil)
	plan := buildBandMomentSubsetPlan(bandMomentShapeUnderTest, baseBody, qualityStatsBody(t, [5]int{}, 0, nil, nil, nil))
	t.Logf("candidate 0 rows: admitted=%s", boolWord(plan.valid))
	if !plan.valid || !plan.emptyCandidate {
		t.Errorf("candidate 0 rows: valid = %v, emptyCandidate = %v (%s)", plan.valid, plan.emptyCandidate, plan.refusal)
	}
}

func boolWord(b bool) string {
	if b {
		return "yes"
	}
	return "no"
}

// TestQualityStatsShapes_Siblings lists every corpus request whose Parity
// declares either evidence-quality shape, and every request whose Parity
// declares an evidence-quality mean/stddev leaf at all: the shapes sit on
// exactly the two team-scoped investment requests, and no other
// team-scoped request carries those leaves.
func TestQualityStatsShapes_Siblings(t *testing.T) {
	if len(restEndpointSpecs) == 0 {
		t.Fatal("empty REST corpus")
	}
	shaped := map[string]bool{}
	leafCarriers := map[string]bool{}
	for operation, spec := range restEndpointSpecs {
		for _, req := range spec.Requests {
			key := operation + " " + req.Name
			for _, d := range req.Parity.BaselineDefects {
				if d.QualityDriversRecomputeShape != nil || d.BandMomentSubsetShape != nil {
					shaped[key] = true
				}
			}
			for _, table := range []map[string]string{req.Parity.FloatTierB, req.Parity.FloatExactLeaves} {
				for _, leaf := range []string{"data.evidence_quality_stats.mean", "data.evidence_quality_stats.stddev", "data.confidence.quality_mean", "data.confidence.quality_stddev"} {
					if _, ok := table[leaf]; ok {
						leafCarriers[key] = true
					}
				}
			}
		}
	}
	carriers := make([]string, 0, len(leafCarriers))
	for key := range leafCarriers {
		carriers = append(carriers, key)
	}
	sort.Strings(carriers)
	for _, key := range carriers {
		t.Logf("carries an evidence-quality mean/stddev leaf: %s (shaped: %v)", key, shaped[key])
	}
	want := map[string]bool{"REST:GET:/api/v1/investment team_scoped": true, "REST:POST:/api/v1/investment team_scoped": true}
	if len(shaped) != len(want) {
		t.Fatalf("shaped requests = %v, want %v", shaped, want)
	}
	for key := range want {
		if !shaped[key] {
			t.Fatalf("shaped requests = %v, want %v", shaped, want)
		}
	}
	// POST /api/v1/investment/explain's team_scoped request declares no
	// team-scope entry for any of its leaves (its own corpus comment:
	// knowingly uncovered), band_mix counts included, so the moment shape
	// has no declared subset relation there to build on.
	unshapedTeam := map[string]bool{}
	for key := range leafCarriers {
		if !shaped[key] && strings.Contains(key, "team") {
			unshapedTeam[key] = true
		}
	}
	if len(unshapedTeam) != 1 || !unshapedTeam["REST:POST:/api/v1/investment/explain team_scoped"] {
		t.Fatalf("team-scoped requests carrying the leaves without the shapes = %v, want only investment/explain team_scoped", unshapedTeam)
	}
}

// TestBandMomentSubsetPlan_ZeroVarianceBelowTheAllowance pins, for a
// candidate of N values all at 1.0 -- mean 1.0, which the band counts
// allow only with every value at 1.0, so variance exactly 0 -- what the
// moment shape
// does with a reported stddev that deviates from 0 by less than, and by
// more than, the rounding allowance: at N = 1 the allowance is 0 and any
// non-zero stddev is refused; at N >= 2 a stddev inside sqrt(allowance)
// is admitted (the planes' own cancellation produces it) and one outside
// is refused.
func TestBandMomentSubsetPlan_ZeroVarianceBelowTheAllowance(t *testing.T) {
	for _, n := range []int{1, 2, 3, 1029} {
		cand := make([]float64, n)
		for i := range cand {
			cand[i] = 1.0
		}
		excluded := []float64{0.1, 0.9}
		base := append(append([]float64{}, cand...), excluded...)
		cm, _ := reportedStats(cand)
		bm, bs := reportedStats(base)
		baseBody := qualityStatsBody(t, bandCountsOf(base), len(base), &bm, &bs, nil)
		err := reportedMomentError(int64(n))
		own := roundingGamma(2*float64(n) + 16)
		allowance := err.e2 + 2*(err.mean+own) + own
		cells := []struct {
			name string
			sd   float64
			want bool
		}{
			{"stddev 0", 0, true},
			{"stddev 1e-9", 1e-9, n > 1},
			{"stddev 0.5*sqrt(allowance)", 0.5 * math.Sqrt(allowance), n > 1},
			{"stddev 2*sqrt(allowance)", 2 * math.Sqrt(allowance), false},
		}
		for _, c := range cells {
			sd := c.sd
			plan := buildBandMomentSubsetPlan(bandMomentShapeUnderTest, baseBody, qualityStatsBody(t, bandCountsOf(cand), n, &cm, &sd, nil))
			t.Logf("N=%-5d %-28s sd=%.3g admitted=%s (%s)", n, c.name, sd, boolWord(plan.valid), plan.refusal)
			if plan.valid != c.want {
				t.Errorf("N=%d %s: admitted = %v, want %v (%s)", n, c.name, plan.valid, c.want, plan.refusal)
			}
		}
	}
}

// TestBandMomentSubsetPlan_BandEdges puts a candidate of N equal values
// at each band's Low, at its High, and at the next band's floor while
// still counted in this band, and pins the moment shape's verdict. The
// floor value is impossible for the band; at N = 1 the mean is exact and
// it is refused, at N >= 2 it lies within the derived rounding allowance
// of High and is admitted.
func TestBandMomentSubsetPlan_BandEdges(t *testing.T) {
	bandIndex := map[string]int{"high": 0, "moderate": 1, "low": 2, "very_low": 3}
	for i, band := range evidenceQualityBands {
		type point struct {
			name  string
			value float64
		}
		points := []point{{"Low", band.Low}, {"High", band.High}}
		if i+1 < len(evidenceQualityBands) {
			points = append(points, point{"next floor", evidenceQualityBands[i+1].Low})
		}
		for _, p := range points {
			for _, n := range []int{1, 2, 1029} {
				cand := make([]float64, n)
				for j := range cand {
					cand[j] = p.value
				}
				excluded := []float64{0.05, 0.95}
				var candCounts [5]int
				candCounts[bandIndex[band.Name]] = n
				baseCounts := bandCountsOf(excluded)
				baseCounts[bandIndex[band.Name]] += n
				cm, cs := reportedStats(cand)
				bm, bs := reportedStats(append(append([]float64{}, cand...), excluded...))
				plan := buildBandMomentSubsetPlan(bandMomentShapeUnderTest,
					qualityStatsBody(t, baseCounts, n+len(excluded), &bm, &bs, nil),
					qualityStatsBody(t, candCounts, n, &cm, &cs, nil))
				want := p.name != "next floor" || n > 1
				t.Logf("%-8s at %-10s value=%-20v N=%-5d admitted=%s (%s)", band.Name, p.name, p.value, n, boolWord(plan.valid), plan.refusal)
				if plan.valid != want {
					t.Errorf("%s at %s, N=%d: admitted = %v, want %v (%s)", band.Name, p.name, n, plan.valid, want, plan.refusal)
				}
			}
		}
	}
}
