package goapiproof

import (
	"bytes"
	"encoding/json"
	"math"
	"math/big"
	"math/rand/v2"
	"os"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/units"
)

// TestEvidenceQualityBands_MatchWriterThresholds walks every band edge
// through the production band function: each band's Low and High band to
// that band, the float just below Low to the band below, the float just
// above High to the band above, and the ends of the clamp's domain are the
// outer edges. A drift between units.EvidenceQualityBandFloors,
// units.EvidenceQualityBand and this shape's intervals fails here.
func TestEvidenceQualityBands_MatchWriterThresholds(t *testing.T) {
	if len(evidenceQualityBands) != len(units.EvidenceQualityBandFloors) {
		t.Fatalf("%d intervals for %d production bands", len(evidenceQualityBands), len(units.EvidenceQualityBandFloors))
	}
	if got := units.ClampUnit(-1); got != evidenceQualityBands[0].Low {
		t.Fatalf("ClampUnit(-1) = %v, want lowest band Low %v", got, evidenceQualityBands[0].Low)
	}
	top := evidenceQualityBands[len(evidenceQualityBands)-1]
	if got := units.ClampUnit(2); got != top.High {
		t.Fatalf("ClampUnit(2) = %v, want highest band High %v", got, top.High)
	}
	for i, band := range evidenceQualityBands {
		for _, edge := range []float64{band.Low, band.High} {
			if got := units.EvidenceQualityBand(edge); got != band.Name {
				t.Fatalf("EvidenceQualityBand(%v) = %q, want %q", edge, got, band.Name)
			}
		}
		if i > 0 {
			below := math.Nextafter(band.Low, math.Inf(-1))
			if got := units.EvidenceQualityBand(below); got != evidenceQualityBands[i-1].Name {
				t.Fatalf("EvidenceQualityBand(%v) = %q, want %q", below, got, evidenceQualityBands[i-1].Name)
			}
			if prev := evidenceQualityBands[i-1]; prev.High != below {
				t.Fatalf("band %q High %v is not the float just below band %q Low %v", prev.Name, prev.High, band.Name, band.Low)
			}
		}
		if i+1 < len(evidenceQualityBands) {
			above := math.Nextafter(band.High, math.Inf(1))
			if got := units.EvidenceQualityBand(above); got != evidenceQualityBands[i+1].Name {
				t.Fatalf("EvidenceQualityBand(%v) = %q, want %q", above, got, evidenceQualityBands[i+1].Name)
			}
		}
	}
}

// TestEvidenceQualityBandsFrom_FollowsTheFloors builds intervals from a
// different set of floors: the intervals follow the floors they are given,
// so the shape reads the production thresholds rather than a copy.
func TestEvidenceQualityBandsFrom_FollowsTheFloors(t *testing.T) {
	got := evidenceQualityBandsFrom([]units.EvidenceQualityBandFloor{{Name: "a", Floor: 0}, {Name: "b", Floor: 0.5}}, 1)
	want := []evidenceQualityBand{{Name: "a", Low: 0, High: math.Nextafter(0.5, math.Inf(-1))}, {Name: "b", Low: 0.5, High: 1}}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("evidenceQualityBandsFrom = %+v, want %+v", got, want)
	}
}

// withoutQualityStatsShapes returns investmentTeamScopedParity with the
// declarations carrying the named shapes removed.
func withoutQualityStatsShapes(drivers, moments bool) Options {
	opts := investmentTeamScopedParity
	kept := make([]BaselineDefect, 0, len(opts.BaselineDefects))
	for _, d := range opts.BaselineDefects {
		if drivers && d.QualityDriversRecomputeShape != nil {
			continue
		}
		if moments && d.BandMomentSubsetShape != nil {
			continue
		}
		kept = append(kept, d)
	}
	opts.BaselineDefects = kept
	return opts
}

func outsidePaths(result Result) map[string]bool {
	covered := map[string]bool{}
	for _, f := range result.Findings {
		covered[tieredPath(f.Path)] = true
	}
	return covered
}

// TestInvestmentTeamScopedParity_QualityStatsGuardRemoval pins that each
// shape is what admits its own leaves on the real captures.
func TestInvestmentTeamScopedParity_QualityStatsGuardRemoval(t *testing.T) {
	cases := []struct {
		name             string
		drivers, moments bool
		want             int
	}{
		{"drivers shape removed", true, false, 1},
		{"moments shape removed", false, true, 2},
		{"both shapes removed", true, true, 3},
	}
	for _, fx := range qualityStatsTeamScopedFixtures {
		baseline := teamScopeRealBodySnapshotFromFile(t, fx.baseline)
		candidate := teamScopeRealBodySnapshotFromFile(t, fx.candidate)
		for _, tc := range cases {
			result := Compare(baseline, candidate, withoutQualityStatsShapes(tc.drivers, tc.moments))
			if result.DifferencesOutsideBaselineDefect != tc.want {
				t.Fatalf("%s %s: outside = %d, want %d: findings %+v", fx.label, tc.name, result.DifferencesOutsideBaselineDefect, tc.want, result.Findings)
			}
		}
	}
}

// mutatedQualityStatsPair decodes the real GET capture, applies mutate
// to the baseline and candidate evidence_quality_stats objects, and
// returns the two snapshots.
func mutatedQualityStatsPair(t *testing.T, mutate func(base, cand map[string]any)) (Snapshot, Snapshot) {
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
	encode := func(v map[string]any) Snapshot {
		body, err := json.Marshal(v)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		snapshot, err := DecodeRESTSnapshot(body)
		if err != nil {
			t.Fatalf("decode snapshot: %v", err)
		}
		return snapshot
	}
	return encode(base), encode(cand)
}

// TestInvestmentTeamScopedParity_QualityStatsNegatives mutates one input
// of the real GET capture per case and pins the exact outside count.
// quality_drivers is one finding; mean and stddev are admitted or refused
// together, so a moments refusal leaves two.
func TestInvestmentTeamScopedParity_QualityStatsNegatives(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(base, cand map[string]any)
		want   int
		paths  []string
	}{
		{
			name:   "baseline reports a driver its own inputs do not give",
			mutate: func(base, _ map[string]any) { base["quality_drivers"] = []any{"low_text_signal"} },
			want:   1, paths: []string{"data.evidence_quality_stats.quality_drivers"},
		},
		{
			name:   "candidate reports thin_component with unknown 0",
			mutate: func(_, cand map[string]any) { cand["quality_drivers"] = []any{"thin_component"} },
			want:   1, paths: []string{"data.evidence_quality_stats.quality_drivers"},
		},
		{
			name:   "candidate mean outside its own band interval",
			mutate: func(_, cand map[string]any) { cand["mean"] = json.Number("0.70") },
			want:   2, paths: []string{"data.evidence_quality_stats.mean", "data.evidence_quality_stats.stddev"},
		},
		{
			name:   "excluded population mean outside its own band interval",
			mutate: func(base, _ map[string]any) { base["mean"] = json.Number("0.55") },
			want:   2, paths: []string{"data.evidence_quality_stats.mean", "data.evidence_quality_stats.stddev"},
		},
		{
			name:   "excluded population variance negative",
			mutate: func(base, _ map[string]any) { base["stddev"] = json.Number("0.01") },
			want:   2, paths: []string{"data.evidence_quality_stats.mean", "data.evidence_quality_stats.stddev"},
		},
		{
			name: "unknown band non-zero on the baseline",
			mutate: func(base, _ map[string]any) {
				bands := base["band_counts"].(map[string]any)
				bands["very_low"] = json.Number("994")
				bands["unknown"] = json.Number("1")
			},
			want: 2, paths: []string{"data.evidence_quality_stats.mean", "data.evidence_quality_stats.stddev"},
		},
	}
	for _, tc := range cases {
		baseline, candidate := mutatedQualityStatsPair(t, tc.mutate)
		result := Compare(baseline, candidate, investmentTeamScopedParity)
		if result.DifferencesOutsideBaselineDefect != tc.want {
			t.Fatalf("%s: outside = %d, want %d: findings %+v", tc.name, result.DifferencesOutsideBaselineDefect, tc.want, result.Findings)
		}
		present := outsidePaths(result)
		for _, p := range tc.paths {
			if !present[p] {
				t.Fatalf("%s: no finding at %s: findings %+v", tc.name, p, result.Findings)
			}
		}
		// The refusing declaration is reported live but unexplained, not
		// matched.
		ticket := investmentTeamScopeQualityStatsDefects[0].Ticket
		live := false
		for _, unexplained := range result.LiveBaselineDefectsUnexplained {
			live = live || unexplained == ticket
		}
		if !live {
			t.Fatalf("%s: live unexplained = %v, want %s among them", tc.name, result.LiveBaselineDefectsUnexplained, ticket)
		}
	}
}

// qualityStatsBody builds a neutral {"data":{"evidence_quality_stats":…}}
// body from band counts (high, moderate, low, very_low, unknown), total,
// mean, stddev and drivers. A nil mean/stddev is written as null.
func qualityStatsBody(t *testing.T, bands [5]int, total int, mean, stddev *float64, drivers []string) any {
	t.Helper()
	num := func(f *float64) any {
		if f == nil {
			return nil
		}
		return json.Number(jsonFloat(*f))
	}
	list := make([]any, 0, len(drivers))
	for _, d := range drivers {
		list = append(list, d)
	}
	return map[string]any{"evidence_quality_stats": map[string]any{
		"total": json.Number(jsonInt(total)),
		"band_counts": map[string]any{
			"high": json.Number(jsonInt(bands[0])), "moderate": json.Number(jsonInt(bands[1])),
			"low": json.Number(jsonInt(bands[2])), "very_low": json.Number(jsonInt(bands[3])),
			"unknown": json.Number(jsonInt(bands[4])),
		},
		"mean": num(mean), "stddev": num(stddev), "quality_drivers": list,
	}}
}

func f64(v float64) *float64 { return &v }

var qualityDriversShapeUnderTest = &QualityDriversRecomputeShape{
	StatsPath:   "data.evidence_quality_stats",
	DriversPath: "data.evidence_quality_stats.quality_drivers",
}

func TestQualityDriversRecomputePlan(t *testing.T) {
	agreeing := qualityStatsBody(t, [5]int{0, 10, 0, 0, 0}, 10, f64(0.7), f64(0.05), []string{})
	cases := []struct {
		name string
		leg  any
		want bool
	}{
		{"list equals its own recomputation", agreeing, true},
		{
			"two drivers in rule order",
			qualityStatsBody(t, [5]int{0, 0, 2, 8, 0}, 10, f64(0.3), f64(0.05), []string{"low_text_signal", "weak_cross_links"}),
			true,
		},
		{
			"two drivers out of rule order",
			qualityStatsBody(t, [5]int{0, 0, 2, 8, 0}, 10, f64(0.3), f64(0.05), []string{"weak_cross_links", "low_text_signal"}),
			false,
		},
		{
			"band counts cross a threshold the list does not report",
			qualityStatsBody(t, [5]int{0, 4, 0, 6, 0}, 10, f64(0.5), f64(0.05), []string{}),
			false,
		},
		{
			"thin_component determined absent with unknown under the floor",
			qualityStatsBody(t, [5]int{0, 8, 0, 0, 2}, 10, f64(0.7), f64(0.05), []string{}),
			true,
		},
		{
			"thin_component undetermined refuses",
			qualityStatsBody(t, [5]int{0, 6, 0, 0, 4}, 10, f64(0.7), f64(0.05), []string{"missing_evidence_metadata"}),
			false,
		},
		{
			"thin_component undetermined refuses an empty list",
			qualityStatsBody(t, [5]int{0, 6, 0, 0, 4}, 10, f64(0.7), f64(0.05), []string{}),
			false,
		},
		{
			"thin_component reported with unknown 0",
			qualityStatsBody(t, [5]int{0, 10, 0, 0, 0}, 10, f64(0.7), f64(0.05), []string{"thin_component"}),
			false,
		},
	}
	noList := qualityStatsBody(t, [5]int{0, 10, 0, 0, 0}, 10, f64(0.7), f64(0.05), nil)
	delete(noList.(map[string]any)["evidence_quality_stats"].(map[string]any), "quality_drivers")
	if plan := buildQualityDriversRecomputePlan(qualityDriversShapeUnderTest, agreeing, noList); plan.valid {
		t.Fatalf("leg with no quality_drivers key admitted")
	}
	for _, tc := range cases {
		plan := buildQualityDriversRecomputePlan(qualityDriversShapeUnderTest, agreeing, tc.leg)
		if plan.valid != tc.want {
			t.Fatalf("%s (candidate leg): valid = %v, want %v", tc.name, plan.valid, tc.want)
		}
		plan = buildQualityDriversRecomputePlan(qualityDriversShapeUnderTest, tc.leg, agreeing)
		if plan.valid != tc.want {
			t.Fatalf("%s (baseline leg): valid = %v, want %v", tc.name, plan.valid, tc.want)
		}
	}
}

var bandMomentShapeUnderTest = &BandMomentSubsetShape{
	StatsPath:  "data.evidence_quality_stats",
	MeanPath:   "data.evidence_quality_stats.mean",
	StddevPath: "data.evidence_quality_stats.stddev",
}

// momentsOf returns the population mean and standard deviation of values.
func momentsOf(values ...float64) (*float64, *float64) {
	var sum, sq float64
	for _, v := range values {
		sum += v
		sq += v * v
	}
	n := float64(len(values))
	mean := sum / n
	variance := sq/n - mean*mean
	sd := math.Sqrt(math.Max(variance, 0))
	return &mean, &sd
}

func TestBandMomentSubsetPlan(t *testing.T) {
	// Candidate: 0.7, 0.7 (moderate x2). Excluded: 0.1, 0.3 (very_low x2).
	candMean, candSD := momentsOf(0.7, 0.7)
	baseMean, baseSD := momentsOf(0.7, 0.7, 0.1, 0.3)
	base := qualityStatsBody(t, [5]int{0, 2, 0, 2, 0}, 4, baseMean, baseSD, nil)
	cand := qualityStatsBody(t, [5]int{0, 2, 0, 0, 0}, 2, candMean, candSD, nil)
	if plan := buildBandMomentSubsetPlan(bandMomentShapeUnderTest, base, cand); !plan.valid || plan.emptyCandidate {
		t.Fatalf("consistent subset pair: valid = %v emptyCandidate = %v refusal = %q", plan.valid, plan.emptyCandidate, plan.refusal)
	}
	if plan := buildBandMomentSubsetPlan(bandMomentShapeUnderTest, base, qualityStatsBody(t, [5]int{0, 0, 0, 0, 0}, 0, nil, nil, nil)); !plan.valid || !plan.emptyCandidate {
		t.Fatalf("empty candidate: valid = %v emptyCandidate = %v refusal = %q", plan.valid, plan.emptyCandidate, plan.refusal)
	}

	// Candidate: 0.4, 0.8 (very_low, high) reported with stddev 0.1, below
	// the 0.2 its two bands force. Excluded: 0.05, 0.35 (very_low x2).
	spreadCand := qualityStatsBody(t, [5]int{1, 0, 0, 1, 0}, 2, f64(0.6), f64(0.1), nil)
	spreadBase := qualityStatsBody(t, [5]int{1, 0, 0, 3, 0}, 4, f64(0.4), f64(math.Sqrt(0.21625-0.16)), nil)
	// Candidate as above (valid); excluded very_low + low with mean 0.3
	// and E[x^2] 0.095, below the 0.1 those two bands force.
	tightBase := qualityStatsBody(t, [5]int{0, 2, 1, 1, 0}, 4, f64(0.5), f64(math.Sqrt(0.2925-0.25)), nil)

	extraKey := qualityStatsBody(t, [5]int{0, 2, 0, 0, 0}, 2, candMean, candSD, nil)
	extraKey.(map[string]any)["evidence_quality_stats"].(map[string]any)["band_counts"].(map[string]any)["other"] = json.Number("0")
	negativeCount := qualityStatsBody(t, [5]int{0, 3, 0, -1, 0}, 2, candMean, candSD, nil)

	cases := []struct {
		name       string
		base, cand any
		refusal    string
	}{
		{"band_counts with a sixth key", base, extraKey, momentRefusalUnreadable},
		{"one-row candidate with stddev 4e-8", qualityStatsBody(t, [5]int{2, 0, 0, 0, 0}, 2, f64(0.8), f64(0), nil), qualityStatsBody(t, [5]int{1, 0, 0, 0, 0}, 1, f64(0.8), f64(4e-8), nil), momentRefusalSingleRowStddev},
		{"empty candidate with a mean", base, qualityStatsBody(t, [5]int{0, 0, 0, 0, 0}, 0, f64(0.5), nil, nil), momentRefusalEmptyCandidate},
		{"empty candidate with a stddev", base, qualityStatsBody(t, [5]int{0, 0, 0, 0, 0}, 0, nil, f64(0.1), nil), momentRefusalEmptyCandidate},
		{"empty candidate with band counts", base, qualityStatsBody(t, [5]int{0, 1, 0, 0, 0}, 0, nil, nil, nil), momentRefusalEmptyCandidate},
		{"empty candidate, baseline with unknown rows", qualityStatsBody(t, [5]int{0, 2, 0, 1, 1}, 4, baseMean, baseSD, nil), qualityStatsBody(t, [5]int{0, 0, 0, 0, 0}, 0, nil, nil, nil), momentRefusalUnknown},
		{"empty candidate, baseline pair infeasible", qualityStatsBody(t, [5]int{0, 2, 0, 2, 0}, 4, f64(0.25), f64(0.5), nil), qualityStatsBody(t, [5]int{0, 0, 0, 0, 0}, 0, nil, nil, nil), momentPopulationBaseline + momentPairMeanBelow},
		{"negative band count", base, negativeCount, momentRefusalUnreadable},
		{"band counts do not sum to total", qualityStatsBody(t, [5]int{0, 2, 0, 2, 0}, 5, baseMean, baseSD, nil), cand, momentRefusalBandSum},
		{"unknown non-zero on the candidate", base, qualityStatsBody(t, [5]int{0, 2, 0, 0, 1}, 3, candMean, candSD, nil), momentRefusalUnknown},
		{"null mean on the baseline", qualityStatsBody(t, [5]int{0, 2, 0, 2, 0}, 4, nil, baseSD, nil), cand, momentRefusalNullMoment},
		{"null stddev on the candidate", base, qualityStatsBody(t, [5]int{0, 2, 0, 0, 0}, 2, candMean, nil, nil), momentRefusalNullMoment},
		{"negative stddev on the baseline", qualityStatsBody(t, [5]int{0, 2, 0, 2, 0}, 4, baseMean, f64(-*baseSD), nil), cand, momentRefusalNegativeStddev},
		{"negative stddev on the candidate", base, qualityStatsBody(t, [5]int{0, 2, 0, 0, 0}, 2, candMean, f64(-0.01), nil), momentRefusalNegativeStddev},
		{"candidate band above baseline band", base, qualityStatsBody(t, [5]int{1, 2, 0, 0, 0}, 3, candMean, candSD, nil), momentRefusalNotSubset},
		{"equal populations leave no excluded rows", base, qualityStatsBody(t, [5]int{0, 2, 0, 2, 0}, 4, f64(0.5), baseSD, nil), momentRefusalNoExcluded},
		{"excluded variance negative", qualityStatsBody(t, [5]int{0, 2, 0, 2, 0}, 4, f64(0.45), f64(0.23), nil), cand, momentRefusalExcludedVariance},
		{"baseline mean below its band interval", qualityStatsBody(t, [5]int{0, 2, 0, 2, 0}, 4, f64(0.25), f64(0.5), nil), cand, momentPopulationBaseline + momentPairMeanBelow},
		{"candidate E[x^2] below the least its bands allow", spreadBase, spreadCand, momentPopulationCandidate + momentPairSecondBelow},
		{"excluded E[x^2] below the least its bands allow", tightBase, cand, momentPopulationExcluded + momentPairSecondBelow},
	}
	// Candidate 0, 0.19, 0.38 (very_low x3) reporting its sample stddev
	// 0.19; the greatest three values in [0, 0.4] with mean 0.19 reach is
	// 0.1639. Excluded: 0.62 x5, 0.78 x5 (moderate).
	smallValues := []float64{0, 0.19, 0.38}
	smallMean, _ := momentsOf(smallValues...)
	smallBaseMean, smallBaseSD := momentsOf(append(append([]float64{}, smallValues...), 0.62, 0.62, 0.62, 0.62, 0.62, 0.78, 0.78, 0.78, 0.78, 0.78)...)
	sampleCand := qualityStatsBody(t, [5]int{0, 0, 0, 3, 0}, 3, smallMean, f64(0.19), nil)
	sampleBase := qualityStatsBody(t, [5]int{0, 10, 0, 3, 0}, 13, smallBaseMean, smallBaseSD, nil)
	// Candidate 0.1 x5, 0.7 x5 reporting stddev below its true 0.3;
	// excluded: one row, 0.2, whose variance must be 0.
	pairValues := []float64{0.1, 0.1, 0.1, 0.1, 0.1, 0.7, 0.7, 0.7, 0.7, 0.7}
	pairMean, _ := momentsOf(pairValues...)
	oneBaseMean, oneBaseSD := momentsOf(append(append([]float64{}, pairValues...), 0.2)...)
	understatedCand := qualityStatsBody(t, [5]int{0, 5, 0, 5, 0}, 10, pairMean, f64(math.Sqrt(0.09-0.0039)), nil)
	oneBase := qualityStatsBody(t, [5]int{0, 5, 0, 6, 0}, 11, oneBaseMean, oneBaseSD, nil)
	cases = append(cases,
		struct {
			name       string
			base, cand any
			refusal    string
		}{"small candidate reporting its sample stddev", sampleBase, sampleCand, momentPopulationCandidate + momentPairSecondAbove},
		struct {
			name       string
			base, cand any
			refusal    string
		}{"one excluded row implying a non-zero variance", oneBase, understatedCand, momentPopulationExcluded + momentPairSecondAbove},
	)
	for _, tc := range cases {
		plan := buildBandMomentSubsetPlan(bandMomentShapeUnderTest, tc.base, tc.cand)
		if plan.valid || plan.refusal != tc.refusal {
			t.Fatalf("%s: valid = %v refusal = %q, want refusal %q", tc.name, plan.valid, plan.refusal, tc.refusal)
		}
	}
}

// TestMomentPairRefusal pins each bound on a four-row population, two
// very_low and two moderate. For mean 0.45 the least E[x^2] is 0.225
// (0.3, 0.3, 0.6, 0.6) and the greatest is 0.33 (0, 0.2, 0.8, 0.8).
func TestMomentPairRefusal(t *testing.T) {
	counts := map[string]int64{"very_low": 2, "low": 0, "moderate": 2, "high": 0}
	cases := []struct {
		name     string
		n        int64
		mean, e2 float64
		want     string
	}{
		{"no rows", 0, 0.45, 0.25, momentPairEmpty},
		{"mean at the low edge", 4, 0.3, 0.18, ""},
		{"mean below the low edge", 4, 0.29, 0.18, momentPairMeanBelow},
		{"mean just inside the high edge", 4, 0.59, 0.39, ""},
		{"level in a higher band than the first non-empty one", 4, 0.59, 0.38, momentPairSecondBelow},
		{"mean above the high edge", 4, 0.61, 0.4, momentPairMeanAbove},
		{"E[x^2] at the least", 4, 0.45, 0.225, ""},
		{"E[x^2] below the least", 4, 0.45, 0.224, momentPairSecondBelow},
		{"E[x^2] just below the greatest", 4, 0.45, 0.3299, ""},
		{"E[x^2] just above the greatest", 4, 0.45, 0.3301, momentPairSecondAbove},
	}
	for _, tc := range cases {
		if got := momentPairRefusal(counts, tc.n, tc.mean, tc.e2, momentError{}); got != tc.want {
			t.Fatalf("%s: refusal = %q, want %q", tc.name, got, tc.want)
		}
	}
	// An upper-edge mean for 1183 very_low + 258 high rows forces every
	// value to its High (0.4 and 1.0), so stddev 0 is impossible.
	edge := map[string]int64{"very_low": 1183, "low": 0, "moderate": 0, "high": 258}
	edgeMean := 0.5074253990284525
	if got := momentPairRefusal(edge, 1441, edgeMean, edgeMean*edgeMean, reportedMomentError(1441)); got != momentPairSecondBelow {
		t.Fatalf("upper-edge mean with stddev 0: refusal = %q, want %q", got, momentPairSecondBelow)
	}
	// A mean rounded just past the upper edge still puts every value at
	// its High.
	past := (1183*0.4 + 258*1.0) / 1441 * (1 + 1e-15)
	if past*1441 <= 1183*0.4+258*1.0 {
		t.Fatalf("test mean %v is not past the upper edge", past)
	}
	if got := momentPairRefusal(edge, 1441, past, past*past, reportedMomentError(1441)); got != momentPairSecondBelow {
		t.Fatalf("mean just past the upper edge with stddev 0: refusal = %q, want %q", got, momentPairSecondBelow)
	}
	// A sum exactly on the lower edge of 28 low + 2 high rows (every value
	// at its Low), with an empty band below the first non-empty one:
	// E[x^2] is exactly (28*0.16 + 2*0.64)/30 = 0.192, so anything below
	// it is refused as below the least.
	lowEdge := map[string]int64{"very_low": 0, "low": 28, "moderate": 0, "high": 2}
	if got := momentSumPairRefusal(lowEdge, 30, new(big.Rat).SetFloat64(momentBreaks(lowEdge)[0]), 0.192-0.0001, reportedMomentError(30)); got != momentPairSecondBelow {
		t.Fatalf("lower-edge sum over an empty lower band: refusal = %q, want %q", got, momentPairSecondBelow)
	}
	// One row has variance 0 whatever its value.
	one := map[string]int64{"very_low": 1, "low": 0, "moderate": 0, "high": 0}
	if got := momentPairRefusal(one, 1, 0.2, 0.2*0.2+0.02, reportedMomentError(1)); got != momentPairSecondAbove {
		t.Fatalf("one row with stddev sqrt(0.02): refusal = %q, want %q", got, momentPairSecondAbove)
	}
	// Values exactly on one band edge, with no reported rounding: this
	// function's own arithmetic must not refuse them.
	top := map[string]int64{"very_low": 0, "low": 0, "moderate": 0, "high": 1516}
	if got := momentPairRefusal(top, 1516, 0.8, 0.8*0.8, momentError{}); got != "" {
		t.Fatalf("1516 values all 0.8, exact stats: refusal = %q, want none", got)
	}
}

// TestMomentGreatestSquares_MatchesEveryVertex compares
// momentGreatestSquares with the greatest sum of squares over every
// vertex (each value at its band's Low or High, at most one free) on
// random small populations, so a vertex the threshold enumeration misses
// fails here.
func TestMomentGreatestSquares_MatchesEveryVertex(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	checked := 0
	for trial := 0; trial < 20000; trial++ {
		var rows []evidenceQualityBand
		counts := map[string]int64{}
		size := 1 + rng.IntN(8)
		for i := 0; i < size; i++ {
			band := evidenceQualityBands[rng.IntN(len(evidenceQualityBands))]
			rows = append(rows, band)
			counts[band.Name]++
		}
		var lo, hi float64
		for _, r := range rows {
			lo += r.Low
			hi += r.High
		}
		// Half the sums sit within one band width of either end, where
		// the free value alone carries the surplus.
		var sum float64
		switch trial % 4 {
		case 0:
			sum = lo + rng.Float64()*math.Min(0.4, hi-lo)
		case 1:
			sum = hi - rng.Float64()*math.Min(0.4, hi-lo)
		default:
			sum = lo + rng.Float64()*(hi-lo)
		}
		want := math.Inf(-1)
		for free := 0; free < len(rows); free++ {
			for mask := 0; mask < 1<<len(rows); mask++ {
				var s, sq float64
				for i, r := range rows {
					if i == free {
						continue
					}
					v := r.Low
					if mask&(1<<i) != 0 {
						v = r.High
					}
					s += v
					sq += v * v
				}
				x := sum - s
				if x < rows[free].Low-1e-12 || x > rows[free].High+1e-12 {
					continue
				}
				want = math.Max(want, sq+x*x)
			}
		}
		got := momentGreatestSquares(counts, sum, 0)
		if math.Abs(got-want) > 1e-9 {
			t.Fatalf("counts %v sum %v: momentGreatestSquares = %v, every-vertex greatest = %v", counts, sum, got, want)
		}
		checked++
	}
	if checked == 0 {
		t.Fatalf("no population checked")
	}
	// Every value at its band's High: the threshold band's own count caps
	// k, and that capped vertex is the only one.
	allHigh := map[string]int64{"very_low": 4}
	if got := momentGreatestSquares(allHigh, 1.6, 0); math.Abs(got-0.64) > 1e-12 {
		t.Fatalf("four very_low values at 0.4: momentGreatestSquares = %v, want 0.64", got)
	}
	// 1531 low and 2 moderate rows with mean 0.6: the greatest puts both
	// moderate rows at 0.8 and two low rows at 0.4 (1529*0.36 + 2*0.16 +
	// 2*0.64 = 552.04). Rounding in the k range can put the high endpoint
	// one step short there, and the low endpoint then carries the vertex.
	atBreak := map[string]int64{"low": 1531, "moderate": 2}
	if got := momentGreatestSquares(atBreak, 919.8, 0); math.Abs(got-552.04) > 1e-9 {
		t.Fatalf("1531 low + 2 moderate at mean 0.6: momentGreatestSquares = %v, want 552.04", got)
	}
}

// TestRecomputeQualityDrivers_Boundaries holds each threshold at its
// boundary (not reported) and one step past it (reported), over 100 rows.
func TestRecomputeQualityDrivers_Boundaries(t *testing.T) {
	below04 := math.Nextafter(0.4, 0)
	above025 := math.Nextafter(0.25, 1)
	bands := func(unknown, lowPlus int64) map[string]int64 {
		return map[string]int64{"high": 0, "moderate": 100 - unknown - lowPlus, "low": lowPlus, "very_low": 0, "unknown": unknown}
	}
	cases := []struct {
		name         string
		bands        map[string]int64
		mean, stddev *float64
		want         []string
		determinable bool
	}{
		{"unknown share at 0.3, known floor at 0.7", bands(30, 0), f64(0.7), f64(0.1), []string{}, true},
		{"unknown share past 0.3, known floor below 0.7", bands(31, 0), f64(0.7), f64(0.1), nil, false},
		{"mean at 0.4", bands(0, 0), f64(0.4), f64(0.1), []string{}, true},
		{"mean just below 0.4", bands(0, 0), f64(below04), f64(0.1), []string{"low_text_signal"}, true},
		{"stddev at 0.25", bands(0, 0), f64(0.7), f64(0.25), []string{}, true},
		{"stddev just above 0.25", bands(0, 0), f64(0.7), f64(above025), []string{"high_uncertainty_spread"}, true},
		{"low share at 0.5", bands(0, 50), f64(0.7), f64(0.1), []string{}, true},
		{"low share past 0.5", bands(0, 51), f64(0.7), f64(0.1), []string{"weak_cross_links"}, true},
		{"null mean and stddev", bands(0, 0), nil, nil, []string{}, true},
	}
	for _, tc := range cases {
		got, ok := RecomputeQualityDrivers(100, tc.bands, tc.mean, tc.stddev)
		if ok != tc.determinable || (ok && !equalStringLists(got, tc.want)) {
			t.Fatalf("%s: drivers = %v ok = %v, want %v ok = %v", tc.name, got, ok, tc.want, tc.determinable)
		}
	}
	// total 0 falls back to the band_counts sum for T, and
	// quality_known_count <= total = 0 determines thin_component present.
	got, ok := RecomputeQualityDrivers(0, bands(0, 51), f64(0.7), f64(0.1))
	if !ok || !equalStringLists(got, []string{"weak_cross_links", "thin_component"}) {
		t.Fatalf("total 0: drivers = %v ok = %v, want [weak_cross_links thin_component] true", got, ok)
	}
}

// reportedStats computes a population's mean and stddev the way
// avgIf/stddevPopIf do in float64: one pass of sum and sum of squares,
// varPop = sumsq/n - mean^2.
func reportedStats(values []float64) (mean, stddev float64) {
	var sum, sq float64
	for _, v := range values {
		sum += v
		sq += v * v
	}
	n := float64(len(values))
	mean = sum / n
	// varPop's sum-of-squares form, (m2 - m1*m1/m0)/m0.
	return mean, math.Sqrt(math.Max((sq-sum*sum/n)/n, 0))
}

// bandCountsOf returns the [high, moderate, low, very_low, unknown] band
// counts of values banded by the writer's own thresholds.
func bandCountsOf(values []float64) [5]int {
	var counts [5]int
	for _, v := range values {
		switch units.EvidenceQualityBand(v) {
		case "high":
			counts[0]++
		case "moderate":
			counts[1]++
		case "low":
			counts[2]++
		default:
			counts[3]++
		}
	}
	return counts
}

// TestBandMomentSubsetPlan_CorrectSubsetsAlwaysAdmitted builds correct
// baseline/candidate pairs from real values, reports their stats the
// way the planes compute them, and requires every pair to be admitted.
// It includes the zero-variance cases where the exact pair sits on a
// bound: one excluded row, excluded rows that all share one value, and
// a candidate whose values all sit on one band edge.
func TestBandMomentSubsetPlan_CorrectSubsetsAlwaysAdmitted(t *testing.T) {
	rng := rand.New(rand.NewPCG(3, 4))
	draw := func(n int) []float64 {
		values := make([]float64, n)
		for i := range values {
			values[i] = rng.Float64()
		}
		return values
	}
	same := func(n int, v float64) []float64 {
		values := make([]float64, n)
		for i := range values {
			values[i] = v
		}
		return values
	}
	cases := []struct {
		name     string
		trials   int
		cand     func() []float64
		excluded func() []float64
	}{
		{"one excluded row", 400, func() []float64 { return draw(1029) }, func() []float64 { return draw(1) }},
		{"two identical excluded rows", 400, func() []float64 { return draw(1029) }, func() []float64 { return same(2, rng.Float64()) }},
		{"fifty identical excluded rows", 400, func() []float64 { return draw(1029) }, func() []float64 { return same(50, rng.Float64()) }},
		{"seven hundred identical excluded rows", 200, func() []float64 { return draw(1029) }, func() []float64 { return same(700, rng.Float64()) }},
		{"seven hundred distinct excluded rows", 200, func() []float64 { return draw(1029) }, func() []float64 { return draw(700) }},
		{"candidate all on one band edge", 50, func() []float64 { return same(516, 0.6) }, func() []float64 { return draw(10) }},
		{"candidate all at the top", 50, func() []float64 { return same(1516, 1.0) }, func() []float64 { return draw(3) }},
		{"small team", 400, func() []float64 { return draw(3) }, func() []float64 { return draw(10) }},
	}
	for _, tc := range cases {
		refused := map[string]int{}
		for trial := 0; trial < tc.trials; trial++ {
			cand := tc.cand()
			base := append(append([]float64{}, cand...), tc.excluded()...)
			cm, cs := reportedStats(cand)
			bm, bs := reportedStats(base)
			plan := buildBandMomentSubsetPlan(bandMomentShapeUnderTest,
				qualityStatsBody(t, bandCountsOf(base), len(base), &bm, &bs, nil),
				qualityStatsBody(t, bandCountsOf(cand), len(cand), &cm, &cs, nil))
			if !plan.valid {
				refused[plan.refusal]++
			}
		}
		if len(refused) != 0 {
			t.Errorf("%s: correct pairs refused %v of %d", tc.name, refused, tc.trials)
		}
	}
	// One band edge, checked directly: 1516 values all 0.8.
	edge := map[string]int64{"very_low": 0, "low": 0, "moderate": 0, "high": 1516}
	m, sd := reportedStats(same(1516, 0.8))
	if got := momentPairRefusal(edge, 1516, m, sd*sd+m*m, reportedMomentError(1516)); got != "" {
		t.Fatalf("1516 values all 0.8: refusal = %q, want none", got)
	}
}
