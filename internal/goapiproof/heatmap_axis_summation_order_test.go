package goapiproof

import (
	"fmt"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"
)

// A real production GET /api/v1/heatmap hotspot_risk team-scoped pair:
// two files whose per-week cell values carry one 1-ULP difference on
// the candidate leg (116.13751233352814 vs ...815). Summed week
// ascending -- the route's own ORDER BY week row order -- both files
// total 241.41495059667758 on both legs and the candidate breaks the
// tie by name ascending at data.axes.y[14]/[15]; summed in another
// order the candidate's own total for one of them is ...756 instead,
// and the tie disappears.
const (
	heatmapTieGroupRealBaselinePath  = "testdata/heatmap_hotspot_risk_tiegroup_team_scoped_baseline_6d9c49a1.json"
	heatmapTieGroupRealCandidatePath = "testdata/heatmap_hotspot_risk_tiegroup_team_scoped_candidate_c8d84cbf.json"
	heatmapSummationOrderRuns        = 200
)

func readHeatmapFixture(t *testing.T, path string) string {
	t.Helper()
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fixture %s: %v", path, err)
	}
	return string(body)
}

func decodeHeatmapPair(t *testing.T, baseBody, candBody string) (Snapshot, Snapshot) {
	t.Helper()
	baseline, err := DecodeRESTSnapshot([]byte(baseBody))
	if err != nil {
		t.Fatalf("decode baseline: %v", err)
	}
	candidate, err := DecodeRESTSnapshot([]byte(candBody))
	if err != nil {
		t.Fatalf("decode candidate: %v", err)
	}
	return baseline, candidate
}

// replaceOnce swaps exactly one occurrence of old in body, failing the
// test when old is absent or ambiguous so a fixture edit never silently
// becomes a no-op.
func replaceOnce(t *testing.T, body, old, replacement string) string {
	t.Helper()
	if n := strings.Count(body, old); n != 1 {
		t.Fatalf("fixture carries %d occurrences of %q, want exactly 1", n, old)
	}
	return strings.Replace(body, old, replacement, 1)
}

func findingPathSet(result Result) []string {
	paths := make([]string, 0, len(result.Findings))
	for _, finding := range result.Findings {
		paths = append(paths, finding.Path)
	}
	sort.Strings(paths)
	return paths
}

// Every run decodes the pair afresh, so every run builds new maps and
// draws a new map iteration order: a verdict that depends on that order
// shows up as a failing run inside one test invocation.
func TestHeatmapHotspotRiskRealTiePair_AdmittedOnEveryRun(t *testing.T) {
	baseBody := readHeatmapFixture(t, heatmapTieGroupRealBaselinePath)
	candBody := readHeatmapFixture(t, heatmapTieGroupRealCandidatePath)
	wantPaths := []string{"$.data.axes.y[14]", "$.data.axes.y[15]"}
	for _, tc := range []struct {
		name string
		opts Options
	}{
		{"hotspot_risk_org", heatmapHotspotRiskParity},
		{"hotspot_risk_team_scoped", heatmapHotspotRiskTeamScopedParity},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for run := 0; run < heatmapSummationOrderRuns; run++ {
				baseline, candidate := decodeHeatmapPair(t, baseBody, candBody)
				result := Compare(baseline, candidate, tc.opts)
				if result.DifferencesOutsideBaselineDefect != 0 {
					t.Fatalf("run %d: outside = %d, want 0: findings %+v", run, result.DifferencesOutsideBaselineDefect, result.Findings)
				}
				if got := findingPathSet(result); !reflect.DeepEqual(got, wantPaths) {
					t.Fatalf("run %d: findings %v, want %v", run, got, wantPaths)
				}
				if !reflect.DeepEqual(result.BaselineDefectsMatched, []string{heatmapHotspotRiskAxisTieGroupDefect.Ticket}) {
					t.Fatalf("run %d: matched %v, want only %s", run, result.BaselineDefectsMatched, heatmapHotspotRiskAxisTieGroupDefect.Ticket)
				}
			}
		})
	}
}

// The candidate's tie group in name-DESCENDING order (and the baseline's
// in the other order, so the two positions still differ): not the one
// tiebreak this port applies, so both positions stay outside.
func TestHeatmapHotspotRiskRealTiePair_NameDescendingCandidateStaysOutside(t *testing.T) {
	const (
		contracts = `"full-chaos/dev-health-acr:contracts/jsonschema/v1/mcp_investigation_result_response.v1.schema.json"`
		internal  = `"full-chaos/dev-health-acr:internal/mcp/schemas/mcp_investigation_result_response.v1.schema.json"`
	)
	baseBody := replaceOnce(t, readHeatmapFixture(t, heatmapTieGroupRealBaselinePath), internal+","+contracts, contracts+","+internal)
	candBody := replaceOnce(t, readHeatmapFixture(t, heatmapTieGroupRealCandidatePath), contracts+","+internal, internal+","+contracts)
	baseline, candidate := decodeHeatmapPair(t, baseBody, candBody)
	result := Compare(baseline, candidate, heatmapHotspotRiskTeamScopedParity)
	if result.DifferencesOutsideBaselineDefect != 2 {
		t.Fatalf("outside = %d, want 2: findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
	if got, want := findingPathSet(result), []string{"$.data.axes.y[14]", "$.data.axes.y[15]"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("findings %v, want %v", got, want)
	}
}

// One tied file's first-week cell raised on BOTH legs by the same
// amount: the pair is no longer a tie, the candidate's name-ascending
// placement is then out of total order, and both positions stay outside.
func TestHeatmapHotspotRiskRealTiePair_BrokenTieStaysOutside(t *testing.T) {
	const (
		cell   = `{"x":"2026-08-30","y":"full-chaos/dev-health-acr:internal/mcp/schemas/mcp_investigation_result_response.v1.schema.json","value":15.774671525674766}`
		raised = `{"x":"2026-08-30","y":"full-chaos/dev-health-acr:internal/mcp/schemas/mcp_investigation_result_response.v1.schema.json","value":15.774671525674866}`
	)
	baseBody := replaceOnce(t, readHeatmapFixture(t, heatmapTieGroupRealBaselinePath), cell, raised)
	candBody := replaceOnce(t, readHeatmapFixture(t, heatmapTieGroupRealCandidatePath), cell, raised)
	baseline, candidate := decodeHeatmapPair(t, baseBody, candBody)
	result := Compare(baseline, candidate, heatmapHotspotRiskTeamScopedParity)
	if result.DifferencesOutsideBaselineDefect != 2 {
		t.Fatalf("outside = %d, want 2: findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
	if got, want := findingPathSet(result), []string{"$.data.axes.y[14]", "$.data.axes.y[15]"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("findings %v, want %v", got, want)
	}
}

// heatmapSummationOrderSensitivePair builds a pair whose "org/alpha"
// total is 241.41495059667758 summed week ascending and
// 241.41495059667756 in another order, beside "org/aardvark" at exactly
// 241.41495059667756 and "org/gamma" doubled on the baseline (two agreeing
// 2x shared cells) so it moves from last to first. Summed week ascending
// there is no tie and every axis rule holds; in another order alpha and
// beta tie.
func heatmapSummationOrderSensitivePair(t *testing.T) (baseBody, candBody string) {
	t.Helper()
	shared := []string{
		heatmapBoundaryCell("2026-08-02", "org/alpha", 15.774671525674766),
		heatmapBoundaryCell("2026-08-09", "org/alpha", 116.13751233352814),
		heatmapBoundaryCell("2026-08-16", "org/alpha", 109.50276673747467),
		heatmapBoundaryCell("2026-08-02", "org/aardvark", 241.41495059667756),
	}
	baseCells := append(append([]string{}, shared...),
		heatmapBoundaryCell("2026-08-02", "org/gamma", 200),
		heatmapBoundaryCell("2026-08-09", "org/gamma", 200),
	)
	candCells := append(append([]string{}, shared...),
		heatmapBoundaryCell("2026-08-02", "org/gamma", 100),
		heatmapBoundaryCell("2026-08-09", "org/gamma", 100),
	)
	baseBody = heatmapBoundaryBody(t, joinJSON(baseCells), []string{"org/gamma", "org/alpha", "org/aardvark"})
	candBody = heatmapBoundaryBody(t, joinJSON(candCells), []string{"org/alpha", "org/aardvark", "org/gamma"})
	return baseBody, candBody
}

func TestHeatmapAxisRepoOrderShape_TotalsIndependentOfMapOrder(t *testing.T) {
	baseBody, candBody := heatmapSummationOrderSensitivePair(t)
	shape := heatmapRepoTouchpointsAxisOrderDefect.HeatmapAxisRepoOrderShape
	if shape == nil {
		t.Fatal("heatmapRepoTouchpointsAxisOrderDefect carries no HeatmapAxisRepoOrderShape")
	}
	for run := 0; run < heatmapSummationOrderRuns; run++ {
		baseline, candidate := decodeHeatmapPair(t, baseBody, candBody)
		plan := buildHeatmapAxisRepoOrderPlan(shape, baseline.Data, candidate.Data)
		if !plan.valid {
			t.Fatalf("run %d: plan refused a pair whose week-ascending totals carry no tie", run)
		}
	}
}

// Every request the corpus sends to GET /api/v1/heatmap, fed both the
// real tie pair and the summation-order-sensitive pair: the outside count
// and the finding set are the same on every run.
func TestHeatmapCorpus_VerdictIndependentOfMapOrder(t *testing.T) {
	spec, ok := restEndpointSpecs["REST:GET:/api/v1/heatmap"]
	if !ok || len(spec.Requests) == 0 {
		t.Fatal("corpus carries no GET /api/v1/heatmap requests")
	}
	syntheticBase, syntheticCand := heatmapSummationOrderSensitivePair(t)
	pairs := []struct {
		name       string
		base, cand string
	}{
		{"real_tie_pair", readHeatmapFixture(t, heatmapTieGroupRealBaselinePath), readHeatmapFixture(t, heatmapTieGroupRealCandidatePath)},
		{"summation_order_sensitive_pair", syntheticBase, syntheticCand},
	}
	for _, request := range spec.Requests {
		if request.BodyMode != RESTBodyModeJSON {
			continue
		}
		for _, pair := range pairs {
			t.Run(request.Name+"/"+pair.name, func(t *testing.T) {
				var first string
				for run := 0; run < heatmapSummationOrderRuns; run++ {
					baseline, candidate := decodeHeatmapPair(t, pair.base, pair.cand)
					result := Compare(baseline, candidate, request.Parity)
					verdict := fmt.Sprintf("outside=%d findings=%v matched=%v", result.DifferencesOutsideBaselineDefect, findingPathSet(result), result.BaselineDefectsMatched)
					if run == 0 {
						first = verdict
						continue
					}
					if verdict != first {
						t.Fatalf("run %d verdict %q differs from run 0 verdict %q", run, verdict, first)
					}
				}
				t.Logf("%s", first)
			})
		}
	}
}

// heatmapFanoutAxisPair builds a pair where the first name's repository
// is doubled on the baseline (two agreeing 2x shared cells, one per
// file when the names are file keys of one repository) and the axes are
// as given.
func heatmapFanoutAxisPair(t *testing.T, fanned1, fanned2, other string, otherTotal float64, axisBase, axisCand []string) (Snapshot, Snapshot) {
	t.Helper()
	base := []string{
		heatmapBoundaryCell("2026-08-02", fanned1, 4),
		heatmapBoundaryCell("2026-08-09", fanned2, 4),
		heatmapBoundaryCell("2026-08-02", other, otherTotal),
	}
	cand := []string{
		heatmapBoundaryCell("2026-08-02", fanned1, 2),
		heatmapBoundaryCell("2026-08-09", fanned2, 2),
		heatmapBoundaryCell("2026-08-02", other, otherTotal),
	}
	return heatmapAxisSnapshotsFromCells(t, base, cand, axisBase, axisCand)
}

// Every totals-sorted heatmap axis case in the corpus, at every scope,
// fed a short list whose only difference is a verified fan-out reorder
// (admitted), a fan-out tie in more than one reference order (admitted),
// and a reorder no verified multiplier produces (axis findings outside).
func TestHeatmapCorpus_FanoutAxisReorderAtEveryScope(t *testing.T) {
	type pair struct {
		label              string
		other              float64
		axisBase, axisCand []string
		wantOutside        int
	}
	repoPairs := func(a, b string) []pair {
		// One repository name carries both fanned cells: base 8, cand 4.
		return []pair{
			{"reorder", 6, []string{a, b}, []string{b, a}, 0},
			{"tie, reference order fanned first", 8, []string{a, b}, []string{b, a}, 0},
			{"unexplained", 1, []string{a, b}, []string{b, a}, 2},
		}
	}
	filePairs := func(a1, a2, b string) []pair {
		// Two files of one repository, one fanned cell each: base 4, cand 2.
		return []pair{
			{"reorder", 3, []string{a1, a2, b}, []string{b, a1, a2}, 0},
			{"tie, reference order a1,b,a2", 4, []string{a1, b, a2}, []string{b, a1, a2}, 0},
			{"tie, reference order a2,a1,b", 4, []string{a2, a1, b}, []string{b, a1, a2}, 0},
			{"unexplained", 1, []string{a1, a2, b}, []string{b, a1, a2}, 3},
		}
	}
	for _, tc := range []struct {
		name   string
		opts   Options
		fanned [2]string
		other  string
		pairs  []pair
	}{
		{"repo_touchpoints org and repo scope", heatmapRepoTouchpointsParity, [2]string{"org/alpha", "org/alpha"}, "org/beta", repoPairs("org/alpha", "org/beta")},
		{"repo_touchpoints team scope", heatmapRepoTouchpointsTeamScopedParity, [2]string{"org/alpha", "org/alpha"}, "org/beta", repoPairs("org/alpha", "org/beta")},
		{"hotspot_risk org scope", heatmapHotspotRiskParity, [2]string{"org/alpha:one.go", "org/alpha:two.go"}, "org/beta:x.go", filePairs("org/alpha:one.go", "org/alpha:two.go", "org/beta:x.go")},
		{"hotspot_risk team scope", heatmapHotspotRiskTeamScopedParity, [2]string{"org/alpha:one.go", "org/alpha:two.go"}, "org/beta:x.go", filePairs("org/alpha:one.go", "org/alpha:two.go", "org/beta:x.go")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, p := range tc.pairs {
				baseline, candidate := heatmapFanoutAxisPair(t, tc.fanned[0], tc.fanned[1], tc.other, p.other, p.axisBase, p.axisCand)
				assertHeatmapOutside(t, p.label, baseline, candidate, tc.opts, p.wantOutside)
			}
		})
	}
}

func assertHeatmapOutside(t *testing.T, label string, baseline, candidate Snapshot, opts Options, want int) {
	t.Helper()
	result := Compare(baseline, candidate, opts)
	if result.DifferencesOutsideBaselineDefect != want {
		t.Fatalf("%s: outside = %d, want %d: findings %v matched %v", label, result.DifferencesOutsideBaselineDefect, want, findingPathSet(result), result.BaselineDefectsMatched)
	}
}

func TestHeatmapAxisMatchesTieRuns(t *testing.T) {
	totals := map[string]float64{"a": 8, "b": 8, "c": 1}
	for _, tc := range []struct {
		axis []string
		want bool
	}{
		{[]string{"a", "b", "c"}, true},
		{[]string{"b", "a", "c"}, true},
		{[]string{"a", "c", "b"}, false},
		{[]string{"c", "a", "b"}, false},
		{[]string{"a", "b"}, false},
		{[]string{"a", "b", "c", "d"}, false},
		{[]string{}, false},
		{[]string{"a", "a", "c"}, false},
	} {
		if got := heatmapAxisMatchesTieRuns(tc.axis, totals); got != tc.want {
			t.Errorf("heatmapAxisMatchesTieRuns(%v) = %t, want %t", tc.axis, got, tc.want)
		}
	}
	if !heatmapAxisMatchesTieRuns([]string{}, map[string]float64{}) {
		t.Error("an empty axis over empty totals matches")
	}
}
