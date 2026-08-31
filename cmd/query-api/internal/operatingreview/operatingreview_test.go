package operatingreview

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"math"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graphqldate"
)

// --- fake QueryClient, same pattern as hotspots_test.go/cognitiveload_test.go ---

type fakeRowScanner struct {
	rows   [][]any
	cursor int
	err    error
	// failAfterRows, when > 0 and err != nil, lets Next() serve this many
	// rows successfully before reporting the failure -- simulating a
	// mid-stream driver error (some rows already Scan()'d, then Next()
	// returns false and Err() is non-nil), the exact shape
	// discardOnError exists for. Zero (the default) preserves the
	// original "fail before any row" behavior every other fake usage in
	// this file relies on.
	failAfterRows int
}

func (f *fakeRowScanner) Next() bool {
	if f.err != nil {
		limit := f.failAfterRows
		if limit > len(f.rows) {
			limit = len(f.rows)
		}
		return f.cursor < limit
	}
	return f.cursor < len(f.rows)
}

func (f *fakeRowScanner) Scan(dest ...any) error {
	row := f.rows[f.cursor]
	f.cursor++
	if len(dest) != len(row) {
		return errors.New("operatingreview test: scan arity mismatch")
	}
	for i, d := range dest {
		switch ptr := d.(type) {
		case *string:
			*ptr = row[i].(string)
		case *uint64:
			*ptr = row[i].(uint64)
		case *uint32:
			*ptr = row[i].(uint32)
		case *float64:
			*ptr = row[i].(float64)
		case *time.Time:
			*ptr = row[i].(time.Time)
		case **float64:
			if row[i] == nil {
				*ptr = nil
			} else {
				v := row[i].(float64)
				*ptr = &v
			}
		case **string:
			if row[i] == nil {
				*ptr = nil
			} else {
				v := row[i].(string)
				*ptr = &v
			}
		default:
			return errors.New("operatingreview test: unsupported scan destination")
		}
	}
	return nil
}

func (f *fakeRowScanner) Err() error   { return f.err }
func (f *fakeRowScanner) Close() error { return nil }

// fakeClient answers each Query call in order from responses/errs -- call
// index i uses errs[i] if set (non-nil), else responses[i]. This matches
// fetchPeriodRows' FIXED, DOCUMENTED per-period call order (work_items,
// state_durations, repo_metrics, hotspots, complexity, deployments,
// incidents, investment, ai_impact, ai_governance), current period first
// then prior -- 20 calls total for one Resolve.
type fakeClient struct {
	responses  []*fakeRowScanner
	errs       []error
	calls      int
	statements []string
}

func (f *fakeClient) Query(_ context.Context, statement string, _ []clickhouse.Binding) (clickhouse.RowScanner, error) {
	i := f.calls
	f.calls++
	f.statements = append(f.statements, statement)
	if i < len(f.errs) && f.errs[i] != nil {
		return nil, f.errs[i]
	}
	return f.responses[i], nil
}

func day(s string) time.Time {
	t, err := time.Parse("2006-01-02", s)
	if err != nil {
		panic(err)
	}
	return t
}

// emptyScanners returns n scanners with zero rows -- the "table has no
// data this period" baseline every test starts from and overrides
// selectively.
func emptyScanners(n int) []*fakeRowScanner {
	out := make([]*fakeRowScanner, n)
	for i := range out {
		out[i] = &fakeRowScanner{}
	}
	return out
}

// ---------------------------------------------------------------------------
// weekBounds / priorWeekStart
// ---------------------------------------------------------------------------

func TestWeekBounds(t *testing.T) {
	start := day("2026-08-24")
	gotStart, gotEnd := weekBounds(start)
	if !gotStart.Equal(start) {
		t.Fatalf("weekBounds start = %v, want %v", gotStart, start)
	}
	wantEnd := day("2026-08-31")
	if !gotEnd.Equal(wantEnd) {
		t.Fatalf("weekBounds end = %v, want %v", gotEnd, wantEnd)
	}
}

func TestPriorWeekStart(t *testing.T) {
	got := priorWeekStart(day("2026-08-24"))
	want := day("2026-08-17")
	if !got.Equal(want) {
		t.Fatalf("priorWeekStart = %v, want %v", got, want)
	}
}

// ---------------------------------------------------------------------------
// Reducer helpers -- ports _value/_present_values/_sum/_avg/_max/_min/
// _weighted_avg (metrics/operating_review.py:769-816).
// ---------------------------------------------------------------------------

func TestSumAvgMaxMin_SkipNilLikePythonNone(t *testing.T) {
	vals := []*float64{f(1), nil, f(3), nil, f(5)}
	if got := sumF(vals); got != 9 {
		t.Errorf("sumF = %v, want 9", got)
	}
	if got := avgF(vals); got != 3 {
		t.Errorf("avgF = %v, want 3", got)
	}
	if got := maxF(vals); got != 5 {
		t.Errorf("maxF = %v, want 5", got)
	}
	if got := minF(vals); got != 1 {
		t.Errorf("minF = %v, want 1", got)
	}
}

func TestSumAvgMaxMin_AllNilDefaultsToZero(t *testing.T) {
	vals := []*float64{nil, nil}
	if got := sumF(vals); got != 0 {
		t.Errorf("sumF(all nil) = %v, want 0", got)
	}
	if got := avgF(vals); got != 0 {
		t.Errorf("avgF(all nil) = %v, want 0", got)
	}
	if got := maxF(vals); got != 0 {
		t.Errorf("maxF(all nil) = %v, want 0", got)
	}
	if got := minF(vals); got != 0 {
		t.Errorf("minF(all nil) = %v, want 0", got)
	}
}

func TestWeightedAvgF_MissingWeightDefaultsToOne(t *testing.T) {
	// row1: value=10, weight=2 ; row2: value=20, weight missing (-> 1.0)
	values := []*float64{f(10), f(20)}
	weights := []*float64{f(2), nil}
	got := weightedAvgF(values, weights)
	want := (10.0*2 + 20.0*1) / (2.0 + 1.0)
	if got != want {
		t.Errorf("weightedAvgF = %v, want %v", got, want)
	}
}

func TestWeightedAvgF_PresentZeroWeightAlsoDefaultsToOne(t *testing.T) {
	// Python's `weight = _value(row, weight_key) or 1.0` treats a PRESENT
	// weight of exactly 0.0 as falsy too -- reproduced, not "fixed".
	values := []*float64{f(10)}
	weights := []*float64{f(0)}
	got := weightedAvgF(values, weights)
	if got != 10 {
		t.Errorf("weightedAvgF with zero weight = %v, want 10 (weight defaults to 1.0)", got)
	}
}

func TestWeightedAvgF_EmptyReturnsZero(t *testing.T) {
	if got := weightedAvgF(nil, nil); got != 0 {
		t.Errorf("weightedAvgF(empty) = %v, want 0", got)
	}
}

// ---------------------------------------------------------------------------
// knownCountGuard -- CHAOS-4563's port of the shipped known_count guard
// (resolvers/analytics.py:262-269, base SHA e9ea257ff): `mean=float(mean_value)
// if mean_value is not None and known_count > 0 else None`. Every test below
// carries a CONTROL (a populated-count case producing a definitively
// different, non-nil, EXACT-valued result) alongside the zero-count case --
// required per this package's own acceptance bar, since "returns nil" here
// is ALSO exactly what a swallowed error or a totally broken fetch produces;
// without the control, a test asserting only the zero-count side could not
// tell a working guard from a bug that nils out everything unconditionally.
// ---------------------------------------------------------------------------

func TestKnownCountGuard_ZeroCountReturnsNilEvenForNaN(t *testing.T) {
	// The exact live shape this guard exists for: ClickHouse avg() over a
	// genuinely empty window returns NaN, never NULL, on a plain
	// (non-Nullable) Float64 column -- math.NaN() stands in for that here.
	got := knownCountGuard(context.Background(), "test_field", math.NaN(), 0)
	if got != nil {
		t.Fatalf("knownCountGuard(NaN, count=0) = %v, want nil", *got)
	}
}

func TestKnownCountGuard_ZeroCountReturnsNilForOrdinaryValueToo(t *testing.T) {
	// Same gate, a non-NaN raw value. Proves the guard checks the COUNTED
	// FACT about the rows (knownCount), never a property of the value
	// itself (e.g. math.IsNaN(raw)) -- exactly the CHAOS-4563 trap: gating
	// on "is the value NaN" would happen to catch the case above but is
	// the wrong mechanism, and this case is how a wrong mechanism would be
	// caught: count is what decides it, unconditionally.
	got := knownCountGuard(context.Background(), "test_field", 0.4, 0)
	if got != nil {
		t.Fatalf("knownCountGuard(0.4, count=0) = %v, want nil (count is the gate, not the value)", *got)
	}
}

func TestKnownCountGuard_PositiveCountKeepsTheExactValue(t *testing.T) {
	// THE CONTROL: a populated window must produce a definitively
	// different, non-nil result carrying the EXACT scanned value -- this
	// is what distinguishes "the guard fired correctly" from "a bug nils
	// out every value regardless of count".
	got := knownCountGuard(context.Background(), "test_field", 0.4, 5)
	if got == nil {
		t.Fatal("knownCountGuard(0.4, count=5) = nil, want a pointer to 0.4")
	}
	if *got != 0.4 {
		t.Errorf("knownCountGuard(0.4, count=5) = %v, want 0.4", *got)
	}
}

// TestKnownCountGuardFiredCounter_ConsumedByARealMeterReader is brief §8's
// "verify something consumes it" requirement, executed literally: a real
// OTel SDK ManualReader (not a mock, not a bare "counter var is non-nil"
// check) is installed as the global MeterProvider, knownCountGuard is
// called once with knownCount=0 (must record) and once with knownCount=5
// (must NOT record), and the reader's Collect output is inspected for the
// exact data point -- proving the telemetry is both emitted AND actually
// reaches a consumer, with the correct "field" attribute and without
// over-firing on the populated-count call.
func TestKnownCountGuardFiredCounter_ConsumedByARealMeterReader(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	prevProvider := otel.GetMeterProvider()
	otel.SetMeterProvider(provider)
	defer otel.SetMeterProvider(prevProvider)

	ctx := context.Background()
	knownCountGuard(ctx, "hotspot_risk_score", math.NaN(), 0) // must fire
	knownCountGuard(ctx, "hotspot_risk_score", 0.4, 5)        // must NOT fire

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rm); err != nil {
		t.Fatalf("reader.Collect: %v", err)
	}

	var sum *metricdata.Sum[int64]
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == "devhealth_query_api_operating_review_known_count_guard_fired_total" {
				if s, ok := m.Data.(metricdata.Sum[int64]); ok {
					sum = &s
				}
			}
		}
	}
	if sum == nil {
		t.Fatal("devhealth_query_api_operating_review_known_count_guard_fired_total not found in collected metrics -- the reader consumed nothing")
	}
	if len(sum.DataPoints) != 1 {
		t.Fatalf("expected exactly 1 data point (one distinct field attribute, one fire), got %d", len(sum.DataPoints))
	}
	dp := sum.DataPoints[0]
	if dp.Value != 1 {
		t.Errorf("counter value = %d, want 1 (fired once, not twice -- the populated-count call must not have recorded)", dp.Value)
	}
	fieldVal, ok := dp.Attributes.Value("field")
	if !ok || fieldVal.AsString() != "hotspot_risk_score" {
		t.Errorf("field attribute = %v (ok=%v), want %q", fieldVal, ok, "hotspot_risk_score")
	}
}

// ---------------------------------------------------------------------------
// fetchHotspotsAgg / fetchComplexityAgg / fetchRepoMetrics -- the three
// CHAOS-4563 guard sites, exercised through the real fetch functions (scan
// + guard together, not knownCountGuard in isolation) with the exact
// single-row shape a live ClickHouse scalar aggregate returns for a
// genuinely empty window: ClickHouse ALWAYS returns exactly one row for a
// `SELECT avg(...), count() FROM (...)` query with no outer GROUP BY, even
// when the inner subquery is empty -- avg() on that row is NaN, count() is
// 0. Each guard site's test pairs the empty-window case with its own
// populated-window control, same reasoning as knownCountGuard's tests above.
// ---------------------------------------------------------------------------

func TestFetchHotspotsAgg_EmptyWindowGuardsNaNToNil(t *testing.T) {
	client := &fakeClient{
		responses: []*fakeRowScanner{{rows: [][]any{{math.NaN(), uint64(0)}}}},
		errs:      make([]error, 1),
	}
	rows, err := fetchHotspotsAgg(context.Background(), client, "org1", day("2026-08-24"), day("2026-08-31"))
	if err != nil {
		t.Fatalf("fetchHotspotsAgg returned error: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected exactly 1 row (a live scalar aggregate always returns one), got %d", len(rows))
	}
	if rows[0].riskScore != nil {
		t.Errorf("riskScore = %v, want nil for hotspots_count=0", *rows[0].riskScore)
	}
}

func TestFetchHotspotsAgg_PopulatedWindowKeepsExactValue(t *testing.T) {
	client := &fakeClient{
		responses: []*fakeRowScanner{{rows: [][]any{{0.4, uint64(5)}}}},
		errs:      make([]error, 1),
	}
	rows, err := fetchHotspotsAgg(context.Background(), client, "org1", day("2026-08-24"), day("2026-08-31"))
	if err != nil {
		t.Fatalf("fetchHotspotsAgg returned error: %v", err)
	}
	if len(rows) != 1 || rows[0].riskScore == nil || *rows[0].riskScore != 0.4 {
		t.Fatalf("riskScore = %v, want pointer to 0.4 (control: populated window keeps the real value)", rows[0].riskScore)
	}
}

func TestFetchComplexityAgg_EmptyWindowGuardsNaNToNil(t *testing.T) {
	client := &fakeClient{
		responses: []*fakeRowScanner{{rows: [][]any{{math.NaN(), uint64(0)}}}},
		errs:      make([]error, 1),
	}
	rows, err := fetchComplexityAgg(context.Background(), client, "org1", day("2026-08-24"), day("2026-08-31"))
	if err != nil {
		t.Fatalf("fetchComplexityAgg returned error: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected exactly 1 row, got %d", len(rows))
	}
	if rows[0].cyclomaticPerKloc != nil {
		t.Errorf("cyclomaticPerKloc = %v, want nil for complexity_known_count=0", *rows[0].cyclomaticPerKloc)
	}
}

func TestFetchComplexityAgg_PopulatedWindowKeepsExactValue(t *testing.T) {
	client := &fakeClient{
		responses: []*fakeRowScanner{{rows: [][]any{{12.5, uint64(3)}}}},
		errs:      make([]error, 1),
	}
	rows, err := fetchComplexityAgg(context.Background(), client, "org1", day("2026-08-24"), day("2026-08-31"))
	if err != nil {
		t.Fatalf("fetchComplexityAgg returned error: %v", err)
	}
	if len(rows) != 1 || rows[0].cyclomaticPerKloc == nil || *rows[0].cyclomaticPerKloc != 12.5 {
		t.Fatalf("cyclomaticPerKloc = %v, want pointer to 12.5 (control: populated window keeps the real value)", rows[0].cyclomaticPerKloc)
	}
}

func TestFetchRepoMetrics_EmptyWindowGuardsNaNToNil(t *testing.T) {
	// prs_merged=0, pr_first_review_p50_hours=NULL (Nullable column,
	// unaffected by this guard), single_owner_file_ratio_30d=NaN,
	// code_ownership_gini=NaN (deliberately un-gated, see repoMetricsRow's
	// doc comment), bus_factor=0, change_failure_rate=NaN,
	// mttr_hours=NULL, repo_metrics_known_count=0.
	client := &fakeClient{
		responses: []*fakeRowScanner{{rows: [][]any{{uint64(0), nil, math.NaN(), math.NaN(), uint32(0), math.NaN(), nil, uint64(0)}}}},
		errs:      make([]error, 1),
	}
	rows, err := fetchRepoMetrics(context.Background(), client, "org1", day("2026-08-24"), day("2026-08-31"))
	if err != nil {
		t.Fatalf("fetchRepoMetrics returned error: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected exactly 1 row, got %d", len(rows))
	}
	if rows[0].singleOwnerFileRatio30d != nil {
		t.Errorf("singleOwnerFileRatio30d = %v, want nil for repo_metrics_known_count=0", *rows[0].singleOwnerFileRatio30d)
	}
	if rows[0].changeFailureRate != nil {
		t.Errorf("changeFailureRate = %v, want nil for repo_metrics_known_count=0", *rows[0].changeFailureRate)
	}
	// codeOwnershipGini is deliberately un-gated -- still carries the raw
	// (unused) NaN. Asserting this pins the "no reachable defect, so no
	// gate" scope decision against an accidental future gate-everything
	// refactor silently changing it.
	if !math.IsNaN(rows[0].codeOwnershipGini) {
		t.Errorf("codeOwnershipGini = %v, want NaN unchanged (deliberately un-gated, unread by any section)", rows[0].codeOwnershipGini)
	}
}

func TestFetchRepoMetrics_PopulatedWindowKeepsExactValues(t *testing.T) {
	client := &fakeClient{
		responses: []*fakeRowScanner{{rows: [][]any{{uint64(20), 4.0, 0.6, 0.3, uint32(3), 0.1, 2.0, uint64(20)}}}},
		errs:      make([]error, 1),
	}
	rows, err := fetchRepoMetrics(context.Background(), client, "org1", day("2026-08-24"), day("2026-08-31"))
	if err != nil {
		t.Fatalf("fetchRepoMetrics returned error: %v", err)
	}
	if len(rows) != 1 || rows[0].singleOwnerFileRatio30d == nil || *rows[0].singleOwnerFileRatio30d != 0.6 {
		t.Fatalf("singleOwnerFileRatio30d = %v, want pointer to 0.6 (control: populated window keeps the real value)", rows[0].singleOwnerFileRatio30d)
	}
	if rows[0].changeFailureRate == nil || *rows[0].changeFailureRate != 0.1 {
		t.Fatalf("changeFailureRate = %v, want pointer to 0.1 (control: populated window keeps the real value)", rows[0].changeFailureRate)
	}
}

// ---------------------------------------------------------------------------
// aiGovernanceRatio / aiGovernanceCoverage -- the declared Go-side fix.
// Ports AIGovernanceCoverageDaily's _ratio (audit/ai_governance/models.py:
// 156-158) and _ai_governance_coverage (metrics/operating_review.py:
// 847-857) on top of it.
// ---------------------------------------------------------------------------

func TestAIGovernanceRatio_ZeroOrNegativeDenominatorReturnsOne(t *testing.T) {
	// The load-bearing edge case: "fully covered" default when there is
	// no AI activity to measure coverage against, NOT 0.0. Getting this
	// backwards would invert the metric for every org with no AI usage.
	if got := aiGovernanceRatio(5, 0); got != 1.0 {
		t.Errorf("aiGovernanceRatio(5, 0) = %v, want 1.0", got)
	}
	if got := aiGovernanceRatio(5, -1); got != 1.0 {
		t.Errorf("aiGovernanceRatio(5, -1) = %v, want 1.0", got)
	}
}

func TestAIGovernanceRatio_NormalDivision(t *testing.T) {
	if got := aiGovernanceRatio(3, 4); got != 0.75 {
		t.Errorf("aiGovernanceRatio(3, 4) = %v, want 0.75", got)
	}
}

func TestAIGovernanceCoverage_EmptyRowsReturnsZero(t *testing.T) {
	// Matches Python's ACTUAL current behavior with an empty rows list
	// (which is what production sees today, every call, because the
	// underlying query cannot execute -- see fetchAIGovernance's doc
	// comment). This is the "Python side" half of the expected
	// divergence: with NO rows, Go's fix computes the exact same 0.0
	// Python is pinned to.
	if got := aiGovernanceCoverage(nil); got != 0.0 {
		t.Errorf("aiGovernanceCoverage(nil) = %v, want 0.0", got)
	}
}

func TestAIGovernanceCoverage_RealRowsProducesNonZero(t *testing.T) {
	// This is the "Go side" half of the expected divergence: given real
	// per-group counts (which Python's broken query can never fetch),
	// the fix computes an actual ratio instead of the pinned 0.0.
	rows := []aiGovernanceRawRow{
		{aiArtifacts: 10, declaredArtifacts: 8, humanReviewedPrs: 5, securityScannedPrs: 10, inPolicyArtifacts: 9},
		{aiArtifacts: 0, declaredArtifacts: 0, humanReviewedPrs: 0, securityScannedPrs: 0, inPolicyArtifacts: 0}, // no AI activity -> every ratio 1.0
	}
	got := aiGovernanceCoverage(rows)
	if got <= 0.0 {
		t.Fatalf("aiGovernanceCoverage(real rows) = %v, want > 0 (Python is pinned to exactly 0.0 -- a non-zero result is the fix taking effect)", got)
	}
	// declaration: avg(8/10, 1.0) = 0.9 ; human_review: avg(5/10, 1.0) = 0.75
	// security: avg(10/10, 1.0) = 1.0 ; in_policy: avg(9/10, 1.0) = 0.95
	// all > 0 -> present = all four -> mean = (0.9+0.75+1.0+0.95)/4 = 0.9
	want := 0.9
	if diff := got - want; diff > 1e-9 || diff < -1e-9 {
		t.Errorf("aiGovernanceCoverage = %v, want %v", got, want)
	}
}

// ---------------------------------------------------------------------------
// investmentKey / investmentUnits -- ports _investment_key/_investment_units
// (metrics/operating_review.py:883-902).
// ---------------------------------------------------------------------------

func TestInvestmentKey(t *testing.T) {
	cases := map[string]string{
		"ktlo":                  "ktlo",
		"maintenance":           "ktlo",
		"maintenance_tech_debt": "ktlo",
		"new_value":             "new_value",
		"feature_delivery":      "new_value",
		"features":              "new_value",
		"security":              "security",
		"risk_security":         "security",
		"infra":                 "infra",
		"infrastructure":        "infra",
		"operational_support":   "infra",
	}
	for input, want := range cases {
		got, ok := investmentKey(input)
		if !ok || got != want {
			t.Errorf("investmentKey(%q) = (%q, %v), want (%q, true)", input, got, ok, want)
		}
	}
	if _, ok := investmentKey("something_else"); ok {
		t.Errorf("investmentKey(unknown) should return ok=false")
	}
}

func TestInvestmentUnits_SumsByKeyIgnoresUnknown(t *testing.T) {
	rows := []investmentRow{
		{investmentArea: "KTLO", deliveryUnits: 3},
		{investmentArea: " maintenance ", deliveryUnits: 2},
		{investmentArea: "new_value", deliveryUnits: 10},
		{investmentArea: "unmapped_area", deliveryUnits: 999},
	}
	units := investmentUnits(rows)
	if units["ktlo"] != 5 {
		t.Errorf("ktlo = %v, want 5", units["ktlo"])
	}
	if units["new_value"] != 10 {
		t.Errorf("new_value = %v, want 10", units["new_value"])
	}
	if units["security"] != 0 || units["infra"] != 0 {
		t.Errorf("security/infra should stay 0, got %v/%v", units["security"], units["infra"])
	}
}

// ---------------------------------------------------------------------------
// buildMetric / deltaSummary -- ports _metric/_delta_summary
// (metrics/operating_review.py:700-745).
// ---------------------------------------------------------------------------

func TestBuildMetric_ImprovedWorsenedUnchanged(t *testing.T) {
	improved := buildMetric("k", "Label", 5, 10, "u", lowerIsBetter) // lower is better, value dropped -> improved
	if improved.delta.status != "improved" {
		t.Errorf("lower-is-better decrease status = %q, want improved", improved.delta.status)
	}
	worsened := buildMetric("k", "Label", 10, 5, "u", lowerIsBetter) // increased -> worsened
	if worsened.delta.status != "worsened" {
		t.Errorf("lower-is-better increase status = %q, want worsened", worsened.delta.status)
	}
	unchanged := buildMetric("k", "Label", 5, 5, "u", higherIsBetter)
	if unchanged.delta.status != "unchanged" {
		t.Errorf("equal values status = %q, want unchanged", unchanged.delta.status)
	}
	neutralChanged := buildMetric("k", "Label", 5, 3, "u", neutral)
	if neutralChanged.delta.status != "changed" {
		t.Errorf("neutral direction status = %q, want changed", neutralChanged.delta.status)
	}
}

func TestBuildMetric_PercentNilWhenPriorZeroAndDeltaNonZero(t *testing.T) {
	m := buildMetric("k", "Label", 5, 0, "u", higherIsBetter)
	if m.delta.percent != nil {
		t.Errorf("percent = %v, want nil (prior=0, delta!=0)", *m.delta.percent)
	}
}

func TestBuildMetric_PercentZeroWhenBothZero(t *testing.T) {
	m := buildMetric("k", "Label", 0, 0, "u", higherIsBetter)
	if m.delta.percent == nil || *m.delta.percent != 0.0 {
		t.Errorf("percent = %v, want pointer to 0.0", m.delta.percent)
	}
}

func TestDeltaSummary_Format(t *testing.T) {
	m := buildMetric("throughput", "Throughput", 12, 10, "items completed", higherIsBetter)
	got := deltaSummary(m)
	want := "Throughput improved by +2.0 items completed"
	if got != want {
		t.Errorf("deltaSummary = %q, want %q", got, want)
	}
}

// ---------------------------------------------------------------------------
// Resolve -- end-to-end with a fake client, proving (a) the happy path
// computes a real payload and (b) a single swallowed query degrades only
// its own section, matching Python's per-query (not per-batch) granularity.
// ---------------------------------------------------------------------------

func TestResolve_NilClientErrors(t *testing.T) {
	_, err := Resolve(context.Background(), nil, "org1", nil, graphqldate.New(day("2026-08-24")))
	if err == nil {
		t.Fatal("Resolve(nil client) should error")
	}
}

func TestResolve_HappyPath_ComputesRealPayload(t *testing.T) {
	// One representative row per table for the CURRENT period; the PRIOR
	// period gets all-empty responses (10 scanners) so every delta is
	// "value vs 0".
	current := []*fakeRowScanner{
		{rows: [][]any{{day("2026-08-24"), uint64(10), uint64(8), uint32(4), 5.0, 9.0, 1.0, 2.0}}},                   // work_items
		{rows: [][]any{{"in_review", uint64(6), 3.0, 1.5}}},                                                          // state_durations
		{rows: [][]any{{uint64(20), 4.0, 0.6, 0.3, uint32(3), 0.1, 2.0, uint64(20)}}},                                // repo_metrics (+ CHAOS-4563 known_count)
		{rows: [][]any{{0.4, uint64(5)}}},                                                                            // hotspots
		{rows: [][]any{{12.5, uint64(3)}}},                                                                           // complexity (+ CHAOS-4563 known_count)
		{rows: [][]any{{uint64(9), uint64(1)}}},                                                                      // deployments
		{rows: [][]any{{uint64(2), 3.0}}},                                                                            // incidents
		{rows: [][]any{{"feature_delivery", uint64(7)}}},                                                             // investment
		{rows: [][]any{{"human", uint64(10), uint64(2), uint64(1), uint64(6), uint64(1), 1.0, 1.2, 0.1, 0.05, 0.0}}}, // ai_impact
		{rows: [][]any{{day("2026-08-24"), nil, nil, uint64(4), uint64(3), uint64(2), uint64(4), uint64(4)}}},        // ai_governance
	}
	client := &fakeClient{
		responses: append(append([]*fakeRowScanner{}, current...), emptyScanners(10)...),
		errs:      make([]error, 20),
	}

	got, err := Resolve(context.Background(), client, "org1", nil, graphqldate.New(day("2026-08-24")))
	if err != nil {
		t.Fatalf("Resolve returned error: %v", err)
	}
	if client.calls != 20 {
		t.Fatalf("expected 20 Query calls (10 tables x 2 periods), got %d", client.calls)
	}
	if got.OrgID != "org1" {
		t.Errorf("OrgID = %q, want org1", got.OrgID)
	}
	if len(got.Sections) != 6 {
		t.Fatalf("expected 6 sections, got %d", len(got.Sections))
	}

	section := func(key string) *sectionOut {
		for i, s := range got.Sections {
			if s.Key == key {
				return &sectionOut{idx: i}
			}
		}
		t.Fatalf("section %q not found", key)
		return nil
	}
	deliv := section("delivery_movement")
	throughput := got.Sections[deliv.idx].Metrics[1] // throughput is metric index 1 in _delivery_section
	if throughput.Key != "throughput" {
		t.Fatalf("expected throughput at index 1, got %q", throughput.Key)
	}
	if throughput.Value != 8 { // items_completed summed = 8, prior = 0
		t.Errorf("throughput value = %v, want 8", throughput.Value)
	}
	if throughput.Delta.Status != "improved" {
		t.Errorf("throughput status = %q, want improved", throughput.Delta.Status)
	}

	// ai_governance_coverage must NOT be pinned to 0.0 -- proves the
	// Go-side fix took effect (Python is pinned to exactly 0.0 here;
	// see fetchAIGovernance's doc comment for why a MATCH would be the
	// suspicious result on the real dual-run).
	aiSection := section("ai_workflow_intelligence")
	var coverageMetric *reviewMetricOut
	for _, m := range got.Sections[aiSection.idx].Metrics {
		if m.Key == "ai_governance_coverage" {
			v := reviewMetricOut{value: m.Value}
			coverageMetric = &v
		}
	}
	if coverageMetric == nil {
		t.Fatal("ai_governance_coverage metric not found")
	}
	if coverageMetric.value <= 0.0 {
		t.Errorf("ai_governance_coverage = %v, want > 0 (Python would be exactly 0.0 here)", coverageMetric.value)
	}

	// CHAOS-4563 populated-window control, paired with
	// TestResolve_EmptyWindowGuardedMetricsAreNotNaN below: a real,
	// countable window must carry through to the exact scanned value --
	// hotspots_count=5/complexity_known_count=3/repo_metrics_known_count=20
	// are all > 0 in this fixture. Without this control, the empty-window
	// test's "value == 0.0, not NaN" assertion alone could not distinguish
	// a working guard from a bug that zeroes these metrics unconditionally.
	riskSec := section("risk")
	riskMetric := func(key string) float64 {
		for _, m := range got.Sections[riskSec.idx].Metrics {
			if m.Key == key {
				return m.Value
			}
		}
		t.Fatalf("risk metric %q not found", key)
		return 0
	}
	if v := riskMetric("hotspot_risk_score"); v != 0.4 {
		t.Errorf("hotspot_risk_score = %v, want 0.4 (populated window, hotspots_count=5)", v)
	}
	if v := riskMetric("ownership_concentration"); v != 0.6 {
		t.Errorf("ownership_concentration = %v, want 0.6 (populated window, repo_metrics_known_count=20)", v)
	}
	if v := riskMetric("complexity_per_kloc"); v != 12.5 {
		t.Errorf("complexity_per_kloc = %v, want 12.5 (populated window, complexity_known_count=3)", v)
	}
}

// TestResolve_EmptyWindowGuardedMetricsAreNotNaN is CHAOS-4563's core
// acceptance test: a genuinely empty window (the live ClickHouse shape --
// one scalar-aggregate row, NaN value, companion count = 0, for EVERY one
// of the three guard sites at once) must produce Resolve success with
// well-defined 0.0 metric values, never a NaN that would fail gqlgen
// marshaling downstream (this package's own boundary is the metric Value
// float64 field; the actual marshal failure happens one layer further out,
// in generated resolver wiring this package does not own -- see the package
// doc comment). Paired with the populated-window control immediately above
// in TestResolve_HappyPath_ComputesRealPayload: same code path, same three
// metrics, non-trivially different (non-zero, exact) values there --
// without that pairing, "value == 0.0, not NaN" here could not distinguish
// the guard actually firing from a bug that always returns 0 regardless of
// the input.
func TestResolve_EmptyWindowGuardedMetricsAreNotNaN(t *testing.T) {
	responses := emptyScanners(20)
	// The exact live shape for a scalar aggregate over zero underlying
	// rows: exactly one row, NaN on the plain-Float64 aggregate, companion
	// count = 0. Deployments (call index 5) is left at its emptyScanners
	// zero-row default so change_failure_rate's sum(deployments)==0
	// branch falls through to the repo_metrics avg guard being tested,
	// rather than masking it behind the deployments>0 branch.
	responses[2] = &fakeRowScanner{rows: [][]any{{uint64(0), nil, math.NaN(), math.NaN(), uint32(0), math.NaN(), nil, uint64(0)}}} // repo_metrics
	responses[3] = &fakeRowScanner{rows: [][]any{{math.NaN(), uint64(0)}}}                                                         // hotspots
	responses[4] = &fakeRowScanner{rows: [][]any{{math.NaN(), uint64(0)}}}                                                         // complexity

	client := &fakeClient{responses: responses, errs: make([]error, 20)}

	got, err := Resolve(context.Background(), client, "org1", nil, graphqldate.New(day("2026-08-24")))
	if err != nil {
		t.Fatalf("Resolve should succeed on a genuinely empty window, got error: %v", err)
	}

	riskSec := -1
	for i, s := range got.Sections {
		if s.Key == "risk" {
			riskSec = i
		}
	}
	if riskSec < 0 {
		t.Fatal("risk section not found")
	}
	for _, key := range []string{"hotspot_risk_score", "ownership_concentration", "complexity_per_kloc"} {
		found := false
		for _, m := range got.Sections[riskSec].Metrics {
			if m.Key != key {
				continue
			}
			found = true
			if math.IsNaN(m.Value) {
				t.Errorf("%s = NaN, want a finite value (the known_count guard must have gated this out before it ever reached compute)", key)
			}
			if m.Value != 0.0 {
				t.Errorf("%s = %v, want 0.0 (genuinely empty window, matching the same absent-data convention every other field in this port already uses)", key, m.Value)
			}
		}
		if !found {
			t.Fatalf("metric %q not found in risk section", key)
		}
	}

	reliabilitySec := -1
	for i, s := range got.Sections {
		if s.Key == "reliability" {
			reliabilitySec = i
		}
	}
	if reliabilitySec < 0 {
		t.Fatal("reliability section not found")
	}
	for _, m := range got.Sections[reliabilitySec].Metrics {
		if m.Key != "change_failure_rate" {
			continue
		}
		if math.IsNaN(m.Value) {
			t.Error("change_failure_rate = NaN, want a finite value")
		}
		if m.Value != 0.0 {
			t.Errorf("change_failure_rate = %v, want 0.0 (deployments empty AND repo_metrics empty)", m.Value)
		}
	}
}

// sectionOut/reviewMetricOut are tiny local helpers so the test above can
// look sections/metrics up by key without re-deriving _section's fixed
// index order everywhere.
type sectionOut struct{ idx int }
type reviewMetricOut struct{ value float64 }

func TestResolve_SwallowsOneTableFailure_DegradesOnlyThatSection(t *testing.T) {
	// The per-query granularity claim, executed: call index 0 (current
	// period's work_items) errors; every other call (including the OTHER
	// 9 current-period tables and all 10 prior-period tables) succeeds
	// with real data. Resolve must still succeed, and every section OTHER
	// than delivery_movement's throughput/wip_count (both fed by
	// work_items) must reflect the real data -- proving a single swallow
	// does not collapse to an all-or-nothing response.
	responses := emptyScanners(20)
	// current repo_metrics (call index 2) carries real data so
	// review_latency_hours (bottleneck section) is provably non-zero,
	// independent of the swallowed work_items table.
	responses[2] = &fakeRowScanner{rows: [][]any{{uint64(5), 6.0, 0.2, 0.1, uint32(2), 0.05, 1.0, uint64(5)}}} // + CHAOS-4563 known_count

	errs := make([]error, 20)
	errs[0] = errors.New("simulated work_item_metrics_daily failure")

	client := &fakeClient{responses: responses, errs: errs}

	got, err := Resolve(context.Background(), client, "org1", nil, graphqldate.New(day("2026-08-24")))
	if err != nil {
		t.Fatalf("Resolve should swallow the single table failure, got error: %v", err)
	}
	if client.calls != 20 {
		t.Fatalf("expected all 20 calls to still be attempted, got %d", client.calls)
	}

	var bottleneck, delivery []struct {
		key   string
		value float64
	}
	for _, s := range got.Sections {
		for _, m := range s.Metrics {
			row := struct {
				key   string
				value float64
			}{m.Key, m.Value}
			if s.Key == "bottleneck" {
				bottleneck = append(bottleneck, row)
			}
			if s.Key == "delivery_movement" {
				delivery = append(delivery, row)
			}
		}
	}

	foundReviewLatency := false
	for _, m := range bottleneck {
		if m.key == "review_latency_hours" {
			foundReviewLatency = true
			if m.value != 6.0 {
				t.Errorf("review_latency_hours = %v, want 6.0 (repo_metrics table, unaffected by the swallowed work_items table)", m.value)
			}
		}
	}
	if !foundReviewLatency {
		t.Fatal("review_latency_hours metric not found")
	}

	for _, m := range delivery {
		if m.key == "throughput" && m.value != 0 {
			t.Errorf("throughput = %v, want 0 (work_items table was swallowed, defaults to empty)", m.value)
		}
	}
}

// ---------------------------------------------------------------------------
// discardOnError -- codex review round 1 (PR #2008, P2): a fetcher can
// Scan() rows successfully before the driver's iteration itself fails
// mid-stream (rows.Next() -> false, rows.Err() -> non-nil, with rows
// already appended). Verified against source before this fix (see
// discardOnError's own doc comment for the Python-side reasoning); these
// tests execute the actual failure shape, not just the helper in
// isolation, since the earlier per-table-swallow test only covered a
// query-level (zero-rows) failure.
// ---------------------------------------------------------------------------

func TestDiscardOnError(t *testing.T) {
	if got := discardOnError([]int{1, 2, 3}, nil); len(got) != 3 {
		t.Errorf("discardOnError(rows, nil) = %v, want the original 3 rows unchanged", got)
	}
	if got := discardOnError([]int{1, 2, 3}, errors.New("mid-stream failure")); got != nil {
		t.Errorf("discardOnError(rows, err) = %v, want nil (partial rows discarded)", got)
	}
}

func TestFetchPeriodRows_MidStreamFailureDiscardsPartialRows(t *testing.T) {
	// Call 0 (work_items) yields ONE successfully-scanned row, THEN the
	// driver reports an error -- rows.Next() returns false after that one
	// row, rows.Err() is non-nil. Without discardOnError, fetchPeriodRows
	// would keep that one partial row and compute a "plausible-looking"
	// throughput/cycle-time from it; with the fix, the whole table is
	// discarded, matching a query-level failure's effect exactly.
	// Deliberately no fakeClient.errs entry here (codex review round 2,
	// PR #2008: setting errs[0] makes Query itself return (nil, err)
	// immediately, short-circuiting BEFORE the scanner is ever touched --
	// that only re-exercises the already-covered query-level failure
	// path, the exact case discardOnError does not change the behavior
	// of, and the test would keep passing even with discardOnError
	// removed. The mid-stream case this test exists for is scanner-level:
	// Query succeeds, then Next()/Err() fails partway through iteration --
	// expressed by the scanner's own err/failAfterRows fields alone.
	responses := emptyScanners(10)
	responses[0] = &fakeRowScanner{
		rows: [][]any{
			{day("2026-08-24"), uint64(10), uint64(8), uint32(4), 5.0, 9.0, 1.0, 2.0},
		},
		err:           errors.New("simulated mid-stream driver failure"),
		failAfterRows: 1,
	}

	client := &fakeClient{responses: responses, errs: make([]error, 10)}

	result := fetchPeriodRows(context.Background(), client, "org1", nil, day("2026-08-24"))

	if len(result.workItems) != 0 {
		t.Fatalf("workItems = %d rows, want 0 -- the one successfully-scanned row before the mid-stream failure must be discarded, not kept", len(result.workItems))
	}
	if client.calls != 10 {
		t.Fatalf("expected 10 calls (one full period), got %d", client.calls)
	}
}

// ---------------------------------------------------------------------------
// rootCause -- chris's cross-lane finding, sourced from Lane A's codex
// round: the REAL QueryClient (github.com/full-chaos/dev-health-go/
// clickhouse.Client) wraps every driver error as an unexported
// operationError whose Error() returns ONLY a fixed "ClickHouse
// <operation> failed" string, discarding the real driver message even
// though its Unwrap() still returns it. fakeOperationError below is a
// deliberate MIRROR of that exact shape (same Error()/Unwrap() contract,
// same "fixed string, real cause reachable only via Unwrap" behavior) --
// a fake that just wraps with fmt.Errorf's own %w would trivially pass
// here regardless of whether rootCause worked at all, since %w already
// embeds the wrapped message in its own Error() text. This is the same
// "pin the fix with a fake that mirrors the real shape" discipline
// discardOnError's own tests already apply.
// ---------------------------------------------------------------------------

type fakeOperationError struct {
	operation string
	cause     error
}

func (e *fakeOperationError) Error() string { return "ClickHouse " + e.operation + " failed" }
func (e *fakeOperationError) Unwrap() error { return e.cause }

func TestRootCause_RecoversDetailLostByOperationErrorShapedWrapper(t *testing.T) {
	driverErr := errors.New("Code: 47. DB::Exception: Unknown expression or function identifier `declaration_coverage`")
	wrapped := &fakeOperationError{operation: "query", cause: driverErr}
	// This package's own fetch functions wrap client errors one more
	// level with fmt.Errorf("...: %w", err) -- reproduce that too, so
	// this test exercises the SAME two-level chain errSwallow actually
	// receives (fmt.Errorf wrapper -> fakeOperationError -> driver error).
	doubleWrapped := fmt.Errorf("operatingreview: ai_governance query: %w", wrapped)

	if got := wrapped.Error(); got != "ClickHouse query failed" {
		t.Fatalf("sanity check failed: fakeOperationError.Error() = %q, want the fixed string (confirms the fake actually mirrors the real defect)", got)
	}

	got := rootCause(doubleWrapped)
	if got != driverErr.Error() {
		t.Errorf("rootCause(doubleWrapped) = %q, want the real driver message %q -- rootCause must walk PAST fakeOperationError's fixed-string Error(), not stop there", got, driverErr.Error())
	}
}

func TestRootCause_NoWrapping_ReturnsErrorItself(t *testing.T) {
	plain := errors.New("plain failure, no wrapping")
	if got := rootCause(plain); got != plain.Error() {
		t.Errorf("rootCause(plain) = %q, want %q", got, plain.Error())
	}
}

func TestErrSwallow_LogsRootCauseNotJustFixedString(t *testing.T) {
	// End-to-end proof at the level errSwallow ACTUALLY runs at (not just
	// rootCause in isolation): capture errSwallow's real log.Printf
	// output for the exact chain shape production code produces, and
	// assert the captured text carries the real driver detail -- not
	// just the fixed "ClickHouse query failed" string every table would
	// otherwise share regardless of cause.
	var buf bytes.Buffer
	prevOutput := log.Writer()
	prevFlags := log.Flags()
	log.SetOutput(&buf)
	log.SetFlags(0)
	defer func() {
		log.SetOutput(prevOutput)
		log.SetFlags(prevFlags)
	}()

	driverErr := errors.New("Code: 47. DB::Exception: Unknown expression or function identifier `declaration_coverage`")
	wrapped := &fakeOperationError{operation: "query", cause: driverErr}
	doubleWrapped := fmt.Errorf("operatingreview: ai_governance query: %w", wrapped)

	errSwallow(context.Background(), "ai_governance", doubleWrapped)

	logged := buf.String()
	if !strings.Contains(logged, driverErr.Error()) {
		t.Fatalf("errSwallow's logged output = %q, want it to contain the real driver message %q -- every table's failure would otherwise log as indistinguishable fixed-string text", logged, driverErr.Error())
	}
}
