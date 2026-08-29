package operatingreview

import (
	"context"
	"errors"
	"testing"
	"time"

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
		{rows: [][]any{{uint64(20), 4.0, 0.6, 0.3, uint32(3), 0.1, 2.0}}},                                            // repo_metrics
		{rows: [][]any{{0.4, uint64(5)}}},                                                                            // hotspots
		{rows: [][]any{{12.5}}},                                                                                      // complexity
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
	responses[2] = &fakeRowScanner{rows: [][]any{{uint64(5), 6.0, 0.2, 0.1, uint32(2), 0.05, 1.0}}}

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
