package home

import (
	"encoding/json"
	"reflect"
	"sort"
	"strings"
	"testing"
)

func TestSeverityForImpact(t *testing.T) {
	cases := []struct {
		impact float64
		want   string
	}{
		{60, "critical"}, {59.9, "high"}, {35, "high"}, {34.9, "medium"}, {15, "medium"}, {14.9, "low"}, {0, "low"},
	}
	for _, tc := range cases {
		if got := severityForImpact(tc.impact); got != tc.want {
			t.Errorf("severityForImpact(%v) = %q, want %q", tc.impact, got, tc.want)
		}
	}
}

func TestMetricImpactWorsenedVsImproved(t *testing.T) {
	// cycle_time is lower-is-better: a positive delta (got worse) keeps
	// full magnitude; a negative delta (improved) is dampened 0.35x.
	if got := metricImpact("cycle_time", 40); got != 40 {
		t.Errorf("worsened cycle_time impact = %v, want 40", got)
	}
	if got := metricImpact("cycle_time", -40); got != 14 {
		t.Errorf("improved cycle_time impact = %v, want 14", got)
	}
	// throughput is higher-is-better: negative delta (got worse) keeps
	// full magnitude.
	if got := metricImpact("throughput", -40); got != 40 {
		t.Errorf("worsened throughput impact = %v, want 40", got)
	}
	if got := metricImpact("throughput", 40); got != 14 {
		t.Errorf("improved throughput impact = %v, want 14", got)
	}
	if got := metricImpact("throughput", 0.5); got != 0 {
		t.Errorf("flat-band impact = %v, want 0", got)
	}
}

func TestRankSignalsStableOnTies(t *testing.T) {
	signals := []Signal{
		{ID: "a", Severity: "high", EvidenceCount: 1},
		{ID: "b", Severity: "high", EvidenceCount: 1},
		{ID: "c", Severity: "critical", EvidenceCount: 0},
	}
	ranked := RankSignals(signals)
	if len(ranked) != 3 || ranked[0].ID != "c" || ranked[1].ID != "a" || ranked[2].ID != "b" {
		ids := []string{ranked[0].ID, ranked[1].ID, ranked[2].ID}
		t.Fatalf("RankSignals order = %v, want [c a b] (critical first, then input order on ties)", ids)
	}
}

func TestBuildDataConfidenceLevels(t *testing.T) {
	high := BuildDataConfidence(map[string]float64{"a": 80, "b": 90}, map[string]string{"github": "ok"})
	if high.Level != "high" {
		t.Errorf("high coverage/no missing = %q, want high", high.Level)
	}
	medium := BuildDataConfidence(map[string]float64{"a": 50}, map[string]string{"github": "ok"})
	if medium.Level != "medium" {
		t.Errorf("medium coverage = %q, want medium", medium.Level)
	}
	low := BuildDataConfidence(map[string]float64{}, map[string]string{})
	if low.Level != "low" || low.CoveragePct != nil {
		t.Errorf("no coverage data = %+v, want low/nil", low)
	}
	if len(low.Caveats) == 0 {
		t.Error("expected a caveat when coverage cannot be computed")
	}
}

func TestRiskSignalSuppressesBareUUIDLabel(t *testing.T) {
	score := 0.42
	row := RiskRow{Scope: "repo", ScopeID: "r1", Score: &score, Severity: "high", ScopeDisplayName: "5c1f1b2a-1111-2222-3333-444455556666"}
	if _, ok := RiskSignal(row, Filters{}, DataConfidence{}); ok {
		t.Fatal("expected RiskSignal to suppress a bare-UUID display name (A8)")
	}
}

func TestRiskSignalSuppressesEmptyScopeID(t *testing.T) {
	score := 0.42
	row := RiskRow{Scope: "repo", ScopeID: "", Score: &score, Severity: "high", ScopeDisplayName: "checkout-service"}
	if _, ok := RiskSignal(row, Filters{}, DataConfidence{}); ok {
		t.Fatal("expected RiskSignal to suppress an empty scope id (B7)")
	}
}

func TestRecommendationSignalRequiresTitle(t *testing.T) {
	if _, ok := RecommendationSignal(RecommendationRow{}, Filters{}, DataConfidence{}); ok {
		t.Fatal("expected RecommendationSignal to reject an empty title")
	}
}

func TestSelectConstraintPicksHighestDeltaPct(t *testing.T) {
	deltas := []MetricDelta{
		{Metric: "a", Label: "A", DeltaPct: pctp(-10), HasData: true, HasPriorData: true},
		{Metric: "b", Label: "B", DeltaPct: pctp(30), HasData: true, HasPriorData: true},
		{Metric: "c", Label: "C", DeltaPct: pctp(5), HasData: true, HasPriorData: true},
	}
	got, ok := SelectConstraint(deltas)
	if !ok || got.Metric != "b" {
		t.Fatalf("SelectConstraint = %q (ok %v), want %q", got.Metric, ok, "b")
	}
}

// A constraint is a claim about a move between two measured values: a metric
// with data in one window only is not named, and with no such metric there is
// no constraint (not "Cycle Time held steady 0%").
func TestSelectConstraintNeedsTwoMeasuredWindows(t *testing.T) {
	oneSided := []MetricDelta{
		{Metric: "a", Label: "A", DeltaPct: pctp(0), HasData: true, HasPriorData: false},
		{Metric: "b", Label: "B", DeltaPct: pctp(0), HasData: false, HasPriorData: true},
	}
	if got, ok := SelectConstraint(oneSided); ok {
		t.Fatalf("SelectConstraint over one-sided deltas = %q, want none", got.Metric)
	}
	mixed := append(oneSided, MetricDelta{Metric: "c", Label: "C", DeltaPct: pctp(-5), HasData: true, HasPriorData: true})
	if got, ok := SelectConstraint(mixed); !ok || got.Metric != "c" {
		t.Fatalf("SelectConstraint = %q (ok %v), want the only complete delta c", got.Metric, ok)
	}
	if _, ok := topDeltaByMagnitude(oneSided); ok {
		t.Fatal("topDeltaByMagnitude named a one-sided delta")
	}
}

// A signal of a metric with a value in this window only keeps its current
// value but states no prior value, no delta and no direction claim.
func TestMetricSignalOfAOneSidedDeltaStatesNoMove(t *testing.T) {
	deltas := []MetricDelta{
		{Metric: "churn", Label: "Code Churn", Unit: "loc", Value: 5, DeltaPct: pctp(0), HasData: true, HasPriorData: false},
		{Metric: "cycle_time", Label: "Cycle Time", Unit: "days", Value: 4, DeltaPct: pctp(-20), HasData: true, HasPriorData: true},
	}
	signals := BuildMetricSignals(deltas, Filters{}, DataConfidence{})
	byMetric := map[string]Signal{}
	for _, signal := range signals {
		byMetric[signal.Metric] = signal
	}
	one := byMetric["churn"]
	if one.PriorValue != nil || one.Delta != nil || one.CurrentValue != "5 loc" {
		t.Errorf("one-sided signal = current %q prior %v delta %v, want current 5 loc and no prior value and no delta", one.CurrentValue, one.PriorValue, one.Delta)
	}
	if strings.Contains(one.Title, "flat") || strings.Contains(one.WhyItMatters, "flat") || strings.Contains(one.Title, "steady") {
		t.Errorf("one-sided signal claims a trend: %q / %q", one.Title, one.WhyItMatters)
	}
	two := byMetric["cycle_time"]
	if two.PriorValue == nil || two.Delta == nil || *two.Delta != "-20%" {
		t.Errorf("two-sided signal = prior %v delta %v, want a prior value and -20%%", two.PriorValue, two.Delta)
	}
}

// CHAOS-8178: a served signal value groups the digits of its whole part in
// threes. The web shows the string as served, so "3387254 loc" was what a
// reader saw.
func TestFormatValueGroupsTheWholePartInThrees(t *testing.T) {
	for _, tc := range []struct {
		value float64
		unit  string
		want  string
	}{
		{999, "loc", "999 loc"},              // below the first group: unchanged
		{1000, "loc", "1,000 loc"},           // the first value that changes
		{3387254, "loc", "3,387,254 loc"},    // the value of the ticket
		{1234567.8, "", "1,234,568"},         // 100 or more: no fraction, then grouped
		{-12345.7, "hours", "-12,346 hours"}, // the sign stays in front
		{-999, "hours", "-999 hours"},
		{100000, "items", "100,000 items"},
		{99.5, "%", "99.5 %"}, // below 100 with a fraction: one decimal, no group
		{12.25, "days", "12.2 days"},
		{0, "items", "0 items"},
	} {
		if got := formatValue(tc.value, tc.unit); got != tc.want {
			t.Errorf("formatValue(%v, %q) = %q, want %q", tc.value, tc.unit, got, tc.want)
		}
	}
}

func TestGroupThousands(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"0", "0"},
		{"12", "12"},
		{"123", "123"},
		{"1234", "1,234"},
		{"12345", "12,345"},
		{"123456", "123,456"},
		{"1234567", "1,234,567"},
		{"-1234", "-1,234"},
		{"-123", "-123"},
		{"1234.5", "1,234.5"},
		{"-1234567.8", "-1,234,567.8"},
		{"0.5", "0.5"},
		// %f of a value that is not finite: left as it is, never grouped.
		{"NaN", "NaN"},
		{"+Inf", "+Inf"},
		{"-Inf", "-Inf"},
	} {
		if got := groupThousands(tc.in); got != tc.want {
			t.Errorf("groupThousands(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// The signal a caller reads carries the grouped value, current and prior.
func TestBuildMetricSignalsServesGroupedValues(t *testing.T) {
	deltas := []MetricDelta{{Metric: "churn", Label: "Code Churn", Unit: "loc", Value: 3387254, DeltaPct: pctp(25), HasData: true, HasPriorData: true}}
	signals := BuildMetricSignals(deltas, Filters{Scope: ScopeFilter{Level: "org"}}, DataConfidence{})
	if len(signals) != 1 {
		t.Fatalf("signals = %d, want 1", len(signals))
	}
	if signals[0].CurrentValue != "3,387,254 loc" {
		t.Errorf("currentValue = %q, want %q", signals[0].CurrentValue, "3,387,254 loc")
	}
	if signals[0].PriorValue == nil || *signals[0].PriorValue != "2,709,803 loc" {
		t.Errorf("priorValue = %v, want 2,709,803 loc (3387254 / 1.25, grouped)", signals[0].PriorValue)
	}
}

func TestFormatValueIntegerVsDecimal(t *testing.T) {
	if got := formatValue(3.0, "days"); got != "3 days" {
		t.Errorf("formatValue(3.0) = %q, want %q", got, "3 days")
	}
	if got := formatValue(3.25, "days"); got != "3.2 days" {
		t.Errorf("formatValue(3.25) = %q, want %q", got, "3.2 days")
	}
	if got := formatValue(150.5, ""); got != "150" {
		t.Errorf("formatValue(150.5) = %q, want %q", got, "150")
	}
}

// The risk signal carries the coverage of the score it shows (CHAOS-6545); every
// other signal has none.
func TestRiskSignalCarriesTheCoverageOfItsScore(t *testing.T) {
	score, coverage := 0.9, 0.3
	row := RiskRow{Scope: "repo", ScopeID: "r1", Score: &score, Severity: "high", ScopeDisplayName: "checkout-service", Coverage: &coverage}
	signal, ok := RiskSignal(row, DefaultFilters(), DataConfidence{})
	if !ok || signal.Coverage == nil || *signal.Coverage != 0.3 {
		t.Fatalf("risk signal coverage = %v (ok %v), want 0.3", signal.Coverage, ok)
	}
	row.Coverage = nil
	signal, ok = RiskSignal(row, DefaultFilters(), DataConfidence{})
	if !ok || signal.Coverage != nil {
		t.Fatalf("a row with no coverage gives a signal with none: %v", signal.Coverage)
	}
}

// From a measured 0 (both windows stored, prior 0, current not 0) the percent
// is undefined: the sentence and the signal state the change in absolute values,
// never "held steady", "flat", a percent or the current value as the prior.
func TestAMeasuredZeroPriorIsNeverSteadyOrAPercent(t *testing.T) {
	fromZero := MetricDelta{Metric: "churn", Label: "Code Churn", Unit: "loc", Value: 5, DeltaPct: nil, HasData: true, HasPriorData: true}
	if got := MoveWords(fromZero); got != "rose from 0 loc to 5 loc" {
		t.Errorf("MoveWords from a measured zero = %q", got)
	}
	fell := MetricDelta{Metric: "churn", Label: "Code Churn", Unit: "loc", Value: -5, DeltaPct: nil, HasData: true, HasPriorData: true}
	if got := MoveWords(fell); got != "fell from 0 loc to -5 loc" {
		t.Errorf("MoveWords from a measured zero to a negative value = %q", got)
	}
	// Both windows 0 is a true 0 %: steady.
	steady := MetricDelta{Metric: "throughput", Label: "Throughput", Unit: "items", Value: 0, DeltaPct: pctp(0), HasData: true, HasPriorData: true}
	if got := MoveWords(steady); got != "held steady 0%" {
		t.Errorf("MoveWords 0 -> 0 = %q, want held steady 0%%", got)
	}

	signals := BuildMetricSignals([]MetricDelta{fromZero}, Filters{}, DataConfidence{})
	if len(signals) != 1 {
		t.Fatalf("signals = %d, want 1", len(signals))
	}
	s := signals[0]
	if s.PriorValue == nil || *s.PriorValue != "0 loc" || s.Delta == nil || *s.Delta != "+5 loc" || s.Direction != "up" || s.CurrentValue != "5 loc" {
		t.Errorf("signal = current %q prior %v delta %v direction %q, want 5 loc, the measured 0 loc, +5 loc, up", s.CurrentValue, s.PriorValue, s.Delta, s.Direction)
	}
	if strings.Contains(s.Title, "flat") || strings.Contains(s.Title, "steady") || strings.Contains(s.WhyItMatters, "flat") {
		t.Errorf("signal claims no change: %q / %q", s.Title, s.WhyItMatters)
	}
	if s.Delta != nil && strings.Contains(*s.Delta, "%") {
		t.Errorf("signal delta %q states a percent against zero", *s.Delta)
	}

	// With a defined percent elsewhere, that delta ranks first; with none, the
	// rise from a measured 0 is still named.
	defined := MetricDelta{Metric: "cycle_time", Label: "Cycle Time", Value: 4, DeltaPct: pctp(-20), HasData: true, HasPriorData: true}
	if got, ok := topDeltaByMagnitude([]MetricDelta{fromZero, defined}); !ok || got.Metric != "cycle_time" {
		t.Errorf("topDeltaByMagnitude = %q (ok %v), want the delta with a defined percent", got.Metric, ok)
	}
	if got, ok := topDeltaByMagnitude([]MetricDelta{steady, fromZero}); !ok || got.Metric != "churn" {
		t.Errorf("topDeltaByMagnitude over a true 0 %% and a rise from zero = %q (ok %v), want the rise from zero", got.Metric, ok)
	}
	if got, ok := topDeltaByMagnitude([]MetricDelta{fromZero}); !ok || got.Metric != "churn" {
		t.Errorf("topDeltaByMagnitude over a rise from zero only = %q (ok %v), want churn", got.Metric, ok)
	}
	if got, ok := SelectConstraint([]MetricDelta{fromZero}); !ok || got.Metric != "churn" {
		t.Errorf("SelectConstraint over a rise from zero only = %q (ok %v), want churn", got.Metric, ok)
	}
}

// A metric signal carries the repository-filter flag of the metric it is built from
// (CHAOS-9093), and a signal that is not built from a metric spec carries none.
func TestMetricSignalCarriesTheRepositoryFilterFlagOfItsMetric(t *testing.T) {
	yes, no := true, false
	signals := BuildMetricSignals([]MetricDelta{
		{Metric: "churn", Label: "Code Churn", Unit: "loc", Value: 5, DeltaPct: pctp(10), HasData: true, HasPriorData: true, RepoFilterApplied: &yes},
		{Metric: "throughput", Label: "Throughput", Unit: "items", Value: 5, DeltaPct: pctp(10), HasData: true, HasPriorData: true, RepoFilterApplied: &no},
		{Metric: "cycle_time", Label: "Cycle Time", Unit: "days", Value: 5, DeltaPct: pctp(10), HasData: true, HasPriorData: true},
	}, Filters{}, DataConfidence{})
	byMetric := map[string]Signal{}
	for _, signal := range signals {
		byMetric[signal.Metric] = signal
	}
	if got := byMetric["churn"].RepoFilterApplied; got == nil || !*got {
		t.Errorf("churn signal repoFilterApplied = %v, want true", got)
	}
	if got := byMetric["throughput"].RepoFilterApplied; got == nil || *got {
		t.Errorf("throughput signal repoFilterApplied = %v, want false", got)
	}
	if got := byMetric["cycle_time"].RepoFilterApplied; got != nil {
		t.Errorf("cycle_time signal repoFilterApplied = %v, want null (the request names no repository)", got)
	}
	score := 0.9
	if risk, ok := RiskSignal(RiskRow{Scope: "repo", ScopeID: "r1", Score: &score, Severity: "high", ScopeDisplayName: "checkout"}, DefaultFilters(), DataConfidence{}); !ok || risk.RepoFilterApplied != nil {
		t.Errorf("a risk signal is not built from a metric spec: repoFilterApplied = %v (ok %v), want null", risk.RepoFilterApplied, ok)
	}
}

// repoFilterApplied is null with no repository named (a team scope alone
// included); otherwise true for a repository-keyed metric (also when the named
// repositories resolved to nothing: the filter was applied and the metric has no
// data) and false only for a team-keyed metric.
func TestRepoFilterAppliedFollowsTheSpecScope(t *testing.T) {
	named := Filters{Scope: ScopeFilter{Level: "repo", IDs: []string{"r1"}}}
	byWhat := Filters{What: WhatFilter{Repos: []string{"r1"}}}
	teamOnly := Filters{Scope: ScopeFilter{Level: "team", IDs: []string{"t1"}}}
	teamAndRepos := Filters{Scope: ScopeFilter{Level: "team", IDs: []string{"t1"}}, What: WhatFilter{Repos: []string{"r1"}}}
	for _, tc := range []struct {
		name        string
		f           Filters
		metricScope string
		want        string
	}{
		{"nothing named, repo metric", Filters{}, "repo", "nil"},
		{"nothing named, team metric", Filters{}, "team", "nil"},
		{"a team scope alone, repo metric", teamOnly, "repo", "nil"},
		{"a team scope alone, team metric", teamOnly, "team", "nil"},
		{"repo scope, repo metric", named, "repo", "true"},
		{"what.repos, repo metric", byWhat, "repo", "true"},
		{"repo scope, team metric", named, "team", "true"},
		// A team metric of a team-scope request carries a team condition, which is not a repository filter.
		{"team scope and what.repos, team metric", teamAndRepos, "team", "true"},
		{"team scope and what.repos, repo metric", teamAndRepos, "repo", "true"},
	} {
		got := "nil"
		if p := repoFilterApplied(tc.f, tc.metricScope); p != nil {
			got = map[bool]string{true: "true", false: "false"}[*p]
		}
		if got != tc.want {
			t.Errorf("%s: repoFilterApplied = %s, want %s", tc.name, got, tc.want)
		}
	}
}

// The REST Home answer carries NO coverage key: the field exists on the Go
// signal and is served by GraphQL only (CHAOS-6545), because the REST response
// model is pinned by a Python-recorded golden. The key set of a signal that
// holds a coverage is the marshalled bytes' key set, not a Go snapshot.
func TestRESTSignalJSONHasNoCoverageKey(t *testing.T) {
	coverage := 0.3
	raw, err := json.Marshal(Signal{ID: "risk:repo:r1", Metric: "compounding_risk", Coverage: &coverage})
	if err != nil {
		t.Fatal(err)
	}
	var keys map[string]json.RawMessage
	if err := json.Unmarshal(raw, &keys); err != nil {
		t.Fatal(err)
	}
	got := make([]string, 0, len(keys))
	for key := range keys {
		got = append(got, key)
	}
	sort.Strings(got)
	want := []string{
		"affected_scope", "category", "confidence", "current_value", "delta", "direction", "evidence_count",
		"evidence_ref", "id", "metric", "prior_value", "recommended_action", "scope_entity", "severity", "title", "why_it_matters",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("REST signal keys = %v, want %v (no coverage key)", got, want)
	}
	if strings.Contains(string(raw), "coverage") {
		t.Fatalf("a REST signal must not carry coverage: %s", raw)
	}
}

// CHAOS-9111: a metric with no stored value in a window has a null percent, and a null
// percent is nobody's mover: it is neither the top move nor "held steady".
func TestNoDataDeltaIsNeitherTheTopMoverNorSteady(t *testing.T) {
	noData := MetricDelta{Metric: "cycle_time", Label: "Cycle Time"}
	if _, ok := noData.Percent(); ok {
		t.Fatal("a delta with no data has a percent")
	}
	top, ok := topDeltaByMagnitude([]MetricDelta{noData, {Metric: "throughput", HasData: true, HasPriorData: true, DeltaPct: func() *float64 { v := 25.0; return &v }()}})
	if !ok || top.Metric != "throughput" {
		t.Errorf("top mover = %v %v, want throughput", top.Metric, ok)
	}
	if got, ok := topDeltaByMagnitude([]MetricDelta{noData}); ok {
		t.Errorf("a response with only a no-data metric names a top mover: %v", got.Metric)
	}
}
