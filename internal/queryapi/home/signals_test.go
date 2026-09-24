package home

import (
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
		{Metric: "a", Label: "A", DeltaPct: -10},
		{Metric: "b", Label: "B", DeltaPct: 30},
		{Metric: "c", Label: "C", DeltaPct: 5},
	}
	got := SelectConstraint(deltas)
	if got.Metric != "b" {
		t.Fatalf("SelectConstraint = %q, want %q", got.Metric, "b")
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
