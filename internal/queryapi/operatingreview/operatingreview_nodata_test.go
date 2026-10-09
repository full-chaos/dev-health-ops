package operatingreview

import (
	"context"
	"errors"
	"math"
	"sort"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/changefailure"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graphqldate"
)

// CHAOS-8115: every metric says whether its week holds a stored value
// (hasData) and whether the prior week does (delta.hasPriorData). The values
// themselves are unchanged (the frozen golden pins them): a missing week and a
// stored zero both give value 0, and only these two fields tell them apart.

// allMetricKeys are the 26 metrics of the review, in section order.
var allMetricKeys = []string{
	"cycle_time_p50_hours", "throughput", "wip_count",
	"state_duration_hours", "review_latency_hours", "wip_age_p90_hours",
	"hotspot_risk_score", "ownership_concentration", "complexity_per_kloc", "bus_factor",
	"deployments_count", "change_failure_rate", "deployment_failure_rate", "revert_rate", "incidents_count", "mttr_hours",
	"ktlo_units", "new_value_units", "security_units", "infra_units",
	"ai_adoption_ratio", "ai_cycle_time_delta_hours", "ai_review_amplification", "ai_risk_drag",
	"ai_governance_coverage", "ai_opportunity_signals",
}

func metricsByKey(t *testing.T, review *model.OperatingReview) map[string]model.OperatingReviewMetric {
	t.Helper()
	out := map[string]model.OperatingReviewMetric{}
	for _, section := range review.Sections {
		for _, m := range section.Metrics {
			if m.Delta == nil {
				t.Fatalf("%s has no delta", m.Key)
			}
			out[m.Key] = m
		}
	}
	if len(out) != len(allMetricKeys) {
		t.Fatalf("the review has %d metrics, want %d", len(out), len(allMetricKeys))
	}
	for _, key := range allMetricKeys {
		if _, ok := out[key]; !ok {
			t.Fatalf("the review has no metric %q", key)
		}
	}
	return out
}

// withData lists the metrics whose week (or prior week) holds data, sorted.
func withData(metrics map[string]model.OperatingReviewMetric, prior bool) []string {
	var out []string
	for key, m := range metrics {
		if (prior && m.Delta.HasPriorData) || (!prior && m.HasData) {
			out = append(out, key)
		}
	}
	sort.Strings(out)
	return out
}

func sorted(keys ...string) []string {
	out := append([]string(nil), keys...)
	sort.Strings(out)
	return out
}

func sameKeys(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// emptyWeekScanners is what ClickHouse answers for a week with no stored row:
// the grouped reads return no row, and each scalar aggregate returns ONE row of
// zeros / NULL / NaN with a known count of 0.
func emptyWeekScanners() []*fakeRowScanner {
	out := emptyScanners(11)
	out[2] = &fakeRowScanner{rows: [][]any{{uint64(0), nil, math.NaN(), math.NaN(), uint32(0), nil, nil, uint64(0)}}} // repo_metrics
	out[3] = &fakeRowScanner{rows: [][]any{{math.NaN(), uint64(0)}}}                                                  // hotspots
	out[4] = &fakeRowScanner{rows: [][]any{{math.NaN(), uint64(0)}}}                                                  // complexity
	out[5] = &fakeRowScanner{rows: [][]any{{uint64(0), uint64(0), uint64(0)}}}                                        // deployments
	out[6] = &fakeRowScanner{rows: [][]any{{uint64(0), nil, uint64(0)}}}                                              // incidents
	out[10] = &fakeRowScanner{rows: [][]any{{uint64(0), uint64(0), uint64(0), uint64(0), uint64(0), uint64(0)}}}      // change_failure
	return out
}

// storedZeroWeekScanners is a week in which every table holds a stored row and
// every stored value is 0 (the hotspot read keeps only risk scores above 0, so
// its value is the one that cannot be a stored zero).
func storedZeroWeekScanners() []*fakeRowScanner {
	return []*fakeRowScanner{
		{rows: [][]any{{day("2026-08-24"), uint64(0), uint64(0), uint32(0), 0.0, 0.0, 0.0, 0.0}}},                  // work_items
		{rows: [][]any{{"todo", uint64(0), 0.0, 0.0}}},                                                             // state_durations
		{rows: [][]any{{uint64(0), 0.0, 0.0, 0.0, uint32(0), 0.0, 0.0, uint64(1)}}},                                // repo_metrics
		{rows: [][]any{{0.4, uint64(1)}}},                                                                          // hotspots
		{rows: [][]any{{0.0, uint64(1)}}},                                                                          // complexity
		{rows: [][]any{{uint64(1), uint64(0), uint64(1)}}},                                                         // deployments
		{rows: [][]any{{uint64(0), 0.0, uint64(1)}}},                                                               // incidents
		{rows: [][]any{{"feature_delivery", uint64(0)}}},                                                           // investment
		{rows: [][]any{{"human", uint64(5), uint64(0), uint64(0), uint64(5), uint64(0), 0.0, 0.0, 0.0, 0.0, 0.0}}}, // ai_impact
		{rows: [][]any{{day("2026-08-24"), nil, nil, uint64(0), uint64(0), uint64(0), uint64(0), uint64(0)}}},      // ai_governance
		{rows: [][]any{{uint64(1), uint64(0), uint64(0), uint64(1), uint64(0), uint64(1)}}},                        // change_failure
	}
}

func resolveWeeks(t *testing.T, current, prior []*fakeRowScanner, errs []error) map[string]model.OperatingReviewMetric {
	t.Helper()
	return metricsByKey(t, resolveReviewWeeks(t, current, prior, errs))
}

// resolveReviewWeeks reaches the production Resolve builder with the same
// eleven-current-then-eleven-prior read schedule used by the GraphQL resolver.
// Tests that inspect review-level statements and recommendations use this
// rather than constructing a review or section directly.
func resolveReviewWeeks(t *testing.T, current, prior []*fakeRowScanner, errs []error) *model.OperatingReview {
	t.Helper()
	client := &fakeClient{responses: append(append([]*fakeRowScanner{}, current...), prior...), errs: errs}
	review, err := Resolve(context.Background(), client, "org-1", nil, graphqldate.New(day("2026-08-24")))
	if err != nil {
		t.Fatal(err)
	}
	if client.calls != 22 {
		t.Fatalf("Resolve made %d reads, want 22", client.calls)
	}
	return review
}

// The pair the ticket exists for: a stored zero and a missing week give the
// same value, and hasData separates them.
func TestHasData_SeparatesAStoredZeroFromAMissingWeek(t *testing.T) {
	stored := resolveWeeks(t, storedZeroWeekScanners(), storedZeroWeekScanners(), nil)
	missing := resolveWeeks(t, emptyWeekScanners(), emptyWeekScanners(), nil)

	if got := withData(stored, false); !sameKeys(got, sorted(allMetricKeys...)) {
		t.Errorf("stored week: hasData is true for %v, want all %d metrics", got, len(allMetricKeys))
	}
	if got := withData(stored, true); !sameKeys(got, sorted(allMetricKeys...)) {
		t.Errorf("stored prior week: hasPriorData is true for %v, want all %d metrics", got, len(allMetricKeys))
	}
	if got := withData(missing, false); len(got) != 0 {
		t.Errorf("missing week: hasData is true for %v, want none", got)
	}
	if got := withData(missing, true); len(got) != 0 {
		t.Errorf("missing prior week: hasPriorData is true for %v, want none", got)
	}

	zeros := 0
	for _, key := range allMetricKeys {
		s, m := stored[key], missing[key]
		if m.Value != 0 || m.Delta.PriorValue != 0 {
			t.Errorf("%s: a missing week gives value %v / prior %v, want the 0 placeholder", key, m.Value, m.Delta.PriorValue)
		}
		if math.IsNaN(s.Value) || math.IsNaN(m.Value) {
			t.Errorf("%s: NaN reached the answer", key)
		}
		// ai_governance_coverage reads "no AI artifact" as fully covered (1.0), and the hotspot
		// read cannot store a zero; every other stored-zero metric is 0, the same value as missing.
		// deployments_count is 1: a deployment and failure rate is only defined
		// over at least one deployment, so a stored-zero rate needs one.
		if s.Value == 0 {
			zeros++
		} else if key != "hotspot_risk_score" && key != "ai_governance_coverage" && key != "deployments_count" {
			t.Errorf("%s: stored-zero week gives %v, want 0", key, s.Value)
		}
	}
	if zeros != len(allMetricKeys)-3 {
		t.Errorf("%d metrics are a stored 0 with hasData true, want %d: the zero/missing pair is not measured", zeros, len(allMetricKeys)-3)
	}
}

// Each week has its own flag: the current week's data says nothing about the
// prior week, and the other way round.
func TestHasData_TheTwoWeeksAreIndependent(t *testing.T) {
	onlyCurrent := resolveWeeks(t, storedZeroWeekScanners(), emptyWeekScanners(), nil)
	if got := withData(onlyCurrent, false); len(got) != len(allMetricKeys) {
		t.Errorf("current week stored: hasData true for %d metrics, want %d", len(got), len(allMetricKeys))
	}
	if got := withData(onlyCurrent, true); len(got) != 0 {
		t.Errorf("prior week missing: hasPriorData true for %v", got)
	}
	onlyPrior := resolveWeeks(t, emptyWeekScanners(), storedZeroWeekScanners(), nil)
	if got := withData(onlyPrior, false); len(got) != 0 {
		t.Errorf("current week missing: hasData true for %v", got)
	}
	if got := withData(onlyPrior, true); len(got) != len(allMetricKeys) {
		t.Errorf("prior week stored: hasPriorData true for %d metrics, want %d", len(got), len(allMetricKeys))
	}
}

// TestMissingWeekSuppressesComparativeClaims reproduces CHAOS-8525 through
// Resolve, the production Operating Review builder. A missing current or
// prior week still serializes its numeric zero placeholder for wire
// compatibility, but it cannot support a status, section sentence, or
// recommendation. Stored zeroes in BOTH weeks remain comparable.
func TestMissingWeekSuppressesComparativeClaims(t *testing.T) {
	for _, tc := range []struct {
		name           string
		current, prior []*fakeRowScanner
	}{
		{name: "missing current", current: emptyWeekScanners(), prior: storedZeroWeekScanners()},
		{name: "missing prior", current: storedZeroWeekScanners(), prior: emptyWeekScanners()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			review := resolveReviewWeeks(t, tc.current, tc.prior, nil)
			for key, metric := range metricsByKey(t, review) {
				if metric.HasData && metric.Delta.HasPriorData {
					t.Fatalf("%s unexpectedly has data in both weeks", key)
				}
				if metric.Delta.Status != "" {
					t.Errorf("%s status = %q, want no claim when a comparison week is missing", key, metric.Delta.Status)
				}
			}
			for _, section := range review.Sections {
				if len(section.Changed) != 0 || len(section.Improved) != 0 || len(section.Worsened) != 0 {
					t.Errorf("%s sentences = changed:%q improved:%q worsened:%q, want none", section.Key, section.Changed, section.Improved, section.Worsened)
				}
			}
			if len(review.Recommendations) != 0 {
				t.Errorf("recommendations = %q, want none", review.Recommendations)
			}
		})
	}

	stored := resolveReviewWeeks(t, storedZeroWeekScanners(), storedZeroWeekScanners(), nil)
	for key, metric := range metricsByKey(t, stored) {
		if !metric.HasData || !metric.Delta.HasPriorData {
			t.Fatalf("%s must retain both stored-zero data flags", key)
		}
		if metric.Delta.Status == "" {
			t.Errorf("%s status is empty for two stored zeroes", key)
		}
	}
}

// A read that fails is no data for the metrics of that table only, and the
// answer still comes back.
func TestHasData_AFailedReadIsNoDataForItsMetricsOnly(t *testing.T) {
	for name, tc := range map[string]struct {
		call int
		lost []string
	}{
		"work_items":      {0, []string{"cycle_time_p50_hours", "throughput", "wip_count", "wip_age_p90_hours"}},
		"state_durations": {1, []string{"state_duration_hours"}},
		// mttr_hours falls back to the repository row, so losing it loses mttr_hours only when the
		// incident read has no value: here incidents hold an MTTR.
		"repo_metrics":  {2, []string{"review_latency_hours", "ownership_concentration", "bus_factor", "revert_rate"}},
		"hotspots":      {3, []string{"hotspot_risk_score"}},
		"complexity":    {4, []string{"complexity_per_kloc"}},
		"deployments":   {5, []string{"deployments_count", "deployment_failure_rate"}},
		"incidents":     {6, []string{"incidents_count"}},
		"investment":    {7, []string{"ktlo_units", "new_value_units", "security_units", "infra_units"}},
		"ai_impact":     {8, []string{"ai_adoption_ratio", "ai_cycle_time_delta_hours", "ai_review_amplification", "ai_risk_drag"}},
		"ai_governance": {9, []string{"ai_governance_coverage"}},
		// change failure rate has one source: its own counts.
		"change_failure": {10, []string{"change_failure_rate"}},
	} {
		t.Run(name, func(t *testing.T) {
			errs := make([]error, 22)
			errs[tc.call] = errors.New("boom")
			metrics := resolveWeeks(t, storedZeroWeekScanners(), storedZeroWeekScanners(), errs)
			lost := map[string]bool{}
			for _, key := range tc.lost {
				lost[key] = true
			}
			for _, key := range allMetricKeys {
				if metrics[key].HasData == lost[key] {
					t.Errorf("%s: hasData = %v after the %s read failed", key, metrics[key].HasData, name)
				}
				if !metrics[key].Delta.HasPriorData {
					t.Errorf("%s: hasPriorData = false, but the prior week's reads did not fail", key)
				}
			}
		})
	}
}

func fp(v float64) *float64 { return &v }

// The presence rule of each metric, clause by clause, on rows built by hand:
// which inputs make a metric "have data" and which do not.
func TestHasData_PresenceRuleOfEachMetric(t *testing.T) {
	cases := []struct {
		name string
		rows periodRows
		want []string
	}{
		{"nothing stored", periodRows{}, nil},
		{"work item rows whose hour columns are all NULL",
			periodRows{workItems: []workItemsRow{{}}},
			[]string{"throughput", "wip_count"}},
		{"a cycle time", periodRows{workItems: []workItemsRow{{cycleTimeP50Hours: fp(0)}}},
			[]string{"throughput", "wip_count", "cycle_time_p50_hours"}},
		{"a WIP age p90", periodRows{workItems: []workItemsRow{{wipAgeP90Hours: fp(0)}}},
			[]string{"throughput", "wip_count", "wip_age_p90_hours"}},
		{"the other two hour columns only", periodRows{workItems: []workItemsRow{{cycleTimeP90Hours: fp(1), wipAgeP50Hours: fp(1)}}},
			[]string{"throughput", "wip_count"}},
		{"a state duration row", periodRows{stateDurations: []stateDurationRow{{}}}, []string{"state_duration_hours"}},
		{"the repository aggregate over no row", periodRows{repoMetrics: []repoMetricsRow{{}}}, nil},
		{"repository rows stored, every nullable column NULL", periodRows{repoMetrics: []repoMetricsRow{{storedRows: 3}}},
			[]string{"bus_factor"}},
		{"a review latency", periodRows{repoMetrics: []repoMetricsRow{{prFirstReviewP50Hours: fp(0)}}}, []string{"review_latency_hours"}},
		{"an ownership ratio", periodRows{repoMetrics: []repoMetricsRow{{singleOwnerFileRatio30d: fp(0)}}}, []string{"ownership_concentration"}},
		{"a revert rate", periodRows{repoMetrics: []repoMetricsRow{{revertRate: fp(0)}}}, []string{"revert_rate"}},
		{"a repository MTTR", periodRows{repoMetrics: []repoMetricsRow{{mttrHours: fp(0)}}}, []string{"mttr_hours"}},
		{"the hotspot aggregate over no row", periodRows{hotspots: []hotspotsAggRow{{}}}, nil},
		{"a hotspot risk", periodRows{hotspots: []hotspotsAggRow{{riskScore: fp(0.4), hotspotsCount: 1}}}, []string{"hotspot_risk_score"}},
		{"the complexity aggregate over no row", periodRows{complexity: []complexityAggRow{{}}}, nil},
		{"a complexity", periodRows{complexity: []complexityAggRow{{cyclomaticPerKloc: fp(0)}}}, []string{"complexity_per_kloc"}},
		{"the deployment aggregate over no row", periodRows{deployments: []deploymentsAggRow{{}}}, nil},
		{"deployment rows stored, no deployment", periodRows{deployments: []deploymentsAggRow{{storedRows: 2}}},
			[]string{"deployments_count"}},
		{"deployments", periodRows{deployments: []deploymentsAggRow{{deploymentsCount: 4, storedRows: 2}}},
			[]string{"deployments_count", "deployment_failure_rate"}},
		{"change-failure rows with deployments and no incident evidence",
			periodRows{changeFailure: []changeFailureAggRow{{view: changefailure.View{Counts: changefailure.Counts{Deployments: 4}, StoredRows: 2}}}}, nil},
		{"change-failure rows with incident evidence and no deployment",
			periodRows{changeFailure: []changeFailureAggRow{{view: changefailure.View{Counts: changefailure.Counts{IncidentsDirect: 1}, StoredRows: 1}}}}, nil},
		{"change-failure rows with deployments and a direct incident",
			periodRows{changeFailure: []changeFailureAggRow{{view: changefailure.View{Counts: changefailure.Counts{Deployments: 4, IncidentsDirect: 1}, StoredRows: 2}}}},
			[]string{"change_failure_rate"}},
		{"change-failure rows with deployments and a via-deployment incident",
			periodRows{changeFailure: []changeFailureAggRow{{view: changefailure.View{Counts: changefailure.Counts{Deployments: 4, IncidentsViaDeployment: 1}, StoredRows: 2}}}},
			[]string{"change_failure_rate"}},
		{"the incident aggregate over no row", periodRows{incidents: []incidentsAggRow{{}}}, nil},
		{"incident rows stored, MTTR NULL", periodRows{incidents: []incidentsAggRow{{storedRows: 1}}}, []string{"incidents_count"}},
		{"an incident MTTR", periodRows{incidents: []incidentsAggRow{{mttrP50Hours: fp(0), storedRows: 1}}},
			[]string{"incidents_count", "mttr_hours"}},
		{"an investment row of an area the review does not show", periodRows{investment: []investmentRow{{investmentArea: "other"}}},
			[]string{"ktlo_units", "new_value_units", "security_units", "infra_units"}},
		{"AI rows that count no pull request", periodRows{aiImpact: []aiImpactRow{{}}}, nil},
		{"AI rows with pull requests", periodRows{aiImpact: []aiImpactRow{{prsTotal: 3}}}, []string{"ai_adoption_ratio"}},
		{"an AI cycle time delta", periodRows{aiImpact: []aiImpactRow{{aiCycleTimeDeltaHours: fp(0)}}}, []string{"ai_cycle_time_delta_hours"}},
		{"an AI review amplification", periodRows{aiImpact: []aiImpactRow{{aiReviewAmplification: fp(0)}}},
			[]string{"ai_review_amplification", "ai_opportunity_signals"}},
		{"an AI rework drag rate", periodRows{aiImpact: []aiImpactRow{{reworkDragRate: fp(0)}}}, []string{"ai_risk_drag", "ai_opportunity_signals"}},
		{"an AI test gap rate", periodRows{aiImpact: []aiImpactRow{{testGapRate: fp(0)}}}, []string{"ai_risk_drag", "ai_opportunity_signals"}},
		// The incident drag rate feeds the risk drag, and is not one of the opportunity signals.
		{"an AI incident drag rate", periodRows{aiImpact: []aiImpactRow{{incidentDragRate: fp(0)}}}, []string{"ai_risk_drag"}},
		{"an AI governance row", periodRows{aiGovernance: []aiGovernanceRawRow{{}}}, []string{"ai_governance_coverage", "ai_opportunity_signals"}},
	}
	week := day("2026-08-24")
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			asCurrent := metricsByKey(t, computeReview("org-1", nil, week, tc.rows, periodRows{}))
			if got := withData(asCurrent, false); !sameKeys(got, sorted(tc.want...)) {
				t.Errorf("as the current week: hasData true for %v, want %v", got, sorted(tc.want...))
			}
			if got := withData(asCurrent, true); len(got) != 0 {
				t.Errorf("as the current week: hasPriorData true for %v, want none", got)
			}
			asPrior := metricsByKey(t, computeReview("org-1", nil, week, periodRows{}, tc.rows))
			if got := withData(asPrior, true); !sameKeys(got, sorted(tc.want...)) {
				t.Errorf("as the prior week: hasPriorData true for %v, want %v", got, sorted(tc.want...))
			}
			if got := withData(asPrior, false); len(got) != 0 {
				t.Errorf("as the prior week: hasData true for %v, want none", got)
			}
		})
	}
}

// The two counts that tell a stored zero from no row are read from the store.
func TestFetchDeploymentsAndIncidents_CarryTheStoredRowCount(t *testing.T) {
	ctx := context.Background()
	client := &fakeClient{responses: []*fakeRowScanner{
		{rows: [][]any{{uint64(0), uint64(0), uint64(7)}}},
		{rows: [][]any{{uint64(0), nil, uint64(3)}}},
	}}
	deployments, err := fetchDeploymentsAgg(ctx, client, "org-1", day("2026-08-24"), day("2026-08-31"))
	if err != nil || len(deployments) != 1 || deployments[0].storedRows != 7 {
		t.Fatalf("deployments = %+v, %v; want storedRows 7", deployments, err)
	}
	incidents, err := fetchIncidentsAgg(ctx, client, "org-1", day("2026-08-24"), day("2026-08-31"))
	if err != nil || len(incidents) != 1 || incidents[0].storedRows != 3 {
		t.Fatalf("incidents = %+v, %v; want storedRows 3", incidents, err)
	}
}

// Change failure rate carries the state of the CURRENT week, so unknown, not
// applicable and a week with no stored counts are three answers, not one "no
// data". The prior week's state is not served: hasPriorData says whether it
// is measured. No other metric carries a state.
func TestChangeFailureRateCarriesTheStateOfTheCurrentWeek(t *testing.T) {
	rows := func(counts changefailure.Counts, stored uint64) periodRows {
		return periodRows{changeFailure: []changeFailureAggRow{{view: changefailure.View{Counts: counts, StoredRows: stored}}}}
	}
	week := day("2026-08-24")
	for _, tc := range []struct {
		name    string
		rows    periodRows
		value   float64
		hasData bool
		state   string
	}{
		{"no read result", periodRows{}, 0, false, "null"},
		{"no stored row", rows(changefailure.Counts{}, 0), 0, false, "null"},
		{"stored rows of zeros", rows(changefailure.Counts{}, 3), 0, false, "not_applicable_no_deployments"},
		{"an incident, no deployment", rows(changefailure.Counts{IncidentsViaDeployment: 1}, 1), 0, false, "not_applicable_no_deployments"},
		{"deployments, no incident evidence", rows(changefailure.Counts{Deployments: 4, FailedNative: 1}, 2), 0, false, "unknown_no_incident_evidence"},
		{"measured zero", rows(changefailure.Counts{Deployments: 4, IncidentsDirect: 1}, 2), 0, true, "measured"},
		{"measured", rows(changefailure.Counts{Deployments: 4, FailedNative: 1, FailedHeuristic: 1, IncidentsDirect: 1}, 2), 0.5, true, "measured"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			state := func(m model.OperatingReviewMetric) string {
				if m.RateState == nil {
					return "null"
				}
				return *m.RateState
			}
			asCurrent := metricsByKey(t, computeReview("org-1", nil, week, tc.rows, periodRows{}))
			got := asCurrent["change_failure_rate"]
			if state(got) != tc.state || got.HasData != tc.hasData || got.Value != tc.value {
				t.Errorf("as the current week: value %v hasData %v rateState %s, want %v %v %s", got.Value, got.HasData, state(got), tc.value, tc.hasData, tc.state)
			}
			for key, m := range asCurrent {
				if key != "change_failure_rate" && m.RateState != nil {
					t.Errorf("%s carries rateState %s", key, *m.RateState)
				}
			}
			asPrior := metricsByKey(t, computeReview("org-1", nil, week, periodRows{}, tc.rows))["change_failure_rate"]
			if asPrior.RateState != nil || asPrior.Delta.HasPriorData != tc.hasData || asPrior.Delta.PriorValue != tc.value {
				t.Errorf("as the prior week: rateState %s hasPriorData %v priorValue %v, want null %v %v", state(asPrior), asPrior.Delta.HasPriorData, asPrior.Delta.PriorValue, tc.hasData, tc.value)
			}
		})
	}
}
