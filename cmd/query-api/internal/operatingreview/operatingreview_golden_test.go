package operatingreview

import (
	"encoding/json"
	"math"
	"os"
	"reflect"
	"testing"
	"time"
)

// The golden holds the exact output of the weekly operating review
// computation for nine neutral, synthetic period-row sets (both periods
// populated, reversed periods, no deployments, empty periods, current-only,
// prior-only, identical periods, two AI threshold-boundary sets). The rows are the shape the ten period
// queries return; the expected review is frozen data. Regenerating it needs
// a reference implementation; a change to a metric, unit, direction or
// threshold goes red here.
//
// Floats compare within goldenFloatTolerance (relative). The reference
// implementation summed with compensated summation, so a sum of three
// unequal rates can differ from the plain left-to-right sum in the last bit;
// no status, recommendation or key depends on that bit.

const goldenFloatTolerance = 1e-12

func goldenFloatEqual(a, b float64) bool {
	if a == b {
		return true
	}
	diff := math.Abs(a - b)
	return diff <= goldenFloatTolerance*math.Max(math.Abs(a), math.Abs(b))
}

func goldenPercentEqual(a, b *float64) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return goldenFloatEqual(*a, *b)
}

type goldenRows map[string][]map[string]any

type goldenDelta struct {
	Value      float64  `json:"value"`
	PriorValue float64  `json:"prior_value"`
	Absolute   float64  `json:"absolute"`
	Percent    *float64 `json:"percent"`
	Status     string   `json:"status"`
}

type goldenMetric struct {
	Key   string      `json:"key"`
	Label string      `json:"label"`
	Value float64     `json:"value"`
	Unit  string      `json:"unit"`
	Delta goldenDelta `json:"delta"`
}

type goldenSection struct {
	Key      string         `json:"key"`
	Title    string         `json:"title"`
	Changed  []string       `json:"changed"`
	Improved []string       `json:"improved"`
	Worsened []string       `json:"worsened"`
	Metrics  []goldenMetric `json:"metrics"`
}

type goldenReview struct {
	PriorWeekStart            string          `json:"prior_week_start"`
	Recommendations           []string        `json:"recommendations"`
	RecommendationsEmptyState string          `json:"recommendations_empty_state"`
	Sections                  []goldenSection `json:"sections"`
}

type goldenCase struct {
	Name      string       `json:"name"`
	OrgID     string       `json:"org_id"`
	TeamID    *string      `json:"team_id"`
	WeekStart string       `json:"week_start"`
	Current   goldenRows   `json:"current"`
	Prior     goldenRows   `json:"prior"`
	Expected  goldenReview `json:"expected"`
}

func gNum(row map[string]any, key string) float64 {
	v, _ := row[key].(float64)
	return v
}

func gNullable(row map[string]any, key string) *float64 {
	v, ok := row[key].(float64)
	if !ok {
		return nil
	}
	return &v
}

func (g goldenRows) toPeriodRows() periodRows {
	var p periodRows
	for _, r := range g["work_items"] {
		p.workItems = append(p.workItems, workItemsRow{
			itemsStarted: gNum(r, "items_started"), itemsCompleted: gNum(r, "items_completed"),
			wipCountEndOfDay:  gNum(r, "wip_count_end_of_day"),
			cycleTimeP50Hours: gNullable(r, "cycle_time_p50_hours"), cycleTimeP90Hours: gNullable(r, "cycle_time_p90_hours"),
			wipAgeP50Hours: gNullable(r, "wip_age_p50_hours"), wipAgeP90Hours: gNullable(r, "wip_age_p90_hours"),
		})
	}
	for _, r := range g["state_durations"] {
		p.stateDurations = append(p.stateDurations, stateDurationRow{
			itemsTouched: gNum(r, "items_touched"), durationHours: gNum(r, "duration_hours"), avgWip: gNum(r, "avg_wip"),
		})
	}
	for _, r := range g["repo_metrics"] {
		p.repoMetrics = append(p.repoMetrics, repoMetricsRow{
			prsMerged: gNum(r, "prs_merged"), prFirstReviewP50Hours: gNullable(r, "pr_first_review_p50_hours"),
			singleOwnerFileRatio30d: gNullable(r, "single_owner_file_ratio_30d"),
			codeOwnershipGini:       gNum(r, "code_ownership_gini"), busFactor: gNum(r, "bus_factor"),
			changeFailureRate: gNullable(r, "change_failure_rate"), mttrHours: gNullable(r, "mttr_hours"),
		})
	}
	for _, r := range g["hotspots"] {
		p.hotspots = append(p.hotspots, hotspotsAggRow{riskScore: gNullable(r, "risk_score"), hotspotsCount: gNum(r, "hotspots_count")})
	}
	for _, r := range g["complexity"] {
		p.complexity = append(p.complexity, complexityAggRow{cyclomaticPerKloc: gNullable(r, "cyclomatic_per_kloc")})
	}
	for _, r := range g["deployments"] {
		p.deployments = append(p.deployments, deploymentsAggRow{
			deploymentsCount: gNum(r, "deployments_count"), failedDeploymentsCount: gNum(r, "failed_deployments_count"),
		})
	}
	for _, r := range g["incidents"] {
		p.incidents = append(p.incidents, incidentsAggRow{incidentsCount: gNum(r, "incidents_count"), mttrP50Hours: gNullable(r, "mttr_p50_hours")})
	}
	for _, r := range g["investment"] {
		area, _ := r["investment_area"].(string)
		p.investment = append(p.investment, investmentRow{investmentArea: area, deliveryUnits: gNum(r, "delivery_units")})
	}
	for _, r := range g["ai_impact"] {
		p.aiImpact = append(p.aiImpact, aiImpactRow{
			prsTotal: gNum(r, "prs_total"), aiAssistedPrs: gNum(r, "ai_assisted_prs"), agentCreatedPrs: gNum(r, "agent_created_prs"),
			humanPrs: gNum(r, "human_prs"), unknownPrs: gNum(r, "unknown_prs"),
			aiCycleTimeDeltaHours: gNullable(r, "ai_cycle_time_delta_hours"), aiReviewAmplification: gNullable(r, "ai_review_amplification"),
			reworkDragRate: gNullable(r, "rework_drag_rate"), testGapRate: gNullable(r, "test_gap_rate"), incidentDragRate: gNullable(r, "incident_drag_rate"),
		})
	}
	return p
}

func normalizeStrings(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}

func TestOperatingReviewMatchesTheFrozenGolden(t *testing.T) {
	raw, err := os.ReadFile("testdata/operating_review_golden.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []goldenCase
	if err := json.Unmarshal(raw, &cases); err != nil {
		t.Fatal(err)
	}
	if len(cases) != 9 {
		t.Fatalf("golden holds %d cases, want 9", len(cases))
	}
	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			week, err := time.Parse("2006-01-02", tc.WeekStart)
			if err != nil {
				t.Fatal(err)
			}
			got := computeReview(tc.OrgID, tc.TeamID, week, tc.Current.toPeriodRows(), tc.Prior.toPeriodRows())
			if got.PriorWeekStart.String() != tc.Expected.PriorWeekStart {
				t.Errorf("prior week %s, want %s", got.PriorWeekStart.String(), tc.Expected.PriorWeekStart)
			}
			if got.RecommendationsEmptyState != tc.Expected.RecommendationsEmptyState {
				t.Errorf("empty state %q, want %q", got.RecommendationsEmptyState, tc.Expected.RecommendationsEmptyState)
			}
			if !reflect.DeepEqual(normalizeStrings(got.Recommendations), normalizeStrings(tc.Expected.Recommendations)) {
				t.Errorf("recommendations %q, want %q", got.Recommendations, tc.Expected.Recommendations)
			}
			if len(got.Sections) != len(tc.Expected.Sections) {
				t.Fatalf("%d sections, want %d", len(got.Sections), len(tc.Expected.Sections))
			}
			for i, want := range tc.Expected.Sections {
				sec := got.Sections[i]
				if sec.Key != want.Key || sec.Title != want.Title {
					t.Errorf("section %d is %s/%s, want %s/%s", i, sec.Key, sec.Title, want.Key, want.Title)
				}
				for name, pair := range map[string][2][]string{
					"changed": {sec.Changed, want.Changed}, "improved": {sec.Improved, want.Improved}, "worsened": {sec.Worsened, want.Worsened},
				} {
					if !reflect.DeepEqual(normalizeStrings(pair[0]), normalizeStrings(pair[1])) {
						t.Errorf("section %s %s = %q, want %q", want.Key, name, pair[0], pair[1])
					}
				}
				if len(sec.Metrics) != len(want.Metrics) {
					t.Fatalf("section %s has %d metrics, want %d", want.Key, len(sec.Metrics), len(want.Metrics))
				}
				for j, wm := range want.Metrics {
					m := sec.Metrics[j]
					if m.Key != wm.Key || m.Label != wm.Label || m.Unit != wm.Unit || !goldenFloatEqual(m.Value, wm.Value) ||
						m.Delta == nil || !goldenFloatEqual(m.Delta.Value, wm.Delta.Value) || !goldenFloatEqual(m.Delta.PriorValue, wm.Delta.PriorValue) ||
						!goldenFloatEqual(m.Delta.Absolute, wm.Delta.Absolute) || m.Delta.Status != wm.Delta.Status ||
						!goldenPercentEqual(m.Delta.Percent, wm.Delta.Percent) {
						t.Errorf("section %s metric %d: got %+v delta %+v, want %+v", want.Key, j, m, m.Delta, wm)
					}
				}
			}
		})
	}
}
