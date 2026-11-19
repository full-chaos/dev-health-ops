package report

import (
	"context"
	"strings"
	"testing"
	"time"
)

const provenanceHeading = "## Provenance"

// hostileValues are free text a plan or a data point can hold that a reader must never take for the Provenance
// heading it splits the report on (web MarkdownRenderer splitProvenance).
var hostileValues = []string{
	provenanceHeading, "### Provenance", "a " + provenanceHeading, "x\n" + provenanceHeading, "x\r\n" + provenanceHeading + "\r\ny",
}

func renderHostile(t *testing.T, mutate func(plan *Plan, chart *ChartResult, value string)) []string {
	t.Helper()
	var out []string
	for _, value := range hostileValues {
		plan := Plan{
			PlanID: "plan-1", ReportType: "weekly_health", Audience: "team_lead",
			TimeRangeStart: "2026-01-01", TimeRangeEnd: "2026-01-07",
			Sections: []string{"summary", "quality"}, ConfidenceThreshold: "direct_fact",
			CreatedAt: time.Date(2026, 1, 8, 0, 0, 0, 0, time.UTC), OrganizationID: "org-1",
		}
		chart := ChartResult{
			Spec: ChartSpec{
				ChartID: "chart-1", PlanID: "plan-1", ChartType: "bar", Metric: "failure_rate",
				GroupBy: "team", Title: "Failure rate", OrganizationID: "org-1",
			},
			DataPoints: []DataPoint{{X: "2026-01-01", Y: 0.5, Group: "team-a"}},
			Title:      "Failure rate", SourceTable: "t", Unit: "ratio",
		}
		mutate(&plan, &chart, value)
		artifact, err := NewDeterministicRenderer().Render(context.Background(), QueryResult{
			Plan: plan, Charts: []ChartResult{chart}, Metadata: map[string]string{"renderer_version": "reports.v1"},
		})
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, artifact.Markdown)
	}
	return out
}

// A reader splits the report on the Provenance heading; only the one real section may hold that text, and it is the
// last whole line of the report body part.
func requireOneProvenanceHeading(t *testing.T, markdown string) {
	t.Helper()
	if n := strings.Count(markdown, provenanceHeading); n != 1 {
		t.Errorf("report holds %q %d times, want 1:\n%s", provenanceHeading, n, markdown)
	}
	idx := strings.Index(markdown, provenanceHeading)
	if idx < 0 {
		return
	}
	if !strings.Contains(markdown[:idx], "### Charts") || !strings.Contains(markdown[:idx], "## Summary") {
		t.Errorf("the split text sits before the report body ends:\n%s", markdown)
	}
	if !strings.HasPrefix(markdown[idx:], provenanceHeading+"\n") || (idx > 0 && markdown[idx-1] != '\n') {
		t.Errorf("the split text is not a whole line:\n%s", markdown)
	}
}

func TestRenderPointLabelsNeverHoldTheProvenanceHeading(t *testing.T) {
	for _, markdown := range renderHostile(t, func(_ *Plan, chart *ChartResult, value string) {
		chart.DataPoints = []DataPoint{{X: value, Y: 5, Group: value}}
	}) {
		requireOneProvenanceHeading(t, markdown)
	}
}

func TestRenderChartTitleNeverHoldsTheProvenanceHeading(t *testing.T) {
	for _, markdown := range renderHostile(t, func(_ *Plan, chart *ChartResult, value string) {
		chart.Title = value
	}) {
		requireOneProvenanceHeading(t, markdown)
	}
	for _, markdown := range renderHostile(t, func(_ *Plan, chart *ChartResult, value string) {
		chart.Title = value
		chart.DataPoints = nil
	}) {
		requireOneProvenanceHeading(t, markdown)
	}
	for _, markdown := range renderHostile(t, func(_ *Plan, chart *ChartResult, value string) {
		chart.Title = value
		chart.NotApplicable = value
	}) {
		requireOneProvenanceHeading(t, markdown)
	}
	for _, markdown := range renderHostile(t, func(_ *Plan, chart *ChartResult, value string) {
		chart.Note = value
	}) {
		requireOneProvenanceHeading(t, markdown)
	}
}

func TestRenderPlanTextNeverHoldsTheProvenanceHeading(t *testing.T) {
	fields := map[string]func(plan *Plan, value string){
		"report type": func(plan *Plan, value string) { plan.ReportType = value },
		"section":     func(plan *Plan, value string) { plan.Sections = append(plan.Sections, value) },
		"window":      func(plan *Plan, value string) { plan.TimeRangeStart, plan.TimeRangeEnd = value, value },
		"team scope":  func(plan *Plan, value string) { plan.ScopeTeams = []string{value} },
		"repo scope":  func(plan *Plan, value string) { plan.ScopeRepos = []string{value} },
		"service":     func(plan *Plan, value string) { plan.ScopeServices = []string{value} },
		"uncharted label": func(plan *Plan, value string) {
			plan.UnchartedMetrics = []UnchartedMetric{{Label: value, Reason: value}}
		},
	}
	for name, set := range fields {
		t.Run(name, func(t *testing.T) {
			for _, markdown := range renderHostile(t, func(plan *Plan, _ *ChartResult, value string) { set(plan, value) }) {
				if n := strings.Count(markdown, provenanceHeading); n != 1 {
					t.Errorf("report holds %q %d times, want 1:\n%s", provenanceHeading, n, markdown)
				}
				if name != "team scope" && name != "repo scope" && name != "service" {
					requireOneProvenanceHeading(t, markdown)
				}
			}
		})
	}
}

func TestRenderChartTitleStaysOneHeadingLine(t *testing.T) {
	for _, title := range []string{"a\nb", "a\r\nb", "a\rb"} {
		out := renderChart(ChartResult{Title: title, DataPoints: []DataPoint{{X: "x", Y: 1}}})
		if first, _, _ := strings.Cut(out, "\n"); first != "#### a b" {
			t.Errorf("title %q renders as the first line %q, want \"#### a b\"", title, first)
		}
	}
}
