package report

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

var cicdMetricNames = []string{"pipelines_count", "avg_duration_minutes", "p90_duration_minutes", "avg_queue_minutes"}

func cicdCharts(groupBy string) []ChartResult {
	charts := make([]ChartResult, 0, len(cicdMetricNames))
	for _, metric := range cicdMetricNames {
		unit := "minutes"
		if metric == "pipelines_count" {
			unit = "count"
		}
		charts = append(charts, ChartResult{
			Spec:  ChartSpec{ChartID: "chart-" + metric, PlanID: "plan-3", ChartType: "bar", Metric: metric, GroupBy: groupBy},
			Title: "Chart of " + metric, SourceTable: "cicd_metrics_daily", Unit: unit,
			DataPoints: []DataPoint{{X: "a", Y: 5}, {X: "b", Y: 7}},
		})
	}
	return charts
}

func renderPlan(t *testing.T, sections []string, charts []ChartResult) string {
	t.Helper()
	artifact, err := NewDeterministicRenderer().Render(context.Background(), QueryResult{
		Plan: Plan{
			PlanID: "plan-3", ReportType: "weekly_health", Audience: "team_lead",
			TimeRangeStart: "2026-01-01", TimeRangeEnd: "2026-01-07", ComparisonPeriod: "prior_week",
			Sections: sections, ConfidenceThreshold: "direct_fact",
			CreatedAt: time.Date(2026, 1, 8, 0, 0, 0, 0, time.UTC), OrganizationID: "org-1",
		},
		Charts: charts,
	})
	if err != nil {
		t.Fatal(err)
	}
	return artifact.Markdown
}

func requireTables(t *testing.T, markdown string, charts []ChartResult) {
	t.Helper()
	for _, chart := range charts {
		if got := strings.Count(markdown, "#### "+chart.Title+"\n"); got != 1 {
			t.Errorf("chart %q is printed %d times, want 1:\n%s", chart.Title, got, markdown)
		}
	}
	if got, want := strings.Count(markdown, "| x | y | group |"), len(charts); got != want {
		t.Errorf("table headers = %d, want %d (one per chart):\n%s", got, want, markdown)
	}
}

// A saved plan lists the sections its author chose: a chart whose metric no listed section carries must still print.
func TestChartInNoListedSectionPrintsItsTableOnce(t *testing.T) {
	t.Parallel()
	defaultSections := []string{"summary", "delivery", "quality", "wellbeing"}
	for _, test := range []struct {
		name     string
		sections []string
		fallback bool
	}{
		{"default plan sections", defaultSections, true},
		{"summary and testops", []string{"summary", "testops"}, true},
		{"summary only", []string{"summary"}, true},
		{"plan lists cicd", []string{"summary", "cicd"}, false},
	} {
		for _, groupBy := range []string{"day", "repo", "team"} {
			charts := cicdCharts(groupBy)
			markdown := renderPlan(t, test.sections, charts)
			requireTables(t, markdown, charts)
			if got := strings.Contains(markdown, "## Other Charts\n"); got != test.fallback {
				t.Errorf("%s/%s: Other Charts heading = %v, want %v:\n%s", test.name, groupBy, got, test.fallback, markdown)
			}
		}
	}
}

func TestCICDSectionTitleAndCharts(t *testing.T) {
	t.Parallel()
	charts := cicdCharts("team")
	markdown := renderPlan(t, []string{"summary", "cicd"}, charts)
	head := "## CI/CD\n"
	at := strings.Index(markdown, head)
	if at < 0 {
		t.Fatalf("no %q section:\n%s", head, markdown)
	}
	section := markdown[at:]
	if end := strings.Index(section[len(head):], "\n## "); end >= 0 {
		section = section[:len(head)+end]
	}
	for _, chart := range charts {
		if !strings.Contains(section, "#### "+chart.Title+"\n") {
			t.Errorf("chart %q not under the CI/CD section:\n%s", chart.Title, markdown)
		}
	}
}

// success_rate is a testops metric: a plan that does not list testops sends it to the fallback, a plan that lists it keeps it there.
func TestTestopsMetricInAPlanWithoutTestopsReachesTheFallback(t *testing.T) {
	t.Parallel()
	charts := []ChartResult{{
		Spec:  ChartSpec{ChartID: "chart-s", PlanID: "plan-3", ChartType: "line", Metric: "success_rate", GroupBy: "day"},
		Title: "Success rate chart", SourceTable: "testops_pipeline_metrics_daily", Unit: "ratio",
		DataPoints: []DataPoint{{X: "2026-01-01", Y: 0.95}},
	}}
	without := renderPlan(t, []string{"summary", "quality"}, charts)
	requireTables(t, without, charts)
	if !strings.Contains(without, "## Other Charts\n#### Success rate chart\n") {
		t.Errorf("testops metric did not reach the fallback:\n%s", without)
	}
	with := renderPlan(t, []string{"summary", "testops"}, charts)
	requireTables(t, with, charts)
	if strings.Contains(with, "## Other Charts") {
		t.Errorf("a chart under its section also prints in the fallback:\n%s", with)
	}
}

func TestNoChartsPrintsNoFallbackHeading(t *testing.T) {
	t.Parallel()
	for _, sections := range [][]string{{"summary"}, {"summary", "cicd", "testops"}} {
		if markdown := renderPlan(t, sections, nil); strings.Contains(markdown, "Other Charts") {
			t.Errorf("sections %v: fallback heading with no chart:\n%s", sections, markdown)
		}
	}
}

func TestEveryCICDRegistryMetricIsInTheCICDSection(t *testing.T) {
	t.Parallel()
	for name, definition := range supportedMetrics {
		if definition.SourceTable == "cicd_metrics_daily" && !contains(sectionMetrics["cicd"], name) {
			t.Errorf("registry metric %s (cicd_metrics_daily) is in no cicd section", name)
		}
	}
	if sectionTitles["cicd"] != "CI/CD" {
		t.Errorf("cicd section title = %q, want CI/CD", sectionTitles["cicd"])
	}
}

func TestOtherChartsMatchesGolden(t *testing.T) {
	t.Parallel()
	team := []DataPoint{{X: "team-a", Y: 5}, {X: "team-b", Y: 7}}
	input := QueryResult{
		Plan: Plan{
			PlanID: "plan-2", ReportType: "weekly_health", Audience: "team_lead", ScopeTeams: []string{"team-a"},
			TimeRangeStart: "2026-01-01", TimeRangeEnd: "2026-01-07", ComparisonPeriod: "prior_week",
			Sections: []string{"summary", "testops"}, RequestedMetrics: []string{"success_rate"},
			ConfidenceThreshold: "direct_fact", CreatedAt: time.Date(2026, 1, 8, 0, 0, 0, 0, time.UTC), OrganizationID: "org-1",
		},
		Charts: []ChartResult{
			{Spec: ChartSpec{ChartID: "chart-1", PlanID: "plan-2", ChartType: "line", Metric: "success_rate", GroupBy: "day"},
				Title: "Success rate", SourceTable: "testops_pipeline_metrics_daily", Unit: "ratio",
				DataPoints: []DataPoint{{X: "2026-01-01", Y: 0.95}}},
			{Spec: ChartSpec{ChartID: "chart-2", PlanID: "plan-2", ChartType: "bar", Metric: "pipelines_count", GroupBy: "team"},
				Title: "Pipelines count", SourceTable: "cicd_metrics_daily", Unit: "count", DataPoints: team},
			{Spec: ChartSpec{ChartID: "chart-3", PlanID: "plan-2", ChartType: "bar", Metric: "avg_duration_minutes", GroupBy: "team"},
				Title: "Average duration", SourceTable: "cicd_metrics_daily", Unit: "minutes",
				DataPoints: []DataPoint{{X: "team-a", Y: 3.5}, {X: "team-b", Y: 4}}},
		},
	}
	artifact, err := NewDeterministicRenderer().Render(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	want, err := os.ReadFile(filepath.Join("testdata", "other_charts.golden.md"))
	if err != nil {
		t.Fatal(err)
	}
	if artifact.Markdown != string(want) {
		t.Fatalf("markdown drifted from the golden:\n%s", artifact.Markdown)
	}
}

// A chart prints under the first listed section that names it; a repeated section copy prints no second table.
func TestRepeatedListedSectionPrintsAChartOnce(t *testing.T) {
	t.Parallel()
	one := cicdCharts("team")[:1]
	for _, sections := range [][]string{
		{"summary", "cicd", "cicd"},
		{"summary", "cicd", "delivery", "cicd", "cicd"},
	} {
		markdown := renderPlan(t, sections, one)
		requireTables(t, markdown, one)
		if strings.Contains(markdown, "## Other Charts") {
			t.Errorf("sections %v: a printed chart reached the fallback:\n%s", sections, markdown)
		}
	}
	four := cicdCharts("team")
	markdown := renderPlan(t, []string{"summary", "cicd", "cicd"}, four)
	requireTables(t, markdown, four)
	if got := strings.Count(markdown, "No charts linked to this section."); got != 1 {
		t.Errorf("the second cicd copy should say no charts linked: %d notices:\n%s", got, markdown)
	}
}

// The fallback heading is the only "## Other Charts" heading: a plan section whose title is the same keeps a distinct title.
func TestPlanSectionTitledOtherChartsKeepsOneFallbackHeading(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"other charts", "Other Charts", "other_charts", "OTHER_CHARTS"} {
		unlisted := cicdCharts("team")
		markdown := renderPlan(t, []string{"summary", name}, unlisted)
		requireTables(t, markdown, unlisted)
		if got := strings.Count(markdown, "\n## Other Charts\n"); got != 1 {
			t.Errorf("section %q with unlisted charts: %d Other Charts headings, want 1:\n%s", name, got, markdown)
		}
		if !strings.Contains(markdown, "\n## Other Charts (plan section)\n") {
			t.Errorf("section %q lost its own heading:\n%s", name, markdown)
		}
		covered := cicdCharts("team")
		markdown = renderPlan(t, []string{"summary", name, "cicd"}, covered)
		requireTables(t, markdown, covered)
		if got := strings.Count(markdown, "\n## Other Charts\n"); got != 0 {
			t.Errorf("section %q with every chart printed: %d fallback headings, want 0:\n%s", name, got, markdown)
		}
		if got := strings.Count(markdown, "\n## Other Charts (plan section)\n"); got != 1 {
			t.Errorf("section %q with every chart printed: its heading count = %d, want 1:\n%s", name, got, markdown)
		}
	}
}

// A plan section whose title shows as "Other Charts" must not make a second heading of that name. A "#" suffix is
// escaped, so it is part of the title and such a section keeps its name (TestOtherChartsCollisionIsExactName).
func TestPlanSectionTitleShowingAsOtherChartsKeepsOneFallbackHeading(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"OTHER CHARTS", "Other Charts"} {
		unlisted := cicdCharts("team")
		markdown := renderPlan(t, []string{"summary", name}, unlisted)
		requireTables(t, markdown, unlisted)
		shown := 0
		for _, line := range strings.Split(markdown, "\n") {
			if strings.HasPrefix(line, "## ") && strings.EqualFold(strings.TrimRight(strings.TrimPrefix(line, "## "), "# "), "Other Charts") {
				shown++
			}
		}
		if shown != 1 {
			t.Errorf("section %q with unlisted charts: %d lines show as the Other Charts heading, want 1:\n%s", name, shown, markdown)
		}
		if !strings.Contains(markdown, "\n## Other Charts (plan section)\n") {
			t.Errorf("section %q lost its own heading:\n%s", name, markdown)
		}
		covered := cicdCharts("team")
		markdown = renderPlan(t, []string{"summary", name, "cicd"}, covered)
		requireTables(t, markdown, covered)
		if got := strings.Count(markdown, "\n## Other Charts\n") + strings.Count(markdown, "\n## Other Charts ##\n"); got != 0 {
			t.Errorf("section %q with every chart printed: %d lines show as the fallback heading, want 0:\n%s", name, got, markdown)
		}
	}
}
