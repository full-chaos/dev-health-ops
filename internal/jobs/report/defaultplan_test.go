package report

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"
)

var defaultPlanNow = time.Date(2026, 11, 18, 0, 0, 0, 0, time.UTC)

func decodeDefault(t *testing.T, parameters string) ReportDefinition {
	t.Helper()
	definition, err := decodeReportDefinition("r1", "org-1", nil, []byte(parameters), defaultPlanNow)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	return definition
}

func chartMetrics(definition ReportDefinition) []string {
	metrics := []string{}
	for _, chart := range definition.Charts {
		metrics = append(metrics, chart.Metric)
	}
	return metrics
}

func TestDefaultPlanChartsEachFormLabelWithAProductMapping(t *testing.T) {
	definition := decodeDefault(t, `{"scope":"org","dateRange":"last_7_days","metrics":["Deployment Frequency","Lead Time","Change Failure Rate","Time to Restore","Test Coverage","CI Success Rate"]}`)
	if got, want := chartMetrics(definition), []string{"deployments_count", "change_failure_rate", "success_rate"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("chart metrics = %v, want %v", got, want)
	}
	for _, chart := range definition.Charts {
		if chart.ChartType != "line" || chart.GroupBy != "day" || chart.PlanID != "auto-r1" ||
			chart.OrganizationID != "org-1" || chart.ChartID != "auto-r1-"+chart.Metric ||
			chart.TimeRangeStart != "2026-11-11" || chart.TimeRangeEnd != "2026-11-18" {
			t.Fatalf("chart = %#v", chart)
		}
	}
}

func TestDefaultPlanChartsDeployFrequencyUnderBothLabelsOnce(t *testing.T) {
	definition := decodeDefault(t, `{"metrics":["Deploy Frequency","Deployment Frequency","deployments_count"]}`)
	if got, want := chartMetrics(definition), []string{"deployments_count"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("chart metrics = %v, want %v", got, want)
	}
}

func TestDefaultPlanSkipsLabelsWithoutAProductMetric(t *testing.T) {
	definition := decodeDefault(t, `{"metrics":["Lead Time","Time to Restore","Test Coverage"]}`)
	if got := definition.Charts; len(got) != 0 {
		t.Fatalf("charts = %v, want none", got)
	}
}

func TestDefaultPlanDropsUnknownLabelAndKeepsTheReport(t *testing.T) {
	definition := decodeDefault(t, `{"metrics":["Nope","Lead Time","success_rate"]}`)
	if got, want := chartMetrics(definition), []string{"success_rate"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("chart metrics = %v, want %v", got, want)
	}
}

func TestDefaultPlanPassesRegistryNamesThroughOnce(t *testing.T) {
	definition := decodeDefault(t, `{"metrics":["pass_rate","CI Success Rate","success_rate","pass_rate"]}`)
	if got, want := chartMetrics(definition), []string{"pass_rate", "success_rate"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("chart metrics = %v, want %v", got, want)
	}
}

func TestDefaultPlanDropsRegistryNamesThatAreNotNumeric(t *testing.T) {
	definition := decodeDefault(t, `{"metrics":["assignee","commit_hash","success_rate"]}`)
	if got, want := chartMetrics(definition), []string{"success_rate"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("chart metrics = %v, want %v", got, want)
	}
}

func TestDefaultPlanWithoutMetricsHasNoCharts(t *testing.T) {
	if got := decodeDefault(t, `{"scope":"org"}`).Charts; len(got) != 0 {
		t.Fatalf("charts = %v", got)
	}
}

func TestSavedPlanIsNotGivenDefaultCharts(t *testing.T) {
	parameters := []byte(`{"metrics":["CI Success Rate"]}`)
	bare := []byte(`{"plan_id":"p1","report_type":"weekly_health","org_id":"org-1","sections":["summary"],"requested_metrics":["CI Success Rate"]}`)
	definition, err := decodeReportDefinition("r1", "org-1", bare, parameters, defaultPlanNow)
	if err != nil || len(definition.Charts) != 0 {
		t.Fatalf("bare saved plan: charts = %v, err = %v", definition.Charts, err)
	}
	withSpec := []byte(`{"plan_id":"p1","report_type":"weekly_health","org_id":"org-1","requested_metrics":["CI Success Rate","Change Failure Rate"],
		"chart_specs":[{"chart_id":"c1","plan_id":"p1","chart_type":"line","metric":"pass_rate","group_by":"day","org_id":"org-1"}]}`)
	definition, err = decodeReportDefinition("r1", "org-1", withSpec, parameters, defaultPlanNow)
	if err != nil || !reflect.DeepEqual(chartMetrics(definition), []string{"pass_rate"}) {
		t.Fatalf("saved plan with spec: charts = %v, err = %v", chartMetrics(definition), err)
	}
}

func TestDefaultPlanChartsCarryTheScope(t *testing.T) {
	team := decodeDefault(t, `{"scope":"team","team_ids":["t1","t2"],"metrics":["CI Success Rate"]}`)
	if len(team.Charts) != 1 || !reflect.DeepEqual(team.Charts[0].FilterTeams, []string{"t1", "t2"}) || len(team.Charts[0].FilterRepos) != 0 {
		t.Fatalf("team scope chart = %#v", team.Charts)
	}
	repo := decodeDefault(t, `{"scope":"repo","repo_ids":["r9"],"metrics":["CI Success Rate"]}`)
	if len(repo.Charts) != 1 || !reflect.DeepEqual(repo.Charts[0].FilterRepos, []string{"r9"}) || len(repo.Charts[0].FilterTeams) != 0 {
		t.Fatalf("repo scope chart = %#v", repo.Charts)
	}
	org := decodeDefault(t, `{"scope":"org","team_ids":["t1"],"metrics":["CI Success Rate"]}`)
	if len(org.Charts) != 1 || len(org.Charts[0].FilterTeams) != 0 || len(org.Charts[0].FilterRepos) != 0 {
		t.Fatalf("org scope chart = %#v", org.Charts)
	}
}

func renderDefinition(t *testing.T, definition ReportDefinition) string {
	t.Helper()
	charts := make([]ChartResult, 0, len(definition.Charts))
	for _, spec := range definition.Charts {
		charts = append(charts, ChartResult{Spec: spec, Title: "Chart of " + spec.Metric, SourceTable: "t", Unit: "count",
			DataPoints: []DataPoint{{X: "2026-11-17", Y: 1}, {X: "2026-11-18", Y: 2}}})
	}
	artifact, err := NewDeterministicRenderer().Render(context.Background(), QueryResult{Plan: definition.Plan, Charts: charts})
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	return artifact.Markdown
}

const unchartedHeading = "## Requested Metrics Without a Chart\n"

func TestDefaultPlanReportNamesEachLabelWithoutAChart(t *testing.T) {
	markdown := renderDefinition(t, decodeDefault(t, `{"metrics":["Lead Time","CI Success Rate","Nope","Lead Time"]}`))
	want := unchartedHeading + "- Lead Time: no metric matches this label\n- Nope: no metric matches this label\n"
	if strings.Count(markdown, unchartedHeading) != 1 || !strings.Contains(markdown, want) || strings.Count(markdown, "- Lead Time:") != 1 {
		t.Fatalf("report does not name the uncharted labels once each, want %q in:\n%s", want, markdown)
	}
	if !strings.Contains(markdown, "#### Chart of success_rate\n") {
		t.Fatalf("charted label lost its chart:\n%s", markdown)
	}
	if strings.Index(markdown, unchartedHeading) > strings.Index(markdown, "## Provenance") {
		t.Fatalf("uncharted section must come before Provenance:\n%s", markdown)
	}
}

func TestDefaultPlanReportDoesNotNameChartedLabels(t *testing.T) {
	markdown := renderDefinition(t, decodeDefault(t, `{"metrics":["Deploy Frequency","Deployment Frequency","deployments_count","CI Success Rate"]}`))
	if strings.Contains(markdown, unchartedHeading) {
		t.Fatalf("every label is charted, want no uncharted section:\n%s", markdown)
	}
}

func TestDefaultPlanReportNamesANonNumericRegistryName(t *testing.T) {
	markdown := renderDefinition(t, decodeDefault(t, `{"metrics":["assignee","success_rate"]}`))
	if !strings.Contains(markdown, unchartedHeading+"- assignee: this metric cannot be shown as a chart\n\n## Provenance") {
		t.Fatalf("non-numeric registry name is not named:\n%s", markdown)
	}
}

func TestDefaultPlanReportEscapesTheLabel(t *testing.T) {
	markdown := renderDefinition(t, decodeDefault(t, `{"metrics":["Lead *Time*|x\n## Fake","## Provenance","\n---\n"]}`))
	want := unchartedHeading + "- Lead \\*Time\\*\\|x \\#\\# Fake: no metric matches this label\n" +
		"- \\#\\# Provenance: no metric matches this label\n-  --- : no metric matches this label\n"
	if !strings.Contains(markdown, want) {
		t.Fatalf("labels are not escaped onto one line, want %q in:\n%s", want, markdown)
	}
	if strings.Count(markdown, "## Provenance") != 1 || strings.Count(markdown, "\n---\n") != 1 ||
		strings.Index(markdown, "## Provenance") < strings.Index(markdown, unchartedHeading) {
		t.Fatalf("a label moved the Provenance or footer split:\n%s", markdown)
	}
}

func TestSavedPlanWithoutChartSpecsNamesEachRequestedLabel(t *testing.T) {
	bare := []byte(`{"plan_id":"p1","report_type":"weekly_health","org_id":"org-1","sections":["summary"],"requested_metrics":["CI Success Rate","Lead Time"]}`)
	definition, err := decodeReportDefinition("r1", "org-1", bare, nil, defaultPlanNow)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	markdown := renderDefinition(t, definition)
	want := unchartedHeading + "- CI Success Rate: the saved report has no chart for this label\n" +
		"- Lead Time: the saved report has no chart for this label\n"
	if !strings.Contains(markdown, want) {
		t.Fatalf("saved plan without chart specs does not name its labels, want %q in:\n%s", want, markdown)
	}
}

func TestSavedPlanWithChartSpecsHasNoUnchartedSection(t *testing.T) {
	withSpec := []byte(`{"plan_id":"p1","report_type":"weekly_health","org_id":"org-1","requested_metrics":["CI Success Rate","Lead Time"],
		"chart_specs":[{"chart_id":"c1","plan_id":"p1","chart_type":"line","metric":"pass_rate","group_by":"day","org_id":"org-1"}]}`)
	definition, err := decodeReportDefinition("r1", "org-1", withSpec, nil, defaultPlanNow)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if markdown := renderDefinition(t, definition); strings.Contains(markdown, unchartedHeading) {
		t.Fatalf("saved plan with chart specs must not change:\n%s", markdown)
	}
}
