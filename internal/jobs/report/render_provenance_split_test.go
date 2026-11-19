package report

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

const provenanceHeading = "## Provenance"

// hostileValues are free text a plan or a data point can hold that a reader must never take for the Provenance
// heading it splits the report on (web MarkdownRenderer splitProvenance).
var hostileValues = []string{
	provenanceHeading, "### Provenance", `\## Provenance`, `\\## Provenance`, "a " + provenanceHeading, "x\n" + provenanceHeading, "x\r\n" + provenanceHeading + "\r\ny",
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

// A user backslash must not undo the "#" escape: the backslash is escaped first, so `a \# b` reads as `a \# b` (the
// backslash kept) and `\## Provenance` at a line start never reads as a heading or holds the split text.
func TestRenderEscapesBackslashBeforeHash(t *testing.T) {
	for _, value := range []string{`a \# b`, `\## Provenance`, `\\# x`, `end\`} {
		heading := renderChart(ChartResult{Title: value, DataPoints: []DataPoint{{X: "x", Y: 1}}})
		first, _, _ := strings.Cut(heading, "\n")
		if got := cellText(strings.TrimPrefix(first, "#### ")); got != value {
			t.Errorf("title %q renders %q (written %q), want the value back", value, got, first)
		}
		row := strings.Split(renderChart(ChartResult{Title: "t", DataPoints: []DataPoint{{X: value, Y: 1, Group: value}}}), "\n")[4]
		for _, i := range []int{0, 2} {
			if got := cellText(strings.TrimSpace(splitRow(row)[i])); got != value {
				t.Errorf("cell %d of %q reads %q, want the value back", i, row, got)
			}
		}
		if strings.Contains(heading, provenanceHeading) || strings.Contains(row, provenanceHeading) {
			t.Errorf("value %q leaves the split text in:\n%s\n%s", value, heading, row)
		}
	}
}

func TestRenderFooterConfidenceNeverHoldsTheProvenanceHeading(t *testing.T) {
	for _, markdown := range renderHostile(t, func(plan *Plan, _ *ChartResult, value string) {
		plan.ConfidenceThreshold = "direct_fact\n" + value + "\nforged"
	}) {
		if n := strings.Count(markdown, provenanceHeading); n != 1 {
			t.Errorf("report holds %q %d times, want 1:\n%s", provenanceHeading, n, markdown)
		}
	}
}

func decodeStoredPlan(t *testing.T, level string) ReportDefinition {
	t.Helper()
	plan, err := json.Marshal(map[string]string{
		"plan_id": "plan-1", "report_type": "weekly_health", "org_id": "org-1", "confidence_threshold": level,
	})
	if err != nil {
		t.Fatal(err)
	}
	definition, err := decodeReportDefinition("report-1", "org-1", plan, nil, time.Date(2026, 1, 8, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("a stored plan with confidence %q must still decode: %v", level, err)
	}
	return definition
}

// A stored plan with an unknown confidence level still runs: the footer shows the value as stored (escaped, so it cannot
// start a heading or a split), one WARN names the plan and not the value, and one counter tick is recorded.
func TestStoredPlanWithAnUnknownConfidenceStillRendersEscaped(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	counter, err := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test").Int64Counter(unknownConfidenceCounterName)
	if err != nil {
		t.Fatal(err)
	}
	previousCounter, previousLogger := unknownConfidenceCounter, slog.Default()
	t.Cleanup(func() { unknownConfidenceCounter = previousCounter; slog.SetDefault(previousLogger) })
	unknownConfidenceCounter = counter
	var logs bytes.Buffer
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))

	count := func() int64 {
		var data metricdata.ResourceMetrics
		if err := reader.Collect(context.Background(), &data); err != nil {
			t.Fatal(err)
		}
		var total int64
		for _, scope := range data.ScopeMetrics {
			for _, m := range scope.Metrics {
				if sum, ok := m.Data.(metricdata.Sum[int64]); ok && m.Name == unknownConfidenceCounterName {
					for _, point := range sum.DataPoints {
						total += point.Value
					}
				}
			}
		}
		return total
	}

	for _, level := range []string{"direct_fact", "inferred", "hypothesis", ""} {
		definition := decodeStoredPlan(t, level)
		warnUnknownConfidence(context.Background(), definition.Plan.PlanID, definition.Plan.ConfidenceThreshold)
	}
	if logs.Len() != 0 || count() != 0 {
		t.Fatalf("a known level warned or counted: logs=%q count=%d", logs.String(), count())
	}

	forged := "direct_fact\n## Provenance\nforged"
	definition := decodeStoredPlan(t, forged)
	if definition.Plan.ConfidenceThreshold != forged {
		t.Fatalf("stored value changed to %q, want it kept as stored", definition.Plan.ConfidenceThreshold)
	}
	warnUnknownConfidence(context.Background(), definition.Plan.PlanID, definition.Plan.ConfidenceThreshold)
	if got := strings.Count(logs.String(), "level=WARN"); got != 1 || !strings.Contains(logs.String(), "unknown confidence level") ||
		!strings.Contains(logs.String(), "plan_id=plan-1") || strings.Contains(logs.String(), "forged") {
		t.Errorf("want one WARN with the plan id and without the value, got %q", logs.String())
	}
	if got := count(); got != 1 {
		t.Errorf("counter = %d, want 1", got)
	}
	artifact, err := NewDeterministicRenderer().Render(context.Background(), QueryResult{Plan: definition.Plan})
	if err != nil {
		t.Fatal(err)
	}
	requireOneProvenanceHeadingCount(t, artifact.Markdown)
	if !strings.Contains(artifact.Markdown, "Confidence: direct_fact \\#\\# Provenance forged") {
		t.Errorf("footer does not show the stored value escaped:\n%s", artifact.Markdown)
	}
}

func requireOneProvenanceHeadingCount(t *testing.T, markdown string) {
	t.Helper()
	if n := strings.Count(markdown, provenanceHeading); n != 1 {
		t.Errorf("report holds %q %d times, want 1:\n%s", provenanceHeading, n, markdown)
	}
}
