package report

import (
	"context"
	"strings"
	"testing"
	"time"
)

func renderLiteral(t *testing.T, reportType string, sections []string, charts []ChartResult) string {
	t.Helper()
	artifact, err := NewDeterministicRenderer().Render(context.Background(), QueryResult{
		Plan: Plan{
			PlanID: "plan-lit", ReportType: reportType, Audience: "team_lead",
			TimeRangeStart: "2026-01-01", TimeRangeEnd: "2026-01-07", ComparisonPeriod: "prior_week",
			Sections: sections, ConfidenceThreshold: "direct_fact",
			CreatedAt: time.Date(2026, 1, 8, 0, 0, 0, 0, time.UTC), OrganizationID: "org-1",
		},
		Charts: charts,
	})
	if err != nil {
		t.Fatal(err)
	}
	return strings.ReplaceAll(artifact.Markdown, "\\", "")
}

func customMetricChart(metric string) ChartResult {
	return ChartResult{
		Spec:  ChartSpec{ChartID: "chart-x", PlanID: "plan-lit", ChartType: "line", Metric: metric},
		Title: "Chart x", SourceTable: "t", Unit: "count",
		DataPoints: []DataPoint{{X: "a", Y: 1}, {X: "b", Y: 2}},
	}
}

func TestCustomFreeTextShowsLiterally(t *testing.T) {
	t.Parallel()
	const typed = "Release *Candidate*"
	t.Run("report type", func(t *testing.T) {
		t.Parallel()
		if got := renderLiteral(t, typed, []string{"summary"}, nil); !strings.HasPrefix(got, "# "+typed+" Report") {
			t.Fatalf("custom report type not literal:\n%s", got)
		}
	})
	t.Run("section name", func(t *testing.T) {
		t.Parallel()
		if got := renderLiteral(t, "weekly_health", []string{"summary", typed}, nil); !strings.Contains(got, "\n## "+typed+"\n") {
			t.Fatalf("custom section name not literal:\n%s", got)
		}
	})
	t.Run("fallback metric name", func(t *testing.T) {
		t.Parallel()
		got := renderLiteral(t, "weekly_health", []string{"summary"}, []ChartResult{customMetricChart(typed)})
		if !strings.Contains(got, "\n"+typed+" appears near 2") {
			t.Fatalf("custom metric name not literal in body sentence:\n%s", got)
		}
	})
}

func TestKnownKeysKeepTitleCase(t *testing.T) {
	t.Parallel()
	got := renderLiteral(t, "weekly_health", []string{"summary", "testops", "cicd"}, []ChartResult{customMetricChart("pipelines_count")})
	for _, want := range []string{"# Weekly Health Report", "\n## TestOps\n", "\n## CI/CD\n"} {
		if !strings.Contains(got, want) {
			t.Errorf("known key lost its display text, want %q in:\n%s", want, got)
		}
	}
}

func TestKeyLikeFreeTextShowsAsTyped(t *testing.T) {
	t.Parallel()
	for _, typed := range []string{"Weekly_Health", "weekly health", "WEEKLY_HEALTH", "Release _Candidate_"} {
		if got := renderLiteral(t, typed, []string{"summary"}, nil); !strings.HasPrefix(got, "# "+typed+" Report") {
			t.Errorf("key-like report type %q not literal:\n%s", typed, got)
		}
	}
	for _, typed := range []string{"TestOps", "TESTOPS", "Cicd", "Release _Candidate_"} {
		if got := renderLiteral(t, "weekly_health", []string{"summary", typed}, nil); !strings.Contains(got, "\n## "+typed+"\n") {
			t.Errorf("key-like section %q not literal:\n%s", typed, got)
		}
	}
	for _, typed := range []string{"Failure_Rate", "FAILURE_RATE", "Pipelines Count", "Release _Candidate_"} {
		if got := renderLiteral(t, "weekly_health", []string{"summary"}, []ChartResult{customMetricChart(typed)}); !strings.Contains(got, "\n"+typed+" appears near 2") {
			t.Errorf("key-like metric %q not literal:\n%s", typed, got)
		}
	}
}

func TestEveryBuiltinReportTypeIsKnown(t *testing.T) {
	t.Parallel()
	for _, dateRange := range []string{"", "last_24_hours", "last_7_days", "last_30_days", "last_90_days"} {
		plan := defaultWirePlan("r1", "org-1", map[string]any{"dateRange": dateRange}, time.Date(2026, 11, 18, 0, 0, 0, 0, time.UTC))
		if _, ok := builtinReportTypes[plan.ReportType]; !ok {
			t.Errorf("dateRange %q builds report type %q that is not in builtinReportTypes", dateRange, plan.ReportType)
		}
		if got := renderLiteral(t, plan.ReportType, []string{"summary"}, nil); !strings.HasPrefix(got, "# "+titleCase(plan.ReportType)+" Report") {
			t.Errorf("built-in type %q lost its title case:\n%s", plan.ReportType, got)
		}
	}
	if len(builtinReportTypes) != 2 {
		t.Errorf("builtinReportTypes has %d keys, want 2 (weekly_health, monthly_review)", len(builtinReportTypes))
	}
}
