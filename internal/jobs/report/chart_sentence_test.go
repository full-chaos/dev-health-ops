package report

import (
	"strings"
	"testing"
)

// The narrative compares the first and the last point only on a time axis. On a group axis the points are groups
// ordered by value, not times: the sentence gives the range across the groups and no opening-of-window comparison.
func TestChartSentenceComparesOnlyOnATimeAxis(t *testing.T) {
	t.Parallel()
	points := []DataPoint{{X: "a", Y: 5}, {X: "b", Y: 3}, {X: "c", Y: 2}}
	for groupBy, want := range map[string]string{
		"team":    "Pipelines Count ranges from 2 to 5 across 3 team groups in the selected window.",
		"repo":    "Pipelines Count ranges from 2 to 5 across 3 repository groups in the selected window.",
		"service": "Pipelines Count ranges from 2 to 5 across 3 service groups in the selected window.",
	} {
		got := chartSentence(ChartResult{Spec: ChartSpec{ChartType: "bar", Metric: "pipelines_count", GroupBy: groupBy}, Unit: "count", DataPoints: points})
		if got != want {
			t.Errorf("group %s: %q, want %q", groupBy, got, want)
		}
	}
	timePoints := []DataPoint{{X: "2026-01-01", Y: 2}, {X: "2026-01-02", Y: 5}}
	for _, spec := range []ChartSpec{
		{ChartType: "bar", Metric: "pipelines_count", GroupBy: "day"},
		{ChartType: "bar", Metric: "pipelines_count", GroupBy: "week"},
		{ChartType: "bar", Metric: "pipelines_count", GroupBy: "month"},
		{ChartType: "line", Metric: "pipelines_count"},
		{ChartType: "heatmap", Metric: "pipelines_count"},
	} {
		got := chartSentence(ChartResult{Spec: spec, Unit: "count", DataPoints: timePoints})
		if want := "Pipelines Count appears near 5, compared with 2 at the opening of the selected window."; got != want {
			t.Errorf("time axis %+v: %q, want %q", spec, got, want)
		}
	}
	one := chartSentence(ChartResult{Spec: ChartSpec{ChartType: "bar", Metric: "pipelines_count", GroupBy: "team"}, Unit: "count", DataPoints: points[:1]})
	if one != "Pipelines Count appears near 5 for the selected window." || strings.Contains(one, "opening") {
		t.Errorf("one group: %q", one)
	}
}
