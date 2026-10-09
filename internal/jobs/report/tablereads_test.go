package report

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
)

// Every table a registry metric reads has a read declaration, and every
// declaration is read by some metric: no table is charted raw, and no entry is
// dead. The integration test holds the declarations against the migrated
// schema.
func TestEveryRegistryTableHasOneReadDeclaration(t *testing.T) {
	used := map[string]bool{}
	for name, definition := range supportedMetrics {
		// A metric with a chart rule is read from the rule's table.
		table := withChartRule(definition).SourceTable
		used[table] = true
		used[definition.SourceTable] = true
		if _, ok := tableReads[table]; !ok {
			t.Errorf("metric %s reads %s, which has no tableReads declaration", name, table)
		}
	}
	for table, read := range tableReads {
		if !used[table] {
			t.Errorf("tableReads declares %s, which no registry metric reads", table)
		}
		if len(read.Key) == 0 || read.Version == "" {
			t.Errorf("tableReads[%s] = %+v lacks a key or a version column", table, read)
		}
	}
}

// A registry table without a declaration is refused before any query, as the
// same typed request error as a non-numeric metric. Not parallel: it edits the
// declaration map and restores it.
func TestChartOfAnUndeclaredTableIsRefusedBeforeAnyQuery(t *testing.T) {
	const metric, table = "commits_count", "user_metrics_daily"
	saved, ok := tableReads[table]
	if !ok {
		t.Fatalf("fixture drift: %s is not declared", table)
	}
	delete(tableReads, table)
	t.Cleanup(func() { tableReads[table] = saved })

	err := validateChartMetrics(context.Background(), []ChartSpec{{ChartID: "c", Metric: metric}})
	var refused *ChartMetricError
	if !errors.As(err, &refused) || refused.Metric != metric || refused.Table != table {
		t.Fatalf("err = %v, want a ChartMetricError naming %s and %s", err, metric, table)
	}
	if !errors.Is(err, ErrContractMismatch) || errors.Is(err, ErrDependencyUnavailable) {
		t.Fatalf("refusal must be an invalid-request error: %v", err)
	}
	if _, _, buildErr := buildChartQuery(ChartSpec{Metric: metric}, supportedMetrics[metric]); !errors.As(buildErr, &refused) {
		t.Fatalf("buildChartQuery of an undeclared table = %v, want the same refusal", buildErr)
	}
}

// The refusal is logged with its name and the metric, so a refused plan is
// found from the log. Not parallel: it swaps the default logger.
func TestRefusalOfANonNumericMetricIsLoggedWithTheMetricName(t *testing.T) {
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })

	if err := validateChartMetrics(context.Background(), []ChartSpec{{ChartID: "c", Metric: "assignee"}}); err == nil {
		t.Fatal("assignee was not refused")
	}
	for _, want := range []string{"report.chart_metric_refused", "metric=assignee", "value_kind=text", "source_table=work_item_cycle_times"} {
		if !strings.Contains(logs.String(), want) {
			t.Fatalf("log %q lacks %q", logs.String(), want)
		}
	}
}
