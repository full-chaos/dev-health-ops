package report

import (
	"context"
	"errors"
	"testing"
)

// Every table a registry metric reads has a read declaration, and every
// declaration is read by some metric: no table is charted raw, and no entry is
// dead. The integration test holds the declarations against the migrated
// schema.
func TestEveryRegistryTableHasOneReadDeclaration(t *testing.T) {
	used := map[string]bool{}
	for name, definition := range supportedMetrics {
		used[definition.SourceTable] = true
		if _, ok := tableReads[definition.SourceTable]; !ok {
			t.Errorf("metric %s reads %s, which has no tableReads declaration", name, definition.SourceTable)
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
