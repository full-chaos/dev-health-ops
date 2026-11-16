package report

import (
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/testopspooled"
)

var pooledMetricExpressions = map[string]struct{ source, expression string }{
	"p95_duration_seconds":       {testopspooled.PipelineRunsSource, testopspooled.P95},
	"median_duration_seconds":    {testopspooled.PipelineRunsSource, testopspooled.Median},
	"avg_queue_seconds":          {testopspooled.PipelineRunsSource, testopspooled.QueueMean},
	"p95_queue_seconds":          {testopspooled.PipelineRunsSource, testopspooled.QueueP95},
	"suite_duration_p95_seconds": {testopspooled.SuiteRunsSource, testopspooled.P95},
	"suite_duration_p50_seconds": {testopspooled.SuiteRunsSource, testopspooled.Median},
}

func pooledDefinition(t *testing.T, metric string) metricDefinition {
	t.Helper()
	registered, ok := supportedMetrics[metric]
	if !ok {
		t.Fatalf("%s is not a registry metric", metric)
	}
	definition := withPooledChart(withChartRule(registered), time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC))
	if definition.pooled == nil {
		t.Fatalf("%s has no pooled definition", metric)
	}
	return definition
}

// The six TestOps duration and queue metrics are charted over the raw samples: the statement reads the shared source and
// expression, never avg() of the stored column, and the registry keeps its daily table.
func TestPooledMetricsReadTheSharedSourceAndExpression(t *testing.T) {
	t.Parallel()
	for metric, want := range pooledMetricExpressions {
		definition := pooledDefinition(t, metric)
		statement, _, err := buildChartQuery(ChartSpec{ChartType: "bar", Metric: metric, OrganizationID: "org-1",
			TimeRangeStart: "2026-01-01", TimeRangeEnd: "2026-01-06"}, definition)
		if err != nil {
			t.Fatalf("%s: %v", metric, err)
		}
		if !strings.Contains(statement, want.source) || !strings.Contains(statement, want.expression) {
			t.Errorf("%s: the statement is not the shared source and expression:\n%s", metric, statement)
		}
		if strings.Contains(statement, "avg("+metric+")") || strings.Contains(statement, "AVG("+metric+")") {
			t.Errorf("%s: the statement still takes the mean of the stored value", metric)
		}
		if definition.SourceTable != supportedMetrics[metric].SourceTable {
			t.Errorf("%s: the chart names %s, the registry %s", metric, definition.SourceTable, supportedMetrics[metric].SourceTable)
		}
	}
	for _, other := range []string{"success_rate", "flake_rate", "rerun_rate", "p95_queue_minutes", "avg_duration_minutes"} {
		if definition, ok := supportedMetrics[other]; ok && withPooledChart(definition, time.Now()).pooled != nil {
			t.Errorf("%s must not be pooled", other)
		}
	}
}

// The shared source carries the counted-run predicate (a deleted run is no sample) and the bound the tiles carry.
func TestPooledStatementCarriesTheDeletedRunPredicateAndTheRowBound(t *testing.T) {
	t.Parallel()
	statement, _, err := buildChartQuery(ChartSpec{ChartType: "bar", Metric: "p95_duration_seconds", OrganizationID: "org-1"},
		pooledDefinition(t, "p95_duration_seconds"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(statement, testopspooled.CountedRunPredicate) {
		t.Errorf("a deleted run is not left out:\n%s", statement)
	}
	if !strings.Contains(statement, "max_rows_to_read = 5000000, read_overflow_mode = 'throw'") {
		t.Errorf("the pooled read has no row bound:\n%s", statement)
	}
	if !strings.Contains(statement, "HAVING y IS NOT NULL") {
		t.Errorf("a bucket with no sample is not left out:\n%s", statement)
	}
}

// The chart range is inclusive and the shared source's end is exclusive.
func TestPooledWindowEndIsInclusive(t *testing.T) {
	t.Parallel()
	_, parameters, err := buildChartQuery(ChartSpec{ChartType: "bar", Metric: "p95_duration_seconds", OrganizationID: "org-1",
		TimeRangeStart: "2026-01-01", TimeRangeEnd: "2026-01-31"}, pooledDefinition(t, "p95_duration_seconds"))
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]any{}
	for _, parameter := range parameters {
		named, ok := parameter.(driver.NamedValue)
		if !ok {
			t.Fatalf("parameter %T is not a named value", parameter)
		}
		got[named.Name] = named.Value
	}
	if got["org_id"] != "org-1" || got["start_date"] != "2026-01-01" || got["end_date"] != "2026-02-01" {
		t.Fatalf("parameters = %v, want org-1, 2026-01-01 and the exclusive end 2026-02-01", got)
	}
}

// A team group reads the owners at the chart (an ownership join), the stored team column is not read.
func TestPooledTeamGroupReadsTheOwnersNotTheStoredTeam(t *testing.T) {
	t.Parallel()
	statement, _, err := buildChartQuery(ChartSpec{ChartType: "bar", Metric: "p95_duration_seconds", GroupBy: "team", OrganizationID: "org-1"},
		pooledDefinition(t, "p95_duration_seconds"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(statement, "own.owner_team_id AS x") || !strings.Contains(statement, "FROM team_repo_ownership AS o FINAL") {
		t.Errorf("the team group does not read the owners at the request:\n%s", statement)
	}
	if strings.Contains(statement, "team_id AS x") && !strings.Contains(statement, "owner_team_id AS x") {
		t.Errorf("the team group reads the stored team column")
	}
}

// The dedup guard counts the daily table's retried generations; a pooled chart reads the raw runs, so it has none.
func TestPooledChartHasNoDedupGuard(t *testing.T) {
	t.Parallel()
	if _, _, _, ok := dedupGuardQuery(ChartSpec{Metric: "p95_duration_seconds", OrganizationID: "org-1"}, pooledDefinition(t, "p95_duration_seconds")); ok {
		t.Fatal("a pooled chart has a dedup guard query")
	}
}
