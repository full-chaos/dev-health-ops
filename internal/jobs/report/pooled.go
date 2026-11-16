package report

import (
	"fmt"
	"strings"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/teamscope"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/testopspooled"
)

// pooledChart is the definition of a TestOps duration or queue chart metric: the value of a bucket is taken over the raw
// samples of its runs (the shared package testopspooled, the very text the TestOps tiles read), never as the mean of the
// stored per-repository-day values: a percentile or a mean of a pool cannot be made from the values of its parts.
type pooledChart struct {
	table      string
	source     string
	expression string
	asOf       time.Time
}

// pooledCharts maps a registry metric to its pooled definition. The registry keeps naming the daily table the metric is
// stored in (the chart reports the table it reads, as for chartRules), so validateChartMetrics still sees a declared table.
var pooledCharts = map[string]pooledChart{
	"p95_duration_seconds":       {table: "testops_pipeline_metrics_daily", source: testopspooled.PipelineRunsSource, expression: testopspooled.P95},
	"median_duration_seconds":    {table: "testops_pipeline_metrics_daily", source: testopspooled.PipelineRunsSource, expression: testopspooled.Median},
	"avg_queue_seconds":          {table: "testops_pipeline_metrics_daily", source: testopspooled.PipelineRunsSource, expression: testopspooled.QueueMean},
	"p95_queue_seconds":          {table: "testops_pipeline_metrics_daily", source: testopspooled.PipelineRunsSource, expression: testopspooled.QueueP95},
	"suite_duration_p95_seconds": {table: "testops_test_metrics_daily", source: testopspooled.SuiteRunsSource, expression: testopspooled.P95},
	"suite_duration_p50_seconds": {table: "testops_test_metrics_daily", source: testopspooled.SuiteRunsSource, expression: testopspooled.Median},
}

// withPooledChart returns the definition of a chart of the metric with its pooled definition when it has one. asOf is the
// instant the team owners are read at (the request, as the tiles do).
func withPooledChart(definition metricDefinition, asOf time.Time) metricDefinition {
	if pooled, ok := pooledCharts[definition.CanonicalName]; ok && definition.SourceTable == pooled.table {
		pooled.asOf = asOf.UTC()
		definition.pooled = &pooled
	}
	return definition
}

// pooledDateBounds are the bounds of a chart with no time range: every day a Date holds.
const (
	pooledFirstDay = "1970-01-01"
	pooledEndDay   = "2149-06-06"
)

// buildPooledChartQuery builds the chart statement of a pooled metric. The statement reads the shared source over
// [start, end] (the chart's range is inclusive; the source's end is exclusive), and the same bound the tiles carry on the
// rows one statement reads: past it the read is refused, never answered in part, and the chart fails as unavailable.
func buildPooledChartQuery(spec ChartSpec, definition metricDefinition) (string, []any, error) {
	pooled := definition.pooled
	if spec.OrganizationID == "" {
		return "", nil, ErrContractMismatch
	}
	start, endExclusive := pooledFirstDay, pooledEndDay
	if spec.TimeRangeStart != "" {
		if _, err := time.Parse(time.DateOnly, spec.TimeRangeStart); err != nil {
			return "", nil, ErrContractMismatch
		}
		start = spec.TimeRangeStart
	}
	if spec.TimeRangeEnd != "" {
		last, err := time.Parse(time.DateOnly, spec.TimeRangeEnd)
		if err != nil {
			return "", nil, ErrContractMismatch
		}
		endExclusive = last.AddDate(0, 0, 1).Format(time.DateOnly)
	}
	parameters := []any{
		clickhouse.Named("org_id", spec.OrganizationID),
		clickhouse.Named("start_date", start),
		clickhouse.Named("end_date", endExclusive),
	}

	xExpression, xType, temporal := "'total'", "String", false
	join := ""
	where := []string{"1"}
	switch spec.GroupBy {
	case "day":
		xExpression, xType, temporal = "toDate(day)", "Date", true
	case "week":
		xExpression, xType, temporal = "toStartOfWeek(day)", "Date", true
	case "month":
		xExpression, xType, temporal = "toStartOfMonth(day)", "Date", true
	case "repo":
		xExpression = "toString(repo_id)"
	case "service":
		xExpression = "ifNull(service_id, '')"
	case "team":
		// A team is the owner of the repository as of the request, a co-owned repository whole for each owner; the stored
		// team of a repository-day is not read.
		fromWhere, bindings := teamscope.OwnershipFromWhere(spec.OrganizationID, pooled.asOf)
		scope := ""
		if len(spec.FilterTeams) > 0 {
			scope = fmt.Sprintf("\n      AND o.team_id IN {%s:Array(String)}", teamscope.BindingTeamIDs)
			bindings = append(bindings, dhclickhouse.Binding{Name: teamscope.BindingTeamIDs, Value: spec.FilterTeams})
		}
		named, err := pooledNamedArguments(bindings)
		if err != nil {
			return "", nil, err
		}
		parameters = append(parameters, named...)
		join = fmt.Sprintf(`
    INNER JOIN (
        SELECT DISTINCT o.team_id AS owner_team_id, coalesce(toString(o.repo_id), toString(r.id)) AS owned_repo_id
%s%s
    ) AS own ON toString(repo_id) = own.owned_repo_id`, fromWhere, scope)
		xExpression = "own.owner_team_id"
	}
	if spec.GroupBy == "" && (spec.ChartType == "line" || spec.ChartType == "heatmap") {
		xExpression, xType, temporal = "toDate(day)", "Date", true
	}
	if len(spec.FilterTeams) > 0 && spec.GroupBy != "team" {
		condition, bindings := teamscope.RepoCondition(spec.OrganizationID, "toString(repo_id)", spec.FilterTeams, pooled.asOf)
		named, err := pooledNamedArguments(bindings)
		if err != nil {
			return "", nil, err
		}
		parameters = append(parameters, named...)
		where = append(where, condition)
	}
	if len(spec.FilterRepos) > 0 {
		where = append(where, "toString(repo_id) IN {filter_repos:Array(String)}")
		parameters = append(parameters, clickhouse.Named("filter_repos", spec.FilterRepos))
	}
	settings := fmt.Sprintf("SETTINGS max_rows_to_read = %d, read_overflow_mode = 'throw'", testopspooled.MaxRowsToRead)
	if spec.GroupBy == "" && (spec.ChartType == "scorecard" || spec.ChartType == "trend_delta" || spec.ChartType == "table") {
		return fmt.Sprintf(`SELECT
        CAST('total', '%s') AS x,
        CAST(NULL, 'Nullable(String)') AS group_value,
        %s AS y
    FROM %s%s
    WHERE
        %s
    %s`, xType, pooled.expression, pooled.source, join, strings.Join(where, " AND\n        "), settings), parameters, nil
	}
	order := "y DESC, x"
	if temporal {
		order = "x"
	}
	return fmt.Sprintf(`SELECT
        %s AS x,
        CAST(NULL, 'Nullable(String)') AS group_value,
        %s AS y
    FROM %s%s
    WHERE
        %s
    GROUP BY x
    HAVING y IS NOT NULL
    ORDER BY %s
    %s`, xExpression, pooled.expression, pooled.source, join, strings.Join(where, " AND\n        "), order, settings), parameters, nil
}

// pooledNamedArguments binds each teamscope binding with a typed clickhouse.Named call, so every bound type is one the
// parameter census proves; a type it does not know is an error, never a value of an unproven encoding.
func pooledNamedArguments(bindings []dhclickhouse.Binding) ([]any, error) {
	arguments := make([]any, 0, len(bindings))
	for _, binding := range bindings {
		switch value := binding.Value.(type) {
		case string:
			arguments = append(arguments, clickhouse.Named(binding.Name, value))
		case []string:
			arguments = append(arguments, clickhouse.Named(binding.Name, value))
		case time.Time:
			// A time.Time bound to a DateTime64(3, 'UTC') parameter is refused by the server; the literal is not.
			arguments = append(arguments, clickhouse.Named(binding.Name, value.UTC().Format("2006-01-02 15:04:05.000")))
		default:
			return nil, fmt.Errorf("report: ownership binding %q has type %T, which has no proven encoding", binding.Name, binding.Value)
		}
	}
	return arguments, nil
}
