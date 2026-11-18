package report

import (
	"fmt"
	"strings"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/testops"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/teamscope"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/testopspooled"
	"github.com/full-chaos/dev-health-ops/internal/storage/clickhouse/liverow"
)

// pooledChart is the definition of a pooled chart metric: the value of a bucket is taken over the raw samples of its rows
// (for TestOps the shared package testopspooled, the very text the TestOps tiles read; for CI/CD the cicd daily writer's
// rules below; for the deploy time, pull request size and MTTR metrics the writers' rules of delivery_pooled.go), never as
// the mean of the stored per-repository-day values: a percentile or a mean of a pool cannot be made from the values of
// its parts.
type pooledChart struct {
	table      string
	source     string
	expression string
	// bind returns the parameters the source binds beyond the organization and the dates.
	bind func(asOf time.Time) []any
	asOf time.Time
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
	"avg_duration_minutes":       {table: "cicd_metrics_daily", source: cicdRunsSource, expression: cicdDurationMean},
	"p90_duration_minutes":       {table: "cicd_metrics_daily", source: cicdRunsSource, expression: cicdDurationP90},
	"avg_queue_minutes":          {table: "cicd_metrics_daily", source: cicdRunsSource, expression: cicdQueueMean},
	"deploy_time_p50_hours":      {table: "deploy_metrics_daily", source: deploymentsSource, expression: deliveryHoursMedian, bind: bindCanceledStatuses},
	"pr_size_p50_loc":            {table: "repo_metrics_daily", source: pullRequestSizesSource, expression: deliveryLOCMedian},
	"pr_size_p90_loc":            {table: "repo_metrics_daily", source: pullRequestSizesSource, expression: deliveryLOCP90},
	"mttr_hours":                 {table: "repo_metrics_daily", source: bugRepairsSource, expression: deliveryHoursMean},
	"mttr_p50_hours":             {table: "incident_metrics_daily", source: incidentRepairsSource, expression: deliveryHoursMedian, bind: bindMappingAsOf},
	"mttr_p90_hours":             {table: "incident_metrics_daily", source: incidentRepairsSource, expression: deliveryHoursP90, bind: bindMappingAsOf},
}

// The CI/CD pooled source holds the runs and samples of the cicd daily writer (internal/jobs/metrics/daily/cicd): a run
// counts on the UTC day it finishes (the writer's rule), the newest synced
// version of a run (FINAL) is the run, a deleted run is no run. A duration sample is finished_at - started_at in minutes
// when not negative, a queue sample started_at - queued_at in minutes when not negative; the stored duration_seconds and
// queue_seconds are not read, as the writer does not read them. Only the repository-days the writer computed are read: the
// keys whose NEWEST row is live (a retired repository-day, newest row retracted = 1, admits no run, also a run synced after
// the retraction). cicd_pooled_test.go pins these rules to the writer's text.
const (
	cicdRunMinutes          = "cicd_run_minutes"
	cicdQueueMinutes        = "cicd_queue_minutes"
	cicdCountedRunPredicate = "lowerUTF8(trimBoth(ifNull(status, ''))) != '" + testops.StatusDeleted + "'"
)

var cicdRunsSource = `(
    SELECT
        org_id,
        repo_id,
        service_id,
        toDate(finished_at) AS day,
        if(finished_at >= started_at, dateDiff('millisecond', started_at, finished_at) / 60000, NULL) AS ` + cicdRunMinutes + `,
        if(queued_at IS NOT NULL AND started_at >= queued_at, dateDiff('millisecond', queued_at, started_at) / 60000, NULL) AS ` + cicdQueueMinutes + `
    FROM ci_pipeline_runs FINAL
    WHERE org_id = {org_id:String}
      AND finished_at IS NOT NULL
      AND toDate(finished_at) >= {start_date:Date} AND toDate(finished_at) < {end_date:Date}
      AND ` + cicdCountedRunPredicate + `
      AND ` + testops.NotOpenRunSQL + `
      AND (repo_id, toDate(finished_at)) IN (
          SELECT repo_id, day FROM cicd_metrics_daily
          WHERE org_id = {org_id:String} AND day >= {start_date:Date} AND day < {end_date:Date}
          GROUP BY repo_id, day
          HAVING ` + liverow.NewestPredicate("cicd_metrics_daily", "") + `)
) AS cicd_pipeline_runs`

// The writer's mean and p90 (linear between closest ranks), NULL for a group with no sample.
const (
	cicdDurationMean = "if(count(" + cicdRunMinutes + ") = 0, NULL, avg(" + cicdRunMinutes + "))"
	cicdDurationP90  = "if(count(" + cicdRunMinutes + ") = 0, NULL, quantileExactInclusive(0.9)(" + cicdRunMinutes + "))"
	cicdQueueMean    = "if(count(" + cicdQueueMinutes + ") = 0, NULL, avg(" + cicdQueueMinutes + "))"
)

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
	if pooled.bind != nil {
		parameters = append(parameters, pooled.bind(pooled.asOf)...)
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
	case "repo", "service", "team":
		switch {
		case !definition.hasDimension(spec.GroupBy):
			// As buildChartQuery: a metric without the dimension is one bucket, never grouped by it.
			xExpression = "'unscoped'"
		case spec.GroupBy == "repo":
			xExpression = "toString(repo_id)"
		case spec.GroupBy == "service":
			xExpression = "ifNull(service_id, '')"
		default:
			ownerJoin, named, err := repositoryOwnerJoin(spec.OrganizationID, spec.FilterTeams, pooled.asOf)
			if err != nil {
				return "", nil, err
			}
			parameters = append(parameters, named...)
			join = ownerJoin
			xExpression = "own.owner_team_id"
		}
	}
	if spec.GroupBy == "" && (spec.ChartType == "line" || spec.ChartType == "heatmap") {
		xExpression, xType, temporal = "toDate(day)", "Date", true
	}
	if len(spec.FilterTeams) > 0 && definition.hasDimension("team") && spec.GroupBy != "team" {
		condition, bindings := teamscope.RepoCondition(spec.OrganizationID, "toString(repo_id)", spec.FilterTeams, pooled.asOf)
		if condition == "" {
			condition = "0"
		}
		named, err := pooledNamedArguments(bindings)
		if err != nil {
			return "", nil, err
		}
		parameters = append(parameters, named...)
		where = append(where, condition)
	}
	if len(spec.FilterRepos) > 0 && definition.hasDimension("repo") {
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

// repositoryOwnerJoin is the join of a team group: one row per (owner team, owned repository) as of asOf, so a co-owned
// repository is whole in each owner's bucket and the stored team of a row is not read. A team filter keeps the filtered
// owners only.
func repositoryOwnerJoin(orgID string, teams []string, asOf time.Time) (string, []any, error) {
	fromWhere, bindings := teamscope.OwnershipFromWhere(orgID, asOf)
	scope := ""
	if len(teams) > 0 {
		scope = fmt.Sprintf("\n      AND o.team_id IN {%s:Array(String)}", teamscope.BindingTeamIDs)
		bindings = append(bindings, dhclickhouse.Binding{Name: teamscope.BindingTeamIDs, Value: teams})
	}
	named, err := pooledNamedArguments(bindings)
	if err != nil {
		return "", nil, err
	}
	return fmt.Sprintf(`
    INNER JOIN (
        SELECT DISTINCT o.team_id AS owner_team_id, coalesce(toString(o.repo_id), toString(r.id)) AS owned_repo_id
%s%s
    ) AS own ON toString(repo_id) = own.owned_repo_id`, fromWhere, scope), named, nil
}

// repositoryOwnedTables are the charted daily tables with no team column: the team of a row is the owner of its
// repository as of the chart run, as for a pooled chart.
var repositoryOwnedTables = map[string]bool{"cicd_metrics_daily": true}

// withRepositoryOwners returns the definition of a stored-row chart of a repository-owned table with its owners instant
// asOf, when the metric has the team dimension.
func withRepositoryOwners(definition metricDefinition, asOf time.Time) metricDefinition {
	if definition.pooled == nil && repositoryOwnedTables[definition.SourceTable] && definition.hasDimension("team") {
		at := asOf.UTC()
		definition.ownersAsOf = &at
	}
	return definition
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
