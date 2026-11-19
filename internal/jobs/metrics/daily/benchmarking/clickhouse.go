package benchmarking

import (
	"context"
	"fmt"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily/familyerr"
	"sort"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/ownershipwindow"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/rowview"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/teamscope"
	"github.com/full-chaos/dev-health-ops/internal/storage/clickhouse/liverow"
	"github.com/full-chaos/dev-health-ops/internal/teamactive"
)

// conn is the narrow ClickHouse capability this package needs, matching
// internal/jobs/metrics/daily/cicd's conn interface shape.
type conn interface {
	Query(ctx context.Context, query string, args ...any) (driver.Rows, error)
	PrepareBatch(ctx context.Context, query string, opts ...driver.PrepareBatchOption) (driver.Batch, error)
}

// metricDefinitions ports _common.py's METRIC_DEFINITIONS (:46-156) verbatim.
// The value column, the grouping columns and the supported scopes are all part
// of the query's meaning, so they are reproduced rather than re-derived.
var metricDefinitions = map[string]MetricDefinition{
	"success_rate":              pooledRate(pipelineMetric(""), "success_count", completedRuns),
	"failure_rate":              pooledRate(pipelineMetric(""), "failure_count", completedRuns),
	"rerun_rate":                pooledRate(pipelineMetric(""), "rerun_rate * pipelines_count", "pipelines_count"),
	"median_duration_seconds":   pipelineMetric("median_duration_seconds"),
	"p95_duration_seconds":      pipelineMetric("p95_duration_seconds"),
	"avg_queue_seconds":         pipelineMetric("avg_queue_seconds"),
	"p95_queue_seconds":         pipelineMetric("p95_queue_seconds"),
	"pass_rate":                 pooledRate(testMetric(""), "passed_count", executedCases),
	"flake_rate":                pooledRate(testMetric(""), measuredCases("flake_cases", "flake_rate", "flake_cases"), measuredCases("flake_cases", "flake_rate", "distinct_cases")),
	"retry_dependency_rate":     pooledRate(testMetric(""), measuredCases("distinct_cases", "retry_dependency_rate", "retry_dependency_rate * distinct_cases"), measuredCases("distinct_cases", "retry_dependency_rate", "distinct_cases")),
	"failure_recurrence_score":  testMetric("failure_recurrence_score"),
	"line_coverage_pct":         pooledRate(coverageMetric(""), "if("+coveredLines+", 100 * lines_covered, NULL)", "if("+coveredLines+", lines_total, NULL)"),
	"branch_coverage_pct":       pooledRate(coverageMetric(""), "if("+coveredBranches+", 100 * branches_covered, NULL)", "if("+coveredBranches+", branches_total, NULL)"),
	"coverage_delta_pct":        coverageMetric("coverage_delta_pct"),
	"coverage_regression_count": coverageMetric("coverage_regression_count"),
	"cycle_time_hours": {
		Table:             "work_item_metrics_daily",
		ValueColumn:       "cycle_time_p50_hours",
		ScopeSupport:      map[string]bool{ScopeTeam: true, ScopeGlobal: true},
		InnerGroupColumns: []string{"team_id", "work_scope_id", "provider", "row_view"},
		// Each item once in the organization (CHAOS-8863): legacy rows and the org view.
		ExtraFilters: []string{rowview.OrgPredicate("work_item_metrics_daily", "")},
	},
	// The stored rate is new_bugs_count / new_items_count, 0 when there is no new item, and is not read. A row with no
	// new item adds 0 to both sums, so a point with none has no value, never 0.
	"defect_intro_rate": pooledRate(MetricDefinition{
		Table:             "work_item_metrics_daily",
		ScopeSupport:      map[string]bool{ScopeTeam: true, ScopeGlobal: true},
		InnerGroupColumns: []string{"team_id", "work_scope_id", "provider", "row_view"},
		// Each item once in the organization (CHAOS-8863): legacy rows and the org view.
		ExtraFilters: []string{rowview.OrgPredicate("work_item_metrics_daily", "")},
	}, "new_bugs_count", "new_items_count"),
	"deployment_frequency": {
		Table:             "dora_metrics_daily",
		ValueColumn:       "value",
		ScopeSupport:      map[string]bool{ScopeRepo: true, ScopeGlobal: true},
		InnerGroupColumns: []string{"repo_id"},
		// dora_metrics_daily holds many metrics in one table, so this one must
		// select itself by name.
		ExtraFilters: []string{"metric_name = {metric_name:String}"},
	},
}

// testopsDailyTables hold the team as a value of the (org, repo, day) key: the main owner of the day it was computed.
// Their team series read the owners at the run instead (ownersJoin).
var testopsDailyTables = map[string]bool{
	"testops_pipeline_metrics_daily": true,
	"testops_test_metrics_daily":     true,
	"testops_coverage_metrics_daily": true,
}

func testopsScopes() map[string]bool {
	return map[string]bool{ScopeRepo: true, ScopeTeam: true, ScopeGlobal: true}
}

func pipelineMetric(column string) MetricDefinition {
	return MetricDefinition{
		Table: "testops_pipeline_metrics_daily", ValueColumn: column,
		ScopeSupport: testopsScopes(), InnerGroupColumns: []string{"repo_id"},
	}
}

// pooledRate is a ratio of stored counts: the group's numerator over its denominator, so a repo-day weighs by its
// denominator. A stored rate times pipelines_count is its count (rerun_rate is reruns / pipelines_count, unrounded). A
// repo-day whose denominator is 0 adds 0 to both sums.
func pooledRate(definition MetricDefinition, numerator, denominator string) MetricDefinition {
	definition.Numerator, definition.Denominator = numerator, denominator
	return definition
}

// completedRuns are the runs that succeeded or failed: the denominator of the success and failure rates. A repo-day
// with none (cancelled runs only, or a retraction row of zeros) is in neither sum, so a day with none in the group has
// no point, never a sample of 0. The stored success_rate and failure_rate columns hold 0 on that day and are not read;
// the counts are exact UInt32 columns.
const completedRuns = "success_count + failure_count"

// executedCases are the cases that passed or failed: the denominator of the pass rate. A repo-day with none (every
// case skipped, or a retraction row of zeros) is in neither sum, so a day with none in the group has no point, never a
// sample of 0. The stored pass_rate column divides by every case, skipped ones too, holds 0 on that day and is not
// read; the counts are exact UInt32 columns.
const executedCases = "passed_count + failed_count"

// measuredCases is a flake or retry count or the distinct cases of a repo-day, NULL when the rate, the distinct cases
// or (for flake) the flake cases are NULL (a day that measured no case, or a row stored before the counts existed), so such a
// repo-day is in neither sum. retry_dependency_rate * distinct_cases is a float, the form of rerun_rate *
// pipelines_count, not a stored count.
func measuredCases(count, rate, expression string) string {
	return "if(" + rate + " IS NULL OR distinct_cases IS NULL OR " + count + " IS NULL, NULL, " + expression + ")"
}

func testMetric(column string) MetricDefinition {
	return MetricDefinition{
		Table: "testops_test_metrics_daily", ValueColumn: column,
		ScopeSupport: testopsScopes(), InnerGroupColumns: []string{"repo_id"},
	}
}

// coveredLines is a repo-day whose stored line counts measure a coverage: both counts set and lines_total above 0 (the
// rule of the query-api line coverage). Any other repo-day (a NULL count, a stored pct with no counts, no lines) is in
// neither sum, so a point made only of such repo-days has no value, never 0. The stored line_coverage_pct is not read.
// coverage_delta_pct (a difference of two percentages) and coverage_regression_count (a count) stay the mean.
const coveredLines = "lines_covered IS NOT NULL AND lines_total IS NOT NULL AND lines_total > 0"

// coveredBranches is the same rule over the stored branch counts. A row stored before the counts existed reads NULL and
// is in neither sum; the stored branch_coverage_pct is not read.
const coveredBranches = "branches_covered IS NOT NULL AND branches_total IS NOT NULL AND branches_total > 0"

func coverageMetric(column string) MetricDefinition {
	return MetricDefinition{
		Table: "testops_coverage_metrics_daily", ValueColumn: column,
		ScopeSupport: testopsScopes(), InnerGroupColumns: []string{"repo_id"},
	}
}

// clickHouseDateLayout is the wire form for a {name:Date} query parameter.
// See FetchMetricSeriesByScope's binding note for why a time.Time cannot be
// bound directly.
const clickHouseDateLayout = "2006-01-02"

// ClickHouseLoader implements SeriesFetcher against a real ClickHouse.
type ClickHouseLoader struct {
	conn  conn
	orgID string
	// droppedInactive holds the inactive team ids whose series a read left out.
	droppedInactive map[string]struct{}
	// ownersAsOf is the run instant the owners of a TestOps team series are read at.
	ownersAsOf time.Time
}

// SetOwnersAsOf sets the instant at which a TestOps team series reads the owners of its repositories: the run
// instant, as the analytics team series read them at the request. A TestOps team read without it is refused.
func (loader *ClickHouseLoader) SetOwnersAsOf(asOf time.Time) { loader.ownersAsOf = asOf.UTC() }

// DroppedInactiveTeams returns, sorted, the inactive team ids whose series the
// loader's team-scope reads have left out so far.
func (loader *ClickHouseLoader) DroppedInactiveTeams() []string {
	ids := make([]string, 0, len(loader.droppedInactive))
	for id := range loader.droppedInactive {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func NewClickHouseLoader(connection conn, orgID string) (*ClickHouseLoader, error) {
	if connection == nil {
		return nil, fmt.Errorf("benchmarking: clickhouse connection is required")
	}
	return &ClickHouseLoader{conn: connection, orgID: orgID}, nil
}

// liveRowHaving keeps a key of a team-keyed daily table only when its newest
// row is a measurement (see package liverow). A retraction row holds 0 in a
// column that is not Nullable: it is not a sample of the mean, and a team that
// holds only such rows has no series to benchmark.
func liveRowHaving(table string) string {
	if !liverow.Registered(table) {
		return ""
	}
	return "\n    HAVING " + liverow.NewestPredicate(table, "")
}

// FetchMetricSeriesByScope ports fetch_metric_series_by_scope
// (_common.py:259-341). The SQL is Python's shape verbatim: an inner
// argMax(value, computed_at) grouped by day plus the metric's own grouping
// columns, then an outer avg() per (scope_key, day) with NULL metric values
// dropped, ordered by scope_key then day.
//
// NOTE ON ORG SCOPING: Python's query carries NO org_id predicate at all --
// the sink it runs against is already org-bound (BaseMetricsSink), so the
// filtering happens a layer up. A Go loader talking straight to the driver has
// no such binding, so an explicit org_id filter is added. That NARROWS what
// already-correct data satisfies the query and cannot widen it -- the same
// reasoning CHAOS-4775 applied to family_readback.
// Several ValueColumn choices (e.g. testops_pipeline_metrics_daily's
// duration/queue columns, testops_coverage_metrics_daily's coverage
// columns, work_item_metrics_daily.cycle_time_p50_hours) are
// Nullable(Float64); the projected column is always tuple-wrapped so
// argMax cannot skip a newest row that carries NULL, regardless of which
// metric is selected.
func (loader *ClickHouseLoader) FetchMetricSeriesByScope(
	ctx context.Context, metricName string, startDay, endDay time.Time, scopeType string,
) (map[string][]MetricPoint, error) {
	if loader == nil || loader.conn == nil {
		return nil, fmt.Errorf("benchmarking: loader unavailable")
	}
	canonical := CanonicalMetricName(metricName)
	definition, ok := metricDefinitions[canonical]
	if !ok {
		return nil, fmt.Errorf("benchmarking: unsupported metric %q", metricName)
	}
	if !definition.ScopeSupport[scopeType] {
		return nil, fmt.Errorf("benchmarking: metric %s does not support scope_type=%s", canonical, scopeType)
	}
	ownersRead := scopeType == ScopeTeam && testopsDailyTables[definition.Table]
	if ownersRead && loader.ownersAsOf.IsZero() {
		return nil, fmt.Errorf("benchmarking: metric %s: a TestOps team series needs the run instant of its owners", canonical)
	}

	filters := []string{"day >= {start_day:Date}", "day <= {end_day:Date}", "org_id = {org_id:String}"}
	filters = append(filters, definition.ExtraFilters...)

	// The team of a work-item key is the team of its NEWEST row. A blank
	// scope_key is dropped below, as Python drops a blank scope_key. A TestOps
	// team read keys by repository first and takes the owners after.
	var scopeExpression string
	switch {
	case scopeType == ScopeRepo || ownersRead:
		scopeExpression = "toString(repo_id)"
	case scopeType == ScopeTeam:
		scopeExpression = "ifNull((argMax(tuple(team_id), computed_at)).1, '')"
	default:
		scopeExpression = "'global'"
	}

	// org_id LEADS the group, matching every source table's CURRENT sorting
	// key (migrations 027/042 prepended it). The org filter above is already
	// pre-aggregation so this cannot change a row today -- org_id is constant
	// within the query -- but it makes the tenant boundary STRUCTURAL rather
	// than dependent on the filter staying where it is. Hoisting that filter
	// into an outer query is a plausible refactor, and it is exactly what
	// turns this shape into ai-families' cross-tenant P1.
	innerGroup := append([]string{"org_id", "day"}, definition.InnerGroupColumns...)
	keyAlias := "scope_key"
	if ownersRead {
		keyAlias = "repo_key"
	}
	pooled := definition.Numerator != ""
	valueSelect := "(argMax(tuple(" + definition.ValueColumn + "), computed_at)).1 AS metric_value"
	outerValue, outerWhere := "avg(metric_value)", "metric_value IS NOT NULL"
	if pooled {
		valueSelect = "(argMax(tuple(" + definition.Numerator + ", " + definition.Denominator + "), computed_at)).1 AS metric_num,\n" +
			"        (argMax(tuple(" + definition.Numerator + ", " + definition.Denominator + "), computed_at)).2 AS metric_den"
		outerValue, outerWhere = pooledValue(""), "1"
	}
	inner := fmt.Sprintf(`
    SELECT
        %s AS %s,
        day,
        %s
    FROM %s
    WHERE %s
    GROUP BY %s%s`,
		scopeExpression, keyAlias, valueSelect, definition.Table,
		strings.Join(filters, " AND "), strings.Join(innerGroup, ", "), liveRowHaving(definition.Table),
	)
	query := fmt.Sprintf(`
SELECT scope_key, day, %s AS value
FROM (%s
)
WHERE %s
GROUP BY scope_key, day
ORDER BY scope_key, day`, outerValue, inner, outerWhere)
	var ownerArguments []any
	if ownersRead {
		var err error
		query, ownerArguments, err = ownersJoin(inner, loader.orgID, loader.ownersAsOf, pooled)
		if err != nil {
			return nil, err
		}
	}

	arguments := []any{
		// Date parameters are bound as YYYY-MM-DD STRINGS, not time.Time. The
		// driver serialises a time.Time for a {name:Date} parameter as a full
		// DateTime literal and the server rejects it outright -- "Cannot parse
		// date here: toDateTime('2026-08-24 00:00:00') cannot be parsed as
		// Date". Caught on the sibling compounding_risk loader by CI (#2231's
		// shard leg) and fixed here in the same pass BEFORE this family ever
		// reached CI, since the shape is identical. Same fix and reasoning as
		// RecommendationsLoader.windowArguments (recommendations_loader.go:77-97);
		// the string form is also the closer mirror of what Python's
		// clickhouse-connect puts on the wire for a datetime.date.
		clickhouse.Named("start_day", startDay.Format(clickHouseDateLayout)),
		clickhouse.Named("end_day", endDay.Format(clickHouseDateLayout)),
		clickhouse.Named("org_id", loader.orgID),
	}
	if canonical == "deployment_frequency" {
		arguments = append(arguments, clickhouse.Named("metric_name", "deployment_frequency"))
	}
	arguments = append(arguments, ownerArguments...)

	// A team series is a target the job benchmarks, so an inactive team (a
	// superseded or retired id) has none: a day of it that was not computed
	// again still holds its last row.
	var inactive teamactive.Inactive
	if scopeType == ScopeTeam {
		loaded, err := teamactive.LoadInactive(ctx, loader.conn, loader.orgID)
		if err != nil {
			return nil, fmt.Errorf("fetch metric series %s: %w", canonical, err)
		}
		inactive = loaded
	}

	rows, err := loader.conn.Query(ctx, query, arguments...)
	if err != nil {
		return nil, fmt.Errorf("fetch metric series %s: %w", canonical, err)
	}
	defer rows.Close()

	result := make(map[string][]MetricPoint)
	for rows.Next() {
		var (
			scopeKey string
			day      time.Time
			value    *float64
		)
		if err := rows.Scan(&scopeKey, &day, &value); err != nil {
			return nil, fmt.Errorf("scan metric series row: %w", err)
		}
		// Python drops a blank scope_key and a NULL value
		// (_common.py:333-340); `avg()` over an all-NULL group is NULL, which
		// the outer WHERE cannot catch because it filters the INNER value.
		if scopeKey == "" || value == nil {
			continue
		}
		if inactive.Has(scopeKey) {
			if loader.droppedInactive == nil {
				loader.droppedInactive = map[string]struct{}{}
			}
			loader.droppedInactive[scopeKey] = struct{}{}
			continue
		}
		result[scopeKey] = append(result[scopeKey], MetricPoint{Day: day, Value: *value})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

// LiveInputKeys counts, per table of InputTables(metrics, pairs), the
// organization's (repo_id, day) keys over [InputWindowStart(asOfDay), asOfDay]
// whose newest row is live by the rule FetchMetricSeriesByScope applies. Every
// table is in the result, 0 when it holds no such key. A failed read is an
// error, never 0.
func (loader *ClickHouseLoader) LiveInputKeys(
	ctx context.Context, asOfDay time.Time, metrics [][2]string, pairs [][3]string,
) (map[string]uint64, error) {
	if loader == nil || loader.conn == nil {
		return nil, fmt.Errorf("benchmarking: loader unavailable")
	}
	tables, err := InputTables(metrics, pairs)
	if err != nil {
		return nil, err
	}
	arms := make([]string, 0, len(tables))
	for _, table := range tables {
		arms = append(arms, fmt.Sprintf(`
    SELECT '%s' AS input_table
    FROM %s
    WHERE day >= {start_day:Date} AND day <= {end_day:Date} AND org_id = {org_id:String}
    GROUP BY org_id, day, repo_id%s`, table, table, liveRowHaving(table)))
	}
	query := `
SELECT input_table, count() AS live_keys
FROM (` + strings.Join(arms, "\n    UNION ALL") + `
)
GROUP BY input_table
ORDER BY input_table`
	rows, err := loader.conn.Query(ctx, query,
		clickhouse.Named("start_day", InputWindowStart(asOfDay).Format(clickHouseDateLayout)),
		clickhouse.Named("end_day", asOfDay.Format(clickHouseDateLayout)),
		clickhouse.Named("org_id", loader.orgID),
	)
	if err != nil {
		return nil, fmt.Errorf("benchmarking: read the live input keys: %w", err)
	}
	defer rows.Close()
	live := make(map[string]uint64, len(tables))
	for _, table := range tables {
		live[table] = 0
	}
	for rows.Next() {
		var (
			table string
			keys  uint64
		)
		if err := rows.Scan(&table, &keys); err != nil {
			return nil, fmt.Errorf("benchmarking: scan the live input keys: %w", err)
		}
		live[table] = keys
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("benchmarking: read the live input keys: %w", err)
	}
	return live, nil
}

// ownersJoin is the statement of a TestOps team series: the newest value of each (org, repository, day) key, then
// the owners of the repository at asOf by the ownership rule of every team scope (teamscope.OwnershipFromWhere:
// validity window, active teams, catalog guard). A co-owned repository goes in full to each owner, never at its share,
// and a repository no team owns gives no team a series. Every day of the window takes the owners at asOf.
func ownersJoin(inner, orgID string, asOf time.Time, pooled bool) (string, []any, error) {
	fromWhere, bindings := teamscope.OwnershipFromWhere(orgID, asOf)
	outerValue, outerWhere := "avg(k.metric_value)", "k.metric_value IS NOT NULL"
	if pooled {
		outerValue, outerWhere = pooledValue("k."), "1"
	}
	query := fmt.Sprintf(`
SELECT own.owner_team_id AS scope_key, k.day AS day, %s AS value
FROM (%s
) AS k
INNER JOIN (
    SELECT DISTINCT o.team_id AS owner_team_id, %s AS owned_repo_id
%s
) AS own ON k.repo_key = own.owned_repo_id
WHERE %s
GROUP BY scope_key, day
ORDER BY scope_key, day`, outerValue, inner, ownershipwindow.RepoKey, fromWhere, outerWhere)
	arguments, err := namedArguments(bindings)
	if err != nil {
		return "", nil, err
	}
	return query, arguments, nil
}

// pooledValue is the ratio of sums of a pooled metric over the repo-days of one (scope, day), NULL when no run: the
// caller drops a NULL value, so such a day is never a sample of 0.
func pooledValue(prefix string) string {
	return "if(sum(" + prefix + "metric_den) = 0, NULL, sum(" + prefix + "metric_num) / sum(" + prefix + "metric_den))"
}

// namedArguments binds each teamscope binding with a typed clickhouse.Named call, so every bound type is one the
// parameter census proves; a type it does not know is an error, never a value of an unproven encoding.
func namedArguments(bindings []dhclickhouse.Binding) ([]any, error) {
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
			return nil, fmt.Errorf("benchmarking: ownership binding %q has type %T, which has no proven encoding", binding.Name, binding.Value)
		}
	}
	return arguments, nil
}

// Writer persists all six benchmarking output tables.
type Writer struct {
	conn conn
}

func NewWriter(connection conn) (*Writer, error) {
	if connection == nil {
		return nil, fmt.Errorf("benchmarking: clickhouse connection is required")
	}
	return &Writer{conn: connection}, nil
}

// WriteOutputs writes every non-empty collection across the six benchmarking
// output tables and returns the total rows written. Column lists and their
// order are the sink's verbatim (sinks/clickhouse/dora.py:56-198).
//
// Fails closed on an empty orgID: org_id is a filter column on all six tables
// and an unscoped row is invisible to every org-bound read.
//
// Mirrors write_benchmarking_outputs' per-collection emptiness checks
// (runner.py:243-256) -- an empty collection is skipped, not written as zero
// rows.
//
// PARTIAL WRITES ARE REPORTED, NOT SWALLOWED (CHAOS-4288, codex r1 on #2235).
//
// The six tables are written sequentially and each write is a separate batch,
// so a failure on the third leaves the first two ON DISK. Every output table is
// a plain MergeTree with no version column, so nothing replaces those rows
// later -- and if the caller fails open to the Python bridge, the bridge writes
// all six again and the first two DUPLICATE. The duplication is invisible to
// readers (argMax-style reads still return a sane latest value) and shows up
// only as row-count growth, which is why it could run for a long time
// unnoticed.
//
// So this returns TWO things on failure that the old shape threw away:
//
//   - the TRUE number of rows written before the failure, not 0. Reporting 0
//     for a write that landed thousands of rows tells an operator the opposite
//     of what happened, and the re-drive decision depends on knowing.
//   - an error wrapping familyerr.ErrPartialWrite, but ONLY once something has
//     actually been written. Before the first successful write there is nothing
//     to duplicate, so the ordinary fail-open path is still correct and the
//     error is returned plain. Wrapping too eagerly would suppress the bridge's
//     legitimate fallback and lose the family for that partition -- the
//     opposite failure, equally silent.
//
// The compute/write split lives in the caller: everything above this function
// is pure computation and can fail freely without touching a table.
func (writer *Writer) WriteOutputs(ctx context.Context, outputs Outputs, orgID string) (int, error) {
	if writer == nil || writer.conn == nil {
		return 0, fmt.Errorf("benchmarking: writer unavailable")
	}
	if orgID == "" {
		return 0, fmt.Errorf("benchmarking: organization id is required to write benchmarking tables")
	}

	// Ordered so the failure report can name the table, and so "how many
	// batches landed" is answerable from the index alone.
	steps := []struct {
		table string
		write func() (int, error)
	}{
		{"testops_metric_baselines", func() (int, error) {
			return writer.writeBaselines(ctx, outputs.Baselines, orgID)
		}},
		{"testops_maturity_bands", func() (int, error) {
			return writer.writeMaturityBands(ctx, outputs.MaturityBands, orgID)
		}},
		{"testops_metric_anomalies", func() (int, error) {
			return writer.writeAnomalies(ctx, outputs.Anomalies, orgID)
		}},
		{"testops_period_comparisons", func() (int, error) {
			return writer.writePeriodComparisons(ctx, outputs.PeriodComparisons, orgID)
		}},
		{"testops_metric_correlations", func() (int, error) {
			return writer.writeCorrelations(ctx, outputs.Correlations, orgID)
		}},
		{"testops_benchmark_insights", func() (int, error) {
			return writer.writeInsights(ctx, outputs.Insights, orgID)
		}},
	}

	total := 0
	for index, step := range steps {
		written, err := step.write()
		if err != nil {
			// #2276 confirmation-pass P1: `written` is this step's OWN true
			// count -- each write's own batch.Send() branch already reports
			// it correctly on an ambiguous network error (the F1 sweep), so
			// it must be added to `total` on the FAILURE path too, not only
			// after a confirmed success. Discarding it here would silently
			// re-introduce the exact bug this whole PR closes, one layer
			// up. Mirrors work_graph_edges_native_executor.go's idiom
			// (`written += writtenX` before the error check, every time).
			if total+written == 0 {
				// Nothing landed: no duplication is possible, so the caller's
				// ordinary fail-open to the Python bridge is still correct.
				return 0, fmt.Errorf("benchmarking: write %s: %w", step.table, err)
			}
			// Rows may already be on disk in append-only tables (either from
			// earlier steps, or from this step's own ambiguous Send()). The
			// caller must NOT fail open, and it needs the real count.
			total += written
			recordRowsWritten(total, orgID != "")
			return total, fmt.Errorf(
				"benchmarking: write %s (table %d of %d, %d row(s) potentially landed across %d earlier table(s) plus this one): %w: %v",
				step.table, index+1, len(steps), total, index,
				familyerr.ErrPartialWrite, err,
			)
		}
		total += written
	}

	recordRowsWritten(total, orgID != "")
	return total, nil
}

func (writer *Writer) writeBaselines(ctx context.Context, rows []BenchmarkBaselineRecord, orgID string) (int, error) {
	if len(rows) == 0 {
		return 0, nil
	}
	batch, err := writer.conn.PrepareBatch(ctx, `INSERT INTO testops_metric_baselines (
		metric_name, scope_type, scope_key, period_start, period_end,
		rolling_window_days, current_value, baseline_value, percentile_rank,
		p25_value, p50_value, p75_value, p90_value, sample_size, org_id, computed_at
	)`)
	if err != nil {
		return 0, fmt.Errorf("prepare testops_metric_baselines batch: %w", err)
	}
	for _, row := range rows {
		if err := batch.Append(
			row.MetricName, row.ScopeType, row.ScopeKey, row.PeriodStart, row.PeriodEnd,
			// rolling_window_days is a fixed configuration constant
			// (DefaultRollingWindowDays); sample_size is the length of one
			// cross-section slice built in memory for this baseline -- both
			// are bounded well below their column widths and never
			// negative, so they keep a direct conversion.
			uint16(row.RollingWindowDays), row.CurrentValue, row.BaselineValue, row.PercentileRank,
			row.P25Value, row.P50Value, row.P75Value, row.P90Value, uint32(row.SampleSize),
			orgID, row.ComputedAt,
		); err != nil {
			return 0, fmt.Errorf("append testops_metric_baselines row: %w", err)
		}
	}
	// CHAOS-5190 confirmation-pass sweep: Send is the one call here that
	// crosses the network, so a Send error is AMBIGUOUS -- ClickHouse may
	// have committed the insert server-side and only the acknowledgement
	// was lost. Report the true row count on this specific error path
	// (never on PrepareBatch/Append, which have not crossed the network
	// and genuinely wrote nothing), so the caller fails CLOSED on the
	// ambiguity instead of silently open (matches
	// work_graph_edges_native_clickhouse.go's established pattern).
	if err := batch.Send(); err != nil {
		return len(rows), fmt.Errorf("send testops_metric_baselines batch: %w", err)
	}
	return len(rows), nil
}

func (writer *Writer) writeMaturityBands(ctx context.Context, rows []MaturityBandRecord, orgID string) (int, error) {
	if len(rows) == 0 {
		return 0, nil
	}
	batch, err := writer.conn.PrepareBatch(ctx, `INSERT INTO testops_maturity_bands (
		metric_name, scope_type, scope_key, period_start, period_end,
		value, percentile_rank, maturity_band, confidence, org_id, computed_at
	)`)
	if err != nil {
		return 0, fmt.Errorf("prepare testops_maturity_bands batch: %w", err)
	}
	for _, row := range rows {
		if err := batch.Append(
			row.MetricName, row.ScopeType, row.ScopeKey, row.PeriodStart, row.PeriodEnd,
			row.Value, row.PercentileRank, row.MaturityBand, row.Confidence, orgID, row.ComputedAt,
		); err != nil {
			return 0, fmt.Errorf("append testops_maturity_bands row: %w", err)
		}
	}
	// CHAOS-5190 confirmation-pass sweep: Send is the one call here that
	// crosses the network, so a Send error is AMBIGUOUS -- ClickHouse may
	// have committed the insert server-side and only the acknowledgement
	// was lost. Report the true row count on this specific error path
	// (never on PrepareBatch/Append, which have not crossed the network
	// and genuinely wrote nothing), so the caller fails CLOSED on the
	// ambiguity instead of silently open (matches
	// work_graph_edges_native_clickhouse.go's established pattern).
	if err := batch.Send(); err != nil {
		return len(rows), fmt.Errorf("send testops_maturity_bands batch: %w", err)
	}
	return len(rows), nil
}

func (writer *Writer) writeAnomalies(ctx context.Context, rows []BenchmarkAnomalyRecord, orgID string) (int, error) {
	if len(rows) == 0 {
		return 0, nil
	}
	batch, err := writer.conn.PrepareBatch(ctx, `INSERT INTO testops_metric_anomalies (
		metric_name, scope_type, scope_key, day, value, baseline_value,
		z_score, anomaly_type, direction, severity, volatility_score, org_id, computed_at
	)`)
	if err != nil {
		return 0, fmt.Errorf("prepare testops_metric_anomalies batch: %w", err)
	}
	for _, row := range rows {
		if err := batch.Append(
			row.MetricName, row.ScopeType, row.ScopeKey, row.Day, row.Value, row.BaselineValue,
			row.ZScore, row.AnomalyType, row.Direction, row.Severity, row.VolatilityScore,
			orgID, row.ComputedAt,
		); err != nil {
			return 0, fmt.Errorf("append testops_metric_anomalies row: %w", err)
		}
	}
	// CHAOS-5190 confirmation-pass sweep: Send is the one call here that
	// crosses the network, so a Send error is AMBIGUOUS -- ClickHouse may
	// have committed the insert server-side and only the acknowledgement
	// was lost. Report the true row count on this specific error path
	// (never on PrepareBatch/Append, which have not crossed the network
	// and genuinely wrote nothing), so the caller fails CLOSED on the
	// ambiguity instead of silently open (matches
	// work_graph_edges_native_clickhouse.go's established pattern).
	if err := batch.Send(); err != nil {
		return len(rows), fmt.Errorf("send testops_metric_anomalies batch: %w", err)
	}
	return len(rows), nil
}

// writePeriodComparisons writes the table families.json OMITTED from this
// family's `writes` list (CHAOS-4288's surviving ledger defect) -- it is a real
// DDL table that write_benchmarking_outputs has always written.
func (writer *Writer) writePeriodComparisons(ctx context.Context, rows []PeriodComparisonRecord, orgID string) (int, error) {
	if len(rows) == 0 {
		return 0, nil
	}
	batch, err := writer.conn.PrepareBatch(ctx, `INSERT INTO testops_period_comparisons (
		metric_name, scope_type, scope_key, current_period_start, current_period_end,
		comparison_period_start, comparison_period_end, current_value, comparison_value,
		absolute_delta, percentage_change, trend_direction, org_id, computed_at
	)`)
	if err != nil {
		return 0, fmt.Errorf("prepare testops_period_comparisons batch: %w", err)
	}
	for _, row := range rows {
		if err := batch.Append(
			row.MetricName, row.ScopeType, row.ScopeKey,
			row.CurrentPeriodStart, row.CurrentPeriodEnd,
			row.ComparisonPeriodStart, row.ComparisonPeriodEnd,
			row.CurrentValue, row.ComparisonValue, row.AbsoluteDelta,
			row.PercentageChange, row.TrendDirection, orgID, row.ComputedAt,
		); err != nil {
			return 0, fmt.Errorf("append testops_period_comparisons row: %w", err)
		}
	}
	// CHAOS-5190 confirmation-pass sweep: Send is the one call here that
	// crosses the network, so a Send error is AMBIGUOUS -- ClickHouse may
	// have committed the insert server-side and only the acknowledgement
	// was lost. Report the true row count on this specific error path
	// (never on PrepareBatch/Append, which have not crossed the network
	// and genuinely wrote nothing), so the caller fails CLOSED on the
	// ambiguity instead of silently open (matches
	// work_graph_edges_native_clickhouse.go's established pattern).
	if err := batch.Send(); err != nil {
		return len(rows), fmt.Errorf("send testops_period_comparisons batch: %w", err)
	}
	return len(rows), nil
}

func (writer *Writer) writeCorrelations(ctx context.Context, rows []MetricCorrelationRecord, orgID string) (int, error) {
	if len(rows) == 0 {
		return 0, nil
	}
	batch, err := writer.conn.PrepareBatch(ctx, `INSERT INTO testops_metric_correlations (
		metric_name, paired_metric_name, scope_type, scope_key, period_start, period_end,
		coefficient, p_value, sample_size, is_significant, interpretation, org_id, computed_at
	)`)
	if err != nil {
		return 0, fmt.Errorf("prepare testops_metric_correlations batch: %w", err)
	}
	for _, row := range rows {
		significant := uint8(0)
		if row.IsSignificant {
			significant = 1
		}
		if err := batch.Append(
			row.MetricName, row.PairedMetricName, row.ScopeType, row.ScopeKey,
			row.PeriodStart, row.PeriodEnd, row.Coefficient, row.PValue,
			// sample_size is the length of an in-memory day-correlation
			// slice -- bounded well below UInt32 range and never negative,
			// so it keeps a direct conversion.
			uint32(row.SampleSize), significant, row.Interpretation, orgID, row.ComputedAt,
		); err != nil {
			return 0, fmt.Errorf("append testops_metric_correlations row: %w", err)
		}
	}
	// CHAOS-5190 confirmation-pass sweep: Send is the one call here that
	// crosses the network, so a Send error is AMBIGUOUS -- ClickHouse may
	// have committed the insert server-side and only the acknowledgement
	// was lost. Report the true row count on this specific error path
	// (never on PrepareBatch/Append, which have not crossed the network
	// and genuinely wrote nothing), so the caller fails CLOSED on the
	// ambiguity instead of silently open (matches
	// work_graph_edges_native_clickhouse.go's established pattern).
	if err := batch.Send(); err != nil {
		return len(rows), fmt.Errorf("send testops_metric_correlations batch: %w", err)
	}
	return len(rows), nil
}

func (writer *Writer) writeInsights(ctx context.Context, rows []BenchmarkInsightRecord, orgID string) (int, error) {
	if len(rows) == 0 {
		return 0, nil
	}
	batch, err := writer.conn.PrepareBatch(ctx, `INSERT INTO testops_benchmark_insights (
		insight_id, insight_type, scope_type, scope_key, metric_name, paired_metric_name,
		period_start, period_end, severity, summary, evidence_json, org_id, computed_at
	)`)
	if err != nil {
		return 0, fmt.Errorf("prepare testops_benchmark_insights batch: %w", err)
	}
	for _, row := range rows {
		if err := batch.Append(
			row.InsightID, row.InsightType, row.ScopeType, row.ScopeKey, row.MetricName,
			row.PairedMetricName, row.PeriodStart, row.PeriodEnd, row.Severity,
			row.Summary, row.EvidenceJSON, orgID, row.ComputedAt,
		); err != nil {
			return 0, fmt.Errorf("append testops_benchmark_insights row: %w", err)
		}
	}
	// CHAOS-5190 confirmation-pass sweep: Send is the one call here that
	// crosses the network, so a Send error is AMBIGUOUS -- ClickHouse may
	// have committed the insert server-side and only the acknowledgement
	// was lost. Report the true row count on this specific error path
	// (never on PrepareBatch/Append, which have not crossed the network
	// and genuinely wrote nothing), so the caller fails CLOSED on the
	// ambiguity instead of silently open (matches
	// work_graph_edges_native_clickhouse.go's established pattern).
	if err := batch.Send(); err != nil {
		return len(rows), fmt.Errorf("send testops_benchmark_insights batch: %w", err)
	}
	return len(rows), nil
}
