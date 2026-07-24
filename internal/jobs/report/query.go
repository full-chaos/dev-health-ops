package report

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type ReportDefinition struct {
	Plan   Plan
	Charts []ChartSpec
}

type ReportLoader interface {
	Load(context.Context, QueryInput) (ReportDefinition, error)
}

type PostgresReportLoader struct {
	pool *pgxpool.Pool
	now  func() time.Time
}

func NewPostgresReportLoader(pool *pgxpool.Pool) (*PostgresReportLoader, error) {
	if pool == nil {
		return nil, ErrDependencyUnavailable
	}
	return &PostgresReportLoader{pool: pool, now: time.Now}, nil
}

func (loader *PostgresReportLoader) Load(ctx context.Context, input QueryInput) (ReportDefinition, error) {
	if loader == nil || loader.pool == nil || loader.now == nil || input.ReportID == "" || input.RunID == "" {
		return ReportDefinition{}, ErrDependencyUnavailable
	}
	var planJSON, parametersJSON []byte
	var organizationID string
	err := loader.pool.QueryRow(ctx, `
SELECT report.report_plan, report.parameters, report.org_id
FROM public.saved_reports AS report
JOIN public.report_runs AS run ON run.report_id = report.id
WHERE report.id = $1::uuid AND run.id = $2::uuid
  AND report.is_active = TRUE AND run.status = 'running'`,
		input.ReportID, input.RunID).Scan(&planJSON, &parametersJSON, &organizationID)
	if errors.Is(err, pgx.ErrNoRows) {
		return ReportDefinition{}, ErrContractMismatch
	}
	if err != nil {
		return ReportDefinition{}, fmt.Errorf("load saved report: %w", ErrDependencyUnavailable)
	}
	return decodeReportDefinition(input.ReportID, organizationID, planJSON, parametersJSON, loader.now().UTC())
}

type wirePlan struct {
	PlanID              string          `json:"plan_id"`
	ReportType          string          `json:"report_type"`
	Audience            string          `json:"audience"`
	ScopeTeams          []string        `json:"scope_teams"`
	ScopeRepos          []string        `json:"scope_repos"`
	ScopeServices       []string        `json:"scope_services"`
	TimeRangeStart      string          `json:"time_range_start"`
	TimeRangeEnd        string          `json:"time_range_end"`
	ComparisonPeriod    string          `json:"comparison_period"`
	Sections            []string        `json:"sections"`
	RequestedMetrics    []string        `json:"requested_metrics"`
	ConfidenceThreshold string          `json:"confidence_threshold"`
	CreatedAt           string          `json:"created_at"`
	OrganizationID      string          `json:"org_id"`
	ChartSpecs          []wireChartSpec `json:"chart_specs"`
}

type wireChartSpec struct {
	ChartID        string   `json:"chart_id"`
	PlanID         string   `json:"plan_id"`
	ChartType      string   `json:"chart_type"`
	Metric         string   `json:"metric"`
	GroupBy        *string  `json:"group_by"`
	FilterTeams    []string `json:"filter_teams"`
	FilterRepos    []string `json:"filter_repos"`
	TimeRangeStart string   `json:"time_range_start"`
	TimeRangeEnd   string   `json:"time_range_end"`
	Title          *string  `json:"title"`
	OrganizationID string   `json:"org_id"`
}

func decodeReportDefinition(
	reportID, organizationID string,
	planJSON, parametersJSON []byte,
	now time.Time,
) (ReportDefinition, error) {
	var plan wirePlan
	if len(planJSON) > 0 && string(planJSON) != "null" && string(planJSON) != "{}" {
		if err := json.Unmarshal(planJSON, &plan); err != nil {
			return ReportDefinition{}, fmt.Errorf("decode report plan: %w", ErrContractMismatch)
		}
	} else {
		var parameters map[string]any
		if len(parametersJSON) > 0 {
			if err := json.Unmarshal(parametersJSON, &parameters); err != nil {
				return ReportDefinition{}, fmt.Errorf("decode report parameters: %w", ErrContractMismatch)
			}
		}
		plan = defaultWirePlan(reportID, organizationID, parameters, now)
	}
	if plan.PlanID == "" || plan.ReportType == "" {
		return ReportDefinition{}, ErrContractMismatch
	}
	if plan.OrganizationID == "" {
		plan.OrganizationID = organizationID
	}
	if plan.OrganizationID != organizationID {
		return ReportDefinition{}, ErrContractMismatch
	}
	createdAt := now
	if plan.CreatedAt != "" {
		parsed, err := time.Parse(time.RFC3339Nano, plan.CreatedAt)
		if err != nil {
			return ReportDefinition{}, ErrContractMismatch
		}
		createdAt = parsed.UTC()
	}
	result := ReportDefinition{Plan: Plan{
		PlanID: plan.PlanID, ReportType: plan.ReportType, Audience: plan.Audience,
		ScopeTeams: append([]string(nil), plan.ScopeTeams...), ScopeRepos: append([]string(nil), plan.ScopeRepos...),
		ScopeServices: append([]string(nil), plan.ScopeServices...), TimeRangeStart: plan.TimeRangeStart,
		TimeRangeEnd: plan.TimeRangeEnd, ComparisonPeriod: plan.ComparisonPeriod,
		Sections: append([]string(nil), plan.Sections...), RequestedMetrics: append([]string(nil), plan.RequestedMetrics...),
		ConfidenceThreshold: plan.ConfidenceThreshold, CreatedAt: createdAt, OrganizationID: plan.OrganizationID,
	}}
	if result.Plan.ConfidenceThreshold == "" {
		result.Plan.ConfidenceThreshold = "direct_fact"
	}
	for _, chart := range plan.ChartSpecs {
		groupBy, title := "", ""
		if chart.GroupBy != nil {
			groupBy = *chart.GroupBy
		}
		if chart.Title != nil {
			title = *chart.Title
		}
		if chart.PlanID != plan.PlanID || chart.ChartID == "" || chart.Metric == "" {
			return ReportDefinition{}, ErrContractMismatch
		}
		if chart.OrganizationID == "" {
			chart.OrganizationID = organizationID
		}
		if chart.OrganizationID != organizationID {
			return ReportDefinition{}, ErrContractMismatch
		}
		result.Charts = append(result.Charts, ChartSpec{
			ChartID: chart.ChartID, PlanID: chart.PlanID, ChartType: chart.ChartType, Metric: chart.Metric,
			GroupBy: groupBy, FilterTeams: append([]string(nil), chart.FilterTeams...),
			FilterRepos: append([]string(nil), chart.FilterRepos...), TimeRangeStart: chart.TimeRangeStart,
			TimeRangeEnd: chart.TimeRangeEnd, Title: title, OrganizationID: chart.OrganizationID,
		})
	}
	return result, nil
}

func defaultWirePlan(reportID, organizationID string, parameters map[string]any, now time.Time) wirePlan {
	days := 7
	switch parameters["dateRange"] {
	case "last_24_hours":
		days = 1
	case "last_30_days":
		days = 30
	case "last_90_days":
		days = 90
	}
	end := now.Format(time.DateOnly)
	start := now.AddDate(0, 0, -days).Format(time.DateOnly)
	reportType, comparison := "weekly_health", "prior_week"
	if days > 7 {
		reportType, comparison = "monthly_review", "prior_month"
	}
	plan := wirePlan{
		PlanID: "auto-" + reportID, ReportType: reportType, Audience: "team_lead",
		TimeRangeStart: start, TimeRangeEnd: end, ComparisonPeriod: comparison,
		Sections:            []string{"summary", "delivery", "quality", "wellbeing"},
		ConfidenceThreshold: "direct_fact", OrganizationID: organizationID,
	}
	plan.RequestedMetrics = stringSlice(parameters["metrics"])
	switch parameters["scope"] {
	case "team":
		plan.ScopeTeams = stringSlice(parameters["team_ids"])
	case "repo":
		plan.ScopeRepos = stringSlice(parameters["repo_ids"])
	}
	return plan
}

func stringSlice(value any) []string {
	values, ok := value.([]any)
	if !ok {
		return nil
	}
	result := make([]string, 0, len(values))
	for _, item := range values {
		if text, ok := item.(string); ok {
			result = append(result, text)
		}
	}
	return result
}

type ClickHouseQueryAdapter struct {
	loader ReportLoader
	conn   driver.Conn
}

func NewClickHouseQueryAdapter(loader ReportLoader, conn driver.Conn) (*ClickHouseQueryAdapter, error) {
	if loader == nil || conn == nil {
		return nil, ErrDependencyUnavailable
	}
	return &ClickHouseQueryAdapter{loader: loader, conn: conn}, nil
}

func (adapter *ClickHouseQueryAdapter) Query(ctx context.Context, input QueryInput) (QueryResult, error) {
	if adapter == nil || adapter.loader == nil || adapter.conn == nil {
		return QueryResult{}, ErrDependencyUnavailable
	}
	definition, err := adapter.loader.Load(ctx, input)
	if err != nil {
		return QueryResult{}, err
	}
	result := QueryResult{Plan: definition.Plan, Metadata: map[string]string{"renderer_version": "reports.v1"}}
	for _, spec := range definition.Charts {
		chart, err := adapter.executeChart(ctx, spec)
		if err != nil {
			return QueryResult{}, err
		}
		result.Charts = append(result.Charts, chart)
	}
	return result, nil
}

type metricDefinition struct {
	CanonicalName string   `json:"canonical_name"`
	DisplayName   string   `json:"display_name"`
	Unit          string   `json:"unit"`
	Dimensions    []string `json:"dimensions"`
	SourceTable   string   `json:"source_table"`
}

type metricRegistryArtifact struct {
	SchemaVersion int                `json:"schema_version"`
	Metrics       []metricDefinition `json:"metrics"`
}

//go:embed metric_registry.json
var metricRegistryJSON []byte

var supportedMetrics = mustLoadMetricRegistry()

func mustLoadMetricRegistry() map[string]metricDefinition {
	var artifact metricRegistryArtifact
	if err := json.Unmarshal(metricRegistryJSON, &artifact); err != nil || artifact.SchemaVersion != 1 {
		panic("invalid embedded report metric registry")
	}
	result := make(map[string]metricDefinition, len(artifact.Metrics))
	for _, definition := range artifact.Metrics {
		if definition.CanonicalName == "" || definition.DisplayName == "" ||
			definition.Unit == "" || !identifier.MatchString(definition.CanonicalName) ||
			!identifier.MatchString(definition.SourceTable) {
			panic("invalid embedded report metric definition")
		}
		if _, exists := result[definition.CanonicalName]; exists {
			panic("duplicate embedded report metric definition")
		}
		result[definition.CanonicalName] = definition
	}
	return result
}

var identifier = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)

func (adapter *ClickHouseQueryAdapter) executeChart(ctx context.Context, spec ChartSpec) (ChartResult, error) {
	definition, ok := supportedMetrics[spec.Metric]
	if !ok || !identifier.MatchString(spec.Metric) || !identifier.MatchString(definition.SourceTable) {
		return ChartResult{}, fmt.Errorf("unsupported chart metric %q", spec.Metric)
	}
	statement, parameters, err := buildChartQuery(spec, definition)
	if err != nil {
		return ChartResult{}, err
	}
	rows, err := adapter.conn.Query(ctx, statement, parameters...)
	if err != nil {
		return ChartResult{}, fmt.Errorf("query report chart: %w", ErrDependencyUnavailable)
	}
	defer rows.Close()
	result := ChartResult{
		Spec: spec, Title: spec.Title, SourceTable: definition.SourceTable, Unit: definition.Unit,
	}
	if result.Title == "" {
		result.Title = definition.DisplayName
	}
	for rows.Next() {
		var group *string
		var y *float64
		var x string
		temporal := spec.GroupBy == "day" || spec.GroupBy == "week" || spec.GroupBy == "month" ||
			(spec.GroupBy == "" && (spec.ChartType == "line" || spec.ChartType == "heatmap"))
		if temporal {
			var instant time.Time
			if err := rows.Scan(&instant, &group, &y); err != nil {
				return ChartResult{}, fmt.Errorf("scan report chart: %w", ErrDependencyUnavailable)
			}
			x = instant.Format(time.DateOnly)
		} else if err := rows.Scan(&x, &group, &y); err != nil {
			return ChartResult{}, fmt.Errorf("scan report chart: %w", ErrDependencyUnavailable)
		}
		if y == nil {
			continue
		}
		point := DataPoint{X: x, Y: *y}
		if group != nil {
			point.Group = *group
		}
		result.DataPoints = append(result.DataPoints, point)
	}
	if err := rows.Err(); err != nil {
		return ChartResult{}, fmt.Errorf("iterate report chart: %w", ErrDependencyUnavailable)
	}
	return result, nil
}

func buildChartQuery(spec ChartSpec, definition metricDefinition) (string, []any, error) {
	xExpression, xType, temporal := "'total'", "String", false
	switch spec.GroupBy {
	case "day":
		xExpression, xType, temporal = "toDate(day)", "Date", true
	case "week":
		xExpression, xType, temporal = "toStartOfWeek(day)", "Date", true
	case "month":
		xExpression, xType, temporal = "toStartOfMonth(day)", "Date", true
	case "team", "repo", "service":
		if definition.hasDimension(spec.GroupBy) {
			xExpression = spec.GroupBy + "_id"
		} else {
			xExpression = "'unscoped'"
		}
	}
	if spec.GroupBy == "" && (spec.ChartType == "line" || spec.ChartType == "heatmap") &&
		definition.hasDimension("day") {
		xExpression, xType, temporal = "toDate(day)", "Date", true
	}
	aggregate := "avg"
	if strings.HasSuffix(spec.Metric, "_count") || definition.Unit == "count" {
		aggregate = "sum"
	}
	clauses := []string{spec.Metric + " IS NOT NULL"}
	parameters := make([]any, 0, 5)
	if spec.OrganizationID != "" {
		clauses = append(clauses, "org_id = {org_id:String}")
		parameters = append(parameters, clickhouse.Named("org_id", spec.OrganizationID))
	}
	if spec.TimeRangeStart != "" {
		if _, err := time.Parse(time.DateOnly, spec.TimeRangeStart); err != nil {
			return "", nil, ErrContractMismatch
		}
		clauses = append(clauses, "day >= {time_range_start:Date}")
		parameters = append(parameters, clickhouse.Named("time_range_start", spec.TimeRangeStart))
	}
	if spec.TimeRangeEnd != "" {
		if _, err := time.Parse(time.DateOnly, spec.TimeRangeEnd); err != nil {
			return "", nil, ErrContractMismatch
		}
		clauses = append(clauses, "day <= {time_range_end:Date}")
		parameters = append(parameters, clickhouse.Named("time_range_end", spec.TimeRangeEnd))
	}
	if len(spec.FilterTeams) > 0 && definition.hasDimension("team") {
		clauses = append(clauses, "team_id IN {filter_teams:Array(String)}")
		parameters = append(parameters, clickhouse.Named("filter_teams", spec.FilterTeams))
	}
	if len(spec.FilterRepos) > 0 && definition.hasDimension("repo") {
		clauses = append(clauses, "repo_id IN {filter_repos:Array(String)}")
		parameters = append(parameters, clickhouse.Named("filter_repos", spec.FilterRepos))
	}
	where := strings.Join(clauses, " AND\n        ")
	if spec.GroupBy == "" && (spec.ChartType == "scorecard" || spec.ChartType == "trend_delta" || spec.ChartType == "table") {
		return fmt.Sprintf(`SELECT
        CAST('total', '%s') AS x,
        CAST(NULL, 'Nullable(String)') AS group_value,
        %s(%s) AS y
    FROM %s
    WHERE
        %s`, xType, aggregate, spec.Metric, definition.SourceTable, where), parameters, nil
	}
	order := "y DESC, x"
	if temporal {
		order = "x"
	}
	return fmt.Sprintf(`SELECT
        %s AS x,
        CAST(NULL, 'Nullable(String)') AS group_value,
        %s(%s) AS y
    FROM %s
    WHERE
        %s
    GROUP BY x
    ORDER BY %s`, xExpression, aggregate, spec.Metric, definition.SourceTable, where, order), parameters, nil
}

func (definition metricDefinition) hasDimension(target string) bool {
	for _, dimension := range definition.Dimensions {
		if dimension == target {
			return true
		}
	}
	return false
}
