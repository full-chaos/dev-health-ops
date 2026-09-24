package analytics

import (
	"context"
	"fmt"
	"log/slog"
	"regexp"
	"sort"
	"strings"

	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

// Catalog values query bounds. A repository catalog larger than
// repositoryScopeLimit is rejected rather than truncated, so a scope reader
// never sees a partial repository list.
const (
	repositoryScopeLimit = 100
	defaultCatalogLimit  = 100
)

// repositoryPart matches one half of an "owner/name" repository slug.
var repositoryPart = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9._-]{0,98}[a-z0-9])?$`)

var catalogDimensionOrder = []Dimension{
	DimensionTeam,
	DimensionRepo,
	DimensionAuthor,
	DimensionWorkType,
	DimensionTheme,
	DimensionSubcategory,
}

var catalogDimensionDescriptions = map[Dimension]string{
	DimensionTeam:        "Team identifier for grouping work",
	DimensionRepo:        "Repository identifier",
	DimensionAuthor:      "Author/contributor identifier",
	DimensionWorkType:    "Type of work item (issue, PR, etc.)",
	DimensionTheme:       "Investment theme category",
	DimensionSubcategory: "Investment subcategory",
}

type catalogMeasureEntry struct {
	measure     Measure
	description string
}

var catalogMeasures = []catalogMeasureEntry{
	{MeasureCount, "Count of work units"},
	{MeasureChurnLOC, "Lines of code changed"},
	{MeasurePRReworkRatio, "Share of PRs that required multiple review rounds"},
	{MeasureCycleTimeHours, "Average cycle time in hours"},
	{MeasureThroughput, "Distinct work units completed"},
	{MeasurePipelineSuccessRate, "CI/CD pipeline success rate"},
	{MeasurePipelineFailureRate, "CI/CD pipeline failure rate"},
	{MeasurePipelineDurationP95, "P95 pipeline duration in seconds"},
	{MeasurePipelineQueueTime, "Average pipeline queue time in seconds"},
	{MeasurePipelineRerunRate, "Pipeline rerun/retry rate"},
	{MeasureTestPassRate, "Test pass rate"},
	{MeasureTestFailureRate, "Test failure rate"},
	{MeasureTestFlakeRate, "Test flake rate (pass/fail flip)"},
	{MeasureTestSuiteDurationP95, "P95 test suite duration in seconds"},
	{MeasureCoverageLinePct, "Line coverage percentage"},
	{MeasureCoverageBranchPct, "Branch coverage percentage"},
	{MeasureCoverageDeltaPct, "Coverage change from prior period"},
	{MeasureFlagFrictionDelta, "Feature flag friction delta (error-rate change gated by flag)"},
	{MeasureFlagErrorRateDelta, "Error-rate change attributable to feature flag rollout"},
	{MeasureFlagCoverageRatio, "Share of flagged code paths exercised by tests"},
	{MeasureFlagActivationRate, "Rate of feature flag activations over time"},
}

// catalogDimension maps the wire enum to the internal dimension.
func catalogDimension(in model.DimensionInput) (Dimension, error) {
	switch in {
	case model.DimensionInputTeam:
		return DimensionTeam, nil
	case model.DimensionInputRepo:
		return DimensionRepo, nil
	case model.DimensionInputAuthor:
		return DimensionAuthor, nil
	case model.DimensionInputWorkType:
		return DimensionWorkType, nil
	case model.DimensionInputTheme:
		return DimensionTheme, nil
	case model.DimensionInputSubcategory:
		return DimensionSubcategory, nil
	}
	return "", newValidationError("dimension", string(in), "unknown dimension %q", string(in))
}

// catalogValueRow is one raw (value, count) row of a catalog values query.
type catalogValueRow struct {
	Value string
	Count uint64
}

// CompileCatalogValues ports compile_catalog_values (compiler.py:553-620).
// The repository dimension reads the repos table and rejects every active
// filter; the team dimension lists the teams table (filters do not apply);
// every other dimension counts distinct values from its event source and
// applies the filter translation the analytics operations share.
func CompileCatalogValues(dimension Dimension, limit int, orgID string, timeoutSeconds int, filters *model.FilterInput) (compiledQuery, error) {
	limitBinding := clickhouse.Binding{Name: "limit", Value: uint32(limit)}
	orgBinding := clickhouse.Binding{Name: "org_id", Value: orgID}

	useInvestment := dimension == DimensionTheme || dimension == DimensionSubcategory || dimension == DimensionWorkType

	if dimension == DimensionRepo {
		if hasActiveFilters(filters) {
			return compiledQuery{}, newValidationError("filters", "repo", "repository catalog filters are not supported")
		}
		sql := fmt.Sprintf(`
SELECT
    canonical_repo AS value,
    count() AS count
FROM (
    SELECT lowerUTF8(trimBoth(repo)) AS canonical_repo
    FROM repos FINAL
    WHERE org_id = {org_id:String}
)
WHERE match(
    canonical_repo,
    '^[a-z0-9]([a-z0-9._-]{0,98}[a-z0-9])?/[a-z0-9]([a-z0-9._-]{0,98}[a-z0-9])?$'
)
GROUP BY canonical_repo
ORDER BY value
LIMIT {limit:UInt32}
%s
`, settingsMaxExecutionTime(timeoutSeconds))
		return compiledQuery{sql: sql, bindings: []clickhouse.Binding{limitBinding, orgBinding}}, nil
	}

	if dimension == DimensionTeam {
		sql := fmt.Sprintf(`
SELECT
    t.id AS value,
    COALESCE(activity.count, 0) AS count
FROM (
    SELECT id, name
    FROM teams FINAL
    WHERE org_id = {org_id:String}
      AND is_active = 1
      AND id != ''
) AS t
LEFT JOIN (
    SELECT
        toString(team_id) AS team_id,
        COUNT(*) AS count
    FROM %s
    WHERE team_id IS NOT NULL
      AND org_id = {org_id:String}
      AND toString(team_id) != ''
    GROUP BY team_id
) AS activity ON activity.team_id = t.id
ORDER BY count DESC, t.name ASC, t.id ASC
LIMIT {limit:UInt32}
%s
`, investmentMetricsDailyDedupSource, settingsMaxExecutionTime(timeoutSeconds))
		return compiledQuery{sql: sql, bindings: []clickhouse.Binding{limitBinding, orgBinding}}, nil
	}

	fc, err := translateFilters(filters, useInvestment, defaultFilterColumns())
	if err != nil {
		return compiledQuery{}, err
	}
	dimCol, err := dbColumn(dimension, useInvestment)
	if err != nil {
		return compiledQuery{}, err
	}

	var source, alias, extraClauses string
	if useInvestment {
		ictx := investmentContextFor([]Dimension{dimension}, needsTeamJoin(filters), needsAuthorJoin(filters))
		source, alias, extraClauses = ictx.Source, ictx.Alias, ictx.ExtraClauses
	} else {
		source, alias = investmentMetricsDailyDedupSource, "investment_metrics_daily"
	}

	sql := fmt.Sprintf(`
SELECT
    toString(%[1]s) AS value,
    COUNT(*) AS count
FROM %[2]s
%[3]s
WHERE %[1]s IS NOT NULL
  AND %[4]s.org_id = {org_id:String}
  AND toString(%[1]s) != ''
%[5]s
GROUP BY value
ORDER BY count DESC, value ASC
LIMIT {limit:UInt32}
%[6]s
`, dimCol, source, extraClauses, alias, fc.sql, settingsMaxExecutionTime(timeoutSeconds))

	bindings := []clickhouse.Binding{limitBinding, orgBinding}
	bindings = append(bindings, fc.bindings...)
	return compiledQuery{sql: sql, bindings: bindings}, nil
}

// canonicalRepositoryValues ports _canonical_repository_values: trims and
// lowercases each slug, keeps only well-formed "owner/name" values, merges
// duplicates by summing counts, rejects a catalog above the scope limit and
// returns the slugs sorted.
func canonicalRepositoryValues(rows []catalogValueRow) ([]model.CatalogValueItem, error) {
	counts := map[string]int{}
	for _, row := range rows {
		parts := strings.Split(strings.ToLower(strings.TrimSpace(row.Value)), "/")
		if len(parts) != 2 || !repositoryPart.MatchString(parts[0]) || !repositoryPart.MatchString(parts[1]) {
			continue
		}
		slug := parts[0] + "/" + parts[1]
		counts[slug] = counts[slug] + int(row.Count)
	}
	if len(counts) > repositoryScopeLimit {
		return nil, newValidationError("dimension", "repo", "repository catalog exceeds supported scope limit")
	}
	slugs := make([]string, 0, len(counts))
	if len(counts) == 0 {
		return []model.CatalogValueItem{}, nil
	}
	for slug := range counts {
		slugs = append(slugs, slug)
	}
	sort.Strings(slugs)
	items := make([]model.CatalogValueItem, 0, len(slugs))
	for _, slug := range slugs {
		items = append(items, model.CatalogValueItem{Value: slug, Count: counts[slug]})
	}
	return items, nil
}

func executeCatalogValues(ctx context.Context, client QueryClient, q compiledQuery) ([]catalogValueRow, error) {
	rows, err := client.Query(ctx, q.sql, q.bindings)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	defer rows.Close()

	var out []catalogValueRow
	for rows.Next() {
		var row catalogValueRow
		if scanErr := rows.Scan(&row.Value, &row.Count); scanErr != nil {
			return nil, fmt.Errorf("scan: %w", scanErr)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows: %w", err)
	}
	return out, nil
}

// ResolveCatalog ports resolve_catalog (catalog.py): the static dimension,
// measure and limit lists, plus distinct values for one dimension when one
// is requested. A failed values query answers an empty (non-nil) values
// list, logged at WARN, and is reported through the second return value so
// callers can mark the call degraded; a validation rejection stays an error.
func ResolveCatalog(ctx context.Context, client QueryClient, orgID string, dimension *model.DimensionInput, filters *model.FilterInput) (*model.CatalogResult, bool, error) {
	dimensions := make([]model.CatalogDimension, 0, len(catalogDimensionOrder))
	for _, d := range catalogDimensionOrder {
		dimensions = append(dimensions, model.CatalogDimension{Name: string(d), Description: catalogDimensionDescriptions[d]})
	}
	measures := make([]model.CatalogMeasure, 0, len(catalogMeasures))
	for _, m := range catalogMeasures {
		measures = append(measures, model.CatalogMeasure{Name: string(m.measure), Description: m.description})
	}
	result := &model.CatalogResult{
		Dimensions: dimensions,
		Measures:   measures,
		Limits: &model.CatalogLimits{
			MaxDays:        maxDays,
			MaxBuckets:     maxBuckets,
			MaxTopN:        maxTopN,
			MaxSankeyNodes: maxSankeyNodes,
			MaxSankeyEdges: maxSankeyEdges,
			MaxSubRequests: maxSubRequests,
		},
	}
	if dimension == nil {
		return result, false, nil
	}

	dim, err := catalogDimension(*dimension)
	if err != nil {
		return nil, false, err
	}
	limit := defaultCatalogLimit
	if dim == DimensionRepo {
		limit = repositoryScopeLimit + 1
	}
	q, err := CompileCatalogValues(dim, limit, orgID, queryTimeoutSecs, filters)
	if err != nil {
		return nil, false, err
	}

	rows, err := executeCatalogValues(ctx, client, q)
	if err != nil {
		slog.WarnContext(ctx, "query-api: catalog values query failed, answering empty values", "operation", "catalog", "dimension", string(dim), "error", err)
		result.Values = []model.CatalogValueItem{}
		return result, true, nil
	}
	if dim == DimensionRepo {
		values, err := canonicalRepositoryValues(rows)
		if err != nil {
			return nil, false, err
		}
		result.Values = values
		return result, false, nil
	}
	values := make([]model.CatalogValueItem, 0, len(rows))
	for _, row := range rows {
		values = append(values, model.CatalogValueItem{Value: row.Value, Count: int(row.Count)})
	}
	result.Values = values
	return result, false, nil
}
