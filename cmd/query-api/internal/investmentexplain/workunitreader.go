package investmentexplain

import (
	"context"
	"fmt"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/analytics"
)

// lookupChunkSize ports work_unit_investments.py's _LOOKUP_CHUNK_SIZE.
const lookupChunkSize = 250

func chunkStrings(values []string, size int) [][]string {
	var chunks [][]string
	for start := 0; start < len(values); start += size {
		end := start + size
		if end > len(values) {
			end = len(values)
		}
		chunks = append(chunks, values[start:end])
	}
	return chunks
}

// uniqueNonEmpty ports work_unit_investments.py's _unique_non_empty.
func uniqueNonEmpty(values []string) []string {
	out := make([]string, 0, len(values))
	for _, v := range values {
		if v != "" {
			out = append(out, v)
		}
	}
	return dedupeStrings(out)
}

// WorkUnitInvestmentRow is one fetch_work_unit_investments result row
// (work_unit_investments.py:26-79).
//
// LEAST SURE, pending live-schema verification once bigboy's testcontainer
// pause lifts: which of these columns are Nullable in the underlying
// work_unit_investments table (CHAOS-4547 notes four Nullable columns on
// this exact CTE) determines whether the pointer fields below are the
// right Scan destinations -- chosen defensively (Nullable(Float64)/
// Nullable(String) scan cleanly into *float64/*string with clickhouse-go
// v2) but not yet proven against a real server.
type WorkUnitInvestmentRow struct {
	WorkUnitID                  string
	WorkUnitType                *string
	WorkUnitName                *string
	FromTS                      time.Time
	ToTS                        time.Time
	RepoID                      *string
	Provider                    *string
	EffortMetric                *string
	EffortValue                 *float64
	ThemeDistributionJSON       *string
	SubcategoryDistributionJSON *string
	StructuralEvidenceJSON      *string
	EvidenceQuality             *float64
	EvidenceQualityBand         *string
	CategorizationStatus        *string
	CategorizationModelVersion  *string
	CategorizationRunID         *string
	ComputedAt                  time.Time
}

// WorkUnitInvestmentsFilter is fetch_work_unit_investments' filter
// parameter set (work_unit_investments.py:26-35).
type WorkUnitInvestmentsFilter struct {
	OrgID      string
	StartTS    time.Time
	EndTS      time.Time
	RepoIDs    []string
	Limit      int
	WorkUnitID string
}

// FetchWorkUnitInvestments ports fetch_work_unit_investments
// (work_unit_investments.py:26-79). Matches FetchInvestmentBreakdown's
// FROM %s AS work_unit_investments convention (reader.go) rather than
// Python's WITH-clause form -- same reasoning, same source.
func (reader *Reader) FetchWorkUnitInvestments(ctx context.Context, filter WorkUnitInvestmentsFilter) ([]WorkUnitInvestmentRow, error) {
	if reader == nil || reader.client == nil {
		return nil, ErrUnavailable
	}

	var scopeSQL string
	var scopeBindings []dhclickhouse.Binding
	if len(filter.RepoIDs) > 0 {
		scopeSQL = " AND work_unit_investments.repo_id IN {repo_ids:Array(String)}"
		scopeBindings = append(scopeBindings, dhclickhouse.Binding{Name: "repo_ids", Value: dedupeStrings(filter.RepoIDs)})
	}
	var workUnitSQL string
	if filter.WorkUnitID != "" {
		workUnitSQL = " AND work_unit_investments.work_unit_id = {work_unit_id:String}"
		scopeBindings = append(scopeBindings, dhclickhouse.Binding{Name: "work_unit_id", Value: filter.WorkUnitID})
	}

	limit := filter.Limit
	if limit < 1 {
		limit = 1
	}

	bindings := []dhclickhouse.Binding{
		{Name: "start_date", Value: dateBindingValue(filter.StartTS)},
		{Name: "end_date", Value: dateBindingValue(filter.EndTS)},
		{Name: "org_id", Value: filter.OrgID},
		{Name: "limit", Value: uint64(limit)},
	}
	bindings = append(bindings, scopeBindings...)

	query := fmt.Sprintf(`
SELECT
    work_unit_id,
    work_unit_type,
    work_unit_name,
    from_ts,
    to_ts,
    repo_id,
    provider,
    effort_metric,
    effort_value,
    theme_distribution_json,
    subcategory_distribution_json,
    structural_evidence_json,
    evidence_quality,
    evidence_quality_band,
    categorization_status,
    categorization_model_version,
    categorization_run_id,
    latest_computed_at AS computed_at
FROM %s AS work_unit_investments
WHERE work_unit_investments.from_ts < {end_date:Date}
  AND work_unit_investments.to_ts >= {start_date:Date}
  AND work_unit_investments.org_id = {org_id:String}
%s
%s
ORDER BY effort_value DESC, work_unit_id ASC
LIMIT {limit:UInt64}
%s
`, analytics.LatestWorkUnitInvestmentsSource(), scopeSQL, workUnitSQL, settingsMaxExecutionTime())

	rows, err := reader.client.Query(ctx, query, bindings)
	if err != nil {
		return nil, fmt.Errorf("query work unit investments: %w", err)
	}
	defer rows.Close()

	results := make([]WorkUnitInvestmentRow, 0)
	for rows.Next() {
		var row WorkUnitInvestmentRow
		if err := rows.Scan(
			&row.WorkUnitID, &row.WorkUnitType, &row.WorkUnitName,
			&row.FromTS, &row.ToTS, &row.RepoID, &row.Provider,
			&row.EffortMetric, &row.EffortValue,
			&row.ThemeDistributionJSON, &row.SubcategoryDistributionJSON, &row.StructuralEvidenceJSON,
			&row.EvidenceQuality, &row.EvidenceQualityBand,
			&row.CategorizationStatus, &row.CategorizationModelVersion, &row.CategorizationRunID,
			&row.ComputedAt,
		); err != nil {
			return nil, fmt.Errorf("scan work unit investment row: %w", err)
		}
		results = append(results, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate work unit investment rows: %w", err)
	}
	return results, nil
}

// FetchRepoScopes ports fetch_repo_scopes (work_unit_investments.py:82-108).
func (reader *Reader) FetchRepoScopes(ctx context.Context, orgID string, repoIDs []string) (map[string]string, error) {
	if reader == nil || reader.client == nil {
		return nil, ErrUnavailable
	}
	ids := uniqueNonEmpty(repoIDs)
	if len(ids) == 0 {
		return map[string]string{}, nil
	}

	query := fmt.Sprintf(`
SELECT
    toString(id) AS repo_id,
    repo
FROM repos
WHERE id IN {repo_ids:Array(String)}
  AND org_id = {org_id:String}
%s
`, settingsMaxExecutionTime())

	result := map[string]string{}
	for _, chunk := range chunkStrings(ids, lookupChunkSize) {
		bindings := []dhclickhouse.Binding{
			{Name: "repo_ids", Value: chunk},
			{Name: "org_id", Value: orgID},
		}
		rows, err := reader.client.Query(ctx, query, bindings)
		if err != nil {
			return nil, fmt.Errorf("query repo scopes: %w", err)
		}
		for rows.Next() {
			var repoID, repo string
			if err := rows.Scan(&repoID, &repo); err != nil {
				rows.Close()
				return nil, fmt.Errorf("scan repo scope row: %w", err)
			}
			if repoID != "" {
				result[repoID] = repo
			}
		}
		rowsErr := rows.Err()
		rows.Close()
		if rowsErr != nil {
			return nil, fmt.Errorf("iterate repo scope rows: %w", rowsErr)
		}
	}
	return result, nil
}

// FetchRepoIdentities ports fetch_repo_identities
// (work_unit_investments.py:111-172).
func (reader *Reader) FetchRepoIdentities(ctx context.Context, orgID string, repoIDs []string) (map[string]repoIdentity, error) {
	if reader == nil || reader.client == nil {
		return nil, ErrUnavailable
	}
	ids := uniqueNonEmpty(repoIDs)
	if len(ids) == 0 {
		return map[string]repoIdentity{}, nil
	}

	query := fmt.Sprintf(`
SELECT
    toString(id) AS repo_id,
    argMax(repo, last_synced) AS repo,
    if(uniqExact(provider) = 1, argMax(provider, last_synced), '') AS provider
FROM repos
WHERE id IN {repo_ids:Array(String)}
  AND org_id = {org_id:String}
GROUP BY org_id, id
%s
`, settingsMaxExecutionTime())

	result := map[string]repoIdentity{}
	for _, chunk := range chunkStrings(ids, lookupChunkSize) {
		bindings := []dhclickhouse.Binding{
			{Name: "repo_ids", Value: chunk},
			{Name: "org_id", Value: orgID},
		}
		rows, err := reader.client.Query(ctx, query, bindings)
		if err != nil {
			return nil, fmt.Errorf("query repo identities: %w", err)
		}
		for rows.Next() {
			var repoID, repo, provider string
			if err := rows.Scan(&repoID, &repo, &provider); err != nil {
				rows.Close()
				return nil, fmt.Errorf("scan repo identity row: %w", err)
			}
			if repoID != "" && repo != "" && provider != "" {
				result[repoID] = repoIdentity{Slug: repo, Provider: provider}
			}
		}
		rowsErr := rows.Err()
		rows.Close()
		if rowsErr != nil {
			return nil, fmt.Errorf("iterate repo identity rows: %w", rowsErr)
		}
	}
	return result, nil
}

// FetchWorkItemTeamAssignments ports fetch_work_item_team_assignments
// (work_unit_investments.py:175-214).
func (reader *Reader) FetchWorkItemTeamAssignments(ctx context.Context, orgID string, workItemIDs []string) (map[string]teamAssignment, error) {
	if reader == nil || reader.client == nil {
		return nil, ErrUnavailable
	}
	ids := uniqueNonEmpty(workItemIDs)
	if len(ids) == 0 {
		return map[string]teamAssignment{}, nil
	}

	query := fmt.Sprintf(`
SELECT
    work_item_id,
    team_id,
    team_name
FROM work_item_team_attributions FINAL
WHERE work_item_id IN {work_item_ids:Array(String)}
  AND org_id = {org_id:String}
  AND is_primary = 1
  AND (work_item_id, computed_at) IN (
      SELECT work_item_id, max(computed_at)
      FROM work_item_team_attributions
      WHERE work_item_id IN {work_item_ids:Array(String)}
        AND org_id = {org_id:String}
      GROUP BY work_item_id
  )
%s
`, settingsMaxExecutionTime())

	result := map[string]teamAssignment{}
	for _, chunk := range chunkStrings(ids, lookupChunkSize) {
		bindings := []dhclickhouse.Binding{
			{Name: "work_item_ids", Value: chunk},
			{Name: "org_id", Value: orgID},
		}
		rows, err := reader.client.Query(ctx, query, bindings)
		if err != nil {
			return nil, fmt.Errorf("query work item team assignments: %w", err)
		}
		for rows.Next() {
			var workItemID, teamID, teamName string
			if err := rows.Scan(&workItemID, &teamID, &teamName); err != nil {
				rows.Close()
				return nil, fmt.Errorf("scan work item team assignment row: %w", err)
			}
			if workItemID != "" {
				result[workItemID] = teamAssignment{TeamID: teamID, TeamName: teamName}
			}
		}
		rowsErr := rows.Err()
		rows.Close()
		if rowsErr != nil {
			return nil, fmt.Errorf("iterate work item team assignment rows: %w", rowsErr)
		}
	}
	return result, nil
}

// WorkUnitRunPair is one (work_unit_id, categorization_run_id) lookup key
// for FetchWorkUnitInvestmentQuotes.
type WorkUnitRunPair struct {
	WorkUnitID string
	RunID      string
}

// WorkUnitInvestmentQuoteRow is one fetch_work_unit_investment_quotes
// result row.
type WorkUnitInvestmentQuoteRow struct {
	WorkUnitID          string
	Quote               string
	SourceType          string
	SourceID            string
	CategorizationRunID string
}

// FetchWorkUnitInvestmentQuotes ports fetch_work_unit_investment_quotes
// (work_unit_investments.py:217-244).
func (reader *Reader) FetchWorkUnitInvestmentQuotes(ctx context.Context, orgID string, unitRuns []WorkUnitRunPair) ([]WorkUnitInvestmentQuoteRow, error) {
	if reader == nil || reader.client == nil {
		return nil, ErrUnavailable
	}

	seen := map[WorkUnitRunPair]bool{}
	pairs := make([]WorkUnitRunPair, 0, len(unitRuns))
	for _, pair := range unitRuns {
		if pair.WorkUnitID == "" || pair.RunID == "" {
			continue
		}
		if seen[pair] {
			continue
		}
		seen[pair] = true
		pairs = append(pairs, pair)
	}
	if len(pairs) == 0 {
		return nil, nil
	}

	query := fmt.Sprintf(`
SELECT
    work_unit_id,
    quote,
    source_type,
    source_id,
    categorization_run_id
FROM work_unit_investment_quotes
WHERE (work_unit_id, categorization_run_id) IN {pairs:Array(Tuple(String, String))}
  AND org_id = {org_id:String}
%s
`, settingsMaxExecutionTime())

	var results []WorkUnitInvestmentQuoteRow
	for _, chunk := range chunkPairs(pairs, lookupChunkSize) {
		tuples := make([][2]string, len(chunk))
		for i, pair := range chunk {
			tuples[i] = [2]string{pair.WorkUnitID, pair.RunID}
		}
		bindings := []dhclickhouse.Binding{
			{Name: "pairs", Value: tuples},
			{Name: "org_id", Value: orgID},
		}
		rows, err := reader.client.Query(ctx, query, bindings)
		if err != nil {
			return nil, fmt.Errorf("query work unit investment quotes: %w", err)
		}
		for rows.Next() {
			var row WorkUnitInvestmentQuoteRow
			if err := rows.Scan(&row.WorkUnitID, &row.Quote, &row.SourceType, &row.SourceID, &row.CategorizationRunID); err != nil {
				rows.Close()
				return nil, fmt.Errorf("scan work unit investment quote row: %w", err)
			}
			results = append(results, row)
		}
		rowsErr := rows.Err()
		rows.Close()
		if rowsErr != nil {
			return nil, fmt.Errorf("iterate work unit investment quote rows: %w", rowsErr)
		}
	}
	return results, nil
}

func chunkPairs(pairs []WorkUnitRunPair, size int) [][]WorkUnitRunPair {
	var chunks [][]WorkUnitRunPair
	for start := 0; start < len(pairs); start += size {
		end := start + size
		if end > len(pairs) {
			end = len(pairs)
		}
		chunks = append(chunks, pairs[start:end])
	}
	return chunks
}
