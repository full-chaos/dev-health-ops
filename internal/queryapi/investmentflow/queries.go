// ClickHouse readers -- ports api/queries/investment.py's six
// fetch_investment_{subcategory,team,repo_team,team_category_repo,
// team_subcategory_repo,unassigned_counts} functions, api/services/
// investment.py's _tables_present/_columns_present schema-drift guard,
// and investment.py's own REPO_ALLOCATED_WORK_UNIT_INVESTMENTS_SOURCE and
// unit_team_window_filter, restructured (no WITH clause, every CTE an
// inlined derived-table subquery composed by string substitution) the
// same way internal/queryapi/analytics/investment.go and
// internal/queryapi/sankey's own readers already are.
//
// DEDUP: every read here is built on analytics.LatestWorkUnitInvestments
// Source()/LatestWorkUnitRepoEffortSource() -- the already argMax-tuple-
// fixed ports of LATEST_WORK_UNIT_INVESTMENTS_CTE/LATEST_WORK_UNIT_REPO_
// EFFORT_CTE -- and analytics.BuildUnitTeamSubquery(), the already-fixed
// port of build_unit_team_subquery, reused rather than re-derived (same
// CTE, same Python source, same dedup-fix history).
//
// investmentFlowRepoDedupParity (declared Python-plane defect, cited in
// internal/goapiproof/restcorpus.go and this port's own RISK-NOTES):
// every one of the five edge fetchers below that joins
// `repos` (subcategory/repo_team/team_category_repo/team_subcategory_repo
// -- team_edges does not join repos at all) does so in Python with a
// plain, undeduped `LEFT JOIN repos AS r ON toString(r.id) =
// toString(repo_id)`, no FINAL, no org_id predicate -- the exact same
// class of defect sankey/queries.go's fetchInvestmentFlowItems doc
// comment already documents and fixes for the SAME table read from the
// SAME investment surface. This port reads `repos FINAL`, org_id inside
// the JOIN's own ON clause, everywhere Python's fetchers read `repos`
// plain.
package investmentflow

import (
	"context"
	"fmt"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/analytics"
)

func settingsMaxExecutionTime() string {
	return "SETTINGS max_execution_time = 30"
}

// tablesPresent ports _tables_present (api/services/investment.py:42-61,
// byte-identical query shape to api/services/sankey.py's own copy this
// package's sibling ports as sankey.tablesPresent). A lookup failure
// degrades to false (not present), matching Python's own
// try/except-and-return-false fallback.
func tablesPresent(ctx context.Context, client QueryClient, tables []string) bool {
	if len(tables) == 0 {
		return true
	}
	query := fmt.Sprintf(`
SELECT name
FROM system.tables
WHERE database = currentDatabase()
  AND name IN {tables:Array(String)}
%s
`, settingsMaxExecutionTime())
	rows, err := client.Query(ctx, query, []dhclickhouse.Binding{{Name: "tables", Value: tables}})
	if err != nil {
		return false
	}
	defer rows.Close()
	present := map[string]bool{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return false
		}
		present[name] = true
	}
	if rows.Err() != nil {
		return false
	}
	for _, t := range tables {
		if !present[t] {
			return false
		}
	}
	return true
}

// columnsPresent ports _columns_present (api/services/investment.py:
// 64-86).
func columnsPresent(ctx context.Context, client QueryClient, table string, columns []string) bool {
	if len(columns) == 0 {
		return true
	}
	query := fmt.Sprintf(`
SELECT name
FROM system.columns
WHERE database = currentDatabase()
  AND table = {table:String}
  AND name IN {columns:Array(String)}
%s
`, settingsMaxExecutionTime())
	rows, err := client.Query(ctx, query, []dhclickhouse.Binding{
		{Name: "table", Value: table},
		{Name: "columns", Value: columns},
	})
	if err != nil {
		return false
	}
	defer rows.Close()
	present := map[string]bool{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return false
		}
		present[name] = true
	}
	if rows.Err() != nil {
		return false
	}
	for _, c := range columns {
		if !present[c] {
			return false
		}
	}
	return true
}

// repoAllocatedWorkUnitInvestmentsSource ports REPO_ALLOCATED_WORK_UNIT_
// INVESTMENTS_SOURCE (api/queries/investment.py:155-172) -- NOT this
// binary's own internal/queryapi/analytics.repoAllocationInvestment
// Source, a similar-looking but DIFFERENT definition that package's own
// doc comment warns against conflating with this one: this version's
// match flag is the explicit `has_allocation = 1` LATEST_WORK_UNIT_REPO_
// EFFORT_CTE itself projects, not `wure.work_unit_id != ”`, and it
// projects only the columns investment_flow.py's own fetchers read
// (work_unit_id/from_ts/to_ts/org_id/repo_id/effort_value/
// has_allocation/subcategory_distribution_json/structural_evidence_json)
// rather than the GraphQL compiler's full investment-row column set.
// Built from analytics.LatestWorkUnitInvestmentsSource()/
// LatestWorkUnitRepoEffortSource() -- the shared, already argMax-tuple-
// fixed CTEs -- rather than a third, unsynced copy of that dedup history.
func repoAllocatedWorkUnitInvestmentsSource() string {
	return fmt.Sprintf(`(
                SELECT
                    wui.work_unit_id AS work_unit_id,
                    wui.from_ts AS from_ts,
                    wui.to_ts AS to_ts,
                    wui.org_id AS org_id,
                    if(wure.has_allocation = 1, wure.repo_id, wui.repo_id) AS repo_id,
                    if(wure.has_allocation = 1, wure.repo_effort_value, wui.effort_value) AS effort_value,
                    if(wure.has_allocation = 1, 1, 0) AS has_allocation,
                    wui.subcategory_distribution_json AS subcategory_distribution_json,
                    wui.structural_evidence_json AS structural_evidence_json
                FROM %s AS wui
                LEFT JOIN %s AS wure
                    ON wure.org_id = wui.org_id
                    AND wure.work_unit_id = wui.work_unit_id
            ) AS work_unit_investments`, analytics.LatestWorkUnitInvestmentsSource(), analytics.LatestWorkUnitRepoEffortSource())
}

// unitTeamWindowFilter ports unit_team_window_filter (api/queries/
// investment.py:513-521) exactly, categoryFilter empty for every caller
// except fetchInvestmentUnassignedCounts (the only Python call site that
// passes a non-default category_filter).
func unitTeamWindowFilter(scopeFilter, categoryFilter string) string {
	return fmt.Sprintf(`                WHERE work_unit_investments.from_ts < {end_ts:DateTime64(3, 'UTC')}
                  AND work_unit_investments.to_ts >= {start_ts:DateTime64(3, 'UTC')}
                  AND work_unit_investments.org_id = {org_id:String}
                %s
                %s`, scopeFilter, categoryFilter)
}

// categoryFilterClause builds the "AND (<theme/subcategory predicates>)"
// suffix every fetcher below shares -- ports each function's own local
// `filters`/`category_filter` construction (byte-identical shape across
// all six, confirmed by reading each in turn).
func categoryThemeSubcategoryClause(themeExpr string, themes, subcategories []string) (clause string, bindings []dhclickhouse.Binding) {
	var parts []string
	if len(themes) > 0 {
		parts = append(parts, fmt.Sprintf("%s IN {themes:Array(String)}", themeExpr))
		bindings = append(bindings, dhclickhouse.Binding{Name: "themes", Value: themes})
	}
	if len(subcategories) > 0 {
		parts = append(parts, "subcategory_kv.1 IN {subcategories:Array(String)}")
		bindings = append(bindings, dhclickhouse.Binding{Name: "subcategories", Value: subcategories})
	}
	if len(parts) == 0 {
		return "", nil
	}
	clause = " AND (" + joinOR(parts) + ")"
	return clause, bindings
}

func joinOR(parts []string) string {
	out := parts[0]
	for _, p := range parts[1:] {
		out += " OR " + p
	}
	return out
}

func baseBindings(startTS, endTS time.Time, orgID string, scopeBindings, categoryBindings []dhclickhouse.Binding) []dhclickhouse.Binding {
	out := []dhclickhouse.Binding{
		{Name: "start_ts", Value: startTS},
		{Name: "end_ts", Value: endTS},
		{Name: "org_id", Value: orgID},
	}
	out = append(out, scopeBindings...)
	out = append(out, categoryBindings...)
	return out
}

// edgeRow is fetch_investment_subcategory_edges'/fetch_investment_team_
// edges' shared row shape (source, target, value).
type edgeRow struct {
	Source string
	Target string
	Value  float64
}

func scanEdgeRows(rows dhclickhouse.RowScanner, errPrefix string) ([]edgeRow, error) {
	defer rows.Close()
	out := make([]edgeRow, 0)
	for rows.Next() {
		var row edgeRow
		if err := rows.Scan(&row.Source, &row.Target, &row.Value); err != nil {
			return nil, fmt.Errorf("%s: scan row: %w", errPrefix, err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%s: iterate rows: %w", errPrefix, err)
	}
	return out, nil
}

// fetchInvestmentSubcategoryEdges ports fetch_investment_subcategory_edges
// (api/queries/investment.py:650-689).
func fetchInvestmentSubcategoryEdges(ctx context.Context, client QueryClient, startTS, endTS time.Time, scopeFilterSQL string, scopeBindings []dhclickhouse.Binding, orgID string, themes, subcategories []string) ([]edgeRow, error) {
	categoryFilter, categoryBindings := categoryThemeSubcategoryClause("splitByChar('.', subcategory_kv.1)[1]", themes, subcategories)
	query := fmt.Sprintf(`
        SELECT
            subcategory_kv.1 AS source,
            ifNull(r.repo, if(repo_id IS NULL, 'unassigned', toString(repo_id))) AS target,
            sum(subcategory_kv.2 * effort_value) AS value
        FROM %s AS work_unit_investments
        LEFT JOIN repos AS r FINAL ON toString(r.id) = toString(repo_id) AND r.org_id = {org_id:String}
        ARRAY JOIN CAST(subcategory_distribution_json AS Array(Tuple(String, Float32))) AS subcategory_kv
        WHERE work_unit_investments.from_ts < {end_ts:DateTime64(3, 'UTC')}
          AND work_unit_investments.to_ts >= {start_ts:DateTime64(3, 'UTC')}
          AND work_unit_investments.org_id = {org_id:String}
        %s
        %s
        GROUP BY source, target
        ORDER BY value DESC
        %s
    `, analytics.LatestWorkUnitInvestmentsSource(), scopeFilterSQL, categoryFilter, settingsMaxExecutionTime())
	rows, err := client.Query(ctx, query, baseBindings(startTS, endTS, orgID, scopeBindings, categoryBindings))
	if err != nil {
		return nil, fmt.Errorf("investmentflow: fetch investment subcategory edges: %w", err)
	}
	return scanEdgeRows(rows, "investmentflow: fetch investment subcategory edges")
}

// fetchInvestmentTeamEdges ports fetch_investment_team_edges (api/queries/
// investment.py:692-736). No repos join -- team_edges is the one edge
// fetcher of the five that never reads repos.
func fetchInvestmentTeamEdges(ctx context.Context, client QueryClient, startTS, endTS time.Time, scopeFilterSQL string, scopeBindings []dhclickhouse.Binding, orgID string, themes, subcategories []string) ([]edgeRow, error) {
	categoryFilter, categoryBindings := categoryThemeSubcategoryClause("splitByChar('.', subcategory_kv.1)[1]", themes, subcategories)
	unitTeamSQL := analytics.BuildUnitTeamSubquery(analytics.UnitTeamSubqueryOptions{
		Source:         fmt.Sprintf("%s AS work_unit_investments", analytics.LatestWorkUnitInvestmentsSource()),
		Where:          unitTeamWindowFilter(scopeFilterSQL, ""),
		InnerTeamAlias: "team",
	})
	query := fmt.Sprintf(`
        SELECT
            subcategory_kv.1 AS source,
            ifNull(nullIf(unit_team.team, ''), 'unassigned') AS target,
            sum(subcategory_kv.2 * effort_value) AS value
        FROM %s AS work_unit_investments
        LEFT JOIN (%s) AS unit_team ON unit_team.work_unit_id = work_unit_investments.work_unit_id
        ARRAY JOIN CAST(subcategory_distribution_json AS Array(Tuple(String, Float32))) AS subcategory_kv
        WHERE work_unit_investments.from_ts < {end_ts:DateTime64(3, 'UTC')}
          AND work_unit_investments.to_ts >= {start_ts:DateTime64(3, 'UTC')}
          AND work_unit_investments.org_id = {org_id:String}
        %s
        %s
        GROUP BY source, target
        ORDER BY value DESC
        %s
    `, analytics.LatestWorkUnitInvestmentsSource(), unitTeamSQL, scopeFilterSQL, categoryFilter, settingsMaxExecutionTime())
	rows, err := client.Query(ctx, query, baseBindings(startTS, endTS, orgID, scopeBindings, categoryBindings))
	if err != nil {
		return nil, fmt.Errorf("investmentflow: fetch investment team edges: %w", err)
	}
	return scanEdgeRows(rows, "investmentflow: fetch investment team edges")
}

// repoTeamEdgeRow is fetch_investment_repo_team_edges' row shape
// (subcategory, repo, team, value).
type repoTeamEdgeRow struct {
	Subcategory string
	Repo        string
	Team        string
	Value       float64
}

// fetchInvestmentRepoTeamEdges ports fetch_investment_repo_team_edges
// (api/queries/investment.py:739-786).
func fetchInvestmentRepoTeamEdges(ctx context.Context, client QueryClient, startTS, endTS time.Time, scopeFilterSQL string, scopeBindings []dhclickhouse.Binding, orgID string, themes, subcategories []string) ([]repoTeamEdgeRow, error) {
	categoryFilter, categoryBindings := categoryThemeSubcategoryClause("splitByChar('.', subcategory_kv.1)[1]", themes, subcategories)
	source := repoAllocatedWorkUnitInvestmentsSource()
	unitTeamSQL := analytics.BuildUnitTeamSubquery(analytics.UnitTeamSubqueryOptions{
		Source:         source,
		Where:          unitTeamWindowFilter(scopeFilterSQL, ""),
		InnerTeamAlias: "team",
	})
	query := fmt.Sprintf(`
        SELECT
            subcategory_kv.1 AS subcategory,
            ifNull(r.repo, if(repo_id IS NULL, 'unassigned', toString(repo_id))) AS repo,
            ifNull(nullIf(unit_team.team, ''), 'unassigned') AS team,
            sum(subcategory_kv.2 * effort_value) AS value
        FROM %s
        LEFT JOIN repos AS r FINAL ON toString(r.id) = toString(repo_id) AND r.org_id = {org_id:String}
        LEFT JOIN (%s) AS unit_team ON unit_team.work_unit_id = work_unit_investments.work_unit_id
        ARRAY JOIN CAST(subcategory_distribution_json AS Array(Tuple(String, Float32))) AS subcategory_kv
        WHERE work_unit_investments.from_ts < {end_ts:DateTime64(3, 'UTC')}
          AND work_unit_investments.to_ts >= {start_ts:DateTime64(3, 'UTC')}
          AND work_unit_investments.org_id = {org_id:String}
        %s
        %s
        GROUP BY subcategory, repo, team
        ORDER BY value DESC
        %s
    `, source, unitTeamSQL, scopeFilterSQL, categoryFilter, settingsMaxExecutionTime())
	rows, err := client.Query(ctx, query, baseBindings(startTS, endTS, orgID, scopeBindings, categoryBindings))
	if err != nil {
		return nil, fmt.Errorf("investmentflow: fetch investment repo team edges: %w", err)
	}
	defer rows.Close()
	out := make([]repoTeamEdgeRow, 0)
	for rows.Next() {
		var row repoTeamEdgeRow
		if err := rows.Scan(&row.Subcategory, &row.Repo, &row.Team, &row.Value); err != nil {
			return nil, fmt.Errorf("investmentflow: scan investment repo team edge row: %w", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("investmentflow: iterate investment repo team edge rows: %w", err)
	}
	return out, nil
}

// teamCategoryRepoEdgeRow is fetch_investment_team_category_repo_edges'
// row shape (team, category, repo, value).
type teamCategoryRepoEdgeRow struct {
	Team     string
	Category string
	Repo     string
	Value    float64
}

// fetchInvestmentTeamCategoryRepoEdges ports fetch_investment_team_
// category_repo_edges (api/queries/investment.py:789-836).
func fetchInvestmentTeamCategoryRepoEdges(ctx context.Context, client QueryClient, startTS, endTS time.Time, scopeFilterSQL string, scopeBindings []dhclickhouse.Binding, orgID string, themes, subcategories []string) ([]teamCategoryRepoEdgeRow, error) {
	categoryFilter, categoryBindings := categoryThemeSubcategoryClause("splitByChar('.', subcategory_kv.1)[1]", themes, subcategories)
	source := repoAllocatedWorkUnitInvestmentsSource()
	unitTeamSQL := analytics.BuildUnitTeamSubquery(analytics.UnitTeamSubqueryOptions{
		Source:         source,
		Where:          unitTeamWindowFilter(scopeFilterSQL, ""),
		InnerTeamAlias: "team",
	})
	query := fmt.Sprintf(`
        SELECT
            ifNull(nullIf(unit_team.team, ''), 'unassigned') AS team,
            splitByChar('.', subcategory_kv.1)[1] AS category,
            ifNull(r.repo, if(repo_id IS NULL, 'unassigned', toString(repo_id))) AS repo,
            sum(subcategory_kv.2 * effort_value) AS value
        FROM %s
        LEFT JOIN repos AS r FINAL ON toString(r.id) = toString(repo_id) AND r.org_id = {org_id:String}
        LEFT JOIN (%s) AS unit_team ON unit_team.work_unit_id = work_unit_investments.work_unit_id
        ARRAY JOIN CAST(subcategory_distribution_json AS Array(Tuple(String, Float32))) AS subcategory_kv
        WHERE work_unit_investments.from_ts < {end_ts:DateTime64(3, 'UTC')}
          AND work_unit_investments.to_ts >= {start_ts:DateTime64(3, 'UTC')}
          AND work_unit_investments.org_id = {org_id:String}
        %s
        %s
        GROUP BY team, category, repo
        ORDER BY value DESC
        %s
    `, source, unitTeamSQL, scopeFilterSQL, categoryFilter, settingsMaxExecutionTime())
	rows, err := client.Query(ctx, query, baseBindings(startTS, endTS, orgID, scopeBindings, categoryBindings))
	if err != nil {
		return nil, fmt.Errorf("investmentflow: fetch investment team category repo edges: %w", err)
	}
	defer rows.Close()
	out := make([]teamCategoryRepoEdgeRow, 0)
	for rows.Next() {
		var row teamCategoryRepoEdgeRow
		if err := rows.Scan(&row.Team, &row.Category, &row.Repo, &row.Value); err != nil {
			return nil, fmt.Errorf("investmentflow: scan investment team category repo edge row: %w", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("investmentflow: iterate investment team category repo edge rows: %w", err)
	}
	return out, nil
}

// teamSubcategoryRepoEdgeRow is fetch_investment_team_subcategory_repo_
// edges' row shape (team, subcategory, repo, value).
type teamSubcategoryRepoEdgeRow struct {
	Team        string
	Subcategory string
	Repo        string
	Value       float64
}

// fetchInvestmentTeamSubcategoryRepoEdges ports fetch_investment_team_
// subcategory_repo_edges (api/queries/investment.py:839-886).
func fetchInvestmentTeamSubcategoryRepoEdges(ctx context.Context, client QueryClient, startTS, endTS time.Time, scopeFilterSQL string, scopeBindings []dhclickhouse.Binding, orgID string, themes, subcategories []string) ([]teamSubcategoryRepoEdgeRow, error) {
	categoryFilter, categoryBindings := categoryThemeSubcategoryClause("splitByChar('.', subcategory_kv.1)[1]", themes, subcategories)
	source := repoAllocatedWorkUnitInvestmentsSource()
	unitTeamSQL := analytics.BuildUnitTeamSubquery(analytics.UnitTeamSubqueryOptions{
		Source:         source,
		Where:          unitTeamWindowFilter(scopeFilterSQL, ""),
		InnerTeamAlias: "team",
	})
	query := fmt.Sprintf(`
        SELECT
            ifNull(nullIf(unit_team.team, ''), 'unassigned') AS team,
            subcategory_kv.1 AS subcategory,
            ifNull(r.repo, if(repo_id IS NULL, 'unassigned', toString(repo_id))) AS repo,
            sum(subcategory_kv.2 * effort_value) AS value
        FROM %s
        LEFT JOIN repos AS r FINAL ON toString(r.id) = toString(repo_id) AND r.org_id = {org_id:String}
        LEFT JOIN (%s) AS unit_team ON unit_team.work_unit_id = work_unit_investments.work_unit_id
        ARRAY JOIN CAST(subcategory_distribution_json AS Array(Tuple(String, Float32))) AS subcategory_kv
        WHERE work_unit_investments.from_ts < {end_ts:DateTime64(3, 'UTC')}
          AND work_unit_investments.to_ts >= {start_ts:DateTime64(3, 'UTC')}
          AND work_unit_investments.org_id = {org_id:String}
        %s
        %s
        GROUP BY team, subcategory, repo
        ORDER BY value DESC
        %s
    `, source, unitTeamSQL, scopeFilterSQL, categoryFilter, settingsMaxExecutionTime())
	rows, err := client.Query(ctx, query, baseBindings(startTS, endTS, orgID, scopeBindings, categoryBindings))
	if err != nil {
		return nil, fmt.Errorf("investmentflow: fetch investment team subcategory repo edges: %w", err)
	}
	defer rows.Close()
	out := make([]teamSubcategoryRepoEdgeRow, 0)
	for rows.Next() {
		var row teamSubcategoryRepoEdgeRow
		if err := rows.Scan(&row.Team, &row.Subcategory, &row.Repo, &row.Value); err != nil {
			return nil, fmt.Errorf("investmentflow: scan investment team subcategory repo edge row: %w", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("investmentflow: iterate investment team subcategory repo edge rows: %w", err)
	}
	return out, nil
}

// unassignedCounts is fetch_investment_unassigned_counts' row shape.
//
// countDistinctIf always returns UInt64 in ClickHouse regardless of the
// counted column's type -- the driver refuses to scan that into *int64.
// api/queries/investment.py's fetch_investment_unassigned_counts wraps
// both fields in int(...) before returning them, and this package's own
// caller (investmentflow.go) immediately narrows both into a
// map[string]int, so the wire contract for these two fields is a plain
// integer, never a float. uint64 scans the driver's actual result type
// directly instead of a SQL cast, matching that integer contract with no
// intermediate float roundtrip.
type unassignedCounts struct {
	MissingTeam uint64
	MissingRepo uint64
}

// fetchInvestmentUnassignedCounts ports fetch_investment_unassigned_counts
// (api/queries/investment.py:889-956). Unlike the five edge fetchers, this
// one's category_filter IS forwarded into the unit_team subquery's own
// WHERE (unit_team_window_filter's second argument) -- ported verbatim,
// not a simplification.
func fetchInvestmentUnassignedCounts(ctx context.Context, client QueryClient, startTS, endTS time.Time, scopeFilterSQL string, scopeBindings []dhclickhouse.Binding, orgID string, themes, subcategories []string) (unassignedCounts, error) {
	var categoryFilter string
	var categoryBindings []dhclickhouse.Binding
	var parts []string
	if len(themes) > 0 {
		parts = append(parts, "arrayExists(k -> splitByChar('.', k)[1] IN {themes:Array(String)}, mapKeys(CAST(subcategory_distribution_json AS Map(String, Float32))))")
		categoryBindings = append(categoryBindings, dhclickhouse.Binding{Name: "themes", Value: themes})
	}
	if len(subcategories) > 0 {
		parts = append(parts, "hasAny(mapKeys(CAST(subcategory_distribution_json AS Map(String, Float32))), {subcategories:Array(String)})")
		categoryBindings = append(categoryBindings, dhclickhouse.Binding{Name: "subcategories", Value: subcategories})
	}
	if len(parts) > 0 {
		categoryFilter = " AND (" + joinOR(parts) + ")"
	}

	source := repoAllocatedWorkUnitInvestmentsSource()
	unitTeamSQL := analytics.BuildUnitTeamSubquery(analytics.UnitTeamSubqueryOptions{
		Source:         source,
		Where:          unitTeamWindowFilter(scopeFilterSQL, categoryFilter),
		InnerTeamAlias: "team",
	})
	query := fmt.Sprintf(`
        SELECT
            countDistinctIf(
                work_unit_investments.work_unit_id,
                has_allocation = 0 AND repo_id IS NULL
            ) AS missing_repo,
            countDistinctIf(
                work_unit_investments.work_unit_id,
                ifNull(nullIf(unit_team.team, ''), '') = ''
            ) AS missing_team
        FROM %s
        LEFT JOIN (%s) AS unit_team ON unit_team.work_unit_id = work_unit_investments.work_unit_id
        WHERE work_unit_investments.from_ts < {end_ts:DateTime64(3, 'UTC')}
          AND work_unit_investments.to_ts >= {start_ts:DateTime64(3, 'UTC')}
          AND work_unit_investments.org_id = {org_id:String}
        %s
        %s
        %s
    `, source, unitTeamSQL, scopeFilterSQL, categoryFilter, settingsMaxExecutionTime())
	rows, err := client.Query(ctx, query, baseBindings(startTS, endTS, orgID, scopeBindings, categoryBindings))
	if err != nil {
		return unassignedCounts{}, fmt.Errorf("investmentflow: fetch investment unassigned counts: %w", err)
	}
	defer rows.Close()
	var out unassignedCounts
	if rows.Next() {
		if err := rows.Scan(&out.MissingRepo, &out.MissingTeam); err != nil {
			return unassignedCounts{}, fmt.Errorf("investmentflow: scan investment unassigned counts row: %w", err)
		}
	}
	if err := rows.Err(); err != nil {
		return unassignedCounts{}, fmt.Errorf("investmentflow: iterate investment unassigned counts rows: %w", err)
	}
	return out, nil
}
