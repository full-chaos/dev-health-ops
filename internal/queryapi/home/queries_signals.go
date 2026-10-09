// ClickHouse readers for the recommendation and compounding-risk signal
// panels -- ports the two SQL blocks and readers api/services/home.py
// declares directly (_RECOMMENDATIONS_SQL/_fetch_recommendation_signals,
// _COMPOUNDING_RISK_SQL/_fetch_risk_signals) plus _resolve_scope_labels.
//
// recommendations_daily and compounding_risk_daily are both
// ReplacingMergeTree(computed_at) since creation (confirmed against
// prod); both readers already dedup correctly with a
// two-stage argMax(..., computed_at)/argMax(tuple(...), computed_at)
// pattern matching their natural key -- ported verbatim, no divergence.
//
// DEDUP (declared Python-plane defect fixed here):
// resolveScopeLabels' repos/teams reads (_resolve_scope_labels,
// services/home.py:647-713) run with no dedup at all in Python -- both
// tables are ReplacingMergeTree, read FINAL here.
package home

import (
	"context"
	"fmt"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/storage/clickhouse/liverow"
)

const recommendationsSQL = `
    SELECT
        team_id,
        org_id,
        rule_id,
        argMax(latest_fired,             window_end) AS latest_fired,
        argMax(latest_severity,          window_end) AS latest_severity,
        argMax(latest_title,             window_end) AS latest_title,
        argMax(latest_rationale,         window_end) AS latest_rationale,
        argMax(latest_success_criterion, window_end) AS latest_success_criterion,
        argMax(latest_evidence_json,     window_end) AS latest_evidence_json,
        argMax(latest_window_start,      window_end) AS latest_window_start,
        max(window_end)                              AS latest_window_end,
        argMax(latest_computed_at,       window_end) AS latest_computed_at
    FROM (
        SELECT
            team_id,
            org_id,
            rule_id,
            window_end,
            argMax(fired,               computed_at) AS latest_fired,
            argMax(severity,            computed_at) AS latest_severity,
            argMax(title,               computed_at) AS latest_title,
            argMax(rationale,           computed_at) AS latest_rationale,
            argMax(success_criterion,   computed_at) AS latest_success_criterion,
            argMax(evidence_json,       computed_at) AS latest_evidence_json,
            argMax(window_start,        computed_at) AS latest_window_start,
            max(computed_at)                         AS latest_computed_at
        FROM recommendations_daily
        WHERE org_id = {org_id:String}
          AND window_end >= {window_start:Date}
          AND window_end <= {window_end:Date}
          %s
        GROUP BY org_id, team_id, rule_id, window_end
    )
    GROUP BY org_id, team_id, rule_id
    HAVING latest_fired = true
    ORDER BY latest_window_end DESC, latest_severity DESC, rule_id
    LIMIT 10
`

// fetchRecommendationSignals ports _fetch_recommendation_signals
// (services/home.py:771-800): only runs at team scope with at least one
// team id, matching Python's own early-return guard.
//
// A read failure is RETURNED (CHAOS-8186). Python answered it with an empty
// list (`except Exception: logger.exception(...); return []`) and this port
// copied that: the caller then got a Home answer with no recommendation
// signals and HTTP 200, the same answer as a team that has none. A failed
// read and an empty panel are different states, so the error goes up and
// BuildResponse fails the request, as it does for every other Home read.
func fetchRecommendationSignals(ctx context.Context, client QueryClient, f Filters, startDay, endDay time.Time, orgID string) ([]RecommendationRow, error) {
	if f.Scope.Level != "team" || len(f.Scope.IDs) == 0 {
		return nil, nil
	}
	query := fmt.Sprintf(recommendationsSQL, "AND team_id IN {team_ids:Array(String)}")
	bindings := []dhclickhouse.Binding{
		{Name: "org_id", Value: orgID},
		{Name: "window_start", Value: formatDay(startDay)},
		{Name: "window_end", Value: formatDay(endDay)},
		{Name: "team_ids", Value: f.Scope.IDs},
	}
	rows, err := client.Query(ctx, query, bindings)
	if err != nil {
		return nil, fmt.Errorf("home: fetch_recommendation_signals query: %w", err)
	}
	defer rows.Close()

	var out []RecommendationRow
	for rows.Next() {
		var teamID, orgIDCol, ruleID string
		var latestFired bool
		var latestSeverity, latestTitle, latestRationale, latestSuccess, latestEvidence string
		var latestWindowStart, latestWindowEnd time.Time
		var latestComputedAt time.Time
		if err := rows.Scan(&teamID, &orgIDCol, &ruleID, &latestFired, &latestSeverity, &latestTitle,
			&latestRationale, &latestSuccess, &latestEvidence, &latestWindowStart, &latestWindowEnd,
			&latestComputedAt); err != nil {
			return nil, fmt.Errorf("home: fetch_recommendation_signals scan: %w", err)
		}
		out = append(out, RecommendationRow{
			TeamID: teamID, RuleID: ruleID, LatestSeverity: latestSeverity, LatestTitle: latestTitle,
			LatestRationale: latestRationale, LatestSuccess: latestSuccess, LatestEvidence: latestEvidence,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("home: fetch_recommendation_signals rows: %w", err)
	}
	return out, nil
}

const compoundingRiskSQLBase = `
    SELECT
        scope,
        scope_id,
        tupleElement(latest_row, 1) AS score,
        tupleElement(latest_row, 2) AS severity,
        tupleElement(latest_row, 3) AS latest_computed_at
    FROM (
        SELECT
            scope,
            scope_id,
            argMax(tuple(compounding_risk, severity, computed_at), computed_at) AS latest_row
        FROM compounding_risk_daily
        WHERE org_id = {org_id:String}
          AND day = (
              SELECT maxOrNull(day)
              FROM (
                  SELECT
                      day,
                      count() AS row_count,
                      countIf(tupleElement(latest_row, 1) IS NULL) AS missing_scores
                  FROM (
                      SELECT
                          day,
                          scope,
                          scope_id,
                          argMax(tuple(compounding_risk), computed_at) AS latest_row
                      FROM compounding_risk_daily
                      WHERE org_id = {org_id:String}
                        AND day >= {start_day:Date}
                        AND day < {end_day:Date}
                      %s
                      GROUP BY day, scope, scope_id
                      HAVING %s
                  )
                  GROUP BY day
              )
              WHERE row_count > 0 AND missing_scores = 0
          )
`

// fetchRiskSignals ports _fetch_risk_signals (services/home.py:803-860),
// including its subsequent scope-label resolution.
//
// A read failure is RETURNED (CHAOS-8186), for the reason given on
// fetchRecommendationSignals: no risk signal because the read failed is not
// the state "no risk row in this window". The scope-label lookup stays
// best-effort (resolveScopeLabels): a label is a name, not a value, and a
// row with no label falls back to its id.
func fetchRiskSignals(ctx context.Context, client QueryClient, f Filters, startDay, endDay time.Time, orgID string) ([]RiskRow, error) {
	latestScopeFilter := ""
	scoped := f.Scope.Level == "team" || f.Scope.Level == "repo"
	if scoped && len(f.Scope.IDs) > 0 {
		latestScopeFilter = "AND scope = {scope:String} AND scope_id IN {scope_ids:Array(String)}"
	}
	// measuredRisk keeps a scope id only when its newest row is a measurement
	// (package liverow). A retraction row over the key of a retired team id
	// has a NULL score: counted as a row it would make every day look
	// incomplete (missing_scores > 0) and the day picker would refuse the
	// newest day, and listed as a row it would be a signal with no score.
	measuredRisk := liverow.NewestPredicate("compounding_risk_daily", "")
	query := fmt.Sprintf(compoundingRiskSQLBase, latestScopeFilter, measuredRisk)
	if scoped && len(f.Scope.IDs) > 0 {
		query += `
      AND scope = {scope:String}
      AND scope_id IN {scope_ids:Array(String)}
        `
	}
	query += `
        GROUP BY scope, scope_id
        HAVING ` + measuredRisk + `
    )
    ORDER BY score DESC NULLS LAST
    LIMIT 5
    `
	bindings := []dhclickhouse.Binding{
		{Name: "org_id", Value: orgID},
		{Name: "start_day", Value: formatDay(startDay)},
		{Name: "end_day", Value: formatDay(endDay)},
	}
	if scoped && len(f.Scope.IDs) > 0 {
		bindings = append(bindings,
			dhclickhouse.Binding{Name: "scope", Value: f.Scope.Level},
			dhclickhouse.Binding{Name: "scope_ids", Value: f.Scope.IDs},
		)
	}

	rows, err := client.Query(ctx, query, bindings)
	if err != nil {
		return nil, fmt.Errorf("home: fetch_risk_signals query: %w", err)
	}
	defer rows.Close()

	var out []RiskRow
	for rows.Next() {
		var scope, scopeID string
		var score *float64
		var severity string
		var latestComputedAt time.Time
		if err := rows.Scan(&scope, &scopeID, &score, &severity, &latestComputedAt); err != nil {
			return nil, fmt.Errorf("home: fetch_risk_signals scan: %w", err)
		}
		out = append(out, RiskRow{Scope: scope, ScopeID: scopeID, Score: score, Severity: severity})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("home: fetch_risk_signals rows: %w", err)
	}
	if len(out) == 0 {
		return nil, nil
	}

	labels := resolveScopeLabels(ctx, client, orgID, out)
	for i := range out {
		out[i].ScopeDisplayName = labels[out[i].ScopeID]
	}
	return out, nil
}

// resolveScopeLabels ports _resolve_scope_labels (services/home.py:
// 647-713), reading repos/teams FINAL (see this file's own package doc
// comment for the dedup fix). Best-effort: either lookup's
// failure is swallowed, matching Python's `except Exception:
// logger.warning(...)`.
func resolveScopeLabels(ctx context.Context, client QueryClient, orgID string, rows []RiskRow) map[string]string {
	var repoIDs, teamIDs []string
	for _, r := range rows {
		if r.Scope == "repo" && r.ScopeID != "" {
			repoIDs = append(repoIDs, r.ScopeID)
		}
		if r.Scope == "team" && r.ScopeID != "" {
			teamIDs = append(teamIDs, r.ScopeID)
		}
	}

	out := map[string]string{}

	if len(repoIDs) > 0 {
		query := `
                SELECT toString(id) AS scope_id, repo AS display_name
                FROM repos FINAL
                WHERE org_id = {org_id:String}
                  AND toString(id) IN {scope_ids:Array(String)}
                `
		bindings := []dhclickhouse.Binding{
			{Name: "org_id", Value: orgID},
			{Name: "scope_ids", Value: repoIDs},
		}
		if rs, err := client.Query(ctx, query, bindings); err == nil {
			func() {
				defer rs.Close()
				for rs.Next() {
					var id, name string
					if rs.Scan(&id, &name) == nil {
						if name == "" {
							name = id
						}
						out[id] = name
					}
				}
			}()
		}
	}

	if len(teamIDs) > 0 {
		query := `
                SELECT toString(id) AS scope_id, name AS display_name
                FROM teams FINAL
                WHERE org_id = {org_id:String}
                `
		bindings := []dhclickhouse.Binding{{Name: "org_id", Value: orgID}}
		if rs, err := client.Query(ctx, query, bindings); err == nil {
			func() {
				defer rs.Close()
				for rs.Next() {
					var id, name string
					if rs.Scan(&id, &name) == nil {
						if name == "" {
							name = id
						}
						out[id] = name
					}
				}
			}()
		}
	}

	return out
}
