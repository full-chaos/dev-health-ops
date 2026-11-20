// ClickHouse readers for the recommendation and compounding-risk signal
// panels -- ports the two SQL blocks and readers api/services/home.py
// declares directly (_RECOMMENDATIONS_SQL/_fetch_recommendation_signals,
// _COMPOUNDING_RISK_SQL/_fetch_risk_signals) plus _resolve_scope_labels.
//
// recommendations_daily and compounding_risk_daily are both
// ReplacingMergeTree(computed_at) since creation (confirmed against
// prod). Each reader takes ONE version of a key: the newest computed_at,
// then a hash of the version's values (CHAOS-14355), so two versions that
// share a computed_at serve one of them whatever the scan order. The outer
// by-window_end pick of the recommendation read cannot tie: window_end is a
// GROUP BY key of its inner read.
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
            argMax(
                tuple(recommendations_daily.fired, recommendations_daily.severity, recommendations_daily.title,
                    recommendations_daily.rationale, recommendations_daily.success_criterion,
                    recommendations_daily.evidence_json, recommendations_daily.window_start),
                (recommendations_daily.computed_at, cityHash64(tuple(
                    recommendations_daily.fired, recommendations_daily.severity, recommendations_daily.title,
                    recommendations_daily.rationale, recommendations_daily.success_criterion,
                    recommendations_daily.evidence_json, recommendations_daily.window_start)))
            ) AS picked,
            picked.1 AS latest_fired,
            picked.2 AS latest_severity,
            picked.3 AS latest_title,
            picked.4 AS latest_rationale,
            picked.5 AS latest_success_criterion,
            picked.6 AS latest_evidence_json,
            picked.7 AS latest_window_start,
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

// compoundingRiskNewestOrder orders the versions of one compounding_risk_daily
// key: the newest computed_at, then a hash of the version's values (the
// severity and every liverow column). The day picker and the served rows order
// by it, so both judge the same version of a key whose versions share a
// computed_at.
const compoundingRiskNewestOrder = `(compounding_risk_daily.computed_at, cityHash64(tuple(
    compounding_risk_daily.severity, compounding_risk_daily.w_churn, compounding_risk_daily.w_complexity,
    compounding_risk_daily.w_ownership, compounding_risk_daily.w_review, compounding_risk_daily.threshold_elevated,
    compounding_risk_daily.threshold_high, compounding_risk_daily.compounding_risk,
    compounding_risk_daily.churn_norm, compounding_risk_daily.complexity_norm, compounding_risk_daily.ownership_norm,
    compounding_risk_daily.review_norm, compounding_risk_daily.rework_churn, compounding_risk_daily.complexity_delta,
    compounding_risk_daily.bus_factor, compounding_risk_daily.ownership_gini,
    compounding_risk_daily.single_owner_ratio, compounding_risk_daily.review_latency_p90h,
    compounding_risk_daily.merged_prs_30d, compounding_risk_daily.reviewed_prs_30d,
    compounding_risk_daily.review_share_30d)))`

const compoundingRiskSQLBase = `
    SELECT
        scope,
        scope_id,
        tupleElement(latest_row, 1) AS score,
        tupleElement(latest_row, 2) AS severity,
        tupleElement(latest_row, 3) AS latest_computed_at,
        -- the share of the weight that was present in the score: served only
        -- beside a score (a row with no score has no coverage)
        if(
            tupleElement(latest_row, 1) IS NULL
              OR (tupleElement(latest_row, 8) + tupleElement(latest_row, 9) + tupleElement(latest_row, 10) + tupleElement(latest_row, 11)) <= 0,
            NULL,
            (
                if(tupleElement(latest_row, 4) IS NULL, 0, tupleElement(latest_row, 8))
              + if(tupleElement(latest_row, 5) IS NULL, 0, tupleElement(latest_row, 9))
              + if(tupleElement(latest_row, 6) IS NULL, 0, tupleElement(latest_row, 10))
              + if(tupleElement(latest_row, 7) IS NULL, 0, tupleElement(latest_row, 11))
            ) / (tupleElement(latest_row, 8) + tupleElement(latest_row, 9) + tupleElement(latest_row, 10) + tupleElement(latest_row, 11))
        ) AS coverage
    FROM (
        SELECT
            scope,
            scope_id,
            argMax(tuple(
                compounding_risk_daily.compounding_risk, compounding_risk_daily.severity,
                compounding_risk_daily.computed_at, compounding_risk_daily.churn_norm,
                compounding_risk_daily.complexity_norm, compounding_risk_daily.ownership_norm,
                compounding_risk_daily.review_norm, compounding_risk_daily.w_churn,
                compounding_risk_daily.w_complexity, compounding_risk_daily.w_ownership,
                compounding_risk_daily.w_review, compounding_risk_daily.threshold_elevated,
                compounding_risk_daily.threshold_high, compounding_risk_daily.rework_churn,
                compounding_risk_daily.complexity_delta, compounding_risk_daily.bus_factor,
                compounding_risk_daily.ownership_gini, compounding_risk_daily.single_owner_ratio,
                compounding_risk_daily.review_latency_p90h, compounding_risk_daily.merged_prs_30d,
                compounding_risk_daily.reviewed_prs_30d, compounding_risk_daily.review_share_30d
            ), ` + compoundingRiskNewestOrder + `) AS latest_row,
            tupleElement(latest_row, 8) AS w_churn,
            tupleElement(latest_row, 9) AS w_complexity,
            tupleElement(latest_row, 10) AS w_ownership,
            tupleElement(latest_row, 11) AS w_review,
            tupleElement(latest_row, 12) AS threshold_elevated,
            tupleElement(latest_row, 13) AS threshold_high,
            tupleElement(latest_row, 1) AS compounding_risk,
            tupleElement(latest_row, 4) AS churn_norm,
            tupleElement(latest_row, 5) AS complexity_norm,
            tupleElement(latest_row, 6) AS ownership_norm,
            tupleElement(latest_row, 7) AS review_norm,
            tupleElement(latest_row, 14) AS rework_churn,
            tupleElement(latest_row, 15) AS complexity_delta,
            tupleElement(latest_row, 16) AS bus_factor,
            tupleElement(latest_row, 17) AS ownership_gini,
            tupleElement(latest_row, 18) AS single_owner_ratio,
            tupleElement(latest_row, 19) AS review_latency_p90h,
            tupleElement(latest_row, 20) AS merged_prs_30d,
            tupleElement(latest_row, 21) AS reviewed_prs_30d,
            tupleElement(latest_row, 22) AS review_share_30d
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
                          argMax(tuple(
                              compounding_risk_daily.compounding_risk, compounding_risk_daily.w_churn,
                              compounding_risk_daily.w_complexity, compounding_risk_daily.w_ownership,
                              compounding_risk_daily.w_review, compounding_risk_daily.threshold_elevated,
                              compounding_risk_daily.threshold_high, compounding_risk_daily.churn_norm,
                              compounding_risk_daily.complexity_norm, compounding_risk_daily.ownership_norm,
                              compounding_risk_daily.review_norm, compounding_risk_daily.rework_churn,
                              compounding_risk_daily.complexity_delta, compounding_risk_daily.bus_factor,
                              compounding_risk_daily.ownership_gini, compounding_risk_daily.single_owner_ratio,
                              compounding_risk_daily.review_latency_p90h, compounding_risk_daily.merged_prs_30d,
                              compounding_risk_daily.reviewed_prs_30d, compounding_risk_daily.review_share_30d
                          ), ` + compoundingRiskNewestOrder + `) AS latest_row,
                          tupleElement(latest_row, 2) AS w_churn,
                          tupleElement(latest_row, 3) AS w_complexity,
                          tupleElement(latest_row, 4) AS w_ownership,
                          tupleElement(latest_row, 5) AS w_review,
                          tupleElement(latest_row, 6) AS threshold_elevated,
                          tupleElement(latest_row, 7) AS threshold_high,
                          tupleElement(latest_row, 1) AS compounding_risk,
                          tupleElement(latest_row, 8) AS churn_norm,
                          tupleElement(latest_row, 9) AS complexity_norm,
                          tupleElement(latest_row, 10) AS ownership_norm,
                          tupleElement(latest_row, 11) AS review_norm,
                          tupleElement(latest_row, 12) AS rework_churn,
                          tupleElement(latest_row, 13) AS complexity_delta,
                          tupleElement(latest_row, 14) AS bus_factor,
                          tupleElement(latest_row, 15) AS ownership_gini,
                          tupleElement(latest_row, 16) AS single_owner_ratio,
                          tupleElement(latest_row, 17) AS review_latency_p90h,
                          tupleElement(latest_row, 18) AS merged_prs_30d,
                          tupleElement(latest_row, 19) AS reviewed_prs_30d,
                          tupleElement(latest_row, 20) AS review_share_30d
                      FROM compounding_risk_daily
                      WHERE org_id = {org_id:String}
                        AND day >= {start_day:Date}
                        AND day < {end_day:Date}
                      %s
                      GROUP BY day, scope, scope_id
                  ) AS newest
                  WHERE %s
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
	// measuredRisk keeps a scope id only when its picked newest row is a
	// measurement (package liverow). A retraction row over the key of a retired team id
	// has a NULL score: counted as a row it would make every day look
	// incomplete (missing_scores > 0) and the day picker would refuse the
	// newest day, and listed as a row it would be a signal with no score.
	measuredRisk := liverow.Predicate("compounding_risk_daily", "newest")
	query := fmt.Sprintf(compoundingRiskSQLBase, latestScopeFilter, measuredRisk)
	if scoped && len(f.Scope.IDs) > 0 {
		query += `
      AND scope = {scope:String}
      AND scope_id IN {scope_ids:Array(String)}
        `
	}
	query += `
        GROUP BY scope, scope_id
    ) AS newest
    WHERE ` + measuredRisk + `
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
		var coverage *float64
		if err := rows.Scan(&scope, &scopeID, &score, &severity, &latestComputedAt, &coverage); err != nil {
			return nil, fmt.Errorf("home: fetch_risk_signals scan: %w", err)
		}
		out = append(out, RiskRow{Scope: scope, ScopeID: scopeID, Score: score, Severity: severity, Coverage: coverage})
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

// resolveScopeLabels ports _resolve_scope_labels (services/home.py:647-713) onto
// the ONE name resolver of the Home prose (resolveScopeNames: repos/teams FINAL,
// a name equal to its id is no name). An id with no name is absent from the
// answer (CHAOS-9116: the id is never the label). A failed lookup is logged and
// leaves the labels out, as the Python warning did.
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
		for id, name := range resolveScopeNames(ctx, client, orgID, "repo", repoIDs, "home risk signals") {
			out[id] = name
		}
	}
	if len(teamIDs) > 0 {
		for id, name := range resolveScopeNames(ctx, client, orgID, "team", teamIDs, "home risk signals") {
			out[id] = name
		}
	}
	return out
}
