// Package compoundingrisk serves the compoundingRisk GraphQL field from the
// append-only compounding_risk_daily table. It only reads: it surfaces the
// persisted composite, components, weights and thresholds exactly as they
// were computed and never recomputes a score.
//
// The org is always the caller's own org. A repository breakout reads the
// stored repo-scope rows; a team breakout reads the stored team-scope rows
// keyed by team id as they are, and only when no team row exists for the
// day does it derive team points from the repo rows, selecting each team's
// repositories through the shared team-ownership condition.
package compoundingrisk

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/full-chaos/dev-health-go/clickhouse"
)

// QueryClient is the narrow ClickHouse boundary this package needs.
type QueryClient interface {
	Query(ctx context.Context, statement string, bindings []clickhouse.Binding) (clickhouse.RowScanner, error)
}

const (
	// maxRows bounds the rows returned and the ids a filter may carry.
	maxRows = 500
	// maxTrendDays bounds the trend window.
	maxTrendDays = 365
	dateLayout   = "2006-01-02"
)

// storedRow is one latest-per-scope compounding_risk_daily row.
type storedRow struct {
	scopeID           string
	score             *float64
	severity          string
	churnNorm         *float64
	complexityNorm    *float64
	ownershipNorm     *float64
	reviewNorm        *float64
	reworkChurn       *float64
	complexityDelta   *float64
	busFactor         *float64
	ownershipGini     *float64
	singleOwnerRatio  *float64
	reviewLatencyP90h *float64
	wChurn            float64
	wComplexity       float64
	wOwnership        float64
	wReview           float64
	thresholdElevated float64
	thresholdHigh     float64
	computedAt        time.Time
}

// idList is an optional list of ids: nil is "no filter", an empty non-nil
// list is "the filter excludes everything".
type idList []string

func (l idList) excludesEverything() bool { return l != nil && len(l) == 0 }

func (l idList) bounded() []string {
	if len(l) > maxRows {
		return append([]string(nil), l[:maxRows]...)
	}
	return append([]string(nil), l...)
}

// repoIDFilter is the predicate that keeps stored repo rows whose scope id
// names a repository of the org by id or by full name.
const repoIDFilter = `
                  AND scope_id IN (
                      SELECT toString(id) FROM repos
                      WHERE org_id = {org_id:String}
                        AND (repo IN {repo_ids:Array(String)} OR toString(id) IN {repo_ids:Array(String)})
                  )
`

// latestDay returns the newest day in [start, end] holding at least one
// scored row for the scope, or nil.
func latestDay(ctx context.Context, client QueryClient, orgID, scope string, ids idList, start, end time.Time) (*time.Time, error) {
	if ids.excludesEverything() {
		return nil, nil
	}
	query := `
        SELECT maxOrNull(day) AS day
        FROM (
            SELECT
                day,
                countIf(tupleElement(latest_row, 1) IS NOT NULL) AS scored_rows
            FROM (
                SELECT
                    day,
                    scope_id,
                    argMax(tuple(compounding_risk), computed_at) AS latest_row
                FROM compounding_risk_daily
                WHERE org_id = {org_id:String}
                  AND scope = {scope:String}
                  AND day >= {start_day:Date}
                  AND day <= {end_day:Date}
    `
	bindings := []clickhouse.Binding{
		{Name: "org_id", Value: orgID},
		{Name: "scope", Value: scope},
		{Name: "start_day", Value: start.Format(dateLayout)},
		{Name: "end_day", Value: end.Format(dateLayout)},
	}
	if len(ids) > 0 {
		if scope == "repo" {
			query += repoIDFilter
			bindings = append(bindings, clickhouse.Binding{Name: "repo_ids", Value: ids.bounded()})
		} else {
			query += "\n                  AND scope_id IN {scope_ids:Array(String)}"
			bindings = append(bindings, clickhouse.Binding{Name: "scope_ids", Value: ids.bounded()})
		}
	}
	query += `
                GROUP BY day, scope_id
            )
            GROUP BY day
        )
        WHERE scored_rows > 0
    `
	rs, err := client.Query(ctx, query, bindings)
	if err != nil {
		return nil, fmt.Errorf("compoundingrisk: latest day query: %w", err)
	}
	defer rs.Close()
	var out *time.Time
	if rs.Next() {
		var day *time.Time
		if err := rs.Scan(&day); err != nil {
			return nil, fmt.Errorf("compoundingrisk: latest day scan: %w", err)
		}
		out = day
	}
	if err := rs.Err(); err != nil {
		return nil, fmt.Errorf("compoundingrisk: latest day rows: %w", err)
	}
	return out, nil
}

// latestRows returns the latest computed row per scope id on one day,
// ordered by score descending with unscored rows last, capped at maxRows.
func latestRows(ctx context.Context, client QueryClient, orgID string, day time.Time, scope string, ids idList) ([]storedRow, error) {
	if ids.excludesEverything() {
		return nil, nil
	}
	query := `
        SELECT
            scope_id,
            tupleElement(latest_row, 1)  AS score,
            tupleElement(latest_row, 2)  AS severity,
            tupleElement(latest_row, 3)  AS churn_norm,
            tupleElement(latest_row, 4)  AS complexity_norm,
            tupleElement(latest_row, 5)  AS ownership_norm,
            tupleElement(latest_row, 6)  AS review_norm,
            tupleElement(latest_row, 7)  AS rework_churn,
            tupleElement(latest_row, 8)  AS complexity_delta,
            tupleElement(latest_row, 9)  AS bus_factor,
            tupleElement(latest_row, 10) AS ownership_gini,
            tupleElement(latest_row, 11) AS single_owner_ratio,
            tupleElement(latest_row, 12) AS review_latency_p90h,
            tupleElement(latest_row, 13) AS w_churn,
            tupleElement(latest_row, 14) AS w_complexity,
            tupleElement(latest_row, 15) AS w_ownership,
            tupleElement(latest_row, 16) AS w_review,
            tupleElement(latest_row, 17) AS threshold_elevated,
            tupleElement(latest_row, 18) AS threshold_high,
            tupleElement(latest_row, 19) AS latest_computed_at
        FROM (
            SELECT
                scope_id,
                argMax(
                    tuple(
                        compounding_risk,
                        severity,
                        churn_norm,
                        complexity_norm,
                        ownership_norm,
                        review_norm,
                        rework_churn,
                        complexity_delta,
                        bus_factor,
                        ownership_gini,
                        single_owner_ratio,
                        review_latency_p90h,
                        w_churn,
                        w_complexity,
                        w_ownership,
                        w_review,
                        threshold_elevated,
                        threshold_high,
                        computed_at
                    ),
                    computed_at
                ) AS latest_row
            FROM compounding_risk_daily
            WHERE org_id = {org_id:String}
              AND scope = {scope:String}
              AND day = {day:Date}
    `
	bindings := []clickhouse.Binding{
		{Name: "org_id", Value: orgID},
		{Name: "day", Value: day.Format(dateLayout)},
		{Name: "scope", Value: scope},
	}
	if len(ids) > 0 {
		if scope == "repo" {
			query += repoIDFilter
			bindings = append(bindings, clickhouse.Binding{Name: "repo_ids", Value: ids.bounded()})
		} else {
			query += "\n  AND scope_id IN {scope_ids:Array(String)}"
			bindings = append(bindings, clickhouse.Binding{Name: "scope_ids", Value: ids.bounded()})
		}
	}
	query += fmt.Sprintf("\n    GROUP BY scope_id\n)\nORDER BY score DESC NULLS LAST\nLIMIT %d", maxRows)

	rs, err := client.Query(ctx, query, bindings)
	if err != nil {
		return nil, fmt.Errorf("compoundingrisk: latest rows query: %w", err)
	}
	defer rs.Close()
	var out []storedRow
	for rs.Next() {
		var r storedRow
		if err := rs.Scan(&r.scopeID, &r.score, &r.severity, &r.churnNorm, &r.complexityNorm, &r.ownershipNorm,
			&r.reviewNorm, &r.reworkChurn, &r.complexityDelta, &r.busFactor, &r.ownershipGini,
			&r.singleOwnerRatio, &r.reviewLatencyP90h, &r.wChurn, &r.wComplexity, &r.wOwnership,
			&r.wReview, &r.thresholdElevated, &r.thresholdHigh, &r.computedAt); err != nil {
			return nil, fmt.Errorf("compoundingrisk: latest rows scan: %w", err)
		}
		out = append(out, r)
	}
	if err := rs.Err(); err != nil {
		return nil, fmt.Errorf("compoundingrisk: latest rows rows: %w", err)
	}
	return out, nil
}

// trendRow is one day of the mean of the latest repo scores.
type trendRow struct {
	day      time.Time
	avgScore *float64
}

// repoTrend returns, per day of [end-(days-1), end], the mean over repos of
// the latest score computed that day.
func repoTrend(ctx context.Context, client QueryClient, orgID string, end time.Time, days int, ids idList) ([]trendRow, error) {
	if ids.excludesEverything() {
		return nil, nil
	}
	if days < 1 {
		days = 1
	}
	start := end.AddDate(0, 0, -(days - 1))
	query := `
        SELECT day,
               avg(score) AS avg_score
        FROM (
            SELECT
                day,
                scope_id,
                tupleElement(argMax(tuple(compounding_risk), computed_at), 1) AS score
            FROM compounding_risk_daily
            WHERE org_id = {org_id:String}
              AND scope = 'repo'
              AND day >= {start:Date} AND day <= {end:Date}
    `
	bindings := []clickhouse.Binding{
		{Name: "org_id", Value: orgID},
		{Name: "start", Value: start.Format(dateLayout)},
		{Name: "end", Value: end.Format(dateLayout)},
	}
	if len(ids) > 0 {
		query += `
              AND scope_id IN (
                  SELECT toString(id) FROM repos
                  WHERE org_id = {org_id:String}
                    AND (repo IN {repo_ids:Array(String)} OR toString(id) IN {repo_ids:Array(String)})
              )`
		bindings = append(bindings, clickhouse.Binding{Name: "repo_ids", Value: ids.bounded()})
	}
	query += `
            GROUP BY day, scope_id
        )
        GROUP BY day ORDER BY day
    `
	rs, err := client.Query(ctx, query, bindings)
	if err != nil {
		return nil, fmt.Errorf("compoundingrisk: trend query: %w", err)
	}
	defer rs.Close()
	var out []trendRow
	for rs.Next() {
		var t trendRow
		if err := rs.Scan(&t.day, &t.avgScore); err != nil {
			return nil, fmt.Errorf("compoundingrisk: trend scan: %w", err)
		}
		out = append(out, t)
	}
	if err := rs.Err(); err != nil {
		return nil, fmt.Errorf("compoundingrisk: trend rows: %w", err)
	}
	return out, nil
}

// repoLabels maps repo ids to their full names; an empty name reads as the id.
func repoLabels(ctx context.Context, client QueryClient, orgID string, ids []string) (map[string]string, error) {
	out := map[string]string{}
	if len(ids) == 0 {
		return out, nil
	}
	rs, err := client.Query(ctx, `
        SELECT toString(id) AS repo_id, repo AS full_name
        FROM repos
        WHERE org_id = {org_id:String}
          AND toString(id) IN {repo_ids:Array(String)}
        `, []clickhouse.Binding{{Name: "org_id", Value: orgID}, {Name: "repo_ids", Value: ids}})
	if err != nil {
		return nil, fmt.Errorf("compoundingrisk: repo labels query: %w", err)
	}
	defer rs.Close()
	for rs.Next() {
		var id, name string
		if err := rs.Scan(&id, &name); err != nil {
			return nil, fmt.Errorf("compoundingrisk: repo labels scan: %w", err)
		}
		if name == "" {
			name = id
		}
		out[id] = name
	}
	if err := rs.Err(); err != nil {
		return nil, fmt.Errorf("compoundingrisk: repo labels rows: %w", err)
	}
	return out, nil
}

// teamLabels maps team ids to their names. The lookup is best effort: when
// the teams table cannot be read the labels are empty and every team reads
// as its id, so a broken catalog never fails the request.
func teamLabels(ctx context.Context, client QueryClient, orgID string) (map[string]string, []string) {
	labels := map[string]string{}
	var order []string
	rs, err := client.Query(ctx, `
            SELECT id, name
            FROM teams
            WHERE org_id = {org_id:String}
            `, []clickhouse.Binding{{Name: "org_id", Value: orgID}})
	if err != nil {
		log.Printf("compoundingrisk: could not load teams: %v", err)
		return map[string]string{}, nil
	}
	defer rs.Close()
	for rs.Next() {
		var id, name string
		if err := rs.Scan(&id, &name); err != nil {
			log.Printf("compoundingrisk: could not read a team row: %v", err)
			return map[string]string{}, nil
		}
		if _, seen := labels[id]; !seen {
			order = append(order, id)
		}
		if name == "" {
			name = id
		}
		labels[id] = name
	}
	if err := rs.Err(); err != nil {
		log.Printf("compoundingrisk: could not read teams: %v", err)
		return map[string]string{}, nil
	}
	return labels, order
}

var errNoClient = errors.New("compoundingrisk: clickhouse client is required")
