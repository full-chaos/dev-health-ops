package security

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graphqldate"
)

const topReposLimit = 10

// ResolveOverview returns the org's security posture: open-alert KPIs,
// the open-alert severity breakdown, the ten repos with the most open
// alerts, and a 30-day opened/fixed trend. All four read the same
// filtered alert set.
func ResolveOverview(ctx context.Context, client QueryClient, orgID string, filters *model.SecurityAlertFilterInput) (*model.SecurityOverview, error) {
	if client == nil {
		return nil, errors.New("security: clickhouse client is required")
	}
	f := buildFilter(orgID, filters)
	withOpen := append(append([]clickhouse.Binding{}, f.bindings...),
		clickhouse.Binding{Name: "open_states_agg", Value: openStates})

	kpis, err := readKpis(ctx, client, f, withOpen)
	if err != nil {
		return nil, err
	}
	breakdown, err := readBreakdown(ctx, client, f, withOpen)
	if err != nil {
		return nil, err
	}
	top, err := readTopRepos(ctx, client, f, withOpen)
	if err != nil {
		return nil, err
	}
	trend, err := readTrend(ctx, client, f)
	if err != nil {
		return nil, err
	}
	return &model.SecurityOverview{Kpis: kpis, SeverityBreakdown: breakdown, TopRepos: top, Trend: trend}, nil
}

func readKpis(ctx context.Context, client QueryClient, f filterClause, b []clickhouse.Binding) (*model.SecurityKpis, error) {
	query := `SELECT
    countIf(sa.state IN {open_states_agg:Array(String)}) AS open_total,
    countIf(sa.state IN {open_states_agg:Array(String)} AND sa.severity = 'critical') AS critical,
    countIf(sa.state IN {open_states_agg:Array(String)} AND sa.severity = 'high') AS high,
    avgIf(
        dateDiff('day', sa.created_at, sa.fixed_at),
        sa.fixed_at IS NOT NULL
        AND sa.fixed_at >= now() - INTERVAL 30 DAY
    ) AS mean_days_to_fix_30d,
    countIf(
        sa.state IN {open_states_agg:Array(String)}
        AND sa.created_at >= now() - INTERVAL 30 DAY
    ) - countIf(
        sa.state IN {open_states_agg:Array(String)}
        AND sa.created_at < now() - INTERVAL 30 DAY
        AND (sa.fixed_at IS NULL OR sa.fixed_at >= now() - INTERVAL 30 DAY)
        AND (sa.dismissed_at IS NULL OR sa.dismissed_at >= now() - INTERVAL 30 DAY)
    ) AS open_delta_30d
FROM security_alerts sa
INNER JOIN repos r ON sa.repo_id = r.id
` + f.sql
	rows, err := client.Query(ctx, query, b)
	if err != nil {
		return nil, fmt.Errorf("security: kpi query: %w", err)
	}
	defer rows.Close()
	k := &model.SecurityKpis{}
	if rows.Next() {
		var open, crit, high uint64
		var delta int64
		var mean *float64
		if err := rows.Scan(&open, &crit, &high, &mean, &delta); err != nil {
			return nil, fmt.Errorf("security: kpi scan: %w", err)
		}
		k.OpenTotal, k.Critical, k.High = int(open), int(crit), int(high)
		k.OpenDelta30d = int(delta)
		if mean != nil && !math.IsNaN(*mean) {
			v := *mean
			k.MeanDaysToFix30d = &v
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("security: kpi rows: %w", err)
	}
	return k, nil
}

func readBreakdown(ctx context.Context, client QueryClient, f filterClause, b []clickhouse.Binding) ([]model.SeverityBucket, error) {
	query := `SELECT
    coalesce(sa.severity, 'unknown') AS severity,
    count() AS count
FROM security_alerts sa
INNER JOIN repos r ON sa.repo_id = r.id
` + f.sql + `
AND sa.state IN {open_states_agg:Array(String)}
GROUP BY severity
ORDER BY count DESC`
	rows, err := client.Query(ctx, query, b)
	if err != nil {
		return nil, fmt.Errorf("security: breakdown query: %w", err)
	}
	defer rows.Close()
	out := []model.SeverityBucket{}
	for rows.Next() {
		var sev string
		var n uint64
		if err := rows.Scan(&sev, &n); err != nil {
			return nil, fmt.Errorf("security: breakdown scan: %w", err)
		}
		out = append(out, model.SeverityBucket{Severity: sev, Count: int(n)})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("security: breakdown rows: %w", err)
	}
	return out, nil
}

func readTopRepos(ctx context.Context, client QueryClient, f filterClause, b []clickhouse.Binding) ([]model.RepoAlertCount, error) {
	query := fmt.Sprintf(`SELECT
    toString(sa.repo_id) AS repo_id,
    r.repo AS repo_name,
    count() AS count
FROM security_alerts sa
INNER JOIN repos r ON sa.repo_id = r.id
%s
AND sa.state IN {open_states_agg:Array(String)}
GROUP BY sa.repo_id, r.repo
ORDER BY count DESC
LIMIT %d`, f.sql, topReposLimit)
	rows, err := client.Query(ctx, query, b)
	if err != nil {
		return nil, fmt.Errorf("security: top repos query: %w", err)
	}
	defer rows.Close()
	out := []model.RepoAlertCount{}
	for rows.Next() {
		var repoID, repoName string
		var n uint64
		if err := rows.Scan(&repoID, &repoName, &n); err != nil {
			return nil, fmt.Errorf("security: top repos scan: %w", err)
		}
		out = append(out, model.RepoAlertCount{RepoID: repoID, RepoName: repoName, Count: int(n)})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("security: top repos rows: %w", err)
	}
	return out, nil
}

func readTrend(ctx context.Context, client QueryClient, f filterClause) ([]model.TrendPoint, error) {
	query := `SELECT
    toDate(day) AS day,
    countIf(event_type = 'opened') AS opened,
    countIf(event_type = 'fixed') AS fixed
FROM (
    SELECT sa.created_at AS day, 'opened' AS event_type
    FROM security_alerts sa
    INNER JOIN repos r ON sa.repo_id = r.id
    ` + f.sql + `
    AND sa.created_at >= now() - INTERVAL 30 DAY
    UNION ALL
    SELECT sa.fixed_at AS day, 'fixed' AS event_type
    FROM security_alerts sa
    INNER JOIN repos r ON sa.repo_id = r.id
    ` + f.sql + `
    AND sa.fixed_at IS NOT NULL
    AND sa.fixed_at >= now() - INTERVAL 30 DAY
)
GROUP BY day
ORDER BY day ASC`
	rows, err := client.Query(ctx, query, f.bindings)
	if err != nil {
		return nil, fmt.Errorf("security: trend query: %w", err)
	}
	defer rows.Close()
	out := []model.TrendPoint{}
	for rows.Next() {
		var day time.Time
		var opened, fixed uint64
		if err := rows.Scan(&day, &opened, &fixed); err != nil {
			return nil, fmt.Errorf("security: trend scan: %w", err)
		}
		out = append(out, model.TrendPoint{Day: graphqldate.New(day), Opened: int(opened), Fixed: int(fixed)})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("security: trend rows: %w", err)
	}
	return out, nil
}
