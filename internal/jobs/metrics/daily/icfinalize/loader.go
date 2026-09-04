package icfinalize

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// RollingStat is one row of the 30-day rolling window, one per identity.
type RollingStat struct {
	IdentityID      string
	TeamID          string
	ChurnLOC30d     float64
	DeliveryUnits30 float64
	CycleP5030dHrs  float64
	WIPMax30d       float64
}

// rollingWindowDays mirrors `start = as_of - timedelta(days=29)` — a 30-day
// window INCLUSIVE of as_of, not 30 days before it.
const rollingWindowDays = 29

// rollingStatsSQL ports ClickHouseDataLoader.load_user_metrics_rolling_30d
// (loaders/clickhouse.py:1851). It is the CLICKHOUSE loader, deliberately.
//
// A SQLAlchemy implementation of the same method exists at
// loaders/sqlalchemy.py:389 and DISAGREES semantically -- MAX(team_id) vs
// any(team_id), and AVG(cycle_p50_hours) vs median(cycle_p50_hours). AVG and
// median are different numbers, not different spellings. job_daily.py:242
// constructs ClickHouseDataLoader, so this one is live and the SQLAlchemy one
// is dead on this path; porting the wrong one would produce a plausible,
// wrong, and very hard to spot result.
//
// The FROM clause reproduces clickhouse_dedup.dedup_from("user_metrics_daily")
// exactly. user_metrics_daily is APPEND-ONLY (it is absent from
// RERUN_DEDUPED_DAILY_TABLES, which holds only work_item_metrics_daily and
// work_item_user_metrics_daily) so it gets the LIMIT 1 BY form rather than
// FINAL, keyed on _APPEND_ONLY_DAILY_KEYS' natural key
// (org_id, repo_id, author_email, day) — read from the current helper, never
// from a migration comment.
//
// any(team_id) is NON-DETERMINISTIC by definition: ClickHouse returns whichever
// value it reaches first. Two runs over identical data may assign a different
// team_id to the same identity, which then selects a different per-team
// normalization cohort downstream. Replicated rather than "fixed" — choosing a
// deterministic aggregate here would be a behaviour change wearing a
// determinism costume (team-lead's Q2 ruling).
const rollingStatsSQL = `
SELECT
    identity_id,
    any(team_id)                AS team_id,
    sum(loc_touched)            AS churn_loc_30d,
    sum(delivery_units)         AS delivery_units_30d,
    median(cycle_p50_hours)     AS cycle_p50_30d_hours,
    max(work_items_active)      AS wip_max_30d
FROM (
    SELECT *
    FROM user_metrics_daily
    ORDER BY computed_at DESC
    LIMIT 1 BY org_id, repo_id, author_email, day
) AS user_metrics_daily
WHERE day >= {start:Date} AND day <= {end:Date}
  AND org_id = {org_id:String}
GROUP BY identity_id`

// LoadRollingStats reads the 30-day rolling window for one organization.
//
// The Date and String parameters are bound as Go STRINGS, not as time.Time or
// a typed wrapper: clickhouse-go's typed-parameter binding does not round-trip
// {x:Date} the way the Python driver does, and a mismatch there fails at query
// time rather than at compile time.
func LoadRollingStats(
	ctx context.Context, conn driver.Conn, orgID string, asOf time.Time,
) ([]RollingStat, error) {
	end := asOf.UTC().Truncate(24 * time.Hour)
	start := end.AddDate(0, 0, -rollingWindowDays)

	rows, err := conn.Query(ctx, rollingStatsSQL,
		driver.Named("start", start.Format("2006-01-02")),
		driver.Named("end", end.Format("2006-01-02")),
		driver.Named("org_id", orgID),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var stats []RollingStat
	for rows.Next() {
		var stat RollingStat
		if err := rows.Scan(
			&stat.IdentityID, &stat.TeamID, &stat.ChurnLOC30d,
			&stat.DeliveryUnits30, &stat.CycleP5030dHrs, &stat.WIPMax30d,
		); err != nil {
			return nil, err
		}
		stats = append(stats, stat)
	}
	return stats, rows.Err()
}
