package icfinalize

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"

	"github.com/full-chaos/dev-health-ops/internal/teamactive"
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
// THE TEAM OF A PERSON is a difference from the reference, which takes
// any(team_id): whichever stored value ClickHouse reaches first among the
// person's rows of the 30 days. Those rows are of many days and repositories,
// and a row of a day that was not computed again can hold the id of a team
// that was since replaced. any() could give the person that id, and a
// different id on the next run over the same rows. The statement takes the
// team of the person's NEWEST row instead (by computed_at; the day and the
// repository id break a tie, so two runs over the same rows agree), and
// LoadRollingStats then drops an inactive id from it.
const rollingStatsSQL = `
SELECT
    identity_id,
    argMax(team_id, tuple(computed_at, day, repo_id)) AS team_id,
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
// Named parameters come from clickhouse.Named (the top-level package), NOT
// driver.Named -- driver has no Named, and the mistake is a BUILD failure, not
// a runtime one, which is worth knowing because a build failure and a passing
// mutation proof are indistinguishable by exit code alone.
//
// The Date parameters bind as Go STRINGS, matching the fleet rule for typed CH
// params and the precedent this package already sets: repouser/clickhouse.go
// passes dateTimeArgument(t), a formatted string, for its own {start:DateTime}
// binds rather than a time.Time.
// It takes the package's narrow Conn rather than the clickhouse-go driver's
// full connection interface, which carries AsyncInsert and much else this never calls, and
// depending on it would force every caller -- including the executor, which
// holds the narrow one -- to supply capabilities it does not use.
//
// inactive is the organization's inactive team ids. A person whose newest row
// holds one gets no team here (the stored id names a team that was replaced):
// ComputeLandscape then gives the person the mapped team, or "unassigned".
func LoadRollingStats(
	ctx context.Context, conn Conn, orgID string, asOf time.Time, inactive teamactive.Inactive,
) ([]RollingStat, error) {
	end := asOf.UTC().Truncate(24 * time.Hour)
	start := end.AddDate(0, 0, -rollingWindowDays)

	rows, err := conn.Query(ctx, rollingStatsSQL,
		clickhouse.Named("start", start.Format("2006-01-02")),
		clickhouse.Named("end", end.Format("2006-01-02")),
		clickhouse.Named("org_id", orgID),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var stats []RollingStat
	for rows.Next() {
		var stat RollingStat
		// sum() over the UInt32 loc_touched/delivery_units columns (005_ic_metrics.sql)
		// promotes to UInt64 in ClickHouse regardless of either source column's or
		// ic_landscape_rolling_30d's own declared width, and max() over the UInt32
		// work_items_active column stays UInt32 -- none of which clickhouse-go will
		// scan into a plain float64 destination ("converting UInt64/UInt32 to
		// *float64 is unsupported"). Scan into the matching integer width, then
		// convert: RollingStat itself stays float64 because everything downstream
		// (ComputeLandscape's normalization) does float arithmetic on it.
		var churnLOC30d, deliveryUnits30 uint64
		var wipMax30d uint32
		if err := rows.Scan(
			&stat.IdentityID, &stat.TeamID, &churnLOC30d,
			&deliveryUnits30, &stat.CycleP5030dHrs, &wipMax30d,
		); err != nil {
			return nil, err
		}
		if inactive.Has(stat.TeamID) {
			stat.TeamID = ""
		}
		stat.ChurnLOC30d = float64(churnLOC30d)
		stat.DeliveryUnits30 = float64(deliveryUnits30)
		stat.WIPMax30d = float64(wipMax30d)
		stats = append(stats, stat)
	}
	return stats, rows.Err()
}
