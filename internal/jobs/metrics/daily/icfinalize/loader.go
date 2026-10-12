package icfinalize

import (
	"context"
	"sort"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"

	"github.com/full-chaos/dev-health-ops/internal/storage/clickhouse/liverow"
)

// RollingStat is one row of the 30-day rolling window, one per (owning team,
// identity).
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
// THE TEAM is the team that OWNS the repository of a row (ruling D5985), never
// the person's membership and never the team id stored on the row: a person's
// numbers in a team are the person's rows in the repositories the team owns.
// A row of a repository owned by several teams counts in each of them (each
// team sees its own repositories whole, as a team scope of the query API
// does). A row of a repository no team owns, and a row of a person with no
// git row (the synthesized repository, which no team owns), is "unassigned".
// A blank or "unknown" identity is no person and keeps a blank team, as in
// ComputeLandscape; its rows join no owner (the join key is blank), so a row of
// a repository with several owners is still counted once. The owners come in as two parallel arrays (owner_repo_ids,
// owner_team_ids) from teamownership.OwnersByRepo.
var rollingStatsSQL = `
SELECT
    owner_team,
    identity_id,
    sum(loc_touched)            AS churn_loc_30d,
    sum(delivery_units)         AS delivery_units_30d,
    median(cycle_p50_hours)     AS cycle_p50_30d_hours,
    max(work_items_active)      AS wip_max_30d
FROM (
    SELECT
        u.identity_id AS identity_id,
        if(u.identity_id IN ('', 'unknown'), '',
           if(ow.owner_team_id = '', '` + unassignedTeamID + `', ow.owner_team_id)) AS owner_team,
        u.loc_touched AS loc_touched,
        u.delivery_units AS delivery_units,
        u.cycle_p50_hours AS cycle_p50_hours,
        u.work_items_active AS work_items_active
    FROM (
        SELECT *
        FROM (
            SELECT *
            FROM user_metrics_daily
            ORDER BY computed_at DESC
            LIMIT 1 BY org_id, repo_id, author_email, day
        ) AS user_metrics_daily
        WHERE day >= {start:Date} AND day <= {end:Date}
          AND org_id = {org_id:String}
          AND ` + liverow.Predicate("user_metrics_daily", "user_metrics_daily") + `
    ) AS u
    LEFT JOIN (
        SELECT tupleElement(pair, 1) AS owner_repo_id, tupleElement(pair, 2) AS owner_team_id
        FROM (
            SELECT arrayJoin(arrayZip(
                {owner_repo_ids:Array(String)}, {owner_team_ids:Array(String)}
            )) AS pair
        )
    ) AS ow ON ow.owner_repo_id = if(u.identity_id IN ('', 'unknown'), '', toString(u.repo_id))
)
GROUP BY owner_team, identity_id`

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
// owners is teamownership.OwnersByRepo as of the end of asOf's day: the active
// owning teams of each repository (an inactive team owns nothing).
func LoadRollingStats(
	ctx context.Context, conn Conn, orgID string, asOf time.Time, owners map[string][]string,
) ([]RollingStat, error) {
	end := asOf.UTC().Truncate(24 * time.Hour)
	start := end.AddDate(0, 0, -rollingWindowDays)
	ownerRepoIDs, ownerTeamIDs := ownerPairs(owners)

	rows, err := conn.Query(ctx, rollingStatsSQL,
		clickhouse.Named("start", start.Format("2006-01-02")),
		clickhouse.Named("end", end.Format("2006-01-02")),
		clickhouse.Named("org_id", orgID),
		clickhouse.Named("owner_repo_ids", ownerRepoIDs),
		clickhouse.Named("owner_team_ids", ownerTeamIDs),
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
			&stat.TeamID, &stat.IdentityID, &churnLOC30d,
			&deliveryUnits30, &stat.CycleP5030dHrs, &wipMax30d,
		); err != nil {
			return nil, err
		}
		stat.ChurnLOC30d = float64(churnLOC30d)
		stat.DeliveryUnits30 = float64(deliveryUnits30)
		stat.WIPMax30d = float64(wipMax30d)
		stats = append(stats, stat)
	}
	return stats, rows.Err()
}

// ownerPairs flattens the owners of each repository into the two parallel
// arrays rollingStatsSQL joins on, in repository then team order.
func ownerPairs(owners map[string][]string) (repoIDs, teamIDs []string) {
	keys := make([]string, 0, len(owners))
	for repoID := range owners {
		keys = append(keys, repoID)
	}
	sort.Strings(keys)
	repoIDs, teamIDs = []string{}, []string{}
	for _, repoID := range keys {
		for _, teamID := range owners[repoID] {
			repoIDs = append(repoIDs, repoID)
			teamIDs = append(teamIDs, teamID)
		}
	}
	return repoIDs, teamIDs
}
