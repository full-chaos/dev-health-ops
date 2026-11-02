package icfinalize

import (
	"context"
	"sort"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/rowview"
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
// One point is one (team, person). Its numbers come from two arms with two
// rules for the team:
//
//   - rollingStatsGitArmSQL: the person's git rows. THE TEAM is the team that
//     OWNS the repository of a row (ruling D5985).
//   - rollingStatsWorkItemArmSQL: the person's work items. THE TEAM is the team
//     of the items (the ladder, CHAOS-12535).
//
// Neither arm reads the person's membership or the team id stored on a row.
// The work-item counts enter from the work-item arm only, so an item never
// counts on the owner of the person's repository too.
var rollingStatsSQL = `
SELECT
    owner_team,
    identity_id,
    sum(loc_touched)            AS churn_loc_30d,
    sum(delivery_units)         AS delivery_units_30d,
    ifNull(median(cycle_p50_hours), 0) AS cycle_p50_30d_hours,
    toUInt32(max(work_items_active)) AS wip_max_30d
FROM (` + rollingStatsGitArmSQL + `
    UNION ALL` + rollingStatsWorkItemArmSQL + `
)
GROUP BY owner_team, identity_id`

// rollingStatsGitArmSQL is the git half of a point: loc_touched, the merged
// pull requests (delivery_units less the work items ic_finalize added to the
// row, MergeICUserMetrics) and the cycle time. It gives no WIP.
//
// The FROM clause reproduces clickhouse_dedup.dedup_from("user_metrics_daily")
// exactly. user_metrics_daily is APPEND-ONLY (it is absent from
// RERUN_DEDUPED_DAILY_TABLES, which holds only work_item_metrics_daily and
// work_item_user_metrics_daily) so it gets the LIMIT 1 BY form rather than
// FINAL, keyed on _APPEND_ONLY_DAILY_KEYS' natural key
// (org_id, repo_id, author_email, day) — read from the current helper, never
// from a migration comment.
//
// A row of a repository owned by several teams counts in each of them (each
// team sees its own repositories whole, as a team scope of the query API
// does). A row of a repository no team owns is "unassigned". A row of the
// synthesized repository (a person with work items and no git row,
// SynthesizedRepoID) is no git fact and is left out: its work items are in the
// work-item arm. A blank or "unknown" identity is no person and keeps a blank
// team, as in ComputeLandscape; its rows join no owner (the join key is
// blank), so a row of a repository with several owners is still counted once.
// The owners come in as two parallel arrays (owner_repo_ids, owner_team_ids)
// from teamownership.OwnersByRepo.
var rollingStatsGitArmSQL = `
    SELECT
        u.identity_id AS identity_id,
        if(u.identity_id IN ('', 'unknown'), '',
           if(ow.owner_team_id = '', '` + unassignedTeamID + `', ow.owner_team_id)) AS owner_team,
        toUInt64(u.loc_touched) AS loc_touched,
        toUInt64(greatest(toInt64(u.delivery_units) - toInt64(u.work_items_completed), 0)) AS delivery_units,
        CAST(u.cycle_p50_hours, 'Nullable(Float64)') AS cycle_p50_hours,
        toUInt64(0) AS work_items_active
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
          AND NOT has({synthesized_repo_ids:Array(String)}, toString(repo_id))
    ) AS u
    LEFT JOIN (
        SELECT tupleElement(pair, 1) AS owner_repo_id, tupleElement(pair, 2) AS owner_team_id
        FROM (
            SELECT arrayJoin(arrayZip(
                {owner_repo_ids:Array(String)}, {owner_team_ids:Array(String)}
            )) AS pair
        )
    ) AS ow ON ow.owner_repo_id = if(u.identity_id IN ('', 'unknown'), '', toString(u.repo_id))`

// rollingStatsWorkItemArmSQL is the work-item half of a point, one row per
// (person, team, day): the completed items and the items open at the end of
// the day, summed over the person's providers and work scopes, from the team
// split of work_item_user_metrics_daily (the same rows ic_finalize folds into
// user_metrics_daily). work_item_user_metrics_daily is in
// RERUN_DEDUPED_DAILY_TABLES, so it takes FINAL, with the org view rows and the
// live rows of workItemMetricsSQL.
//
// The team of an entry is the team of the items (section 0.1: the primary
// team, unassigned for an item with none; section 0.4d: a co-owner team takes
// the item whole, as a co-owned repository row counts whole in each owner).
// What the primary entries of a row do not hold (a row written before the
// split existed holds none) is unassigned; a remainder of 0 gives no entry, so
// a person whose items are all in teams has no point on unassigned. A blank
// or "unknown" identity keeps a blank team, and "unassigned" (an item with no
// assignee) is no person of a team: its items stay on unassigned. Neither is a
// person a co-owner team ranks, so each item of theirs counts once, by its
// primary entry.
var rollingStatsWorkItemArmSQL = `
    SELECT
        identity_id,
        owner_team,
        toUInt64(0) AS loc_touched,
        toUInt64(sum(completed)) AS delivery_units,
        CAST(NULL, 'Nullable(Float64)') AS cycle_p50_hours,
        toUInt64(sum(wip)) AS work_items_active
    FROM (
        SELECT
            w.user_identity AS identity_id,
            w.day AS day,
            if(w.user_identity IN ('', 'unknown'), '',
               if(w.user_identity = '` + unassignedTeamID + `', '` + unassignedTeamID + `', tupleElement(split, 1))) AS owner_team,
            tupleElement(split, 3) AS completed,
            tupleElement(split, 4) AS wip
        FROM work_item_user_metrics_daily AS w FINAL
        ARRAY JOIN arrayConcat(
            arrayZip(w.team_split_team_ids, w.team_split_is_primary, w.team_split_items_completed, w.team_split_wip_end_of_day),
            arrayFilter(remainder -> tupleElement(remainder, 3) > 0 OR tupleElement(remainder, 4) > 0, [tuple(
                '` + unassignedTeamID + `',
                toUInt8(1),
                toUInt32(greatest(toInt64(w.items_completed) - toInt64(arraySum(arrayFilter(
                    (c, p) -> p = 1, w.team_split_items_completed, w.team_split_is_primary))), 0)),
                toUInt32(greatest(toInt64(w.wip_count_end_of_day) - toInt64(arraySum(arrayFilter(
                    (c, p) -> p = 1, w.team_split_wip_end_of_day, w.team_split_is_primary))), 0))
            )])
        ) AS split
        WHERE w.day >= {start:Date} AND w.day <= {end:Date}
          AND w.org_id = {org_id:String}
          AND ` + rowview.OrgPredicate("work_item_user_metrics_daily", "w") + `
          AND ` + liverow.Predicate("work_item_user_metrics_daily", "w") + `
          AND (tupleElement(split, 2) = 1 OR w.user_identity NOT IN ('', 'unknown', '` + unassignedTeamID + `'))
    )
    GROUP BY identity_id, owner_team, day`

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
	synthesized, err := synthesizedRepoIDsOfWindow(ctx, conn, orgID, start, end)
	if err != nil {
		return nil, err
	}

	rows, err := conn.Query(ctx, rollingStatsSQL,
		clickhouse.Named("start", start.Format("2006-01-02")),
		clickhouse.Named("end", end.Format("2006-01-02")),
		clickhouse.Named("org_id", orgID),
		clickhouse.Named("owner_repo_ids", ownerRepoIDs),
		clickhouse.Named("owner_team_ids", ownerTeamIDs),
		clickhouse.Named("synthesized_repo_ids", synthesized),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var stats []RollingStat
	for rows.Next() {
		var stat RollingStat
		// The arms carry loc_touched/delivery_units/work_items_active as UInt64,
		// so the two sums are UInt64, and wip_max_30d is cast to UInt32 (the
		// width of ic_landscape_rolling_30d's column) -- none of which clickhouse-go will
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

// synthesizedRepoIDsOfWindowSQL reads the people of the window's
// user_metrics_daily rows; each one's synthesized repository id is computed in
// Go (SynthesizedRepoID is a UUIDv5, which ClickHouse does not compute).
var synthesizedRepoIDsOfWindowSQL = `
SELECT DISTINCT author_email
FROM user_metrics_daily
WHERE org_id = {org_id:String} AND day >= {start:Date} AND day <= {end:Date}`

// synthesizedRepoIDsOfWindow is the synthesized repository id of every person
// of the window, sorted: the rows the git arm leaves out.
func synthesizedRepoIDsOfWindow(ctx context.Context, conn Conn, orgID string, start, end time.Time) ([]string, error) {
	rows, err := conn.Query(ctx, synthesizedRepoIDsOfWindowSQL,
		clickhouse.Named("org_id", orgID),
		clickhouse.Named("start", start.Format("2006-01-02")),
		clickhouse.Named("end", end.Format("2006-01-02")),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ids := []string{}
	for rows.Next() {
		var identity string
		if err := rows.Scan(&identity); err != nil {
			return nil, err
		}
		ids = append(ids, SynthesizedRepoID(orgID, identity).String())
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	sort.Strings(ids)
	return ids, nil
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
