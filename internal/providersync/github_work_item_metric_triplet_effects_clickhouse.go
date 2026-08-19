package providersync

import (
	"context"
	"reflect"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

type GitHubWorkItemMetricsDailyClickHouseEffects struct {
	Conn  driver.Conn
	Lease providerfoundation.LeaseGuard
}

type GitHubWorkItemUserMetricsDailyClickHouseEffects struct {
	Conn  driver.Conn
	Lease providerfoundation.LeaseGuard
}

type GitHubWorkItemCycleTimesClickHouseEffects struct {
	Conn  driver.Conn
	Lease providerfoundation.LeaseGuard
}

func (sink GitHubWorkItemMetricsDailyClickHouseEffects) WriteGitHubWorkItemEffect(
	ctx context.Context, identity GitHubWorkItemEffectIdentity, effect EffectBatch,
) error {
	rows, err := validateGitHubWorkItemMetricEffect[githubWorkItemMetricsDailyRow](identity, effect, githubWorkItemMetricsDailyDestination)
	if err != nil || ctx == nil || sink.Lease == nil {
		return ErrInvalidConfiguration
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return err
	}
	if len(rows) == 0 {
		return nil
	}
	if sink.Conn == nil {
		return ErrInvalidConfiguration
	}
	batch, err := sink.Conn.PrepareBatch(ctx, `INSERT INTO work_item_metrics_daily (
day, provider, work_scope_id, team_id, team_name, items_started, items_completed,
items_started_unassigned, items_completed_unassigned, wip_count_end_of_day,
wip_unassigned_end_of_day, cycle_time_p50_hours, cycle_time_p90_hours,
lead_time_p50_hours, lead_time_p90_hours, wip_age_p50_hours, wip_age_p90_hours,
bug_completed_ratio, story_points_completed, new_bugs_count, new_items_count,
defect_intro_rate, wip_congestion_ratio, predictability_score, computed_at, org_id)`)
	if err != nil {
		return err
	}
	defer batch.Abort()
	for _, row := range rows {
		day, err := row.Day.time()
		if err != nil {
			return ErrInvalidConfiguration
		}
		if err := batch.Append(
			day, row.Provider, row.WorkScopeID, row.TeamID, row.TeamName,
			row.ItemsStarted, row.ItemsCompleted, row.ItemsStartedUnassigned,
			row.ItemsCompletedUnassigned, row.WIPCountEndOfDay, row.WIPUnassignedEndOfDay,
			row.CycleTimeP50Hours, row.CycleTimeP90Hours, row.LeadTimeP50Hours,
			row.LeadTimeP90Hours, row.WIPAgeP50Hours, row.WIPAgeP90Hours,
			row.BugCompletedRatio, row.StoryPointsCompleted, row.NewBugsCount,
			row.NewItemsCount, row.DefectIntroRate, row.WIPCongestionRatio,
			row.PredictabilityScore, row.ComputedAt, row.OrgID,
		); err != nil {
			return err
		}
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return err
	}
	return batch.Send()
}

func (sink GitHubWorkItemMetricsDailyClickHouseEffects) InspectGitHubWorkItemEffect(
	ctx context.Context, identity GitHubWorkItemEffectIdentity, effect EffectBatch,
) (EffectInspection, error) {
	rows, err := validateGitHubWorkItemMetricEffect[githubWorkItemMetricsDailyRow](identity, effect, githubWorkItemMetricsDailyDestination)
	if err != nil || ctx == nil || sink.Lease == nil {
		return EffectConflict, ErrInvalidConfiguration
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return EffectConflict, err
	}
	if len(rows) == 0 {
		return EffectAbsent, nil
	}
	if sink.Conn == nil {
		return EffectConflict, ErrInvalidConfiguration
	}
	return inspectGitHubWorkItemMetricRows(rows, func(row githubWorkItemMetricsDailyRow) (EffectInspection, error) {
		return sink.inspect(ctx, row)
	})
}

func (sink GitHubWorkItemMetricsDailyClickHouseEffects) inspect(
	ctx context.Context, expected githubWorkItemMetricsDailyRow,
) (EffectInspection, error) {
	rows, err := sink.Conn.Query(ctx, `SELECT day, provider, work_scope_id, team_id, team_name,
items_started, items_completed, items_started_unassigned, items_completed_unassigned,
wip_count_end_of_day, wip_unassigned_end_of_day, cycle_time_p50_hours,
cycle_time_p90_hours, lead_time_p50_hours, lead_time_p90_hours, wip_age_p50_hours,
wip_age_p90_hours, bug_completed_ratio, story_points_completed, new_bugs_count,
new_items_count, defect_intro_rate, wip_congestion_ratio, predictability_score,
computed_at, org_id FROM work_item_metrics_daily FINAL
WHERE org_id = ? AND provider = ? AND day = ? AND work_scope_id = ? AND team_id = ?`,
		expected.OrgID, expected.Provider, string(expected.Day), expected.WorkScopeID, expected.TeamID)
	if err != nil {
		return EffectConflict, err
	}
	defer rows.Close()
	var actual githubWorkItemMetricsDailyRow
	found := 0
	for rows.Next() {
		var day time.Time
		// Every counter on this table is UInt32 (migration 001/002). The driver
		// refuses to scan an unsigned column into a Go int outright, so binding
		// the row fields directly makes EVERY readback fail as a transport
		// error -- never as a verdict -- and the effect can never be confirmed.
		var counters githubWorkItemMetricsDailyCounters
		if err := rows.Scan(
			&day, &actual.Provider, &actual.WorkScopeID, &actual.TeamID, &actual.TeamName,
			&counters.itemsStarted, &counters.itemsCompleted, &counters.itemsStartedUnassigned,
			&counters.itemsCompletedUnassigned, &counters.wipCountEndOfDay, &counters.wipUnassignedEndOfDay,
			&actual.CycleTimeP50Hours, &actual.CycleTimeP90Hours, &actual.LeadTimeP50Hours,
			&actual.LeadTimeP90Hours, &actual.WIPAgeP50Hours, &actual.WIPAgeP90Hours,
			&actual.BugCompletedRatio, &actual.StoryPointsCompleted, &counters.newBugsCount,
			&counters.newItemsCount, &actual.DefectIntroRate, &actual.WIPCongestionRatio,
			&actual.PredictabilityScore, &actual.ComputedAt, &actual.OrgID,
		); err != nil {
			return EffectConflict, err
		}
		counters.applyTo(&actual)
		actual.Day = newGitHubWorkItemMetricDay(day)
		found++
	}
	if err := rows.Err(); err != nil {
		return EffectConflict, err
	}
	return compareGitHubWorkItemMetricsDailyVersion(expected, actual, found), nil
}

func (sink GitHubWorkItemUserMetricsDailyClickHouseEffects) WriteGitHubWorkItemEffect(
	ctx context.Context, identity GitHubWorkItemEffectIdentity, effect EffectBatch,
) error {
	rows, err := validateGitHubWorkItemMetricEffect[githubWorkItemUserMetricsDailyRow](identity, effect, githubWorkItemUserMetricsDailyDestination)
	if err != nil || ctx == nil || sink.Lease == nil {
		return ErrInvalidConfiguration
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return err
	}
	if len(rows) == 0 {
		return nil
	}
	if sink.Conn == nil {
		return ErrInvalidConfiguration
	}
	batch, err := sink.Conn.PrepareBatch(ctx, `INSERT INTO work_item_user_metrics_daily (
day, provider, work_scope_id, user_identity, team_id, team_name, items_started,
items_completed, wip_count_end_of_day, cycle_time_p50_hours,
cycle_time_p90_hours, computed_at, org_id)`)
	if err != nil {
		return err
	}
	defer batch.Abort()
	for _, row := range rows {
		day, err := row.Day.time()
		if err != nil {
			return ErrInvalidConfiguration
		}
		if err := batch.Append(
			day, row.Provider, row.WorkScopeID, row.UserIdentity, row.TeamID,
			row.TeamName, row.ItemsStarted, row.ItemsCompleted, row.WIPCountEndOfDay,
			row.CycleTimeP50Hours, row.CycleTimeP90Hours, row.ComputedAt, row.OrgID,
		); err != nil {
			return err
		}
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return err
	}
	return batch.Send()
}

func (sink GitHubWorkItemUserMetricsDailyClickHouseEffects) InspectGitHubWorkItemEffect(
	ctx context.Context, identity GitHubWorkItemEffectIdentity, effect EffectBatch,
) (EffectInspection, error) {
	rows, err := validateGitHubWorkItemMetricEffect[githubWorkItemUserMetricsDailyRow](identity, effect, githubWorkItemUserMetricsDailyDestination)
	if err != nil || ctx == nil || sink.Lease == nil {
		return EffectConflict, ErrInvalidConfiguration
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return EffectConflict, err
	}
	if len(rows) == 0 {
		return EffectAbsent, nil
	}
	if sink.Conn == nil {
		return EffectConflict, ErrInvalidConfiguration
	}
	return inspectGitHubWorkItemMetricRows(rows, func(row githubWorkItemUserMetricsDailyRow) (EffectInspection, error) {
		return sink.inspect(ctx, row)
	})
}

func (sink GitHubWorkItemUserMetricsDailyClickHouseEffects) inspect(
	ctx context.Context, expected githubWorkItemUserMetricsDailyRow,
) (EffectInspection, error) {
	rows, err := sink.Conn.Query(ctx, `SELECT day, provider, work_scope_id, user_identity,
team_id, team_name, items_started, items_completed, wip_count_end_of_day,
cycle_time_p50_hours, cycle_time_p90_hours, computed_at, org_id
FROM work_item_user_metrics_daily FINAL
WHERE org_id = ? AND provider = ? AND work_scope_id = ? AND user_identity = ? AND day = ?`,
		expected.OrgID, expected.Provider, expected.WorkScopeID, expected.UserIdentity, string(expected.Day))
	if err != nil {
		return EffectConflict, err
	}
	defer rows.Close()
	var actual githubWorkItemUserMetricsDailyRow
	found := 0
	for rows.Next() {
		var day time.Time
		// UInt32 counters, same as the group rollup -- see the note there.
		var itemsStarted, itemsCompleted, wipCountEndOfDay uint32
		if err := rows.Scan(
			&day, &actual.Provider, &actual.WorkScopeID, &actual.UserIdentity,
			&actual.TeamID, &actual.TeamName, &itemsStarted, &itemsCompleted,
			&wipCountEndOfDay, &actual.CycleTimeP50Hours,
			&actual.CycleTimeP90Hours, &actual.ComputedAt, &actual.OrgID,
		); err != nil {
			return EffectConflict, err
		}
		actual.ItemsStarted = int(itemsStarted)
		actual.ItemsCompleted = int(itemsCompleted)
		actual.WIPCountEndOfDay = int(wipCountEndOfDay)
		actual.Day = newGitHubWorkItemMetricDay(day)
		found++
	}
	if err := rows.Err(); err != nil {
		return EffectConflict, err
	}
	return compareGitHubWorkItemUserMetricsDailyVersion(expected, actual, found), nil
}

func (sink GitHubWorkItemCycleTimesClickHouseEffects) WriteGitHubWorkItemEffect(
	ctx context.Context, identity GitHubWorkItemEffectIdentity, effect EffectBatch,
) error {
	rows, err := validateGitHubWorkItemMetricEffect[githubWorkItemCycleTimePersistenceRow](identity, effect, githubWorkItemCycleTimesDestination)
	if err != nil || ctx == nil || sink.Lease == nil {
		return ErrInvalidConfiguration
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return err
	}
	if len(rows) == 0 {
		return nil
	}
	if sink.Conn == nil {
		return ErrInvalidConfiguration
	}
	batch, err := sink.Conn.PrepareBatch(ctx, `INSERT INTO work_item_cycle_times (
work_item_id, provider, day, work_scope_id, team_id, team_name, assignee, type,
status, created_at, started_at, completed_at, cycle_time_hours, lead_time_hours,
computed_at, org_id)`)
	if err != nil {
		return err
	}
	defer batch.Abort()
	for _, row := range rows {
		day, err := row.Day.time()
		if err != nil {
			return ErrInvalidConfiguration
		}
		if err := batch.Append(
			row.WorkItemID, row.Provider, day, row.WorkScopeID, row.TeamID, row.TeamName,
			row.Assignee, row.Type, row.Status, row.CreatedAt, row.StartedAt,
			row.CompletedAt, row.CycleTimeHours, row.LeadTimeHours, row.ComputedAt, row.OrgID,
		); err != nil {
			return err
		}
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return err
	}
	return batch.Send()
}

func (sink GitHubWorkItemCycleTimesClickHouseEffects) InspectGitHubWorkItemEffect(
	ctx context.Context, identity GitHubWorkItemEffectIdentity, effect EffectBatch,
) (EffectInspection, error) {
	rows, err := validateGitHubWorkItemMetricEffect[githubWorkItemCycleTimePersistenceRow](identity, effect, githubWorkItemCycleTimesDestination)
	if err != nil || ctx == nil || sink.Lease == nil {
		return EffectConflict, ErrInvalidConfiguration
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return EffectConflict, err
	}
	if len(rows) == 0 {
		return EffectAbsent, nil
	}
	if sink.Conn == nil {
		return EffectConflict, ErrInvalidConfiguration
	}
	return inspectGitHubWorkItemMetricRows(rows, func(row githubWorkItemCycleTimePersistenceRow) (EffectInspection, error) {
		return sink.inspect(ctx, row)
	})
}

func (sink GitHubWorkItemCycleTimesClickHouseEffects) inspect(
	ctx context.Context, expected githubWorkItemCycleTimePersistenceRow,
) (EffectInspection, error) {
	// `day` is fenced even though it is NOT part of this table's sorting key
	// (org_id, provider, work_item_id). It is the PARTITION key --
	// toYYYYMM(day), migration 001 -- and whether FINAL collapses one natural
	// key across two partitions depends on a server setting this code does not
	// set and cannot see: measured on a real server, the default returns one
	// row (newest wins), while `do_not_merge_across_partitions_select_final = 1`
	// returns BOTH and lets the stale one be scanned last.
	//
	// That setting is an ordinary performance knob and can be enabled in a
	// server profile. A readback whose verdict depends on it is not a readback.
	// The other two destinations need nothing here: their partition column, day,
	// is already inside their sorting keys.
	//
	// Reachable whenever an item's completed_at moves across a month boundary
	// between generations, since day is derived from completed_at.
	rows, err := sink.Conn.Query(ctx, `SELECT work_item_id, provider, day, work_scope_id,
team_id, team_name, assignee, type, status, created_at, started_at, completed_at,
cycle_time_hours, lead_time_hours, computed_at, org_id
FROM work_item_cycle_times FINAL
WHERE org_id = ? AND provider = ? AND work_item_id = ? AND day = ?`,
		expected.OrgID, expected.Provider, expected.WorkItemID, string(expected.Day))
	if err != nil {
		return EffectConflict, err
	}
	defer rows.Close()
	var actual githubWorkItemCycleTimePersistenceRow
	found := 0
	for rows.Next() {
		var day time.Time
		// team_id and team_name are Nullable(String) on this table (migration
		// 001), unlike the two daily rollups. Production always writes them
		// normalized and non-null, but scanning a NULL straight into a Go
		// string errors instead of comparing, so a pre-existing legacy row
		// would turn a readback into a transport failure rather than the
		// EffectConflict it actually is. Scan through nullable locals.
		var teamID, teamName *string
		if err := rows.Scan(
			&actual.WorkItemID, &actual.Provider, &day, &actual.WorkScopeID,
			&teamID, &teamName, &actual.Assignee, &actual.Type, &actual.Status,
			&actual.CreatedAt, &actual.StartedAt, &actual.CompletedAt,
			&actual.CycleTimeHours, &actual.LeadTimeHours, &actual.ComputedAt, &actual.OrgID,
		); err != nil {
			return EffectConflict, err
		}
		actual.TeamID = githubWorkItemDerivationStringValue(teamID)
		actual.TeamName = githubWorkItemDerivationStringValue(teamName)
		actual.Day = newGitHubWorkItemMetricDay(day)
		found++
	}
	if err := rows.Err(); err != nil {
		return EffectConflict, err
	}
	return compareGitHubWorkItemCycleTimeVersion(expected, actual, found), nil
}

func validateGitHubWorkItemMetricEffect[T any](
	identity GitHubWorkItemEffectIdentity,
	effect EffectBatch,
	destination string,
) ([]T, error) {
	if strings.TrimSpace(identity.OrgID) == "" || !isDerivedWorkItemProvider(identity.Provider) ||
		!isWorkItemFamilyDataset(identity.Dataset) || strings.TrimSpace(identity.Generation) == "" ||
		identity.Destination != destination || effect.Destination != destination ||
		identity.ContentDigest != effect.ContentDigest || identity.RowCount != len(effect.Rows) ||
		effect.Recovery != EffectReadbackRequired {
		return nil, ErrInvalidConfiguration
	}
	rebuilt, err := BuildEffectBatch(effect.Destination, effect.Recovery, effect.Rows)
	if err != nil || rebuilt.ContentDigest != effect.ContentDigest || rebuilt.PayloadBytes != effect.PayloadBytes {
		return nil, ErrInvalidConfiguration
	}
	rows, err := decodeEffectRows[T](effect)
	if err != nil {
		return nil, err
	}
	seen := make(map[string]struct{}, len(rows))
	for _, generic := range rows {
		var key string
		switch row := any(generic).(type) {
		case githubWorkItemMetricsDailyRow:
			if !validGitHubWorkItemMetricsDailyRow(row, identity) {
				return nil, ErrInvalidConfiguration
			}
			key = strings.Join([]string{row.OrgID, row.Provider, string(row.Day), row.WorkScopeID, row.TeamID}, "\x00")
		case githubWorkItemUserMetricsDailyRow:
			if !validGitHubWorkItemUserMetricsDailyRow(row, identity) {
				return nil, ErrInvalidConfiguration
			}
			key = strings.Join([]string{row.OrgID, row.Provider, row.WorkScopeID, row.UserIdentity, string(row.Day)}, "\x00")
		case githubWorkItemCycleTimePersistenceRow:
			if !validGitHubWorkItemCycleTimeRow(row, identity) {
				return nil, ErrInvalidConfiguration
			}
			key = strings.Join([]string{row.OrgID, row.Provider, row.WorkItemID}, "\x00")
		default:
			return nil, ErrInvalidConfiguration
		}
		if _, duplicate := seen[key]; duplicate {
			return nil, ErrInvalidConfiguration
		}
		seen[key] = struct{}{}
	}
	return rows, nil
}

// The three row validators below deliberately carry NO finiteness check on
// their float columns. Every row reaching them was decoded by decodeEffectRows
// from an EffectBatch payload, and that payload is JSON: the format has no NaN
// or infinity literal, and a magnitude that would overflow float64 (`1e999`)
// fails to unmarshal rather than becoming +Inf. So a non-finite float cannot
// arrive here at all, and a guard for one would be an untestable branch that
// reads to a later reviewer as though the case were handled.
// TestGitHubWorkItemMetricEffectCannotCarryNonFiniteFloats pins that boundary
// property instead, where it is actually observable.
func validGitHubWorkItemMetricsDailyRow(row githubWorkItemMetricsDailyRow, identity GitHubWorkItemEffectIdentity) bool {
	return validMetricCommon(row.Day, row.Provider, row.TeamID, row.TeamName, row.ComputedAt, row.OrgID, identity) &&
		row.ItemsStarted >= 0 && row.ItemsCompleted >= 0 && row.ItemsStartedUnassigned >= 0 &&
		row.ItemsCompletedUnassigned >= 0 && row.WIPCountEndOfDay >= 0 &&
		row.WIPUnassignedEndOfDay >= 0 && row.NewBugsCount >= 0 && row.NewItemsCount >= 0
}

func validGitHubWorkItemUserMetricsDailyRow(row githubWorkItemUserMetricsDailyRow, identity GitHubWorkItemEffectIdentity) bool {
	return validMetricCommon(row.Day, row.Provider, row.TeamID, row.TeamName, row.ComputedAt, row.OrgID, identity) &&
		strings.TrimSpace(row.UserIdentity) != "" && row.ItemsStarted >= 0 &&
		row.ItemsCompleted >= 0 && row.WIPCountEndOfDay >= 0
}

func validGitHubWorkItemCycleTimeRow(row githubWorkItemCycleTimePersistenceRow, identity GitHubWorkItemEffectIdentity) bool {
	return validMetricCommon(row.Day, row.Provider, row.TeamID, row.TeamName, row.ComputedAt, row.OrgID, identity) &&
		strings.TrimSpace(row.WorkItemID) != "" && strings.TrimSpace(row.Type) != "" &&
		strings.TrimSpace(row.Status) != "" && !row.CreatedAt.IsZero() && row.CompletedAt != nil
}

func validMetricCommon(
	day githubWorkItemMetricDay, provider, teamID, teamName string,
	computedAt time.Time, orgID string, identity GitHubWorkItemEffectIdentity,
) bool {
	_, dayErr := day.time()
	return dayErr == nil && provider == identity.Provider && orgID == identity.OrgID &&
		strings.TrimSpace(teamID) != "" && strings.TrimSpace(teamName) != "" && !computedAt.IsZero()
}

func inspectGitHubWorkItemMetricRows[T any](
	rows []T, inspect func(T) (EffectInspection, error),
) (EffectInspection, error) {
	// A zero-row effect has written nothing, so it cannot be Exact. Without
	// this the exact == len(rows) test below is satisfied vacuously by
	// 0 == 0 and reports a verified write that never happened. The three
	// adapters short-circuit an empty effect before reaching here, so today
	// this is unreachable through them -- but that makes it a rail, not dead
	// code: a fourth caller would otherwise inherit the vacuous answer with
	// nothing pointing it out.
	if len(rows) == 0 {
		return EffectAbsent, nil
	}
	exact, absent := 0, 0
	for _, row := range rows {
		inspection, err := inspect(row)
		if err != nil {
			return EffectConflict, err
		}
		switch inspection {
		case EffectExact:
			exact++
		case EffectAbsent:
			absent++
		default:
			return EffectConflict, nil
		}
	}
	if exact == len(rows) {
		return EffectExact, nil
	}
	if absent == len(rows) {
		return EffectAbsent, nil
	}
	return EffectConflict, nil
}

// The three comparisons below all use reflect.DeepEqual rather than Go's `!=`
// on the struct. Every one of these rows carries nullable columns as POINTERS
// (*float64 percentiles, *time.Time instants, *string assignee), and `!=`
// compares a pointer by ADDRESS: the expected row comes from the effect payload
// and the actual row from a fresh scan, so their pointers are never the same
// allocation and a struct comparison would report Conflict for every row with a
// single non-null nullable column. That is a permanent, self-inflicted conflict
// -- the effect can never be verified, so the unit can never complete -- and it
// is invisible in a test that passes the same struct value as both sides,
// because then the pointers ARE identical. DeepEqual follows them.
func compareGitHubWorkItemMetricsDailyVersion(
	expected, actual githubWorkItemMetricsDailyRow, found int,
) EffectInspection {
	// found != 1 is tested FIRST, before anything looks at the scanned row.
	// The scan keeps only the LAST row it saw, so with more than one row the
	// verdict would otherwise be decided by whichever row the driver happened
	// to return last: a stale one reads as Absent and the committer rewrites
	// forever, a newer one reads as Conflict and the unit never completes.
	// Neither is a judgement about the effect; both are judgements about row
	// order. More than one row per natural key means the key or the query is
	// wrong, which is a conflict regardless of what the rows contain.
	if found != 1 {
		if found == 0 {
			return EffectAbsent
		}
		return EffectConflict
	}
	if actual.ComputedAt.IsZero() || actual.ComputedAt.Before(clickHouseSecond(expected.ComputedAt)) {
		return EffectAbsent
	}
	if actual.ComputedAt.After(clickHouseSecond(expected.ComputedAt)) {
		return EffectConflict
	}
	expected.ComputedAt = clickHouseSecond(expected.ComputedAt)
	actual.ComputedAt = clickHouseSecond(actual.ComputedAt)
	if !reflect.DeepEqual(actual, expected) {
		return EffectConflict
	}
	return EffectExact
}

func compareGitHubWorkItemUserMetricsDailyVersion(
	expected, actual githubWorkItemUserMetricsDailyRow, found int,
) EffectInspection {
	// found != 1 is tested FIRST, before anything looks at the scanned row.
	// The scan keeps only the LAST row it saw, so with more than one row the
	// verdict would otherwise be decided by whichever row the driver happened
	// to return last: a stale one reads as Absent and the committer rewrites
	// forever, a newer one reads as Conflict and the unit never completes.
	// Neither is a judgement about the effect; both are judgements about row
	// order. More than one row per natural key means the key or the query is
	// wrong, which is a conflict regardless of what the rows contain.
	if found != 1 {
		if found == 0 {
			return EffectAbsent
		}
		return EffectConflict
	}
	if actual.ComputedAt.IsZero() || actual.ComputedAt.Before(clickHouseSecond(expected.ComputedAt)) {
		return EffectAbsent
	}
	if actual.ComputedAt.After(clickHouseSecond(expected.ComputedAt)) {
		return EffectConflict
	}
	expected.ComputedAt = clickHouseSecond(expected.ComputedAt)
	actual.ComputedAt = clickHouseSecond(actual.ComputedAt)
	if !reflect.DeepEqual(actual, expected) {
		return EffectConflict
	}
	return EffectExact
}

func compareGitHubWorkItemCycleTimeVersion(
	expected, actual githubWorkItemCycleTimePersistenceRow, found int,
) EffectInspection {
	// found != 1 is tested FIRST, before anything looks at the scanned row.
	// The scan keeps only the LAST row it saw, so with more than one row the
	// verdict would otherwise be decided by whichever row the driver happened
	// to return last: a stale one reads as Absent and the committer rewrites
	// forever, a newer one reads as Conflict and the unit never completes.
	// Neither is a judgement about the effect; both are judgements about row
	// order. More than one row per natural key means the key or the query is
	// wrong, which is a conflict regardless of what the rows contain.
	if found != 1 {
		if found == 0 {
			return EffectAbsent
		}
		return EffectConflict
	}
	if actual.ComputedAt.IsZero() || actual.ComputedAt.Before(clickHouseSecond(expected.ComputedAt)) {
		return EffectAbsent
	}
	if actual.ComputedAt.After(clickHouseSecond(expected.ComputedAt)) {
		return EffectConflict
	}
	expected = normalizeGitHubWorkItemCycleTimeStorage(expected)
	actual = normalizeGitHubWorkItemCycleTimeStorage(actual)
	if !reflect.DeepEqual(actual, expected) {
		return EffectConflict
	}
	return EffectExact
}

func normalizeGitHubWorkItemCycleTimeStorage(row githubWorkItemCycleTimePersistenceRow) githubWorkItemCycleTimePersistenceRow {
	row.CreatedAt = clickHouseSecond(row.CreatedAt)
	row.StartedAt = clickHouseSecondPointer(row.StartedAt)
	row.CompletedAt = clickHouseSecondPointer(row.CompletedAt)
	row.ComputedAt = clickHouseSecond(row.ComputedAt)
	return row
}

// githubWorkItemMetricsDailyCounters is the scan target for the group rollup's
// eight UInt32 columns. It exists so the widening back to the compute type is
// one named step rather than eight assignments interleaved with the scan.
type githubWorkItemMetricsDailyCounters struct {
	itemsStarted, itemsCompleted                     uint32
	itemsStartedUnassigned, itemsCompletedUnassigned uint32
	wipCountEndOfDay, wipUnassignedEndOfDay          uint32
	newBugsCount, newItemsCount                      uint32
}

func (counters githubWorkItemMetricsDailyCounters) applyTo(row *githubWorkItemMetricsDailyRow) {
	row.ItemsStarted = int(counters.itemsStarted)
	row.ItemsCompleted = int(counters.itemsCompleted)
	row.ItemsStartedUnassigned = int(counters.itemsStartedUnassigned)
	row.ItemsCompletedUnassigned = int(counters.itemsCompletedUnassigned)
	row.WIPCountEndOfDay = int(counters.wipCountEndOfDay)
	row.WIPUnassignedEndOfDay = int(counters.wipUnassignedEndOfDay)
	row.NewBugsCount = int(counters.newBugsCount)
	row.NewItemsCount = int(counters.newItemsCount)
}

func clickHouseSecond(value time.Time) time.Time { return value.UTC().Truncate(time.Second) }

func clickHouseSecondPointer(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	result := clickHouseSecond(*value)
	return &result
}

var _ GitHubWorkItemEffectAdapter = GitHubWorkItemMetricsDailyClickHouseEffects{}
var _ GitHubWorkItemEffectAdapter = GitHubWorkItemUserMetricsDailyClickHouseEffects{}
var _ GitHubWorkItemEffectAdapter = GitHubWorkItemCycleTimesClickHouseEffects{}
