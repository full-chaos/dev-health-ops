package icfinalize

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

// FamilyName is the families.json family this package computes, and it is the
// SINGLE source of truth for the string on the Go side.
//
// The same literal appears in Python, at run_daily_metrics_finalize's gate
// (`if "ic_finalize" not in skip_families`). Those two must agree or the
// mechanism silently produces TWO writers: Go computes and writes, Python does
// not recognise its key, recomputes, and its rows supersede via
// `computed_at DESC LIMIT 1 BY`. Nothing errors and nothing is red.
//
// The agreement is asserted by a test that reads families.json and the Python
// source, with a negative control -- see executor_test.go. It cannot be left
// to review: the two-writer integration test registers the family under the
// literal it also asserts on, so it CANNOT catch a mismatch.
const FamilyName = "ic_finalize"

// Conn is the narrow ClickHouse capability this package needs -- query plus
// batch insert, matching the shape repouser already depends on (driver.Conn
// satisfies it directly).
type Conn interface {
	Query(ctx context.Context, query string, args ...any) (driver.Rows, error)
	PrepareBatch(ctx context.Context, query string, opts ...driver.PrepareBatchOption) (driver.Batch, error)
}

// gitMetricsSQL reads back what the partitions wrote for the target day. It
// goes through the same dedup form as the rolling loader, for the same reason:
// user_metrics_daily is append-only, so a raw read mixes superseded
// generations.
const gitMetricsSQL = `
SELECT author_email, team_id, loc_added, loc_deleted, prs_authored, prs_merged,
       median_pr_cycle_hours, pr_cycle_p90_hours
FROM (
    SELECT *
    FROM user_metrics_daily
    ORDER BY computed_at DESC
    LIMIT 1 BY org_id, repo_id, author_email, day
) AS user_metrics_daily
WHERE day = {day:Date} AND org_id = {org_id:String}`

// workItemMetricsSQL mirrors run_daily_metrics_finalize's own readback, which
// uses FINAL rather than LIMIT 1 BY: work_item_user_metrics_daily IS in
// RERUN_DEDUPED_DAILY_TABLES, so it is a ReplacingMergeTree and takes the RMT
// form. The two tables genuinely differ; using one form for both would be
// wrong for whichever it did not fit.
const workItemMetricsSQL = `
SELECT user_identity, provider, work_scope_id, team_id, team_name,
       items_started, items_completed, wip_count_end_of_day,
       cycle_time_p50_hours, cycle_time_p90_hours
FROM work_item_user_metrics_daily FINAL
WHERE day = {day:Date} AND org_id = {org_id:String}`

// Executor computes the ic_finalize family natively.
type Executor struct {
	conn       Conn
	now        func() time.Time
	newID      func() uuid.UUID
	teamMapper TeamMapper
}

// NewExecutor builds the executor. now and newID are injected so the two
// non-deterministic values the reference produces -- computed_at and the
// synthesized repo_id -- are controllable in tests rather than ambient.
func NewExecutor(conn Conn) *Executor {
	return &Executor{conn: conn, now: func() time.Time { return time.Now().UTC() }, newID: uuid.New}
}

func (executor *Executor) loadGitMetrics(ctx context.Context, orgID string, day time.Time) ([]GitUserMetric, error) {
	rows, err := executor.conn.Query(ctx, gitMetricsSQL,
		clickhouse.Named("day", day.UTC().Format("2006-01-02")),
		clickhouse.Named("org_id", orgID))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var metrics []GitUserMetric
	for rows.Next() {
		var metric GitUserMetric
		if err := rows.Scan(&metric.AuthorEmail, &metric.TeamID, &metric.LOCAdded,
			&metric.LOCDeleted, &metric.PRsAuthored, &metric.PRsMerged,
			&metric.MedianPRCycleHours, &metric.PRCycleP90Hours); err != nil {
			return nil, err
		}
		metrics = append(metrics, metric)
	}
	return metrics, rows.Err()
}

func (executor *Executor) loadWorkItemMetrics(ctx context.Context, orgID string, day time.Time) ([]WorkItemUserMetric, error) {
	rows, err := executor.conn.Query(ctx, workItemMetricsSQL,
		clickhouse.Named("day", day.UTC().Format("2006-01-02")),
		clickhouse.Named("org_id", orgID))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var metrics []WorkItemUserMetric
	for rows.Next() {
		var metric WorkItemUserMetric
		if err := rows.Scan(&metric.UserIdentity, &metric.Provider, &metric.WorkScopeID,
			&metric.TeamID, &metric.TeamName, &metric.ItemsStarted, &metric.ItemsCompleted,
			&metric.WIPCountEndOfDay, &metric.CycleTimeP50Hrs, &metric.CycleTimeP90Hrs); err != nil {
			return nil, err
		}
		metrics = append(metrics, metric)
	}
	return metrics, rows.Err()
}

const userMetricsInsertSQL = `INSERT INTO user_metrics_daily (
    repo_id, day, author_email, identity_id, team_id, loc_touched,
    prs_opened, work_items_completed, work_items_active, delivery_units,
    cycle_p50_hours, cycle_p90_hours, computed_at, org_id)`

const landscapeInsertSQL = `INSERT INTO ic_landscape_rolling_30d (
    repo_id, as_of_day, identity_id, team_id, map_name,
    x_raw, y_raw, x_norm, y_norm,
    churn_loc_30d, delivery_units_30d, cycle_p50_30d_hours, wip_max_30d,
    computed_at, org_id)`

// landscapeRepoID is the all-zeros UUID compute_ic_landscape_rolling writes
// for every landscape row (`repo_id=uuid.UUID(int=0)`), under a comment in the
// reference that openly doubts itself ("Placeholder, landscape is cross-repo
// usually?"). It is part of ic_landscape_rolling_30d's sorting key, but being
// CONSTANT it contributes nothing to uniqueness -- unlike the synthesized
// repo_id on the user-metrics side, which is random and therefore breaks
// dedup. Replicated exactly per the Q5 ruling; recorded in RISK-NOTES rather
// than "improved".
var landscapeRepoID = uuid.UUID{}

// computeForDay is the real work, taking its scope explicitly so tests can
// drive it without constructing a daily.Run.
//
// It reads back what the partitions wrote for the day, merges git and
// work-item metrics, writes the merged user rows, then reads the 30-day
// rolling window (which includes what it just wrote, exactly as the Python
// sequence does) and writes the landscape rows.
//
// The ordering is load-bearing and mirrors run_daily_metrics_finalize: the
// landscape input is a READBACK of the user-metrics write, so the two halves
// cannot be reordered or run independently.
func (executor *Executor) computeForDay(
	ctx context.Context, orgID string, day time.Time, teamMap map[string]string,
) (int, error) {
	gitMetrics, err := executor.loadGitMetrics(ctx, orgID, day)
	if err != nil {
		return 0, err
	}
	workItems, err := executor.loadWorkItemMetrics(ctx, orgID, day)
	if err != nil {
		return 0, err
	}

	merged := MergeICUserMetrics(gitMetrics, workItems, teamMap)
	computedAt := executor.now()
	written, err := executor.writeUserMetrics(ctx, orgID, day, computedAt, merged)
	if err != nil {
		return 0, err
	}

	stats, err := LoadRollingStats(ctx, executor.conn, orgID, day)
	if err != nil {
		return written, err
	}
	landscapeWritten, err := executor.writeLandscape(
		ctx, orgID, day, computedAt, ComputeLandscape(stats, teamMap))
	if err != nil {
		return written, err
	}
	return written + landscapeWritten, nil
}

func (executor *Executor) writeUserMetrics(
	ctx context.Context, orgID string, day, computedAt time.Time, metrics []ICUserMetric,
) (int, error) {
	if len(metrics) == 0 {
		return 0, nil
	}
	batch, err := executor.conn.PrepareBatch(ctx, userMetricsInsertSQL)
	if err != nil {
		return 0, err
	}
	for _, metric := range metrics {
		// The reference mints a FRESH random repo_id for an identity with no
		// git record. That UUID is part of this table's dedup key
		// (org_id, repo_id, author_email, day), so those rows never collapse
		// across re-runs -- each re-drive appends another. Replicated per Q1;
		// the suspected accumulation defect is filed, Python untouched.
		repoID := landscapeRepoID
		if metric.SynthesizedRepoID {
			repoID = executor.newID()
		}
		if err := batch.Append(
			repoID, day, metric.IdentityID, metric.IdentityID, metric.TeamID,
			metric.LOCTouched, metric.PRsOpened, metric.WorkItemsComplete,
			metric.WorkItemsActive, metric.DeliveryUnits,
			metric.CycleP50Hours, metric.CycleP90Hours, computedAt, orgID,
		); err != nil {
			return 0, err
		}
	}
	if err := batch.Send(); err != nil {
		return 0, err
	}
	return len(metrics), nil
}

func (executor *Executor) writeLandscape(
	ctx context.Context, orgID string, asOf, computedAt time.Time, records []LandscapeRecord,
) (int, error) {
	if len(records) == 0 {
		return 0, nil
	}
	batch, err := executor.conn.PrepareBatch(ctx, landscapeInsertSQL)
	if err != nil {
		return 0, err
	}
	for _, record := range records {
		if err := batch.Append(
			landscapeRepoID, asOf, record.IdentityID, record.TeamID, record.MapName,
			record.XRaw, record.YRaw, record.XNorm, record.YNorm,
			record.Churn, record.Delivery, record.CycleP50, record.WIPMax,
			computedAt, orgID,
		); err != nil {
			return 0, err
		}
	}
	if err := batch.Send(); err != nil {
		return 0, err
	}
	return len(records), nil
}

// TeamMapper resolves identity -> team, mirroring Python's load_team_map().
// Injected rather than read here so the executor has one reason to fail.
type TeamMapper func(ctx context.Context) (map[string]string, error)

// SetTeamMapper wires the resolver. A nil mapper means an empty map, which is
// the reference's behaviour when load_team_map() returns nothing: identities
// fall through to "unassigned" rather than the family failing.
func (executor *Executor) SetTeamMapper(mapper TeamMapper) { executor.teamMapper = mapper }

// ComputeFinalizeFamily implements daily.NativeFinalizeFamilyExecutor.
//
// The interface is deliberately run-scoped rather than day-scoped: the run
// carries both the organization and the target day, and taking them from ONE
// place removes any chance of computing a day for the wrong org. computeForDay
// keeps the explicit form for tests.
func (executor *Executor) ComputeFinalizeFamily(ctx context.Context, run RunScope) (int, error) {
	teamMap := map[string]string{}
	if executor.teamMapper != nil {
		resolved, err := executor.teamMapper(ctx)
		if err != nil {
			return 0, err
		}
		teamMap = resolved
	}
	return executor.computeForDay(ctx, run.OrganizationID, run.TargetDay, teamMap)
}

// RunScope is the subset of daily.Run this package needs. Declaring it here
// rather than importing daily keeps the dependency pointing one way -- daily
// registers icfinalize, not the reverse -- and avoids an import cycle.
type RunScope struct {
	OrganizationID string
	TargetDay      time.Time
}
