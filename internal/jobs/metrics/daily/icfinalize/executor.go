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

// synthesizedRepoNamespace seeds the deterministic repo_id below. A fixed
// namespace constant, never regenerated: the whole value of the id is that it
// is reproducible, and a namespace that changed would move every synthesized
// key at once.
//
// Generated once for CHAOS-4290 and pinned here.
var synthesizedRepoNamespace = uuid.MustParse("1b4e28ba-2fa1-11d2-883f-b9a761bde3fb")

// SynthesizedRepoID is the repo_id for an identity that has work-item metrics
// but no git record.
//
// DELIBERATE DIVERGENCE FROM PYTHON (CHAOS-4290, ruled by team-lead).
// compute_ic.py mints uuid.uuid4() here. repo_id is part of
// user_metrics_daily's dedup key (org_id, repo_id, author_email, day), so a
// random value means the same identity lands on a NEW key every run: the rows
// never collapse, and each re-drive appends another surviving row. That is a
// data-writing defect, and it became load-bearing once any native finalize
// failure redrives the run -- replicating it faithfully would have converted a
// rare silent overwrite into guaranteed accumulation.
//
// So the Go side is deterministic: a UUIDv5 over (org_id, identity_id). The
// same identity in the same org always resolves to the same repo_id, a redrive
// lands on the SAME key with a later computed_at, and the dedup read supersedes
// instead of accumulating.
//
// The day is deliberately NOT part of the seed. It is already a column of the
// dedup key, so including it would change nothing about uniqueness while
// fragmenting one identity's synthetic repo across days -- the id is meant to
// stand for "this identity has no repo", which is a property of the identity,
// not of the day.
//
// Python keeps uuid4 (no Python fixes for metrics); the divergence is stated in
// RISK-NOTES and the ticket is filed. PR3's parity kind observes it directly:
// the native side replays Idempotent while Python replays changed_key_set on
// the same corpus.
func SynthesizedRepoID(orgID, identityID string) uuid.UUID {
	return uuid.NewSHA1(synthesizedRepoNamespace, []byte(orgID+"\x1f"+identityID))
}

// Executor computes the ic_finalize family natively.
type Executor struct {
	conn       Conn
	now        func() time.Time
	teamMapper TeamMapper
}

// NewExecutor builds the executor. now is injected so computed_at -- the one
// remaining non-deterministic value the reference produces -- is controllable
// in tests rather than ambient. The synthesized repo_id used to be injected for
// the same reason and no longer needs to be: it is a pure function of the org
// and identity now.
func NewExecutor(conn Conn) *Executor {
	return &Executor{conn: conn, now: func() time.Time { return time.Now().UTC() }}
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
		// user_metrics_daily's loc_added/loc_deleted/prs_authored/prs_merged are
		// UInt32 (001_metrics_v2.sql) -- the clickhouse-go driver refuses to scan
		// a UInt32 column straight into a Go int64 destination ("converting
		// UInt32 to *int64 is unsupported"), so scan into the matching uint32
		// width first. GitUserMetric itself stays int64: the wider type is what
		// every downstream sum/merge in this package (and MergeICUserMetrics'
		// SUM aggregation) actually wants.
		var locAdded, locDeleted, prsAuthored, prsMerged uint32
		if err := rows.Scan(&metric.AuthorEmail, &metric.TeamID, &locAdded,
			&locDeleted, &prsAuthored, &prsMerged,
			&metric.MedianPRCycleHours, &metric.PRCycleP90Hours); err != nil {
			return nil, err
		}
		metric.LOCAdded = int64(locAdded)
		metric.LOCDeleted = int64(locDeleted)
		metric.PRsAuthored = int64(prsAuthored)
		metric.PRsMerged = int64(prsMerged)
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
		// Same class as loadGitMetrics above: work_item_user_metrics_daily's
		// items_started/items_completed/wip_count_end_of_day are UInt32
		// (001_metrics_v2.sql), not the Go int64 WorkItemUserMetric models them
		// as -- scan into the matching width, then widen.
		var itemsStarted, itemsCompleted, wipCountEndOfDay uint32
		if err := rows.Scan(&metric.UserIdentity, &metric.Provider, &metric.WorkScopeID,
			&metric.TeamID, &metric.TeamName, &itemsStarted, &itemsCompleted,
			&wipCountEndOfDay, &metric.CycleTimeP50Hrs, &metric.CycleTimeP90Hrs); err != nil {
			return nil, err
		}
		metric.ItemsStarted = int64(itemsStarted)
		metric.ItemsCompleted = int64(itemsCompleted)
		metric.WIPCountEndOfDay = int64(wipCountEndOfDay)
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
		// Deterministic, NOT the reference's uuid4 -- see SynthesizedRepoID for
		// why this is the one place the port deliberately diverges.
		repoID := landscapeRepoID
		if metric.SynthesizedRepoID {
			repoID = SynthesizedRepoID(orgID, metric.IdentityID)
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
