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

// Conn is the narrow ClickHouse capability this package needs.
type Conn interface {
	Query(ctx context.Context, query string, args ...any) (driver.Rows, error)
	Exec(ctx context.Context, query string, args ...any) error
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
	conn  Conn
	now   func() time.Time
	newID func() uuid.UUID
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
