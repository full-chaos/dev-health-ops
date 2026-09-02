package cicd

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/remaining"
)

// conn is the narrow ClickHouse capability this package needs, matching
// internal/jobs/metrics/daily/repouser's conn interface shape.
type conn interface {
	Query(ctx context.Context, query string, args ...any) (driver.Rows, error)
	PrepareBatch(ctx context.Context, query string, opts ...driver.PrepareBatchOption) (driver.Batch, error)
}

// ClickHouseLoader reads ci_pipeline_runs rows for one partition (an org's
// repos for one day).
type ClickHouseLoader struct {
	conn conn
}

func NewClickHouseLoader(connection conn) (*ClickHouseLoader, error) {
	if connection == nil {
		return nil, fmt.Errorf("cicd: clickhouse connection is required")
	}
	return &ClickHouseLoader{conn: connection}, nil
}

// pipelineRunsQuery ports load_cicd_data's pipe_query (loaders/clickhouse.py:
// 1212): filtered by finished_at, NOT started_at -- see the package doc
// comment on the DOUBLE WINDOW FILTER. finished_at IS NULL (a still-running
// pipeline) never matches this range filter, matching Python exactly.
//
// No FINAL: ci_pipeline_runs is a ReplacingMergeTree, but load_cicd_data's
// real query never uses FINAL on it either (verified against
// loaders/clickhouse.py:1212-1217) -- this is parity with a pre-existing
// Python gap, not an oversight. See repouser/clickhouse.go's identical note
// for git_commits/git_pull_requests for the same reasoning.
const pipelineRunsQuery = `
SELECT repo_id, run_id, status, queued_at, started_at, finished_at
FROM ci_pipeline_runs
WHERE finished_at >= {start:DateTime64(3, 'UTC')} AND finished_at < {end:DateTime64(3, 'UTC')}
  AND repo_id IN {repo_ids:Array(UUID)}
  AND org_id = {org_id:String}`

// LoadPipelineRuns loads ci_pipeline_runs rows whose finished_at falls in
// [start, end) for repoIDs, scoped to orgID. Callers pass these straight to
// ComputeCICDMetricsDaily, which applies the started_at re-filter.
func (loader *ClickHouseLoader) LoadPipelineRuns(
	ctx context.Context, orgID string, repoIDs []uuid.UUID, start, end time.Time,
) ([]PipelineRunRow, error) {
	if loader == nil || loader.conn == nil {
		return nil, fmt.Errorf("cicd: loader unavailable")
	}
	if len(repoIDs) == 0 {
		return nil, nil
	}
	rows, err := loader.conn.Query(ctx, pipelineRunsQuery,
		clickhouse.Named("start", remaining.DateTime64Argument(start, remaining.DateTime64MillisecondPrecision)),
		clickhouse.Named("end", remaining.DateTime64Argument(end, remaining.DateTime64MillisecondPrecision)),
		clickhouse.Named("repo_ids", repoIDs),
		clickhouse.Named("org_id", orgID),
	)
	if err != nil {
		return nil, fmt.Errorf("load pipeline runs: %w", err)
	}
	defer rows.Close()

	var result []PipelineRunRow
	for rows.Next() {
		var (
			repoID     uuid.UUID
			runID      string
			status     *string
			queuedAt   *time.Time
			startedAt  time.Time
			finishedAt *time.Time
		)
		if err := rows.Scan(&repoID, &runID, &status, &queuedAt, &startedAt, &finishedAt); err != nil {
			return nil, fmt.Errorf("scan pipeline run row: %w", err)
		}
		result = append(result, PipelineRunRow{
			RepoID:     repoID,
			RunID:      runID,
			Status:     derefStr(status),
			QueuedAt:   queuedAt,
			StartedAt:  startedAt,
			FinishedAt: finishedAt,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

// Writer persists ComputeCICDMetricsDaily's output to cicd_metrics_daily.
type Writer struct {
	conn conn
}

func NewWriter(connection conn) (*Writer, error) {
	if connection == nil {
		return nil, fmt.Errorf("cicd: clickhouse connection is required")
	}
	return &Writer{conn: connection}, nil
}

// WriteResult writes cicd_metrics_daily and returns the number of rows
// written. Fails closed on an empty orgID -- CHAOS-4341 established this
// discipline for repouser's writer; this package follows it from the start
// rather than retrofitting it after a prod incident.
func (writer *Writer) WriteResult(ctx context.Context, rows []CICDMetric, orgID string) (int, error) {
	if writer == nil || writer.conn == nil {
		return 0, fmt.Errorf("cicd: writer unavailable")
	}
	if orgID == "" {
		return 0, fmt.Errorf("cicd: organization id is required to write cicd_metrics_daily")
	}
	if len(rows) == 0 {
		return 0, nil
	}
	batch, err := writer.conn.PrepareBatch(ctx, `INSERT INTO cicd_metrics_daily (
		repo_id, day, pipelines_count, success_rate, avg_duration_minutes,
		p90_duration_minutes, avg_queue_minutes, computed_at, org_id
	)`)
	if err != nil {
		return 0, fmt.Errorf("prepare cicd_metrics_daily batch: %w", err)
	}
	for _, row := range rows {
		if err := batch.Append(
			row.RepoID, row.Day, uint32(row.PipelinesCount), row.SuccessRate,
			row.AvgDurationMinutes, row.P90DurationMinutes, row.AvgQueueMinutes,
			row.ComputedAt, orgID,
		); err != nil {
			return 0, fmt.Errorf("append cicd_metrics_daily row: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return 0, fmt.Errorf("send cicd_metrics_daily batch: %w", err)
	}
	recordRowsWritten(len(rows), orgID != "")
	return len(rows), nil
}

func derefStr(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}
