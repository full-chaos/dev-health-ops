package cicd

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/remaining"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/testops"
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

// PipelineRunsSource is the cicd family's source: exactly the runs
// ComputeCICDMetricsDaily keeps. A finished run counts on the UTC day it
// finishes (finished_at in the day window); started_at is not a day filter (see
// the package doc). The zero-row source check reads this same text, so a run
// counts as source only when this family would count it. finished_at IS NULL (a
// still-running pipeline) never matches the range filter. A run
// still open (testops.IsOpenRun) is left out too: a provider sync stamps its
// last update time into finished_at. A run whose
// stored status is testops.StatusDeleted (GitHub no longer has it) is left out,
// as ComputeCICDMetricsDaily leaves it out; the SQL trim is ASCII only while the
// compute trims Unicode space, so a mismatch can only miss a flag.
//
// FINAL: ci_pipeline_runs is a ReplacingMergeTree; without it unmerged
// versions of one run count twice, and a stale version can match the
// window that the live version no longer matches.
const PipelineRunsSource = `
FROM ci_pipeline_runs FINAL
WHERE finished_at >= {start:DateTime64(3, 'UTC')} AND finished_at < {end:DateTime64(3, 'UTC')}
  AND repo_id IN {repo_ids:Array(UUID)}
  AND org_id = {org_id:String}
  AND lowerUTF8(trimBoth(ifNull(status, ''))) != '` + testops.StatusDeleted + `'
  AND ` + testops.NotOpenRunSQL

const pipelineRunsQuery = `
SELECT repo_id, run_id, status, queued_at, started_at, finished_at` + PipelineRunsSource

// SourceArgs binds the parameters PipelineRunsSource reads.
func SourceArgs(orgID string, repoIDs []uuid.UUID, start, end time.Time) []any {
	return []any{
		clickhouse.Named("start", remaining.DateTime64Argument(start, remaining.DateTime64MillisecondPrecision)),
		clickhouse.Named("end", remaining.DateTime64Argument(end, remaining.DateTime64MillisecondPrecision)),
		clickhouse.Named("repo_ids", repoIDs),
		clickhouse.Named("org_id", orgID),
	}
}

// LoadPipelineRuns loads the PipelineRunsSource rows for [start, end), repoIDs
// and orgID. ComputeCICDMetricsDaily still re-applies the finished_at filter,
// so the result is the same whether or not the SQL already applied it.
func (loader *ClickHouseLoader) LoadPipelineRuns(
	ctx context.Context, orgID string, repoIDs []uuid.UUID, start, end time.Time,
) ([]PipelineRunRow, error) {
	if loader == nil || loader.conn == nil {
		return nil, fmt.Errorf("cicd: loader unavailable")
	}
	if len(repoIDs) == 0 {
		return nil, nil
	}
	rows, err := loader.conn.Query(ctx, pipelineRunsQuery, SourceArgs(orgID, repoIDs, start, end)...)
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
		p90_duration_minutes, avg_queue_minutes, computed_at, org_id, retracted
	)`)
	if err != nil {
		return 0, fmt.Errorf("prepare cicd_metrics_daily batch: %w", err)
	}
	for _, row := range rows {
		var retracted uint8
		if row.Retracted {
			retracted = 1
		}
		if err := batch.Append(
			// pipelines_count is a per-(repo, day) tally with no LIMIT --
			// bounded by how many pipeline runs a repo can have in one day,
			// never negative -- so it keeps a direct conversion.
			row.RepoID, row.Day, uint32(row.PipelinesCount), row.SuccessRate,
			row.AvgDurationMinutes, row.P90DurationMinutes, row.AvgQueueMinutes,
			row.ComputedAt, orgID, retracted,
		); err != nil {
			return 0, fmt.Errorf("append cicd_metrics_daily row: %w", err)
		}
	}
	// CHAOS-5190 confirmation-pass sweep: Send is the one call here that
	// crosses the network, so a Send error is AMBIGUOUS -- ClickHouse may
	// have committed the insert server-side and only the acknowledgement
	// was lost. Report the true row count on this specific error path
	// (never on PrepareBatch/Append, which have not crossed the network
	// and genuinely wrote nothing), so the caller fails CLOSED on the
	// ambiguity instead of silently open (matches
	// work_graph_edges_native_clickhouse.go's established pattern).
	if err := batch.Send(); err != nil {
		return len(rows), fmt.Errorf("send cicd_metrics_daily batch: %w", err)
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
