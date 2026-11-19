package daily

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/deploymentstatus"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/numerical"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/remaining"
)

// deploySource is the deploy family's source: exactly the deployments
// ComputeDeployMetrics counts. deployed_at in [start, end) is load_cicd_data's
// deploy_query window (src/dev_health_ops/metrics/loaders/clickhouse.py:
// 1218-1223); the canceled exclusion is deploymentstatus.Classify's, which the
// compute applies as well. The zero-row source check reads this same text.
var deploySource = `
FROM deployments FINAL
WHERE org_id = {org_id:String}
  AND deployed_at >= {start:DateTime64(3, 'UTC')} AND deployed_at < {end:DateTime64(3, 'UTC')}
  AND repo_id IN {repo_ids:Array(UUID)}
  AND ` + deploymentstatus.CanceledExclusionSQL("status")

func deploySourceArgs(organizationID string, repoIDs []uuid.UUID, start, end time.Time) []any {
	return []any{
		clickhouse.Named("org_id", organizationID),
		clickhouse.Named("start", remaining.DateTime64Argument(start, remaining.DateTime64MillisecondPrecision)),
		clickhouse.Named("end", remaining.DateTime64Argument(end, remaining.DateTime64MillisecondPrecision)),
		clickhouse.Named("repo_ids", repoIDs),
		clickhouse.Named("canceled_statuses", deploymentstatus.CanceledStatuses),
	}
}

// LoadDeployments ports load_cicd_data's deploy_query through deploySource --
// see DeployExecutor's doc comment (deploy_native_executor.go) for why this
// filters on deployed_at alone rather than the
// four-way coalesce a DIFFERENT family's (DORA's) loader uses on the same
// table. It reads FINAL, so a re-synced deployment counts once, in its current
// version, before ClickHouse merges the parts. Only the columns compute_deploy_metrics_daily actually reads
// (compute_deployments.py:53-104) are selected -- deployment_id,
// environment, pull_request_number etc. are part of DeploymentRow but never
// read by this family's compute function.
func LoadDeployments(
	ctx context.Context, conn repositoryRows, organizationID string, repoIDs []uuid.UUID, start, end time.Time,
) ([]numerical.DeployRow, error) {
	if conn == nil || strings.TrimSpace(organizationID) == "" || !start.Before(end) {
		return nil, ErrInvalidState
	}
	if len(repoIDs) == 0 {
		return nil, nil
	}
	rows, err := conn.Query(ctx, `
SELECT repo_id, status, started_at, finished_at, deployed_at, merged_at`+deploySource,
		deploySourceArgs(organizationID, repoIDs, start, end)...)
	if err != nil {
		return nil, fmt.Errorf("load deployments: %w", err)
	}
	defer rows.Close()

	var result []numerical.DeployRow
	for rows.Next() {
		var (
			repoID     uuid.UUID
			status     *string
			startedAt  *time.Time
			finishedAt *time.Time
			deployedAt *time.Time
			mergedAt   *time.Time
		)
		if err := rows.Scan(&repoID, &status, &startedAt, &finishedAt, &deployedAt, &mergedAt); err != nil {
			return nil, fmt.Errorf("scan deployment: %w", err)
		}
		result = append(result, numerical.DeployRow{
			RepoID:     repoID.String(),
			Status:     derefWellbeingString(status),
			StartedAt:  derefDeployTime(startedAt),
			FinishedAt: derefDeployTime(finishedAt),
			DeployedAt: derefDeployTime(deployedAt),
			MergedAt:   derefDeployTime(mergedAt),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate deployments: %w", err)
	}
	return result, nil
}

func derefDeployTime(value *time.Time) time.Time {
	if value == nil {
		return time.Time{}
	}
	return *value
}

// deployBatchConn is the narrow write capability WriteDeployMetricsDaily needs.
type deployBatchConn interface {
	PrepareBatch(context.Context, string, ...driver.PrepareBatchOption) (driver.Batch, error)
}

// WriteDeployMetricsDaily ports the write side of write_deploy_metrics
// (sinks/clickhouse/ci.py:67-83) -- the same table (deploy_metrics_daily,
// internal/chmigrate/history/004_quality_delivery_metrics.sql:35-45) and column
// order, org_id stamped on every row (CHAOS-4341's org-scoping lesson,
// already applied to repo_metrics_daily/team_metrics_daily, applies here
// too: an org_id="" row is invisible to every org-scoped reader).
func WriteDeployMetricsDaily(
	ctx context.Context, conn deployBatchConn, organizationID string, rows []numerical.DeployMetric,
) (int, error) {
	if len(rows) == 0 {
		return 0, nil
	}
	if conn == nil || strings.TrimSpace(organizationID) == "" {
		return 0, ErrInvalidState
	}
	batch, err := conn.PrepareBatch(ctx, `INSERT INTO deploy_metrics_daily (
		repo_id, day, deployments_count, failed_deployments_count,
		deploy_time_p50_hours, lead_time_p50_hours, computed_at, org_id,
		pooled_deploy_hours, pooled_samples_stored
	)`)
	if err != nil {
		return 0, fmt.Errorf("prepare deploy_metrics_daily batch: %w", err)
	}
	for _, row := range rows {
		repoID, err := uuid.Parse(row.RepoID)
		if err != nil {
			return 0, fmt.Errorf("%w: deploy metric repo_id %q: %v", ErrInvalidState, row.RepoID, err)
		}
		samples := row.DeployHoursSamples
		fit := pooledSamplesFit(ctx, "deploy", organizationID, row.RepoID, row.Day, len(samples))
		if !fit {
			samples = nil
		}
		if err := batch.Append(
			// deployments_count/failed_deployments_count are per-(repo, day)
			// tallies incremented once per deployment row with no LIMIT --
			// bounded by a day's deployment volume, never negative -- so
			// they keep a direct conversion.
			repoID, row.Day, uint32(row.DeploymentsCount), uint32(row.FailedDeploymentsCount),
			row.DeployTimeP50Hours, row.LeadTimeP50Hours, row.ComputedAt.UTC(), organizationID,
			float64Samples(samples), pooledMarker(fit),
		); err != nil {
			return 0, fmt.Errorf("append deploy_metrics_daily row: %w", err)
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
		return len(rows), fmt.Errorf("send deploy_metrics_daily batch: %w", err)
	}
	return len(rows), nil
}
