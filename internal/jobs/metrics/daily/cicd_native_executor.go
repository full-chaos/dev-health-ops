package daily

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily/cicd"
)

// CICDExecutor is the NATIVE implementation of the cicd metrics.daily family
// (CHAOS-4292), following RepoUserCommitExecutor's (CHAOS-4275) minimal-
// scoping shape rather than TeamWellbeingExecutor's (CHAOS-4276): cicd has
// no team/repo-pattern resolver and no cross-repo aggregation, and
// cicd_metrics_daily's dedup key already includes repo_id, so one shared
// computedAt for the whole partition is safe -- no per-repo-group timestamp
// tie-break problem exists here.
//
// Fidelity is documented on internal/jobs/metrics/daily/cicd's package doc
// comment (the double window filter, the no-FINAL parity, the "zero rows ==
// no record" behaviour) -- this type is a thin ClickHouse-connection adapter
// over that package's pure Compute kernel and loader/writer.
type CICDExecutor struct {
	loader *cicd.ClickHouseLoader
	writer *cicd.Writer
	nowUTC func() time.Time
}

var errCICDUnavailable = fmt.Errorf("cicd native executor unavailable")

// NewCICDExecutor fails closed on a nil connection, matching
// NewRepoUserCommitExecutor's construction-time policy: a refused executor
// simply never enters PartitionHandler's native family map, and cicd stays
// on the Python compatibility bridge for every partition until the worker
// restarts with a healthy connection.
func NewCICDExecutor(conn driver.Conn) (*CICDExecutor, error) {
	if conn == nil {
		return nil, errCICDUnavailable
	}
	loader, err := cicd.NewClickHouseLoader(conn)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", errCICDUnavailable, err)
	}
	writer, err := cicd.NewWriter(conn)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", errCICDUnavailable, err)
	}
	return &CICDExecutor{
		loader: loader, writer: writer,
		nowUTC: func() time.Time { return time.Now().UTC() },
	}, nil
}

// ComputeFamily implements NativeFamilyExecutor.
func (executor *CICDExecutor) ComputeFamily(
	ctx context.Context, run Run, partition Partition,
) (int, error) {
	if executor == nil || executor.loader == nil || executor.writer == nil {
		return 0, errCICDUnavailable
	}
	if run.OrganizationID == "" || run.TargetDay.IsZero() {
		return 0, fmt.Errorf("%w: partition %s run has no organization or target day", ErrInvalidState, partition.ID)
	}

	// Validated before any ClickHouse round-trip -- a malformed durable
	// partition scope is a precondition failure, not a transient dependency
	// error, and must not spend a query proving that. parseRepositoryUUIDs
	// is shared with RepoUserCommitExecutor/TeamWellbeingExecutor
	// (wellbeing_native_executor.go).
	repoIDs, err := parseRepositoryUUIDs(partition.RepoIDs)
	if err != nil {
		return 0, fmt.Errorf("%w: partition %s repo_ids: %v", ErrInvalidState, partition.ID, err)
	}
	if len(repoIDs) == 0 {
		// No repositories in this partition: nothing to compute, and not an
		// error -- MaterializeScheduledFanout already terminalizes the
		// no_repositories case upstream.
		return 0, nil
	}

	dayStart := time.Date(run.TargetDay.UTC().Year(), run.TargetDay.UTC().Month(), run.TargetDay.UTC().Day(), 0, 0, 0, 0, time.UTC)
	dayEnd := dayStart.AddDate(0, 0, 1)

	pipelineRuns, err := executor.loader.LoadPipelineRuns(ctx, run.OrganizationID, repoIDs, dayStart, dayEnd)
	if err != nil {
		return 0, err
	}

	computedAt := executor.nowUTC()
	result := cicd.ComputeCICDMetricsDaily(dayStart, pipelineRuns, computedAt)

	rowsWritten, err := executor.writer.WriteResult(ctx, result, run.OrganizationID)
	if err != nil {
		return 0, err
	}
	return rowsWritten, nil
}

var _ NativeFamilyExecutor = (*CICDExecutor)(nil)
