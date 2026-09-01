package daily

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/numerical"
)

// DeployExecutor is the NATIVE implementation of the `deploy` metrics.daily
// family (CHAOS-4293) -- the third family to leave the HTTP compatibility
// bridge, after team_wellbeing (CHAOS-4276) and repo_user_commit (CHAOS-4275).
//
// # Fidelity
//
// The Python job is the authority
// (src/dev_health_ops/metrics/compute_deployments.py:53,
// compute_deploy_metrics_daily, fed by ClickHouseDataLoader.load_cicd_data's
// deploy_query, src/dev_health_ops/metrics/loaders/clickhouse.py:1218-1223).
// Two things about the LOAD query specifically are easy to "improve" into a
// divergence -- see LoadDeployments' doc comment (deploy_native_clickhouse.go)
// for both:
//
//  1. NO FINAL, even though `deployments` is a ReplacingMergeTree and a
//     DIFFERENT family's Go port (DORA's loadDeployments,
//     dora_native_clickhouse.go:238) already queries the SAME table WITH
//     FINAL. That is a different Python function on a different bridge --
//     matching the ACTUAL Python this family replaces (load_cicd_data, not
//     job_dora.py's _load_deployments) is what makes this executor
//     row-identical to what it is replacing. Mirrors repouser's identical
//     documented choice for git_commits/git_pull_requests (clickhouse.go:
//     28-37 doc comment): "PARITY, not an oversight."
//  2. THE WINDOW FILTER IS deployed_at ONLY. compute_deploy_metrics_daily
//     itself has a `deployed_at or started_at` fallback
//     (compute_deployments.py:67, ported verbatim in
//     numerical.ComputeDeployMetrics) -- but that fallback is DEAD in the
//     actual production call chain: load_cicd_data's SQL already excludes
//     every row with a NULL deployed_at before the compute function ever
//     sees it. This executor's loader reproduces that same prefilter.
type DeployExecutor struct {
	conn   driver.Conn
	nowUTC func() time.Time
}

var errDeployUnavailable = fmt.Errorf("deploy native executor unavailable")

// NewDeployExecutor fails closed on a nil connection, matching
// NewTeamWellbeingExecutor/NewRepoUserCommitExecutor's construction-time
// policy: a refused executor simply never enters PartitionHandler's native
// family map, and `deploy` stays on the Python compatibility bridge for
// every partition until the worker restarts with a healthy connection.
func NewDeployExecutor(conn driver.Conn) (*DeployExecutor, error) {
	if conn == nil {
		return nil, errDeployUnavailable
	}
	return &DeployExecutor{
		conn:   conn,
		nowUTC: func() time.Time { return time.Now().UTC() },
	}, nil
}

// ComputeFamily implements NativeFamilyExecutor.
func (executor *DeployExecutor) ComputeFamily(
	ctx context.Context, run Run, partition Partition,
) (int, error) {
	if executor == nil || executor.conn == nil {
		return 0, errDeployUnavailable
	}
	if run.OrganizationID == "" || run.TargetDay.IsZero() {
		return 0, fmt.Errorf("%w: partition %s run has no organization or target day", ErrInvalidState, partition.ID)
	}

	// Validated before any ClickHouse round-trip -- a malformed durable
	// partition scope is a precondition failure, not a transient dependency
	// error, and must not spend a query proving that. parseRepositoryUUIDs
	// is shared with TeamWellbeingExecutor/RepoUserCommitExecutor
	// (wellbeing_native_executor.go).
	repoIDs, err := parseRepositoryUUIDs(partition.RepoIDs)
	if err != nil {
		return 0, fmt.Errorf("%w: partition %s repo_ids: %v", ErrInvalidState, partition.ID, err)
	}
	if len(repoIDs) == 0 {
		// No repositories in this partition: nothing to compute, and not an
		// error -- MaterializeScheduledFanout already terminalizes the
		// no_repositories case upstream (matches RepoUserCommitExecutor).
		return 0, nil
	}

	day := time.Date(run.TargetDay.UTC().Year(), run.TargetDay.UTC().Month(), run.TargetDay.UTC().Day(), 0, 0, 0, 0, time.UTC)
	start := day
	end := start.Add(24 * time.Hour)

	rows, err := LoadDeployments(ctx, executor.conn, run.OrganizationID, repoIDs, start, end)
	if err != nil {
		return 0, err
	}

	computedAt := executor.nowUTC()
	metrics := numerical.ComputeDeployMetrics(day, rows, computedAt)

	written, err := WriteDeployMetricsDaily(ctx, executor.conn, run.OrganizationID, metrics)
	if err != nil {
		return 0, err
	}
	return written, nil
}

var _ NativeFamilyExecutor = (*DeployExecutor)(nil)
