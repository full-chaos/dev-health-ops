package daily

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/numerical"
	"github.com/full-chaos/dev-health-ops/internal/teamkeytables"
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
//  1. FINAL, as DORA's loadDeployments (dora_native_clickhouse.go:238) reads
//     the same ReplacingMergeTree table. Python's load_cicd_data reads it raw
//     (a legacy path); the Go loader does not copy that, because a re-synced
//     deployment (status moved to canceled, deployed_at on another day) would
//     keep its old version counted until ClickHouse merges the parts. The
//     window and canceled predicates are on non-key columns, so they apply to
//     the current version.
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

	// The instant of the read, taken before it: the version of a row that retires a key (see below).
	snapshot := executor.nowUTC().UTC().Truncate(time.Second)
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
	// A repository of this partition whose stored row of the day is live and that has no deployment now (cancelled, or
	// moved to another day) gets a row that retires the key (stale_team_keys.go), at the instant of the read: the day then
	// reads as absent, never 0. A repository with no stored row gets nothing; only the partition's own repositories.
	produced := make([]staleKey, 0, len(metrics))
	for _, metric := range metrics {
		produced = append(produced, staleKey{metric.RepoID})
	}
	retired, err := supersedeStaleTeamKeys(ctx, executor.conn, teamkeytables.DeployMetricsDaily, run.OrganizationID,
		day, newStaleKeyScope(repositoryScopeTuples(repoIDs)...), produced, snapshot)
	if err != nil {
		return written + retired, fmt.Errorf("retire stale deploy_metrics_daily days: %w", err)
	}
	return written + retired, nil
}

var _ NativeFamilyExecutor = (*DeployExecutor)(nil)
