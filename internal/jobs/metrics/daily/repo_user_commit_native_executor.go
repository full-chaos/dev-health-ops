package daily

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily/repouser"
)

// repoUserCommitWindowDays mirrors job_daily.py's h_start_date = d - timedelta(days=29):
// a 30-day INCLUSIVE window ending on the target day, used by the
// rework-churn/single-owner-file/bus-factor/code-ownership-gini kernels.
const repoUserCommitWindowDays = 30

// RepoUserCommitExecutor is the NATIVE implementation of the repo_user_commit
// metrics.daily family (CHAOS-4275) -- Wave 1's OTHER reference
// implementation alongside CHAOS-4276's TeamWellbeingExecutor, and the first
// to register against the per-family native-family cutover mechanism
// (PartitionHandler.SetNativeFamilies) that CHAOS-4276 built.
//
// Fidelity is documented on internal/jobs/metrics/daily/repouser's package
// doc comment (team attribution and identity alias resolution are NOT
// ported; PR-title revert detection is parity-DEAD on both sides, by
// design) -- this type is a thin ClickHouse-connection adapter over that
// package's pure Compute kernel and loader/writer, and carries no
// additional fidelity notes of its own.
type RepoUserCommitExecutor struct {
	loader *repouser.ClickHouseLoader
	writer *repouser.Writer
	nowUTC func() time.Time
}

var errRepoUserCommitUnavailable = fmt.Errorf("repo_user_commit native executor unavailable")

// NewRepoUserCommitExecutor fails closed on a nil connection, matching
// NewTeamWellbeingExecutor's construction-time policy: a refused executor
// simply never enters PartitionHandler's native family map, and
// repo_user_commit stays on the Python compatibility bridge for every
// partition until the worker restarts with a healthy connection.
func NewRepoUserCommitExecutor(conn driver.Conn) (*RepoUserCommitExecutor, error) {
	if conn == nil {
		return nil, errRepoUserCommitUnavailable
	}
	loader, err := repouser.NewClickHouseLoader(conn)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", errRepoUserCommitUnavailable, err)
	}
	writer, err := repouser.NewWriter(conn)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", errRepoUserCommitUnavailable, err)
	}
	return &RepoUserCommitExecutor{
		loader: loader, writer: writer,
		nowUTC: func() time.Time { return time.Now().UTC() },
	}, nil
}

// ComputeFamily implements NativeFamilyExecutor.
func (executor *RepoUserCommitExecutor) ComputeFamily(
	ctx context.Context, run Run, partition Partition,
) (int, error) {
	if executor == nil || executor.loader == nil || executor.writer == nil {
		return 0, errRepoUserCommitUnavailable
	}
	if run.OrganizationID == "" || run.TargetDay.IsZero() {
		return 0, fmt.Errorf("%w: partition %s run has no organization or target day", ErrInvalidState, partition.ID)
	}

	// Validated before any ClickHouse round-trip -- a malformed durable
	// partition scope is a precondition failure, not a transient dependency
	// error, and must not spend a query proving that. parseRepositoryUUIDs
	// is shared with TeamWellbeingExecutor (wellbeing_native_executor.go).
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
	windowStart := dayStart.AddDate(0, 0, -(repoUserCommitWindowDays - 1))

	commits, prs, reviews, err := executor.loader.LoadGitRows(ctx, run.OrganizationID, repoIDs, dayStart, dayEnd)
	if err != nil {
		return 0, err
	}
	windowCommits, err := executor.loader.LoadWindowCommitStats(ctx, run.OrganizationID, repoIDs, windowStart, dayEnd)
	if err != nil {
		return 0, err
	}
	bugItems, err := executor.loader.LoadBugWorkItems(ctx, run.OrganizationID, repoIDs, dayStart, dayEnd)
	if err != nil {
		return 0, err
	}

	mttrByRepo := repouser.MTTRByRepo(dayStart, bugItems)
	reworkByRepo := make(map[uuid.UUID]float64, len(repoIDs))
	singleOwnerByRepo := make(map[uuid.UUID]float64, len(repoIDs))
	busFactorByRepo := make(map[uuid.UUID]int, len(repoIDs))
	giniByRepo := make(map[uuid.UUID]float64, len(repoIDs))
	for _, repoID := range repoIDs {
		reworkByRepo[repoID] = repouser.ReworkChurnRatio(repoID, windowCommits)
		singleOwnerByRepo[repoID] = repouser.SingleOwnerFileRatio(repoID, windowCommits, 0.75, repouser.NoResolverNormalizeIdentity)
		busFactorByRepo[repoID] = repouser.BusFactor(repoID, windowCommits, 0.5)
		giniByRepo[repoID] = repouser.CodeOwnershipGini(repoID, windowCommits)
	}

	computedAt := executor.nowUTC()
	result := repouser.Compute(
		dayStart, commits, prs, reviews, computedAt,
		repouser.DefaultNormalizeIdentity, 1000,
		mttrByRepo, reworkByRepo, singleOwnerByRepo, busFactorByRepo, giniByRepo,
	)

	repoRows, userRows, commitRows, err := executor.writer.WriteResult(ctx, result, run.OrganizationID)
	if err != nil {
		return 0, err
	}
	return repoRows + userRows + commitRows, nil
}

var _ NativeFamilyExecutor = (*RepoUserCommitExecutor)(nil)
