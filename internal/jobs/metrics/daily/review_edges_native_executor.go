package daily

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily/reviewedges"
)

// ReviewEdgesExecutor is the NATIVE implementation of the review_edges
// metrics.daily family (CHAOS-4279). Fidelity notes -- the append-only write
// mode, the read-side dedup convention, the dropped-edge quirk, and the
// deliberate loader-level dedup that changes reviews_count -- live on
// internal/jobs/metrics/daily/reviewedges' package and query doc comments;
// this type is a thin ClickHouse-connection adapter over that package's pure
// compute kernel and its loader/writer.
//
// # pre_bridge, unlike this lane's compounding_risk
//
// Registered in the ordinary PRE-bridge native map. Both of this family's
// inputs -- git_pull_requests and git_pull_request_reviews -- are RAW SYNC
// tables written by the provider sync path, not by any daily metrics family,
// so there is no same-partition write-ordering dependency of the kind that
// forces compounding_risk (repo_metrics_daily, written by repo_user_commit)
// and work_item_state (work_item_team_attributions, written by
// work_item_attribution) into post_bridge. Fail-open therefore applies in
// full: if this executor refuses or errors, Python computes and writes the
// family for that partition exactly as before.
type ReviewEdgesExecutor struct {
	loader *reviewedges.ClickHouseLoader
	writer *reviewedges.Writer
	nowUTC func() time.Time
}

var errReviewEdgesUnavailable = fmt.Errorf("review_edges native executor unavailable")

// NewReviewEdgesExecutor fails closed on a nil connection, matching
// NewCICDExecutor's construction-time policy: a refused executor never enters
// PartitionHandler's native family map, and review_edges stays on the Python
// compatibility bridge for every partition until the worker restarts with a
// healthy connection.
func NewReviewEdgesExecutor(conn driver.Conn) (*ReviewEdgesExecutor, error) {
	if conn == nil {
		return nil, errReviewEdgesUnavailable
	}
	loader, err := reviewedges.NewClickHouseLoader(conn)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", errReviewEdgesUnavailable, err)
	}
	writer, err := reviewedges.NewWriter(conn)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", errReviewEdgesUnavailable, err)
	}
	return &ReviewEdgesExecutor{
		loader: loader, writer: writer,
		nowUTC: func() time.Time { return time.Now().UTC() },
	}, nil
}

// ComputeFamily implements NativeFamilyExecutor.
func (executor *ReviewEdgesExecutor) ComputeFamily(
	ctx context.Context, run Run, partition Partition,
) (int, error) {
	if executor == nil || executor.loader == nil || executor.writer == nil {
		return 0, errReviewEdgesUnavailable
	}
	if run.OrganizationID == "" || run.TargetDay.IsZero() {
		return 0, fmt.Errorf("%w: partition %s run has no organization or target day", ErrInvalidState, partition.ID)
	}

	repoIDs, err := parseRepositoryUUIDs(partition.RepoIDs)
	if err != nil {
		return 0, fmt.Errorf("%w: partition %s repo_ids: %v", ErrInvalidState, partition.ID, err)
	}
	if len(repoIDs) == 0 {
		return 0, nil
	}

	targetDay := run.TargetDay.UTC()
	dayStart := time.Date(targetDay.Year(), targetDay.Month(), targetDay.Day(), 0, 0, 0, 0, time.UTC)
	dayEnd := dayStart.AddDate(0, 0, 1)

	// Reviews first: Python returns [] without ever reading the PR rows when
	// there are none (reviews.py:33-34), and skipping the PR query in that
	// case is the same short-circuit, one layer out.
	reviews, err := executor.loader.LoadReviews(ctx, run.OrganizationID, repoIDs, dayStart, dayEnd)
	if err != nil {
		return 0, err
	}
	if len(reviews) == 0 {
		return 0, nil
	}

	pullRequests, err := executor.loader.LoadPullRequests(ctx, run.OrganizationID, repoIDs, dayStart, dayEnd)
	if err != nil {
		return 0, err
	}

	computedAt := executor.nowUTC()
	records := reviewedges.ComputeReviewEdgesDaily(
		dayStart, pullRequests, reviews, computedAt, run.OrganizationID,
	)

	rowsWritten, err := executor.writer.WriteRecords(ctx, records, run.OrganizationID)
	if err != nil {
		return 0, err
	}
	return rowsWritten, nil
}

var _ NativeFamilyExecutor = (*ReviewEdgesExecutor)(nil)
