package workerservice

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/edges"
)

// stalePRTargetCommitIssueEdgesCleanupPreStep retires commit -> issue edges
// whose target is a pull request work item. Pure cleanup of rows an earlier
// issue_commit_edges build wrote; it reads and writes nothing another pre-step
// touches, so it registers beside the other stale-edge cleanup.
type stalePRTargetCommitIssueEdgesCleanupPreStep struct {
	conn driver.Conn
}

func newStalePRTargetCommitIssueEdgesCleanupPreStep(conn driver.Conn) (*stalePRTargetCommitIssueEdgesCleanupPreStep, error) {
	if conn == nil {
		return nil, errWorkerDependencyUnavailable
	}
	return &stalePRTargetCommitIssueEdgesCleanupPreStep{conn: conn}, nil
}

func (step *stalePRTargetCommitIssueEdgesCleanupPreStep) Name() string {
	return "stale_pr_target_commit_issue_edges_cleanup"
}

func (step *stalePRTargetCommitIssueEdgesCleanupPreStep) Run(ctx context.Context, claim workgraph.Claim) (map[string]any, error) {
	if err := edges.DeleteStalePRTargetCommitIssueEdges(ctx, step.conn, claim.Request.OrganizationID, time.Now()); err != nil {
		return nil, err
	}
	return nil, nil
}
