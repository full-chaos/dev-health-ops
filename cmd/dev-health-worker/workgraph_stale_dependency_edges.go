package main

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/edges"
)

// staleDependencyIssueEdgesCleanupPreStep adapts
// `_delete_stale_pr_dependency_issue_edges` (CHAOS-4924) to the work-graph
// build's pre-step seam. Python ran this as the FIRST action inside
// `build()`, before any of the (already-native) edge-derivation stages; this
// step is registered first in buildPreStepOrder for the same reason -- it is
// pure cleanup with no dependency on anything another pre-step writes, so
// running it first just matches Python's own position rather than any
// ordering requirement (same "free position, placed to preserve Python's
// relative order" reasoning buildPreStepOrder's own doc comment gives for
// flag_guards_edges/operational_incident_edges).
type staleDependencyIssueEdgesCleanupPreStep struct {
	conn driver.Conn
}

func newStaleDependencyIssueEdgesCleanupPreStep(conn driver.Conn) (*staleDependencyIssueEdgesCleanupPreStep, error) {
	if conn == nil {
		return nil, errWorkerDependencyUnavailable
	}
	return &staleDependencyIssueEdgesCleanupPreStep{conn: conn}, nil
}

func (step *staleDependencyIssueEdgesCleanupPreStep) Name() string {
	return "stale_pr_dependency_issue_edges_cleanup"
}

func (step *staleDependencyIssueEdgesCleanupPreStep) Run(ctx context.Context, claim workgraph.Claim) (map[string]any, error) {
	if err := edges.DeleteStalePRDependencyIssueEdges(ctx, step.conn, claim.Request.OrganizationID); err != nil {
		return nil, err
	}
	return nil, nil
}
