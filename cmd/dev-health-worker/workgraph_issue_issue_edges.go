package main

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/edges"
)

// issueIssueEdgesPreStep is the native port of `_build_issue_issue_edges`
// (CHAOS-4924).
//
// # Why a pre-step now, and why it was a post-step before
//
// Python's `build()` used to run its issue-issue stage unconditionally,
// writing `confidence=1.0` (builder.py:905) where this port writes 0.9.
// `work_graph_edges` is `ReplacingMergeTree(last_synced)` and its ORDER BY
// excludes `confidence`, so the two writes addressed the SAME row and
// collapsed by `last_synced` -- registered as a pre-step, this producer would
// have written variant-C's 0.9 for the associative family and Python would
// have replaced it with 1.0 microseconds later, on every build, with every
// count reconciling and no error raised. Running last was therefore not a
// preference, it was the only ordering under which the divergence this port
// applies survived while the Python stage still ran.
//
// CHAOS-4924 deleted `_build_issue_issue_edges` (and its `build()` call site)
// outright, making this the sole producer of `issue_issue_edges`. There is no
// longer anything for a later-running Python stage to overwrite, so this step
// moved ahead of the bridge into the ordinary pre-step order -- exactly the
// resolution this doc comment used to name as the eventual follow-up.
//
// # THE DELETED PYTHON DID MORE THAN WRITE EDGES (CHAOS-5303 r1, P1)
//
// `_build_issue_issue_edges` also called `_delete_dependency_edge_candidates`
// (pruned stale native blocker-family edges under a superset of ids covering
// every legacy orientation) and `_publish_blocker_projection` (advanced a
// `work_graph_projection_runs` watermark row that `native_status_change.py`'s
// blocker-status API still reads). The first cut of this port carried the
// edge-derivation logic only and left both of those as dead, unwired Go code
// (`edges.BuildCleanupPlan`/`BuildBlockerProjection` in cleanup.go, "computed
// but not executed" by their own doc comments) -- a real regression, caught
// by codex review before merge, not by design. Run() now performs the full
// sequence Python did, in Python's own order: delete stale projection-run
// rows for this org+rule, read the currently-live blocker-family edge ids,
// compute and execute the cleanup plan, write the fresh edges, then publish
// the new watermark. Delete-before-write matters: the ids this step is about
// to re-create are exactly the ids the cleanup step may also name (a
// still-current edge is BOTH an "existing id" input to the plan AND
// re-inserted moments later), and Python performs the delete first for the
// same reason -- a write ordered before the matching delete would be
// immediately undone by it.
type issueIssueEdgesPreStep struct {
	connection driver.Conn
	observer   jobruntime.WorkGraphIssueEdgesObserver
	now        func() time.Time
}

func newIssueIssueEdgesPreStep(
	connection driver.Conn, observer jobruntime.WorkGraphIssueEdgesObserver,
) (*issueIssueEdgesPreStep, error) {
	if connection == nil || observer == nil {
		// An observer is required rather than optional: this step's counters are
		// the only way to see that variant-C is still being applied, and a step
		// running without them is the failure they exist to detect.
		return nil, errWorkerDependencyUnavailable
	}
	return &issueIssueEdgesPreStep{connection: connection, observer: observer, now: time.Now}, nil
}

func (step *issueIssueEdgesPreStep) Name() string { return "issue_issue_edges" }

// Run reads dependencies, derives the edge set and writes it, then publishes
// the per-row tally.
//
// It takes no window from the request scope. The read is deliberately
// window-independent: PR1 pinned that structurally, and two live captures with
// and without a window produced an identical edge-id set. So there is no scope
// parsing here and no dependency on the three date shapes the bridge accepts.
func (step *issueIssueEdgesPreStep) Run(
	ctx context.Context, claim workgraph.Claim,
) (map[string]any, error) {
	started := step.now()
	organizationID := claim.Request.OrganizationID

	rows, err := edges.ReadDependencies(ctx, step.connection, organizationID)
	if err != nil {
		return nil, fmt.Errorf("read dependencies: %w", err)
	}
	result, err := edges.DeriveIssueIssueEdges(rows, started)
	if err != nil {
		return nil, fmt.Errorf("derive issue<->issue edges: %w", err)
	}

	// CHAOS-5303 r1 P1: delete-before-write, matching
	// `_delete_dependency_edge_candidates`'s own ordering (see the type doc
	// comment above for why). A failure at any step here aborts the whole
	// Run() -- a partial cleanup (projection rows wiped but edges not
	// re-pruned, or edges pruned but the fresh write never lands) is worse
	// than refusing outright, since it leaves the next run's "existing ids"
	// read describing a state this run half-produced.
	if err := edges.DeleteProjectionRuns(ctx, step.connection, organizationID, edges.BlockerProjectionName, edges.BlockerProjectionRuleVersion()); err != nil {
		return nil, fmt.Errorf("delete stale work_graph_projection_runs: %w", err)
	}
	existingBlockerEdgeIDs, err := edges.ReadExistingBlockerEdgeIDs(ctx, step.connection, organizationID)
	if err != nil {
		return nil, fmt.Errorf("read existing blocker edge ids: %w", err)
	}
	cleanupPlan := edges.BuildCleanupPlan(rows, existingBlockerEdgeIDs)
	if err := edges.DeleteEdgesByID(ctx, step.connection, organizationID, cleanupPlan); err != nil {
		return nil, fmt.Errorf("delete stale blocker-family work_graph_edges: %w", err)
	}

	written, err := edges.WriteEdges(ctx, step.connection, organizationID, result.Edges)
	if err != nil {
		return nil, fmt.Errorf("write work_graph_edges: %w", err)
	}

	projectionRun := edges.BuildBlockerProjection(organizationID, "", result.Edges, started)
	if err := edges.WriteProjectionRun(ctx, step.connection, projectionRun); err != nil {
		return nil, fmt.Errorf("publish work_graph_projection_runs watermark: %w", err)
	}

	if err := edges.ObserveDerivation(step.observer, result, len(rows), step.now().Sub(started)); err != nil {
		// A tally that does not partition its input is refused rather than
		// published, and that refusal fails the step: publishing a wrong
		// accounting is worse than publishing none, because the counters are
		// what anyone would read to decide whether this step is healthy.
		return nil, fmt.Errorf("observe derivation: %w", err)
	}

	return map[string]any{
		"rows_read":          len(rows),
		"edges_written":      written,
		"missing_timestamps": result.MissingTimestamps,
		"outcomes":           outcomeCounts(result),
	}, nil
}

// outcomeCounts renders the tally with string keys so it survives the ledger's
// JSON round trip unchanged.
func outcomeCounts(result edges.DeriveResult) map[string]any {
	counts := make(map[string]any, len(result.Counts))
	for outcome, count := range result.Counts {
		counts[string(outcome)] = count
	}
	return counts
}
