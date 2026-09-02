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

// issueIssueEdgesPostStep is the native port of `_build_issue_issue_edges`,
// registered as a POST-step.
//
// # Why after the bridge rather than before
//
// Python's `build()` runs every stage unconditionally, and its issue-issue
// stage writes `confidence=1.0` (builder.py:905) where this port writes 0.9.
// `work_graph_edges` is `ReplacingMergeTree(last_synced)` and its ORDER BY
// excludes `confidence`, so the two are THE SAME ROW to the engine and collapse
// by `last_synced` -- the divergence is erased because the dedup key excludes
// the column that diverges. Registered as a
// pre-step, this producer would write variant-C's 0.9 for the associative
// family and Python would replace it with 1.0 microseconds later, on every
// build, with every count reconciling and no error raised.
//
// Running last is therefore not a preference, it is the only ordering under
// which the divergence this port exists to apply survives while the Python
// stage still runs. It is a bridge-era arrangement: when
// `_build_issue_issue_edges` retires, this step should move ahead of the bridge
// or stop being a step at all.
type issueIssueEdgesPostStep struct {
	connection driver.Conn
	observer   jobruntime.WorkGraphIssueEdgesObserver
	now        func() time.Time
}

func newIssueIssueEdgesPostStep(
	connection driver.Conn, observer jobruntime.WorkGraphIssueEdgesObserver,
) (*issueIssueEdgesPostStep, error) {
	if connection == nil || observer == nil {
		// An observer is required rather than optional: this step's counters are
		// the only way to see that variant-C is still being applied, and a step
		// running without them is the failure they exist to detect.
		return nil, errWorkerDependencyUnavailable
	}
	return &issueIssueEdgesPostStep{connection: connection, observer: observer, now: time.Now}, nil
}

func (step *issueIssueEdgesPostStep) Name() string { return "issue_issue_edges" }

// Run reads dependencies, derives the edge set and writes it, then publishes
// the per-row tally.
//
// It takes no window from the request scope. The read is deliberately
// window-independent: PR1 pinned that structurally, and two live captures with
// and without a window produced an identical edge-id set. So there is no scope
// parsing here and no dependency on the three date shapes the bridge accepts.
func (step *issueIssueEdgesPostStep) Run(
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
	written, err := edges.WriteEdges(ctx, step.connection, organizationID, result.Edges)
	if err != nil {
		return nil, fmt.Errorf("write work_graph_edges: %w", err)
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
