package edges

import (
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
	"sort"
	"time"
)

// cleanupPageSize is Python's 1000-row batch (builder.py:995-996 and the read's
// LIMIT 1000 at :959). Both the cursor page and the delete batch use it.
const cleanupPageSize = 1000

// CleanupPlan is the delete work `_delete_dependency_edge_candidates` performs
// (builder.py:922-1006), computed but not executed, so it is assertable against
// the frozen mutations without a database.
type CleanupPlan struct {
	// CandidateIDs, sorted. Python sorts before paging (:994), so the ORDER is
	// part of the contract, not an implementation detail.
	CandidateIDs []string
	// Pages are CandidateIDs cut into cleanupPageSize chunks, in order.
	Pages [][]string
}

// BuildCleanupPlan recomputes the delete set from the inputs.
//
// # WHY A SUPERSET, AND WHY IT IS NOT OVER-DELETION
//
// For every blocker row it generates SIX ids: both endpoint directions crossed
// with BLOCKS, IS_BLOCKED_BY and RELATES. That looks excessive until you see what
// it is for — a legacy row may have been written under any of those orientations
// by an older canonicalisation, and the rewrite that follows only re-creates the
// one current orientation. Deleting only the current id would leave the other
// five alive as orphans that no later run ever revisits.
//
// It is not over-deletion because every id it names is an id THIS run either
// re-creates or has established should not exist.
//
// # WHAT IT DOES NOT COVER
//
// Only the blocker family. Stale `relates`, `duplicates`, `parent_of` and
// `child_of` issue<->issue edges are cleaned by nothing, ever (CHAOS-4812 item 3).
// Replicated verbatim: widening it here would delete rows Python leaves alive.
func BuildCleanupPlan(rows []DependencyRow, existingEdgeIDs []string) CleanupPlan {
	candidates := make(map[string]struct{}, len(existingEdgeIDs)+len(rows)*6)
	for _, id := range existingEdgeIDs {
		// Python's `if row.get("edge_id")` (:966): a falsy id is dropped.
		if id != "" {
			candidates[id] = struct{}{}
		}
	}

	for _, row := range rows {
		// Python lowercases before the membership test (:927).
		if _, isBlocker := blockerTypes[pythonparity.Lower(row.RelationshipType)]; !isBlocker {
			continue
		}
		// NOTE these are the RAW endpoints, NOT the canonicalised ones — Python
		// reads them straight off the row here (:975-977) rather than calling
		// _canonical_dependency. Using the canonical pair would generate ids for
		// the post-swap orientation only, which is the opposite of the point.
		source, target := row.SourceWorkItemID, row.TargetWorkItemID
		if source == "" || target == "" {
			continue
		}
		for _, pair := range [2][2]string{{source, target}, {target, source}} {
			for _, edgeType := range [3]string{EdgeTypeBlocks, EdgeTypeIsBlockedBy, EdgeTypeRelates} {
				candidates[EdgeID(NodeTypeIssue, pair[0], edgeType, NodeTypeIssue, pair[1])] = struct{}{}
			}
		}
	}

	ordered := make([]string, 0, len(candidates))
	for id := range candidates {
		ordered = append(ordered, id)
	}
	sort.Strings(ordered)

	plan := CleanupPlan{CandidateIDs: ordered}
	for start := 0; start < len(ordered); start += cleanupPageSize {
		end := start + cleanupPageSize
		if end > len(ordered) {
			end = len(ordered)
		}
		plan.Pages = append(plan.Pages, ordered[start:end])
	}
	return plan
}

// BlockerProjectionName is the `projection_name` value BuildBlockerProjection
// stamps and DeleteProjectionRuns must be called with, so a caller that needs
// to wipe the prior watermark before publishing a fresh one (CHAOS-5303 r1
// P1: `_delete_dependency_edge_candidates` runs before
// `_publish_blocker_projection`, builder.py:911-920 vs 1016-1045) names the
// exact same projection this package will publish under, rather than
// duplicating the string literal at the call site.
const BlockerProjectionName = "issue_blockers"

// BlockerProjectionRuleVersion exposes blockerProjectionRuleVersion
// (canonical.go) to callers outside this package -- specifically, a caller
// that must delete stale `work_graph_projection_runs` rows for the exact same
// rule this package's BuildBlockerProjection will publish under.
func BlockerProjectionRuleVersion() string { return blockerProjectionRuleVersion }

// ProjectionRun is the `work_graph_projection_runs` row
// `_publish_blocker_projection` writes (builder.py:1016-1045).
type ProjectionRun struct {
	OrgID          string
	ProjectionName string
	ScopeRepoID    string
	RuleVersion    string
	InputWatermark time.Time
	RowCount       int
	CompletedAt    time.Time
}

// BuildBlockerProjection computes the watermark row for a run.
//
// # THE TWO FIELDS ARE COMPUTED FROM DIFFERENT SUBSETS
//
// `RowCount` counts every BLOCKS edge; `InputWatermark` is the max event_ts over
// only those whose event_ts is a real timestamp (Python's `isinstance(datetime)`
// filter, :1024-1027). So the two can describe different sets and the persisted
// projection can misstate progress while every edge is individually correct —
// CHAOS-4812. Replicated rather than reconciled.
//
// The empty case takes the build clock (`max(..., default=self._now)`), so a run
// that produced no blocker edges still stamps a watermark of "now" rather than
// leaving it null — which reads as progress that did not happen.
func BuildBlockerProjection(
	organizationID string, scopeRepoID string, edges []Row, buildClock time.Time,
) ProjectionRun {
	watermark := buildClock
	found := false
	blockerCount := 0
	for _, edge := range edges {
		if edge.EdgeType != EdgeTypeBlocks {
			continue
		}
		blockerCount++
		if edge.EventTs.IsZero() {
			continue // the isinstance(datetime) filter's analogue
		}
		if !found || edge.EventTs.After(watermark) {
			watermark = edge.EventTs
			found = true
		}
	}
	return ProjectionRun{
		OrgID:          organizationID,
		ProjectionName: BlockerProjectionName,
		ScopeRepoID:    scopeRepoID,
		RuleVersion:    blockerProjectionRuleVersion,
		InputWatermark: watermark,
		RowCount:       blockerCount,
		CompletedAt:    buildClock,
	}
}
