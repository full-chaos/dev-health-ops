package issuepredges

import (
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/edges"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/issueprlinks"
)

// HeuristicWorkItemRow is one row of the work-item read
// (builder.py:1022-1041): repo_id, work_item_id, updated_at.
type HeuristicWorkItemRow struct {
	RepoID     uuid.UUID
	WorkItemID string
	UpdatedAt  time.Time
}

// HeuristicPullRequestRow is one row of the PR read (builder.py:1046-1074):
// repo_id, number, created_at.
type HeuristicPullRequestRow struct {
	RepoID    uuid.UUID
	Number    int
	CreatedAt time.Time
}

// ExplicitLink is one (work_item_id, pr_number) pair already linked --
// Python's `explicit_links` parameter, which used to arrive as the in-memory
// union of _build_issue_pr_edges_from_fast_path's and _build_issue_pr_edges'
// own results. Both are now native pre-steps that commit their rows to
// work_graph_issue_pr before this one runs, so this package reads the SAME
// set fresh via LoadExplicitLinks instead of carrying it in memory -- see
// issuepredges' package doc's "Ordering" section, which anticipated exactly
// this read when the fast-path/text-parse pair was ported.
type ExplicitLink struct {
	WorkItemID string
	PRNumber   int
}

// HeuristicInputs is everything DeriveHeuristicEdges reads. A pure-function
// shape, same reasoning as prcommit.Inputs/issueprlinks.Inputs: testable
// without ClickHouse.
type HeuristicInputs struct {
	WorkItems           []HeuristicWorkItemRow
	PullRequests        []HeuristicPullRequestRow
	ExplicitLinks       []ExplicitLink
	HeuristicDaysWindow int
	HeuristicConfidence float32
}

// HeuristicResult is one derivation's output: the two writes Python performs
// (builder.py:1193-1194).
type HeuristicResult struct {
	Edges []edges.Row
	Links []issueprlinks.Link
}

// DeriveHeuristicEdges ports _build_heuristic_issue_pr_edges
// (builder.py:1000-1196) exactly, including two quirks worth calling out
// because they read as bugs in isolation but are the CONTRACT this port must
// reproduce:
//
//   - A work item is excluded ENTIRELY (never even considered for a
//     heuristic match) if it has ANY explicit link, for ANY pr_number --
//     builder.py:1107,1118-1119's `linked_work_items` set. The narrower
//     per-pair check at builder.py:1136-1137 (kept here too, see below) can
//     therefore never actually fire for a work item that reaches it, since
//     such a work item was already excluded by the item-level check first.
//     Preserved verbatim rather than "simplified away," since a port's
//     contract is behavioural parity, not the cleanest equivalent program.
//   - `HeuristicDaysWindow == 0` short-circuits to an empty result
//     (builder.py:1014-1015's falsy check on an int) -- a NEGATIVE window is
//     NOT short-circuited (Python's `bool(-1)` is True), so this checks `==
//     0` specifically, not `<= 0`.
//
// Binary search bounds mirror Python's bisect_left/bisect_right exactly:
// bisect_left finds the first index >= the lower bound, bisect_right finds
// the first index > the upper bound, so the resulting slice is inclusive on
// BOTH ends of the [updated_at-window, updated_at+window] range.
func DeriveHeuristicEdges(inputs HeuristicInputs, buildTime time.Time) HeuristicResult {
	if inputs.HeuristicDaysWindow == 0 {
		return HeuristicResult{}
	}

	prsByRepo := make(map[string][]HeuristicPullRequestRow)
	for _, pr := range inputs.PullRequests {
		// Python's grouping loop drops a row with a null created_at before
		// it ever enters the per-repo list (builder.py:1084-1085).
		if pr.CreatedAt.IsZero() {
			continue
		}
		key := pr.RepoID.String()
		prsByRepo[key] = append(prsByRepo[key], pr)
	}
	for key, prs := range prsByRepo {
		// SliceStable, not Slice: Python's list.sort() is stable, and two PRs
		// sharing one created_at value (a real possibility in seeded/test
		// data, however rare in production) would otherwise let an
		// unspecified Go sort order pick a different "first, tied" winner in
		// the nearest-match loop below than Python's stable sort would.
		sort.SliceStable(prs, func(i, j int) bool { return prs[i].CreatedAt.Before(prs[j].CreatedAt) })
		prsByRepo[key] = prs
	}

	linkedWorkItems := make(map[string]struct{}, len(inputs.ExplicitLinks))
	linkedPairs := make(map[ExplicitLink]struct{}, len(inputs.ExplicitLinks))
	for _, link := range inputs.ExplicitLinks {
		linkedWorkItems[link.WorkItemID] = struct{}{}
		linkedPairs[link] = struct{}{}
	}

	windowDuration := time.Duration(inputs.HeuristicDaysWindow) * 24 * time.Hour
	evidence := fmt.Sprintf("time_window_%dd", inputs.HeuristicDaysWindow)

	var result HeuristicResult
	for _, workItem := range inputs.WorkItems {
		repoKey := workItem.RepoID.String()
		prs, ok := prsByRepo[repoKey]
		if !ok {
			continue
		}
		if _, linked := linkedWorkItems[workItem.WorkItemID]; linked {
			continue
		}
		if workItem.UpdatedAt.IsZero() {
			continue
		}

		lowerBound := workItem.UpdatedAt.Add(-windowDuration)
		upperBound := workItem.UpdatedAt.Add(windowDuration)
		left := sort.Search(len(prs), func(i int) bool { return !prs[i].CreatedAt.Before(lowerBound) })
		right := sort.Search(len(prs), func(i int) bool { return prs[i].CreatedAt.After(upperBound) })

		var best *HeuristicPullRequestRow
		var bestDiff time.Duration
		for index := left; index < right; index++ {
			candidate := prs[index]
			if _, linked := linkedPairs[ExplicitLink{WorkItemID: workItem.WorkItemID, PRNumber: candidate.Number}]; linked {
				continue
			}
			diff := candidate.CreatedAt.Sub(workItem.UpdatedAt)
			if diff < 0 {
				diff = -diff
			}
			if best == nil || diff < bestDiff {
				pinned := candidate
				best = &pinned
				bestDiff = diff
			}
		}
		if best == nil {
			continue
		}

		eventTs := workItem.UpdatedAt
		if best.CreatedAt.After(eventTs) {
			eventTs = best.CreatedAt
		}

		repoID := workItem.RepoID
		prID := edges.GeneratePRID(repoID, best.Number)
		edgeID := edges.EdgeID(edges.NodeTypePR, prID, edges.EdgeTypeRelates, edges.NodeTypeIssue, workItem.WorkItemID)

		result.Edges = append(result.Edges, edges.Row{
			EdgeID:       edgeID,
			SourceType:   edges.NodeTypePR,
			SourceID:     prID,
			TargetType:   edges.NodeTypeIssue,
			TargetID:     workItem.WorkItemID,
			EdgeType:     edges.EdgeTypeRelates,
			Provenance:   edges.ProvenanceHeuristic,
			Confidence:   inputs.HeuristicConfidence,
			Evidence:     evidence,
			RepoID:       &repoID,
			DiscoveredAt: buildTime,
			LastSynced:   buildTime,
			EventTs:      eventTs,
			Day:          edges.DayFor(eventTs),
		})
		result.Links = append(result.Links, issueprlinks.Link{
			RepoID:     repoID,
			WorkItemID: workItem.WorkItemID,
			PRNumber:   uint32(best.Number),
			Confidence: inputs.HeuristicConfidence,
			Provenance: edges.ProvenanceHeuristic,
			Evidence:   evidence,
		})
	}
	return result
}
