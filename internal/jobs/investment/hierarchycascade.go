package investment

import (
	"sort"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/chquery"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/units"
)

// RepoSourceOwnEdges/Ancestor/Children are the repo_source provenance values
// (CHAOS-5359, 4452 design-of-record R22, migration 089). AncestorSource
// builds the "ancestor:<issue_id>" form.
const (
	RepoSourceOwnEdges = "own_edges"
	RepoSourceChildren = "children"
)

// AncestorSource names the specific ancestor issue a cascade inherited from.
func AncestorSource(issueID string) string {
	return "ancestor:" + issueID
}

// hierarchyCascadeMaxDepth bounds the upward parent_id walk. Ten hops is far
// past any real project/epic/task/sub-task nesting seen in the executed
// evidence (CHAOS-5359 ticket comment); it exists to guarantee termination
// on malformed data, not to model a real depth limit.
const hierarchyCascadeMaxDepth = 10

// repoCascade is one component's inherited repo attribution. Source is ""
// when no cascade signal was found (the component stays unassigned).
type repoCascade struct {
	RepoID *uuid.UUID
	Source string
}

// computeRepoHierarchyCascade implements R22 (CHAOS-5359, 4452
// design-of-record vol.2): a component with no OWN repo resolution
// (ownRepoByComponent[idx] == nil -- collectSingleRepoID found no single
// agreeing repo among the component's own PR/commit/repo edges, the
// "pure-issue component" shape that is 76.1% of unresolved
// work_unit_investments rows) may still inherit a repo through the issue
// hierarchy:
//
//  1. Walk each member issue's parent_id chain upward (bounded depth,
//     cycle-safe) to the nearest ancestor issue that is itself a member of a
//     DIFFERENT component which resolved its OWN repo. If every such
//     ancestor found (across every member issue's chain) agrees on one repo,
//     inherit it, tagged with the nearest one's issue id. Two ancestors
//     resolving to different repos is ambiguous -- no ancestor signal.
//  2. Failing that, collect every direct child of every member issue
//     (workItems whose ParentID names a member issue) that itself belongs to
//     an own-resolved component. If at least one such child exists and every
//     one agrees on a single repo, inherit it.
//  3. Otherwise the component stays unassigned -- including the known
//     ceiling that an ancestor/child issue outside every component in THIS
//     run (dropped, isolated, or not fetched) is invisible to issueComponent
//     and therefore out of scope for this run (RISK-NOTES: cross-run
//     ancestors are a follow-up ticket, not fixed here).
//
// This is explicitly NOT component fusion (CHAOS-2774): no component is
// merged, no work_unit_id changes. Only the repo attribution on the SAME
// unit gains an inherited value.
func computeRepoHierarchyCascade(
	components []units.Component,
	ownRepoByComponent map[int]*uuid.UUID,
	workItems map[string]chquery.WorkItem,
	issueComponent map[string]int,
) map[int]repoCascade {
	// childrenByParent is built once, org-wide over every work item this run
	// fetched (not just component members), so a child in a different
	// component from its parent is still found.
	childrenByParent := make(map[string][]string, len(workItems))
	for issueID, item := range workItems {
		if item.ParentID == "" {
			continue
		}
		childrenByParent[item.ParentID] = append(childrenByParent[item.ParentID], issueID)
	}

	results := make(map[int]repoCascade, len(components))
	for idx, component := range components {
		if ownRepoByComponent[idx] != nil {
			continue // own_edges already resolved; not this pass's concern.
		}
		memberIssues, _, _ := splitNodeIDsByType(dedupeNodeKeys(component.Nodes))
		if len(memberIssues) == 0 {
			continue
		}
		sort.Strings(memberIssues)

		if cascade, ok := ancestorCascade(idx, memberIssues, ownRepoByComponent, workItems, issueComponent); ok {
			results[idx] = cascade
			continue
		}
		if cascade, ok := childrenCascade(idx, memberIssues, ownRepoByComponent, childrenByParent, issueComponent); ok {
			results[idx] = cascade
		}
	}
	return results
}

// ancestorCascade walks every member issue's parent chain independently and
// requires every ancestor found across all of them to agree on one repo.
func ancestorCascade(
	componentIdx int,
	memberIssues []string,
	ownRepoByComponent map[int]*uuid.UUID,
	workItems map[string]chquery.WorkItem,
	issueComponent map[string]int,
) (repoCascade, bool) {
	type found struct {
		repoID  *uuid.UUID
		issueID string
		depth   int
	}
	var hits []found

	for _, startIssue := range memberIssues {
		visited := map[string]struct{}{startIssue: {}}
		current := startIssue
		for depth := 1; depth <= hierarchyCascadeMaxDepth; depth++ {
			item, ok := workItems[current]
			if !ok || item.ParentID == "" {
				break
			}
			parentID := item.ParentID
			if _, cyclic := visited[parentID]; cyclic {
				break
			}
			visited[parentID] = struct{}{}

			if parentIdx, memberOfComponent := issueComponent[parentID]; memberOfComponent && parentIdx != componentIdx {
				if repoID := ownRepoByComponent[parentIdx]; repoID != nil {
					hits = append(hits, found{repoID: repoID, issueID: parentID, depth: depth})
					break // nearest ancestor along THIS issue's own chain only.
				}
			}
			current = parentID
		}
	}

	if len(hits) == 0 {
		return repoCascade{}, false
	}
	distinct := map[string]*uuid.UUID{}
	for _, hit := range hits {
		distinct[hit.repoID.String()] = hit.repoID
	}
	if len(distinct) != 1 {
		return repoCascade{}, false // ambiguous multi-repo ancestor: no signal.
	}
	sort.Slice(hits, func(i, j int) bool {
		if hits[i].depth != hits[j].depth {
			return hits[i].depth < hits[j].depth
		}
		return hits[i].issueID < hits[j].issueID
	})
	nearest := hits[0]
	return repoCascade{RepoID: nearest.repoID, Source: AncestorSource(nearest.issueID)}, true
}

// childrenCascade requires every direct child of every member issue that
// itself resolves (via its own component) to agree on one repo. A child with
// no component in this run, or whose component has no own repo, is silently
// skipped -- only RESOLVED children must agree.
func childrenCascade(
	componentIdx int,
	memberIssues []string,
	ownRepoByComponent map[int]*uuid.UUID,
	childrenByParent map[string][]string,
	issueComponent map[string]int,
) (repoCascade, bool) {
	distinct := map[string]*uuid.UUID{}
	for _, parentIssue := range memberIssues {
		for _, childID := range childrenByParent[parentIssue] {
			childIdx, memberOfComponent := issueComponent[childID]
			if !memberOfComponent || childIdx == componentIdx {
				continue
			}
			repoID := ownRepoByComponent[childIdx]
			if repoID == nil {
				continue
			}
			distinct[repoID.String()] = repoID
		}
	}
	if len(distinct) != 1 {
		return repoCascade{}, false
	}
	for _, repoID := range distinct {
		return repoCascade{RepoID: repoID, Source: RepoSourceChildren}, true
	}
	return repoCascade{}, false
}

// buildIssueComponentIndex maps every issue node id in ANY component this
// run built to that component's index -- the run-scoped membership index
// both cascade passes above key off of.
func buildIssueComponentIndex(components []units.Component) map[string]int {
	index := make(map[string]int)
	for idx, component := range components {
		issueIDs, _, _ := splitNodeIDsByType(component.Nodes)
		for _, issueID := range issueIDs {
			index[issueID] = idx
		}
	}
	return index
}
