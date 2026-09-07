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

// TeamOwnershipSource names the team whose repo ownership a cascade inherited
// from (CHAOS-5459), in the same "<kind>:<id>" shape AncestorSource uses --
// so a repo_source value always says which specific record produced it, not
// merely which tier fired.
func TeamOwnershipSource(teamID string) string {
	return "team:" + teamID
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
	// Source is the repo_source provenance string (migration 089): which
	// specific ancestor issue, child tier, or team produced this repo.
	Source string
	// AllocationSource is the work_unit_repo_effort.allocation_source value
	// this cascade writes. It is carried EXPLICITLY rather than derived from
	// Source: the two tiers below share one "inherited" concept but are
	// separately alertable, and a reader partitioning coverage by
	// allocation_source must never have to parse a provenance string to learn
	// which tier fired.
	AllocationSource string
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
//
//  2. Failing that, collect every direct child of every member issue
//     (workItems whose ParentID names a member issue) that itself belongs to
//     an own-resolved component. If at least one such child exists and every
//     one agrees on a single repo, inherit it.
//
//  3. Failing BOTH of those, fall back to the owning TEAM's repository
//     (CHAOS-5459): if every member issue that has a team-owned repo agrees
//     on one repo, inherit it, tagged `team:<team_id>`. This is the tier that
//     covers the shape neither hierarchy tier can reach -- a standalone
//     Linear issue with no parent, no children, and no linked PR. Measured on
//     org 70d529e0 run c8065d4a8c1648568d369519045747f2: 767 of 1365 units
//     ended unresolved, 767/767 of them Linear issues on one team that owns
//     six repositories, and only 200 of their 825 member issues had any
//     parent_id at all -- so the hierarchy tiers were structurally incapable
//     of reaching 76% of the residue, no matter how deep they walked.
//
//  4. Otherwise the component stays unassigned -- including the known
//     ceiling that an ancestor/child issue outside every component in THIS
//     run (dropped, isolated, or not fetched) is invisible to issueComponent
//     and therefore out of scope for this run (RISK-NOTES: cross-run
//     ancestors are a follow-up ticket, not fixed here).
//
// Tier ORDER is the point, not just tier membership: a team-owned repo is the
// weakest signal here (it says who owns the code, not which code this unit
// touched), so it must never displace an ancestor or child that resolved a
// repo from real churn. It runs last and only on what the others left.
//
// This is explicitly NOT component fusion (CHAOS-2774): no component is
// merged, no work_unit_id changes. Only the repo attribution on the SAME
// unit gains an inherited value.
func computeRepoHierarchyCascade(
	components []units.Component,
	ownRepoByComponent map[int]*uuid.UUID,
	workItems map[string]chquery.WorkItem,
	issueComponent map[string]int,
	teamRepos map[string]chquery.TeamOwnedRepo,
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
			continue
		}
		if cascade, ok := teamOwnershipCascade(memberIssues, teamRepos); ok {
			results[idx] = cascade
		}
	}
	return results
}

// teamOwnershipCascade resolves a component from its member issues' owning
// team (CHAOS-5459).
//
// Unanimity is required, exactly as in the two tiers above: if the member
// issues resolve to more than one distinct repository -- a component fusing
// issues from two teams that own different code -- there is no honest single
// answer and the component stays unassigned rather than picking a winner. An
// issue with no team-owned repo is SKIPPED, not treated as disagreement:
// otherwise one untriaged issue joining a component would silently delete the
// attribution of every other issue in it.
//
// The chosen team id comes from the lexicographically first member issue that
// carries the winning repo, so a multi-issue component's repo_source is
// stable run to run rather than dependent on map iteration order.
func teamOwnershipCascade(
	memberIssues []string,
	teamRepos map[string]chquery.TeamOwnedRepo,
) (repoCascade, bool) {
	distinct := map[string]*uuid.UUID{}
	teamByRepo := map[string]string{}
	for _, issueID := range memberIssues {
		owned, ok := teamRepos[issueID]
		if !ok || owned.RepoID == "" {
			continue
		}
		repoID := units.ParseRepoID(owned.RepoID)
		if repoID == nil {
			// A malformed repo_id is a data defect, not a reason to attribute
			// this unit somewhere else. Skipping keeps it unassigned; the
			// loader's own SELECT already excludes NULLs, so reaching here at
			// all means the stored value is not a UUID.
			continue
		}
		key := repoID.String()
		distinct[key] = repoID
		// memberIssues is sorted by the caller, so the FIRST writer wins and
		// the result does not depend on map ordering.
		if _, seen := teamByRepo[key]; !seen {
			teamByRepo[key] = owned.TeamID
		}
	}
	if len(distinct) != 1 {
		return repoCascade{}, false
	}
	for key, repoID := range distinct {
		return repoCascade{
			RepoID:           repoID,
			Source:           TeamOwnershipSource(teamByRepo[key]),
			AllocationSource: units.AllocationSourceTeamOwnership,
		}, true
	}
	return repoCascade{}, false
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
	return repoCascade{
		RepoID:           nearest.repoID,
		Source:           AncestorSource(nearest.issueID),
		AllocationSource: units.AllocationSourceHierarchyCascade,
	}, true
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
		return repoCascade{
			RepoID:           repoID,
			Source:           RepoSourceChildren,
			AllocationSource: units.AllocationSourceHierarchyCascade,
		}, true
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
