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
	// RootIssueID is the issue whose OWN signal ultimately supplied RepoID --
	// not necessarily the relative this component inherited from directly.
	// With the fixed point below, an inheritance can be several hops from the
	// nearest own signal, and `ancestor:<the issue next door>` would name a
	// component that has no repo of its own either, which is useless to anyone
	// auditing where a repo came from. Carrying the transitive root keeps
	// repo_source's promise ("which specific record produced this") true at
	// every hop, with no schema change.
	RootIssueID string
	// Hops is 1 for a component that inherited directly from an own-signal
	// relative, 2 for one that inherited from a component resolved at hop 1,
	// and so on. Reported in the run's telemetry so a deepening cascade is
	// visible; it is NOT a metric label (the counter's `source` vocabulary
	// stays closed).
	Hops int
}

// hierarchyCascadeMaxPasses bounds the fixed point below. It reuses
// hierarchyCascadeMaxDepth's value for the same reason that limit exists: to
// guarantee termination on malformed data, not to model a real shape. The loop
// already terminates on its own -- every pass either resolves at least one new
// component or stops, and a resolved component is never revisited, so it
// cannot run longer than there are components -- but a hard cap means a future
// edit that accidentally makes a pass re-resolve something still terminates.
const hierarchyCascadeMaxPasses = hierarchyCascadeMaxDepth

// computeRepoHierarchyCascade implements R22 (CHAOS-5359) and its CHAOS-5459
// fixed-point correction: a component with no OWN repo resolution may inherit
// one through the issue hierarchy.
//
//  1. Walk each member issue's parent_id chain upward (bounded depth,
//     cycle-safe) to the nearest ancestor issue that is itself a member of a
//     DIFFERENT component which has a repo. If every such ancestor found
//     agrees on one repo, inherit it. Two ancestors resolving to different
//     repos is ambiguous -- no ancestor signal.
//  2. Failing that, collect every direct child of every member issue that
//     belongs to a component with a repo. If at least one such child exists
//     and every one agrees on a single repo, inherit it.
//  3. Otherwise the component stays unassigned.
//
// # WHY THIS RUNS TO A FIXED POINT, AND WHY THAT IS THE ACTUAL BUG
//
// The original implementation ran the two tiers ONCE against
// ownRepoByComponent, so it would only ever inherit from a component that had
// resolved its repo from its OWN signal. A component resolved BY THE CASCADE
// was invisible to its own children -- the cascade refused to chain.
//
// That is not a corner case. Measured on org 70d529e0 (run
// 23e2d31cc3c741c8992daf22f52b8e5b, 2026-09-07): of 821 unresolved issues, 198
// have a parent_id, and 178 of those have a parent that DOES have a repo --
// 97 via `ancestor:*`, 81 via `children`, and **zero** via `own_edges`. Every
// one of the 178 was blocked by this single rule. A further 91 have a
// PR-linked or resolved GRANDPARENT, which falls out for free once hop 1
// resolves the parent.
//
// So the tiers now run repeatedly against a `resolved` map seeded from the own
// signals and GROWN by each pass, until a pass adds nothing (or the cap
// trips). Termination is structural: a component is only ever written once
// (`if _, done := resolved[idx]; done { continue }`), so each pass strictly
// shrinks the unresolved set or ends the loop -- that is also the
// component-level cycle guard, and it is why a parent/child cycle between two
// components cannot ping-pong. The per-issue `visited` map inside
// ancestorCascade still guards cycles WITHIN one parent chain; the two guards
// are at different levels and both are needed.
//
// Tier ORDER is preserved within each pass: ancestor before children, and a
// component resolved in an earlier pass is never revisited or upgraded.
//
// This is explicitly NOT component fusion (CHAOS-2774): no component is
// merged, no work_unit_id changes.
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

	// memberIssuesByComponent is computed once rather than per pass: it is
	// pure structure and does not change as repos are resolved.
	memberIssuesByComponent := make(map[int][]string, len(components))
	for idx, component := range components {
		memberIssues, _, _ := splitNodeIDsByType(dedupeNodeKeys(component.Nodes))
		if len(memberIssues) == 0 {
			continue
		}
		sort.Strings(memberIssues)
		memberIssuesByComponent[idx] = memberIssues
	}

	// resolved is the inheritance SOURCE and it grows every pass. It starts as
	// the own signals and takes each pass's results; `results` is the OUTPUT
	// (own-signal components are not cascade results and never appear there).
	resolved := make(map[int]*uuid.UUID, len(components))
	for idx, repoID := range ownRepoByComponent {
		if repoID != nil {
			resolved[idx] = repoID
		}
	}
	results := make(map[int]repoCascade, len(components))

	for pass := 1; pass <= hierarchyCascadeMaxPasses; pass++ {
		found := make(map[int]repoCascade)
		for idx := range components {
			if _, done := resolved[idx]; done {
				continue
			}
			memberIssues, ok := memberIssuesByComponent[idx]
			if !ok {
				continue
			}
			if cascade, ok := ancestorCascade(idx, memberIssues, resolved, results, workItems, issueComponent); ok {
				found[idx] = cascade
				continue
			}
			if cascade, ok := childrenCascade(idx, memberIssues, resolved, results, childrenByParent, issueComponent); ok {
				found[idx] = cascade
			}
		}
		if len(found) == 0 {
			break
		}
		// Commit the pass ATOMICALLY. Applying results as they are discovered
		// inside the loop above would make a component's answer depend on
		// map iteration order -- component 7 would see component 3's brand-new
		// repo only if 3 happened to come first. Committing between passes
		// makes every component in a pass see exactly the same `resolved`
		// state, so the output is deterministic.
		for idx, cascade := range found {
			results[idx] = cascade
			resolved[idx] = cascade.RepoID
		}
	}

	return results
}

// inheritedFrom reports the repo a RELATIVE's component carries, plus the
// provenance to attribute it to: the transitive root issue whose own signal
// supplied it, and the hop count to add to. A component resolved from its own
// signal has no cascade entry, so the relative issue itself is the root.
func inheritedFrom(
	componentIdx int,
	relativeIssueID string,
	resolved map[int]*uuid.UUID,
	cascades map[int]repoCascade,
) (repoID *uuid.UUID, rootIssueID string, hops int, ok bool) {
	repoID, ok = resolved[componentIdx]
	if !ok || repoID == nil {
		return nil, "", 0, false
	}
	if cascade, cascaded := cascades[componentIdx]; cascaded {
		return repoID, cascade.RootIssueID, cascade.Hops, true
	}
	return repoID, relativeIssueID, 0, true
}

// ancestorCascade walks every member issue's parent chain independently and
// requires every ancestor found across all of them to agree on one repo.
//
// It reads `resolved` (own signals PLUS everything earlier passes established),
// not the own-signal map alone -- that widening IS the CHAOS-5459 fix. The
// per-issue `visited` set still guards a cycle inside one parent chain.
func ancestorCascade(
	componentIdx int,
	memberIssues []string,
	resolved map[int]*uuid.UUID,
	cascades map[int]repoCascade,
	workItems map[string]chquery.WorkItem,
	issueComponent map[string]int,
) (repoCascade, bool) {
	type found struct {
		repoID  *uuid.UUID
		rootID  string
		hops    int
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
				if repoID, rootID, hops, ok := inheritedFrom(parentIdx, parentID, resolved, cascades); ok {
					hits = append(hits, found{repoID: repoID, rootID: rootID, hops: hops, issueID: parentID, depth: depth})
					break // nearest resolved ancestor along THIS issue's own chain only.
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
		RepoID:      nearest.repoID,
		Source:      AncestorSource(nearest.rootID),
		RootIssueID: nearest.rootID,
		Hops:        nearest.hops + 1,
	}, true
}

// childrenCascade requires every direct child of every member issue that
// itself has a repo (own signal or an earlier pass's inheritance) to agree on
// one repo. A child with no component in this run, or whose component has no
// repo yet, is silently skipped -- only RESOLVED children must agree.
//
// The chosen root is taken from the lexicographically first child carrying the
// winning repo, so a multi-child component's repo_source is stable run to run
// rather than dependent on map iteration order.
func childrenCascade(
	componentIdx int,
	memberIssues []string,
	resolved map[int]*uuid.UUID,
	cascades map[int]repoCascade,
	childrenByParent map[string][]string,
	issueComponent map[string]int,
) (repoCascade, bool) {
	distinct := map[string]*uuid.UUID{}
	rootByRepo := map[string]string{}
	hopsByRepo := map[string]int{}
	for _, parentIssue := range memberIssues {
		children := append([]string(nil), childrenByParent[parentIssue]...)
		sort.Strings(children)
		for _, childID := range children {
			childIdx, memberOfComponent := issueComponent[childID]
			if !memberOfComponent || childIdx == componentIdx {
				continue
			}
			repoID, rootID, hops, ok := inheritedFrom(childIdx, childID, resolved, cascades)
			if !ok {
				continue
			}
			key := repoID.String()
			distinct[key] = repoID
			if _, seen := rootByRepo[key]; !seen {
				rootByRepo[key] = rootID
				hopsByRepo[key] = hops
			}
		}
	}
	if len(distinct) != 1 {
		return repoCascade{}, false
	}
	for key, repoID := range distinct {
		return repoCascade{
			RepoID:      repoID,
			Source:      RepoSourceChildren,
			RootIssueID: rootByRepo[key],
			Hops:        hopsByRepo[key] + 1,
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

// churnRepoForComponent is the CHAOS-5459 second own-signal source: the single
// repository a component's own PR or commit churn points at, or nil.
//
// # WHY THIS IS AN "OWN" SIGNAL AND NOT A CASCADE
//
// collectSingleRepoID reads repo_id off the component's EDGE rows. That is one
// way a unit's own work names a repository; the other is the PR/commit node
// ids the same component already carries, which is exactly what
// units.AllocateRepoEffort turns into repo-effort rows. Both say "this unit's
// own work happened here" -- neither is inherited from a relative -- so both
// belong in ownRepoByComponent. Only edges were seeded before, which is why a
// PR-linked parent could not be inherited from.
//
// # ONLY THE TWO CHURN TIERS, AND ONLY WHEN THEY AGREE ON ONE REPO
//
// AllocateRepoEffort's lower tiers (active_hours_unassigned, empty) carry a
// nil repo by construction and are not a signal at all. The two churn tiers
// are decided from CommitIDs/PRIDs and their churn maps ALONE -- EffortMetric
// and EffortValue are read only by the active-hours tier below them -- so a
// zero-valued effort here cannot change which tier fires or which repo it
// names. Passing them empty is therefore exact, not an approximation, and the
// guard below makes that structural: a result from any other tier is rejected
// outright rather than trusted.
//
// The single-repo requirement mirrors collectSingleRepoID's own `len(distinct)
// != 1` rule: a component spanning two repositories has no single answer, and
// inventing one here would let an ambiguous parent poison every descendant.
func churnRepoForComponent(component units.Component, entities entitySet) *uuid.UUID {
	_, prIDs, commitIDs := splitNodeIDsByType(dedupeNodeKeys(component.Nodes))
	if len(prIDs) == 0 && len(commitIDs) == 0 {
		return nil
	}

	allocations := units.AllocateRepoEffort(units.AllocateRepoEffortInput{
		PRIDs:       prIDs,
		CommitIDs:   commitIDs,
		PRChurn:     entities.PRChurn,
		CommitChurn: entities.CommitChurn,
	})

	var only *uuid.UUID
	for _, allocation := range allocations {
		if allocation.AllocationSource != units.AllocationSourceCommitChurn &&
			allocation.AllocationSource != units.AllocationSourcePRChurn {
			return nil
		}
		if allocation.RepoID == nil {
			// A churn row with no parseable repo id: the component's own
			// signal is incomplete, so it is not a trustworthy inheritance
			// source even if a sibling row does name a repo.
			return nil
		}
		if only == nil {
			only = allocation.RepoID
			continue
		}
		if *only != *allocation.RepoID {
			return nil // more than one repo: no single answer.
		}
	}
	return only
}
