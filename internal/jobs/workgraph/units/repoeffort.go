package units

import (
	"sort"

	"github.com/google/uuid"
)

// Allocation source names, from work_graph/investment/materialize.py's
// _allocate_repo_effort.
const (
	AllocationSourceCommitChurn         = "commit_churn"
	AllocationSourcePRChurn             = "pr_churn"
	AllocationSourceActiveHoursUnassign = "active_hours_unassigned"
	AllocationSourceEmpty               = "empty"
	// AllocationSourceHierarchyCascade (CHAOS-5359) marks a repo-effort row
	// whose repo_id was NOT derived from this unit's own commit/PR churn --
	// there was none, the "empty" tier fired -- but the issue-hierarchy
	// cascade (investment.computeRepoHierarchyCascade) found an ancestor or
	// unanimous-children resolution instead. Only the caller (materialize.go)
	// ever produces this value; AllocateRepoEffort itself never does, since it
	// has no view of any component but its own.
	AllocationSourceHierarchyCascade = "hierarchy_cascade"
)

// RepoAllocation is one row of _allocate_repo_effort's returned list --
// (repo_id, repo_effort, allocation_weight, allocation_source).
type RepoAllocation struct {
	RepoID           *uuid.UUID
	RepoEffort       float64
	AllocationWeight float64
	AllocationSource string
}

// AllocateRepoEffortInput carries what _allocate_repo_effort reads. Mirrors
// EffortInput's id/map shape (units/effort.go) -- same duplicate-preserving,
// map-driven contract.
type AllocateRepoEffortInput struct {
	IssueIDs     []string
	PRIDs        []string
	CommitIDs    []string
	PRChurn      map[string]float64
	CommitChurn  map[string]float64
	ActiveHours  map[string]float64
	EffortMetric string
	EffortValue  float64
}

// AllocateRepoEffort ports materialize._allocate_repo_effort (:1011-1069)
// exactly: a strict priority chain -- per-repo commit churn, then per-repo PR
// churn, then an unassigned active-hours row, then an empty row -- taking a
// tier only when ITS OWN total is strictly positive, same fall-through
// discipline as ComputeEffort.
//
// # THE TOTALS ARE PLAIN SEQUENTIAL ADDITION, NOT pythonparity.Sum
//
// This is the one place in this file that must NOT reuse effort.go's
// sumChurn. Python's commit_total/pr_total here are accumulated with a bare
// `total += churn` inside the loop -- ordinary IEEE-754 sequential addition --
// while sum(), which sumChurn ports, has used Neumaier compensated summation
// since CPython 3.12. The two algorithms can disagree in the last bit on the
// same input, and that bit decides which tier fires when a churn list nearly
// cancels to zero. Reaching for sumChurn "for consistency" here would be a
// silent divergence from the function it is meant to port.
func AllocateRepoEffort(input AllocateRepoEffortInput) []RepoAllocation {
	commitEffortByRepo := map[string]float64{}
	commitTotal := 0.0
	for _, commitID := range input.CommitIDs {
		churn := input.CommitChurn[commitID]
		commitTotal += churn
		if churn <= 0 {
			continue
		}
		repoKey := repoKeyFromCommitID(commitID)
		commitEffortByRepo[repoKey] += churn
	}
	if commitTotal > 0 {
		return allocationsFromRepoTotals(commitEffortByRepo, commitTotal, AllocationSourceCommitChurn)
	}

	prEffortByRepo := map[string]float64{}
	prTotal := 0.0
	for _, prID := range input.PRIDs {
		churn := input.PRChurn[prID]
		prTotal += churn
		if churn <= 0 {
			continue
		}
		repoKey := repoKeyFromPRID(prID)
		prEffortByRepo[repoKey] += churn
	}
	if prTotal > 0 {
		return allocationsFromRepoTotals(prEffortByRepo, prTotal, AllocationSourcePRChurn)
	}

	if input.EffortMetric == EffortMetricActiveHours && input.EffortValue > 0 {
		return []RepoAllocation{{
			RepoID: nil, RepoEffort: input.EffortValue, AllocationWeight: 1.0,
			AllocationSource: AllocationSourceActiveHoursUnassign,
		}}
	}

	return []RepoAllocation{{
		RepoID: nil, RepoEffort: 0.0, AllocationWeight: 0.0, AllocationSource: AllocationSourceEmpty,
	}}
}

// repoKeyFromCommitID is `str(repo_id) if repo_id else ""` where repo_id
// comes from parse_commit_from_id -- an empty key stands in for None,
// matching Python's use of the dict key itself as the None/present
// discriminator.
func repoKeyFromCommitID(commitID string) string {
	repoID, _, ok := ParseCommitFromID(commitID)
	if !ok || repoID == nil {
		return ""
	}
	return repoID.String()
}

// repoKeyFromPRID mirrors repoKeyFromCommitID for parse_pr_from_id.
func repoKeyFromPRID(prID string) string {
	repoID, _, ok := ParsePRFromID(prID)
	if !ok || repoID == nil {
		return ""
	}
	return repoID.String()
}

// allocationsFromRepoTotals ports the shared tail of both tiers:
// `[(_parse_repo_id(repo_key or None), repo_effort, repo_effort / total, source)
//
//	for repo_key, repo_effort in sorted(effort_by_repo.items())]`
//
// sorted(dict.items()) orders by the dict's own keys; Go string comparison
// is byte-wise, which agrees with Python's code-point ordering for the
// canonical (ASCII) UUID strings these keys always are.
func allocationsFromRepoTotals(effortByRepo map[string]float64, total float64, source string) []RepoAllocation {
	keys := make([]string, 0, len(effortByRepo))
	for key := range effortByRepo {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	allocations := make([]RepoAllocation, 0, len(keys))
	for _, key := range keys {
		repoEffort := effortByRepo[key]
		allocations = append(allocations, RepoAllocation{
			RepoID:           ParseRepoID(key),
			RepoEffort:       repoEffort,
			AllocationWeight: repoEffort / total,
			AllocationSource: source,
		})
	}
	return allocations
}
