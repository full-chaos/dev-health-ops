package investment

import (
	"sort"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/chquery"
	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/chwrite"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/units"
	"github.com/google/uuid"
)

const allocationSourceTeamOwnership = "team_ownership"

// The five outcomes below partition every in-window component this run
// materialized. They exist so the run's log line can say WHY the fallback did
// not fire, not merely that it did not: "0 allocated" alone cannot tell a run
// whose units all had direct evidence from a run whose ownership sync is
// broken. Every one of them is reported on every run, including as zero.
const (
	ownershipOutcomeAllocated       = "allocated"
	ownershipOutcomeOwnRepo         = "own_repo"
	ownershipOutcomeStrongerEffort  = "stronger_allocation"
	ownershipOutcomeDirectRepo      = "direct_repo_evidence"
	ownershipOutcomeNoEligibleOwner = "no_eligible_owner"
)

// allocateTeamOwnership is the final repository fallback. Each distinct owned
// repo receives the same share, independent of duplicate ownership records or
// the number of donor teams. Ranking columns are not effort measurements.
// Existing code and hierarchy evidence always keeps precedence. The scalar
// investment repo stays nil: an NxM allocation has no single primary repo.
//
// The second return value is the outcome this component contributed to the
// run's partition; it is never empty.
func allocateTeamOwnership(result MaterializeComponentResult, input MaterializeComponentInput, donors []chquery.TeamRepoDonor) ([]chwrite.RepoEffortRecord, string) {
	if result.Investment.RepoID != nil {
		return result.RepoEffort, ownershipOutcomeOwnRepo
	}
	for _, record := range result.RepoEffort {
		if record.AllocationSource != units.AllocationSourceEmpty && record.AllocationSource != units.AllocationSourceActiveHoursUnassign {
			return result.RepoEffort, ownershipOutcomeStrongerEffort
		}
	}
	// Ambiguous or zero-churn direct code evidence is still stronger than a
	// team convention. Do not manufacture a primary repo to erase ambiguity.
	for _, edge := range input.Component.Edges {
		if repo := units.ParseRepoID(input.EdgeRepoIDs[edge.EdgeID]); repo != nil && *repo != uuid.Nil {
			return result.RepoEffort, ownershipOutcomeDirectRepo
		}
	}
	issues := make(map[string]bool)
	for _, node := range input.Component.Nodes {
		switch node.Type {
		case "issue":
			issues[node.ID] = true
		case "pr":
			if repo, _, ok := units.ParsePRFromID(node.ID); ok && repo != nil && *repo != uuid.Nil {
				return result.RepoEffort, ownershipOutcomeDirectRepo
			}
		case "commit":
			if repo, _, ok := units.ParseCommitFromID(node.ID); ok && repo != nil && *repo != uuid.Nil {
				return result.RepoEffort, ownershipOutcomeDirectRepo
			}
		}
	}
	if len(donors) == 0 {
		return result.RepoEffort, ownershipOutcomeNoEligibleOwner
	}
	teamsByRepo := map[string]map[string]bool{}
	for _, donor := range donors {
		if !issues[donor.WorkItemID] || donor.RepoID == uuid.Nil || donor.TeamID == "" {
			continue
		}
		repo := donor.RepoID.String()
		if teamsByRepo[repo] == nil {
			teamsByRepo[repo] = map[string]bool{}
		}
		teamsByRepo[repo][donor.TeamID] = true
	}
	if len(teamsByRepo) == 0 {
		return result.RepoEffort, ownershipOutcomeNoEligibleOwner
	}
	repos := make([]string, 0, len(teamsByRepo))
	for repo := range teamsByRepo {
		repos = append(repos, repo)
	}
	sort.Strings(repos)
	share := 1.0 / float64(len(repos))
	records := make([]chwrite.RepoEffortRecord, 0, len(repos))
	for _, repo := range repos {
		teams := make([]string, 0, len(teamsByRepo[repo]))
		for team := range teamsByRepo[repo] {
			teams = append(teams, team)
		}
		sort.Strings(teams)
		provenance := "team:" + strings.Join(teams, ",")
		id := uuid.MustParse(repo)
		records = append(records, chwrite.RepoEffortRecord{
			WorkUnitID: result.Investment.WorkUnitID, RepoID: &id, RepoSource: &provenance,
			EffortMetric: result.Investment.EffortMetric, EffortValue: result.Investment.EffortValue * share,
			AllocationWeight: share, AllocationSource: allocationSourceTeamOwnership,
		})
	}
	return records, ownershipOutcomeAllocated
}
