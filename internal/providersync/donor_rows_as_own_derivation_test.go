package providersync

import "github.com/full-chaos/dev-health-ops/internal/teamattribution"

// withDonorRowsAsTheirOwnDerivation gives facts whose donors carry no stored
// row (a test double's facts) the row each donor's own derivation writes with
// the same facts: a donor that was attributed before, under the facts of this
// run (CHAOS-9460: a linked item copies its donor's row, never a resolution).
func withDonorRowsAsTheirOwnDerivation(facts teamattribution.GithubWorkItemDerivationFacts) teamattribution.GithubWorkItemDerivationFacts {
	if facts.DonorRows != nil {
		return facts
	}
	subjects := make(map[string]teamattribution.GithubWorkItemDerivationSubject, len(facts.DonorItems))
	ids := make(map[string]struct{}, len(facts.DonorItems))
	for _, donor := range facts.DonorItems {
		subjects[donor.WorkItemID] = donor
		ids[donor.WorkItemID] = struct{}{}
	}
	facts.DonorRows = teamattribution.NewGitHubWorkItemDerivationContext(facts).OwnDonorRows(subjects, ids)
	return facts
}
