package providersync

import (
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/teamattribution"
)

// TestTeamRepoOwnershipInferredOutranksProviderAccessGrant proves a
// Linear-inferred team_repo_ownership row wins the single-primary-owner
// pick over a GitHub provider_access row for the same repo.
// teamattribution.RankDerivationCandidates is the ranking every
// team_repo_ownership reader that picks one winner relies on (specificity
// highest first, then priority), so this exercises the real function with
// the real values both writers stamp via teamRepoOwnershipPrecedence.
func TestTeamRepoOwnershipInferredOutranksProviderAccessGrant(t *testing.T) {
	inferredTeam := "chaos"
	grantTeam := "ops-team"
	providerAccess := teamRepoOwnershipPrecedence[teamRepoOwnershipSourceKindProviderAccess]
	inferred := teamRepoOwnershipPrecedence[teamRepoOwnershipSourceKindInferred]

	candidates := []teamattribution.GithubWorkItemDerivationCandidate{
		{
			Source: "repo_ownership", TeamID: &grantTeam,
			IsPrimary: 0, Specificity: int(providerAccess.Specificity), Priority: int(providerAccess.Priority),
		},
		{
			Source: "repo_ownership", TeamID: &inferredTeam,
			IsPrimary: 0, Specificity: int(inferred.Specificity), Priority: int(inferred.Priority),
		},
	}

	ranked := teamattribution.RankDerivationCandidates(candidates)
	if got := *ranked[0].TeamID; got != inferredTeam {
		t.Fatalf("single-primary-owner pick = %q, want the Linear-inferred team %q -- a provider_access repository-access grant must not outrank the team an inference names as doing the work", got, inferredTeam)
	}
}
