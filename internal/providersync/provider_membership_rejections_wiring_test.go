package providersync

import (
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/teamattribution"
)

// TestBuildGitLabWorkItemDerivedEffectsCarriesMembershipRejections,
// TestBuildJiraWorkItemDerivedEffectsCarriesMembershipRejections, and
// TestBuildLinearWorkItemDerivedEffectsCarriesMembershipRejections are
// mutation-resistant pins for a test-strength gap: each of
// these three builders has its OWN copy of the "marshal rows.MembershipRejections
// and attach it to the work_item_team_attributions effect" step (GitHub's
// equivalent attach point, buildGitHubWorkItemsRouteEffects, already has a
// real-pipeline test), but no test called any of the three with
// rows.MembershipRejections populated -- so independently dropping any one
// provider's attach assignment passed the full committed suite. Each test
// calls the real production builder end to end and decodes the result with
// the real reader, decodeGitHubWorkItemTeamAttributionRejections.
func TestBuildGitLabWorkItemDerivedEffectsCarriesMembershipRejections(t *testing.T) {
	teamID := "nonowner"
	effects, err := BuildGitLabWorkItemDerivedEffects(GitLabWorkItemDerivedEffectRows{
		MembershipRejections: []teamattribution.GithubWorkItemDerivationRejectedMembership{{
			WorkItemID: "acme/api#1", Provider: "gitlab", Source: "assignee_membership",
			TeamID: &teamID, Reason: "repo_not_owned",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	assertMembershipRejectionsCarried(t, "gitlab", effects)
}

func TestBuildJiraWorkItemDerivedEffectsCarriesMembershipRejections(t *testing.T) {
	teamID := "nonowner"
	effects, err := BuildJiraWorkItemDerivedEffects(JiraWorkItemDerivedEffectRows{
		MembershipRejections: []teamattribution.GithubWorkItemDerivationRejectedMembership{{
			WorkItemID: "acme/api#1", Provider: "jira", Source: "assignee_membership",
			TeamID: &teamID, Reason: "repo_not_owned",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	assertMembershipRejectionsCarried(t, "jira", effects)
}

func TestBuildLinearWorkItemDerivedEffectsCarriesMembershipRejections(t *testing.T) {
	teamID := "nonowner"
	effects, err := BuildLinearWorkItemDerivedEffects(LinearWorkItemDerivedEffectRows{
		MembershipRejections: []teamattribution.GithubWorkItemDerivationRejectedMembership{{
			WorkItemID: "acme/api#1", Provider: "linear", Source: "assignee_membership",
			TeamID: &teamID, Reason: "repo_not_owned",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	assertMembershipRejectionsCarried(t, "linear", effects)
}

func assertMembershipRejectionsCarried(t *testing.T, provider string, effects []EffectBatch) {
	t.Helper()
	var teamAttributions *EffectBatch
	for index := range effects {
		if effects[index].Destination == githubTeamAttributionsDestination {
			teamAttributions = &effects[index]
		}
	}
	if teamAttributions == nil {
		t.Fatalf("%s: no work_item_team_attributions effect in the built effects", provider)
	}
	decoded, err := decodeGitHubWorkItemTeamAttributionRejections(teamAttributions.MembershipRejections)
	if err != nil {
		t.Fatal(err)
	}
	if len(decoded) != 1 || decoded[0].Provider != provider ||
		decoded[0].Source != "assignee_membership" || decoded[0].Reason != "repo_not_owned" {
		t.Fatalf(
			"%s: decoded rejections = %+v, want exactly the one %s rejection -- the builder's own "+
				"attach point for this provider dropped it",
			provider, decoded, provider,
		)
	}
}
