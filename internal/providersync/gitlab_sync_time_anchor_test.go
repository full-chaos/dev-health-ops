package providersync

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/teamattribution"
)

// anchoringFakeSource is the fake context source plus the anchoring hook the
// ClickHouse source has: it names the paths of the one renamed project.
type anchoringFakeSource struct {
	*fakeGitHubWorkItemDerivationContextSource
	anchored int
	seen     []string
}

func (source *anchoringFakeSource) AnchorGitLabSubjects(_ context.Context, subjects map[string]teamattribution.GithubWorkItemDerivationSubject) error {
	source.anchored++
	for id, subject := range subjects {
		source.seen = append(source.seen, id)
		subject.GitLabPaths = []string{"platform/api-v2", "acme/api"}
		subjects[id] = subject
	}
	return nil
}

// CHAOS-3383 review round 3: a sync-time GitLab item of a renamed project
// (catalog project_id, no paths on the freshly written row) must resolve through
// a full-name repo-ownership grant the way it did when project_id held the
// current path.
func TestSyncTimeRenamedGitLabItemResolvesThroughAFullNameOwnershipGrant(t *testing.T) {
	now := time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)
	claim := nativeTestClaim("gitlab", "work-items")
	catalog, identity := "org-acme:gitlab:123", "acme/api"
	repos, _, _ := teamattribution.RemapLegacyGitLabRepoFacts(claim.OrgID, []teamattribution.GithubWorkItemDerivationRepoFact{
		{Provider: "gitlab", TeamID: "owner-team", TeamName: "Owner", RepoFullName: "platform/api-v2", IsPrimary: 1, Specificity: 100},
	}, map[string]string{"platform/api-v2": "123"}, nil)
	donorID := "gitlab:acme/other#9"
	donorProject, donorKey := "org-acme:gitlab:123", "acme/api"
	source := &anchoringFakeSource{fakeGitHubWorkItemDerivationContextSource: &fakeGitHubWorkItemDerivationContextSource{
		facts: teamattribution.GithubWorkItemDerivationFacts{
			Teams: []teamattribution.GithubWorkItemDerivationTeamFact{{Provider: "gitlab", TeamID: "owner-team"}},
			Repos: repos,
			// A GitLab donor of the same renamed project.
			DonorItems: []teamattribution.GithubWorkItemDerivationSubject{{
				WorkItemID: donorID, Provider: "gitlab", ProjectID: &donorProject, ProjectKey: &donorKey, OrgID: claim.OrgID,
			}},
		},
	}}
	row := githubWorkItemRow{WorkItemID: "gitlab:acme/api#1", Provider: "gitlab", ProjectID: &catalog, ProjectKey: &identity, OrgID: claim.OrgID}
	derived, err := loadWorkItemDerivationContextForProvider(context.Background(), "gitlab", claim,
		githubWorkItemRows{WorkItems: []githubWorkItemRow{row}}, source, now)
	if err != nil {
		t.Fatal(err)
	}
	if source.anchored != 1 {
		t.Fatalf("anchoring hook called %d times, want 1", source.anchored)
	}
	donorSeen := false
	for _, id := range source.seen {
		donorSeen = donorSeen || id == donorID
	}
	if !donorSeen {
		t.Fatalf("the anchoring hook did not receive the GitLab donor: saw %v", source.seen)
	}
	donorTeam, _, _ := derived.Resolve(teamattribution.GithubWorkItemDerivationSubject{
		WorkItemID: donorID, Provider: "gitlab", ProjectID: &donorProject, ProjectKey: &donorKey, OrgID: claim.OrgID,
	})
	if donorTeam == nil || *donorTeam != "owner-team" {
		t.Fatalf("a renamed GitLab donor resolved to %v, want owner-team", donorTeam)
	}
	team, _, _ := derived.Resolve(githubWorkItemDerivationSubjectFromRow(row))
	if team == nil || *team != "owner-team" {
		got := "<nil>"
		if team != nil {
			got = *team
		}
		t.Fatalf("sync-time item resolved to %s, want owner-team", got)
	}
}
