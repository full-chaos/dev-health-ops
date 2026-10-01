package atlassianteams

import (
	"context"
	"testing"
	"time"

	"atlassian/atlassian"
)

// CHAOS-7132: tenantContexts answers a BARE organization UUID and teamSearchV2 refuses it ("Invalid
// Organization Ari: <uuid>"), which failed the whole jira sync on bigboy (0 of 14 units). The search
// takes the ARI form; an ARI the operator stored is passed through unchanged.
type organizationRecorder struct{ got string }

func (r *organizationRecorder) SearchTeams(_ context.Context, organizationID, _, _ string, _ int) ([]atlassian.AtlassianTeam, error) {
	r.got = organizationID
	return nil, nil
}
func (*organizationRecorder) IterTeamUsers(context.Context, string, int) ([]atlassian.TeamworkUserRelation, error) {
	return nil, nil
}
func (*organizationRecorder) IterTeamActiveProjects(context.Context, string, int) ([]atlassian.TeamworkProject, error) {
	return nil, nil
}

func TestCollectSearchesWithTheOrganizationAsAnARI(t *testing.T) {
	t.Parallel()
	const uuid = "a6bfe849-d604-4c9a-bced-35ce5fe54856"
	tests := []struct{ name, in, want string }{
		{"a bare UUID from tenantContexts", uuid, "ari:cloud:platform::org/" + uuid},
		{"a bare UUID with padding", "  " + uuid + " ", "ari:cloud:platform::org/" + uuid},
		{"an ARI already", "ari:cloud:platform::org/" + uuid, "ari:cloud:platform::org/" + uuid},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			client := &organizationRecorder{}
			_, err := Collect(context.Background(), client, Params{
				OrgID: "org", OrganizationID: test.in, SiteID: "site",
				Selections: Selections{Structure: true}, Now: time.Now(),
			})
			if err != nil {
				t.Fatalf("Collect: %v", err)
			}
			if client.got != test.want {
				t.Errorf("SearchTeams got organizationID %q, want %q", client.got, test.want)
			}
		})
	}
}
