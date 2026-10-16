package atlassianteams

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
)

// The catalog write stores the provider's team uuid (not the ARI) and the
// provider's name as the provider identity of a listed team.
func TestWriteTeamsStoresTheTeamUUIDAsProviderIdentity(t *testing.T) {
	const org = "org-1"
	now := time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)
	teams := []TeamRow{{
		ID: "jira:aaaaaaaa-0000-4000-8000-000000000001", TeamUUID: uuid.New(), Name: "Platform", IsActive: 1,
		UpdatedAt: now, OrgID: org, Provider: Provider,
		NativeTeamKey: "ari:cloud:identity::team/AAAAAAAA-0000-4000-8000-000000000001",
	}}
	conn := &createdConn{}
	if err := writeTeams(context.Background(), conn, org, teams, nil, now, false, nil); err != nil {
		t.Fatal(err)
	}
	row := conn.batch.appended[0]
	if got, want := row[len(row)-3], "aaaaaaaa-0000-4000-8000-000000000001"; got != want {
		t.Errorf("provider_team_id = %v, want %v", got, want)
	}
	if got := row[len(row)-2]; got != "Platform" {
		t.Errorf("provider_name = %v, want Platform", got)
	}
}

func TestProviderTeamUUID(t *testing.T) {
	for in, want := range map[string]string{
		"ari:cloud:identity::team/ABC-1": "abc-1",
		"  not-an-ari ":                  "not-an-ari",
		"":                               "",
	} {
		if got := providerTeamUUID(in); got != want {
			t.Errorf("providerTeamUUID(%q) = %q, want %q", in, got, want)
		}
	}
}
