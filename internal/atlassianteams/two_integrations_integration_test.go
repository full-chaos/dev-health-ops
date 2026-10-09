//go:build integration

package atlassianteams

import (
	"context"
	"testing"
	"time"
)

// The Atlassian team rows carry no integration key, so a run proves its own
// site only. One organization, two Jira integrations: site A answers the
// standard teams, site B answers one other team. The run of B must not
// deactivate a team of A or close a membership or a link of A while A is
// active, and says so; when A is not active, B is the only owner; and one
// integration still retracts what its own answer lost.
func TestTwoJiraIntegrationsOfOneOrganizationKeepEachOthersAtlassianTeams(t *testing.T) {
	const teamZ = "ari:cloud:identity::team/99999999-0000-4000-8000-000000000009"
	siteB := func(req request) (int, any) {
		switch req.Operation {
		case "TeamSearchV2":
			return 200, searchPage("", teamNode(teamZ, "Other Site", "ACTIVE"))
		case "TeamworkGraphTeamUsers":
			return 200, connection("teamworkGraph_teamUsers", "", userEdge(teamZ, "zoe-9"))
		case "TeamConnectedContainers":
			return 200, containerPage("")
		}
		return 500, nil
	}
	siteAShrunk := func(req request) (int, any) {
		switch req.Operation {
		case "TeamSearchV2":
			return 200, searchPage("", teamNode(teamA, "Platform", "ACTIVE"))
		case "TeamworkGraphTeamUsers":
			return 200, connection("teamworkGraph_teamUsers", "")
		case "TeamConnectedContainers":
			return 200, containerPage("")
		}
		return 500, nil
	}
	for _, c := range []struct {
		name string
		// census is the organization's Jira integrations at the second run.
		census        integrationCensus
		second        string
		secondSite    func(request) (int, any)
		wantRetracted bool
		// wantState is open memberships of A's teams, open links of A's
		// teams, active teams of site A, after the second run.
		wantUnchanged bool
	}{
		{name: "two active integrations: the run of B closes and deactivates nothing of A",
			census: integrationCensus{"integration-a": true, "integration-b": true}, second: "integration-b", secondSite: siteB, wantUnchanged: true},
		{name: "A is not active any more: B is the only owner and retracts what its answer does not hold",
			census: integrationCensus{"integration-a": false, "integration-b": true}, second: "integration-b", secondSite: siteB, wantRetracted: true},
		{name: "control, one integration: its next run retracts what it lost",
			census: integrationCensus{"integration-a": true}, second: "integration-a", secondSite: siteAShrunk, wantRetracted: true},
	} {
		t.Run(c.name, func(t *testing.T) {
			conn := openClickHouse(t)
			ctx := context.Background()
			const org = "org-1"
			first := time.Date(2026, 9, 25, 3, 0, 0, 0, time.UTC)
			g := newGateway(t, standard)
			run := func(now time.Time, respond func(request) (int, any), census integrationCensus, integrationID string) Result {
				t.Helper()
				g.respond = respond
				p := params(everything)
				p.Now = now
				rows, err := Collect(ctx, g.client(), p)
				if err != nil {
					t.Fatal(err)
				}
				result, err := writeForScopeTest(ctx, conn, org, rows, census, integrationID)
				if err != nil {
					t.Fatal(err)
				}
				return result
			}
			// The rows of site A: everything but the team of site B.
			state := func() [3]string {
				t.Helper()
				return [3]string{
					lines(t, conn, `SELECT toString(count()) FROM team_memberships FINAL WHERE org_id = 'org-1' AND provider = 'jira' AND source = 'native' AND valid_to IS NULL AND team_id != 'jira:99999999-0000-4000-8000-000000000009'`)[0],
					lines(t, conn, `SELECT toString(count()) FROM team_project_ownership FINAL WHERE org_id = 'org-1' AND provider = 'jira' AND source = 'native' AND valid_to IS NULL AND team_id != 'jira:99999999-0000-4000-8000-000000000009'`)[0],
					lines(t, conn, `SELECT toString(count()) FROM teams FINAL WHERE org_id = 'org-1' AND provider = 'jira' AND is_active = 1 AND id != 'jira:99999999-0000-4000-8000-000000000009'`)[0],
				}
			}
			run(first, standard, integrationCensus{"integration-a": true}, "integration-a")
			before := state()
			if before[0] == "0" || before[1] == "0" || before[2] == "0" {
				t.Fatalf("the run of A left memberships/links/active teams = %v: the test measures nothing", before)
			}
			result := run(first.Add(time.Hour), c.secondSite, c.census, c.second)
			after := state()
			if c.wantUnchanged {
				if result.ExpiredMemberships != 0 || result.ExpiredOwnership != 0 || result.DeactivatedTeams != 0 {
					t.Errorf("the second run retracted %d memberships, %d links and deactivated %d teams, want nothing",
						result.ExpiredMemberships, result.ExpiredOwnership, result.DeactivatedTeams)
				}
				if after != before {
					t.Errorf("open memberships/links/active teams of site A = %v after the run of B, want %v unchanged", after, before)
				}
				return
			}
			if !c.wantRetracted || result.ExpiredMemberships == 0 || result.ExpiredOwnership == 0 || result.DeactivatedTeams == 0 {
				t.Errorf("the second run retracted %d memberships, %d links and deactivated %d teams, want each above 0",
					result.ExpiredMemberships, result.ExpiredOwnership, result.DeactivatedTeams)
			}
			if after == before {
				t.Errorf("open memberships/links/active teams of site A = %v, unchanged: the only active integration did not retract", after)
			}
		})
	}
}
