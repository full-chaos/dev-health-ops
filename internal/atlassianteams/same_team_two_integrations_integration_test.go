//go:build integration

package atlassianteams

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providersync"
)

// Two active Jira integrations of one organization that BOTH return team A
// (two sites of one Atlassian organization share its teams). The run of B
// sees team A with no member and no link. The membership kind and the link
// kind must each be behind the scope proof: nothing of team A is closed.
// Control: the same answer from the sole integration closes them.
func TestTwoJiraIntegrationsThatReturnTheSameAtlassianTeam(t *testing.T) {
	onlyAEmpty := func(req request) (int, any) {
		switch req.Operation {
		case "TeamSearchV2":
			return 200, searchPage("", teamNode(teamA, "Platform", "ACTIVE"), teamNode(teamB, "Old", "ARCHIVED"), teamNode(teamC, "Data", "ACTIVE"))
		case "TeamworkGraphTeamUsers":
			return 200, connection("teamworkGraph_teamUsers", "")
		case "TeamConnectedContainers":
			return 200, containerPage("")
		}
		return 500, nil
	}
	for _, c := range []struct {
		name     string
		census   integrationCensus
		wantKept bool
	}{
		{"two active integrations", integrationCensus{"integration-a": true, "integration-b": true}, true},
		{"control: sole integration", integrationCensus{"integration-b": true}, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			conn := openClickHouse(t)
			ctx := context.Background()
			first := time.Date(2026, 9, 25, 3, 0, 0, 0, time.UTC)
			g := newGateway(t, standard)
			run := func(now time.Time, respond func(request) (int, any), census integrationCensus, id string) Result {
				t.Helper()
				g.respond = respond
				p := params(everything)
				p.Now = now
				rows, err := Collect(ctx, g.client(), p)
				if err != nil {
					t.Fatal(err)
				}
				result, err := writeForScopeTest(ctx, conn, "org-1", rows, census, id)
				if err != nil {
					t.Fatal(err)
				}
				return result
			}
			state := func() [2]string {
				return [2]string{
					lines(t, conn, `SELECT toString(count()) FROM team_memberships FINAL WHERE org_id = 'org-1' AND provider = 'jira' AND source = 'native' AND valid_to IS NULL`)[0],
					lines(t, conn, `SELECT toString(count()) FROM team_project_ownership FINAL WHERE org_id = 'org-1' AND provider = 'jira' AND source = 'native' AND valid_to IS NULL`)[0],
				}
			}
			run(first, standard, integrationCensus{"integration-a": true}, "integration-a")
			before := state()
			result := run(first.Add(time.Hour), onlyAEmpty, c.census, "integration-b")
			after := state()
			t.Logf("memberships/links before=%v after=%v expiredMemberships=%d expiredOwnership=%d abandoned=%v", before, after, result.ExpiredMemberships, result.ExpiredOwnership, result.CloseAbandoned)
			if before[0] == "0" || before[1] == "0" {
				t.Fatalf("setup: the run of A left memberships/links = %v: the test measures nothing", before)
			}
			if c.wantKept && (after != before || result.ExpiredMemberships != 0 || result.ExpiredOwnership != 0) {
				t.Errorf("the run of B closed rows of a team both integrations return: %v -> %v", before, after)
			}
			// The result names why the close was given up, and names nothing
			// when the run closed.
			if shared := slices.Contains(result.CloseAbandoned, providersync.OwnershipCloseSkippedScopeShared); shared != c.wantKept {
				t.Errorf("abandoned = %v, want scope_shared named: %v", result.CloseAbandoned, c.wantKept)
			}
			if !c.wantKept && len(result.CloseAbandoned) != 0 {
				t.Errorf("control: abandoned = %v, want none", result.CloseAbandoned)
			}
			if !c.wantKept && (result.ExpiredMemberships == 0 || result.ExpiredOwnership == 0) {
				t.Errorf("control: the sole integration closed %d memberships and %d links, want each above 0", result.ExpiredMemberships, result.ExpiredOwnership)
			}
		})
	}
}
