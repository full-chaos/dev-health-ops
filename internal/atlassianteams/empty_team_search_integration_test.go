//go:build integration

package atlassianteams

import (
	"context"
	"testing"
	"time"
)

// TestATeamSearchThatAnswersNoTeamClosesNothing runs the real collection and
// the real write against a real store. A team search that reaches its end and
// answers NO team is far more often an access change than an organization
// that deleted every team: it deactivates no team and closes no membership
// and no project link. The control (a search that still answers one team)
// closes what the other teams had, so a row left open is the rule's, not a
// harness that cannot close.
func TestATeamSearchThatAnswersNoTeamClosesNothing(t *testing.T) {
	conn := openClickHouse(t)
	ctx := context.Background()
	const org = "org-1"
	first := time.Date(2026, 9, 25, 3, 0, 0, 0, time.UTC)
	g := newGateway(t, standard)
	run := func(now time.Time, respond func(request) (int, any)) Result {
		t.Helper()
		g.respond = respond
		p := params(everything)
		p.Now = now
		rows, err := Collect(ctx, g.client(), p)
		if err != nil {
			t.Fatal(err)
		}
		result, err := Write(ctx, conn, org, rows, everything)
		if err != nil {
			t.Fatal(err)
		}
		return result
	}
	state := func() [3]string {
		t.Helper()
		return [3]string{
			lines(t, conn, `SELECT toString(count()) FROM team_memberships FINAL WHERE org_id = 'org-1' AND provider = 'jira' AND source = 'native' AND valid_to IS NULL`)[0],
			lines(t, conn, `SELECT toString(count()) FROM team_project_ownership FINAL WHERE org_id = 'org-1' AND provider = 'jira' AND source = 'native' AND valid_to IS NULL`)[0],
			lines(t, conn, `SELECT toString(count()) FROM teams FINAL WHERE org_id = 'org-1' AND provider = 'jira' AND is_active = 1`)[0],
		}
	}
	run(first, standard)
	before := state()
	if before[0] == "0" || before[1] == "0" || before[2] == "0" {
		t.Fatalf("the first run left memberships/links/active teams = %v: the test measures nothing", before)
	}

	noTeam := func(req request) (int, any) {
		if req.Operation == "TeamSearchV2" {
			// An explicit empty list: the search reached its end and answered
			// no team (a null or absent list is refused by the client).
			return 200, map[string]any{"data": map[string]any{"team": map[string]any{"teamSearchV2": map[string]any{
				"pageInfo": map[string]any{"hasNextPage": false}, "nodes": []any{}}}}}
		}
		return 500, nil
	}
	result := run(first.Add(time.Hour), noTeam)
	if result.ExpiredMemberships != 0 || result.ExpiredOwnership != 0 || result.DeactivatedTeams != 0 {
		t.Errorf("a search that answered no team retracted %d memberships, %d links and deactivated %d teams, want nothing",
			result.ExpiredMemberships, result.ExpiredOwnership, result.DeactivatedTeams)
	}
	if after := state(); after != before {
		t.Errorf("open memberships/links/active teams = %v after a search that answered no team, want %v unchanged", after, before)
	}

	// Control: the search still answers team A, with no member and no link.
	// Everything of the other teams, and of team A, is closed.
	onlyA := func(req request) (int, any) {
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
	result = run(first.Add(2*time.Hour), onlyA)
	if result.ExpiredMemberships == 0 || result.ExpiredOwnership == 0 || result.DeactivatedTeams == 0 {
		t.Fatalf("control: a search that answers one team retracted %d memberships, %d links and deactivated %d teams, want each above 0",
			result.ExpiredMemberships, result.ExpiredOwnership, result.DeactivatedTeams)
	}
	if after := state(); after[0] != "0" || after[1] != "0" || after[2] != "1" {
		t.Fatalf("control: open memberships/links/active teams = %v, want 0, 0 and 1", after)
	}
}
