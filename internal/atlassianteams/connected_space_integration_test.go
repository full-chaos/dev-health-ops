//go:build integration

package atlassianteams

import (
	"context"
	"strings"
	"testing"
	"time"
)

const ownershipState = `SELECT concat(team_id, '|', project_id, '|', ifNull(project_key, ''), '|', toString(source), '|', toString(specificity), '|', if(valid_to IS NULL, 'open', toString(valid_to))) ` +
	`FROM team_project_ownership FINAL WHERE org_id = 'org-1' AND provider = 'jira' ORDER BY team_id, project_id`

const teamKeysState = `SELECT concat(id, '|', arrayStringConcat(arraySort(project_keys), ',')) FROM teams FINAL WHERE org_id = 'org-1' AND provider = 'jira' ORDER BY id`

func requireLines(t *testing.T, what string, got, want []string) {
	t.Helper()
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("%s:\n%s\nwant:\n%s", what, strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
}

// The measured site through the real client into a real ClickHouse: 11 teams, 10 ownership rows, and a second run
// of the same answer adds no row.
func TestTheConnectedSpacesOfASiteBecomeOwnershipRows(t *testing.T) {
	conn := openClickHouse(t)
	ctx := context.Background()
	g := newGateway(t, serveSite(measuredSite(), nil))
	p := params(everything)
	for run := 0; run < 2; run++ {
		p.Now = time.Date(2026, 9, 25, 3+run, 0, 0, 0, time.UTC)
		rows, err := Collect(ctx, g.client(), p)
		if err != nil {
			t.Fatal(err)
		}
		result, err := Write(ctx, conn, "org-1", rows, everything, soleScope())
		if err != nil {
			t.Fatal(err)
		}
		if result.TeamsWritten != 11 || result.OwnershipWritten != 10 || result.ExpiredOwnership != 0 || result.ProjectLinksIncomplete ||
			result.ProjectLinks != (ProjectLinkCounts{Seen: 10}) {
			t.Fatalf("run %d result = %+v, want 11 teams, 10 links seen and written, none closed, complete", run, result)
		}
	}
	var want []string
	for _, n := range []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 11} {
		want = append(want, syntheticTeamID(n)+"|"+ownershipProject(n)+"|native|110|open")
	}
	requireLines(t, "ownership", lines(t, conn, ownershipState), want)
	if got := lines(t, conn, `SELECT toString(count()) FROM teams FINAL WHERE org_id = 'org-1' AND provider = 'jira' AND is_active = 1`); got[0] != "11" {
		t.Fatalf("active teams = %s, want 11 (the team with no member too)", got[0])
	}
	// The first-seen valid_from stays: the second run replaced each row, it added none.
	if got := lines(t, conn, `SELECT toString(uniqExact(valid_from)) FROM team_project_ownership FINAL WHERE org_id = 'org-1' AND provider = 'jira'`); got[0] != "1" {
		t.Fatalf("distinct valid_from = %s, want 1: the second run moved a link's valid_from", got[0])
	}
}

func ownershipProject(n int) string {
	switch {
	case n < 10:
		return "1000" + string(rune('0'+n)) + "|SYN" + string(rune('0'+n))
	default:
		return "10011|SYN11"
	}
}

// A team that loses its link: a complete sync closes the row, a sync that is not complete keeps it. Many-to-many
// rows of other teams are not touched by either.
func TestALostLinkIsClosedOnlyByACompleteSync(t *testing.T) {
	conn := openClickHouse(t)
	ctx := context.Background()
	linked := []siteTeam{
		{ari: syntheticTeam(1), members: 1, links: []map[string]any{projectEdge("", "SYNA", "10001"), projectEdge("", "SYNB", "10002")}},
		{ari: syntheticTeam(2), members: 1, links: []map[string]any{projectEdge("", "SYNB", "10002")}},
		{ari: syntheticTeam(3), members: 1, links: []map[string]any{projectEdge("", "SYNC", "10003")}},
	}
	// Team 1 lost project SYNA; the other links are as before.
	afterLoss := []siteTeam{
		{ari: syntheticTeam(1), members: 1, links: []map[string]any{projectEdge("", "SYNB", "10002")}},
		linked[1], linked[2],
	}
	one, two, three := syntheticTeamID(1), syntheticTeamID(2), syntheticTeamID(3)
	run := func(hour int, respond func(request) (int, any)) Result {
		t.Helper()
		g := newGateway(t, respond)
		p := params(everything)
		p.Now = time.Date(2026, 9, 25, hour, 0, 0, 0, time.UTC)
		rows, err := Collect(ctx, g.client(), p)
		if err != nil {
			t.Fatal(err)
		}
		result, err := Write(ctx, conn, "org-1", rows, everything, soleScope())
		if err != nil {
			t.Fatal(err)
		}
		return result
	}

	if result := run(3, serveSite(linked, nil)); result.OwnershipWritten != 4 || result.ProjectLinksIncomplete {
		t.Fatalf("first run result = %+v, want 4 links written, complete", result)
	}
	allOpen := []string{
		one + "|10001|SYNA|native|110|open",
		one + "|10002|SYNB|native|110|open",
		two + "|10002|SYNB|native|110|open",
		three + "|10003|SYNC|native|110|open",
	}
	requireLines(t, "ownership after the first run", lines(t, conn, ownershipState), allOpen)

	// The same loss, seen by a run in which team 3's link read fails: nothing is closed, not even team 1's link,
	// whose own read was whole.
	failTeam3 := func(req request) (int, any, bool) {
		if req.Operation == "TeamConnectedContainers" && req.Variables["id"] == syntheticTeam(3) {
			return 500, map[string]any{"errors": []any{map[string]any{"message": "synthetic failure"}}}, true
		}
		return 0, nil, false
	}
	result := run(4, serveSite(afterLoss, failTeam3))
	if !result.ProjectLinksIncomplete || result.ExpiredOwnership != 0 || result.ProjectLinks.FailedTeamReads != 1 || result.OwnershipWritten != 2 {
		t.Fatalf("incomplete run result = %+v, want incomplete, nothing closed, one failed team read, the 2 links read written", result)
	}
	requireLines(t, "ownership after the incomplete run", lines(t, conn, ownershipState), allOpen)
	requireLines(t, "team project keys after the incomplete run", lines(t, conn, teamKeysState),
		[]string{one + "|SYNA,SYNB", two + "|SYNB", three + "|SYNC"})

	// The same loss, seen by a run that read every team to the end: the lost link is closed, and only it.
	result = run(5, serveSite(afterLoss, nil))
	if result.ProjectLinksIncomplete || result.ExpiredOwnership != 1 || result.OwnershipWritten != 3 {
		t.Fatalf("complete run result = %+v, want complete, 1 link closed, 3 written", result)
	}
	requireLines(t, "ownership after the complete run", lines(t, conn, ownershipState), []string{
		one + "|10001|SYNA|native|110|2026-09-25 05:00:00.000",
		one + "|10002|SYNB|native|110|open",
		two + "|10002|SYNB|native|110|open",
		three + "|10003|SYNC|native|110|open",
	})
	requireLines(t, "team project keys after the complete run", lines(t, conn, teamKeysState),
		[]string{one + "|SYNB", two + "|SYNB", three + "|SYNC"})

	// A refused opt-in on every team after that: no row is closed.
	refused := func(req request) (int, any, bool) {
		if req.Operation == "TeamConnectedContainers" {
			return 200, map[string]any{"errors": []any{map[string]any{"message": "synthetic: opt-in not accepted"}}, "data": nil}, true
		}
		return 0, nil, false
	}
	result = run(6, serveSite(afterLoss, refused))
	if !result.ProjectLinksIncomplete || result.ExpiredOwnership != 0 || result.OwnershipWritten != 0 || result.ProjectLinks.FailedTeamReads != 3 || result.TeamsWritten != 3 {
		t.Fatalf("refused run result = %+v, want incomplete, nothing closed or written for links, 3 failed reads, the 3 teams still written", result)
	}
	if open := lines(t, conn, `SELECT toString(count()) FROM team_project_ownership FINAL WHERE org_id = 'org-1' AND provider = 'jira' AND valid_to IS NULL`); open[0] != "3" {
		t.Fatalf("open links after the refused run = %s, want the 3 the complete run left", open[0])
	}
}
