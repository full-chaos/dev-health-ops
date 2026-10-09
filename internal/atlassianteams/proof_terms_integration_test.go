//go:build integration

package atlassianteams

import (
	"context"
	"slices"
	"testing"
	"time"
)

// The member read of one team answers a page that does not prove its end.
// The walker must not take it for the whole list: no membership may be closed
// on it. Real collection, real write, real store.
func TestAMemberReadWithoutAProvenEndClosesNoMembership(t *testing.T) {
	for _, c := range []struct {
		name     string
		teamUser map[string]any
	}{
		{"hasNextPage true and no cursor",
			map[string]any{"version": "1", "pageInfo": map[string]any{"hasNextPage": true}, "edges": []any{userEdge(teamA, "alice-1")}}},
		{"no pageInfo",
			map[string]any{"version": "1", "edges": []any{userEdge(teamA, "alice-1")}}},
		{"no pageInfo and no edges",
			map[string]any{"version": "1"}},
		{"pageInfo null and edges null",
			map[string]any{"version": "1", "pageInfo": nil, "edges": nil}},
	} {
		t.Run(c.name, func(t *testing.T) {
			conn := openClickHouse(t)
			ctx := context.Background()
			first := time.Date(2026, 9, 25, 3, 0, 0, 0, time.UTC)
			g := newGateway(t, standard)
			run := func(now time.Time, respond func(request) (int, any)) Result {
				t.Helper()
				g.respond = respond
				p := params(everything)
				p.Now = now
				rows, err := Collect(ctx, g.client(), p)
				if err != nil {
					t.Logf("the collection refused the answer: %v", err)
					return Result{}
				}
				result, err := writeForScopeTest(ctx, conn, "org-1", rows, integrationCensus{"integration-a": true}, "integration-a")
				if err != nil {
					t.Fatal(err)
				}
				return result
			}
			open := func() string {
				return lines(t, conn, `SELECT toString(count()) FROM team_memberships FINAL WHERE org_id = 'org-1' AND provider = 'jira' AND source = 'native' AND valid_to IS NULL`)[0]
			}
			run(first, standard)
			before := open()
			if before != "3" {
				t.Fatalf("setup: open memberships = %s, want 3", before)
			}
			result := run(first.Add(time.Hour), func(req request) (int, any) {
				if req.Operation == "TeamworkGraphTeamUsers" && req.Variables["teamId"] == teamA {
					return 200, map[string]any{"data": map[string]any{"teamworkGraph_teamUsers": c.teamUser}}
				}
				return standard(req)
			})
			if after := open(); after != before || result.ExpiredMemberships != 0 {
				t.Errorf("a member read that did not prove its end closed %d memberships (open %s -> %s)", result.ExpiredMemberships, before, after)
			}
		})
	}
}

// Each proof term of the Atlassian Teams write gates its own kind. The second
// run's answer lost a member and a project link of team A and no longer holds
// team C. With every term as collected, the write retracts all of it (the
// control). With one term not proven, the kind behind that term closes
// nothing and names the reason, and the other kinds still close.
func TestAnAtlassianWriteClosesAKindOnlyOnItsOwnProofTerm(t *testing.T) {
	shrunk := func(req request) (int, any) {
		switch req.Operation {
		case "TeamSearchV2":
			return 200, searchPage("", teamNode(teamA, "Platform", "ACTIVE"), teamNode(teamB, "Old", "ARCHIVED"))
		case "TeamworkGraphTeamUsers":
			return 200, connection("teamworkGraph_teamUsers", "", userEdge(teamA, "bob-2"))
		case "TeamConnectedContainers":
			return 200, containerPage("")
		}
		return 500, nil
	}
	for _, c := range []struct {
		name                                 string
		unprove                              func(rows *Rows)
		memberships, links, deactivatedTeams int
		reason                               string
	}{
		{"control: every term as collected", func(*Rows) {}, 2, 1, 1, ""},
		{"the member reads are not proven: no membership closes", func(rows *Rows) { rows.MembershipsComplete = false },
			0, 1, 1, snapshotMemberReadsNotEnded},
		// The membership of team C (no longer in the answer) stays: team C is
		// not in scope when the search is not proven. Team A's lost member
		// and lost link still close: team A is in the answer.
		{"the team search is not proven: no team is deactivated or put in scope", func(rows *Rows) { rows.TeamSearchComplete = false },
			1, 1, 0, snapshotTeamSearchNotEnded},
		{"the link reads are not proven: no link closes", func(rows *Rows) { rows.ProjectLinksComplete = false },
			2, 0, 1, snapshotLinkReadsNotEnded},
	} {
		t.Run(c.name, func(t *testing.T) {
			conn := openClickHouse(t)
			ctx := context.Background()
			first := time.Date(2026, 9, 25, 3, 0, 0, 0, time.UTC)
			g := newGateway(t, standard)
			collect := func(now time.Time, respond func(request) (int, any)) Rows {
				t.Helper()
				g.respond = respond
				p := params(everything)
				p.Now = now
				rows, err := Collect(ctx, g.client(), p)
				if err != nil {
					t.Fatal(err)
				}
				return rows
			}
			census := integrationCensus{"integration-a": true}
			if _, err := writeForScopeTest(ctx, conn, "org-1", collect(first, standard), census, "integration-a"); err != nil {
				t.Fatal(err)
			}
			rows := collect(first.Add(time.Hour), shrunk)
			if !rows.MembershipsComplete || !rows.TeamSearchComplete || !rows.ProjectLinksComplete {
				t.Fatalf("the collection did not prove every read: %+v", rows)
			}
			c.unprove(&rows)
			result, err := writeForScopeTest(ctx, conn, "org-1", rows, census, "integration-a")
			if err != nil {
				t.Fatal(err)
			}
			if result.ExpiredMemberships != c.memberships || result.ExpiredOwnership != c.links || result.DeactivatedTeams != c.deactivatedTeams {
				t.Errorf("retracted %d memberships, %d links, deactivated %d teams; want %d, %d and %d",
					result.ExpiredMemberships, result.ExpiredOwnership, result.DeactivatedTeams, c.memberships, c.links, c.deactivatedTeams)
			}
			if c.reason == "" {
				if len(result.CloseAbandoned) != 0 {
					t.Errorf("abandoned = %v, want none", result.CloseAbandoned)
				}
				return
			}
			if !slices.Contains(result.CloseAbandoned, c.reason) {
				t.Errorf("abandoned = %v, want it to name %s", result.CloseAbandoned, c.reason)
			}
		})
	}
}
