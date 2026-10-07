//go:build integration

package atlassianteams

import (
	"context"
	"testing"
	"time"
)

// A row is closed only when the provider no longer returns its link. Each case below is an answer in which the
// provider STILL returns every link of the first run, in a form this code does not write or cannot follow: no row
// may be closed, no team deactivated. The last case is the control: the link is really gone, and its row is closed.
func TestALinkTheProviderStillReturnsIsNeverClosed(t *testing.T) {
	ctx := context.Background()
	one, two, three := syntheticTeamID(1), syntheticTeamID(2), syntheticTeamID(3)
	linked := []siteTeam{
		{ari: syntheticTeam(1), members: 1, links: []map[string]any{projectEdge("", "SYNA", "10001"), projectEdge("", "SYNB", "10002")}},
		{ari: syntheticTeam(2), members: 1, links: []map[string]any{projectEdge("", "SYNB", "10002")}},
		{ari: syntheticTeam(3), members: 1, links: []map[string]any{projectEdge("", "SYNC", "10003")}},
	}
	allOpen := []string{
		one + "|10001|SYNA|native|110|open",
		one + "|10002|SYNB|native|110|open",
		two + "|10002|SYNB|native|110|open",
		three + "|10003|SYNC|native|110|open",
	}
	allKeys := []string{one + "|SYNA,SYNB", two + "|SYNB", three + "|SYNC"}
	links := func(first ...map[string]any) []siteTeam {
		return []siteTeam{{ari: syntheticTeam(1), members: 1, links: first}, linked[1], linked[2]}
	}
	jiraNode := func(fields map[string]any) map[string]any {
		fields["__typename"] = "JiraProject"
		return map[string]any{"id": "edge-x", "node": fields}
	}
	answerTeam1 := func(body map[string]any) func(request) (int, any, bool) {
		return func(req request) (int, any, bool) {
			if req.Operation == "TeamConnectedContainers" && req.Variables["id"] == syntheticTeam(1) {
				return 200, map[string]any{"data": map[string]any{"graphStore_teamConnectedToContainer": body}}, true
			}
			return 0, nil, false
		}
	}
	lastPage := map[string]any{"hasNextPage": false, "endCursor": nil}
	// The team search lists team 1 only and has no pageInfo: it does not say the list ended.
	searchWithoutPageInfo := func(req request) (int, any, bool) {
		if req.Operation == "TeamSearchV2" {
			return 200, map[string]any{"data": map[string]any{"team": map[string]any{"teamSearchV2": map[string]any{
				"nodes": []any{teamNode(syntheticTeam(1), "Synthetic team 1", "ACTIVE")},
			}}}}, true
		}
		return 0, nil, false
	}

	for _, tc := range []struct {
		name          string
		site          []siteTeam
		override      func(request) (int, any, bool)
		collectFails  bool
		unreadable    int
		failedReads   int
		wantOwnership []string
		wantKeys      []string
		closed        int
	}{
		{name: "the link to SYNA has no key",
			site:       links(jiraNode(map[string]any{"id": "ari:cloud:jira:site-uuid:project/10001", "projectId": "10001"}), projectEdge("", "SYNB", "10002")),
			unreadable: 1, wantOwnership: allOpen, wantKeys: allKeys},
		{name: "the link to SYNA has an ARI this code does not read, beside a link it reads",
			site:       links(jiraNode(map[string]any{"id": "ari:cloud:jira:site-uuid:space/10001", "key": "SYNA"}), projectEdge("", "SYNB", "10002")),
			unreadable: 1, wantOwnership: allOpen, wantKeys: allKeys},
		{name: "the link to SYNA has two ids",
			site:       links(jiraNode(map[string]any{"id": "ari:cloud:jira:site-uuid:project/10001", "key": "SYNA", "projectId": "10009"}), projectEdge("", "SYNB", "10002")),
			unreadable: 1, wantOwnership: allOpen, wantKeys: allKeys},
		{name: "the answer of team 1 has no edges field",
			site: linked, override: answerTeam1(map[string]any{"pageInfo": lastPage}),
			failedReads: 1, wantOwnership: allOpen, wantKeys: allKeys},
		{name: "the answer of team 1 has null edges",
			site: linked, override: answerTeam1(map[string]any{"pageInfo": lastPage, "edges": nil}),
			failedReads: 1, wantOwnership: allOpen, wantKeys: allKeys},
		{name: "the team search has no pageInfo and lists one team of three",
			site: linked, override: searchWithoutPageInfo, collectFails: true, wantOwnership: allOpen, wantKeys: allKeys},
		{name: "control: the link to SYNA is gone",
			site: links(projectEdge("", "SYNB", "10002")), closed: 1,
			wantOwnership: []string{
				one + "|10001|SYNA|native|110|2026-09-25 04:00:00.000",
				one + "|10002|SYNB|native|110|open",
				two + "|10002|SYNB|native|110|open",
				three + "|10003|SYNC|native|110|open",
			},
			wantKeys: []string{one + "|SYNB", two + "|SYNB", three + "|SYNC"}},
		{name: "control: team 1 has an explicit empty list of links",
			site: linked, override: answerTeam1(map[string]any{"pageInfo": lastPage, "edges": []any{}}), closed: 2,
			wantOwnership: []string{
				one + "|10001|SYNA|native|110|2026-09-25 04:00:00.000",
				one + "|10002|SYNB|native|110|2026-09-25 04:00:00.000",
				two + "|10002|SYNB|native|110|open",
				three + "|10003|SYNC|native|110|open",
			},
			wantKeys: []string{one + "|", two + "|SYNB", three + "|SYNC"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conn := openClickHouse(t)
			p := params(everything)
			p.Now = time.Date(2026, 9, 25, 3, 0, 0, 0, time.UTC)
			rows, err := Collect(ctx, newGateway(t, serveSite(linked, nil)).client(), p)
			if err != nil {
				t.Fatal(err)
			}
			if result, err := Write(ctx, conn, "org-1", rows, everything); err != nil || result.OwnershipWritten != 4 || result.TeamsWritten != 3 {
				t.Fatalf("first run result = %+v, err = %v, want 3 teams and 4 links", result, err)
			}
			requireLines(t, "ownership after the first run", lines(t, conn, ownershipState), allOpen)

			p.Now = time.Date(2026, 9, 25, 4, 0, 0, 0, time.UTC)
			rows, err = Collect(ctx, newGateway(t, serveSite(tc.site, tc.override)).client(), p)
			switch {
			case tc.collectFails && err == nil:
				// What the write of such a collection does is the defect: run it, then read the state below.
				result, writeErr := Write(ctx, conn, "org-1", rows, everything)
				t.Errorf("the collection returned %d teams and no error (write: %+v, err %v), want a failed collection", len(rows.Teams), result, writeErr)
			case tc.collectFails:
			case err != nil:
				t.Fatal(err)
			default:
				result, err := Write(ctx, conn, "org-1", rows, everything)
				if err != nil {
					t.Fatal(err)
				}
				if result.ExpiredOwnership != tc.closed || result.UnreadableProjectLinkTeams != tc.unreadable || result.ProjectLinks.FailedTeamReads != tc.failedReads ||
					result.DeactivatedTeams != 0 {
					t.Errorf("result = %+v, want %d links closed, %d teams with a link not written, %d failed team reads, no team deactivated",
						result, tc.closed, tc.unreadable, tc.failedReads)
				}
				if clean := tc.unreadable == 0 && tc.failedReads == 0; rows.EveryProjectLinkWritten() != clean {
					t.Errorf("every link written = %t, want %t: a link leg that could not write or read every link is degraded", rows.EveryProjectLinkWritten(), clean)
				}
			}
			requireLines(t, "ownership after the second run", lines(t, conn, ownershipState), tc.wantOwnership)
			requireLines(t, "team project keys after the second run", lines(t, conn, teamKeysState), tc.wantKeys)
			if got := lines(t, conn, `SELECT toString(count()) FROM teams FINAL WHERE org_id = 'org-1' AND provider = 'jira' AND is_active = 1`); got[0] != "3" {
				t.Errorf("active teams = %s, want 3", got[0])
			}
		})
	}
}
