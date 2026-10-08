package atlassianteams

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"testing"
	"time"

	"atlassian/atlassian"
	"atlassian/atlassian/graph"
)

// The tests of this file go through the real vendored client and the fake gateway of collect_test.go. The answer shapes
// are the ones two read-only calls of 2026-10-07 measured on a live site (CHAOS-7903): the team search leaves out a
// team with no member unless the request carries showEmptyTeams: true, and a team's connected containers come back
// as edges { id node { __typename id key projectId } } with pageInfo. Every team id, name, key and number is made up.

// siteTeam is one team of a made-up site.
type siteTeam struct {
	ari     string
	members int
	links   []map[string]any
}

func syntheticTeam(n int) string {
	return fmt.Sprintf("ari:cloud:identity::team/00000000-0000-4000-8000-%012d", n)
}

func syntheticTeamID(n int) string {
	return fmt.Sprintf("jira:00000000-0000-4000-8000-%012d", n)
}

// measuredSite has the distribution the live read measured: 11 teams; 9 with one member and one Jira project, one
// with one member and no project, and one with NO member that does have a project.
func measuredSite() []siteTeam {
	var teams []siteTeam
	for n := 1; n <= 9; n++ {
		teams = append(teams, siteTeam{ari: syntheticTeam(n), members: 1, links: []map[string]any{projectEdge("", fmt.Sprintf("SYN%d", n), fmt.Sprint(10000+n))}})
	}
	teams = append(teams, siteTeam{ari: syntheticTeam(10), members: 1})
	teams = append(teams, siteTeam{ari: syntheticTeam(11), members: 0, links: []map[string]any{projectEdge("", "SYN11", "10011")}})
	return teams
}

// serveSite answers as the site does. override may answer a request first.
func serveSite(teams []siteTeam, override func(request) (int, any, bool)) func(request) (int, any) {
	return func(req request) (int, any) {
		if override != nil {
			if status, body, ok := override(req); ok {
				return status, body
			}
		}
		switch req.Operation {
		case "TeamSearchV2":
			askedForEmpty := strings.Contains(req.Query, "showEmptyTeams: true")
			var nodes []map[string]any
			for i, team := range teams {
				if team.members == 0 && !askedForEmpty {
					continue
				}
				nodes = append(nodes, teamNode(team.ari, fmt.Sprintf("Synthetic team %d", i+1), "ACTIVE"))
			}
			return 200, searchPage("", nodes...)
		case "TeamworkGraphTeamUsers":
			for i, team := range teams {
				if team.ari != req.Variables["teamId"] {
					continue
				}
				var edges []map[string]any
				for m := 0; m < team.members; m++ {
					edges = append(edges, userEdge("", fmt.Sprintf("synthetic-%d-%d", i+1, m)))
				}
				return 200, connection("teamworkGraph_teamUsers", "", edges...)
			}
		case "TeamConnectedContainers":
			for _, team := range teams {
				if team.ari == req.Variables["id"] {
					return 200, containerPage("", team.links...)
				}
			}
		}
		return 500, map[string]any{"errors": []any{map[string]any{"message": "unexpected " + req.Operation}}}
	}
}

func ownershipPairs(rows Rows) []string {
	var out []string
	for _, row := range rows.Ownership {
		out = append(out, row.TeamID+">"+row.ProjectID+":"+row.ProjectKey)
	}
	sort.Strings(out)
	return out
}

func mustCollect(t *testing.T, g *gateway) Rows {
	t.Helper()
	rows, err := Collect(context.Background(), g.client(), params(everything))
	if err != nil {
		t.Fatalf("the collection failed: %v", err)
	}
	return rows
}

// The acceptance fact of the measured site: 11 teams, 10 team-to-project links, one team with none.
func TestConnectedContainersReadThroughTheRealClient(t *testing.T) {
	g := newGateway(t, serveSite(measuredSite(), nil))
	rows := mustCollect(t, g)

	if len(rows.Teams) != 11 {
		t.Fatalf("teams = %d, want 11", len(rows.Teams))
	}
	if len(rows.Memberships) != 10 {
		t.Errorf("memberships = %d, want 10 (ten teams with one member, one with none)", len(rows.Memberships))
	}
	var want []string
	for _, n := range []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 11} {
		want = append(want, fmt.Sprintf("%s>%d:SYN%d", syntheticTeamID(n), 10000+n, n))
	}
	sort.Strings(want)
	if got := ownershipPairs(rows); strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("ownership rows:\n%s\nwant one row per provider link:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
	for _, row := range rows.Ownership {
		if row.Source != "native" || row.Provider != "jira" || row.Specificity != 110 || row.Priority != 10 || row.IsPrimary != 1 || row.OrgID != "org-1" {
			t.Errorf("ownership row %+v", row)
		}
	}
	if want := (ProjectLinkCounts{Seen: 10}); rows.ProjectLinks != want {
		t.Errorf("project link counts = %+v, want %+v", rows.ProjectLinks, want)
	}
	if !rows.ProjectLinksComplete || rows.ProjectLinkFailure != nil || len(rows.FailedProjectLinkTeams) != 0 || len(rows.UnreadableProjectLinkTeams) != 0 {
		t.Errorf("complete=%t failure=%v failed=%v unreadable=%v, want a complete snapshot: every team's read reached its end, and a team with zero links is a valid answer",
			rows.ProjectLinksComplete, rows.ProjectLinkFailure, rows.FailedProjectLinkTeams, rows.UnreadableProjectLinkTeams)
	}
	for _, team := range rows.Teams {
		wantKeys := 1
		if team.ID == syntheticTeamID(10) {
			wantKeys = 0
		}
		if len(team.ProjectKeys) != wantKeys {
			t.Errorf("team %s project keys = %v, want %d", team.ID, team.ProjectKeys, wantKeys)
		}
	}
	if n := g.count("TeamConnectedContainers", ""); n != 11 {
		t.Errorf("connected-container reads = %d, want one per team (11)", n)
	}
	if n := g.count("TeamworkGraphTeamActiveProjects", ""); n != 0 {
		t.Errorf("the activity relation was read %d times: it is not an ownership source", n)
	}
}

// The team with no member is in the search only when the search asks for empty teams, and it owns a project.
func TestTheTeamSearchAsksForEmptyTeams(t *testing.T) {
	g := newGateway(t, serveSite(measuredSite(), nil))
	rows := mustCollect(t, g)
	found := false
	for _, team := range rows.Teams {
		found = found || team.ID == syntheticTeamID(11)
	}
	if !found {
		t.Fatal("the team with no member is not in the collection: the search did not ask for empty teams")
	}
	if got := ownershipPairs(rows); !strings.Contains(strings.Join(got, "\n"), syntheticTeamID(11)+">10011:SYN11") {
		t.Fatalf("the team with no member owns no project: %v", got)
	}
	for _, req := range g.requests {
		if req.Operation == "TeamSearchV2" && !strings.Contains(req.Query, "showEmptyTeams: true") {
			t.Errorf("a team search without showEmptyTeams: true: %s", req.Query)
		}
	}
}

// One team may hold several projects and one project several teams: every link is one row.
func TestATeamToProjectLinkIsManyToMany(t *testing.T) {
	site := []siteTeam{
		{ari: syntheticTeam(1), members: 1, links: []map[string]any{projectEdge("", "SYNA", "10001"), projectEdge("", "SYNB", "10002")}},
		{ari: syntheticTeam(2), members: 1, links: []map[string]any{projectEdge("", "SYNB", "10002")}},
	}
	rows := mustCollect(t, newGateway(t, serveSite(site, nil)))
	want := []string{
		syntheticTeamID(1) + ">10001:SYNA",
		syntheticTeamID(1) + ">10002:SYNB",
		syntheticTeamID(2) + ">10002:SYNB",
	}
	if got := ownershipPairs(rows); strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("ownership rows = %v, want %v", got, want)
	}
	if !rows.ProjectLinksComplete || rows.ProjectLinks.Seen != 3 || rows.ProjectLinks.Skipped() != 0 {
		t.Errorf("complete=%t counts=%+v", rows.ProjectLinksComplete, rows.ProjectLinks)
	}
}

// A team's links on a second page are read: the read follows the cursor to the last page.
func TestConnectedContainersFollowEveryPage(t *testing.T) {
	site := []siteTeam{{ari: syntheticTeam(1), members: 1}}
	g := newGateway(t, serveSite(site, func(req request) (int, any, bool) {
		if req.Operation != "TeamConnectedContainers" {
			return 0, nil, false
		}
		switch req.Variables["after"] {
		case nil:
			return 200, containerPage("cursor-2", projectEdge("", "SYNA", "10001")), true
		case "cursor-2":
			return 200, containerPage("", projectEdge("", "SYNB", "10002")), true
		}
		return 500, map[string]any{"errors": []any{map[string]any{"message": "unexpected cursor"}}}, true
	}))
	rows := mustCollect(t, g)
	want := []string{syntheticTeamID(1) + ">10001:SYNA", syntheticTeamID(1) + ">10002:SYNB"}
	if got := ownershipPairs(rows); strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("ownership rows = %v, want both pages %v", got, want)
	}
	if !rows.ProjectLinksComplete {
		t.Error("both pages were read to the end: the snapshot is complete")
	}
	if n := g.count("TeamConnectedContainers", syntheticTeam(1)); n != 2 {
		t.Errorf("connected-container reads = %d, want 2 pages", n)
	}
}

// requireIncomplete asserts the state of a collection whose link leg degraded: the other legs are whole.
func requireIncomplete(t *testing.T, rows Rows, teams, memberships int) {
	t.Helper()
	if rows.ProjectLinksComplete {
		t.Error("the snapshot says it is complete: a later write would close links the provider still holds")
	}
	if len(rows.Teams) != teams || len(rows.Memberships) != memberships {
		t.Errorf("teams = %d, memberships = %d, want %d and %d: the link leg must not take the other legs with it", len(rows.Teams), len(rows.Memberships), teams, memberships)
	}
}

// A page error on one team's link read: the teams, the members and the other teams' links are kept, nothing is
// complete, and the failed team is named.
func TestAFailedLinkReadOfOneTeamDegradesOnlyTheLinkLeg(t *testing.T) {
	g := newGateway(t, serveSite(measuredSite(), func(req request) (int, any, bool) {
		if req.Operation == "TeamConnectedContainers" && req.Variables["id"] == syntheticTeam(7) {
			return 500, map[string]any{"errors": []any{map[string]any{"message": "synthetic failure"}}}, true
		}
		return 0, nil, false
	}))
	rows := mustCollect(t, g)
	requireIncomplete(t, rows, 11, 10)
	if rows.ProjectLinks.FailedTeamReads != 1 || strings.Join(rows.FailedProjectLinkTeams, ",") != syntheticTeamID(7) {
		t.Errorf("failed reads = %d, failed teams = %v, want team 7 alone", rows.ProjectLinks.FailedTeamReads, rows.FailedProjectLinkTeams)
	}
	// The failure text goes into a log field and the run's stored result: it holds the read's error and no team id
	// (the list above holds the id).
	if rows.ProjectLinkFailure == nil || !strings.Contains(rows.ProjectLinkFailure.Error(), "500") || strings.Contains(rows.ProjectLinkFailure.Error(), syntheticTeamID(7)) {
		t.Errorf("link failure = %v, want the error of the failed read, with no team id in it", rows.ProjectLinkFailure)
	}
	if len(rows.Ownership) != 9 || rows.ProjectLinks.Seen != 9 {
		t.Errorf("ownership rows = %d, links seen = %d, want the 9 links of the teams whose read ended", len(rows.Ownership), rows.ProjectLinks.Seen)
	}
}

// The relation is experimental and opt-in: a gateway that refuses the opt-in answers HTTP 200 with a GraphQL error.
// That is a failed read of every team, never "no team has a project".
func TestARefusedOptInIsAFailedReadNotZeroLinks(t *testing.T) {
	refusal := map[string]any{"errors": []any{map[string]any{"message": "synthetic: this field requires an opt-in that was not accepted"}}, "data": nil}
	partial := map[string]any{
		"errors": []any{map[string]any{"message": "synthetic: partial failure"}},
		"data":   containerPage("")["data"],
	}
	for name, answer := range map[string]map[string]any{"no data": refusal, "an error next to an empty page": partial} {
		t.Run(name, func(t *testing.T) {
			g := newGateway(t, serveSite(measuredSite(), func(req request) (int, any, bool) {
				if req.Operation == "TeamConnectedContainers" {
					return 200, answer, true
				}
				return 0, nil, false
			}))
			rows := mustCollect(t, g)
			requireIncomplete(t, rows, 11, 10)
			if len(rows.Ownership) != 0 || rows.ProjectLinks.FailedTeamReads != 11 || rows.ProjectLinkFailure == nil {
				t.Errorf("ownership = %d, failed reads = %d, failure = %v, want 0 rows and 11 failed reads", len(rows.Ownership), rows.ProjectLinks.FailedTeamReads, rows.ProjectLinkFailure)
			}
			// The read refuses a GraphQL error by itself: a client that is not strict gets the same error.
			lenient := g.client()
			lenient.Strict = false
			if got, err := lenient.IterTeamConnectedContainers(context.Background(), syntheticTeam(1), 2); err == nil {
				t.Errorf("a client that is not strict read %v from an answer with a GraphQL error, want an error", got)
			} else {
				var operation *atlassian.GraphQLOperationError
				if !errors.As(err, &operation) {
					t.Errorf("error = %v (%T), want the GraphQL operation error", err, err)
				}
			}
		})
	}
}

// What each kind of link node becomes. A known arm that is not a Jira project is skipped and counted, and the
// snapshot stays complete; a type this code does not know is a provider-side change and the snapshot is not complete.
func TestEveryLinkTypeIsWrittenSkippedOrMakesTheSnapshotIncomplete(t *testing.T) {
	jiraNode := func(id any, key, projectID any) map[string]any {
		node := map[string]any{"__typename": "JiraProject"}
		for field, value := range map[string]any{"id": id, "key": key, "projectId": projectID} {
			if value != nil {
				node[field] = value
			}
		}
		return map[string]any{"id": "edge-x", "node": node}
	}
	cases := map[string]struct {
		link     map[string]any
		want     ProjectLinkCounts
		rows     int
		complete bool
	}{
		"a Jira project":                 {projectEdge("", "SYNA", "10001"), ProjectLinkCounts{Seen: 1}, 1, true},
		"a Jira project, numeric id":     {jiraNode("ari:cloud:jira:site-uuid:project/10001", "SYNA", 10001), ProjectLinkCounts{Seen: 1}, 1, true},
		"a Jira project, ARI only":       {jiraNode("ari:cloud:jira:site-uuid:project/10001", "SYNA", nil), ProjectLinkCounts{Seen: 1}, 1, true},
		"a Confluence space":             {containerEdge("ConfluenceSpace"), ProjectLinkCounts{Seen: 1, SkippedNonJira: 1}, 0, true},
		"a Loom space":                   {containerEdge("LoomSpace"), ProjectLinkCounts{Seen: 1, SkippedNonJira: 1}, 0, true},
		"a type this code does not know": {containerEdge("SyntheticLaterContainer"), ProjectLinkCounts{Seen: 1, SkippedUnknownType: 1}, 0, false},
		// A project of the Atlassian Goals/Projects product is not a Jira project: its key is in another key space.
		"a Townsquare project": {map[string]any{"id": "edge-t", "node": map[string]any{"__typename": "TownsquareProject", "id": "ari:cloud:townsquare:site-uuid:project/7", "key": "SYNG-1"}},
			ProjectLinkCounts{Seen: 1, SkippedUnknownType: 1}, 0, false},
		"an edge with no node":            {map[string]any{"id": "edge-n", "node": nil}, ProjectLinkCounts{Seen: 1, SkippedUnknownType: 1}, 0, false},
		"a node with no type":             {map[string]any{"id": "edge-n", "node": map[string]any{"id": "ari:cloud:jira:site-uuid:project/10001", "key": "SYNA"}}, ProjectLinkCounts{Seen: 1, SkippedUnknownType: 1}, 0, false},
		"a Jira project with no id":       {jiraNode(nil, "SYNA", nil), ProjectLinkCounts{Seen: 1, SkippedNoNativeID: 1}, 0, true},
		"a Jira project, projectId only":  {jiraNode(nil, "SYNA", "10001"), ProjectLinkCounts{Seen: 1, SkippedNoNativeID: 1}, 0, true},
		"a Jira project, another product": {jiraNode("ari:cloud:townsquare:site-uuid:project/7", "SYNA", nil), ProjectLinkCounts{Seen: 1, SkippedNoNativeID: 1}, 0, true},
		"a Jira project, two ids":         {jiraNode("ari:cloud:jira:site-uuid:project/10001", "SYNA", "10002"), ProjectLinkCounts{Seen: 1, SkippedNoNativeID: 1}, 0, true},
		"a Jira project with no key":      {jiraNode("ari:cloud:jira:site-uuid:project/10001", nil, "10001"), ProjectLinkCounts{Seen: 1, SkippedNoProjectKey: 1}, 0, true},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			site := []siteTeam{{ari: syntheticTeam(1), members: 1, links: []map[string]any{tc.link}}}
			rows := mustCollect(t, newGateway(t, serveSite(site, nil)))
			if rows.ProjectLinks != tc.want {
				t.Errorf("project link counts = %+v, want %+v", rows.ProjectLinks, tc.want)
			}
			if len(rows.Ownership) != tc.rows {
				t.Errorf("ownership rows = %+v, want %d", rows.Ownership, tc.rows)
			}
			if rows.ProjectLinksComplete != tc.complete {
				t.Errorf("complete = %t, want %t", rows.ProjectLinksComplete, tc.complete)
			}
			if len(rows.Teams) != 1 || len(rows.Memberships) != 1 {
				t.Errorf("teams = %d, memberships = %d, want 1 and 1", len(rows.Teams), len(rows.Memberships))
			}
			if tc.rows == 1 && (rows.Ownership[0].ProjectID != "10001" || rows.Ownership[0].ProjectKey != "SYNA") {
				t.Errorf("ownership row = %+v, want project 10001 / SYNA", rows.Ownership[0])
			}
		})
	}
}

// An answer in a shape the decoder does not know is a failed read of that team's links, and only of the links: the
// team and its members are still collected.
func TestALinkAnswerInAnUnknownShapeDoesNotFailTeamsAndMembers(t *testing.T) {
	relation := func(value any) map[string]any {
		return map[string]any{"data": map[string]any{"graphStore_teamConnectedToContainer": value}}
	}
	answers := map[string]map[string]any{
		"the relation is null":      relation(nil),
		"the relation is not there": {"data": map[string]any{}},
		"no pageInfo":               relation(map[string]any{"edges": []any{}}),
		"no hasNextPage":            relation(map[string]any{"pageInfo": map[string]any{}, "edges": []any{}}),
		"a null edge":               relation(map[string]any{"pageInfo": map[string]any{"hasNextPage": false}, "edges": []any{nil}}),
		"edges is not a list":       relation(map[string]any{"pageInfo": map[string]any{"hasNextPage": false}, "edges": "none"}),
	}
	for name, answer := range answers {
		t.Run(name, func(t *testing.T) {
			site := []siteTeam{{ari: syntheticTeam(1), members: 1}}
			rows := mustCollect(t, newGateway(t, serveSite(site, func(req request) (int, any, bool) {
				if req.Operation == "TeamConnectedContainers" {
					return 200, answer, true
				}
				return 0, nil, false
			})))
			requireIncomplete(t, rows, 1, 1)
			if rows.ProjectLinks.FailedTeamReads != 1 || rows.ProjectLinkFailure == nil || len(rows.Ownership) != 0 {
				t.Errorf("failed reads = %d, failure = %v, ownership = %d, want one failed read and no row", rows.ProjectLinks.FailedTeamReads, rows.ProjectLinkFailure, len(rows.Ownership))
			}
		})
	}
}

// A read that never reaches a last page stops at its bound, and a read stopped at a bound is not complete.
func TestALinkReadThatNeverEndsStopsAtItsBound(t *testing.T) {
	site := []siteTeam{{ari: syntheticTeam(1), members: 1}}
	page := 0
	g := newGateway(t, serveSite(site, func(req request) (int, any, bool) {
		if req.Operation != "TeamConnectedContainers" {
			return 0, nil, false
		}
		page++
		return 200, containerPage(fmt.Sprintf("cursor-%d", page), projectEdge("", "SYNA", "10001")), true
	}))
	rows := mustCollect(t, g)
	requireIncomplete(t, rows, 1, 1)
	if !errors.Is(rows.ProjectLinkFailure, graph.ErrTeamConnectedContainersBound) {
		t.Fatalf("link failure = %v, want the page bound", rows.ProjectLinkFailure)
	}
	if len(rows.Ownership) != 0 {
		t.Errorf("ownership rows = %d, want none from a read that did not reach its end", len(rows.Ownership))
	}
	if n := g.count("TeamConnectedContainers", ""); n != graph.TeamConnectedContainersMaxPages {
		t.Errorf("pages read = %d, want the bound %d", n, graph.TeamConnectedContainersMaxPages)
	}
}

// The client's own page rules, without the transport guard of the production client: a next page with no cursor and
// a cursor that repeats are errors, and the page size never exceeds the provider's maximum.
func TestTheConnectedContainersReadRefusesAPageItCannotFollow(t *testing.T) {
	cases := map[string]struct {
		answer   func(request) map[string]any
		requests int
	}{
		// The read stops at the page it cannot follow: it does not ask again.
		"a next page with no cursor": {func(request) map[string]any {
			return map[string]any{"data": map[string]any{"graphStore_teamConnectedToContainer": map[string]any{"pageInfo": map[string]any{"hasNextPage": true, "endCursor": nil}, "edges": []any{}}}}
		}, 1},
		// The second answer names the cursor the first one named: the read stops there.
		"a cursor that repeats": {func(request) map[string]any { return containerPage("cursor-1") }, 2},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			g := newGateway(t, func(req request) (int, any) { return 200, tc.answer(req) })
			client := g.client()
			client.HTTPClient = &http.Client{Timeout: 10 * time.Second}
			if got, err := client.IterTeamConnectedContainers(context.Background(), syntheticTeam(1), 5000); err == nil {
				t.Fatalf("the read returned %v, want an error", got)
			}
			if len(g.requests) != tc.requests {
				t.Errorf("requests = %d, want %d: the read went on after the page it cannot follow", len(g.requests), tc.requests)
			}
			for _, req := range g.requests {
				if first, _ := req.Variables["first"].(float64); first != graph.TeamConnectedContainersMaxPageSize {
					t.Errorf("first = %v, want the provider's maximum %d for a larger request", req.Variables["first"], graph.TeamConnectedContainersMaxPageSize)
				}
			}
		})
	}
}

// A collection that did not select the project links never says they are complete, also when no team is active (no
// link read is owed then, so the count of ended reads alone would say "complete").
func TestACollectionThatDidNotSelectTheProjectLinksIsNeverComplete(t *testing.T) {
	g := newGateway(t, func(req request) (int, any) {
		if req.Operation == "TeamSearchV2" {
			return 200, searchPage("", teamNode(syntheticTeam(1), "Synthetic archived team", "ARCHIVED"))
		}
		return 500, map[string]any{"errors": []any{map[string]any{"message": "unexpected " + req.Operation}}}
	})
	p := params(Selections{Structure: true})
	rows, err := Collect(context.Background(), g.client(), p)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows.Teams) != 1 || rows.ProjectLinksComplete {
		t.Fatalf("teams = %d, complete = %t, want 1 team and not complete: no project link was read", len(rows.Teams), rows.ProjectLinksComplete)
	}
	// The same site with the links selected: no read is owed, and that is a complete (empty) snapshot.
	rows, err = Collect(context.Background(), g.client(), params(everything))
	if err != nil {
		t.Fatal(err)
	}
	if !rows.ProjectLinksComplete {
		t.Fatal("a site whose only team is archived owes no link read: the selected snapshot is complete")
	}
}
