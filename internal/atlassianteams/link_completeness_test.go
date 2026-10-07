package atlassianteams

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"atlassian/atlassian/graph/gen"
)

// The closing rule of the project links: a run may close a team's row only when every Jira project link the
// provider returned for that team is behind a row of this run. The tests here go through the real vendored client
// and the fake gateway of collect_test.go; every id, key and number is made up.

// A Jira project link that is seen and not written names its team, whatever the reason and whatever the team's
// other links are. The team's readable links are still collected, and the reads all ended, so the other teams of
// the run are judged by themselves.
func TestAJiraProjectLinkThatIsNotWrittenNamesItsTeam(t *testing.T) {
	good := projectEdge("", "SYNA", "10001")
	jira := func(id, key, projectID any) map[string]any {
		node := map[string]any{"__typename": "JiraProject"}
		for field, value := range map[string]any{"id": id, "key": key, "projectId": projectID} {
			if value != nil {
				node[field] = value
			}
		}
		return map[string]any{"id": "edge-x", "node": node}
	}
	const ari = "ari:cloud:jira:site-uuid:project/10002"
	for _, tc := range []struct {
		name       string
		links      []map[string]any
		unreadable bool
		rows       int
		want       ProjectLinkCounts
	}{
		{"every link written", []map[string]any{good, projectEdge("", "SYNB", "10002")}, false, 2, ProjectLinkCounts{Seen: 2}},
		{"no link at all", nil, false, 0, ProjectLinkCounts{}},
		{"a second link to the same project is behind the same row", []map[string]any{good, good}, false, 1, ProjectLinkCounts{Seen: 2, SkippedDuplicate: 1}},
		{"links of the other products are not project links", []map[string]any{good, containerEdge("ConfluenceSpace"), containerEdge("LoomSpace")}, false, 1,
			ProjectLinkCounts{Seen: 3, SkippedNonJira: 2}},
		{"no key, beside a written link", []map[string]any{good, jira(ari, nil, "10002")}, true, 1, ProjectLinkCounts{Seen: 2, SkippedNoProjectKey: 1}},
		{"no key, alone", []map[string]any{jira(ari, nil, "10002")}, true, 0, ProjectLinkCounts{Seen: 1, SkippedNoProjectKey: 1}},
		{"a blank key", []map[string]any{good, jira(ari, "  ", "10002")}, true, 1, ProjectLinkCounts{Seen: 2, SkippedNoProjectKey: 1}},
		{"no project ARI, beside a written link", []map[string]any{good, jira(nil, "SYNB", "10002")}, true, 1, ProjectLinkCounts{Seen: 2, SkippedNoNativeID: 1}},
		{"the ARI of another product, beside a written link", []map[string]any{good, jira("ari:cloud:townsquare:site-uuid:project/7", "SYNB", nil)}, true, 1,
			ProjectLinkCounts{Seen: 2, SkippedNoNativeID: 1}},
		{"two ids for one project, beside a written link", []map[string]any{good, jira(ari, "SYNB", "10003")}, true, 1, ProjectLinkCounts{Seen: 2, SkippedNoNativeID: 1}},
		{"no project ARI, alone", []map[string]any{jira(nil, "SYNB", nil)}, true, 0, ProjectLinkCounts{Seen: 1, SkippedNoNativeID: 1}},
		{"a link not written after the duplicate of a written one", []map[string]any{good, good, jira(ari, nil, nil)}, true, 1,
			ProjectLinkCounts{Seen: 3, SkippedDuplicate: 1, SkippedNoProjectKey: 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			site := []siteTeam{
				{ari: syntheticTeam(1), members: 1, links: tc.links},
				{ari: syntheticTeam(2), members: 1, links: []map[string]any{projectEdge("", "SYNZ", "10009")}},
			}
			rows := mustCollect(t, newGateway(t, serveSite(site, nil)))
			want := ""
			if tc.unreadable {
				want = syntheticTeamID(1)
			}
			if got := strings.Join(rows.UnreadableProjectLinkTeams, ","); got != want {
				t.Errorf("teams with a project link that was not written = %q, want %q", got, want)
			}
			tc.want.Seen++ // team 2's one link
			if rows.ProjectLinks != tc.want {
				t.Errorf("project link counts = %+v, want %+v", rows.ProjectLinks, tc.want)
			}
			if len(rows.Ownership) != tc.rows+1 {
				t.Errorf("ownership rows = %v, want %d of team 1 and 1 of team 2: the links that can be written are written", ownershipPairs(rows), tc.rows)
			}
			if !rows.ProjectLinksComplete || rows.ProjectLinkFailure != nil {
				t.Errorf("complete = %t, failure = %v: every read reached its end, the team is named by itself", rows.ProjectLinksComplete, rows.ProjectLinkFailure)
			}
			if rows.EveryProjectLinkWritten() == tc.unreadable {
				t.Errorf("every link written = %t with %d teams named, want the opposite: a named team makes the link leg degraded", rows.EveryProjectLinkWritten(), len(rows.UnreadableProjectLinkTeams))
			}
		})
	}
}

// A link answer with no list of links is a failed read of that team, not a team with no link. An explicit empty
// list is a team with no link.
func TestALinkAnswerWithNoListOfLinksIsAFailedRead(t *testing.T) {
	relation := func(value map[string]any) map[string]any {
		return map[string]any{"data": map[string]any{"graphStore_teamConnectedToContainer": value}}
	}
	lastPage := map[string]any{"hasNextPage": false, "endCursor": nil}
	for _, tc := range []struct {
		name   string
		answer map[string]any
		failed bool
	}{
		{"no edges field", relation(map[string]any{"pageInfo": lastPage}), true},
		{"edges is null", relation(map[string]any{"pageInfo": lastPage, "edges": nil}), true},
		{"edges is not a list", relation(map[string]any{"pageInfo": lastPage, "edges": map[string]any{}}), true},
		{"no pageInfo", relation(map[string]any{"edges": []any{}}), true},
		{"pageInfo is null", relation(map[string]any{"pageInfo": nil, "edges": []any{}}), true},
		{"no hasNextPage", relation(map[string]any{"pageInfo": map[string]any{"endCursor": nil}, "edges": []any{}}), true},
		{"hasNextPage is null", relation(map[string]any{"pageInfo": map[string]any{"hasNextPage": nil}, "edges": []any{}}), true},
		{"an empty list on the last page", relation(map[string]any{"pageInfo": lastPage, "edges": []any{}}), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			site := []siteTeam{{ari: syntheticTeam(1), members: 1}}
			rows := mustCollect(t, newGateway(t, serveSite(site, func(req request) (int, any, bool) {
				if req.Operation == "TeamConnectedContainers" {
					return 200, tc.answer, true
				}
				return 0, nil, false
			})))
			if tc.failed && (rows.ProjectLinkFailure == nil || strings.Contains(rows.ProjectLinkFailure.Error(), syntheticTeamID(1)) ||
				strings.Join(rows.FailedProjectLinkTeams, ",") != syntheticTeamID(1)) {
				t.Errorf("failure = %v, failed teams = %v: the failure text goes into a log field and names no team; the id is in the list of failed teams",
					rows.ProjectLinkFailure, rows.FailedProjectLinkTeams)
			}
			if failed := rows.ProjectLinks.FailedTeamReads == 1; failed != tc.failed {
				t.Errorf("failed team reads = %d (failure %v), want failed = %t", rows.ProjectLinks.FailedTeamReads, rows.ProjectLinkFailure, tc.failed)
			}
			if rows.ProjectLinksComplete == tc.failed {
				t.Errorf("complete = %t, want %t", rows.ProjectLinksComplete, !tc.failed)
			}
			if len(rows.Teams) != 1 || len(rows.Memberships) != 1 {
				t.Errorf("teams = %d, memberships = %d, want 1 and 1: only the link leg is touched", len(rows.Teams), len(rows.Memberships))
			}
		})
	}
}

// A team search answer with a missing part is a failed search: the teams it did not list would be deactivated and
// their links closed. An explicit empty list on a last page is an organization with no team.
func TestATeamSearchAnswerWithAMissingPartIsAFailedSearch(t *testing.T) {
	search := func(value map[string]any) map[string]any {
		return map[string]any{"data": map[string]any{"team": map[string]any{"teamSearchV2": value}}}
	}
	one := []any{teamNode(syntheticTeam(1), "Synthetic team 1", "ACTIVE")}
	lastPage := map[string]any{"hasNextPage": false}
	for _, tc := range []struct {
		name   string
		answer map[string]any
		teams  int // -1: the collection fails
	}{
		{"no pageInfo", search(map[string]any{"nodes": one}), -1},
		{"pageInfo is null", search(map[string]any{"pageInfo": nil, "nodes": one}), -1},
		{"no hasNextPage", search(map[string]any{"pageInfo": map[string]any{}, "nodes": one}), -1},
		{"hasNextPage is null", search(map[string]any{"pageInfo": map[string]any{"hasNextPage": nil}, "nodes": one}), -1},
		{"no nodes field", search(map[string]any{"pageInfo": lastPage}), -1},
		{"nodes is null", search(map[string]any{"pageInfo": lastPage, "nodes": nil}), -1},
		{"a null node", search(map[string]any{"pageInfo": lastPage, "nodes": []any{one[0], nil}}), -1},
		{"a node with no team", search(map[string]any{"pageInfo": lastPage, "nodes": []any{one[0], map[string]any{"team": nil}}}), -1},
		{"an empty list on the last page", search(map[string]any{"pageInfo": lastPage, "nodes": []any{}}), 0},
		{"one team on the last page", search(map[string]any{"pageInfo": lastPage, "nodes": one}), 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			site := []siteTeam{{ari: syntheticTeam(1), members: 1}}
			g := newGateway(t, serveSite(site, func(req request) (int, any, bool) {
				if req.Operation == "TeamSearchV2" {
					return 200, tc.answer, true
				}
				return 0, nil, false
			}))
			rows, err := Collect(context.Background(), g.client(), params(everything))
			switch {
			case tc.teams < 0 && err == nil:
				t.Fatalf("the collection returned %d teams, want an error: the answer does not say the list ended", len(rows.Teams))
			case tc.teams >= 0 && (err != nil || len(rows.Teams) != tc.teams):
				t.Fatalf("teams = %d, err = %v, want %d teams", len(rows.Teams), err, tc.teams)
			}
			if n := g.count("TeamSearchV2", ""); n != 1 {
				t.Errorf("team search requests = %d, want 1", n)
			}
		})
	}
}

// The team search itself refuses a page that promises a next page and names no cursor. The production client has
// a transport guard for the same answer (CompletePagesOnly); this client has none, so the refusal is the search's own.
func TestTheTeamSearchRefusesAPageItCannotFollow(t *testing.T) {
	one := teamNode(syntheticTeam(1), "Synthetic team 1", "ACTIVE")
	page := func(pageInfo map[string]any) map[string]any {
		return map[string]any{"data": map[string]any{"team": map[string]any{"teamSearchV2": map[string]any{"pageInfo": pageInfo, "nodes": []any{one}}}}}
	}
	for name, tc := range map[string]struct {
		pageInfo map[string]any
		teams    int // -1: refused
	}{
		"a next page and no cursor":     {map[string]any{"hasNextPage": true}, -1},
		"a next page and a null cursor": {map[string]any{"hasNextPage": true, "endCursor": nil}, -1},
		"a next page and a blank one":   {map[string]any{"hasNextPage": true, "endCursor": "  "}, -1},
		"the last page":                 {map[string]any{"hasNextPage": false}, 1},
	} {
		t.Run(name, func(t *testing.T) {
			g := newGateway(t, func(request) (int, any) { return 200, page(tc.pageInfo) })
			client := g.client()
			client.HTTPClient = &http.Client{Timeout: 10 * time.Second}
			teams, err := client.SearchTeams(context.Background(), "ari:cloud:platform::org/synthetic", "site-uuid", "", 2)
			switch {
			case tc.teams < 0 && err == nil:
				t.Fatalf("the search returned %d teams, want an error: a short list reads as teams that are gone", len(teams))
			case tc.teams >= 0 && (err != nil || len(teams) != tc.teams):
				t.Fatalf("teams = %d, err = %v, want %d", len(teams), err, tc.teams)
			}
			if n := g.count("TeamSearchV2", ""); n != 1 {
				t.Errorf("requests = %d, want 1", n)
			}
		})
	}
}

// Both decoders, by themselves: what each refuses and what each reads.
func TestTheLinkDecoderRefusesAnAnswerWithAMissingPart(t *testing.T) {
	last := map[string]any{"hasNextPage": false}
	for name, tc := range map[string]struct {
		relation any
		edges    int // -1: refused
	}{
		"no relation":         {nil, -1},
		"no pageInfo":         {map[string]any{"edges": []any{}}, -1},
		"no hasNextPage":      {map[string]any{"pageInfo": map[string]any{}, "edges": []any{}}, -1},
		"no edges":            {map[string]any{"pageInfo": last}, -1},
		"null edges":          {map[string]any{"pageInfo": last, "edges": nil}, -1},
		"a null edge":         {map[string]any{"pageInfo": last, "edges": []any{nil}}, -1},
		"an empty list":       {map[string]any{"pageInfo": last, "edges": []any{}}, 0},
		"one edge, no node":   {map[string]any{"pageInfo": last, "edges": []any{map[string]any{"id": "e"}}}, 1},
		"one edge with field": {map[string]any{"pageInfo": last, "edges": []any{map[string]any{"id": "e", "node": map[string]any{"__typename": "JiraProject"}}}}, 1},
	} {
		t.Run(name, func(t *testing.T) {
			data := map[string]any{}
			if tc.relation != nil {
				data["graphStore_teamConnectedToContainer"] = tc.relation
			}
			page, err := gen.DecodeTeamConnectedContainers(data)
			switch {
			case tc.edges < 0 && err == nil:
				t.Fatalf("decoded %+v, want an error", page)
			case tc.edges >= 0 && (err != nil || len(page.Edges) != tc.edges):
				t.Fatalf("page = %+v, err = %v, want %d edges", page, err, tc.edges)
			}
		})
	}
}

func TestTheTeamSearchDecoderRefusesAnAnswerWithAMissingPart(t *testing.T) {
	last := map[string]any{"hasNextPage": false}
	team := map[string]any{"team": map[string]any{"id": syntheticTeam(1), "displayName": "Synthetic team 1", "state": "ACTIVE"}}
	for name, tc := range map[string]struct {
		search any
		nodes  int // -1: refused
	}{
		"no search":             {nil, -1},
		"no pageInfo":           {map[string]any{"nodes": []any{team}}, -1},
		"null pageInfo":         {map[string]any{"pageInfo": nil, "nodes": []any{team}}, -1},
		"no hasNextPage":        {map[string]any{"pageInfo": map[string]any{"endCursor": "c"}, "nodes": []any{team}}, -1},
		"null hasNextPage":      {map[string]any{"pageInfo": map[string]any{"hasNextPage": nil}, "nodes": []any{team}}, -1},
		"no nodes":              {map[string]any{"pageInfo": last}, -1},
		"null nodes":            {map[string]any{"pageInfo": last, "nodes": nil}, -1},
		"a null node":           {map[string]any{"pageInfo": last, "nodes": []any{nil}}, -1},
		"a node with no team":   {map[string]any{"pageInfo": last, "nodes": []any{map[string]any{}}}, -1},
		"a node with null team": {map[string]any{"pageInfo": last, "nodes": []any{map[string]any{"team": nil}}}, -1},
		"an empty list":         {map[string]any{"pageInfo": last, "nodes": []any{}}, 0},
		"one team":              {map[string]any{"pageInfo": last, "nodes": []any{team}}, 1},
		"one team, a next page": {map[string]any{"pageInfo": map[string]any{"hasNextPage": true, "endCursor": "c"}, "nodes": []any{team}}, 1},
	} {
		t.Run(name, func(t *testing.T) {
			inner := map[string]any{}
			if tc.search != nil {
				inner["teamSearchV2"] = tc.search
			}
			conn, err := gen.DecodeTeamSearchV2(map[string]any{"team": inner})
			switch {
			case tc.nodes < 0 && err == nil:
				t.Fatalf("decoded %+v, want an error", conn)
			case tc.nodes >= 0 && (err != nil || len(conn.Nodes) != tc.nodes):
				t.Fatalf("connection = %+v, err = %v, want %d nodes", conn, err, tc.nodes)
			}
		})
	}
}
