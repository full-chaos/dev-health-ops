package atlassianteams

import (
	"context"
	"strings"
	"testing"

	"atlassian/atlassian/graph/gen"
)

// A team-users answer that does not say where its list ends is refused by
// the decoder (vendored patch 0008): a missing or null pageInfo or
// hasNextPage is not "the last page". A stated page end is an answer.
//
// NOT decided here: a missing or null edges list on a STATED last page still
// reads as zero members. No recorded real answer of an empty roster says
// whether the provider sends an empty list or null for it, and refusing a
// shape the provider really sends would fail the collection of every
// organization that has a team with no member.
func TestTheTeamUsersDecoderRefusesAnAnswerWithoutAStatedPageEnd(t *testing.T) {
	last := map[string]any{"hasNextPage": false}
	edge := userEdge(teamA, "alice-1")
	for name, tc := range map[string]struct {
		users any
		edges int // -1: refused
	}{
		"no relation":                  {nil, -1},
		"no pageInfo":                  {map[string]any{"edges": []any{edge}}, -1},
		"null pageInfo":                {map[string]any{"pageInfo": nil, "edges": []any{edge}}, -1},
		"no hasNextPage":               {map[string]any{"pageInfo": map[string]any{"endCursor": "c"}, "edges": []any{edge}}, -1},
		"null hasNextPage":             {map[string]any{"pageInfo": map[string]any{"hasNextPage": nil}, "edges": []any{edge}}, -1},
		"no pageInfo, no edges":        {map[string]any{"version": "1"}, -1},
		"null pageInfo and null edges": {map[string]any{"pageInfo": nil, "edges": nil}, -1},
		"an empty list":                {map[string]any{"pageInfo": last, "edges": []any{}}, 0},
		"one member":                   {map[string]any{"pageInfo": last, "edges": []any{edge}}, 1},
		"one member, a next page": {map[string]any{"pageInfo": map[string]any{"hasNextPage": true, "endCursor": "c"},
			"edges": []any{edge}}, 1},
		// Not decided (see above): read as zero members.
		"null edges on a stated last page": {map[string]any{"pageInfo": last, "edges": nil}, 0},
		"no edges on a stated last page":   {map[string]any{"pageInfo": last}, 0},
	} {
		t.Run(name, func(t *testing.T) {
			data := map[string]any{}
			if tc.users != nil {
				data["teamworkGraph_teamUsers"] = tc.users
			}
			conn, err := gen.DecodeTeamUsers(data)
			switch {
			case tc.edges < 0 && err == nil:
				t.Fatalf("decoded %+v, want an error", conn)
			case tc.edges >= 0 && (err != nil || len(conn.Edges) != tc.edges):
				t.Fatalf("connection = %+v, err = %v, want %d edges", conn, err, tc.edges)
			}
		})
	}
}

// Through the real client and the real collection: a member read that does
// not state its page end fails the collection (nothing is written and nothing
// can be closed on it), and a roster with a stated end is a finished read,
// an empty one included. The row shape is the one the live gateway answers
// with (userEdge: one user column per edge).
func TestAMemberReadWithoutAStatedPageEndIsAFailedCollection(t *testing.T) {
	last := map[string]any{"hasNextPage": false}
	for _, tc := range []struct {
		name   string
		users  map[string]any
		failed bool
	}{
		{"no pageInfo", map[string]any{"edges": []any{userEdge(teamA, "alice-1")}}, true},
		{"pageInfo is null", map[string]any{"pageInfo": nil, "edges": []any{userEdge(teamA, "alice-1")}}, true},
		{"no hasNextPage", map[string]any{"pageInfo": map[string]any{}, "edges": []any{userEdge(teamA, "alice-1")}}, true},
		{"no pageInfo and no edges", map[string]any{"version": "1"}, true},
		{"pageInfo null and edges null", map[string]any{"pageInfo": nil, "edges": nil}, true},
		{"control (not decided, read as no member): edges is null on a stated last page", map[string]any{"pageInfo": last, "edges": nil}, false},
		{"control: an empty roster on a stated last page", map[string]any{"pageInfo": last, "edges": []any{}}, false},
		{"control: one member on a stated last page", map[string]any{"pageInfo": last, "edges": []any{userEdge(teamA, "alice-1")}}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := newGateway(t, func(req request) (int, any) {
				if req.Operation == "TeamworkGraphTeamUsers" && req.Variables["teamId"] == teamA {
					return 200, map[string]any{"data": map[string]any{"teamworkGraph_teamUsers": tc.users}}
				}
				return standard(req)
			})
			rows, err := Collect(context.Background(), g.client(), params(everything))
			if tc.failed {
				if err == nil {
					t.Fatalf("the collection passed with %d memberships (MembershipsComplete=%v): a member read without a stated page end must fail it",
						len(rows.Memberships), rows.MembershipsComplete)
				}
				if !strings.Contains(err.Error(), "read members of team") || len(rows.Teams) != 0 || rows.MembershipsComplete {
					t.Fatalf("err = %v, rows = %+v: want the member read named as failed and no row", err, rows)
				}
				return
			}
			if err != nil || !rows.MembershipsComplete {
				t.Fatalf("err = %v, MembershipsComplete = %v: a roster with a stated end is a finished read", err, rows.MembershipsComplete)
			}
		})
	}
}
