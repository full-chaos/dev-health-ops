package atlassianteams

import (
	"strings"
	"testing"

	"atlassian/atlassian/graph/gen"
	"atlassian/atlassian/graph/mappers"
)

// The row shapes below use hand-written ids. The one-column user row is the shape measured on the real provider by a structure
// probe on 2026-10-02 (CHAOS-7902: one `user` column per edge; the team is the request variable). A row WITH a team node is the
// shape the vendored mapper was written for: NOT MEASURED on the real provider.

func ariColumn(key, id, typename string) gen.GraphStoreCypherQueryV2Column {
	return gen.GraphStoreCypherQueryV2Column{Key: key, Value: &gen.GraphStoreCypherQueryV2Value{
		Typename: "GraphStoreCypherQueryV2AriNode",
		AriNode:  &gen.GraphStoreCypherQueryV2AriNode{ID: id, Data: &gen.GraphStoreCypherQueryV2AriNodeData{Typename: typename}},
	}}
}

func TestTeamMemberRelationForTeamTakesTheRequestedTeamWhenTheRowHasNone(t *testing.T) {
	row := &gen.GraphStoreCypherQueryV2Node{Columns: []gen.GraphStoreCypherQueryV2Column{ariColumn("user", "ari:cloud:identity::user/u-1", "AtlassianAccountUser")}}
	got, err := mappers.TeamMemberRelationForTeam(row, "  ari:cloud:identity::team/requested  ")
	if err != nil {
		t.Fatal(err)
	}
	if got.TeamID == nil || *got.TeamID != "ari:cloud:identity::team/requested" {
		t.Errorf("team id = %v, want the requested team, trimmed", got.TeamID)
	}
	if got.SubjectUserID != "ari:cloud:identity::user/u-1" || got.RelationType != "TEAM_MEMBER" || got.RelatedUserID != nil {
		t.Errorf("relation = %+v", got)
	}
}

func TestTeamMemberRelationForTeamKeepsATeamNodeOfTheRow(t *testing.T) {
	row := &gen.GraphStoreCypherQueryV2Node{Columns: []gen.GraphStoreCypherQueryV2Column{
		ariColumn("team", "ari:cloud:identity::team/in-row", "TeamV2"),
		ariColumn("user", "ari:cloud:identity::user/u-1", "AtlassianAccountUser"),
	}}
	got, err := mappers.TeamMemberRelationForTeam(row, "ari:cloud:identity::team/requested")
	if err != nil {
		t.Fatal(err)
	}
	if got.TeamID == nil || *got.TeamID != "ari:cloud:identity::team/in-row" {
		t.Errorf("team id = %v, want the row's own team node (behaviour before CHAOS-7902)", got.TeamID)
	}
}

// A team node under a key the mapper does not name is still found (by its TeamV2 type) and its own id wins. NOT MEASURED on the real
// provider: hand-written shape.
func TestTeamMemberRelationForTeamFindsATeamNodeUnderAnotherKey(t *testing.T) {
	row := &gen.GraphStoreCypherQueryV2Node{Columns: []gen.GraphStoreCypherQueryV2Column{
		ariColumn("squad", "ari:cloud:identity::team/in-row", "TeamV2"),
		ariColumn("user", "ari:cloud:identity::user/u-1", "AtlassianAccountUser"),
	}}
	got, err := mappers.TeamMemberRelationForTeam(row, "ari:cloud:identity::team/requested")
	if err != nil {
		t.Fatal(err)
	}
	if got.TeamID == nil || *got.TeamID != "ari:cloud:identity::team/in-row" {
		t.Errorf("team id = %v, want the team node found by type, not the requested team", got.TeamID)
	}
}

func TestTeamMemberRelationForTeamRefusals(t *testing.T) {
	user := ariColumn("user", "ari:cloud:identity::user/u-1", "AtlassianAccountUser")
	cases := []struct {
		name string
		row  *gen.GraphStoreCypherQueryV2Node
		team string
		want string
	}{
		{"nil row", nil, "ari:cloud:identity::team/t", "node is required"},
		{"empty requested team", &gen.GraphStoreCypherQueryV2Node{Columns: []gen.GraphStoreCypherQueryV2Column{user}}, "  ", "team id is required"},
		{"no user", &gen.GraphStoreCypherQueryV2Node{}, "ari:cloud:identity::team/t", "requires a subject user"},
		// A team node under a key the mapper does not name (found by its TeamV2 type): a blank id is refused here too.
		{"team node under another key with a blank id", &gen.GraphStoreCypherQueryV2Node{Columns: []gen.GraphStoreCypherQueryV2Column{ariColumn("squad", "  ", "TeamV2"), user}}, "ari:cloud:identity::team/t", "team.id is required"},
		{"explicit team node with a blank id", &gen.GraphStoreCypherQueryV2Node{Columns: []gen.GraphStoreCypherQueryV2Column{ariColumn("team", "  ", "TeamV2"), user}}, "ari:cloud:identity::team/t", "team.id is required"},
		{"empty user id", &gen.GraphStoreCypherQueryV2Node{Columns: []gen.GraphStoreCypherQueryV2Column{ariColumn("user", "  ", "AtlassianAccountUser")}}, "ari:cloud:identity::team/t", "user.id is required"},
	}
	for _, c := range cases {
		if _, err := mappers.TeamMemberRelationForTeam(c.row, c.team); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: err = %v, want %q", c.name, err, c.want)
		}
	}
}
