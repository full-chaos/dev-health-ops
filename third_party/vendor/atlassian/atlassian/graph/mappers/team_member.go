package mappers

import (
	"errors"
	"strings"

	"atlassian/atlassian"
	"atlassian/atlassian/graph/gen"
)

// TeamMemberRelationForTeam maps one row of the team-users query to a TEAM_MEMBER relation of the team the query asked for
// (CHAOS-7902; local modification, patch 0004). The live gateway answers that query with ONE column per row, the user: the team
// is the request's own variable and is not in the row, which TeamworkUserRelationFromGraphQL requires. A row that does carry a
// team node keeps that node's id (the behaviour before this function); a row without one takes the requested team id. A row with
// no user is refused, and so is an empty requested team id.
func TeamMemberRelationForTeam(node *gen.GraphStoreCypherQueryV2Node, teamID string) (atlassian.TeamworkUserRelation, error) {
	if node == nil {
		return atlassian.TeamworkUserRelation{}, errors.New("node is required")
	}
	requested := strings.TrimSpace(teamID)
	if requested == "" {
		return atlassian.TeamworkUserRelation{}, errors.New("team id is required")
	}
	subject := selectNodeByKey(node.Columns, []string{"user", "userid", "user_id", "member"}, isUserNode)
	if subject == nil {
		subject = selectNode(node.Columns, isUserNode)
	}
	if subject == nil {
		return atlassian.TeamworkUserRelation{}, errors.New("teamwork user relation requires a subject user")
	}
	subjectID := strings.TrimSpace(subject.ID)
	if subjectID == "" {
		return atlassian.TeamworkUserRelation{}, errors.New("user.id is required")
	}
	tid := requested
	teamNode := selectNodeByKey(node.Columns, []string{"team", "teamid", "team_id"}, isTeamNode)
	if teamNode == nil {
		teamNode = selectNode(node.Columns, isTeamNode)
	}
	if teamNode != nil {
		// A team node the row DOES carry must name its team: an explicit node with a blank id is malformed and is refused (as the
		// mapper did before CHAOS-7902), not replaced by the requested team.
		id := strings.TrimSpace(teamNode.ID)
		if id == "" {
			return atlassian.TeamworkUserRelation{}, errors.New("team.id is required")
		}
		tid = id
	}
	return atlassian.TeamworkUserRelation{SubjectUserID: subjectID, RelationType: "TEAM_MEMBER", TeamID: &tid}, nil
}
