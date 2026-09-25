package mappers

import (
	"errors"
	"strings"

	"atlassian/atlassian"
	"atlassian/atlassian/graph/gen"
)

const (
	teamARIPrefix    = "ari:cloud:identity::team/"
	userARIPrefix    = "ari:cloud:identity::user/"
	teamTypename     = "TeamV2"
	userTypename     = "AtlassianAccountUser"
	jiraProjectType  = "JiraProject"
	tsquareProjectType = "TownsquareProject"
)

func isTeamNode(node *gen.GraphStoreCypherQueryV2AriNode) bool {
	if node == nil {
		return false
	}
	if node.Data != nil && node.Data.Typename == teamTypename {
		return true
	}
	return strings.HasPrefix(node.ID, teamARIPrefix)
}

func isUserNode(node *gen.GraphStoreCypherQueryV2AriNode) bool {
	if node == nil {
		return false
	}
	if node.Data != nil && node.Data.Typename == userTypename {
		return true
	}
	return strings.HasPrefix(node.ID, userARIPrefix)
}

func isProjectNode(node *gen.GraphStoreCypherQueryV2AriNode) bool {
	if node == nil {
		return false
	}
	if node.Data == nil {
		return false
	}
	return node.Data.Typename == jiraProjectType || node.Data.Typename == tsquareProjectType
}

func iterNodesFromValue(value *gen.GraphStoreCypherQueryV2Value) []*gen.GraphStoreCypherQueryV2AriNode {
	if value == nil {
		return nil
	}
	if value.AriNode != nil {
		return []*gen.GraphStoreCypherQueryV2AriNode{value.AriNode}
	}
	if value.NodeList != nil {
		out := make([]*gen.GraphStoreCypherQueryV2AriNode, 0, len(value.NodeList.Nodes))
		for i := range value.NodeList.Nodes {
			out = append(out, &value.NodeList.Nodes[i])
		}
		return out
	}
	return nil
}

func iterAllNodes(columns []gen.GraphStoreCypherQueryV2Column) []*gen.GraphStoreCypherQueryV2AriNode {
	var out []*gen.GraphStoreCypherQueryV2AriNode
	for _, col := range columns {
		out = append(out, iterNodesFromValue(col.Value)...)
	}
	return out
}

func selectNodeByKey(columns []gen.GraphStoreCypherQueryV2Column, keys []string, predicate func(*gen.GraphStoreCypherQueryV2AriNode) bool) *gen.GraphStoreCypherQueryV2AriNode {
	for _, col := range columns {
		colKey := strings.ToLower(strings.TrimSpace(col.Key))
		matched := false
		for _, k := range keys {
			if colKey == k {
				matched = true
				break
			}
		}
		if !matched {
			continue
		}
		for _, node := range iterNodesFromValue(col.Value) {
			if predicate(node) {
				return node
			}
		}
	}
	return nil
}

func selectNode(columns []gen.GraphStoreCypherQueryV2Column, predicate func(*gen.GraphStoreCypherQueryV2AriNode) bool) *gen.GraphStoreCypherQueryV2AriNode {
	for _, node := range iterAllNodes(columns) {
		if predicate(node) {
			return node
		}
	}
	return nil
}

// TeamworkProjectFromGraphQL maps a Teamwork Graph query result row to a TeamworkProject.
func TeamworkProjectFromGraphQL(node *gen.GraphStoreCypherQueryV2Node) (atlassian.TeamworkProject, error) {
	if node == nil {
		return atlassian.TeamworkProject{}, errors.New("node is required")
	}

	teamNode := selectNodeByKey(node.Columns, []string{"team", "teamid", "team_id"}, isTeamNode)
	if teamNode == nil {
		teamNode = selectNode(node.Columns, isTeamNode)
	}

	projectNode := selectNodeByKey(node.Columns, []string{"project", "projectid", "project_id"}, isProjectNode)
	if projectNode == nil {
		projectNode = selectNode(node.Columns, isProjectNode)
	}

	if teamNode == nil || projectNode == nil {
		return atlassian.TeamworkProject{}, errors.New("teamwork project mapping requires team and project nodes")
	}

	teamID := strings.TrimSpace(teamNode.ID)
	if teamID == "" {
		return atlassian.TeamworkProject{}, errors.New("team.id is required")
	}
	projectID := strings.TrimSpace(projectNode.ID)
	if projectID == "" {
		return atlassian.TeamworkProject{}, errors.New("project.id is required")
	}

	var projectKey *string
	var projectName *string
	if projectNode.Data != nil {
		if projectNode.Data.Key != nil {
			if k := strings.TrimSpace(*projectNode.Data.Key); k != "" {
				projectKey = &k
			}
		}
		name := ""
		if projectNode.Data.Name != nil {
			name = strings.TrimSpace(*projectNode.Data.Name)
		}
		if name == "" && projectNode.Data.DisplayName != nil {
			name = strings.TrimSpace(*projectNode.Data.DisplayName)
		}
		if name != "" {
			projectName = &name
		}
	}

	return atlassian.TeamworkProject{
		TeamID:      teamID,
		ProjectID:   projectID,
		ProjectKey:  projectKey,
		ProjectName: projectName,
	}, nil
}

// TeamworkUserRelationFromGraphQL maps a Teamwork Graph query result row to a TeamworkUserRelation.
func TeamworkUserRelationFromGraphQL(node *gen.GraphStoreCypherQueryV2Node, relationType string, subjectUserID string) (atlassian.TeamworkUserRelation, error) {
	if node == nil {
		return atlassian.TeamworkUserRelation{}, errors.New("node is required")
	}
	if strings.TrimSpace(relationType) == "" {
		return atlassian.TeamworkUserRelation{}, errors.New("relationType is required")
	}
	relationType = strings.TrimSpace(relationType)

	// Find subject user node
	var subjectNode *gen.GraphStoreCypherQueryV2AriNode
	if subjectUserID != "" {
		subjectUserID = strings.TrimSpace(subjectUserID)
		for _, n := range iterAllNodes(node.Columns) {
			if n.ID == subjectUserID {
				subjectNode = n
				break
			}
		}
	}
	if subjectNode == nil {
		subjectNode = selectNodeByKey(node.Columns, []string{"user", "userid", "user_id", "member"}, isUserNode)
	}
	if subjectNode == nil {
		subjectNode = selectNode(node.Columns, isUserNode)
	}
	if subjectNode == nil {
		return atlassian.TeamworkUserRelation{}, errors.New("teamwork user relation requires a subject user")
	}

	teamNode := selectNodeByKey(node.Columns, []string{"team", "teamid", "team_id"}, isTeamNode)
	if teamNode == nil {
		teamNode = selectNode(node.Columns, isTeamNode)
	}

	// Find related user (manager/report) - a different user from subject
	var relatedUserNode *gen.GraphStoreCypherQueryV2AriNode
	relatedUserNode = selectNodeByKey(node.Columns, []string{"manager", "report", "directreport", "direct_report"}, isUserNode)
	if relatedUserNode == nil {
		for _, n := range iterAllNodes(node.Columns) {
			if isUserNode(n) && n.ID != subjectNode.ID {
				relatedUserNode = n
				break
			}
		}
	}

	subjectID := strings.TrimSpace(subjectNode.ID)
	if subjectID == "" {
		return atlassian.TeamworkUserRelation{}, errors.New("user.id is required")
	}

	if relationType == "TEAM_MEMBER" {
		if teamNode == nil {
			return atlassian.TeamworkUserRelation{}, errors.New("TEAM_MEMBER relation requires team node")
		}
		tid := strings.TrimSpace(teamNode.ID)
		if tid == "" {
			return atlassian.TeamworkUserRelation{}, errors.New("team.id is required")
		}
		return atlassian.TeamworkUserRelation{
			SubjectUserID: subjectID,
			RelationType:  relationType,
			TeamID:        &tid,
			RelatedUserID: nil,
		}, nil
	}

	if relatedUserNode == nil {
		return atlassian.TeamworkUserRelation{}, errors.New("manager relation requires a related user node")
	}
	relatedID := strings.TrimSpace(relatedUserNode.ID)
	if relatedID == "" {
		return atlassian.TeamworkUserRelation{}, errors.New("related_user.id is required")
	}

	return atlassian.TeamworkUserRelation{
		SubjectUserID: subjectID,
		RelationType:  relationType,
		TeamID:        nil,
		RelatedUserID: &relatedID,
	}, nil
}
