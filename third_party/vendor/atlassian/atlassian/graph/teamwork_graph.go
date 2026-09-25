package graph

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"atlassian/atlassian"
	"atlassian/atlassian/graph/gen"
	"atlassian/atlassian/graph/mappers"
)

// twgExperimentalAPIs are the opt-in headers required by Teamwork Graph APIs.
var twgExperimentalAPIs = []string{gen.TeamworkGraphOptIn}

// twgForceDynamoHeaders are the extra headers required for manager/direct-report queries.
var twgForceDynamoHeaders = map[string]string{"X-Force-Dynamo": "true"}

func (c *Client) twgAPIs() []string {
	apis := append([]string{}, c.ExperimentalAPIs...)
	return append(apis, twgExperimentalAPIs...)
}

func twgNextCursor(conn *gen.GraphStoreCypherQueryV2Connection) (string, bool) {
	if conn == nil || !conn.PageInfo.HasNextPage {
		return "", false
	}
	if conn.PageInfo.EndCursor == nil {
		return "", false
	}
	next := strings.TrimSpace(*conn.PageInfo.EndCursor)
	return next, next != ""
}

// IterTeamActiveProjects returns active projects associated with a team.
func (c *Client) IterTeamActiveProjects(ctx context.Context, teamID string, pageSize int) ([]atlassian.TeamworkProject, error) {
	tid := strings.TrimSpace(teamID)
	if tid == "" {
		return nil, errors.New("teamID is required")
	}
	if pageSize <= 0 {
		pageSize = 50
	}

	apis := c.twgAPIs()
	var out []atlassian.TeamworkProject
	var after any = nil
	seen := map[string]struct{}{}

	for {
		vars := map[string]any{
			"teamId": tid,
			"first":  pageSize,
			"after":  after,
		}
		result, err := c.Execute(ctx, gen.TEAMWORKGRAPH_TEAMACTIVEPROJECTS, vars, "TeamworkGraph_teamActiveProjects", apis, 1)
		if err != nil {
			return nil, err
		}
		if result == nil || result.Data == nil {
			return nil, errors.New("missing data in TeamworkGraph_teamActiveProjects response")
		}
		conn, err := gen.DecodeTeamActiveProjects(result.Data)
		if err != nil {
			if len(result.Errors) > 0 {
				return nil, &atlassian.GraphQLOperationError{Errors: result.Errors, PartialData: result.Data}
			}
			return nil, fmt.Errorf("decode TeamworkGraph_teamActiveProjects: %w", err)
		}
		for _, edge := range conn.Edges {
			mapped, err := mappers.TeamworkProjectFromGraphQL(&edge.Node)
			if err != nil {
				return nil, err
			}
			out = append(out, mapped)
		}
		next, ok := twgNextCursor(conn)
		if !ok {
			break
		}
		if _, exists := seen[next]; exists {
			return nil, errors.New("teamActiveProjects pagination cursor repeated; aborting")
		}
		seen[next] = struct{}{}
		after = next
	}
	return out, nil
}

// IterTeamUsers returns users associated with a team as TEAM_MEMBER relations.
func (c *Client) IterTeamUsers(ctx context.Context, teamID string, pageSize int) ([]atlassian.TeamworkUserRelation, error) {
	tid := strings.TrimSpace(teamID)
	if tid == "" {
		return nil, errors.New("teamID is required")
	}
	if pageSize <= 0 {
		pageSize = 50
	}

	apis := c.twgAPIs()
	var out []atlassian.TeamworkUserRelation
	var after any = nil
	seen := map[string]struct{}{}

	for {
		vars := map[string]any{
			"teamId": tid,
			"first":  pageSize,
			"after":  after,
		}
		result, err := c.Execute(ctx, gen.TEAMWORKGRAPH_TEAMUSERS, vars, "TeamworkGraph_teamUsers", apis, 1)
		if err != nil {
			return nil, err
		}
		if result == nil || result.Data == nil {
			return nil, errors.New("missing data in TeamworkGraph_teamUsers response")
		}
		conn, err := gen.DecodeTeamUsers(result.Data)
		if err != nil {
			if len(result.Errors) > 0 {
				return nil, &atlassian.GraphQLOperationError{Errors: result.Errors, PartialData: result.Data}
			}
			return nil, fmt.Errorf("decode TeamworkGraph_teamUsers: %w", err)
		}
		for _, edge := range conn.Edges {
			mapped, err := mappers.TeamworkUserRelationFromGraphQL(&edge.Node, "TEAM_MEMBER", "")
			if err != nil {
				return nil, err
			}
			out = append(out, mapped)
		}
		next, ok := twgNextCursor(conn)
		if !ok {
			break
		}
		if _, exists := seen[next]; exists {
			return nil, errors.New("teamUsers pagination cursor repeated; aborting")
		}
		seen[next] = struct{}{}
		after = next
	}
	return out, nil
}

// IterUserTeams returns teams associated with a user as TEAM_MEMBER relations.
func (c *Client) IterUserTeams(ctx context.Context, userID string, pageSize int) ([]atlassian.TeamworkUserRelation, error) {
	uid := strings.TrimSpace(userID)
	if uid == "" {
		return nil, errors.New("userID is required")
	}
	if pageSize <= 0 {
		pageSize = 50
	}

	apis := c.twgAPIs()
	var out []atlassian.TeamworkUserRelation
	var after any = nil
	seen := map[string]struct{}{}

	for {
		vars := map[string]any{
			"userId": uid,
			"first":  pageSize,
			"after":  after,
		}
		result, err := c.Execute(ctx, gen.TEAMWORKGRAPH_USERTEAMS, vars, "TeamworkGraph_userTeams", apis, 1)
		if err != nil {
			return nil, err
		}
		if result == nil || result.Data == nil {
			return nil, errors.New("missing data in TeamworkGraph_userTeams response")
		}
		conn, err := gen.DecodeUserTeams(result.Data)
		if err != nil {
			if len(result.Errors) > 0 {
				return nil, &atlassian.GraphQLOperationError{Errors: result.Errors, PartialData: result.Data}
			}
			return nil, fmt.Errorf("decode TeamworkGraph_userTeams: %w", err)
		}
		for _, edge := range conn.Edges {
			mapped, err := mappers.TeamworkUserRelationFromGraphQL(&edge.Node, "TEAM_MEMBER", uid)
			if err != nil {
				return nil, err
			}
			out = append(out, mapped)
		}
		next, ok := twgNextCursor(conn)
		if !ok {
			break
		}
		if _, exists := seen[next]; exists {
			return nil, errors.New("userTeams pagination cursor repeated; aborting")
		}
		seen[next] = struct{}{}
		after = next
	}
	return out, nil
}

// GetUserManager returns the manager relation for a user (requires X-Force-Dynamo: true).
func (c *Client) GetUserManager(ctx context.Context, userID string) (*atlassian.TeamworkUserRelation, error) {
	uid := strings.TrimSpace(userID)
	if uid == "" {
		return nil, errors.New("userID is required")
	}

	apis := c.twgAPIs()
	vars := map[string]any{
		"userId": uid,
	}

	result, err := c.ExecuteWithExtraHeaders(ctx, gen.TEAMWORKGRAPH_USERMANAGER, vars, "TeamworkGraph_userManager", apis, 1, twgForceDynamoHeaders)
	if err != nil {
		return nil, err
	}
	if result == nil || result.Data == nil {
		return nil, errors.New("missing data in TeamworkGraph_userManager response")
	}
	conn, err := gen.DecodeUserManager(result.Data)
	if err != nil {
		if len(result.Errors) > 0 {
			return nil, &atlassian.GraphQLOperationError{Errors: result.Errors, PartialData: result.Data}
		}
		return nil, fmt.Errorf("decode TeamworkGraph_userManager: %w", err)
	}
	if len(conn.Edges) == 0 {
		return nil, nil
	}
	mapped, err := mappers.TeamworkUserRelationFromGraphQL(&conn.Edges[0].Node, "REPORTS_TO", uid)
	if err != nil {
		return nil, err
	}
	return &mapped, nil
}

// IterUserDirectReports returns direct reports for a user (requires X-Force-Dynamo: true).
func (c *Client) IterUserDirectReports(ctx context.Context, userID string, pageSize int) ([]atlassian.TeamworkUserRelation, error) {
	uid := strings.TrimSpace(userID)
	if uid == "" {
		return nil, errors.New("userID is required")
	}
	if pageSize <= 0 {
		pageSize = 50
	}

	apis := c.twgAPIs()
	var out []atlassian.TeamworkUserRelation

	// userDirectReports doesn't take pagination args but we handle it consistently
	vars := map[string]any{
		"userId": uid,
	}

	result, err := c.ExecuteWithExtraHeaders(ctx, gen.TEAMWORKGRAPH_USERDIRECTREPORTS, vars, "TeamworkGraph_userDirectReports", apis, 1, twgForceDynamoHeaders)
	if err != nil {
		return nil, err
	}
	if result == nil || result.Data == nil {
		return nil, errors.New("missing data in TeamworkGraph_userDirectReports response")
	}
	conn, err := gen.DecodeUserDirectReports(result.Data)
	if err != nil {
		if len(result.Errors) > 0 {
			return nil, &atlassian.GraphQLOperationError{Errors: result.Errors, PartialData: result.Data}
		}
		return nil, fmt.Errorf("decode TeamworkGraph_userDirectReports: %w", err)
	}
	for _, edge := range conn.Edges {
		mapped, err := mappers.TeamworkUserRelationFromGraphQL(&edge.Node, "MANAGES", uid)
		if err != nil {
			return nil, err
		}
		out = append(out, mapped)
	}
	return out, nil
}
