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

func (c *Client) ListProjectsWithOpsgenieLinkableTeams(ctx context.Context, cloudID string, projectTypes []string, pageSize int) ([]atlassian.CanonicalProjectWithOpsgenieTeams, error) {
	cloud := strings.TrimSpace(cloudID)
	if cloud == "" {
		return nil, errors.New("cloudID is required")
	}
	if pageSize <= 0 {
		pageSize = 50
	}

	cleanTypes := make([]string, 0, len(projectTypes))
	for _, raw := range projectTypes {
		v := strings.ToUpper(strings.TrimSpace(raw))
		if v != "" {
			cleanTypes = append(cleanTypes, v)
		}
	}
	if len(cleanTypes) == 0 {
		return nil, errors.New("projectTypes must be non-empty")
	}

	query, err := gen.BuildJiraProjectsPageQuery(cleanTypes)
	if err != nil {
		return nil, err
	}

	var out []atlassian.CanonicalProjectWithOpsgenieTeams
	var after any = nil
	seenCursors := map[string]struct{}{}

	for {
		vars := map[string]any{
			"cloudId":  cloud,
			"first":    pageSize,
			"after":    after,
			"opsFirst": pageSize,
		}
		result, err := c.Execute(ctx, query, vars, "JiraProjectsPage", c.ExperimentalAPIs, 1)
		if err != nil {
			return nil, err
		}
		if result == nil || result.Data == nil {
			return nil, errors.New("missing data in JiraProjectsPage response")
		}
		page, err := gen.DecodeJiraProjectsPage(result.Data)
		if err != nil {
			if len(result.Errors) > 0 {
				return nil, &atlassian.GraphQLOperationError{Errors: result.Errors, PartialData: result.Data}
			}
			return nil, fmt.Errorf("decode JiraProjectsPage: %w", err)
		}

		conn := page.Jira.Projects
		for _, edge := range conn.Edges {
			project := edge.Node
			teams := make([]gen.OpsgenieTeamNode, 0, len(project.OpsgenieTeams.Edges))
			for _, tEdge := range project.OpsgenieTeams.Edges {
				teams = append(teams, tEdge.Node)
			}
			if project.OpsgenieTeams.PageInfo.HasNextPage {
				more, err := c.paginateOpsgenieTeams(ctx, cloud, project, project.OpsgenieTeams, pageSize)
				if err != nil {
					return nil, err
				}
				teams = append(teams, more...)
			}

			mapped, err := mappers.ProjectWithOpsgenieTeams(cloud, project, teams)
			if err != nil {
				return nil, err
			}
			out = append(out, mapped)
		}

		next, ok, err := nextAfterForProjects(conn.PageInfo, conn.Edges)
		if err != nil {
			return nil, err
		}
		if !ok {
			break
		}
		if _, exists := seenCursors[next]; exists {
			return nil, errors.New("pagination cursor repeated; aborting to prevent infinite loop")
		}
		seenCursors[next] = struct{}{}
		after = next
	}

	return out, nil
}

func nextAfterForProjects(pageInfo gen.PageInfo, edges []gen.JiraProjectEdge) (string, bool, error) {
	if !pageInfo.HasNextPage {
		return "", false, nil
	}
	if gen.PageInfoHasEndCursor && pageInfo.EndCursor != nil && strings.TrimSpace(*pageInfo.EndCursor) != "" {
		return strings.TrimSpace(*pageInfo.EndCursor), true, nil
	}
	if gen.ProjectsEdgeHasCursor && len(edges) > 0 {
		for i := len(edges) - 1; i >= 0; i-- {
			if edges[i].Cursor != nil && strings.TrimSpace(*edges[i].Cursor) != "" {
				return strings.TrimSpace(*edges[i].Cursor), true, nil
			}
		}
	}
	return "", false, errors.New("pagination cursor missing for jira.projects")
}

func nextAfterForOpsgenieTeams(pageInfo gen.PageInfo, edges []gen.OpsgenieTeamEdge) (string, bool, error) {
	if !pageInfo.HasNextPage {
		return "", false, nil
	}
	if gen.PageInfoHasEndCursor && pageInfo.EndCursor != nil && strings.TrimSpace(*pageInfo.EndCursor) != "" {
		return strings.TrimSpace(*pageInfo.EndCursor), true, nil
	}
	if gen.OpsgenieEdgeHasCursor && len(edges) > 0 {
		for i := len(edges) - 1; i >= 0; i-- {
			if edges[i].Cursor != nil && strings.TrimSpace(*edges[i].Cursor) != "" {
				return strings.TrimSpace(*edges[i].Cursor), true, nil
			}
		}
	}
	return "", false, errors.New("pagination cursor missing for opsgenieTeams")
}

func (c *Client) paginateOpsgenieTeams(ctx context.Context, cloudID string, project gen.JiraProjectNode, initial gen.OpsgenieTeamsConnection, pageSize int) ([]gen.OpsgenieTeamNode, error) {
	after, ok, err := nextAfterForOpsgenieTeams(initial.PageInfo, initial.Edges)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}
	seen := map[string]struct{}{after: {}}

	var out []gen.OpsgenieTeamNode
	query := gen.JiraProjectOpsgenieTeamsPageQuery

	for {
		vars := map[string]any{
			"first": pageSize,
			"after": after,
		}
		if gen.RefetchStrategy == "node" {
			if project.ID == nil || strings.TrimSpace(*project.ID) == "" {
				return nil, errors.New("project id is required for node-based opsgenie pagination")
			}
			vars["projectId"] = strings.TrimSpace(*project.ID)
		} else {
			key := strings.TrimSpace(project.Key)
			if key == "" {
				return nil, errors.New("project key is required for opsgenie pagination")
			}
			vars["cloudId"] = cloudID
			vars["projectKey"] = key
		}

		result, err := c.Execute(ctx, query, vars, "JiraProjectOpsgenieTeamsPage", c.ExperimentalAPIs, 1)
		if err != nil {
			return nil, err
		}
		if result == nil || result.Data == nil {
			return nil, errors.New("missing data in JiraProjectOpsgenieTeamsPage response")
		}
		conn, err := gen.DecodeProjectOpsgenieTeams(result.Data)
		if err != nil {
			if len(result.Errors) > 0 {
				return nil, &atlassian.GraphQLOperationError{Errors: result.Errors, PartialData: result.Data}
			}
			return nil, fmt.Errorf("decode JiraProjectOpsgenieTeamsPage: %w", err)
		}
		for _, e := range conn.Edges {
			out = append(out, e.Node)
		}

		next, ok, err := nextAfterForOpsgenieTeams(conn.PageInfo, conn.Edges)
		if err != nil {
			return nil, err
		}
		if !ok {
			break
		}
		if _, exists := seen[next]; exists {
			return nil, errors.New("opsgenie pagination cursor repeated; aborting")
		}
		seen[next] = struct{}{}
		after = next
	}

	return out, nil
}
