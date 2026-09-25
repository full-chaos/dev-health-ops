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

// teamExperimentalAPIs are the beta headers required by the Teams API.
var teamExperimentalAPIs = []string{"teams-beta", "team-members-beta"}

func (c *Client) GetTeamByID(ctx context.Context, teamID string, siteID string) (*atlassian.AtlassianTeam, error) {
	tid := strings.TrimSpace(teamID)
	if tid == "" {
		return nil, errors.New("teamID is required")
	}
	sid := strings.TrimSpace(siteID)

	vars := map[string]any{
		"teamId": tid,
	}
	if sid != "" {
		vars["siteId"] = sid
	}

	apis := append([]string{}, c.ExperimentalAPIs...)
	apis = append(apis, teamExperimentalAPIs...)

	result, err := c.Execute(ctx, gen.TeamByIdQuery, vars, "TeamById", apis, 1)
	if err != nil {
		return nil, err
	}
	if result == nil || result.Data == nil {
		return nil, errors.New("missing data in TeamById response")
	}
	teamNode, err := gen.DecodeTeam(result.Data)
	if err != nil {
		if len(result.Errors) > 0 {
			return nil, &atlassian.GraphQLOperationError{Errors: result.Errors, PartialData: result.Data}
		}
		return nil, fmt.Errorf("decode TeamById: %w", err)
	}
	mapped, err := mappers.TeamFromGraphQL(teamNode)
	if err != nil {
		return nil, err
	}
	return &mapped, nil
}

func (c *Client) SearchTeams(ctx context.Context, organizationID string, siteID string, query string, pageSize int) ([]atlassian.AtlassianTeam, error) {
	orgID := strings.TrimSpace(organizationID)
	if orgID == "" {
		return nil, errors.New("organizationID is required")
	}
	sid := strings.TrimSpace(siteID)
	if sid == "" {
		return nil, errors.New("siteID is required")
	}
	if pageSize <= 0 {
		pageSize = 50
	}

	apis := append([]string{}, c.ExperimentalAPIs...)
	apis = append(apis, teamExperimentalAPIs...)

	var out []atlassian.AtlassianTeam
	var after any = nil
	seenCursors := map[string]struct{}{}

	for {
		vars := map[string]any{
			"organizationId": orgID,
			"siteId":         sid,
			"query":          query,
			"first":          pageSize,
			"after":          after,
		}
		result, err := c.Execute(ctx, gen.TeamSearchV2Query, vars, "TeamSearchV2", apis, 1)
		if err != nil {
			return nil, err
		}
		if result == nil || result.Data == nil {
			return nil, errors.New("missing data in TeamSearchV2 response")
		}
		conn, err := gen.DecodeTeamSearchV2(result.Data)
		if err != nil {
			if len(result.Errors) > 0 {
				return nil, &atlassian.GraphQLOperationError{Errors: result.Errors, PartialData: result.Data}
			}
			return nil, fmt.Errorf("decode TeamSearchV2: %w", err)
		}

		for _, resultNode := range conn.Nodes {
			var teamNode *gen.TeamNode
			if resultNode.Team != nil {
				teamNode = resultNode.Team
			}
			if teamNode == nil {
				continue
			}
			mapped, err := mappers.TeamFromGraphQL(teamNode)
			if err != nil {
				return nil, err
			}
			out = append(out, mapped)
		}

		if !conn.PageInfo.HasNextPage {
			break
		}
		if !gen.TeamSearchPageInfoHasEndCursor || conn.PageInfo.EndCursor == nil {
			break
		}
		next := strings.TrimSpace(*conn.PageInfo.EndCursor)
		if next == "" {
			break
		}
		if _, exists := seenCursors[next]; exists {
			return nil, errors.New("team search pagination cursor repeated; aborting to prevent infinite loop")
		}
		seenCursors[next] = struct{}{}
		after = next
	}

	return out, nil
}
