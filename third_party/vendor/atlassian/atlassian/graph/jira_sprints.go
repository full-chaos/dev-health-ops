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

func (c *Client) GetSprintByID(ctx context.Context, sprintID string) (*atlassian.JiraSprint, error) {
	sprint := strings.TrimSpace(sprintID)
	if sprint == "" {
		return nil, errors.New("sprintID is required")
	}

	result, err := c.Execute(ctx, gen.JiraSprintByIdQuery, map[string]any{"id": sprint}, "JiraSprintById", c.ExperimentalAPIs, 1)
	if err != nil {
		return nil, err
	}
	if result == nil || result.Data == nil {
		return nil, errors.New("missing data in JiraSprintById response")
	}
	decoded, err := gen.DecodeJiraSprintById(result.Data)
	if err != nil {
		if len(result.Errors) > 0 {
			return nil, &atlassian.GraphQLOperationError{Errors: result.Errors, PartialData: result.Data}
		}
		return nil, fmt.Errorf("decode JiraSprintById: %w", err)
	}
	if decoded.SprintById == nil {
		return nil, errors.New("missing sprintById in response")
	}
	mapped, err := mappers.JiraSprintFromGraphQL(*decoded.SprintById)
	if err != nil {
		return nil, err
	}
	return &mapped, nil
}
