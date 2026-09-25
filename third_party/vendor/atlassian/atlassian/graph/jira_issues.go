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

func (c *Client) GetIssueByKey(ctx context.Context, cloudID string, issueKey string) (*atlassian.JiraIssue, error) {
	cloud := strings.TrimSpace(cloudID)
	if cloud == "" {
		return nil, errors.New("cloudID is required")
	}
	key := strings.TrimSpace(issueKey)
	if key == "" {
		return nil, errors.New("issueKey is required")
	}

	vars := map[string]any{
		"cloudId": cloud,
		"key":     key,
	}
	result, err := c.Execute(ctx, gen.JiraIssueByKeyQuery, vars, "JiraIssueByKey", c.ExperimentalAPIs, 1)
	if err != nil {
		return nil, err
	}
	if result == nil || result.Data == nil {
		return nil, errors.New("missing data in JiraIssueByKey response")
	}
	decoded, err := gen.DecodeJiraIssueByKey(result.Data)
	if err != nil {
		if len(result.Errors) > 0 {
			return nil, &atlassian.GraphQLOperationError{Errors: result.Errors, PartialData: result.Data}
		}
		return nil, fmt.Errorf("decode JiraIssueByKey: %w", err)
	}
	if decoded.IssueByKey == nil {
		return nil, errors.New("missing issueByKey in response")
	}
	mapped, err := mappers.JiraIssueFromGraphQL(cloud, *decoded.IssueByKey)
	if err != nil {
		return nil, err
	}
	return &mapped, nil
}
