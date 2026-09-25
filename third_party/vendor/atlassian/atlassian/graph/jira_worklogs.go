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

func (c *Client) ListIssueWorklogs(ctx context.Context, cloudID string, issueKey string, pageSize int) ([]atlassian.JiraWorklog, error) {
	cloud := strings.TrimSpace(cloudID)
	if cloud == "" {
		return nil, errors.New("cloudID is required")
	}
	key := strings.TrimSpace(issueKey)
	if key == "" {
		return nil, errors.New("issueKey is required")
	}
	if pageSize <= 0 {
		pageSize = 50
	}

	var out []atlassian.JiraWorklog
	var after any = nil
	seen := map[string]struct{}{}

	for {
		vars := map[string]any{
			"cloudId": cloud,
			"key":     key,
			"first":   pageSize,
			"after":   after,
		}
		result, err := c.Execute(ctx, gen.JiraIssueWorklogsPageQuery, vars, "JiraIssueWorklogsPage", c.ExperimentalAPIs, 1)
		if err != nil {
			return nil, err
		}
		if result == nil || result.Data == nil {
			return nil, errors.New("missing data in JiraIssueWorklogsPage response")
		}
		conn, err := gen.DecodeIssueWorklogsPage(result.Data)
		if err != nil {
			if len(result.Errors) > 0 {
				return nil, &atlassian.GraphQLOperationError{Errors: result.Errors, PartialData: result.Data}
			}
			return nil, fmt.Errorf("decode JiraIssueWorklogsPage: %w", err)
		}

		for _, edge := range conn.Edges {
			mapped, err := mappers.JiraWorklogFromGraphQL(key, edge.Node)
			if err != nil {
				return nil, err
			}
			out = append(out, mapped)
		}

		next, ok, err := nextAfterForWorklogs(conn.PageInfo, conn.Edges)
		if err != nil {
			return nil, err
		}
		if !ok {
			break
		}
		if _, exists := seen[next]; exists {
			return nil, errors.New("pagination cursor repeated; aborting to prevent infinite loop")
		}
		seen[next] = struct{}{}
		after = next
	}

	return out, nil
}

func nextAfterForWorklogs(pageInfo gen.WorklogPageInfo, edges []gen.JiraWorklogEdge) (string, bool, error) {
	if !pageInfo.HasNextPage {
		return "", false, nil
	}
	if gen.WorklogsPageInfoHasEndCursor && pageInfo.EndCursor != nil && strings.TrimSpace(*pageInfo.EndCursor) != "" {
		return strings.TrimSpace(*pageInfo.EndCursor), true, nil
	}
	if gen.WorklogsEdgeHasCursor && len(edges) > 0 {
		for i := len(edges) - 1; i >= 0; i-- {
			if edges[i].Cursor != nil && strings.TrimSpace(*edges[i].Cursor) != "" {
				return strings.TrimSpace(*edges[i].Cursor), true, nil
			}
		}
	}
	return "", false, errors.New("pagination cursor missing for jira.issue.worklogs")
}
