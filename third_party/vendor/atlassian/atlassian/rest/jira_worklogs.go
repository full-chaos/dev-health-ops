package rest

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"atlassian/atlassian"
	"atlassian/atlassian/rest/gen"
	"atlassian/atlassian/rest/mappers"
)

func (c *JiraRESTClient) ListIssueWorklogsViaREST(ctx context.Context, issueKey string, pageSize int) ([]atlassian.JiraWorklog, error) {
	issue := strings.TrimSpace(issueKey)
	if issue == "" {
		return nil, errors.New("issueKey is required")
	}
	if pageSize <= 0 {
		pageSize = 100
	}

	startAt := 0
	seenStart := map[int]struct{}{}
	var out []atlassian.JiraWorklog

	for {
		if _, ok := seenStart[startAt]; ok {
			return nil, errors.New("pagination startAt repeated; aborting to prevent infinite loop")
		}
		seenStart[startAt] = struct{}{}

		payload, err := c.GetJSON(ctx, "/rest/api/3/issue/"+issue+"/worklog", map[string]string{
			"startAt":    strconv.Itoa(startAt),
			"maxResults": strconv.Itoa(pageSize),
		})
		if err != nil {
			return nil, err
		}
		page, err := gen.DecodePageOfWorklogs(payload)
		if err != nil {
			return nil, fmt.Errorf("decode worklogs response: %w", err)
		}

		for _, wl := range page.Worklogs {
			mapped, err := mappers.JiraWorklogFromREST(issue, wl)
			if err != nil {
				return nil, err
			}
			out = append(out, mapped)
		}

		if page.Total != nil && *page.Total >= 0 {
			if startAt+len(page.Worklogs) >= *page.Total {
				break
			}
		} else if len(page.Worklogs) < pageSize {
			break
		}

		if len(page.Worklogs) == 0 {
			break
		}
		startAt += len(page.Worklogs)
	}

	return out, nil
}
