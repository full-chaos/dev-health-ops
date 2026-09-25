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

func (c *JiraRESTClient) ListIssueChangelogViaREST(ctx context.Context, issueKey string, pageSize int) ([]atlassian.JiraChangelogEvent, error) {
	issue := strings.TrimSpace(issueKey)
	if issue == "" {
		return nil, errors.New("issueKey is required")
	}
	if pageSize <= 0 {
		pageSize = 100
	}

	startAt := 0
	seenStart := map[int]struct{}{}
	var out []atlassian.JiraChangelogEvent

	for {
		if _, ok := seenStart[startAt]; ok {
			return nil, errors.New("pagination startAt repeated; aborting to prevent infinite loop")
		}
		seenStart[startAt] = struct{}{}

		payload, err := c.GetJSON(ctx, "/rest/api/3/issue/"+issue+"/changelog", map[string]string{
			"startAt":    strconv.Itoa(startAt),
			"maxResults": strconv.Itoa(pageSize),
		})
		if err != nil {
			return nil, err
		}
		page, err := gen.DecodePageBeanChangelog(payload)
		if err != nil {
			return nil, fmt.Errorf("decode changelog response: %w", err)
		}

		for _, it := range page.Values {
			mapped, err := mappers.JiraChangelogEventFromREST(issue, it)
			if err != nil {
				return nil, err
			}
			out = append(out, mapped)
		}

		if page.IsLast != nil && *page.IsLast {
			break
		}

		if page.Total != nil && *page.Total >= 0 {
			if startAt+len(page.Values) >= *page.Total {
				break
			}
		} else if len(page.Values) < pageSize {
			break
		}

		if len(page.Values) == 0 {
			break
		}
		startAt += len(page.Values)
	}

	return out, nil
}
