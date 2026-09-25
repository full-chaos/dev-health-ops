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

// ListBoardSprintsViaREST fetches all sprints for a Jira Agile board.
// The state parameter is optional and can be "future", "active", or "closed".
func (c *JiraRESTClient) ListBoardSprintsViaREST(ctx context.Context, boardID int, state string, pageSize int) ([]atlassian.JiraSprint, error) {
	if boardID <= 0 {
		return nil, errors.New("boardID must be a positive integer")
	}
	if pageSize <= 0 {
		pageSize = 50
	}

	var stateClean string
	if state != "" {
		stateClean = strings.ToLower(strings.TrimSpace(state))
		if stateClean != "future" && stateClean != "active" && stateClean != "closed" {
			return nil, errors.New("state must be one of: future, active, closed")
		}
	}

	startAt := 0
	seenStart := map[int]struct{}{}
	var out []atlassian.JiraSprint

	for {
		if _, ok := seenStart[startAt]; ok {
			return nil, errors.New("pagination startAt repeated; aborting to prevent infinite loop")
		}
		seenStart[startAt] = struct{}{}

		params := map[string]string{
			"startAt":    strconv.Itoa(startAt),
			"maxResults": strconv.Itoa(pageSize),
		}
		if stateClean != "" {
			params["state"] = stateClean
		}

		payload, err := c.GetJSON(ctx, fmt.Sprintf("/rest/agile/1.0/board/%d/sprint", boardID), params)
		if err != nil {
			return nil, err
		}
		page, err := gen.DecodeSprintPage(payload)
		if err != nil {
			return nil, fmt.Errorf("decode sprint page response: %w", err)
		}

		values := page.Values
		for _, item := range values {
			sprint, err := mappers.JiraSprintFromREST(item)
			if err != nil {
				return nil, err
			}
			out = append(out, sprint)
		}

		hasIsLast := false
		isLast := false
		if page.IsLast != nil {
			hasIsLast = true
			isLast = *page.IsLast
		}
		if hasIsLast && isLast {
			break
		}

		if len(values) < pageSize {
			break
		}

		if len(values) == 0 {
			if hasIsLast && !isLast {
				return nil, fmt.Errorf("received empty page with isLast=false at startAt=%d", startAt)
			}
			break
		}
		startAt += len(values)
	}

	return out, nil
}
