package rest

import (
	"context"
	"fmt"

	"atlassian/atlassian"
	"atlassian/atlassian/rest/gen"
	"atlassian/atlassian/rest/mappers"
)

func (c *JiraRESTClient) ListBoards(ctx context.Context, pageSize int) ([]atlassian.JiraBoard, error) {
	if pageSize <= 0 {
		pageSize = 50
	}

	var out []atlassian.JiraBoard
	startAt := 0
	seenStartAt := map[int]struct{}{}

	for {
		if _, ok := seenStartAt[startAt]; ok {
			return nil, fmt.Errorf("pagination startAt repeated; aborting to prevent infinite loop")
		}
		seenStartAt[startAt] = struct{}{}

		params := map[string]string{
			"startAt":    fmt.Sprintf("%d", startAt),
			"maxResults": fmt.Sprintf("%d", pageSize),
		}
		payload, err := c.GetJSON(ctx, "/rest/agile/1.0/board", params)
		if err != nil {
			return nil, err
		}

		page, err := gen.DecodeBoardPage(payload)
		if err != nil {
			return nil, fmt.Errorf("decode BoardPage: %w", err)
		}

		for _, item := range page.Values {
			out = append(out, mappers.MapRESTBoard(item))
		}

		if page.IsLast != nil && *page.IsLast {
			break
		}

		if len(page.Values) < pageSize {
			break
		}

		if len(page.Values) == 0 {
			break
		}
		startAt += len(page.Values)
	}

	return out, nil
}
