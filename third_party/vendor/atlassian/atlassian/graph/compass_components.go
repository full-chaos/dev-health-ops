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

// ListCompassComponents fetches all Compass components for a cloud site,
// handling pagination automatically.
func (c *Client) ListCompassComponents(ctx context.Context, cloudID string, pageSize int) ([]atlassian.CompassComponent, error) {
	cloud := strings.TrimSpace(cloudID)
	if cloud == "" {
		return nil, errors.New("cloudID is required")
	}
	if pageSize <= 0 {
		pageSize = 50
	}

	var out []atlassian.CompassComponent
	var after any = nil
	seenCursors := map[string]struct{}{}

	for {
		searchQuery := map[string]any{"first": pageSize}
		if after != nil {
			searchQuery["after"] = after
		}
		vars := map[string]any{
			"cloudId": cloud,
			"query":   searchQuery,
		}
		result, err := c.Execute(ctx, gen.CompassSearchComponentsQuery, vars, "CompassSearchComponents", c.ExperimentalAPIs, 1)
		if err != nil {
			return nil, err
		}
		if result == nil || result.Data == nil {
			return nil, errors.New("missing data in CompassSearchComponents response")
		}
		decoded, err := gen.DecodeCompassSearchComponents(result.Data)
		if err != nil {
			if len(result.Errors) > 0 {
				return nil, &atlassian.GraphQLOperationError{Errors: result.Errors, PartialData: result.Data}
			}
			return nil, fmt.Errorf("decode CompassSearchComponents: %w", err)
		}

		sr := decoded.Compass.SearchComponents
		if sr.Error != nil {
			msg := ""
			if sr.Error.Message != nil {
				msg = *sr.Error.Message
			}
			return nil, fmt.Errorf("CompassSearchComponents error: %s", msg)
		}
		if sr.Connection == nil {
			return nil, errors.New("missing connection in CompassSearchComponents response")
		}
		conn := sr.Connection

		for i := range conn.Edges {
			edge := &conn.Edges[i]
			if edge.Node == nil || edge.Node.Component == nil {
				continue
			}
			mapped, err := mappers.CompassComponentFromGraphQL(cloud, edge.Node)
			if err != nil {
				return nil, fmt.Errorf("map component: %w", err)
			}
			out = append(out, mapped)
		}

		next, ok, err := nextAfterForCompassComponents(conn.PageInfo, conn.Edges)
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

// ListCompassComponentScorecardScores fetches all scorecard scores for a Compass component,
// handling pagination automatically.
func (c *Client) ListCompassComponentScorecardScores(ctx context.Context, componentID string) ([]atlassian.CompassScorecardScore, error) {
	compID := strings.TrimSpace(componentID)
	if compID == "" {
		return nil, errors.New("componentID is required")
	}

	var out []atlassian.CompassScorecardScore
	var after any = nil
	seenCursors := map[string]struct{}{}

	for {
		vars := map[string]any{"componentId": compID}
		if after != nil {
			vars["after"] = after
		}
		result, err := c.Execute(ctx, gen.CompassComponentScorecardsQuery, vars, "CompassComponentScorecards", c.ExperimentalAPIs, 1)
		if err != nil {
			return nil, err
		}
		if result == nil || result.Data == nil {
			return nil, errors.New("missing data in CompassComponentScorecards response")
		}
		decoded, err := gen.DecodeCompassComponentScorecards(result.Data)
		if err != nil {
			if len(result.Errors) > 0 {
				return nil, &atlassian.GraphQLOperationError{Errors: result.Errors, PartialData: result.Data}
			}
			return nil, fmt.Errorf("decode CompassComponentScorecards: %w", err)
		}

		cr := decoded.Compass.Component
		if cr.Error != nil {
			msg := ""
			if cr.Error.Message != nil {
				msg = *cr.Error.Message
			}
			return nil, fmt.Errorf("CompassComponentScorecards error: %s", msg)
		}
		if cr.Connection == nil {
			return nil, errors.New("missing connection in CompassComponentScorecards response")
		}
		conn := cr.Connection

		for i := range conn.Edges {
			edge := &conn.Edges[i]
			node := &edge.Node
			mapped, err := mappers.CompassScorecardScoreFromGraphQL(compID, node)
			if err != nil {
				return nil, fmt.Errorf("map scorecard score: %w", err)
			}
			out = append(out, mapped)
		}

		next, ok, err := nextAfterForCompassScorecards(conn.PageInfo, conn.Edges)
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

func nextAfterForCompassComponents(pageInfo gen.PageInfo, edges []gen.CompassComponentEdge) (string, bool, error) {
	if !pageInfo.HasNextPage {
		return "", false, nil
	}
	if gen.CompassSearchComponentsPageInfoHasEndCursor && pageInfo.EndCursor != nil && strings.TrimSpace(*pageInfo.EndCursor) != "" {
		return strings.TrimSpace(*pageInfo.EndCursor), true, nil
	}
	if gen.CompassSearchComponentsEdgeHasCursor && len(edges) > 0 {
		for i := len(edges) - 1; i >= 0; i-- {
			if edges[i].Cursor != nil && strings.TrimSpace(*edges[i].Cursor) != "" {
				return strings.TrimSpace(*edges[i].Cursor), true, nil
			}
		}
	}
	return "", false, errors.New("pagination cursor missing for compass.searchComponents")
}

func nextAfterForCompassScorecards(pageInfo gen.CompassScorecardPageInfo, edges []gen.CompassScorecardEdge) (string, bool, error) {
	if !pageInfo.HasNextPage {
		return "", false, nil
	}
	if pageInfo.EndCursor != nil && strings.TrimSpace(*pageInfo.EndCursor) != "" {
		return strings.TrimSpace(*pageInfo.EndCursor), true, nil
	}
	for i := len(edges) - 1; i >= 0; i-- {
		if edges[i].Cursor != nil && strings.TrimSpace(*edges[i].Cursor) != "" {
			return strings.TrimSpace(*edges[i].Cursor), true, nil
		}
	}
	return "", false, errors.New("pagination cursor missing for compass.component.appliedScorecards")
}
