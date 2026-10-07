package graph

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"atlassian/atlassian"
	"atlassian/atlassian/graph/gen"
)

const (
	// TeamConnectedContainersMaxPageSize is the largest `first` the gateway takes for the relation.
	TeamConnectedContainersMaxPageSize = 1000
	// TeamConnectedContainersMaxPages bounds one team's read. A read that reaches it has NOT reached the
	// provider's last page and fails with ErrTeamConnectedContainersBound.
	TeamConnectedContainersMaxPages = 200
)

// ErrTeamConnectedContainersBound says a read stopped at its page bound before the provider's last page.
var ErrTeamConnectedContainersBound = errors.New("teamConnectedToContainer read reached its page bound before the last page")

// TeamConnectedContainer is one container a team is connected to. Typename is the union arm the gateway
// answered ("JiraProject", "ConfluenceSpace", "LoomSpace", or a later one); it is empty when the edge had no
// node. ID, Key and ProjectID are set for the JiraProject arm only: ID is the project ARI, ProjectID the
// native project id as text.
type TeamConnectedContainer struct {
	EdgeID    string
	Typename  string
	ID        string
	Key       string
	ProjectID string
}

// IterTeamConnectedContainers returns EVERY container connected to a team (local modification, patch 0006).
// It returns a list only when the provider's last page was read: a transport or GraphQL error (an opt-in
// refusal is one, in strict mode or not), a page that promises a next page without a cursor, a repeated cursor
// and the page bound are errors. The X-Query-Context header is sent when ctx carries it (WithQueryContext).
func (c *Client) IterTeamConnectedContainers(ctx context.Context, teamID string, pageSize int) ([]TeamConnectedContainer, error) {
	tid := strings.TrimSpace(teamID)
	if tid == "" {
		return nil, errors.New("teamID is required")
	}
	if pageSize <= 0 {
		pageSize = 50
	}
	if pageSize > TeamConnectedContainersMaxPageSize {
		pageSize = TeamConnectedContainersMaxPageSize
	}

	apis := append([]string{}, c.ExperimentalAPIs...)
	var out []TeamConnectedContainer
	var after any = nil
	seen := map[string]struct{}{}

	for page := 0; ; page++ {
		if page >= TeamConnectedContainersMaxPages {
			return nil, ErrTeamConnectedContainersBound
		}
		vars := map[string]any{
			"id":    tid,
			"first": pageSize,
			"after": after,
		}
		result, err := c.Execute(ctx, gen.TeamConnectedContainersQuery, vars, "TeamConnectedContainers", apis, 1)
		if err != nil {
			return nil, err
		}
		if result == nil {
			return nil, errors.New("missing TeamConnectedContainers response")
		}
		if len(result.Errors) > 0 {
			return nil, &atlassian.GraphQLOperationError{Errors: result.Errors, PartialData: result.Data}
		}
		if result.Data == nil {
			return nil, errors.New("missing data in TeamConnectedContainers response")
		}
		conn, err := gen.DecodeTeamConnectedContainers(result.Data)
		if err != nil {
			return nil, fmt.Errorf("decode TeamConnectedContainers: %w", err)
		}
		for _, edge := range conn.Edges {
			out = append(out, TeamConnectedContainer{
				EdgeID: edge.ID, Typename: edge.Node.Typename, ID: edge.Node.ID, Key: edge.Node.Key, ProjectID: edge.Node.ProjectID,
			})
		}
		if !conn.HasNextPage {
			return out, nil
		}
		if conn.EndCursor == "" {
			return nil, errors.New("teamConnectedToContainer page promises a next page without a cursor")
		}
		if _, exists := seen[conn.EndCursor]; exists {
			return nil, errors.New("teamConnectedToContainer pagination cursor repeated; aborting")
		}
		seen[conn.EndCursor] = struct{}{}
		after = conn.EndCursor
	}
}
