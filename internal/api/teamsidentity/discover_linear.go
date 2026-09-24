package teamsidentity

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// linearTeamsQuery is providers/linear/client.py's own TEAMS_QUERY
// (client.py:258-288), copied verbatim. iter_teams() is a generic,
// multi-caller paginated-teams helper -- discover_linear reads only
// key/name/description off each node, but every call sends this FULL
// query text (including the members sub-selection discover_linear never
// reads), so this port sends the identical bytes rather than a trimmed
// query only this one caller would need.
const linearTeamsQuery = `
query Teams($first: Int!, $after: String) {
  teams(first: $first, after: $after) {
    nodes {
      id
      key
      name
      description
      createdAt
      updatedAt
      timezone
      members(first: 10) {
        nodes {
          id
          name
          email
          active
        }
        pageInfo {
          hasNextPage
          endCursor
        }
      }
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
`

// linearTeamsPerPage is LinearClient's own default (client.py:512,
// per_page: int = 50) -- discover_linear never overrides it.
const linearTeamsPerPage = 50

type linearGraphQLRequest struct {
	Query     string         `json:"query"`
	Variables map[string]any `json:"variables,omitempty"`
}

type linearTeamsResponse struct {
	Data struct {
		Teams struct {
			Nodes []struct {
				Key         string  `json:"key"`
				Name        string  `json:"name"`
				Description *string `json:"description"`
			} `json:"nodes"`
			PageInfo struct {
				HasNextPage bool   `json:"hasNextPage"`
				EndCursor   string `json:"endCursor"`
			} `json:"pageInfo"`
		} `json:"teams"`
	} `json:"data"`
	Errors []struct {
		Message string `json:"message"`
	} `json:"errors"`
}

// discoverLinear mirrors TeamDiscoveryService.discover_linear
// (team_discovery.py:160-188): walks LinearClient.iter_teams() (a cursor-
// paginated GraphQL query, every page, no upper bound -- unlike GitLab's
// discovery walk, Python sets no cap here) and maps each node's key/name/
// description straight across; description falling back to nothing
// (nil) when Linear returns none, matching `team.get("description")`.
func discoverLinear(ctx context.Context, credential providerfoundation.Credential) ([]discoveredTeam, error) {
	client, err := providerfoundation.NewLinearClient(credential, discoveryHTTPClient, providerfoundation.DefaultRetryPolicy(), alwaysValidLease{})
	if err != nil {
		return nil, err
	}
	var teams []discoveredTeam
	cursor := ""
	for {
		variables := map[string]any{"first": linearTeamsPerPage}
		if cursor != "" {
			variables["after"] = cursor
		}
		body, err := json.Marshal(linearGraphQLRequest{Query: linearTeamsQuery, Variables: variables})
		if err != nil {
			return nil, err
		}
		response, err := client.Do(ctx, "POST", "/graphql", bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		raw, readErr := io.ReadAll(response.Body)
		response.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		var payload linearTeamsResponse
		if err := json.Unmarshal(raw, &payload); err != nil {
			return nil, fmt.Errorf("decode linear teams response: %w", err)
		}
		if len(payload.Errors) > 0 {
			return nil, fmt.Errorf("linear graphql error: %s", payload.Errors[0].Message)
		}
		for _, node := range payload.Data.Teams.Nodes {
			associations := pyjson.NewObject()
			associations.Set("project_keys", []string{node.Key})
			associations.Set("provider_org", "linear")
			teams = append(teams, discoveredTeam{
				ProviderType:   "linear",
				ProviderTeamID: node.Key,
				Name:           node.Name,
				Description:    node.Description,
				Associations:   associations,
			})
		}
		if !payload.Data.Teams.PageInfo.HasNextPage {
			break
		}
		cursor = payload.Data.Teams.PageInfo.EndCursor
	}
	return teams, nil
}
