package gen

import (
	"encoding/json"
	"errors"
	"strings"
)

// TeamConnectedContainersOptIn is the opt-in the gateway requires for the team-to-container relation. The
// field is EXPERIMENTAL at Atlassian ("can change at any moment"): every decode below refuses a shape it does
// not know, so a provider-side change is an error and never an empty list (local modification, patch 0006).
const TeamConnectedContainersOptIn = "GraphStoreTeamConnectedToContainer"

// TeamConnectedContainersQuery reads the containers (Jira projects, called "spaces" in the product; Confluence
// spaces; Loom spaces) a team is connected to. Only the JiraProject arm selects fields: the other arms of the
// union are told apart by __typename and carry nothing a caller writes.
const TeamConnectedContainersQuery = `query TeamConnectedContainers(
  $id: ID!,
  $first: Int,
  $after: String
) {
  graphStore_teamConnectedToContainer(id: $id, first: $first, after: $after) @optIn(to: "GraphStoreTeamConnectedToContainer") {
    pageInfo { hasNextPage endCursor }
    edges {
      id
      node {
        __typename
        ... on JiraProject { id key projectId }
      }
    }
  }
}
`

// TeamConnectedContainerNode is one node of the relation. Typename is empty when the edge carried no node or
// a node without __typename.
type TeamConnectedContainerNode struct {
	Typename  string
	ID        string
	Key       string
	ProjectID string
}

type TeamConnectedContainerEdge struct {
	ID   string
	Node TeamConnectedContainerNode
}

type TeamConnectedContainersPage struct {
	HasNextPage bool
	EndCursor   string
	Edges       []TeamConnectedContainerEdge
}

type teamConnectedContainersData struct {
	Result *struct {
		PageInfo *struct {
			HasNextPage *bool   `json:"hasNextPage"`
			EndCursor   *string `json:"endCursor"`
		} `json:"pageInfo"`
		Edges *[]*struct {
			ID   *string `json:"id"`
			Node *struct {
				Typename  *string         `json:"__typename"`
				ID        *string         `json:"id"`
				Key       *string         `json:"key"`
				ProjectID json.RawMessage `json:"projectId"`
			} `json:"node"`
		} `json:"edges"`
	} `json:"graphStore_teamConnectedToContainer"`
}

func trimmedString(value *string) string {
	if value == nil {
		return ""
	}
	return strings.TrimSpace(*value)
}

// scalarText is a JSON string's value or a JSON number's literal text; anything else is "".
func scalarText(raw json.RawMessage) string {
	text := strings.TrimSpace(string(raw))
	if text == "" || text == "null" {
		return ""
	}
	var asString string
	if err := json.Unmarshal(raw, &asString); err == nil {
		return strings.TrimSpace(asString)
	}
	var asNumber json.Number
	if err := json.Unmarshal(raw, &asNumber); err == nil {
		return asNumber.String()
	}
	return ""
}

// DecodeTeamConnectedContainers reads one page. A missing relation, a missing pageInfo, a missing hasNextPage,
// a missing or null edges list and a null edge are errors: none of them is "the team has no container". Only
// an edges list that is there and empty is.
func DecodeTeamConnectedContainers(data map[string]any) (*TeamConnectedContainersPage, error) {
	b, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}
	var out teamConnectedContainersData
	if err := json.Unmarshal(b, &out); err != nil {
		return nil, err
	}
	if out.Result == nil {
		return nil, errors.New("missing graphStore_teamConnectedToContainer")
	}
	if out.Result.PageInfo == nil || out.Result.PageInfo.HasNextPage == nil {
		return nil, errors.New("missing graphStore_teamConnectedToContainer.pageInfo.hasNextPage")
	}
	page := &TeamConnectedContainersPage{
		HasNextPage: *out.Result.PageInfo.HasNextPage,
		EndCursor:   trimmedString(out.Result.PageInfo.EndCursor),
	}
	if out.Result.Edges == nil {
		return nil, errors.New("missing graphStore_teamConnectedToContainer.edges")
	}
	for _, edge := range *out.Result.Edges {
		if edge == nil {
			return nil, errors.New("null edge in graphStore_teamConnectedToContainer")
		}
		decoded := TeamConnectedContainerEdge{ID: trimmedString(edge.ID)}
		if edge.Node != nil {
			decoded.Node = TeamConnectedContainerNode{
				Typename:  trimmedString(edge.Node.Typename),
				ID:        trimmedString(edge.Node.ID),
				Key:       trimmedString(edge.Node.Key),
				ProjectID: scalarText(edge.Node.ProjectID),
			}
		}
		page.Edges = append(page.Edges, decoded)
	}
	return page, nil
}
