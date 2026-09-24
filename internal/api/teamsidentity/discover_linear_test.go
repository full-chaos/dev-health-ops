package teamsidentity

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// linearPagingDoer answers two pages of GraphQL responses, capturing every
// request it receives, so the test can prove both the pagination walk
// (cursor threading) and the exact outbound request shape (method, path,
// header, and the GraphQL query/variables body).
type linearPagingDoer struct {
	responses []string
	requests  []*http.Request
	bodies    [][]byte
}

func (d *linearPagingDoer) Do(request *http.Request) (*http.Response, error) {
	d.requests = append(d.requests, request)
	body, _ := io.ReadAll(request.Body)
	d.bodies = append(d.bodies, body)
	index := len(d.requests) - 1
	return &http.Response{
		StatusCode: 200,
		Header:     http.Header{"Content-Type": {"application/json"}},
		Body:       io.NopCloser(bytes.NewReader([]byte(d.responses[index]))),
	}, nil
}

func linearTestCredential() providerfoundation.Credential {
	return providerfoundation.NewCredential("linear", "cred-1", map[string]string{}, map[string]secrets.Value{
		"api_key": secrets.NewValue("linear-key"),
	})
}

// TestDiscoverLinearWalksEveryPage mirrors TeamDiscoveryService.
// discover_linear (team_discovery.py:160-188) via LinearClient.iter_teams
// (client.py:882-896): pages until pageInfo.hasNextPage is false, no
// upper bound. Two pages here prove the cursor is threaded from one
// request's pageInfo.endCursor into the next request's "after" variable.
func TestDiscoverLinearWalksEveryPage(t *testing.T) {
	doer := &linearPagingDoer{responses: []string{
		`{"data":{"teams":{"nodes":[{"key":"ENG","name":"Engineering","description":"core"}],"pageInfo":{"hasNextPage":true,"endCursor":"cursor-1"}}}}`,
		`{"data":{"teams":{"nodes":[{"key":"DESIGN","name":"Design","description":null}],"pageInfo":{"hasNextPage":false,"endCursor":""}}}}`,
	}}
	oldClient := discoveryHTTPClient
	discoveryHTTPClient = doer
	defer func() { discoveryHTTPClient = oldClient }()

	teams, err := discoverLinear(context.Background(), linearTestCredential())
	if err != nil {
		t.Fatalf("discoverLinear: %v", err)
	}
	if len(teams) != 2 {
		t.Fatalf("got %d teams, want 2 (one per page): %+v", len(teams), teams)
	}
	if teams[0].ProviderTeamID != "ENG" || teams[0].Name != "Engineering" || teams[0].Description == nil || *teams[0].Description != "core" {
		t.Errorf("teams[0] = %+v", teams[0])
	}
	if teams[1].ProviderTeamID != "DESIGN" || teams[1].Name != "Design" || teams[1].Description != nil {
		t.Errorf("teams[1] = %+v, want a nil description", teams[1])
	}
	for _, team := range teams {
		if team.ProviderType != "linear" {
			t.Errorf("ProviderType = %q, want linear", team.ProviderType)
		}
		providerOrg, _ := team.Associations.Get("provider_org")
		if providerOrg != "linear" {
			t.Errorf("associations.provider_org = %v, want \"linear\" (a literal, not the credential's own url)", providerOrg)
		}
		projectKeys, _ := team.Associations.Get("project_keys")
		if list, ok := projectKeys.([]string); !ok || len(list) != 1 || list[0] != team.ProviderTeamID {
			t.Errorf("associations.project_keys = %v, want [%s]", projectKeys, team.ProviderTeamID)
		}
	}

	if len(doer.requests) != 2 {
		t.Fatalf("got %d requests, want 2", len(doer.requests))
	}
	for _, request := range doer.requests {
		if request.Method != http.MethodPost {
			t.Errorf("method = %s, want POST", request.Method)
		}
		if request.URL.Path != "/graphql" {
			t.Errorf("path = %s, want /graphql", request.URL.Path)
		}
		if got := request.Header.Get("Authorization"); got != "linear-key" {
			t.Errorf("Authorization = %q, want the bare api_key (TokenAuth with an empty prefix)", got)
		}
	}
	var firstBody, secondBody linearGraphQLRequest
	if err := json.Unmarshal(doer.bodies[0], &firstBody); err != nil {
		t.Fatalf("decode first request body: %v", err)
	}
	if err := json.Unmarshal(doer.bodies[1], &secondBody); err != nil {
		t.Fatalf("decode second request body: %v", err)
	}
	if firstBody.Query != linearTeamsQuery {
		t.Error("first request's query does not match TEAMS_QUERY verbatim")
	}
	if _, hasAfter := firstBody.Variables["after"]; hasAfter {
		t.Error("first request must not carry an \"after\" cursor")
	}
	if got := secondBody.Variables["after"]; got != "cursor-1" {
		t.Errorf("second request's after = %v, want the first response's endCursor \"cursor-1\"", got)
	}
	if got := firstBody.Variables["first"]; got != float64(linearTeamsPerPage) {
		t.Errorf("first = %v, want %d (LinearClient's default per_page)", got, linearTeamsPerPage)
	}
}
