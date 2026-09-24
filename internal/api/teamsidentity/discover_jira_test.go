package teamsidentity

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// jiraStubDoer answers every request with a fixed status/body, capturing
// the request it received so the test can assert on the actual outbound
// call (method, path, query, headers) as well as the parsed result.
type jiraStubDoer struct {
	status  int
	body    string
	request *http.Request
}

func (d *jiraStubDoer) Do(request *http.Request) (*http.Response, error) {
	d.request = request
	return &http.Response{
		StatusCode: d.status,
		Header:     http.Header{"Content-Type": {"application/json"}},
		Body:       io.NopCloser(bytes.NewReader([]byte(d.body))),
	}, nil
}

func jiraTestCredential() providerfoundation.Credential {
	return providerfoundation.NewCredential("jira", "cred-1", map[string]string{}, map[string]secrets.Value{
		"email":     secrets.NewValue("dev@acme.test"),
		"api_token": secrets.NewValue("jira-token"),
		"base_url":  secrets.NewValue("https://acme.atlassian.net"),
	})
}

// TestDiscoverJiraParsesRealShapedResponse mirrors
// TeamDiscoveryService.discover_jira (team_discovery.py:299-338): one GET
// to /rest/api/3/project/search?maxResults=100, basic auth, reading only
// values[].key/name/description -- a project with no key or no resolvable
// name is skipped (Python's own `if not project_key or not project_name:
// continue`), and a missing "name" falls back to the key.
func TestDiscoverJiraParsesRealShapedResponse(t *testing.T) {
	doer := &jiraStubDoer{status: 200, body: `{
		"values": [
			{"key": "ENG", "name": "Engineering", "description": "core team"},
			{"key": "DESIGN", "name": null, "description": null},
			{"key": "", "name": "No Key"},
			{"key": "GHOST"}
		]
	}`}
	credential := jiraTestCredential()
	oldClient := discoveryHTTPClient
	discoveryHTTPClient = doer
	defer func() { discoveryHTTPClient = oldClient }()

	teams, err := discoverJira(context.Background(), credential)
	if err != nil {
		t.Fatalf("discoverJira: %v", err)
	}
	if len(teams) != 3 {
		t.Fatalf("got %d teams, want 3 (the key=\"\" project skipped): %+v", len(teams), teams)
	}
	if teams[0].ProviderTeamID != "ENG" || teams[0].Name != "Engineering" || teams[0].Description == nil || *teams[0].Description != "core team" {
		t.Errorf("teams[0] = %+v, want ENG/Engineering/core team", teams[0])
	}
	if teams[1].ProviderTeamID != "DESIGN" || teams[1].Name != "DESIGN" {
		t.Errorf("teams[1] = %+v, want name falling back to the key DESIGN", teams[1])
	}
	if teams[2].ProviderTeamID != "GHOST" || teams[2].Name != "GHOST" {
		t.Errorf("teams[2] = %+v, want the no-name-field project's key used as its name", teams[2])
	}
	for _, team := range teams {
		if team.ProviderType != "jira" {
			t.Errorf("ProviderType = %q, want jira", team.ProviderType)
		}
		projectKeys, _ := team.Associations.Get("project_keys")
		if list, ok := projectKeys.([]string); !ok || len(list) != 1 || list[0] != team.ProviderTeamID {
			t.Errorf("associations.project_keys = %v, want [%s]", projectKeys, team.ProviderTeamID)
		}
		providerOrg, _ := team.Associations.Get("provider_org")
		if providerOrg != "https://acme.atlassian.net" {
			t.Errorf("associations.provider_org = %v, want the credential's base_url", providerOrg)
		}
	}

	if doer.request == nil {
		t.Fatal("no request was made")
	}
	if doer.request.Method != http.MethodGet {
		t.Errorf("method = %s, want GET", doer.request.Method)
	}
	if doer.request.URL.Path != "/rest/api/3/project/search" {
		t.Errorf("path = %s, want /rest/api/3/project/search", doer.request.URL.Path)
	}
	if got := doer.request.URL.Query().Get("maxResults"); got != "100" {
		t.Errorf("maxResults = %q, want 100 (Python's discover_jira never paginates beyond this)", got)
	}
	wantAuth := "Basic ZGV2QGFjbWUudGVzdDpqaXJhLXRva2Vu"
	if got := doer.request.Header.Get("Authorization"); got != wantAuth {
		t.Errorf("Authorization = %q, want %q", got, wantAuth)
	}
}
