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

// githubStubDoer answers by exact path (query string stripped), so a test
// can script a fixed set of endpoints (the team list, a paginated repo
// list, a team-detail lookup) without caring about call order.
type githubStubDoer struct {
	responses map[string]struct {
		status int
		body   string
		link   string
	}
	requests []*http.Request
}

func (d *githubStubDoer) Do(request *http.Request) (*http.Response, error) {
	d.requests = append(d.requests, request)
	key := request.URL.Path
	if request.URL.RawQuery != "" {
		key += "?" + request.URL.RawQuery
	}
	stub, ok := d.responses[key]
	if !ok {
		return &http.Response{StatusCode: 404, Header: http.Header{}, Body: io.NopCloser(bytes.NewReader([]byte(`{}`)))}, nil
	}
	header := http.Header{"Content-Type": {"application/json"}}
	if stub.link != "" {
		header.Set("Link", stub.link)
	}
	return &http.Response{StatusCode: stub.status, Header: header, Body: io.NopCloser(bytes.NewReader([]byte(stub.body)))}, nil
}

func githubTestCredential() providerfoundation.Credential {
	return providerfoundation.NewCredential("github", "cred-1", map[string]string{}, map[string]secrets.Value{
		"token": secrets.NewValue("github-token"),
	})
}

// TestDiscoverGitHubWalksTeamsAndRepoPages mirrors
// TeamDiscoveryService.discover_github (team_discovery.py:127-158):
// org.get_teams() (paginated), per team team.get_repos() (also paginated,
// mapped to "org/repo" strings), and a member_count read -- this test
// pins the SAFE choice documented on discoverGitHub's own doc comment
// (an explicit GET /orgs/{org}/teams/{slug} per team for member_count),
// not a claim about what GitHub's list response itself carries.
func TestDiscoverGitHubWalksTeamsAndRepoPages(t *testing.T) {
	doer := &githubStubDoer{responses: map[string]struct {
		status int
		body   string
		link   string
	}{
		"/orgs/acme/teams?per_page=100": {
			status: 200,
			body:   `[{"slug":"eng","name":"Engineering","description":"core"}]`,
			link:   `<https://api.github.com/orgs/acme/teams?page=2>; rel="next", <https://api.github.com/orgs/acme/teams?page=2>; rel="last"`,
		},
		"/orgs/acme/teams?page=2": {
			status: 200,
			body:   `[{"slug":"design","name":"Design","description":null}]`,
		},
		"/orgs/acme/teams/eng/repos?per_page=100": {
			status: 200,
			body:   `[{"name":"api"},{"name":"web"}]`,
		},
		"/orgs/acme/teams/design/repos?per_page=100": {
			status: 200,
			body:   `[]`,
		},
		"/orgs/acme/teams/eng": {
			status: 200,
			body:   `{"members_count":5}`,
		},
		"/orgs/acme/teams/design": {
			status: 200,
			body:   `{"members_count":0}`,
		},
	}}
	oldClient := discoveryHTTPClient
	discoveryHTTPClient = doer
	defer func() { discoveryHTTPClient = oldClient }()

	teams, err := discoverGitHub(context.Background(), githubTestCredential(), "acme")
	if err != nil {
		t.Fatalf("discoverGitHub: %v", err)
	}
	if len(teams) != 2 {
		t.Fatalf("got %d teams, want 2 (one per page of the team list): %+v", len(teams), teams)
	}
	eng, design := teams[0], teams[1]
	if eng.ProviderTeamID != "eng" || eng.Name != "Engineering" || eng.MemberCount == nil || *eng.MemberCount != 5 {
		t.Errorf("teams[0] = %+v", eng)
	}
	repoPatterns, _ := eng.Associations.Get("repo_patterns")
	if list, ok := repoPatterns.([]string); !ok || len(list) != 2 || list[0] != "acme/api" || list[1] != "acme/web" {
		t.Errorf("eng repo_patterns = %v, want [acme/api acme/web]", repoPatterns)
	}
	if design.ProviderTeamID != "design" || design.MemberCount == nil || *design.MemberCount != 0 {
		t.Errorf("teams[1] = %+v", design)
	}
	designRepos, _ := design.Associations.Get("repo_patterns")
	if list, ok := designRepos.([]string); !ok || len(list) != 0 {
		t.Errorf("design repo_patterns = %v, want an empty list", designRepos)
	}
	for _, team := range teams {
		if team.ProviderType != "github" {
			t.Errorf("ProviderType = %q, want github", team.ProviderType)
		}
		providerOrg, _ := team.Associations.Get("provider_org")
		if providerOrg != "acme" {
			t.Errorf("associations.provider_org = %v, want acme", providerOrg)
		}
	}

	// Both team-list pages were actually fetched (proves pagination, not
	// just the first page happening to be complete).
	sawPage2 := false
	for _, request := range doer.requests {
		if request.URL.Path == "/orgs/acme/teams" && request.URL.RawQuery == "page=2" {
			sawPage2 = true
		}
		if got := request.Header.Get("Authorization"); got != "token github-token" {
			t.Errorf("Authorization = %q, want a PAT-shaped header", got)
		}
	}
	if !sawPage2 {
		t.Error("the team list's second page was never fetched")
	}
}
