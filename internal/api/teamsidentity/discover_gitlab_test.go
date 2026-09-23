package teamsidentity

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

type gitlabStubDoer struct {
	responses map[string]struct {
		body       string
		nextPage   string
		statusCode int
	}
}

func (d *gitlabStubDoer) Do(request *http.Request) (*http.Response, error) {
	key := request.URL.Path + "?" + request.URL.RawQuery
	stub, ok := d.responses[key]
	status := stub.statusCode
	if !ok {
		return &http.Response{StatusCode: 404, Header: http.Header{}, Body: io.NopCloser(bytes.NewReader([]byte(`{}`)))}, nil
	}
	if status == 0 {
		status = 200
	}
	header := http.Header{"Content-Type": {"application/json"}}
	if stub.nextPage != "" {
		header.Set("X-Next-Page", stub.nextPage)
	}
	return &http.Response{StatusCode: status, Header: header, Body: io.NopCloser(bytes.NewReader([]byte(stub.body)))}, nil
}

func gitlabTestCredential() providerfoundation.Credential {
	return providerfoundation.NewCredential("gitlab", "cred-1", map[string]string{}, map[string]secrets.Value{
		"token": secrets.NewValue("gitlab-token"),
	})
}

// TestDiscoverGitLabTruncatesSubgroupsAtTheBound mirrors
// team_discovery.py's _bounded_list applied to the subgroups walk: with
// MORE than maxGitLabDiscoverySubgroups items available, only the bound
// is kept and a truncation warning is appended -- proven here with a tiny
// bound-independent fixture by seeding exactly one extra subgroup beyond
// what one page returns and NOT declaring an X-Next-Page (proving the
// islice(limit+1)-style "peek one past the bound" check, not merely
// "stop when the server says no more pages").
func TestDiscoverGitLabTruncatesSubgroupsAtTheBound(t *testing.T) {
	// Build maxGitLabDiscoverySubgroups+1 subgroups in one page response
	// (per_page=100 would normally paginate, but a single stub response
	// containing more than the bound proves the CODE's own islice-style
	// cutoff, independent of how many real pages GitLab would have used).
	var subgroupsBody bytes.Buffer
	subgroupsBody.WriteByte('[')
	for i := 0; i < maxGitLabDiscoverySubgroups+1; i++ {
		if i > 0 {
			subgroupsBody.WriteByte(',')
		}
		fmt.Fprintf(&subgroupsBody, `{"id":%d,"full_path":"acme/sub-%d","name":"Sub %d","description":""}`, 100+i, i, i)
	}
	subgroupsBody.WriteByte(']')

	responses := map[string]struct {
		body       string
		nextPage   string
		statusCode int
	}{
		"/api/v4/groups/acme?":                              {body: `{"id":1,"full_path":"acme","name":"Acme","description":"root"}`},
		"/api/v4/groups/acme/subgroups?per_page=100&page=1": {body: subgroupsBody.String()},
	}
	// Every group's (root + each subgroup) own project list, and the flat
	// all-projects walk, all answer empty so this test isolates the
	// subgroup-truncation behavior alone.
	responses["/api/v4/groups/1/projects?per_page=100&page=1"] = struct {
		body       string
		nextPage   string
		statusCode int
	}{body: `[]`}
	for i := 0; i < maxGitLabDiscoverySubgroups; i++ {
		key := fmt.Sprintf("/api/v4/groups/%d/projects?per_page=100&page=1", 100+i)
		responses[key] = struct {
			body       string
			nextPage   string
			statusCode int
		}{body: `[]`}
	}
	responses["/api/v4/groups/1/projects?per_page=100&page=1&include_subgroups=true"] = struct {
		body       string
		nextPage   string
		statusCode int
	}{body: `[]`}

	doer := &gitlabStubDoer{responses: responses}
	oldClient := discoveryHTTPClient
	discoveryHTTPClient = doer
	defer func() { discoveryHTTPClient = oldClient }()

	teams, truncated, warnings, err := discoverGitLab(context.Background(), gitlabTestCredential(), "acme")
	if err != nil {
		t.Fatalf("discoverGitLab: %v", err)
	}
	// root + exactly the bound's worth of subgroups, never the (bound+1)th.
	if len(teams) != maxGitLabDiscoverySubgroups+1 {
		t.Fatalf("got %d teams, want %d (root + bound subgroups, the extra one dropped)", len(teams), maxGitLabDiscoverySubgroups+1)
	}
	if !truncated {
		t.Error("truncated = false, want true")
	}
	wantWarning := gitlabTruncationWarning("subgroups", "acme", maxGitLabDiscoverySubgroups)
	found := false
	for _, warning := range warnings {
		if warning == wantWarning {
			found = true
		}
	}
	if !found {
		t.Errorf("warnings = %v, want to contain %q", warnings, wantWarning)
	}
	for _, team := range teams {
		if team.ProviderType != "gitlab" {
			t.Errorf("ProviderType = %q, want gitlab", team.ProviderType)
		}
	}
	if teams[0].ProviderTeamID != "acme" {
		t.Errorf("teams[0] = %+v, want the root group first", teams[0])
	}
}

// TestDiscoverGitLabNotTruncatedWhenUnderBound proves the negative: a
// walk that returns fewer items than the bound reports truncated=false
// and no warning -- otherwise the mutation-kill for the truncation check
// (an always-true condition) would be indistinguishable from correct
// behavior on the happy path.
func TestDiscoverGitLabNotTruncatedWhenUnderBound(t *testing.T) {
	responses := map[string]struct {
		body       string
		nextPage   string
		statusCode int
	}{
		"/api/v4/groups/acme?":                                                 {body: `{"id":1,"full_path":"acme","name":"Acme","description":""}`},
		"/api/v4/groups/acme/subgroups?per_page=100&page=1":                    {body: `[]`},
		"/api/v4/groups/1/projects?per_page=100&page=1":                        {body: `[{"id":5,"path_with_namespace":"acme/api"}]`},
		"/api/v4/groups/1/projects?per_page=100&page=1&include_subgroups=true": {body: `[{"id":5,"path_with_namespace":"acme/api"}]`},
	}
	doer := &gitlabStubDoer{responses: responses}
	oldClient := discoveryHTTPClient
	discoveryHTTPClient = doer
	defer func() { discoveryHTTPClient = oldClient }()

	teams, truncated, warnings, err := discoverGitLab(context.Background(), gitlabTestCredential(), "acme")
	if err != nil {
		t.Fatalf("discoverGitLab: %v", err)
	}
	if truncated {
		t.Errorf("truncated = true, want false: warnings=%v", warnings)
	}
	if len(warnings) != 0 {
		t.Errorf("warnings = %v, want none", warnings)
	}
	if len(teams) != 1 || teams[0].ProviderTeamID != "acme" {
		t.Fatalf("teams = %+v, want just the root group", teams)
	}
	repoPatterns, _ := teams[0].Associations.Get("repo_patterns")
	if list, ok := repoPatterns.([]string); !ok || len(list) != 1 || list[0] != "acme/api" {
		t.Errorf("repo_patterns = %v, want [acme/api]", repoPatterns)
	}
}
