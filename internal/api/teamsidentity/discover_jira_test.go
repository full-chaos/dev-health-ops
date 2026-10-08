package teamsidentity

import (
	"bytes"
	"context"
	"errors"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
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

// TestDiscoverJiraMakesNoProviderCall: Jira team discovery reads the
// Atlassian teams the catalog stored; it never lists the Jira projects (GET
// /rest/api/3/project/search) as teams any more. Without the store it fails
// and sends no request. The stored-team list itself is pinned against a real
// ClickHouse in TestDiscoverJiraListsOnlyStoredActiveAtlassianTeams.
func TestDiscoverJiraMakesNoProviderCall(t *testing.T) {
	doer := &jiraStubDoer{status: 200, body: `{"values": [{"key": "ENG", "name": "Engineering"}]}`}
	oldClient := fakehttp.Client(discoveryHTTPClient)
	discoveryHTTPClient = fakehttp.Client(doer)
	defer func() { discoveryHTTPClient = fakehttp.Client(oldClient) }()

	teams, err := discoverJira(context.Background(), nil, "org-1", jiraTestCredential())
	if !errors.Is(err, errDiscoverJiraNoStore) {
		t.Fatalf("discoverJira without a store: err = %v, want errDiscoverJiraNoStore", err)
	}
	if teams != nil {
		t.Errorf("discoverJira without a store: teams = %+v, want none", teams)
	}
	if doer.request != nil {
		t.Errorf("discoverJira sent %s %s, want no provider request", doer.request.Method, doer.request.URL.Path)
	}
}
