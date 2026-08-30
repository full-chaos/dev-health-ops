package providersync

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// jiraSearchJQLMigrationDoer simulates Atlassian's actual retirement of
// GET /rest/api/3/search (410 Gone, body byte-for-byte matching CHAOS-4585's
// live-captured evidence) beside a working /rest/api/3/search/jql. Before the
// CHAOS-4585 fix, JiraAtlassianRouteHandler.Collect -- the handler actually
// registered for provider=jira dataset=work-items in
// cmd/dev-health-worker/provider_sync.go, NOT the unregistered
// JiraWorkItemsRouteHandler the ticket's original scope note pointed at --
// called the retired path via collectJiraAtlassianIssues and this test failed
// with exactly that shape. After the fix it calls /search/jql and succeeds.
type jiraSearchJQLMigrationDoer struct{ t *testing.T }

func (doer jiraSearchJQLMigrationDoer) Do(request *http.Request) (*http.Response, error) {
	doer.t.Helper()
	switch {
	case request.URL.Path == "/rest/api/3/search":
		body := `{"errorMessages":["The requested API has been removed. Please migrate to the /rest/api/3/search/jql API. A full migration guideline is available at https://developer.atlassian.com/cloud/jira/platform/changelog/#CHANGE-2046."],"errors":{}}`
		return &http.Response{
			StatusCode: http.StatusGone,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(strings.NewReader(body)), Request: request,
		}, nil
	case request.URL.Path == "/rest/api/3/search/jql":
		body := `{"issues":[{"key":"SUP-1","self":"https://acme.atlassian.net/rest/api/3/issue/SUP-1","fields":{"project":{"key":"SUP","id":"20001","name":"Support"},"summary":"Customer cannot log in","status":{"name":"Done","statusCategory":{"key":"done"}},"issuetype":{"name":"Bug"},"labels":[],"created":"2026-08-01T08:00:00Z","updated":"2026-08-02T09:00:00Z"}}],"isLast":true}`
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(strings.NewReader(body)), Request: request,
		}, nil
	case strings.HasSuffix(request.URL.Path, "/changelog"):
		body := `{"values":[],"total":0,"isLast":true}`
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(strings.NewReader(body)), Request: request,
		}, nil
	default:
		doer.t.Fatalf("unexpected Jira request %s", request.URL.String())
		return nil, nil
	}
}

// TestJiraAtlassianRouteMigratedFromRetiredSearchEndpoint is CHAOS-4585's
// red-on-baseline proof: on origin/main 2b3032b63 this FAILS (the registered
// route calls the retired, 410-returning /rest/api/3/search) and on the fix
// it passes (the route calls /rest/api/3/search/jql with cursor paging).
func TestJiraAtlassianRouteMigratedFromRetiredSearchEndpoint(t *testing.T) {
	claim := nativeTestClaim("jira", "work-items")
	claim.SourceExternalID = "SUP"
	client := jiraWorkItemsTestClient(
		t, jiraSearchJQLMigrationDoer{t: t},
		providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
	)
	handler := JiraAtlassianRouteHandler{StatusMapping: loadRealStatusMapping(t), Identity: jiraRouteIdentity}
	batch, err := handler.Collect(
		context.Background(), claim, providerfoundation.Credential{}, client,
		time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatalf(
			"Collect failed against a live-shaped /rest/api/3/search/jql fake -- "+
				"the registered Jira work-items route is still calling the retired "+
				"/rest/api/3/search endpoint (CHAOS-4585): %v", err,
		)
	}
	if batch.Result["work_items_synced"] != 1 {
		t.Fatalf("result=%#v", batch.Result)
	}
}
