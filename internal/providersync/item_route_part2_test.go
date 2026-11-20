package providersync

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// The jira_, linear_, pagerduty_ and native REST item decode sites of CHAOS-14280: each names the route that served its item.
// One cell per site class; the Jira cells also pin that a converted caller stays a permanent comment-read failure.

func itemRouteJiraClient(t *testing.T, bodies map[string]string) *providerfoundation.HTTPClient {
	t.Helper()
	doer := jiraWorkItemsDoerFunc(func(request *http.Request) (*http.Response, error) {
		body, ok := bodies[request.URL.Path]
		if !ok {
			return nil, io.ErrUnexpectedEOF
		}
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
	})
	return jiraWorkItemsTestClient(t, fakehttp.Client(doer), providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }))
}

func requireJiraItemFailure(t *testing.T, err error, route string) {
	t.Helper()
	requireItemRouteFailure(t, err, route)
	if !jiraCommentReadPermanent(err) {
		t.Fatalf("jiraCommentReadPermanent(%v) = false; want true", err)
	}
}

func TestJiraWorkItemsSearchItemFailureNamesTheSearchRoute(t *testing.T) {
	claim := nativeTestClaim("jira", "work-items")
	claim.SourceExternalID = "OPS"
	client := itemRouteJiraClient(t, map[string]string{
		"/rest/api/3/search/jql": `{"issues":[[]],"isLast":true}`,
		"/rest/api/3/field":      `[{"id":"summary","name":"Summary","custom":false,"schema":{"type":"string"}}]`,
	})
	_, err := (JiraWorkItemsRouteHandler{StatusMapping: loadRealStatusMapping(t), Identity: jiraRouteIdentity}).Collect(
		context.Background(), claim, providerfoundation.Credential{}, client, time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC),
	)
	requireJiraItemFailure(t, err, jiraSearchJQLRoute)
}

func TestJiraIncidentSearchItemFailureNamesTheSearchRoute(t *testing.T) {
	t.Parallel()
	claim := nativeTestClaim("jira", "incidents")
	claim.SourceExternalID = "JSM"
	doer := jiraIncidentDoerFunc(func(request *http.Request) (*http.Response, error) {
		body := `{"cloudId":"cloud-123"}`
		switch request.URL.Path {
		case "/rest/servicedeskapi/servicedesk":
			body = `{"values":[{"projectKey":"JSM"}],"isLastPage":true}`
		case "/rest/api/3/search/jql":
			body = `{"issues":[[]],"isLast":true}`
		}
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
	})
	client := jiraWorkItemsTestClient(t, fakehttp.Client(doer), providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }))
	_, err := (JiraIncidentRouteHandler{Entitlement: allowIncidentEntitlement}).Collect(
		context.Background(), claim, providerfoundation.Credential{}, client, time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC),
	)
	requireJiraItemFailure(t, err, jiraSearchJQLRoute)
}

func TestJiraIncidentEnvelopeFailuresNameTheirRoutes(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct{ desk, search, route string }{
		"servicedesk": {`{"values":"x","isLastPage":true}`, `{"issues":[],"isLast":true}`, jiraServiceDeskRoute},
		"search":      {`{"values":[{"projectKey":"JSM"}],"isLastPage":true}`, `{"issues":"x","isLast":true}`, jiraSearchJQLRoute},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			itemRouteJiraIncidentEnvelope(t, tc.desk, tc.search, tc.route)
		})
	}
}

func itemRouteJiraIncidentEnvelope(t *testing.T, desk, search, route string) {
	t.Helper()
	claim := nativeTestClaim("jira", "incidents")
	claim.SourceExternalID = "JSM"
	doer := jiraIncidentDoerFunc(func(request *http.Request) (*http.Response, error) {
		body := `{"cloudId":"cloud-123"}`
		switch request.URL.Path {
		case "/rest/servicedeskapi/servicedesk":
			body = desk
		case "/rest/api/3/search/jql":
			body = search
		}
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
	})
	client := jiraWorkItemsTestClient(t, fakehttp.Client(doer), providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }))
	_, err := (JiraIncidentRouteHandler{Entitlement: allowIncidentEntitlement}).Collect(
		context.Background(), claim, providerfoundation.Credential{}, client, time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC),
	)
	requireJiraItemFailure(t, err, route)
}

func TestJiraCommentItemFailureNamesTheCommentRoute(t *testing.T) {
	t.Parallel()
	client := itemRouteJiraClient(t, map[string]string{"/rest/api/3/issue/OPS-1/comment": `{"comments":[[]],"isLast":true}`})
	_, _, err := collectJiraIssueComments(context.Background(), client, "jira:OPS-1", 5, 50, 0)
	requireJiraItemFailure(t, err, jiraIssueCommentRoute)
}

func TestJiraAtlassianItemFailuresNameTheirRoutes(t *testing.T) {
	t.Parallel()
	t.Run("sprint", func(t *testing.T) {
		t.Parallel()
		client := itemRouteJiraClient(t, map[string]string{"/rest/agile/1.0/board/7/sprint": `{"values":[[]],"isLast":true}`})
		_, _, err := collectJiraBoardSprints(context.Background(), client, 7, 5, 50)
		requireJiraItemFailure(t, err, jiraBoardSprintRoute)
	})
	t.Run("worklog", func(t *testing.T) {
		t.Parallel()
		client := itemRouteJiraClient(t, map[string]string{"/rest/api/3/issue/OPS-1/worklog": `{"worklogs":[[]],"total":1}`})
		_, _, _, err := collectJiraAtlassianWorklogs(context.Background(), client, nil, "OPS-1", false, 5, "cloud")
		requireJiraItemFailure(t, err, jiraIssueWorklogRoute)
	})
	t.Run("team catalog sprint", func(t *testing.T) {
		t.Parallel()
		client := itemRouteJiraClient(t, map[string]string{
			"/rest/agile/1.0/board":          `{"values":[{"id":7}],"isLast":true}`,
			"/rest/agile/1.0/board/7/sprint": `{"values":[[]],"isLast":true}`,
		})
		_, err := (JiraTeamCatalogRouteHandler{}).collectProjectSprints(
			context.Background(), client, nativeTestClaim("jira", "team-catalog"), "OPS", time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC),
		)
		requireJiraItemFailure(t, err, jiraBoardSprintRoute)
	})
}

func TestLinearIssueItemFailureNamesTheGraphQLRoute(t *testing.T) {
	t.Parallel()
	doer := &linearWorkItemsDoer{responses: []string{
		linearTeamResponse(),
		`{"data":{"issues":{"nodes":[[]],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}`,
	}}
	claim := nativeTestClaim("linear", "work-items")
	claim.SourceExternalID = "ENG"
	noCycles := false
	_, err := (LinearWorkItemsRouteHandler{PerPage: 50, MaxPages: 10, FetchCycles: &noCycles}).Collect(
		context.Background(), claim, providerfoundation.Credential{Provider: "linear", ID: claim.CredentialID},
		linearWorkItemsClient(t, doer), time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC),
	)
	requireItemRouteFailure(t, err, graphQLRoute)
}

func TestLinearTeamItemFailureNamesTheGraphQLRoute(t *testing.T) {
	t.Parallel()
	doer := &linearWorkItemsDoer{responses: []string{
		`{"data":{"teams":{"nodes":[[]],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}`,
	}}
	claim := nativeTestClaim("linear", "work-items")
	claim.SourceExternalID = "ENG"
	_, err := (LinearWorkItemsRouteHandler{PerPage: 50, MaxPages: 10}).Collect(
		context.Background(), claim, providerfoundation.Credential{Provider: "linear", ID: claim.CredentialID},
		linearWorkItemsClient(t, doer), time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC),
	)
	requireItemRouteFailure(t, err, graphQLRoute)
}

func TestPagerDutyListItemFailureNamesTheListRoute(t *testing.T) {
	t.Parallel()
	cases := []struct {
		dataset, key, route string
		handler             CompleteRouteHandler
	}{
		{"business-services", "business_services", "/business_services", PagerDutyBusinessServicesRouteHandler{Entitlement: allowIncidentEntitlement}},
		{"escalation-policies", "escalation_policies", "/escalation_policies", PagerDutyEscalationPoliciesRouteHandler{Entitlement: allowIncidentEntitlement}},
		{"on-calls", "oncalls", "/oncalls", PagerDutyOnCallsRouteHandler{Entitlement: allowIncidentEntitlement}},
		{"schedules", "schedules", "/schedules", PagerDutySchedulesRouteHandler{Entitlement: allowIncidentEntitlement}},
		{"services", "services", "/services", PagerDutyServicesRouteHandler{Entitlement: allowIncidentEntitlement}},
		{"teams", "teams", "/teams", PagerDutyTeamsRouteHandler{Entitlement: allowIncidentEntitlement}},
		{"users", "users", "/users", PagerDutyUsersRouteHandler{Entitlement: allowIncidentEntitlement}},
	}
	for _, test := range cases {
		t.Run(test.dataset, func(t *testing.T) {
			t.Parallel()
			doer := &pagerDutyTeamsDoer{t: t, responses: []pagerDutyTeamsResponse{{body: `{"` + test.key + `":[[]],"more":false}`}}}
			client := pagerDutyTeamsTestClient(t, fakehttp.Client(doer), providerfoundation.RetryPolicy{MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond})
			_, err := test.handler.Collect(
				context.Background(), nativeTestClaim("pagerduty", test.dataset),
				providerfoundation.Credential{Provider: "pagerduty", Config: map[string]string{"subdomain": "acme"}},
				client, time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC),
			)
			requireItemRouteFailure(t, err, test.route)
		})
	}
}

func fetchNativeRESTForItemRoute(t *testing.T, provider, dataset string, bodies map[string]string) error {
	t.Helper()
	client, err := providerfoundation.NewHTTPClient(
		provider, "https://fixture.test", fakehttp.Client(itemRoutePathDoer(bodies)), func(*http.Request) error { return nil },
		providerfoundation.RetryPolicy{MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond},
		providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
	)
	if err != nil {
		t.Fatal(err)
	}
	handler := NativeRESTHandler{Now: func() time.Time { return time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC) }}
	_, err = handler.Fetch(context.Background(), nativeTestClaim(provider, dataset), client)
	return err
}

func TestNativeRESTIssueItemFailureNamesTheIssuesRoute(t *testing.T) {
	t.Parallel()
	err := fetchNativeRESTForItemRoute(t, "github", "work-items", map[string]string{"/repos/acme/api/issues": `[[]]`})
	requireItemRouteFailure(t, err, githubIssuesRoute)
}

func TestNativeRESTChildItemFailureNamesTheChildRoute(t *testing.T) {
	t.Parallel()
	err := fetchNativeRESTForItemRoute(t, "github", "work-item-history", map[string]string{
		"/repos/acme/api/issues":          `[{"number":1,"title":"t","state":"open","updated_at":"2026-07-10T00:00:00Z"}]`,
		"/repos/acme/api/issues/1/events": `[[]]`,
	})
	requireItemRouteFailure(t, err, githubIssueEventsRoute)
}

func TestNativeRESTNamedRecordAndProjectItemFailuresNameTheirRoutes(t *testing.T) {
	t.Parallel()
	requireItemRouteFailure(t, fetchNativeRESTForItemRoute(t, "github", "work-item-labels",
		map[string]string{"/repos/acme/api/labels": `[[]]`}), githubLabelsRoute)
	requireItemRouteFailure(t, fetchNativeRESTForItemRoute(t, "github", "work-item-projects",
		map[string]string{"/repos/acme/api/milestones": `[[]]`}), githubMilestonesRoute)
}

func TestLaunchDarklyCodeReferencesDocumentFailureNamesItsRoute(t *testing.T) {
	t.Parallel()
	doer := &launchDarklyRouteDoer{responses: []launchDarklyRouteResponse{
		{status: http.StatusOK, body: `{"items":[],"totalCount":0}`},
		{status: http.StatusOK, body: `{"items":[]}`},
		{status: http.StatusOK, body: `[]`},
	}}
	client, err := providerfoundation.NewHTTPClient(
		"launchdarkly", "https://app.launchdarkly.com", fakehttp.Client(doer),
		func(*http.Request) error { return nil },
		providerfoundation.RetryPolicy{MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond},
		providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
	)
	if err != nil {
		t.Fatal(err)
	}
	claim := nativeTestClaim("launchdarkly", "feature-flags")
	claim.DatasetOptions = map[string]any{"project_key": "payments"}
	_, err = (LaunchDarklyRouteHandler{CodeReferences: staticLaunchDarklyReferenceResolver{}}).Collect(
		context.Background(), claim, providerfoundation.Credential{Provider: "launchdarkly"}, client,
		time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC),
	)
	requireItemRouteFailure(t, err, launchDarklyCodeRefsRoute)
}
