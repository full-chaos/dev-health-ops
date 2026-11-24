package providersync

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

var sweepG4NormalizedAt = time.Date(2026, 8, 10, 12, 0, 0, 123456000, time.UTC)

func sweepG4LinearIssueResponse() string {
	return `{
  "data": {"issues": {"nodes": [{
    "id":"lin-issue-42","identifier":"ENG-42","title":"Preserve the Linear work-item contract",
    "description":"A non-empty issue exercises the canonical normalizer.","priority":2,"estimate":5,
    "createdAt":"2026-07-25T09:00:00Z","updatedAt":"2026-07-28T16:30:00Z","startedAt":"2026-07-26T10:00:00Z",
    "completedAt":null,"canceledAt":null,"dueDate":"2026-08-01T00:00:00Z","url":"https://linear.app/fullchaos/issue/ENG-42",
    "state":{"name":"In Progress","type":"started"},"labels":{"nodes":[{"name":"bug"},{"name":"priority::high"}]},
    "assignee":{"email":"alice@example.com","name":"Alice"},"creator":{"email":"bob@example.com","name":"Bob"},
    "team":{"id":"team-eng","key":"ENG","name":"Engineering"},"project":{"id":"project-platform","name":"Platform"},
    "cycle":{"id":"cycle-7","name":"Sprint 7","number":7},"parent":{"identifier":"ENG-1"},
    "history":{"nodes":[
      {"createdAt":"2026-07-26T10:00:00Z","fromState":{"name":"Todo","type":"unstarted"},"toState":{"name":"In Progress","type":"started"},"actor":{"email":"alice@example.com","name":"Alice"}}
    ]}
  }],"pageInfo":{"hasNextPage":false,"endCursor":null}}}
}`
}

func sweepG4JiraIncidentClient(t *testing.T, doer HTTPDoerForSweep, wait time.Duration) *providerfoundation.HTTPClient {
	t.Helper()
	client, err := providerfoundation.NewHTTPClient(
		"jira", "https://acme.atlassian.net", fakehttp.Client(doer),
		func(request *http.Request) error {
			request.Header.Set("Accept", "application/json")
			request.Header.Set("Content-Type", "application/json")
			return nil
		},
		providerfoundation.RetryPolicy{MaxAttempts: 3, InitialWait: wait, MaxWait: wait},
		providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
	)
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func init() {
	registerRetrySweepRoute(retrySweepRoute{
		name:    "jira/incidents",
		newDoer: func() HTTPDoerForSweep { return &jiraIncidentDoer{t: sweepG4T()} },
		collect: func(t *testing.T, ctx context.Context, doer HTTPDoerForSweep, wait time.Duration) (CompleteRouteBatch, error) {
			claim := nativeTestClaim("jira", "incidents")
			claim.SourceExternalID = "JSM"
			return (JiraIncidentRouteHandler{Entitlement: allowIncidentEntitlement}).Collect(
				ctx, claim, providerfoundation.Credential{},
				sweepG4JiraIncidentClient(t, doer, wait), time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC),
			)
		},
	})
	registerRetrySweepRoute(retrySweepRoute{
		name:    "jira/atlassian",
		newDoer: func() HTTPDoerForSweep { return &jiraAtlassianDoer{t: sweepG4T()} },
		collect: func(t *testing.T, ctx context.Context, doer HTTPDoerForSweep, wait time.Duration) (CompleteRouteBatch, error) {
			return (JiraAtlassianRouteHandler{StatusMapping: sweepStatusMapping(t), Identity: jiraRouteIdentity}).Collect(
				ctx, jiraAtlassianClaim(), providerfoundation.Credential{},
				sweepClient(t, "jira", "https://acme.atlassian.net", doer, wait), sweepG4NormalizedAt,
			)
		},
	})
	registerRetrySweepRoute(retrySweepRoute{
		name:    "jira/work-items",
		newDoer: func() HTTPDoerForSweep { return &jiraWorkItemsDoer{t: sweepG4T()} },
		collect: func(t *testing.T, ctx context.Context, doer HTTPDoerForSweep, wait time.Duration) (CompleteRouteBatch, error) {
			claim := nativeTestClaim("jira", "work-items")
			claim.SourceExternalID = "OPS"
			claim.DatasetOptions = map[string]any{
				"fetch_comments": true,
				"comments_limit": 10,
				"sprint_field":   "customfield_10020",
			}
			return (JiraWorkItemsRouteHandler{
				StatusMapping: sweepStatusMapping(t), Identity: jiraRouteIdentity,
			}).Collect(
				ctx, claim, providerfoundation.Credential{},
				sweepClient(t, "jira", "https://acme.atlassian.net", doer, wait), sweepG4NormalizedAt,
			)
		},
	})
	registerRetrySweepRoute(retrySweepRoute{
		name: "linear/work-items",
		newDoer: func() HTTPDoerForSweep {
			return &linearWorkItemsDoer{responses: []string{linearTeamResponse(), sweepG4LinearIssueResponse()}}
		},
		collect: func(t *testing.T, ctx context.Context, doer HTTPDoerForSweep, wait time.Duration) (CompleteRouteBatch, error) {
			claim := nativeTestClaim("linear", "work-items")
			claim.SourceExternalID = "ENG"
			noCycles := false
			return (LinearWorkItemsRouteHandler{PerPage: 50, MaxPages: 10, FetchCycles: &noCycles}).Collect(
				ctx, claim, providerfoundation.Credential{Provider: "linear", ID: claim.CredentialID},
				sweepClient(t, "linear", "https://api.linear.app", doer, wait), sweepG4NormalizedAt,
			)
		},
	})
	registerRetrySweepRoute(retrySweepRoute{
		name: "linear/work-items-family",
		newDoer: func() HTTPDoerForSweep {
			return &linearWorkItemsDoer{responses: []string{
				linearFamilyEmptyCyclesResponse(),
				linearLifecycleIssueResponse("ENG-1", "ENG"),
			}}
		},
		collect: func(t *testing.T, ctx context.Context, doer HTTPDoerForSweep, wait time.Duration) (CompleteRouteBatch, error) {
			claim := linearFamilyClaim()
			return (LinearWorkItemFamilyRouteHandler{Direct: linearFamilyDirectHandler()}).Collect(
				ctx, claim, providerfoundation.Credential{Provider: "linear", ID: claim.CredentialID},
				sweepClient(t, "linear", "https://api.linear.app", doer, wait), sweepG4NormalizedAt,
			)
		},
	})
	registerRetrySweepRoute(retrySweepRoute{
		name: "launchdarkly/feature-flags",
		newDoer: func() HTTPDoerForSweep {
			return &launchDarklyRouteDoer{responses: []launchDarklyRouteResponse{
				{status: http.StatusOK, body: `{"items":[{"key":"checkout","_projectKey":"payments","creationDate":1725000000123}],"totalCount":1}`},
				{status: http.StatusOK, body: `{"items":[{"_id":"event-1","kind":"toggleFlag","date":1725000001234,"target":{"resources":["proj/payments:env/prod:flag/checkout"]}}]}`},
				{status: http.StatusOK, body: `{"items":[]}`},
			}}
		},
		collect: func(t *testing.T, ctx context.Context, doer HTTPDoerForSweep, wait time.Duration) (CompleteRouteBatch, error) {
			claim := nativeTestClaim("launchdarkly", "feature-flags")
			claim.DatasetOptions = map[string]any{"project_key": "payments", "environment": "production"}
			return (LaunchDarklyRouteHandler{CodeReferences: staticLaunchDarklyReferenceResolver{}}).Collect(
				ctx, claim, providerfoundation.Credential{Provider: "launchdarkly", Config: map[string]string{}},
				sweepClient(t, "launchdarkly", "https://app.launchdarkly.com", doer, wait), time.Date(2026, 7, 23, 12, 34, 56, 789000000, time.UTC),
			)
		},
	})
}

// sweepG4T is the *testing.T the fixture doers keep for an unexpected-request
// Fatalf; the sweep gives newDoer no t, so a detached one stands in.
func sweepG4T() *testing.T { return &testing.T{} }
