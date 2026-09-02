package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

type jiraAtlassianDoer struct {
	t      *testing.T
	mu     sync.Mutex
	paths  []string
	boards int
	// devStatus/devStatusStatuses/devStatusBodies let a test control the
	// dev-status response, including a sequence of statuses (for retry
	// tests) -- zero-value (unset) means "this test does not expect a
	// dev-status call": the default handler below fails loudly if hit
	// unexpectedly.
	devStatus         int
	devStatusStatuses []int
	devStatusBody     string
}

func (doer *jiraAtlassianDoer) Do(request *http.Request) (*http.Response, error) {
	doer.t.Helper()
	doer.mu.Lock()
	doer.paths = append(doer.paths, request.URL.String())
	doer.mu.Unlock()

	var body string
	status := http.StatusOK
	switch {
	case request.URL.Path == "/rest/api/3/search/jql":
		body = `{"issues":[{"id":"10060","key":"OPS-201","self":"https://acme.atlassian.net/rest/api/3/issue/OPS-201","fields":{"project":{"key":"OPS","id":"10001","name":"Operations"},"summary":"Atlassian path","status":{"name":"Done","statusCategory":{"key":"done"}},"issuetype":{"name":"Task"},"labels":["support"],"priority":{"name":"Highest"},"assignee":{"accountId":"assignee-201","displayName":"Assignee"},"reporter":{"emailAddress":"reporter@example.com","accountId":"reporter-201","displayName":"Reporter"},"created":"2026-08-01T08:00:00Z","updated":"2026-08-02T09:00:00Z","resolutiondate":"2026-08-02T08:30:00Z","customfield_10020":[{"id":9001,"name":"August"}],"issuelinks":[{"type":{"outward":"blocks","inward":"is blocked by"},"outwardIssue":{"key":"OPS-202"}}]}}],"isLast":true}`
	case strings.HasPrefix(request.URL.Path, "/rest/api/3/issue/OPS-201/changelog"):
		body = `{"values":[{"created":"2026-08-01T09:00:00Z","author":{"accountId":"account-1"},"items":[{"field":"status","fromString":"To Do","toString":"Done"}]}],"total":1,"isLast":true}`
	case strings.HasPrefix(request.URL.Path, "/rest/api/3/issue/OPS-201/comment"):
		body = `{"comments":[{"created":"2026-08-02T10:00:00Z","author":{"accountId":"commenter"},"body":"verified"}],"isLast":true}`
	case strings.HasPrefix(request.URL.Path, "/rest/api/3/issue/OPS-201/worklog"):
		body = `{"startAt":0,"maxResults":100,"total":1,"worklogs":[{"id":"wl-201","author":{"accountId":"worker","displayName":"Worker"},"started":"2026-08-01T10:00:00.123456Z","timeSpentSeconds":2700,"created":"2026-08-01T10:01:00.123456Z","updated":"2026-08-01T10:02:00.123456Z"}]}`
	case request.URL.Path == "/rest/agile/1.0/board":
		doer.boards++
		body = `{"values":[{"id":77,"name":"Operations"}],"isLast":true}`
	case request.URL.Path == "/rest/agile/1.0/board/77/sprint":
		body = `{"values":[{"id":9001,"name":"August","state":"active","startDate":"2026-08-01T00:00:00Z","endDate":"2026-08-31T00:00:00Z"}],"isLast":true}`
	case request.URL.Path == "/rest/dev-status/1.0/issue/detail":
		index := doer.devStatus
		doer.devStatus++
		if len(doer.devStatusStatuses) == 0 {
			doer.t.Fatalf("unexpected dev-status request %s (test did not configure devStatusStatuses)", request.URL.String())
		}
		if index < len(doer.devStatusStatuses) {
			status = doer.devStatusStatuses[index]
		} else {
			status = doer.devStatusStatuses[len(doer.devStatusStatuses)-1]
		}
		body = doer.devStatusBody
	default:
		doer.t.Fatalf("unexpected Atlassian request %s", request.URL.String())
	}
	return &http.Response{StatusCode: status, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
}

func jiraAtlassianClaim() Claim {
	claim := nativeTestClaim("jira", "work-items")
	claim.SourceExternalID = "OPS"
	claim.DatasetOptions = map[string]any{
		"fetch_worklogs":      true,
		"fetch_board_sprints": true,
		"fetch_comments":      true,
		"comments_limit":      10,
		"sprint_field":        "customfield_10020",
	}
	return claim
}

func jiraAtlassianCompleteHandler(t *testing.T) JiraAtlassianRouteHandler {
	t.Helper()
	classifier, err := NewInvestmentClassifier(investmentConfigPath(t, "real"))
	if err != nil {
		t.Fatal(err)
	}
	statusMapping := loadRealStatusMapping(t)
	return JiraAtlassianRouteHandler{
		StatusMapping: statusMapping,
		Identity:      jiraRouteIdentity,
		Derived: JiraWorkItemDeriver{
			Source:               &githubMultiDayOracleSource{},
			statusMapping:        statusMapping,
			investmentClassifier: classifier,
		},
	}
}

func TestJiraAtlassianRouteCollectsWorklogsBoardsAndCanonicalEdges(t *testing.T) {
	claim := jiraAtlassianClaim()
	doer := &jiraAtlassianDoer{t: t}
	client := jiraWorkItemsTestClient(t, doer, providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }))
	batch, err := jiraAtlassianCompleteHandler(t).Collect(
		context.Background(), claim, providerfoundation.Credential{}, client,
		time.Date(2026, 8, 10, 12, 0, 0, 123456000, time.UTC),
	)
	if err != nil {
		t.Fatal(err)
	}
	if batch.Watermark == nil || !batch.Watermark.Equal(*claim.BeforeAt) {
		t.Fatalf("watermark=%v want=%v", batch.Watermark, claim.BeforeAt)
	}
	if len(batch.Effects) != 19 {
		t.Fatalf("effects=%d want=19 (six canonical facts, worklogs, two project-membership, and ten derived)", len(batch.Effects))
	}
	if batch.Result["worklogs_synced"] != 1 || batch.Result["sprints_synced"] != 1 || batch.Result["dependencies_synced"] != 1 || batch.Result["interactions_synced"] != 1 {
		t.Fatalf("result=%#v", batch.Result)
	}
	if batch.Evidence.Requests != 6 || batch.Evidence.Pages != 1 {
		t.Fatalf("evidence=%+v", batch.Evidence)
	}
	var worklog jiraWorklogRow
	var workItem jiraWorkItemRow
	for _, effect := range batch.Effects {
		if effect.Destination == "work_items" && len(effect.Rows) == 1 {
			if err := json.Unmarshal(effect.Rows[0], &workItem); err != nil {
				t.Fatal(err)
			}
		}
		if effect.Destination == "worklogs" && len(effect.Rows) == 1 {
			if err := json.Unmarshal(effect.Rows[0], &worklog); err != nil {
				t.Fatal(err)
			}
		}
	}
	if worklog.WorklogID != "wl-201" || worklog.TimeSpentSeconds != 2700 || worklog.Author == nil || *worklog.Author != "jira:accountid:worker" {
		t.Fatalf("worklog=%+v", worklog)
	}
	if len(workItem.Assignees) != 1 || workItem.Assignees[0] != "jira:accountid:assignee-201" || workItem.Reporter == nil || *workItem.Reporter != "reporter@example.com" {
		t.Fatalf("Atlassian identity breadth lost: work_item=%+v", workItem)
	}
	for _, raw := range doer.paths {
		parsed, err := url.Parse(raw)
		if err != nil {
			t.Fatal(err)
		}
		if parsed.Path == "/rest/api/3/search/jql" && parsed.Query().Get("expand") != "" {
			// CHAOS-4585 review: this route fetches each issue's changelog
			// separately (collectJiraAtlassianChangelog) and overwrites
			// issue["changelog"] with it, so an inlined search-response
			// changelog is unread bytes that can trip the 2MiB per-object cap.
			t.Fatalf("Atlassian route's search requested expand=%s -- changelog is fetched separately, not from the search response", parsed.Query().Get("expand"))
		}
		if parsed.Path == "/rest/api/3/search/jql" && parsed.Query().Get("startAt") != "0" {
			t.Fatalf("search pagination=%s", raw)
		}
	}
}

// TestJiraAtlassianRouteDevStatusSyncsPrimaryDependencyRow is red on
// origin/main -- the fetch_dev_status option does not exist on
// JiraAtlassianRouteHandler there. codex round 1 (P1) found the first
// implementation wired this into JiraWorkItemsRouteHandler instead, which
// the worker (cmd/dev-health-worker/provider_sync.go) never constructs for
// Jira work-items -- JiraAtlassianRouteHandler is the actually-active route.
func TestJiraAtlassianRouteDevStatusSyncsPrimaryDependencyRow(t *testing.T) {
	claim := jiraAtlassianClaim()
	claim.DatasetOptions["fetch_dev_status"] = true
	doer := &jiraAtlassianDoer{
		t: t, devStatusStatuses: []int{http.StatusOK},
		devStatusBody: `{"detail":[{"pullRequests":[{"url":"https://github.com/acme/api/pull/968"}]}]}`,
	}
	client := jiraWorkItemsTestClient(t, doer, providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }))
	batch, err := jiraAtlassianCompleteHandler(t).Collect(
		context.Background(), claim, providerfoundation.Credential{}, client,
		time.Date(2026, 8, 10, 12, 0, 0, 123456000, time.UTC),
	)
	if err != nil {
		t.Fatal(err)
	}
	if batch.Watermark == nil {
		t.Fatalf("expected the watermark to advance (dev-status success is not incompleteness): batch=%+v", batch)
	}
	if doer.devStatus != 1 {
		t.Fatalf("dev-status requests=%d want=1", doer.devStatus)
	}
	if got := batch.Result["dev_status_pull_requests_synced"]; got != 1 {
		t.Fatalf("result=%v", batch.Result)
	}
	if got := batch.Result["dev_status_unavailable_count"]; got != 0 {
		t.Fatalf("result=%v", batch.Result)
	}
	var devStatusDependency jiraWorkItemDependencyRow
	found := false
	for _, effect := range batch.Effects {
		if effect.Destination != "work_item_dependencies" {
			continue
		}
		for _, raw := range effect.Rows {
			var row jiraWorkItemDependencyRow
			if err := json.Unmarshal(raw, &row); err != nil {
				t.Fatal(err)
			}
			if row.RelationshipTypeRaw == "jira_dev_status" {
				devStatusDependency, found = row, true
			}
		}
	}
	if !found {
		t.Fatalf("no jira_dev_status dependency row in effects")
	}
	if devStatusDependency.SourceWorkItemID != "ghpr:acme/api#968" ||
		devStatusDependency.TargetWorkItemID != "jira:OPS-201" ||
		devStatusDependency.RelationshipType != "relates_to" {
		t.Fatalf("dev-status dependency=%+v", devStatusDependency)
	}
}

// TestJiraAtlassianRouteDevStatusUnavailableIsCleanNoOp is the red-first test
// for the ruling (chris via team-lead, 2026-09-01): an org with no
// GitHub-for-Jira app configured (400/404) must never fail the sync or
// suppress the watermark -- a typed, counted no-op only.
func TestJiraAtlassianRouteDevStatusUnavailableIsCleanNoOp(t *testing.T) {
	for _, status := range []int{http.StatusBadRequest, http.StatusNotFound} {
		status := status
		t.Run(http.StatusText(status), func(t *testing.T) {
			claim := jiraAtlassianClaim()
			claim.DatasetOptions["fetch_dev_status"] = true
			doer := &jiraAtlassianDoer{
				t: t, devStatusStatuses: []int{status}, devStatusBody: `{"errorMessages":["no dev-status data"]}`,
			}
			client := jiraWorkItemsTestClient(t, doer, providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }))
			batch, err := jiraAtlassianCompleteHandler(t).Collect(
				context.Background(), claim, providerfoundation.Credential{}, client,
				time.Date(2026, 8, 10, 12, 0, 0, 123456000, time.UTC),
			)
			if err != nil {
				t.Fatal(err)
			}
			if batch.Watermark == nil {
				t.Fatalf("a clean no-op must not suppress the watermark: batch=%+v", batch)
			}
			if incomplete, ok := batch.Result["incomplete"]; ok {
				t.Fatalf("a clean no-op must not be recorded as incompleteness: incomplete=%v", incomplete)
			}
			if got := batch.Result["dev_status_unavailable_count"]; got != 1 {
				t.Fatalf("result=%v", batch.Result)
			}
			if got := batch.Result["dev_status_pull_requests_synced"]; got != 0 {
				t.Fatalf("result=%v", batch.Result)
			}
		})
	}
}

// TestJiraAtlassianRouteDevStatusCapCountsRealWireAttempts is the red-first
// test for codex round 1's P2 finding: dev_status_max_requests must bound
// actual wire requests (including HTTPClient.Do's internal retries), not
// just logical fetch calls, or a transient outage can cost up to
// RetryPolicy.MaxAttempts times the configured budget with no signal.
// TestJiraAtlassianRouteDevStatusCapLimitsRealWireAttempts is the red-first
// test for codex round 2's P2 finding: counting real wire attempts AFTER
// the fact (round 1's fix) still let a single issue's own retries exceed
// dev_status_max_requests -- a budget of 1 permitted 3 real requests under
// sustained 503s, because nothing capped HTTPClient.Do's own retry policy
// for that call. The budget must cap the retry policy itself.
func TestJiraAtlassianRouteDevStatusCapLimitsRealWireAttempts(t *testing.T) {
	claim := jiraAtlassianClaim()
	claim.DatasetOptions["fetch_dev_status"] = true
	claim.DatasetOptions["dev_status_max_requests"] = 1
	doer := &jiraAtlassianDoer{
		t: t,
		// The client's own RetryPolicy allows 3 attempts, but only 1 remains
		// in the budget -- the fixed code must limit THIS call to 1 real
		// wire request, not exhaust all 3 retries and only notice after.
		devStatusStatuses: []int{http.StatusServiceUnavailable, http.StatusServiceUnavailable, http.StatusServiceUnavailable},
		devStatusBody:     `{"errorMessages":["temporarily unavailable"]}`,
	}
	client := jiraDevStatusTestClientWithRetries(t, doer, 3)
	batch, err := jiraAtlassianCompleteHandler(t).Collect(
		context.Background(), claim, providerfoundation.Credential{}, client,
		time.Date(2026, 8, 10, 12, 0, 0, 123456000, time.UTC),
	)
	if err != nil {
		t.Fatal(err)
	}
	if doer.devStatus != 1 {
		t.Fatalf("dev-status wire requests=%d want=1 (dev_status_max_requests must cap the retry policy, not just count after the fact)", doer.devStatus)
	}
	// The failed fetch is recorded as optional incompleteness (a genuine
	// error, not the clean no-op), so the watermark is withheld.
	if batch.Watermark != nil {
		t.Fatalf("a genuine dev-status fetch failure must withhold the watermark: batch=%+v", batch)
	}
}

// TestJiraAtlassianRouteDevStatusBudgetIsSharedAcrossIssues is EXECUTED
// coverage for the cross-issue invariant codex round 3 verified only
// statically ("Tests: only single-issue cap coverage exists; no dedicated
// multi-issue exhaustion test... unverified by execution"): two issues, a
// budget of 2, and the first issue alone (via retries on a transient
// failure) exhausts it -- the second issue must be skipped via
// dev_status_cap_skipped, not attempt any wire call, and the running
// devStatusRequestsIssued total must carry forward correctly between issues
// in one Collect call.
func TestJiraAtlassianRouteDevStatusBudgetIsSharedAcrossIssues(t *testing.T) {
	devStatusCalls := 0
	doer := jiraWorkItemsDoerFunc(func(request *http.Request) (*http.Response, error) {
		switch {
		case request.URL.Path == "/rest/api/3/search/jql":
			body := `{"issues":[` +
				`{"id":"20001","key":"OPS-301","self":"https://acme.atlassian.net/rest/api/3/issue/OPS-301","fields":{"project":{"key":"OPS"},"summary":"First","status":{"name":"Open","statusCategory":{"key":"new"}},"issuetype":{"name":"Task"},"labels":[],"created":"2026-08-01T00:00:00Z","updated":"2026-08-01T00:00:00Z"},"changelog":{"histories":[]}},` +
				`{"id":"20002","key":"OPS-302","self":"https://acme.atlassian.net/rest/api/3/issue/OPS-302","fields":{"project":{"key":"OPS"},"summary":"Second","status":{"name":"Open","statusCategory":{"key":"new"}},"issuetype":{"name":"Task"},"labels":[],"created":"2026-08-01T00:00:00Z","updated":"2026-08-01T00:00:00Z"},"changelog":{"histories":[]}}` +
				`],"isLast":true}`
			return &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
		case strings.HasPrefix(request.URL.Path, "/rest/api/3/issue/") && strings.HasSuffix(request.URL.Path, "/changelog"):
			return &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(`{"values":[],"total":0,"isLast":true}`)), Request: request}, nil
		case request.URL.Path == "/rest/dev-status/1.0/issue/detail":
			devStatusCalls++
			// Every wire attempt is a transient 503: with a budget of 2 and a
			// retrying client (MaxAttempts=3), OPS-301 alone should consume
			// exactly the remaining budget (2), leaving 0 for OPS-302.
			return &http.Response{StatusCode: http.StatusServiceUnavailable, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(`{"errorMessages":["temporarily unavailable"]}`)), Request: request}, nil
		default:
			t.Fatalf("unexpected request %s", request.URL.String())
			return nil, nil
		}
	})
	claim := nativeTestClaim("jira", "work-items")
	claim.SourceExternalID = "OPS"
	claim.DatasetOptions = map[string]any{
		"fetch_dev_status": true, "dev_status_max_requests": 2,
	}
	client := jiraDevStatusTestClientWithRetries(t, doer, 3)
	batch, err := jiraAtlassianCompleteHandler(t).Collect(
		context.Background(), claim, providerfoundation.Credential{}, client,
		time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatal(err)
	}
	if devStatusCalls != 2 {
		t.Fatalf("dev-status wire calls=%d want=2 (OPS-301 alone must exhaust the shared budget of 2, leaving none for OPS-302)", devStatusCalls)
	}
	if got := batch.Result["dev_status_cap_skipped"]; got != 1 {
		t.Fatalf("result=%v (OPS-302 must be skipped via the cap, not attempted)", batch.Result)
	}
	if got := batch.Result["dev_status_unavailable_count"]; got != 0 {
		t.Fatalf("result=%v (these are genuine 503 failures, not the clean 400/404 no-op)", batch.Result)
	}
	if batch.Watermark != nil {
		t.Fatalf("a genuine dev-status fetch failure must withhold the watermark: batch=%+v", batch)
	}
}

func TestJiraAtlassianRouteReferenceCacheSkipsBoardEnumeration(t *testing.T) {
	claim := jiraAtlassianClaim()
	claim.DatasetOptions["fetch_worklogs"] = false
	doer := &jiraAtlassianDoer{t: t}
	normalizedAt := time.Date(2026, 8, 10, 12, 0, 0, 123456000, time.UTC)
	refName, refState := "August", "active"
	refs := []jiraSprintRow{{Provider: "jira", SprintID: "9001", Name: &refName, State: &refState, LastSynced: normalizedAt, OrgID: claim.OrgID}}
	client := jiraWorkItemsTestClient(t, doer, providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }))
	handler := jiraAtlassianCompleteHandler(t)
	handler.ReferenceSprints = refs
	batch, err := handler.Collect(
		context.Background(), claim, providerfoundation.Credential{}, client, normalizedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	if doer.boards != 0 {
		t.Fatalf("board enumeration ignored reference cache: %d", doer.boards)
	}
	if batch.Watermark == nil || batch.Result["sprints_synced"] != 1 {
		t.Fatalf("batch=%+v", batch)
	}
}

func TestJiraAtlassianRouteWorklogFailureIsTypedAndWithholdsWatermark(t *testing.T) {
	claim := jiraAtlassianClaim()
	doer := jiraAtlassianDoerFunc(func(request *http.Request) (*http.Response, error) {
		if strings.HasPrefix(request.URL.Path, "/rest/api/3/issue/OPS-201/worklog") {
			return nil, context.DeadlineExceeded
		}
		return (&jiraAtlassianDoer{t: t}).Do(request)
	})
	client := jiraWorkItemsTestClient(t, doer, providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }))
	batch, err := jiraAtlassianCompleteHandler(t).Collect(
		context.Background(), claim, providerfoundation.Credential{}, client, time.Date(2026, 8, 10, 12, 0, 0, 123456000, time.UTC),
	)
	if err != nil {
		t.Fatal(err)
	}
	if batch.Watermark != nil {
		t.Fatalf("optional worklog failure advanced watermark: %v", batch.Watermark)
	}
	if len(batch.Effects) != 19 {
		t.Fatalf("optional worklog failure dropped recoverable effects: %d", len(batch.Effects))
	}
	incomplete, ok := batch.Result["incomplete"].([]string)
	if !ok || len(incomplete) != 1 || incomplete[0] != "worklogs:jira:OPS-201" {
		t.Fatalf("incomplete=%#v", batch.Result["incomplete"])
	}
}

func TestJiraAtlassianReferenceSinkFailureLandsEffectsAndWithholdsWatermark(t *testing.T) {
	claim := jiraAtlassianClaim()
	client := jiraWorkItemsTestClient(
		t,
		&jiraAtlassianDoer{t: t},
		providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
	)
	handler := jiraAtlassianCompleteHandler(t)
	handler.ReferenceSink = func([]jiraSprintRow) error { return errors.New("reference unavailable") }
	batch, err := handler.Collect(
		context.Background(), claim, providerfoundation.Credential{}, client,
		time.Date(2026, 8, 10, 12, 0, 0, 123456000, time.UTC),
	)
	if err != nil {
		t.Fatal(err)
	}
	if batch.Watermark != nil {
		t.Fatalf("reference sink failure advanced watermark: %v", batch.Watermark)
	}
	if len(batch.Effects) != 19 {
		t.Fatalf("reference sink failure dropped recoverable effects: %d", len(batch.Effects))
	}
	incomplete, ok := batch.Result["incomplete"].([]string)
	if !ok || len(incomplete) != 1 || incomplete[0] != "reference_sink" {
		t.Fatalf("incomplete=%#v", batch.Result["incomplete"])
	}
}

func TestJiraAtlassianGraphQLWorklogPreservesNameIdentity(t *testing.T) {
	claim := jiraAtlassianClaim()
	claim.DatasetOptions["atlassian_gql_enabled"] = true
	rest := &jiraAtlassianDoer{t: t}
	graphql := jiraAtlassianDoerFunc(func(request *http.Request) (*http.Response, error) {
		if request.URL.Path != "/graphql" {
			t.Fatalf("unexpected GraphQL request %s", request.URL.String())
		}
		body := `{"data":{"issue":{"worklogs":{"pageInfo":{"hasNextPage":false,"endCursor":""},"edges":[{"node":{"worklogId":"wl-gql","author":{"name":"GraphQL Worker"},"timeSpent":{"timeInSeconds":1800},"created":"2026-08-01T10:01:00.123456Z","updated":"2026-08-01T10:02:00.123456Z","startDate":"2026-08-01T10:00:00.123456Z"}}]}}}}`
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(strings.NewReader(body)), Request: request,
		}, nil
	})
	client := jiraWorkItemsTestClient(t, rest, providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }))
	graphqlClient := jiraWorkItemsTestClient(t, graphql, providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }))
	handler := jiraAtlassianCompleteHandler(t)
	handler.CloudID, handler.GraphQLClient = "cloud-301", graphqlClient
	batch, err := handler.Collect(
		context.Background(), claim, providerfoundation.Credential{}, client,
		time.Date(2026, 8, 10, 12, 0, 0, 123456000, time.UTC),
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.WorklogObservations) != 1 {
		t.Fatalf("worklog observations=%+v", batch.WorklogObservations)
	}
	observation := batch.WorklogObservations[0]
	if !observation.GraphQLAttempted || !observation.GraphQLSucceeded || observation.RESTFallbackUsed || observation.GraphQLRequests != 1 || observation.RESTRequests != 0 {
		t.Fatalf("GraphQL observation=%+v", observation)
	}
	if len(batch.Effects) != 19 {
		t.Fatalf("effects=%d want=19", len(batch.Effects))
	}
	var worklog jiraWorklogRow
	for _, effect := range batch.Effects {
		if effect.Destination != "worklogs" || len(effect.Rows) != 1 {
			continue
		}
		if err := json.Unmarshal(effect.Rows[0], &worklog); err != nil {
			t.Fatal(err)
		}
	}
	if worklog.Author == nil || *worklog.Author != "GraphQL Worker" {
		t.Fatalf("GraphQL name identity=%v want GraphQL Worker", worklog.Author)
	}
}

func TestJiraAtlassianGraphQLFailureRecordsRESTFallbackObservation(t *testing.T) {
	claim := jiraAtlassianClaim()
	claim.DatasetOptions["atlassian_gql_enabled"] = true
	rest := &jiraAtlassianDoer{t: t}
	graphql := jiraAtlassianDoerFunc(func(request *http.Request) (*http.Response, error) {
		if request.URL.Path != "/graphql" {
			t.Fatalf("unexpected GraphQL request %s", request.URL.String())
		}
		return &http.Response{
			StatusCode: http.StatusBadGateway,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(strings.NewReader(`{"errors":[{"message":"temporary"}]}`)), Request: request,
		}, nil
	})
	client := jiraWorkItemsTestClient(t, rest, providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }))
	graphqlClient := jiraWorkItemsTestClient(t, graphql, providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }))
	handler := jiraAtlassianCompleteHandler(t)
	handler.CloudID, handler.GraphQLClient = "cloud-301", graphqlClient
	batch, err := handler.Collect(
		context.Background(), claim, providerfoundation.Credential{}, client,
		time.Date(2026, 8, 10, 12, 0, 0, 123456000, time.UTC),
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.WorklogObservations) != 1 {
		t.Fatalf("worklog observations=%+v", batch.WorklogObservations)
	}
	observation := batch.WorklogObservations[0]
	if !observation.GraphQLAttempted || observation.GraphQLSucceeded || !observation.RESTFallbackUsed || observation.GraphQLRequests != 1 || observation.RESTRequests != 1 {
		t.Fatalf("fallback observation=%+v", observation)
	}
}

type jiraAtlassianDoerFunc func(*http.Request) (*http.Response, error)

func (doer jiraAtlassianDoerFunc) Do(request *http.Request) (*http.Response, error) {
	return doer(request)
}
