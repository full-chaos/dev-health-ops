package providersync

import (
	"context"
	"errors"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
	"io"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

type githubWorkItemsRESTReply struct {
	status int
	body   string
	link   string
}

type githubWorkItemsRESTDoer struct {
	t        *testing.T
	replies  map[string][]githubWorkItemsRESTReply
	requests []string
}

func (doer *githubWorkItemsRESTDoer) Do(request *http.Request) (*http.Response, error) {
	doer.t.Helper()
	key := request.URL.Path
	doer.requests = append(doer.requests, key+"?"+request.URL.RawQuery)
	replies := doer.replies[key]
	if len(replies) == 0 {
		doer.t.Fatalf("unexpected request %s", request.URL.String())
	}
	reply := replies[0]
	if len(replies) > 1 {
		doer.replies[key] = replies[1:]
	}
	status := reply.status
	if status == 0 {
		status = http.StatusOK
	}
	header := http.Header{"Content-Type": []string{"application/json"}}
	if status == http.StatusForbidden {
		header.Set("X-RateLimit-Remaining", "0")
	}
	if reply.link != "" {
		header.Set("Link", reply.link)
	}
	return &http.Response{
		StatusCode: status, Header: header,
		Body: io.NopCloser(strings.NewReader(reply.body)), Request: request,
	}, nil
}

func githubWorkItemsRESTClaim() Claim {
	claim := nativeTestClaim("github", "work-items")
	claim.ProcessorFlags = map[string]bool{
		"family_dataset_work_items":         true,
		"family_dataset_work_item_labels":   true,
		"family_dataset_work_item_projects": true,
		"family_dataset_work_item_history":  true,
		"family_dataset_work_item_comments": true,
		"sync_prs":                          true,
	}
	claim.DatasetOptions = map[string]any{
		"include_issues":        true,
		"include_pull_requests": true,
		"fetch_comments":        true,
		"fetch_milestones":      true,
		"comments_limit":        500,
	}
	return claim
}

func githubWorkItemsRESTFixtures() map[string][]githubWorkItemsRESTReply {
	return map[string][]githubWorkItemsRESTReply{
		"/repos/acme/api":            {{body: `{"id":4567,"full_name":"Acme/API-Renamed"}`}},
		"/repos/acme/api/milestones": {{body: `[{"id":77,"number":7,"title":"July","state":"open","created_at":"2026-07-01T00:00:00Z","due_on":"2026-07-31T00:00:00Z"}]`}},
		"/repos/acme/api/issues": {{body: `[
			{"number":42,"title":"Issue in window","body":"blocks CHAOS-42","state":"open","created_at":"2026-07-02T00:00:00Z","updated_at":"2026-07-20T00:00:00Z","user":{"login":"octocat"}},
			{"number":43,"title":"PR stub","updated_at":"2026-07-20T00:00:00Z","pull_request":{"url":"https://api.github.com/repos/acme/api/pulls/43"}},
			{"number":44,"title":"Future issue","state":"open","created_at":"2026-07-02T00:00:00Z","updated_at":"2026-08-01T00:00:00Z"}
		]`}},
		"/repos/acme/api/issues/42/events":   {{body: `[{"event":"closed","created_at":"2026-07-21T00:00:00Z","actor":{"login":"closer"}}]`}},
		"/repos/acme/api/issues/42/comments": {{body: `[{"id":9007199254740993,"body":"hello 👋","created_at":"2026-07-20T12:00:00Z","user":{"login":"reviewer"}}]`}},
		"/repos/acme/api/pulls": {{body: `[
			{"number":53,"updated_at":"2026-08-01T00:00:00Z"},
			{"number":52,"updated_at":"2026-07-22T00:00:00Z"},
			{"number":51,"updated_at":"2026-06-30T00:00:00Z"}
		]`}},
		"/repos/acme/api/pulls/52": {{body: `{
			"number":52,"title":"PR in window","body":"Fixes CHAOS-52","state":"open","draft":false,"merged":false,
			"created_at":"2026-07-10T00:00:00Z","updated_at":"2026-07-22T00:00:00Z",
			"head":{"ref":"feature/CHAOS-52"},"user":{"login":"dev"}
		}`}},
	}
}

func TestGitHubWorkItemsRESTCollectorBuildsIssueRowsAndPullTargets(t *testing.T) {
	t.Parallel()
	doer := &githubWorkItemsRESTDoer{t: t, replies: githubWorkItemsRESTFixtures()}
	now := time.Date(2026, 8, 4, 12, 0, 0, 123456000, time.UTC)
	result, err := (GitHubWorkItemsRESTCollector{}).Collect(
		context.Background(), githubWorkItemsRESTClaim(),
		gitHubPullRequestClient(t, fakehttp.Client(doer), "https://api.github.com"), now,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !result.NoOptionalDegradation() || len(result.Incomplete) != 0 {
		t.Fatalf("result incomplete=%+v", result.Incomplete)
	}
	if result.RepoFullName != "Acme/API-Renamed" || result.RepoID.String() != "616d4a76-b639-d421-808b-3cef6940d4b9" {
		t.Fatalf("repo=%q id=%s", result.RepoFullName, result.RepoID)
	}
	if len(result.Rows.WorkItems) != 1 || result.Rows.WorkItems[0].WorkItemID != "gh:Acme/API-Renamed#42" ||
		len(result.Rows.StatusTransitions) != 1 || len(result.Rows.Interactions) != 1 ||
		len(result.Rows.Sprints) != 1 || len(result.PullRequests) != 1 ||
		result.PullRequests[0].Number != 52 {
		t.Fatalf("rows=%+v pulls=%+v", result.Rows, result.PullRequests)
	}
	if result.Evidence.Requests != 7 || result.Evidence.Pages != 5 ||
		result.Evidence.Records != 3 || result.Evidence.CapReached {
		t.Fatalf("evidence=%+v requests=%v", result.Evidence, doer.requests)
	}
	wantPaths := []string{
		"/repos/acme/api?", "/repos/acme/api/milestones?", "/repos/acme/api/issues?",
		"/repos/acme/api/issues/42/events?", "/repos/acme/api/issues/42/comments?",
		"/repos/acme/api/pulls?", "/repos/acme/api/pulls/52?",
	}
	for index, prefix := range wantPaths {
		if index >= len(doer.requests) || !strings.HasPrefix(doer.requests[index], prefix) {
			t.Fatalf("requests=%v want prefix order=%v", doer.requests, wantPaths)
		}
	}
	if strings.Contains(strings.Join(doer.requests, "\n"), "/pulls/51?") ||
		strings.Contains(strings.Join(doer.requests, "\n"), "/pulls/53?") ||
		strings.Contains(strings.Join(doer.requests, "\n"), "/issues/43/events?") {
		t.Fatalf("out-of-window or PR-stub detail fetched: %v", doer.requests)
	}
	if !strings.Contains(doer.requests[2], "since=2026-07-01T00%3A00%3A00Z") {
		t.Fatalf("issues window query=%s", doer.requests[2])
	}
}

func TestGitHubWorkItemsRESTCollectorHonorsExplicitFalseOptions(t *testing.T) {
	t.Parallel()
	claim := githubWorkItemsRESTClaim()
	claim.DatasetOptions = map[string]any{
		"include_issues": false, "include_pull_requests": false,
		"fetch_comments": false, "fetch_milestones": false,
	}
	doer := &githubWorkItemsRESTDoer{t: t, replies: map[string][]githubWorkItemsRESTReply{
		"/repos/acme/api": {{body: `{"id":4567,"full_name":"Acme/API"}`}},
	}}
	result, err := (GitHubWorkItemsRESTCollector{}).Collect(
		context.Background(), claim, gitHubPullRequestClient(t, fakehttp.Client(doer), "https://api.github.com"), time.Now(),
	)
	if err != nil || !result.NoOptionalDegradation() || len(result.Rows.WorkItems) != 0 ||
		len(result.Rows.Sprints) != 0 || len(result.PullRequests) != 0 ||
		result.Evidence.Requests != 1 || result.Evidence.Pages != 0 || result.Evidence.Records != 0 {
		t.Fatalf("result=%+v error=%v requests=%v", result, err, doer.requests)
	}
}

func TestGitHubWorkItemsRESTCollectorDefaultsMatchActivePythonFlags(t *testing.T) {
	t.Parallel()
	claim := githubWorkItemsRESTClaim()
	claim.DatasetOptions = nil
	claim.ProcessorFlags["sync_prs"] = false
	options, err := githubWorkItemsRESTOptionsForClaim(claim)
	if err != nil || !options.includeIssues || options.includePullRequests ||
		!options.fetchComments || !options.fetchMilestones || options.commentsLimit != 500 {
		t.Fatalf("options=%+v error=%v", options, err)
	}
	claim.ProcessorFlags["sync_prs"] = true
	options, err = githubWorkItemsRESTOptionsForClaim(claim)
	if err != nil || !options.includePullRequests {
		t.Fatalf("sync_prs options=%+v error=%v", options, err)
	}
}

func TestGitHubWorkItemsRESTResultDoesNotClaimCompositeCompletionWithPendingPRSocial(t *testing.T) {
	t.Parallel()
	result := GitHubWorkItemsRESTResult{
		PullRequests: []GitHubWorkItemsRESTPullRequest{{Number: 42}},
		Incomplete:   []GitHubWorkItemsRESTIncomplete{},
	}
	if !result.NoOptionalDegradation() || len(result.PullRequests) != 1 {
		t.Fatalf("result=%+v", result)
	}
	// NoOptionalDegradation reports only the REST optional-enrichment state.
	// A pending PR target still requires GraphQL social collection before any
	// composite route may claim complete or advance a watermark.
}

func TestGitHubWorkItemsRESTCollectorFailsClosedOnAnyPaginationCap(t *testing.T) {
	t.Parallel()
	claim := githubWorkItemsRESTClaim()
	claim.DatasetOptions = map[string]any{
		"include_issues": true, "include_pull_requests": false,
		"fetch_comments": false, "fetch_milestones": false,
	}
	doer := &githubWorkItemsRESTDoer{t: t, replies: map[string][]githubWorkItemsRESTReply{
		"/repos/acme/api": {{body: `{"id":4567,"full_name":"Acme/API"}`}},
		"/repos/acme/api/issues": {{
			body: `[{"number":42,"title":"Issue","state":"open","updated_at":"2026-07-20T00:00:00Z"}]`,
			link: `<https://api.github.com/repos/acme/api/issues?page=2>; rel="next"`,
		}},
	}}
	result, err := (GitHubWorkItemsRESTCollector{MaxPages: 1}).Collect(
		context.Background(), claim, gitHubPullRequestClient(t, fakehttp.Client(doer), "https://api.github.com"), time.Now(),
	)
	if !errors.Is(err, ErrPaginationCapExceeded) || result.Evidence.CapReached == false ||
		len(result.Rows.WorkItems) != 0 {
		t.Fatalf("result=%+v error=%v", result, err)
	}
}

func TestGitHubWorkItemsRESTCollectorFailsClosedOnOptionalAndRequiredChildCaps(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name string
		path string
	}{
		{name: "milestones", path: "/repos/acme/api/milestones"},
		{name: "issue events", path: "/repos/acme/api/issues/42/events"},
		{name: "issue comments", path: "/repos/acme/api/issues/42/comments"},
		{name: "pull list", path: "/repos/acme/api/pulls"},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			fixtures := githubWorkItemsRESTFixtures()
			if test.name == "pull list" {
				fixtures[test.path][0].body = `[{"number":52,"updated_at":"2026-07-22T00:00:00Z"}]`
			}
			fixtures[test.path][0].link = "<https://api.github.com" + test.path + "?page=2>; rel=\"next\""
			result, err := (GitHubWorkItemsRESTCollector{MaxPages: 1}).Collect(
				context.Background(), githubWorkItemsRESTClaim(),
				gitHubPullRequestClient(t, fakehttp.Client(&githubWorkItemsRESTDoer{t: t, replies: fixtures}), "https://api.github.com"),
				time.Now(),
			)
			if !errors.Is(err, ErrPaginationCapExceeded) || !result.Evidence.CapReached {
				t.Fatalf("component=%s result=%+v error=%v", test.name, result, err)
			}
			if test.name == "milestones" && len(result.Incomplete) != 0 {
				t.Fatalf("pagination cap was degraded as optional: %+v", result.Incomplete)
			}
		})
	}
}

func TestGitHubWorkItemsRESTCollectorMarksOptionalDegradationIncomplete(t *testing.T) {
	t.Parallel()
	claim := githubWorkItemsRESTClaim()
	claim.DatasetOptions["include_pull_requests"] = false
	doer := &githubWorkItemsRESTDoer{t: t, replies: map[string][]githubWorkItemsRESTReply{
		"/repos/acme/api":                    {{body: `{"id":4567,"full_name":"Acme/API"}`}},
		"/repos/acme/api/milestones":         {{status: http.StatusInternalServerError, body: `{"message":"down"}`}},
		"/repos/acme/api/issues":             {{body: `[{"number":42,"title":"Issue","state":"open","created_at":"2026-07-02T00:00:00Z","updated_at":"2026-07-20T00:00:00Z"}]`}},
		"/repos/acme/api/issues/42/events":   {{body: `[]`}},
		"/repos/acme/api/issues/42/comments": {{status: http.StatusBadGateway, body: `{"message":"down"}`}},
	}}
	result, err := (GitHubWorkItemsRESTCollector{}).Collect(
		context.Background(), claim, gitHubPullRequestClient(t, fakehttp.Client(doer), "https://api.github.com"), time.Now(),
	)
	if err != nil || result.NoOptionalDegradation() || len(result.Incomplete) != 2 ||
		result.Incomplete[0].Component != "milestones" ||
		result.Incomplete[0].Cause != "transient" ||
		result.Incomplete[1].Component != "issue_comments" ||
		result.Incomplete[1].SubjectID != "42" || result.Incomplete[1].Cause != "transient" ||
		len(result.Rows.WorkItems) != 1 ||
		len(result.Rows.Sprints) != 0 || len(result.Rows.Interactions) != 0 {
		t.Fatalf("result=%+v error=%v", result, err)
	}
	if result.Evidence.Requests != 5 || result.Evidence.Pages != 2 || result.Evidence.Records != 1 {
		t.Fatalf("evidence=%+v", result.Evidence)
	}
}

func TestGitHubWorkItemsRESTCollectorRetainsOptionalRowsBeforeLaterPageFailure(t *testing.T) {
	t.Parallel()
	claim := githubWorkItemsRESTClaim()
	claim.DatasetOptions["include_pull_requests"] = false
	milestonePath := "/repos/acme/api/milestones"
	commentPath := "/repos/acme/api/issues/42/comments"
	doer := &githubWorkItemsRESTDoer{t: t, replies: map[string][]githubWorkItemsRESTReply{
		"/repos/acme/api": {{body: `{"full_name":"Acme/API"}`}},
		milestonePath: {
			{body: `[{"id":77,"number":7,"title":"July","state":"open","created_at":"2026-07-01T00:00:00Z"}]`, link: "<https://api.github.com" + milestonePath + "?page=2>; rel=\"next\""},
			{status: http.StatusBadGateway, body: `{"message":"down"}`},
		},
		"/repos/acme/api/issues":           {{body: `[{"number":42,"title":"Issue","state":"open","created_at":"2026-07-02T00:00:00Z","updated_at":"2026-07-20T00:00:00Z"}]`}},
		"/repos/acme/api/issues/42/events": {{body: `[]`}},
		commentPath: {
			{body: `[{"id":9,"body":"kept","created_at":"2026-07-20T12:00:00Z","user":{"login":"reviewer"}}]`, link: "<https://api.github.com" + commentPath + "?page=2>; rel=\"next\""},
			{status: http.StatusBadGateway, body: `{"message":"down"}`},
		},
	}}
	result, err := (GitHubWorkItemsRESTCollector{}).Collect(
		context.Background(), claim,
		gitHubPullRequestClient(t, fakehttp.Client(doer), "https://api.github.com"), time.Now(),
	)
	if err != nil || result.NoOptionalDegradation() || len(result.Incomplete) != 2 ||
		len(result.Rows.Sprints) != 1 || len(result.Rows.Interactions) != 1 ||
		result.Evidence.Pages != 4 || result.Evidence.Requests != 7 {
		t.Fatalf("result=%+v error=%v requests=%v", result, err, doer.requests)
	}
}

func TestGitHubWorkItemsRESTCollectorPropagatesOptionalRateLimits(t *testing.T) {
	t.Parallel()
	claim := githubWorkItemsRESTClaim()
	claim.DatasetOptions = map[string]any{
		"include_issues": false, "include_pull_requests": false,
		"fetch_comments": false, "fetch_milestones": true,
	}
	doer := &githubWorkItemsRESTDoer{t: t, replies: map[string][]githubWorkItemsRESTReply{
		"/repos/acme/api":            {{body: `{"id":4567,"full_name":"Acme/API"}`}},
		"/repos/acme/api/milestones": {{status: http.StatusForbidden, body: `{"message":"API rate limit exceeded"}`}},
	}}
	result, err := (GitHubWorkItemsRESTCollector{}).Collect(
		context.Background(), claim,
		gitHubPullRequestClient(t, fakehttp.Client(doer), "https://api.github.com"), time.Now(),
	)
	var providerErr *providerfoundation.ProviderError
	if !errors.As(err, &providerErr) || providerErr.Class != providerfoundation.ErrorRateLimited ||
		len(result.Incomplete) != 0 {
		t.Fatalf("result=%+v error=%v", result, err)
	}
}

type githubWorkItemsRESTExpiringLease struct {
	assertions int
	failAt     int
}

func (lease *githubWorkItemsRESTExpiringLease) Assert(context.Context) error {
	lease.assertions++
	if lease.assertions >= lease.failAt {
		return providerfoundation.ErrLeaseLost
	}
	return nil
}

func TestGitHubWorkItemsRESTCollectorRechecksLeaseBeforeOptionalDegradation(t *testing.T) {
	t.Parallel()
	claim := githubWorkItemsRESTClaim()
	claim.DatasetOptions = map[string]any{
		"include_issues": false, "include_pull_requests": false,
		"fetch_comments": false, "fetch_milestones": true,
	}
	doer := &githubWorkItemsRESTDoer{t: t, replies: map[string][]githubWorkItemsRESTReply{
		"/repos/acme/api":            {{body: `{"id":4567,"full_name":"Acme/API"}`}},
		"/repos/acme/api/milestones": {{status: http.StatusBadGateway, body: `{"message":"down"}`}},
	}}
	lease := &githubWorkItemsRESTExpiringLease{failAt: 5}
	client, err := providerfoundation.NewHTTPClient(
		"github", "https://api.github.com", fakehttp.Client(doer),
		func(*http.Request) error { return nil },
		providerfoundation.RetryPolicy{
			MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond,
		},
		lease,
	)
	if err != nil {
		t.Fatal(err)
	}
	result, err := (GitHubWorkItemsRESTCollector{}).Collect(
		context.Background(), claim, client, time.Now(),
	)
	if !errors.Is(err, providerfoundation.ErrLeaseLost) || len(result.Incomplete) != 0 ||
		lease.assertions != 5 {
		t.Fatalf("result=%+v error=%v assertions=%d", result, err, lease.assertions)
	}
}

func TestGitHubWorkItemsRESTCollectorRequiredFailuresReturnErrors(t *testing.T) {
	t.Parallel()
	// The pull-request paths are deliberately absent: provider.py:339-343
	// catches a failed /pulls traversal and continues with the issues it
	// already has, so under D17 they degrade with typed evidence instead of
	// failing the unit. TestGitHubWorkItemsRESTCollectorDegradesOnOptionalPullRequestFailure
	// owns that behaviour, including the cap that stays fatal.
	for _, failedPath := range []string{
		"/repos/acme/api", "/repos/acme/api/issues", "/repos/acme/api/issues/42/events",
	} {
		failedPath := failedPath
		t.Run(strings.TrimPrefix(failedPath, "/"), func(t *testing.T) {
			fixtures := githubWorkItemsRESTFixtures()
			fixtures[failedPath] = []githubWorkItemsRESTReply{{status: http.StatusBadGateway, body: `{"message":"down"}`}}
			claim := githubWorkItemsRESTClaim()
			claim.DatasetOptions["fetch_milestones"] = false
			claim.DatasetOptions["fetch_comments"] = false
			_, err := (GitHubWorkItemsRESTCollector{}).Collect(
				context.Background(), claim,
				gitHubPullRequestClient(t, fakehttp.Client(&githubWorkItemsRESTDoer{t: t, replies: fixtures}), "https://api.github.com"),
				time.Now(),
			)
			var providerErr *providerfoundation.ProviderError
			if !errors.As(err, &providerErr) {
				t.Fatalf("path=%s error=%v want ProviderError", failedPath, err)
			}
		})
	}
}

func TestGitHubWorkItemsRESTCollectorRowsAreRetryStable(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 4, 12, 0, 0, 123456000, time.UTC)
	collect := func() GitHubWorkItemsRESTResult {
		t.Helper()
		result, err := (GitHubWorkItemsRESTCollector{}).Collect(
			context.Background(), githubWorkItemsRESTClaim(),
			gitHubPullRequestClient(t, fakehttp.Client(&githubWorkItemsRESTDoer{t: t, replies: githubWorkItemsRESTFixtures()}), "https://api.github.com"),
			now,
		)
		if err != nil {
			t.Fatal(err)
		}
		return result
	}
	first, second := collect(), collect()
	if !reflect.DeepEqual(first.Rows, second.Rows) || !reflect.DeepEqual(first.PullRequests, second.PullRequests) {
		t.Fatalf("retry drift:\nfirst=%+v\nsecond=%+v", first, second)
	}
}

func TestGitHubWorkItemsRESTCollectorCountsPhysicalRetries(t *testing.T) {
	t.Parallel()
	claim := githubWorkItemsRESTClaim()
	claim.DatasetOptions = map[string]any{
		"include_issues": false, "include_pull_requests": false,
		"fetch_comments": false, "fetch_milestones": true,
	}
	doer := &githubWorkItemsRESTDoer{t: t, replies: map[string][]githubWorkItemsRESTReply{
		"/repos/acme/api": {{body: `{"id":4567,"full_name":"Acme/API"}`}},
		"/repos/acme/api/milestones": {
			{status: http.StatusServiceUnavailable, body: `{"message":"retry"}`},
			{body: `[]`},
		},
	}}
	client := gitHubPullRequestClient(t, fakehttp.Client(doer), "https://api.github.com")
	client.Retry.MaxAttempts = 2
	result, err := (GitHubWorkItemsRESTCollector{}).Collect(context.Background(), claim, client, time.Now())
	if err != nil || !result.NoOptionalDegradation() || result.Evidence.Requests != 3 || result.Evidence.Pages != 1 {
		t.Fatalf("result=%+v error=%v requests=%v", result, err, doer.requests)
	}
}

func TestGitHubWorkItemsRESTCollectorRejectsInvalidBooleanOption(t *testing.T) {
	t.Parallel()
	claim := githubWorkItemsRESTClaim()
	claim.DatasetOptions["fetch_comments"] = "false"
	_, err := (GitHubWorkItemsRESTCollector{}).Collect(
		context.Background(), claim,
		gitHubPullRequestClient(t, fakehttp.Client(&githubWorkItemsRESTDoer{t: t, replies: map[string][]githubWorkItemsRESTReply{}}), "https://api.github.com"),
		time.Now(),
	)
	if !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("error=%v want ErrInvalidConfiguration", err)
	}
}

func TestGitHubWorkItemsRESTPullPayloadNumberMatchesSelectedTarget(t *testing.T) {
	t.Parallel()
	fixtures := githubWorkItemsRESTFixtures()
	fixtures["/repos/acme/api/pulls/52"] = []githubWorkItemsRESTReply{{body: strings.ReplaceAll(
		fixtures["/repos/acme/api/pulls/52"][0].body, `"number":52`, `"number":`+strconv.Itoa(99),
	)}}
	result, err := (GitHubWorkItemsRESTCollector{}).Collect(
		context.Background(), githubWorkItemsRESTClaim(),
		gitHubPullRequestClient(t, fakehttp.Client(&githubWorkItemsRESTDoer{t: t, replies: fixtures}), "https://api.github.com"),
		time.Now(),
	)
	// A pull-request detail whose number does not match the one we selected is
	// still rejected -- it never becomes a row. Under D17 the rejection
	// degrades the pull-request phase instead of failing the unit, so the guard
	// is now observed through the typed evidence rather than a returned error.
	if err != nil {
		t.Fatalf("error=%v", err)
	}
	if len(result.PullRequests) != 0 {
		t.Fatalf("mismatched pull payload became a target: %+v", result.PullRequests)
	}
	if !reflect.DeepEqual(result.Incomplete, []GitHubWorkItemsRESTIncomplete{
		{Component: "pull_requests", Cause: "invalid_response"},
	}) {
		t.Fatalf("incomplete=%+v", result.Incomplete)
	}
}

func TestGitHubWorkItemsRESTCollectorDegradesOnOptionalPullRequestFailure(t *testing.T) {
	t.Parallel()
	t.Run("listing failure keeps the issues already collected", func(t *testing.T) {
		t.Parallel()
		fixtures := githubWorkItemsRESTFixtures()
		fixtures["/repos/acme/api/pulls"] = []githubWorkItemsRESTReply{
			{status: http.StatusBadGateway, body: `{"message":"down"}`},
		}
		claim := githubWorkItemsRESTClaim()
		claim.DatasetOptions["fetch_milestones"] = false
		claim.DatasetOptions["fetch_comments"] = false
		result, err := (GitHubWorkItemsRESTCollector{}).Collect(
			context.Background(), claim,
			gitHubPullRequestClient(t, fakehttp.Client(&githubWorkItemsRESTDoer{t: t, replies: fixtures}), "https://api.github.com"),
			time.Now(),
		)
		if err != nil {
			t.Fatalf("optional /pulls failure blocked the unit: %v", err)
		}
		if len(result.Rows.WorkItems) == 0 {
			t.Fatal("issues collected before the /pulls failure were discarded")
		}
		if len(result.PullRequests) != 0 {
			t.Fatalf("pull requests=%+v", result.PullRequests)
		}
		if !reflect.DeepEqual(result.Incomplete, []GitHubWorkItemsRESTIncomplete{
			{Component: "pull_requests", Cause: "transient"},
		}) {
			t.Fatalf("incomplete=%+v", result.Incomplete)
		}
	})

	t.Run("detail failure retains earlier pull requests", func(t *testing.T) {
		t.Parallel()
		fixtures := githubWorkItemsRESTFixtures()
		fixtures["/repos/acme/api/pulls/52"] = []githubWorkItemsRESTReply{
			{status: http.StatusBadGateway, body: `{"message":"down"}`},
		}
		claim := githubWorkItemsRESTClaim()
		claim.DatasetOptions["fetch_milestones"] = false
		claim.DatasetOptions["fetch_comments"] = false
		result, err := (GitHubWorkItemsRESTCollector{}).Collect(
			context.Background(), claim,
			gitHubPullRequestClient(t, fakehttp.Client(&githubWorkItemsRESTDoer{t: t, replies: fixtures}), "https://api.github.com"),
			time.Now(),
		)
		if err != nil {
			t.Fatalf("optional pull-detail failure blocked the unit: %v", err)
		}
		if len(result.Incomplete) != 1 || result.Incomplete[0].Component != "pull_requests" {
			t.Fatalf("incomplete=%+v", result.Incomplete)
		}
	})

	t.Run("pagination cap stays fatal", func(t *testing.T) {
		t.Parallel()
		fixtures := githubWorkItemsRESTFixtures()
		fixtures["/repos/acme/api/pulls"] = []githubWorkItemsRESTReply{{
			body: `[{"number":52,"updated_at":"2026-07-22T00:00:00Z"}]`,
			link: `<https://api.github.com/repos/acme/api/pulls?page=2>; rel="next"`,
		}}
		claim := githubWorkItemsRESTClaim()
		claim.DatasetOptions["fetch_milestones"] = false
		claim.DatasetOptions["fetch_comments"] = false
		_, err := (GitHubWorkItemsRESTCollector{MaxPages: 1}).Collect(
			context.Background(), claim,
			gitHubPullRequestClient(t, fakehttp.Client(&githubWorkItemsRESTDoer{t: t, replies: fixtures}), "https://api.github.com"),
			time.Now(),
		)
		if !errors.Is(err, ErrPaginationCapExceeded) {
			t.Fatalf("capped /pulls traversal degraded instead of failing: %v", err)
		}
	})
}

// githubWorkItemsRESTEventClaim is a claim that reads issue events only.
func githubWorkItemsRESTEventClaim() Claim {
	claim := githubWorkItemsRESTClaim()
	claim.DatasetOptions = map[string]any{
		"include_issues": true, "include_pull_requests": false,
		"fetch_comments": false, "fetch_milestones": false,
		"comments_limit": 17,
	}
	return claim
}

// githubWorkItemsRESTEventReplies is a list of `total` events in pages of 100
// whose LAST page carries no rel=next, the way GitHub ends a list.
func githubWorkItemsRESTEventReplies(path string, total int) []githubWorkItemsRESTReply {
	pages := (total + 99) / 100
	replies := githubWorkItemsRESTFullPageReplies(pages, 100, path, func(index int) string {
		event := "closed"
		if index%2 == 1 {
			event = "reopened"
		}
		return `{"id":` + strconv.Itoa(index+1) + `,"event":"` + event +
			`","created_at":"2026-07-20T00:00:00Z"}`
	})
	last := total - (pages-1)*100
	if last < 100 {
		items := make([]string, 0, last)
		for offset := 0; offset < last; offset++ {
			index := (pages-1)*100 + offset
			event := "closed"
			if index%2 == 1 {
				event = "reopened"
			}
			items = append(items, `{"id":`+strconv.Itoa(index+1)+`,"event":"`+event+
				`","created_at":"2026-07-20T00:00:00Z"}`)
		}
		replies[pages-1].body = "[" + strings.Join(items, ",") + "]"
	}
	replies[pages-1].link = ""
	return replies
}

func collectGitHubWorkItemEvents(t *testing.T, total int) (GitHubWorkItemsRESTResult, *githubWorkItemsRESTDoer, error) {
	t.Helper()
	doer := &githubWorkItemsRESTDoer{t: t, replies: map[string][]githubWorkItemsRESTReply{
		"/repos/acme/api":                  {{body: `{"id":4567,"full_name":"Acme/API"}`}},
		"/repos/acme/api/issues":           {{body: `[{"number":42,"title":"Issue","state":"open","created_at":"2026-07-02T00:00:00Z","updated_at":"2026-07-20T00:00:00Z"}]`}},
		"/repos/acme/api/issues/42/events": githubWorkItemsRESTEventReplies("/repos/acme/api/issues/42/events", total),
	}}
	result, err := (GitHubWorkItemsRESTCollector{}).Collect(
		context.Background(), githubWorkItemsRESTEventClaim(),
		gitHubPullRequestClient(t, fakehttp.Client(doer), "https://api.github.com"), time.Now(),
	)
	return result, doer, err
}

// CHAOS-8770: events are no longer cut at 1000. 1150 events across 12 pages
// are all read and all reach the rows.
func TestGitHubWorkItemsRESTCollectorPagesEventsPastTheOldThousandCap(t *testing.T) {
	t.Parallel()
	result, doer, err := collectGitHubWorkItemEvents(t, 1150)
	if err != nil || !result.NoOptionalDegradation() || len(result.Rows.WorkItems) != 1 {
		t.Fatalf("result=%+v error=%v requests=%d", result, err, len(doer.requests))
	}
	if got := countGitHubWorkItemsRESTRequests(doer.requests, "/repos/acme/api/issues/42/events?"); got != 12 {
		t.Fatalf("event requests=%d want 12; requests=%v", got, doer.requests)
	}
	if got := len(result.Rows.StatusTransitions); got != 1150 {
		t.Fatalf("transitions=%d want 1150 (old cap 1000)", got)
	}
}

// Literal numbers: 100 pages (the shared page bound) pass, 101 fail closed with
// the repository, the issue and the field named.
func TestGitHubWorkItemsRESTCollectorEventsPageBoundIsExactlyOneHundredPages(t *testing.T) {
	t.Parallel()
	result, doer, err := collectGitHubWorkItemEvents(t, 10_000)
	if err != nil || len(result.Rows.StatusTransitions) != 10_000 ||
		countGitHubWorkItemsRESTRequests(doer.requests, "/repos/acme/api/issues/42/events?") != 100 {
		t.Fatalf("100 pages: err=%v transitions=%d", err, len(result.Rows.StatusTransitions))
	}
	_, _, err = collectGitHubWorkItemEvents(t, 10_001)
	if !errors.Is(err, ErrPaginationCapExceeded) {
		t.Fatalf("101 pages: err=%v want ErrPaginationCapExceeded", err)
	}
	for _, want := range []string{"events of Acme/API#42", "after 100 pages"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %q lacks %q", err, want)
		}
	}
}

func githubWorkItemsRESTCommentCollect(t *testing.T, limit, total int) (GitHubWorkItemsRESTResult, *githubWorkItemsRESTDoer, error) {
	t.Helper()
	claim := githubWorkItemsRESTClaim()
	claim.DatasetOptions = map[string]any{
		"include_issues": true, "include_pull_requests": false,
		"fetch_comments": true, "fetch_milestones": false,
		"comments_limit": limit,
	}
	replies := githubWorkItemsRESTFullPageReplies(
		total/100, 100, "/repos/acme/api/issues/42/comments",
		func(index int) string {
			return `{"id":` + strconv.Itoa(index+1) +
				`,"body":"comment","created_at":"2026-07-20T00:00:00Z","user":{"login":"reviewer"}}`
		},
	)
	replies[len(replies)-1].link = "" // GitHub ends a list without rel=next.
	doer := &githubWorkItemsRESTDoer{t: t, replies: map[string][]githubWorkItemsRESTReply{
		"/repos/acme/api":                    {{body: `{"id":4567,"full_name":"Acme/API"}`}},
		"/repos/acme/api/issues":             {{body: `[{"number":42,"title":"Issue","state":"open","created_at":"2026-07-02T00:00:00Z","updated_at":"2026-07-20T00:00:00Z"}]`}},
		"/repos/acme/api/issues/42/events":   {{body: `[]`}},
		"/repos/acme/api/issues/42/comments": replies,
	}}
	result, err := (GitHubWorkItemsRESTCollector{}).Collect(
		context.Background(), claim,
		gitHubPullRequestClient(t, fakehttp.Client(doer), "https://api.github.com"), time.Now(),
	)
	return result, doer, err
}

// A list that holds exactly the limit is complete: nothing is cut or counted.
func TestGitHubWorkItemsRESTCollectorUsesConfiguredFullPageCommentLimit(t *testing.T) {
	t.Parallel()
	result, doer, err := githubWorkItemsRESTCommentCollect(t, 200, 200)
	if err != nil || !result.NoOptionalDegradation() || len(result.Rows.Interactions) != 200 ||
		result.CommentsTruncated != 0 || result.Evidence.Pages != 4 || result.Evidence.Requests != 5 {
		t.Fatalf("result=%+v error=%v requests=%d", result, err, len(doer.requests))
	}
	if got := countGitHubWorkItemsRESTRequests(doer.requests, "/repos/acme/api/issues/42/comments?"); got != 2 {
		t.Fatalf("comment requests=%d want 2; requests=%v", got, doer.requests)
	}
}

// The user's comments_limit is an explicit cap. When it cuts a list that still
// had rows, the cut is counted and logged, never silent.
func TestGitHubWorkItemsRESTCollectorCountsACommentCutByTheUserLimit(t *testing.T) {
	t.Parallel()
	result, _, err := githubWorkItemsRESTCommentCollect(t, 200, 300)
	if err != nil || len(result.Rows.Interactions) != 200 || result.CommentsTruncated != 1 {
		t.Fatalf("interactions=%d truncated=%d err=%v", len(result.Rows.Interactions), result.CommentsTruncated, err)
	}
}

// Literal numbers: with no user cap in the way, 100 pages of comments pass and
// 101 fail closed naming the repository, the issue and the field.
func TestGitHubWorkItemsRESTCollectorCommentsPageBoundIsExactlyOneHundredPages(t *testing.T) {
	t.Parallel()
	result, _, err := githubWorkItemsRESTCommentCollect(t, 1_000_000, 10_000)
	if err != nil || len(result.Rows.Interactions) != 10_000 || result.CommentsTruncated != 0 {
		t.Fatalf("100 pages: interactions=%d err=%v", len(result.Rows.Interactions), err)
	}
	_, _, err = githubWorkItemsRESTCommentCollect(t, 1_000_000, 10_100)
	if !errors.Is(err, ErrPaginationCapExceeded) {
		t.Fatalf("101 pages: err=%v want ErrPaginationCapExceeded", err)
	}
	for _, want := range []string{"comments of Acme/API#42", "after 100 pages"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %q lacks %q", err, want)
		}
	}
}

func githubWorkItemsRESTFullPageReplies(
	pages int,
	itemsPerPage int,
	path string,
	item func(int) string,
) []githubWorkItemsRESTReply {
	replies := make([]githubWorkItemsRESTReply, 0, pages)
	for page := 0; page < pages; page++ {
		items := make([]string, 0, itemsPerPage)
		for offset := 0; offset < itemsPerPage; offset++ {
			items = append(items, item(page*itemsPerPage+offset))
		}
		replies = append(replies, githubWorkItemsRESTReply{
			body: "[" + strings.Join(items, ",") + "]",
			// Even the final full page advertises another page. An exact item
			// limit must stop before that speculative request.
			link: "<https://api.github.com" + path + "?page=" +
				strconv.Itoa(page+2) + ">; rel=\"next\"",
		})
	}
	return replies
}

func countGitHubWorkItemsRESTRequests(requests []string, prefix string) int {
	count := 0
	for _, request := range requests {
		if strings.HasPrefix(request, prefix) {
			count++
		}
	}
	return count
}
