package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

type gitHubPullRequestReviewFetchDoer struct {
	t             *testing.T
	graphQLBodies []string
	graphQLReply  []string
	graphQLStatus []int
	requests      int
}

func (doer *gitHubPullRequestReviewFetchDoer) Do(request *http.Request) (*http.Response, error) {
	doer.t.Helper()
	doer.requests++
	if request.URL.Path == "/repos/acme/api" {
		return gitHubPullRequestReviewResponse(request, http.StatusOK, gitHubPullRequestRepoFixture), nil
	}
	if request.URL.Path != "/graphql" {
		doer.t.Fatalf("unexpected request %s", request.URL.String())
	}
	body, err := io.ReadAll(request.Body)
	if err != nil {
		doer.t.Fatal(err)
	}
	doer.graphQLBodies = append(doer.graphQLBodies, string(body))
	index := len(doer.graphQLBodies) - 1
	status := http.StatusOK
	if index < len(doer.graphQLStatus) && doer.graphQLStatus[index] != 0 {
		status = doer.graphQLStatus[index]
	}
	reply := `{"data":{"repository":{}}}`
	if index < len(doer.graphQLReply) {
		reply = doer.graphQLReply[index]
	}
	return gitHubPullRequestReviewResponse(request, status, reply), nil
}

func gitHubPullRequestReviewResponse(request *http.Request, status int, body string) *http.Response {
	header := http.Header{"Content-Type": []string{"application/json"}}
	if status == http.StatusForbidden {
		header.Set("X-RateLimit-Remaining", "0")
	}
	return &http.Response{StatusCode: status, Header: header, Body: io.NopCloser(strings.NewReader(body)), Request: request}
}

func gitHubPullRequestReviewFetchTarget() []GitHubPullRequestReviewTarget {
	return []GitHubPullRequestReviewTarget{{
		Number: 42, CreatedAt: time.Date(2026, 7, 10, 9, 0, 0, 0, time.UTC),
	}}
}

const gitHubPullRequestReviewFetchRepoID = "c7198fbc-1945-3717-05d8-eb78866b4e79"

func TestGitHubPullRequestReviewFetcherPaginatesAndReportsBoundedUsage(t *testing.T) {
	doer := &gitHubPullRequestReviewFetchDoer{t: t, graphQLReply: []string{
		`{"data":{"repository":{"pr0":{"number":42,"reviews":{"nodes":[{"databaseId":9007199254740993,"state":"APPROVED","submittedAt":"2026-07-11T10:30:00Z","author":{"login":"octocat"}}],"pageInfo":{"hasNextPage":true,"endCursor":"cursor-1"}}}}}}`,
		`{"data":{"repository":{"pr0":{"number":42,"reviews":{"nodes":[{"id":"R_kwDO","state":"CHANGES_REQUESTED","submittedAt":null,"author":null}],"pageInfo":{"hasNextPage":false,"endCursor":""}}}}}}`,
	}}
	client := gitHubPullRequestClient(t, doer, "https://api.github.com")
	budget := &gitHubPullRequestReviewBudget{}
	gate := &gitHubPullRequestReviewGate{}
	client.Budget = budget
	client.BudgetKey = providerfoundation.BudgetKey{Provider: "github", OrgID: "org-native", Host: "api.github.com", CostClass: "medium", Limit: 1, TTL: time.Minute}
	client.Gate = gate

	now := time.Date(2026, 8, 4, 12, 0, 0, 123456000, time.UTC)
	result, err := (GitHubPullRequestReviewFetcher{MaxPages: 2}).Fetch(
		context.Background(), nativeTestClaim("github", "pr-reviews"), client,
		gitHubPullRequestReviewFetchRepoID, gitHubPullRequestReviewFetchTarget(), now,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Complete() || result.Incomplete != nil {
		t.Fatalf("result=%+v", result)
	}
	if result.Evidence.Requests != 2 || result.Evidence.Pages != 2 || result.Evidence.Records != 2 {
		t.Fatalf("evidence=%+v", result.Evidence)
	}
	if result.Usage != (GitHubPullRequestReviewUsage{Transport: "graphql", RouteFamily: "pr_social", Dimension: BudgetGraphQLCost, RequestCount: 2}) {
		t.Fatalf("usage=%+v", result.Usage)
	}
	if budget.acquires != 2 || budget.releases != 2 || gate.waits != 2 {
		t.Fatalf("budget acquires=%d releases=%d gate waits=%d", budget.acquires, budget.releases, gate.waits)
	}
	queries := make([]string, 0, len(doer.graphQLBodies))
	for _, body := range doer.graphQLBodies {
		var envelope struct {
			Query string `json:"query"`
		}
		if err := json.Unmarshal([]byte(body), &envelope); err != nil {
			t.Fatal(err)
		}
		queries = append(queries, envelope.Query)
	}
	if len(queries) != 2 || !strings.Contains(queries[0], `reviews(first: 100, after: null)`) ||
		!strings.Contains(queries[1], `after: "cursor-1"`) {
		t.Fatalf("graphql bodies=%q", doer.graphQLBodies)
	}
	if result.Rows[0].ReviewID != "9007199254740993" || result.Rows[1].Reviewer != "Unknown" ||
		!result.Rows[1].SubmittedAt.Equal(gitHubPullRequestReviewFetchTarget()[0].CreatedAt) {
		t.Fatalf("rows=%+v", result.Rows)
	}
}

func TestGitHubPullRequestReviewFetcherRateLimitPropagates(t *testing.T) {
	doer := &gitHubPullRequestReviewFetchDoer{t: t, graphQLStatus: []int{http.StatusForbidden}, graphQLReply: []string{`{"message":"API rate limit exceeded"}`}}
	client := gitHubPullRequestClient(t, doer, "https://api.github.com")
	result, err := (GitHubPullRequestReviewFetcher{}).Fetch(
		context.Background(), nativeTestClaim("github", "pr-reviews"), client,
		gitHubPullRequestReviewFetchRepoID, gitHubPullRequestReviewFetchTarget(), time.Now(),
	)
	var providerErr *providerfoundation.ProviderError
	if !errors.As(err, &providerErr) || providerErr.Class != providerfoundation.ErrorRateLimited {
		t.Fatalf("error=%v", err)
	}
	if result.Incomplete != nil || result.Usage.RequestCount != 1 || result.Evidence.Requests != 1 {
		t.Fatalf("rate limit result lost partial usage=%+v", result)
	}
}

func TestGitHubPullRequestReviewFetcherCountsRetriesButReservesOnce(t *testing.T) {
	doer := &gitHubPullRequestReviewFetchDoer{t: t, graphQLStatus: []int{http.StatusServiceUnavailable}, graphQLReply: []string{
		`{"message":"unavailable"}`,
		`{"data":{"repository":{"pr0":{"number":42,"reviews":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":""}}}}}}`,
	}}
	client := gitHubPullRequestClient(t, doer, "https://api.github.com")
	client.Retry.MaxAttempts = 2
	budget := &gitHubPullRequestReviewBudget{}
	client.Budget = budget
	client.BudgetKey = providerfoundation.BudgetKey{Provider: "github", OrgID: "org-native", Host: "api.github.com", CostClass: "medium", Limit: 1, TTL: time.Minute}

	result, err := (GitHubPullRequestReviewFetcher{}).Fetch(
		context.Background(), nativeTestClaim("github", "pr-reviews"), client,
		gitHubPullRequestReviewFetchRepoID, gitHubPullRequestReviewFetchTarget(), time.Now(),
	)
	if err != nil || !result.Complete() {
		t.Fatalf("result=%+v error=%v", result, err)
	}
	// The logical GraphQL POST acquires one budget reservation. HTTPClient
	// retries that POST within the reservation, while usage records both actual
	// GraphQL wire attempts.
	if budget.acquires != 1 || budget.releases != 1 || result.Evidence.Requests != 2 ||
		result.Usage.RequestCount != 2 || result.Evidence.Pages != 1 {
		t.Fatalf("budget=%+v evidence=%+v usage=%+v", budget, result.Evidence, result.Usage)
	}
}

func TestGitHubPullRequestReviewFetcherMarksNonRateFailuresIncomplete(t *testing.T) {
	for _, test := range []struct {
		name   string
		status int
		body   string
		cause  string
	}{
		{name: "transient HTTP", status: http.StatusServiceUnavailable, body: `{"message":"unavailable"}`, cause: "transient"},
		{name: "graphql errors", body: `{"data":{"repository":{}},"errors":[{"message":"schema changed"}]}`, cause: "invalid_response"},
	} {
		t.Run(test.name, func(t *testing.T) {
			doer := &gitHubPullRequestReviewFetchDoer{t: t, graphQLStatus: []int{test.status}, graphQLReply: []string{test.body}}
			result, err := (GitHubPullRequestReviewFetcher{}).Fetch(
				context.Background(), nativeTestClaim("github", "pr-reviews"),
				gitHubPullRequestClient(t, doer, "https://api.github.com"),
				gitHubPullRequestReviewFetchRepoID, gitHubPullRequestReviewFetchTarget(), time.Now(),
			)
			if err != nil || result.Complete() || result.Incomplete == nil || result.Incomplete.Cause != test.cause ||
				len(result.Rows) != 0 || result.Evidence.Records != 0 {
				t.Fatalf("result=%+v error=%v", result, err)
			}
		})
	}
}

func TestGitHubPullRequestReviewFetcherReturnsTypedPaginationIncomplete(t *testing.T) {
	doer := &gitHubPullRequestReviewFetchDoer{t: t, graphQLReply: []string{
		`{"data":{"repository":{"pr0":{"number":42,"reviews":{"nodes":[],"pageInfo":{"hasNextPage":true,"endCursor":"cursor-1"}}}}}}`,
	}}
	result, err := (GitHubPullRequestReviewFetcher{MaxPages: 1}).Fetch(
		context.Background(), nativeTestClaim("github", "pr-reviews"),
		gitHubPullRequestClient(t, doer, "https://api.github.com"),
		gitHubPullRequestReviewFetchRepoID, gitHubPullRequestReviewFetchTarget(), time.Now(),
	)
	if err != nil || result.Incomplete == nil || result.Incomplete.Cause != "pagination_cap" || result.Complete() {
		t.Fatalf("result=%+v error=%v", result, err)
	}
}

func TestGitHubPullRequestReviewFetcherChecksLeaseBeforeRequest(t *testing.T) {
	doer := &gitHubPullRequestReviewFetchDoer{t: t}
	client, err := providerfoundation.NewHTTPClient("github", "https://api.github.com", doer,
		func(*http.Request) error { return nil }, providerfoundation.RetryPolicy{MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond},
		providerfoundation.LeaseGuardFunc(func(context.Context) error { return providerfoundation.ErrLeaseLost }),
	)
	if err != nil {
		t.Fatal(err)
	}
	result, err := (GitHubPullRequestReviewFetcher{}).Fetch(
		context.Background(), nativeTestClaim("github", "pr-reviews"), client,
		gitHubPullRequestReviewFetchRepoID, gitHubPullRequestReviewFetchTarget(), time.Now(),
	)
	if !errors.Is(err, providerfoundation.ErrLeaseLost) || doer.requests != 0 || result.Incomplete != nil {
		t.Fatalf("result=%+v error=%v requests=%d", result, err, doer.requests)
	}
}

type gitHubPullRequestReviewBudget struct{ acquires, releases int }

func (budget *gitHubPullRequestReviewBudget) Acquire(_ context.Context, _ providerfoundation.BudgetKey) (providerfoundation.Reservation, error) {
	budget.acquires++
	return gitHubPullRequestReviewReservation{budget: budget}, nil
}

type gitHubPullRequestReviewReservation struct {
	budget *gitHubPullRequestReviewBudget
}

func (reservation gitHubPullRequestReviewReservation) Release(context.Context) error {
	reservation.budget.releases++
	return nil
}

type gitHubPullRequestReviewGate struct{ waits int }

func (gate *gitHubPullRequestReviewGate) Wait(context.Context) (time.Duration, error) {
	gate.waits++
	return 0, nil
}
func (gate *gitHubPullRequestReviewGate) Penalize(context.Context, time.Duration) error { return nil }
