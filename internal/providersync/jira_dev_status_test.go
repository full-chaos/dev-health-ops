package providersync

import (
	"context"
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// TestJiraDevStatusPullRequestSourceIDParsesTrustedGitHubURLOnly is red on
// origin/main -- jiraDevStatusPullRequestSourceID does not exist there.
func TestJiraDevStatusPullRequestSourceIDParsesTrustedGitHubURLOnly(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		url  string
		want string
	}{
		{"trusted pull URL", "https://github.com/acme/api/pull/968", "ghpr:acme/api#968"},
		{"untrusted host", "https://ghe.internal.example.com/acme/api/pull/968", ""},
		{"not a pull URL", "https://github.com/acme/api/issues/968", ""},
		{"malformed URL", "://not a url", ""},
		{"userinfo present", "https://user:pass@github.com/acme/api/pull/968", ""},
	}
	for _, testCase := range cases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			if got := jiraDevStatusPullRequestSourceID(testCase.url); got != testCase.want {
				t.Fatalf("jiraDevStatusPullRequestSourceID(%q)=%q want=%q", testCase.url, got, testCase.want)
			}
		})
	}
}

func TestExtractJiraDevStatusDependenciesEmitsDedupedPrimaryEdges(t *testing.T) {
	t.Parallel()
	claim := nativeTestClaim("jira", "work-items")
	normalizedAt := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	workItemID := "jira:OPS-101"
	payload := jiraDevStatusPayload{
		Detail: []struct {
			PullRequests []struct {
				URL string `json:"url"`
			} `json:"pullRequests"`
		}{
			{PullRequests: []struct {
				URL string `json:"url"`
			}{
				{URL: "https://github.com/acme/api/pull/968"},
				// Duplicate must not produce a second row.
				{URL: "https://github.com/acme/api/pull/968"},
				// Untrusted/non-PR URL must be skipped, not error.
				{URL: "https://example.com/not-a-pr"},
			}},
		},
	}
	rows := extractJiraDevStatusDependencies(claim, workItemID, payload, normalizedAt)
	got := make([]string, 0, len(rows))
	for _, row := range rows {
		got = append(got, row.SourceWorkItemID+"|"+row.TargetWorkItemID+"|"+row.RelationshipType+"|"+row.RelationshipTypeRaw)
		if row.OrgID != claim.OrgID || !row.LastSynced.Equal(normalizedAt) ||
			row.RelationshipSemanticsVersion != "canonical-blocks.v2" {
			t.Fatalf("row=%+v", row)
		}
	}
	want := []string{"ghpr:acme/api#968|jira:OPS-101|relates_to|jira_dev_status"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("rows=%v want=%v", got, want)
	}
}

type jiraDevStatusDoer struct {
	t        *testing.T
	status   int
	body     string
	requests int
	// statuses, when non-empty, overrides status with a per-call sequence
	// (the last entry repeats past its length) -- used to simulate
	// HTTPClient.Do's internal retries against a transient status.
	statuses []int
}

func (doer *jiraDevStatusDoer) Do(request *http.Request) (*http.Response, error) {
	doer.t.Helper()
	index := doer.requests
	doer.requests++
	if request.URL.Path != "/rest/dev-status/1.0/issue/detail" {
		doer.t.Fatalf("unexpected path %s", request.URL.Path)
	}
	if got := request.URL.Query().Get("applicationType"); got != "GitHub" {
		doer.t.Fatalf("applicationType=%q", got)
	}
	if got := request.URL.Query().Get("dataType"); got != "pullrequest" {
		doer.t.Fatalf("dataType=%q", got)
	}
	status := doer.status
	if len(doer.statuses) > 0 {
		if index < len(doer.statuses) {
			status = doer.statuses[index]
		} else {
			status = doer.statuses[len(doer.statuses)-1]
		}
	}
	return &http.Response{
		StatusCode: status,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(doer.body)),
		Request:    request,
	}, nil
}

func jiraDevStatusTestClient(t *testing.T, doer providerfoundation.HTTPDoer) *providerfoundation.HTTPClient {
	t.Helper()
	return jiraDevStatusTestClientWithRetries(t, doer, 1)
}

func jiraDevStatusTestClientWithRetries(t *testing.T, doer providerfoundation.HTTPDoer, maxAttempts int) *providerfoundation.HTTPClient {
	t.Helper()
	client, err := providerfoundation.NewHTTPClient(
		"jira", "https://acme.atlassian.net", doer,
		func(request *http.Request) error { return nil },
		providerfoundation.RetryPolicy{MaxAttempts: maxAttempts, InitialWait: time.Millisecond, MaxWait: time.Millisecond},
		providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
	)
	if err != nil {
		t.Fatal(err)
	}
	return client
}

// TestFetchJiraDevStatusPullRequestsCountingAttemptsCountsRetries is the
// red-first test for codex round 1's P2 finding: HTTPClient.Do retries a
// transient (5xx) failure internally, invisible to a caller that only
// increments its budget cap once per logical call -- the counting Doer must
// observe every actual wire attempt, not just one per fetchJiraDevStatusPullRequests
// call.
func TestFetchJiraDevStatusPullRequestsCountingAttemptsCountsRetries(t *testing.T) {
	t.Parallel()
	doer := &jiraDevStatusDoer{
		t: t, statuses: []int{http.StatusServiceUnavailable, http.StatusServiceUnavailable, http.StatusServiceUnavailable},
		body: `{"errorMessages":["temporarily unavailable"]}`,
	}
	client := jiraDevStatusTestClientWithRetries(t, doer, 3)
	_, available, attempts, err := fetchJiraDevStatusPullRequestsCountingAttempts(
		context.Background(), client, "10050", 0,
	)
	if err == nil || available {
		t.Fatalf("expected a genuine error after exhausting retries, available=%v err=%v", available, err)
	}
	if attempts != 3 {
		t.Fatalf("attempts=%d want=3 (RetryPolicy.MaxAttempts, all consumed by transient 503s)", attempts)
	}
	if doer.requests != 3 {
		t.Fatalf("doer observed %d real wire requests, want=3", doer.requests)
	}
}

// TestFetchJiraDevStatusPullRequestsCountingAttemptsHonorsRemainingBudget is
// the red-first test for codex round 2's P2 finding: counting attempts AFTER
// the fact still let one issue's retries alone exceed
// dev_status_max_requests (a budget of 1 permitted 3 real requests under
// sustained 503s, since nothing capped HTTPClient.Do's own retry policy).
// remainingBudget must cap the retry policy itself, not just the count.
func TestFetchJiraDevStatusPullRequestsCountingAttemptsHonorsRemainingBudget(t *testing.T) {
	t.Parallel()
	doer := &jiraDevStatusDoer{
		t: t, statuses: []int{http.StatusServiceUnavailable, http.StatusServiceUnavailable, http.StatusServiceUnavailable},
		body: `{"errorMessages":["temporarily unavailable"]}`,
	}
	// Client policy allows up to 3 attempts, but only 1 remains in the budget.
	client := jiraDevStatusTestClientWithRetries(t, doer, 3)
	_, available, attempts, err := fetchJiraDevStatusPullRequestsCountingAttempts(
		context.Background(), client, "10050", 1,
	)
	if err == nil || available {
		t.Fatalf("expected a genuine error, available=%v err=%v", available, err)
	}
	if attempts != 1 {
		t.Fatalf("attempts=%d want=1 (remainingBudget must cap the retry policy, not just count after the fact)", attempts)
	}
	if doer.requests != 1 {
		t.Fatalf("doer observed %d real wire requests, want=1", doer.requests)
	}
}

// TestFetchJiraDevStatusPullRequestsCountingAttemptsCountsExactlyOneOnSuccess
// is the companion positive case: no retries needed, attempts must be 1, not
// over- or under-counted.
func TestFetchJiraDevStatusPullRequestsCountingAttemptsCountsExactlyOneOnSuccess(t *testing.T) {
	t.Parallel()
	doer := &jiraDevStatusDoer{
		t: t, status: http.StatusOK,
		body: `{"detail":[{"pullRequests":[{"url":"https://github.com/acme/api/pull/968"}]}]}`,
	}
	client := jiraDevStatusTestClientWithRetries(t, doer, 3)
	payload, available, attempts, err := fetchJiraDevStatusPullRequestsCountingAttempts(
		context.Background(), client, "10050", 0,
	)
	if err != nil || !available {
		t.Fatalf("available=%v err=%v", available, err)
	}
	if attempts != 1 {
		t.Fatalf("attempts=%d want=1", attempts)
	}
	if len(payload.Detail) != 1 {
		t.Fatalf("payload=%+v", payload)
	}
}

func TestFetchJiraDevStatusPullRequestsParsesOKResponse(t *testing.T) {
	t.Parallel()
	doer := &jiraDevStatusDoer{
		t: t, status: http.StatusOK,
		body: `{"detail":[{"pullRequests":[{"url":"https://github.com/acme/api/pull/968"}]}]}`,
	}
	payload, available, err := fetchJiraDevStatusPullRequests(
		context.Background(), jiraDevStatusTestClient(t, doer), "10050",
	)
	if err != nil || !available {
		t.Fatalf("available=%v err=%v", available, err)
	}
	if len(payload.Detail) != 1 || len(payload.Detail[0].PullRequests) != 1 ||
		payload.Detail[0].PullRequests[0].URL != "https://github.com/acme/api/pull/968" {
		t.Fatalf("payload=%+v", payload)
	}
}

// TestFetchJiraDevStatusPullRequestsTreats400And404AsCleanNoOp is the
// red-first test for the ruling (chris via team-lead, 2026-09-01): a org
// with no GitHub-for-Jira app configured must be a typed no-op, never an
// error.
func TestFetchJiraDevStatusPullRequestsTreats400And404AsCleanNoOp(t *testing.T) {
	t.Parallel()
	for _, status := range []int{http.StatusBadRequest, http.StatusNotFound} {
		status := status
		t.Run(http.StatusText(status), func(t *testing.T) {
			t.Parallel()
			doer := &jiraDevStatusDoer{t: t, status: status, body: `{"errorMessages":["no dev-status data"]}`}
			payload, available, err := fetchJiraDevStatusPullRequests(
				context.Background(), jiraDevStatusTestClient(t, doer), "10050",
			)
			if err != nil {
				t.Fatalf("expected a clean no-op, got err=%v", err)
			}
			if available {
				t.Fatalf("expected available=false, payload=%+v", payload)
			}
		})
	}
}

func TestFetchJiraDevStatusPullRequestsFailsOnUnexpectedStatus(t *testing.T) {
	t.Parallel()
	doer := &jiraDevStatusDoer{t: t, status: http.StatusInternalServerError, body: `{}`}
	_, available, err := fetchJiraDevStatusPullRequests(
		context.Background(), jiraDevStatusTestClient(t, doer), "10050",
	)
	if err == nil || available {
		t.Fatalf("expected a genuine error, available=%v err=%v", available, err)
	}
}
