package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/google/uuid"
)

type gitHubWorkItemPRSocialFetchDoer struct {
	t        *testing.T
	replies  []string
	status   []int
	bodies   []string
	requests int
}

func (doer *gitHubWorkItemPRSocialFetchDoer) Do(request *http.Request) (*http.Response, error) {
	doer.t.Helper()
	doer.requests++
	if request.URL.Path != "/graphql" {
		doer.t.Fatalf("unexpected request %s", request.URL.String())
	}
	body, err := io.ReadAll(request.Body)
	if err != nil {
		doer.t.Fatal(err)
	}
	doer.bodies = append(doer.bodies, string(body))
	index := len(doer.bodies) - 1
	status := http.StatusOK
	if index < len(doer.status) && doer.status[index] != 0 {
		status = doer.status[index]
	}
	reply := `{"data":{"repository":{}}}`
	if index < len(doer.replies) {
		reply = doer.replies[index]
	}
	header := http.Header{"Content-Type": []string{"application/json"}}
	if status == http.StatusForbidden {
		header.Set("X-RateLimit-Remaining", "0")
	}
	return &http.Response{
		StatusCode: status,
		Header:     header,
		Body:       io.NopCloser(strings.NewReader(reply)),
		Request:    request,
	}, nil
}

func gitHubWorkItemPRSocialClaim() Claim {
	return nativeTestClaim("github", "work-items")
}

var githubWorkItemPRSocialRepoID = uuid.MustParse("c7198fbc-1945-3717-05d8-eb78866b4e79")

func gitHubWorkItemPRSocialQueryFromBody(t *testing.T, body string) string {
	t.Helper()
	var envelope struct {
		Query string `json:"query"`
	}
	if err := json.Unmarshal([]byte(body), &envelope); err != nil {
		t.Fatal(err)
	}
	return envelope.Query
}

func gitHubWorkItemPRSocialConnectionJSON(nodes string, hasNext bool, cursor *string) string {
	endCursor := "null"
	if cursor != nil {
		endCursor = strconv.Quote(*cursor)
	}
	return `{"nodes":` + nodes + `,"pageInfo":{"hasNextPage":` + strconv.FormatBool(hasNext) + `,"endCursor":` + endCursor + `}}`
}

func TestGitHubWorkItemPRSocialFetcherBatchesFiftyAndReportsActualUsage(t *testing.T) {
	firstAliases := make([]string, 50)
	for index := range firstAliases {
		number := index + 1
		firstAliases[index] = `"pr` + strconv.Itoa(index) + `":{"number":` + strconv.Itoa(number) + `,"comments":` +
			gitHubWorkItemPRSocialConnectionJSON(`[{"id":"c`+strconv.Itoa(number)+`","body":"comment"}]`, false, nil) +
			`,"timelineItems":` + gitHubWorkItemPRSocialConnectionJSON(`[{"__typename":"ClosedEvent","createdAt":"2026-08-01T00:00:00Z"}]`, false, nil) + `}`
	}
	doer := &gitHubWorkItemPRSocialFetchDoer{t: t, replies: []string{
		`{"data":{"repository":{` + strings.Join(firstAliases, ",") + `}}}`,
		`{"data":{"repository":{"pr0":{"number":51,"comments":` + gitHubWorkItemPRSocialConnectionJSON(`[]`, false, nil) +
			`,"timelineItems":` + gitHubWorkItemPRSocialConnectionJSON(`[]`, false, nil) + `}}}}`,
	}}
	client := gitHubPullRequestClient(t, doer, "https://api.github.com")
	budget := &gitHubPullRequestReviewBudget{}
	gate := &gitHubPullRequestReviewGate{}
	client.Budget = budget
	client.BudgetKey = providerfoundation.BudgetKey{
		Provider: "github", OrgID: "org-native", Host: "api.github.com",
		CostClass: "medium", Limit: 1, TTL: time.Minute,
	}
	client.Gate = gate
	targets := make([]int, 51)
	for index := range targets {
		targets[index] = index + 1
	}

	result, err := (GitHubWorkItemPRSocialFetcher{}).Fetch(
		context.Background(), gitHubWorkItemPRSocialClaim(), client, targets, 500, 1000,
	)
	if err != nil || !result.Complete() {
		t.Fatalf("result=%+v error=%v", result, err)
	}
	if len(result.Payloads) != 51 || len(result.Payloads[1].Comments) != 1 || len(result.Payloads[1].Events) != 1 {
		t.Fatalf("payloads=%+v", result.Payloads)
	}
	if result.Evidence != (FetchEvidence{Provider: "github", Dataset: "work-items-pr-social", Requests: 2, Pages: 2, Records: 100}) {
		t.Fatalf("evidence=%+v", result.Evidence)
	}
	if result.Usage != (GitHubWorkItemPRSocialUsage{Transport: "graphql", RouteFamily: "work_item_prs", Dimension: BudgetGraphQLCost, RequestCount: 2}) {
		t.Fatalf("usage=%+v", result.Usage)
	}
	usageWasReserved := false
	for _, estimate := range ProviderRequestPlan(
		"github", "work-items", 1, map[string]bool{"sync_prs": true},
	) {
		if estimate.RouteFamily == result.Usage.RouteFamily && estimate.Dimension == result.Usage.Dimension {
			usageWasReserved = true
			break
		}
	}
	if !usageWasReserved {
		t.Fatalf("usage=%+v has no matching request-plan reservation", result.Usage)
	}
	if budget.acquires != 2 || budget.releases != 2 || gate.waits != 2 || doer.requests != 2 {
		t.Fatalf("budget=%+v gate=%+v requests=%d", budget, gate, doer.requests)
	}
	firstQuery := gitHubWorkItemPRSocialQueryFromBody(t, doer.bodies[0])
	if strings.Count(firstQuery, "pullRequest(number:") != 50 || strings.Contains(firstQuery, "reviews(") ||
		!strings.Contains(firstQuery, "comments(first: 100, after: null") ||
		!strings.Contains(firstQuery, "timelineItems(itemTypes: [MERGED_EVENT, CLOSED_EVENT, REOPENED_EVENT], first: 100, after: null") {
		t.Fatalf("unexpected initial query: %s", firstQuery)
	}
}

func TestGitHubWorkItemPRSocialFetcherDrainsCommentAndEventCursorsIndependently(t *testing.T) {
	commentsCursor := "comments-1"
	eventsCursor := "events-1"
	doer := &gitHubWorkItemPRSocialFetchDoer{t: t, replies: []string{
		`{"data":{"repository":{"pr0":{"number":42,"comments":` + gitHubWorkItemPRSocialConnectionJSON(`[{"id":"c1"}]`, true, &commentsCursor) +
			`,"timelineItems":` + gitHubWorkItemPRSocialConnectionJSON(`[{"__typename":"ClosedEvent"}]`, true, &eventsCursor) + `}}}}`,
		`{"data":{"repository":{"pr0":{"number":42,"comments":` + gitHubWorkItemPRSocialConnectionJSON(`[{"id":"c2"}]`, false, nil) + `}}}}`,
		`{"data":{"repository":{"pr0":{"number":42,"timelineItems":` + gitHubWorkItemPRSocialConnectionJSON(`[{"__typename":"ReopenedEvent"}]`, false, nil) + `}}}}`,
	}}
	result, err := (GitHubWorkItemPRSocialFetcher{MaxRequests: 3}).Fetch(
		context.Background(), gitHubWorkItemPRSocialClaim(),
		gitHubPullRequestClient(t, doer, "https://api.github.com"), []int{42}, 500, 1000,
	)
	if err != nil || !result.Complete() {
		t.Fatalf("result=%+v error=%v", result, err)
	}
	if len(result.Payloads[42].Comments) != 2 || len(result.Payloads[42].Events) != 2 ||
		result.Evidence.Pages != 3 || result.Evidence.Requests != 3 {
		t.Fatalf("result=%+v", result)
	}
	commentQuery := gitHubWorkItemPRSocialQueryFromBody(t, doer.bodies[1])
	eventQuery := gitHubWorkItemPRSocialQueryFromBody(t, doer.bodies[2])
	if !strings.Contains(commentQuery, `comments(first: 100, after: "comments-1"`) || strings.Contains(commentQuery, "timelineItems") {
		t.Fatalf("comment continuation query=%s", commentQuery)
	}
	if !strings.Contains(eventQuery, `timelineItems(itemTypes: [MERGED_EVENT, CLOSED_EVENT, REOPENED_EVENT], first: 100, after: "events-1"`) || strings.Contains(eventQuery, "comments(") {
		t.Fatalf("event continuation query=%s", eventQuery)
	}
}

func TestGitHubWorkItemPRSocialFetcherHonorsDeclaredLimitsWithoutOverfetch(t *testing.T) {
	commentsCursor := "more-comments"
	eventsCursor := "more-events"
	doer := &gitHubWorkItemPRSocialFetchDoer{t: t, replies: []string{
		`{"data":{"repository":{"pr0":{"number":42,"comments":` + gitHubWorkItemPRSocialConnectionJSON(`[{"id":"c1"},{"id":"c2"},{"id":"must-not-escape-limit"}]`, true, &commentsCursor) +
			`,"timelineItems":` + gitHubWorkItemPRSocialConnectionJSON(`[{"__typename":"ClosedEvent"},{"__typename":"MustNotEscapeLimit"}]`, true, &eventsCursor) + `}}}}`,
	}}
	result, err := (GitHubWorkItemPRSocialFetcher{}).Fetch(
		context.Background(), gitHubWorkItemPRSocialClaim(),
		gitHubPullRequestClient(t, doer, "https://api.github.com"), []int{42}, 2, 1,
	)
	if err != nil || !result.Complete() || doer.requests != 1 ||
		len(result.Payloads[42].Comments) != 2 || len(result.Payloads[42].Events) != 1 {
		t.Fatalf("result=%+v error=%v requests=%d", result, err, doer.requests)
	}
	query := gitHubWorkItemPRSocialQueryFromBody(t, doer.bodies[0])
	if !strings.Contains(query, "comments(first: 2") || !strings.Contains(query, "first: 1, after: null") {
		t.Fatalf("query=%s", query)
	}
}

func TestGitHubWorkItemPRSocialFetcherRejectsMissingAndStalledCursors(t *testing.T) {
	cursor := "same"
	for _, test := range []struct {
		name          string
		replies       []string
		commentsLimit int
		eventsLimit   int
	}{
		{
			name: "missing comment cursor", commentsLimit: 500,
			replies: []string{`{"data":{"repository":{"pr0":{"number":42,"comments":` +
				gitHubWorkItemPRSocialConnectionJSON(`[]`, true, nil) + `}}}}`},
		},
		{
			name: "stalled comment cursor", commentsLimit: 500,
			replies: []string{
				`{"data":{"repository":{"pr0":{"number":42,"comments":` + gitHubWorkItemPRSocialConnectionJSON(`[]`, true, &cursor) + `}}}}`,
				`{"data":{"repository":{"pr0":{"number":42,"comments":` + gitHubWorkItemPRSocialConnectionJSON(`[]`, true, &cursor) + `}}}}`,
			},
		},
		{
			name: "missing event cursor", eventsLimit: 1000,
			replies: []string{`{"data":{"repository":{"pr0":{"number":42,"timelineItems":` +
				gitHubWorkItemPRSocialConnectionJSON(`[]`, true, nil) + `}}}}`},
		},
		{
			name: "stalled event cursor", eventsLimit: 1000,
			replies: []string{
				`{"data":{"repository":{"pr0":{"number":42,"timelineItems":` + gitHubWorkItemPRSocialConnectionJSON(`[]`, true, &cursor) + `}}}}`,
				`{"data":{"repository":{"pr0":{"number":42,"timelineItems":` + gitHubWorkItemPRSocialConnectionJSON(`[]`, true, &cursor) + `}}}}`,
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			doer := &gitHubWorkItemPRSocialFetchDoer{t: t, replies: test.replies}
			result, err := (GitHubWorkItemPRSocialFetcher{}).Fetch(
				context.Background(), gitHubWorkItemPRSocialClaim(),
				gitHubPullRequestClient(t, doer, "https://api.github.com"), []int{42}, test.commentsLimit, test.eventsLimit,
			)
			if err != nil || result.Complete() || result.Incomplete == nil || result.Incomplete.Cause != "invalid_pagination" || result.Payloads != nil {
				t.Fatalf("result=%+v error=%v", result, err)
			}
		})
	}
}

func TestGitHubWorkItemPRSocialFetcherReturnsTypedGraphQLAndCapIncomplete(t *testing.T) {
	cursor := "next"
	for _, test := range []struct {
		name      string
		replies   []string
		max       int
		wantCause string
	}{
		{
			name: "graphql errors", max: 10, wantCause: "invalid_response",
			replies: []string{`{"data":{"repository":{"pr0":{"number":42,"comments":` + gitHubWorkItemPRSocialConnectionJSON(`[]`, false, nil) + `}}},"errors":[{"message":"schema changed"}]}`},
		},
		{
			name: "request cap", max: 1, wantCause: "pagination_cap",
			replies: []string{`{"data":{"repository":{"pr0":{"number":42,"comments":` + gitHubWorkItemPRSocialConnectionJSON(`[]`, true, &cursor) + `}}}}`},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			doer := &gitHubWorkItemPRSocialFetchDoer{t: t, replies: test.replies}
			result, err := (GitHubWorkItemPRSocialFetcher{MaxRequests: test.max}).Fetch(
				context.Background(), gitHubWorkItemPRSocialClaim(),
				gitHubPullRequestClient(t, doer, "https://api.github.com"), []int{42}, 500, 0,
			)
			if err != nil || result.Complete() || result.Incomplete == nil || result.Incomplete.Cause != test.wantCause || result.Payloads != nil {
				t.Fatalf("result=%+v error=%v", result, err)
			}
			if result.Evidence.Requests != 1 || result.Usage.RequestCount != 1 || doer.requests != 1 {
				t.Fatalf("usage lost on incomplete: result=%+v requests=%d", result, doer.requests)
			}
		})
	}
}

func TestGitHubWorkItemPRSocialFetcherPropagatesRateLimitCancelAndLeaseLoss(t *testing.T) {
	t.Run("rate limit", func(t *testing.T) {
		doer := &gitHubWorkItemPRSocialFetchDoer{t: t, status: []int{http.StatusForbidden}, replies: []string{`{"message":"rate limited"}`}}
		result, err := (GitHubWorkItemPRSocialFetcher{}).Fetch(
			context.Background(), gitHubWorkItemPRSocialClaim(),
			gitHubPullRequestClient(t, doer, "https://api.github.com"), []int{42}, 500, 1000,
		)
		var providerErr *providerfoundation.ProviderError
		if !errors.As(err, &providerErr) || providerErr.Class != providerfoundation.ErrorRateLimited ||
			result.Incomplete != nil || result.Evidence.Requests != 1 || result.Usage.RequestCount != 1 {
			t.Fatalf("result=%+v error=%v", result, err)
		}
	})

	t.Run("cancel", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		doer := &gitHubWorkItemPRSocialFetchDoer{t: t}
		result, err := (GitHubWorkItemPRSocialFetcher{}).Fetch(
			ctx, gitHubWorkItemPRSocialClaim(),
			gitHubPullRequestClient(t, doer, "https://api.github.com"), []int{42}, 500, 1000,
		)
		if !errors.Is(err, context.Canceled) || result.Incomplete != nil || doer.requests != 0 {
			t.Fatalf("result=%+v error=%v requests=%d", result, err, doer.requests)
		}
	})

	t.Run("lease loss", func(t *testing.T) {
		doer := &gitHubWorkItemPRSocialFetchDoer{t: t}
		client, err := providerfoundation.NewHTTPClient(
			"github", "https://api.github.com", doer, func(*http.Request) error { return nil },
			providerfoundation.RetryPolicy{MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond},
			providerfoundation.LeaseGuardFunc(func(context.Context) error { return providerfoundation.ErrLeaseLost }),
		)
		if err != nil {
			t.Fatal(err)
		}
		result, err := (GitHubWorkItemPRSocialFetcher{}).Fetch(
			context.Background(), gitHubWorkItemPRSocialClaim(), client, []int{42}, 500, 1000,
		)
		if !errors.Is(err, providerfoundation.ErrLeaseLost) || result.Incomplete != nil || doer.requests != 0 {
			t.Fatalf("result=%+v error=%v requests=%d", result, err, doer.requests)
		}
	})
}

func TestGitHubWorkItemPRSocialFetcherMarksNonRateProviderFailureIncomplete(t *testing.T) {
	doer := &gitHubWorkItemPRSocialFetchDoer{t: t, status: []int{http.StatusServiceUnavailable}, replies: []string{`{"message":"unavailable"}`}}
	client := gitHubPullRequestClient(t, doer, "https://api.github.com")
	client.Retry.MaxAttempts = 1
	result, err := (GitHubWorkItemPRSocialFetcher{}).Fetch(
		context.Background(), gitHubWorkItemPRSocialClaim(), client, []int{42}, 500, 1000,
	)
	if err != nil || result.Complete() || result.Incomplete == nil || result.Incomplete.Cause != "transient" || result.Payloads != nil {
		t.Fatalf("result=%+v error=%v", result, err)
	}
}

func TestGitHubWorkItemPRSocialFetcherCountsRetryAttemptsButOneLogicalPage(t *testing.T) {
	doer := &gitHubWorkItemPRSocialFetchDoer{
		t:      t,
		status: []int{http.StatusServiceUnavailable, http.StatusOK},
		replies: []string{
			`{"message":"unavailable"}`,
			`{"data":{"repository":{"pr0":{"number":42,"comments":` + gitHubWorkItemPRSocialConnectionJSON(`[{"id":"9007199254740993","body":"preserved"}]`, false, nil) + `}}}}`,
		},
	}
	client := gitHubPullRequestClient(t, doer, "https://api.github.com")
	client.Retry.MaxAttempts = 2
	budget := &gitHubPullRequestReviewBudget{}
	client.Budget = budget
	client.BudgetKey = providerfoundation.BudgetKey{
		Provider: "github", OrgID: "org-native", Host: "api.github.com",
		CostClass: "medium", Limit: 1, TTL: time.Minute,
	}

	result, err := (GitHubWorkItemPRSocialFetcher{}).Fetch(
		context.Background(), gitHubWorkItemPRSocialClaim(), client, []int{42}, 500, 0,
	)
	if err != nil || !result.Complete() {
		t.Fatalf("result=%+v error=%v", result, err)
	}
	if result.Evidence.Requests != 2 || result.Usage.RequestCount != 2 || result.Evidence.Pages != 1 ||
		budget.acquires != 1 || budget.releases != 1 || doer.requests != 2 {
		t.Fatalf("result=%+v budget=%+v requests=%d", result, budget, doer.requests)
	}
	if got := string(result.Payloads[42].Comments[0]); !strings.Contains(got, `"id":"9007199254740993"`) ||
		!strings.Contains(got, `"body":"preserved"`) {
		t.Fatalf("raw payload changed: %s", got)
	}
}

func TestGitHubWorkItemPRSocialFetchAdaptsIntoCompletePRSemanticBundle(t *testing.T) {
	doer := &gitHubWorkItemPRSocialFetchDoer{t: t, replies: []string{
		`{"data":{"repository":{"pr0":{"number":42,"comments":` + gitHubWorkItemPRSocialConnectionJSON(
			`[{"databaseId":9007199254740993,"body":"Blocked by https://linear.app/fullchaos/issue/CHAOS-501/task","createdAt":"2026-08-03T08:30:00Z","author":{"login":"linear[bot]"}}]`, false, nil,
		) + `,"timelineItems":` + gitHubWorkItemPRSocialConnectionJSON(
			`[{"__typename":"ClosedEvent","createdAt":"2026-08-02T08:00:00Z","actor":{"login":"closer"}},{"__typename":"ReopenedEvent","createdAt":"2026-08-03T08:00:00Z","actor":{"login":"reopener"}},{"__typename":"MergedEvent","createdAt":"2026-08-03T09:00:00Z","actor":{"login":"merger"}}]`, false, nil,
		) + `,"closingIssuesReferences":{"nodes":[{"number":501,"repository":{"nameWithOwner":"acme/api"}}]}}}}}`,
	}}
	result, err := (GitHubWorkItemPRSocialFetcher{}).Fetch(
		context.Background(), gitHubWorkItemPRSocialClaim(),
		gitHubPullRequestClient(t, doer, "https://api.github.com"), []int{42}, 500, 1000,
	)
	if err != nil || !result.Complete() {
		t.Fatalf("fetch result=%+v error=%v", result, err)
	}
	if len(result.Payloads[42].ClosingIssueRefs) != 1 {
		t.Fatalf("fetch did not carry closingIssuesReferences through: payload=%+v", result.Payloads[42])
	}
	adapted, err := adaptGitHubWorkItemPRSocialPayload(result.Payloads[42])
	if err != nil {
		t.Fatal(err)
	}
	claim := gitHubWorkItemPRSocialClaim()
	claim.OrgID = "77777777-7777-4777-8777-777777777777"
	rows, err := normalizeGitHubPullRequestBundle(
		claim, "acme/api", githubWorkItemPRSocialRepoID,
		json.RawMessage(`{
		  "number":42,"title":"Keep social facts","body":"Routine repair",
		  "state":"open","merged":false,"draft":false,
		  "created_at":"2026-08-01T08:00:00Z","updated_at":"2026-08-03T09:30:00Z",
		  "labels":[],"assignees":[],"user":{"login":"author"},
		  "head":{"ref":"feature/repair"}
		}`),
		adapted.Events, adapted.Comments, adapted.ClosingIssueRefs,
		nil, time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows.WorkItems) != 1 || len(rows.StatusTransitions) != 3 ||
		len(rows.ReopenEvents) != 1 || len(rows.Interactions) != 1 ||
		len(rows.Dependencies) != 2 {
		t.Fatalf("fetch->adapter->normalize dropped semantic rows: %+v", rows)
	}
	if rows.Interactions[0].Actor == nil || *rows.Interactions[0].Actor != "github:linear[bot]" ||
		rows.Interactions[0].BodyLength != 60 {
		// The exact body length is asserted independently by the live semantic
		// oracle; the fixed independent count ensures the fetched body,
		// timestamp, and author reached the interaction producer instead of a
		// zero-value stand-in.
		actor := "<nil>"
		if rows.Interactions[0].Actor != nil {
			actor = *rows.Interactions[0].Actor
		}
		t.Fatalf("interaction=%+v actor=%q", rows.Interactions[0], actor)
	}
	if dependency := rows.Dependencies[0]; dependency.TargetWorkItemID != "extkey:CHAOS-501" ||
		dependency.RelationshipType != "blocked_by" {
		t.Fatalf("dependency=%+v", dependency)
	}
	// CHAOS-4757: the PRIMARY closingIssuesReferences dependency rides the same
	// fetch->adapt->normalize path, alongside (not instead of) the FALLBACK
	// text-parse dependency above.
	if closing := rows.Dependencies[1]; closing.SourceWorkItemID != "ghpr:acme/api#42" ||
		closing.TargetWorkItemID != "gh:acme/api#501" || closing.RelationshipType != "relates_to" ||
		closing.RelationshipTypeRaw != "github_closing_reference" {
		t.Fatalf("closing-reference dependency=%+v", closing)
	}
	merged := rows.StatusTransitions[2]
	if merged.ToStatus != "done" || merged.ToStatusRaw == nil || *merged.ToStatusRaw != "merged" {
		t.Fatalf("merged transition=%+v", merged)
	}
}

// TestGitHubWorkItemPRSocialFetcherSignalsClosingReferenceTruncation is the
// red-first test for codex round 2b's P2 finding: a PR at or beyond
// gitHubWorkItemPRSocialClosingRefsLimit must surface that as
// ClosingIssueRefsTruncated (pageInfo.hasNextPage), not silently report a
// synced count that looks complete. The nodes count here does not need to
// reach the actual cap (20) -- TestGitHubWorkItemPRSocialFetcherHonorsDeclaredLimitsWithoutOverfetch
// already proves the shared append primitive truncates at the cap; this test
// isolates the pageInfo signal itself.
func TestGitHubWorkItemPRSocialFetcherSignalsClosingReferenceTruncation(t *testing.T) {
	doer := &gitHubWorkItemPRSocialFetchDoer{t: t, replies: []string{
		`{"data":{"repository":{"pr0":{"number":42,"comments":` + gitHubWorkItemPRSocialConnectionJSON(`[]`, false, nil) +
			`,"timelineItems":` + gitHubWorkItemPRSocialConnectionJSON(`[]`, false, nil) +
			`,"closingIssuesReferences":{"nodes":[{"number":501,"repository":{"nameWithOwner":"acme/api"}}],"pageInfo":{"hasNextPage":true}}}}}}`,
	}}
	result, err := (GitHubWorkItemPRSocialFetcher{}).Fetch(
		context.Background(), gitHubWorkItemPRSocialClaim(),
		gitHubPullRequestClient(t, doer, "https://api.github.com"), []int{42}, 500, 1000,
	)
	if err != nil || !result.Complete() {
		t.Fatalf("fetch result=%+v error=%v", result, err)
	}
	payload := result.Payloads[42]
	if len(payload.ClosingIssueRefs) != 1 || !payload.ClosingIssueRefsTruncated {
		t.Fatalf("expected 1 captured ref and truncation flagged, got payload=%+v", payload)
	}
	adapted, err := adaptGitHubWorkItemPRSocialPayload(payload)
	if err != nil {
		t.Fatal(err)
	}
	if !adapted.ClosingIssueRefsTruncated {
		t.Fatalf("adapter dropped the truncation signal: adapted=%+v", adapted)
	}
}

// TestGitHubWorkItemPRSocialFetcherClosingReferenceCompletePageIsNotTruncated
// is the negative case: a page that exhausts the connection (hasNextPage
// false, or the field absent entirely) must NOT be flagged.
func TestGitHubWorkItemPRSocialFetcherClosingReferenceCompletePageIsNotTruncated(t *testing.T) {
	doer := &gitHubWorkItemPRSocialFetchDoer{t: t, replies: []string{
		`{"data":{"repository":{"pr0":{"number":42,"comments":` + gitHubWorkItemPRSocialConnectionJSON(`[]`, false, nil) +
			`,"timelineItems":` + gitHubWorkItemPRSocialConnectionJSON(`[]`, false, nil) +
			`,"closingIssuesReferences":{"nodes":[{"number":501,"repository":{"nameWithOwner":"acme/api"}}],"pageInfo":{"hasNextPage":false}}}}}}`,
	}}
	result, err := (GitHubWorkItemPRSocialFetcher{}).Fetch(
		context.Background(), gitHubWorkItemPRSocialClaim(),
		gitHubPullRequestClient(t, doer, "https://api.github.com"), []int{42}, 500, 1000,
	)
	if err != nil || !result.Complete() {
		t.Fatalf("fetch result=%+v error=%v", result, err)
	}
	if payload := result.Payloads[42]; payload.ClosingIssueRefsTruncated {
		t.Fatalf("a complete page must not be flagged truncated: payload=%+v", payload)
	}
}
