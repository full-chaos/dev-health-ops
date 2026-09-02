package providersync

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

const (
	gitHubWorkItemPRSocialBatchSize          = 50
	gitHubWorkItemPRSocialPageSize           = 100
	gitHubWorkItemPRSocialDefaultMaxRequests = 1000
	gitHubWorkItemPRSocialMaxRequests        = 1000
	// gitHubWorkItemPRSocialClosingRefsLimit bounds the PRIMARY provider-attached
	// PR-issue link mechanism (CHAOS-4757): GitHub's own closingIssuesReferences,
	// requested once per PR on the top-level (non-continuation) batch page only —
	// unlike comments/events this has no drain/pagination loop, since a PR
	// realistically closes at most a handful of issues. A PR at or beyond this
	// limit degrades gracefully -- the extra references are not captured -- but
	// unlike a purely cosmetic field, this IS evidence-bearing data (it feeds
	// team-attribution edges), so the truncation itself is never silent: codex
	// round 2b (P2) correctly flagged that an unbounded-looking truncation with
	// no signal is a silent-failure defect for data this consequential.
	// GitHubWorkItemPRSocialPayload.ClosingIssueRefsTruncated carries the
	// pageInfo.hasNextPage bit through to the route, which records a
	// pull_request_processing incompleteness entry (D17) rather than silently
	// completing.
	gitHubWorkItemPRSocialClosingRefsLimit = 20
)

// GitHubWorkItemPRSocialPayload preserves the GraphQL node payloads consumed
// by the work-item normalizers. This fetch foundation deliberately does not
// normalize rows, emit effects, or own a watermark.
type GitHubWorkItemPRSocialPayload struct {
	Comments []json.RawMessage
	Events   []json.RawMessage
	// ClosingIssueRefs holds the raw closingIssuesReferences nodes for this PR
	// (CHAOS-4757 PRIMARY link mechanism). Nil/absent from the wire response is
	// tolerated as "no closing references" rather than a hard error: unlike
	// Comments/Events (which the query always requests through an explicit,
	// caller-controlled limit and therefore treats a nil connection as a
	// protocol mismatch), this field is requested unconditionally whenever the
	// fetch runs at all, so treating its absence strictly would fail every
	// caller that does not also stub it in a mocked response.
	ClosingIssueRefs []json.RawMessage
	// ClosingIssueRefsTruncated is true when this PR has more closing
	// references than gitHubWorkItemPRSocialClosingRefsLimit captured
	// (pageInfo.hasNextPage on the first, only-fetched page). The route turns
	// this into a durable pull_request_processing incompleteness entry rather
	// than reporting a synced count that looks complete but is not.
	ClosingIssueRefsTruncated bool
}

// GitHubWorkItemPRSocialUsage is safe provider-budget evidence. It records
// actual GraphQL wire attempts (including retries), without retaining query,
// response, repository, or credential data.
type GitHubWorkItemPRSocialUsage struct {
	Transport    string
	RouteFamily  string
	Dimension    string
	RequestCount int
}

// GitHubWorkItemPRSocialFetchIncomplete distinguishes Python's optional
// PR-social degradation from a complete empty result. Cause is a stable class
// or local pagination reason, never provider response text.
type GitHubWorkItemPRSocialFetchIncomplete struct {
	Cause string
}

// GitHubWorkItemPRSocialFetchResult is intentionally not CompleteRouteBatch.
// The work-item route must compose this raw fetch with every other work-item
// surface before it can write effects or advance the canonical watermark.
type GitHubWorkItemPRSocialFetchResult struct {
	Payloads   map[int]GitHubWorkItemPRSocialPayload
	Evidence   FetchEvidence
	Usage      GitHubWorkItemPRSocialUsage
	Incomplete *GitHubWorkItemPRSocialFetchIncomplete
}

func (result GitHubWorkItemPRSocialFetchResult) Complete() bool {
	return result.Incomplete == nil
}

// GitHubWorkItemPRSocialFetcher fetches issue comments and status-bearing
// timeline events for PR numbers selected by the caller's REST traversal.
// Reviews and review comments are intentionally absent from every query. The
// per-fetch request cap bounds the initial alias batches plus all independent
// per-PR connection continuations.
type GitHubWorkItemPRSocialFetcher struct {
	MaxRequests int
}

func (fetcher GitHubWorkItemPRSocialFetcher) Fetch(
	ctx context.Context,
	claim Claim,
	client *providerfoundation.HTTPClient,
	targets []int,
	commentsLimit int,
	eventsLimit int,
) (GitHubWorkItemPRSocialFetchResult, error) {
	result := GitHubWorkItemPRSocialFetchResult{
		Payloads: make(map[int]GitHubWorkItemPRSocialPayload, len(targets)),
		Evidence: FetchEvidence{Provider: "github", Dataset: "work-items-pr-social"},
		Usage: GitHubWorkItemPRSocialUsage{
			Transport: "graphql", RouteFamily: "work_item_prs", Dimension: BudgetGraphQLCost,
		},
	}
	if ctx == nil || claim.Validate() != nil || claim.Provider != "github" ||
		claim.Dataset != "work-items" || client == nil || client.Provider != "github" ||
		client.BaseURL == nil || client.Lease == nil || commentsLimit < 0 || eventsLimit < 0 {
		return GitHubWorkItemPRSocialFetchResult{}, ErrInvalidConfiguration
	}
	maxRequests := fetcher.MaxRequests
	if maxRequests == 0 {
		maxRequests = gitHubWorkItemPRSocialDefaultMaxRequests
	}
	if maxRequests < 1 || maxRequests > gitHubWorkItemPRSocialMaxRequests {
		return GitHubWorkItemPRSocialFetchResult{}, ErrInvalidConfiguration
	}
	seen := make(map[int]struct{}, len(targets))
	for _, number := range targets {
		if number < 1 {
			return GitHubWorkItemPRSocialFetchResult{}, providerfoundation.ErrNormalizationInvalid
		}
		if _, duplicate := seen[number]; duplicate {
			return GitHubWorkItemPRSocialFetchResult{}, providerfoundation.ErrNormalizationInvalid
		}
		seen[number] = struct{}{}
		result.Payloads[number] = GitHubWorkItemPRSocialPayload{}
	}
	if len(targets) == 0 || commentsLimit == 0 && eventsLimit == 0 {
		return result, nil
	}
	owner, repository, err := splitGitHubRepository(claim.SourceExternalID)
	if err != nil {
		return GitHubWorkItemPRSocialFetchResult{}, err
	}
	counted := *client
	counted.Doer = gitHubWorkItemPRSocialCountingDoer{
		delegate: client.Doer, requests: &result.Evidence.Requests,
		graphqlRequests: &result.Usage.RequestCount, graphqlPath: gitHubGraphQLPath(client),
	}

	for start := 0; start < len(targets); start += gitHubWorkItemPRSocialBatchSize {
		end := min(start+gitHubWorkItemPRSocialBatchSize, len(targets))
		commentsFirst := min(commentsLimit, gitHubWorkItemPRSocialPageSize)
		eventsFirst := min(eventsLimit, gitHubWorkItemPRSocialPageSize)
		pulls, err := fetcher.fetchPage(
			ctx, &counted, owner, repository, targets[start:end],
			commentsFirst, nil, eventsFirst, nil, &result,
		)
		if err != nil {
			return fetcher.finishFailure(result, err)
		}
		for _, number := range targets[start:end] {
			pull, ok := pulls[number]
			if !ok {
				return fetcher.finishFailure(result, providerfoundation.ErrGraphQLResponse)
			}
			payload := result.Payloads[number]
			if commentsLimit > 0 {
				if pull.Comments == nil {
					return fetcher.finishFailure(result, providerfoundation.ErrGraphQLResponse)
				}
				appendGitHubWorkItemPRSocialNodes(&payload.Comments, pull.Comments.Nodes, commentsLimit)
				if err := fetcher.drainComments(
					ctx, &counted, owner, repository, number, commentsLimit,
					pull.Comments.PageInfo, &payload, &result,
				); err != nil {
					return fetcher.finishFailure(result, err)
				}
			}
			if eventsLimit > 0 {
				if pull.TimelineItems == nil {
					return fetcher.finishFailure(result, providerfoundation.ErrGraphQLResponse)
				}
				appendGitHubWorkItemPRSocialNodes(&payload.Events, pull.TimelineItems.Nodes, eventsLimit)
				if err := fetcher.drainEvents(
					ctx, &counted, owner, repository, number, eventsLimit,
					pull.TimelineItems.PageInfo, &payload, &result,
				); err != nil {
					return fetcher.finishFailure(result, err)
				}
			}
			// closingIssuesReferences is requested unconditionally on this
			// top-level page (see gitHubWorkItemPRSocialQuery) but tolerated as
			// absent — see GitHubWorkItemPRSocialPayload.ClosingIssueRefs.
			if pull.ClosingIssuesReferences != nil {
				appendGitHubWorkItemPRSocialNodes(
					&payload.ClosingIssueRefs, pull.ClosingIssuesReferences.Nodes,
					gitHubWorkItemPRSocialClosingRefsLimit,
				)
				// codex round 2b (P2): a PR with more references than the cap must
				// signal truncation, not silently report a complete-looking count —
				// this is evidence-bearing data, unlike Comments/Events' social color.
				payload.ClosingIssueRefsTruncated = pull.ClosingIssuesReferences.PageInfo.HasNextPage
			}
			result.Payloads[number] = payload
		}
	}
	for _, payload := range result.Payloads {
		result.Evidence.Records += len(payload.Comments) + len(payload.Events) + len(payload.ClosingIssueRefs)
	}
	return result, nil
}

func (fetcher GitHubWorkItemPRSocialFetcher) drainComments(
	ctx context.Context,
	client *providerfoundation.HTTPClient,
	owner, repository string,
	number, limit int,
	pageInfo gitHubWorkItemPRSocialPageInfo,
	payload *GitHubWorkItemPRSocialPayload,
	result *GitHubWorkItemPRSocialFetchResult,
) error {
	cursor := pageInfo.EndCursor
	for pageInfo.HasNextPage && len(payload.Comments) < limit {
		if cursor == nil || strings.TrimSpace(*cursor) == "" {
			return errGitHubWorkItemPRSocialPaginationInvalid
		}
		first := min(limit-len(payload.Comments), gitHubWorkItemPRSocialPageSize)
		pulls, err := fetcher.fetchPage(
			ctx, client, owner, repository, []int{number}, first, cursor, 0, nil, result,
		)
		if err != nil {
			return err
		}
		pull, ok := pulls[number]
		if !ok || pull.Comments == nil {
			return providerfoundation.ErrGraphQLResponse
		}
		appendGitHubWorkItemPRSocialNodes(&payload.Comments, pull.Comments.Nodes, limit)
		pageInfo = pull.Comments.PageInfo
		next := pageInfo.EndCursor
		if pageInfo.HasNextPage && len(payload.Comments) < limit &&
			(next == nil || strings.TrimSpace(*next) == "" || *next == *cursor) {
			return errGitHubWorkItemPRSocialPaginationInvalid
		}
		cursor = next
	}
	return nil
}

func (fetcher GitHubWorkItemPRSocialFetcher) drainEvents(
	ctx context.Context,
	client *providerfoundation.HTTPClient,
	owner, repository string,
	number, limit int,
	pageInfo gitHubWorkItemPRSocialPageInfo,
	payload *GitHubWorkItemPRSocialPayload,
	result *GitHubWorkItemPRSocialFetchResult,
) error {
	cursor := pageInfo.EndCursor
	for pageInfo.HasNextPage && len(payload.Events) < limit {
		if cursor == nil || strings.TrimSpace(*cursor) == "" {
			return errGitHubWorkItemPRSocialPaginationInvalid
		}
		first := min(limit-len(payload.Events), gitHubWorkItemPRSocialPageSize)
		pulls, err := fetcher.fetchPage(
			ctx, client, owner, repository, []int{number}, 0, nil, first, cursor, result,
		)
		if err != nil {
			return err
		}
		pull, ok := pulls[number]
		if !ok || pull.TimelineItems == nil {
			return providerfoundation.ErrGraphQLResponse
		}
		appendGitHubWorkItemPRSocialNodes(&payload.Events, pull.TimelineItems.Nodes, limit)
		pageInfo = pull.TimelineItems.PageInfo
		next := pageInfo.EndCursor
		if pageInfo.HasNextPage && len(payload.Events) < limit &&
			(next == nil || strings.TrimSpace(*next) == "" || *next == *cursor) {
			return errGitHubWorkItemPRSocialPaginationInvalid
		}
		cursor = next
	}
	return nil
}

func appendGitHubWorkItemPRSocialNodes(destination *[]json.RawMessage, nodes []json.RawMessage, limit int) {
	remaining := limit - len(*destination)
	if remaining <= 0 {
		return
	}
	if len(nodes) > remaining {
		nodes = nodes[:remaining]
	}
	*destination = append(*destination, nodes...)
}

func (fetcher GitHubWorkItemPRSocialFetcher) fetchPage(
	ctx context.Context,
	client *providerfoundation.HTTPClient,
	owner, repository string,
	numbers []int,
	commentsFirst int,
	commentsAfter *string,
	eventsFirst int,
	eventsAfter *string,
	result *GitHubWorkItemPRSocialFetchResult,
) (map[int]gitHubWorkItemPRSocialGraphQLPull, error) {
	maxRequests := fetcher.MaxRequests
	if maxRequests == 0 {
		maxRequests = gitHubWorkItemPRSocialDefaultMaxRequests
	}
	if result.Evidence.Pages >= maxRequests {
		result.Evidence.CapReached = true
		return nil, errGitHubWorkItemPRSocialPaginationCap
	}
	body, err := json.Marshal(map[string]any{
		"query": gitHubWorkItemPRSocialQuery(
			numbers, commentsFirst, commentsAfter, eventsFirst, eventsAfter,
		),
		"variables": map[string]string{"owner": owner, "repo": repository},
	})
	if err != nil {
		return nil, providerfoundation.ErrNormalizationInvalid
	}
	response, err := client.Do(ctx, http.MethodPost, gitHubGraphQLPath(client), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	result.Evidence.Pages++
	defer response.Body.Close()
	decoder := json.NewDecoder(response.Body)
	decoder.UseNumber()
	var envelope gitHubWorkItemPRSocialGraphQLEnvelope
	if err := decoder.Decode(&envelope); err != nil {
		return nil, providerfoundation.ErrGraphQLResponse
	}
	if len(envelope.Errors) > 0 || envelope.Data.Repository == nil {
		return nil, providerfoundation.ErrGraphQLResponse
	}
	pulls := make(map[int]gitHubWorkItemPRSocialGraphQLPull, len(numbers))
	for index, number := range numbers {
		pull, ok := (*envelope.Data.Repository)["pr"+strconv.Itoa(index)]
		if !ok || pull == nil || pull.Number != number {
			return nil, providerfoundation.ErrGraphQLResponse
		}
		pulls[number] = *pull
	}
	return pulls, nil
}

func (fetcher GitHubWorkItemPRSocialFetcher) finishFailure(
	result GitHubWorkItemPRSocialFetchResult,
	err error,
) (GitHubWorkItemPRSocialFetchResult, error) {
	if errors.Is(err, providerfoundation.ErrLeaseLost) || errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) || isRateLimited(err) {
		return result, err
	}
	result.Payloads = nil
	result.Evidence.Records = 0
	result.Incomplete = &GitHubWorkItemPRSocialFetchIncomplete{
		Cause: gitHubWorkItemPRSocialFailureCause(err),
	}
	return result, nil
}

func gitHubWorkItemPRSocialFailureCause(err error) string {
	if errors.Is(err, errGitHubWorkItemPRSocialPaginationCap) {
		return "pagination_cap"
	}
	if errors.Is(err, errGitHubWorkItemPRSocialPaginationInvalid) {
		return "invalid_pagination"
	}
	var providerErr *providerfoundation.ProviderError
	if errors.As(err, &providerErr) && providerErr != nil {
		return string(providerErr.Class)
	}
	return "invalid_response"
}

var (
	errGitHubWorkItemPRSocialPaginationCap     = errors.New("github work-item PR-social request cap reached")
	errGitHubWorkItemPRSocialPaginationInvalid = errors.New("github work-item PR-social cursor is missing or stalled")
)

type gitHubWorkItemPRSocialCountingDoer struct {
	delegate        providerfoundation.HTTPDoer
	requests        *int
	graphqlRequests *int
	graphqlPath     string
}

func (doer gitHubWorkItemPRSocialCountingDoer) Do(request *http.Request) (*http.Response, error) {
	*doer.requests++
	if request.URL.EscapedPath() == doer.graphqlPath {
		*doer.graphqlRequests++
	}
	return doer.delegate.Do(request)
}

type gitHubWorkItemPRSocialGraphQLEnvelope struct {
	Data struct {
		Repository *map[string]*gitHubWorkItemPRSocialGraphQLPull `json:"repository"`
	} `json:"data"`
	Errors []json.RawMessage `json:"errors"`
}

type gitHubWorkItemPRSocialGraphQLPull struct {
	Number                  int                               `json:"number"`
	Comments                *gitHubWorkItemPRSocialConnection `json:"comments"`
	TimelineItems           *gitHubWorkItemPRSocialConnection `json:"timelineItems"`
	ClosingIssuesReferences *gitHubWorkItemPRSocialConnection `json:"closingIssuesReferences"`
}

type gitHubWorkItemPRSocialConnection struct {
	Nodes    []json.RawMessage              `json:"nodes"`
	PageInfo gitHubWorkItemPRSocialPageInfo `json:"pageInfo"`
}

type gitHubWorkItemPRSocialPageInfo struct {
	HasNextPage bool    `json:"hasNextPage"`
	EndCursor   *string `json:"endCursor"`
}

func gitHubWorkItemPRSocialQuery(
	numbers []int,
	commentsFirst int,
	commentsAfter *string,
	eventsFirst int,
	eventsAfter *string,
) string {
	commentsCursor, _ := json.Marshal(commentsAfter)
	eventsCursor, _ := json.Marshal(eventsAfter)
	// closingIssuesReferences (CHAOS-4757) is requested only on the top-level
	// batch page, never on a comments/events continuation page: both cursor
	// args are nil exactly once per PR, on the initial fetchPage call from
	// Fetch's main loop (drainComments/drainEvents each hold one cursor
	// non-nil), so re-requesting it on every continuation page would be
	// redundant work for data that never changes within one Fetch call.
	topLevelBatchPage := commentsAfter == nil && eventsAfter == nil
	aliases := make([]string, 0, len(numbers))
	for index, number := range numbers {
		fields := []string{"number"}
		if commentsFirst > 0 {
			fields = append(fields, fmt.Sprintf(
				"comments(first: %d, after: %s, orderBy: {field: UPDATED_AT, direction: ASC}) { nodes { id databaseId fullDatabaseId body createdAt author { login } } pageInfo { hasNextPage endCursor } }",
				commentsFirst, commentsCursor,
			))
		}
		if eventsFirst > 0 {
			fields = append(fields, fmt.Sprintf(
				"timelineItems(itemTypes: [MERGED_EVENT, CLOSED_EVENT, REOPENED_EVENT], first: %d, after: %s) { nodes { __typename ... on MergedEvent { createdAt actor { login } } ... on ClosedEvent { createdAt actor { login } } ... on ReopenedEvent { createdAt actor { login } } } pageInfo { hasNextPage endCursor } }",
				eventsFirst, eventsCursor,
			))
		}
		if topLevelBatchPage {
			fields = append(fields, fmt.Sprintf(
				"closingIssuesReferences(first: %d) { nodes { number repository { nameWithOwner } } pageInfo { hasNextPage } }",
				gitHubWorkItemPRSocialClosingRefsLimit,
			))
		}
		aliases = append(aliases, fmt.Sprintf(
			"pr%d: pullRequest(number: %d) { %s }", index, number, strings.Join(fields, " "),
		))
	}
	return "query($owner: String!, $repo: String!) { repository(owner: $owner, name: $repo) { " + strings.Join(aliases, " ") + " } }"
}
