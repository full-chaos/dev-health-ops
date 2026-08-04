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
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

const (
	gitHubPullRequestReviewBatchSize = 50
	gitHubPullRequestReviewPageSize  = 100
	gitHubPullRequestReviewMaxPages  = 100
)

// GitHubPullRequestReviewTarget is a PR already selected by the PR collector.
// Reviews deliberately do not list PRs themselves: Python enriches the same
// selected PR set after its REST PR traversal, and this foundation must not
// become a second, independently watermarked git_pull_requests producer.
type GitHubPullRequestReviewTarget struct {
	Number    int
	CreatedAt time.Time
}

// GitHubPullRequestReviewUsage is the safe, bounded accounting emitted by the
// review fetch. It mirrors Python's pr_social GraphQL attribution without
// retaining a query, response body, repository name, or credential detail.
type GitHubPullRequestReviewUsage struct {
	Transport    string
	RouteFamily  string
	Dimension    string
	RequestCount int
}

// GitHubPullRequestReviewFetchIncomplete says the optional review enrichment
// degraded before it established a complete result. Consumers must inspect
// Incomplete rather than treating an empty Rows slice as a complete empty
// review collection. Cause is intentionally the stable provider class (or a
// fixed local reason), never the provider response/error text.
type GitHubPullRequestReviewFetchIncomplete struct {
	Cause string
}

// GitHubPullRequestReviewFetchResult is intentionally not CompleteRouteBatch:
// this is a non-routed fetch foundation and cannot write effects, advance a
// watermark, or assert route readiness. Rate limits, cancellation, and lease
// loss return errors; other fetch failures return Incomplete to preserve the
// Python optional-enrichment behavior without fabricating completeness.
type GitHubPullRequestReviewFetchResult struct {
	Rows       []pullRequestReviewRow
	Evidence   FetchEvidence
	Usage      GitHubPullRequestReviewUsage
	Incomplete *GitHubPullRequestReviewFetchIncomplete
}

func (result GitHubPullRequestReviewFetchResult) Complete() bool {
	return result.Incomplete == nil
}

// GitHubPullRequestReviewFetcher fetches the nested GraphQL reviews
// connection for caller-selected PRs. MaxPages is a total GraphQL request
// budget (initial alias batches plus per-PR continuation requests); zero uses
// the bounded production default. repoID is derived by the caller's existing
// repositoryIdentity path alongside PR selection, exactly as Python passes
// repo_id into _enrich_prs_with_reviews_batch; this fetch never refetches repo
// metadata or creates a competing repository-identity request.
type GitHubPullRequestReviewFetcher struct {
	MaxPages int
}

func (fetcher GitHubPullRequestReviewFetcher) Fetch(
	ctx context.Context,
	claim Claim,
	client *providerfoundation.HTTPClient,
	repoID string,
	targets []GitHubPullRequestReviewTarget,
	normalizedAt time.Time,
) (GitHubPullRequestReviewFetchResult, error) {
	result := GitHubPullRequestReviewFetchResult{
		Evidence: FetchEvidence{Provider: "github", Dataset: "pr-reviews"},
		Usage: GitHubPullRequestReviewUsage{
			Transport: "graphql", RouteFamily: "pr_social", Dimension: BudgetGraphQLCost,
		},
	}
	if ctx == nil || claim.Validate() != nil || claim.Provider != "github" ||
		claim.Dataset != "pr-reviews" || client == nil || client.Provider != "github" ||
		client.BaseURL == nil || client.Lease == nil || normalizedAt.IsZero() {
		return GitHubPullRequestReviewFetchResult{}, ErrInvalidConfiguration
	}
	if len(targets) == 0 {
		return result, nil
	}
	if len(repoID) != 36 {
		return GitHubPullRequestReviewFetchResult{}, providerfoundation.ErrNormalizationInvalid
	}
	maxPages := fetcher.MaxPages
	if maxPages == 0 {
		maxPages = gitHubPullRequestReviewMaxPages
	}
	if maxPages < 1 || maxPages > gitHubPullRequestReviewMaxPages {
		return GitHubPullRequestReviewFetchResult{}, ErrInvalidConfiguration
	}
	targetByNumber := make(map[int]GitHubPullRequestReviewTarget, len(targets))
	for _, target := range targets {
		if target.Number < 1 || target.CreatedAt.IsZero() {
			return GitHubPullRequestReviewFetchResult{}, providerfoundation.ErrNormalizationInvalid
		}
		if _, duplicate := targetByNumber[target.Number]; duplicate {
			return GitHubPullRequestReviewFetchResult{}, providerfoundation.ErrNormalizationInvalid
		}
		targetByNumber[target.Number] = target
	}
	owner, repository, err := splitGitHubRepository(claim.SourceExternalID)
	if err != nil {
		return GitHubPullRequestReviewFetchResult{}, err
	}
	counted := *client
	counted.Doer = gitHubPullRequestReviewCountingDoer{
		delegate: client.Doer, requests: &result.Evidence.Requests,
		graphqlRequests: &result.Usage.RequestCount, graphqlPath: gitHubGraphQLPath(client),
	}

	numbers := make([]int, 0, len(targets))
	for _, target := range targets {
		numbers = append(numbers, target.Number)
	}
	for start := 0; start < len(numbers); start += gitHubPullRequestReviewBatchSize {
		end := min(start+gitHubPullRequestReviewBatchSize, len(numbers))
		connections, err := fetcher.fetchPage(ctx, &counted, owner, repository, numbers[start:end], nil, &result)
		if err != nil {
			return fetcher.finishFailure(result, err)
		}
		for _, number := range numbers[start:end] {
			connection, ok := connections[number]
			if !ok {
				return fetcher.finishFailure(result, providerfoundation.ErrNormalizationInvalid)
			}
			if err := appendGitHubPullRequestReviewRows(&result, claim, repoID, targetByNumber[number], connection.Nodes, normalizedAt); err != nil {
				return fetcher.finishFailure(result, err)
			}
			cursor := connection.PageInfo.EndCursor
			for connection.PageInfo.HasNextPage {
				if cursor == "" {
					return fetcher.finishFailure(result, providerfoundation.ErrPaginationInvalid)
				}
				connections, err = fetcher.fetchPage(ctx, &counted, owner, repository, []int{number}, map[int]string{number: cursor}, &result)
				if err != nil {
					return fetcher.finishFailure(result, err)
				}
				connection, ok = connections[number]
				if !ok {
					return fetcher.finishFailure(result, providerfoundation.ErrNormalizationInvalid)
				}
				if err := appendGitHubPullRequestReviewRows(&result, claim, repoID, targetByNumber[number], connection.Nodes, normalizedAt); err != nil {
					return fetcher.finishFailure(result, err)
				}
				next := connection.PageInfo.EndCursor
				if connection.PageInfo.HasNextPage && next == cursor {
					return fetcher.finishFailure(result, providerfoundation.ErrPaginationInvalid)
				}
				cursor = next
			}
		}
	}
	result.Evidence.Records = len(result.Rows)
	return result, nil
}

// gitHubPullRequestReviewCountingDoer observes actual wire attempts, including
// HTTPClient retries. Budget reservations remain owned by HTTPClient.Do (one
// per logical call); this wrapper only provides the same actual-usage evidence
// the Python instrumented GraphQL client drains on success and failure.
type gitHubPullRequestReviewCountingDoer struct {
	delegate        providerfoundation.HTTPDoer
	requests        *int
	graphqlRequests *int
	graphqlPath     string
}

func (doer gitHubPullRequestReviewCountingDoer) Do(request *http.Request) (*http.Response, error) {
	*doer.requests++
	if request.URL.EscapedPath() == doer.graphqlPath {
		*doer.graphqlRequests++
	}
	return doer.delegate.Do(request)
}

func (fetcher GitHubPullRequestReviewFetcher) fetchPage(
	ctx context.Context,
	client *providerfoundation.HTTPClient,
	owner, repository string,
	numbers []int,
	after map[int]string,
	result *GitHubPullRequestReviewFetchResult,
) (map[int]gitHubPullRequestReviewConnection, error) {
	maxPages := fetcher.MaxPages
	if maxPages == 0 {
		maxPages = gitHubPullRequestReviewMaxPages
	}
	if result.Evidence.Pages >= maxPages {
		return nil, errGitHubPullRequestReviewPaginationCap
	}
	body, err := json.Marshal(map[string]any{
		"query":     gitHubPullRequestReviewsQuery(numbers, after),
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
	var envelope gitHubPullRequestReviewGraphQLEnvelope
	if err := decoder.Decode(&envelope); err != nil {
		return nil, providerfoundation.ErrPaginationInvalid
	}
	if len(envelope.Errors) > 0 || envelope.Data.Repository == nil {
		return nil, providerfoundation.ErrGraphQLResponse
	}
	connections := make(map[int]gitHubPullRequestReviewConnection, len(numbers))
	for index, number := range numbers {
		pull, ok := envelope.Data.Repository["pr"+strconv.Itoa(index)]
		if !ok || pull.Number != number {
			return nil, providerfoundation.ErrNormalizationInvalid
		}
		connections[number] = pull.Reviews
	}
	return connections, nil
}

func (fetcher GitHubPullRequestReviewFetcher) finishFailure(
	result GitHubPullRequestReviewFetchResult, err error,
) (GitHubPullRequestReviewFetchResult, error) {
	if errors.Is(err, providerfoundation.ErrLeaseLost) || errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) || isRateLimited(err) {
		return result, err
	}
	result.Rows = nil
	result.Evidence.Records = 0
	result.Incomplete = &GitHubPullRequestReviewFetchIncomplete{Cause: gitHubPullRequestReviewFailureCause(err)}
	return result, nil
}

func gitHubPullRequestReviewFailureCause(err error) string {
	if errors.Is(err, errGitHubPullRequestReviewPaginationCap) {
		return "pagination_cap"
	}
	var providerErr *providerfoundation.ProviderError
	if errors.As(err, &providerErr) && providerErr != nil {
		return string(providerErr.Class)
	}
	return "invalid_response"
}

var errGitHubPullRequestReviewPaginationCap = errors.New("github pull request review pagination cap reached")

type gitHubPullRequestReviewGraphQLEnvelope struct {
	Data struct {
		Repository map[string]gitHubPullRequestReviewGraphQLPull `json:"repository"`
	} `json:"data"`
	Errors []json.RawMessage `json:"errors"`
}

type gitHubPullRequestReviewGraphQLPull struct {
	Number  int                               `json:"number"`
	Reviews gitHubPullRequestReviewConnection `json:"reviews"`
}

type gitHubPullRequestReviewConnection struct {
	Nodes    []gitHubPullRequestReviewGraphQLNode `json:"nodes"`
	PageInfo struct {
		HasNextPage bool   `json:"hasNextPage"`
		EndCursor   string `json:"endCursor"`
	} `json:"pageInfo"`
}

type gitHubPullRequestReviewGraphQLNode struct {
	ID             any             `json:"id"`
	DatabaseID     any             `json:"databaseId"`
	FullDatabaseID any             `json:"fullDatabaseId"`
	Author         json.RawMessage `json:"author"`
	State          any             `json:"state"`
	SubmittedAt    *string         `json:"submittedAt"`
}

func appendGitHubPullRequestReviewRows(
	result *GitHubPullRequestReviewFetchResult,
	claim Claim,
	repoID string,
	target GitHubPullRequestReviewTarget,
	nodes []gitHubPullRequestReviewGraphQLNode,
	normalizedAt time.Time,
) error {
	for _, node := range nodes {
		payload := gitHubReviewPayload{
			ID:          gitHubPullRequestReviewID(node),
			Reviewer:    node.Author,
			State:       node.State,
			SubmittedAt: node.SubmittedAt,
		}
		row, err := normalizeGitHubPullRequestReview(claim, repoID, target.Number, payload, target.CreatedAt, normalizedAt)
		if err != nil {
			return err
		}
		result.Rows = append(result.Rows, row)
	}
	return nil
}

func gitHubPullRequestReviewID(node gitHubPullRequestReviewGraphQLNode) any {
	for _, candidate := range []any{node.DatabaseID, node.FullDatabaseID, node.ID} {
		if strings.TrimSpace(stringValue(candidate)) != "" {
			return candidate
		}
	}
	return nil
}

func gitHubPullRequestReviewsQuery(numbers []int, after map[int]string) string {
	aliases := make([]string, 0, len(numbers))
	for index, number := range numbers {
		cursor := any(nil)
		if after != nil {
			if value, ok := after[number]; ok {
				cursor = value
			}
		}
		encodedCursor, _ := json.Marshal(cursor)
		aliases = append(aliases, fmt.Sprintf(
			"pr%d: pullRequest(number: %d) { number reviews(first: %d, after: %s) { nodes { id databaseId fullDatabaseId state submittedAt author { login } } pageInfo { hasNextPage endCursor } } }",
			index, number, gitHubPullRequestReviewPageSize, encodedCursor,
		))
	}
	return "query($owner: String!, $repo: String!) { repository(owner: $owner, name: $repo) { " + strings.Join(aliases, " ") + " } }"
}
