package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/google/uuid"
)

const (
	githubWorkItemEventLimit   = 1000
	githubWorkItemCommentLimit = 500
)

// GitHubWorkItemsRESTIncomplete preserves Python's intentionally best-effort
// milestone and issue-comment behavior without turning a failed optional
// request into an indistinguishable successful empty collection. Cause is a
// stable provider error class, never provider response text.
type GitHubWorkItemsRESTIncomplete struct {
	Component string
	SubjectID string
	Cause     string
}

// GitHubWorkItemsRESTPullRequest is a REST-selected PR awaiting the composed
// GraphQL social fetch. The later layer must use Number as its GraphQL target
// and pass Payload to normalizeGitHubPullRequestBundle with the returned
// events/comments; this collector deliberately does not fabricate empty PR
// social data or emit a partially normalized PR row.
type GitHubWorkItemsRESTPullRequest struct {
	Number    int
	CreatedAt time.Time
	Payload   json.RawMessage
}

// GitHubWorkItemsRESTResult is a typed, non-routed collection result. It owns
// no effects, watermark, or capability registration. Rows contains the issue
// and milestone facts REST can establish completely; PullRequests are handed
// to the later GraphQL social layer before the composite can become complete.
type GitHubWorkItemsRESTResult struct {
	RepoFullName string
	RepoID       uuid.UUID
	Rows         githubWorkItemRows
	PullRequests []GitHubWorkItemsRESTPullRequest
	Incomplete   []GitHubWorkItemsRESTIncomplete
	Evidence     FetchEvidence
}

// NoOptionalDegradation reports only whether this REST layer observed an
// optional milestone or issue-comment degradation. It is deliberately not a
// composite-completion signal: every PullRequests entry still awaits the
// GraphQL social fetch before the GitHub work-item producer can be complete.
func (result GitHubWorkItemsRESTResult) NoOptionalDegradation() bool {
	return len(result.Incomplete) == 0
}

// GitHubWorkItemsRESTCollector builds the REST half of GitHub's composite
// work-item producer. It remains intentionally unregistered until the PR
// GraphQL social layer and every direct/derived effect are composed atomically.
type GitHubWorkItemsRESTCollector struct {
	MaxPages int
}

type githubWorkItemsRESTOptions struct {
	includeIssues       bool
	includePullRequests bool
	fetchComments       bool
	fetchMilestones     bool
	commentsLimit       int
}

type githubWorkItemsRESTListIssue struct {
	Number      int             `json:"number"`
	UpdatedAt   *string         `json:"updated_at"`
	PullRequest json.RawMessage `json:"pull_request"`
}

type githubWorkItemsRESTCountingDoer struct {
	delegate providerfoundation.HTTPDoer
	requests *int
}

func (doer githubWorkItemsRESTCountingDoer) Do(request *http.Request) (*http.Response, error) {
	*doer.requests = *doer.requests + 1
	return doer.delegate.Do(request)
}

func (collector GitHubWorkItemsRESTCollector) Collect(
	ctx context.Context,
	claim Claim,
	client *providerfoundation.HTTPClient,
	normalizedAt time.Time,
) (GitHubWorkItemsRESTResult, error) {
	result := GitHubWorkItemsRESTResult{
		Rows:         emptyGitHubWorkItemRows(),
		PullRequests: []GitHubWorkItemsRESTPullRequest{},
		Incomplete:   []GitHubWorkItemsRESTIncomplete{},
		Evidence: FetchEvidence{
			Provider: "github", Dataset: "work-items",
		},
	}
	if ctx == nil || claim.Validate() != nil || claim.Provider != "github" ||
		claim.Dataset != "work-items" || client == nil || client.Provider != "github" ||
		client.BaseURL == nil || client.Doer == nil || normalizedAt.IsZero() {
		return result, ErrInvalidConfiguration
	}
	options, err := githubWorkItemsRESTOptionsForClaim(claim)
	if err != nil {
		return result, err
	}
	maxPages := collector.MaxPages
	if maxPages == 0 {
		maxPages = nativeMaxPages
	}
	if maxPages < 1 || maxPages > nativeMaxPages {
		return result, ErrInvalidConfiguration
	}
	normalizedAt = normalizedAt.UTC().Truncate(time.Millisecond)
	owner, repository, err := splitGitHubRepository(claim.SourceExternalID)
	if err != nil {
		return result, err
	}
	root := providerRelativePath(client, "repos", owner, repository)
	counted := *client
	counted.Doer = githubWorkItemsRESTCountingDoer{
		delegate: client.Doer, requests: &result.Evidence.Requests,
	}

	var repo githubWorkItemsRESTRepositoryPayload
	if err := fetchObject(ctx, &counted, root, &repo); err != nil {
		return result, err
	}
	result.RepoFullName = strings.TrimSpace(repo.FullName)
	repoIdentity, identityErr := repositoryIdentity(result.RepoFullName)
	if identityErr != nil {
		return result, identityErr
	}
	result.RepoID, err = uuid.Parse(repoIdentity)
	if err != nil {
		return result, providerfoundation.ErrNormalizationInvalid
	}

	if options.fetchMilestones {
		page, pageErr := providerfoundation.CollectGitHubLinkPages(
			ctx, &counted, providerfoundation.GitHubPageOptions{
				Path:     root + "/milestones",
				Query:    url.Values{"state": {"all"}, "per_page": {"100"}},
				MaxPages: maxPages,
			},
		)
		result.addPageEvidence(page)
		if pageErr != nil {
			if fatalErr := githubWorkItemsRESTOptionalFailure(
				ctx, &counted, pageErr,
			); fatalErr != nil {
				return result, fatalErr
			}
			result.addIncomplete("milestones", "", pageErr)
		}
		if page.PageBudgetExhausted {
			return result, ErrPaginationCapExceeded
		}
		for _, raw := range page.Items {
			row, normalizeErr := normalizeGitHubSprint(
				claim, result.RepoFullName, raw, normalizedAt,
			)
			if normalizeErr != nil {
				result.addIncomplete("milestones", "", normalizeErr)
				break
			}
			result.Rows.Sprints = append(result.Rows.Sprints, row)
		}
	}

	if options.includeIssues {
		if err := collector.collectIssues(
			ctx, claim, &counted, root, options, normalizedAt, &result,
		); err != nil {
			return result, err
		}
	}
	if options.includePullRequests {
		if err := collector.collectPullRequests(
			ctx, claim, &counted, root, normalizedAt, &result,
		); err != nil {
			// provider.py:339-343 catches everything but a rate limit here and
			// continues with the issues it already has, so a failing /pulls
			// listing degrades the run instead of blocking the five-alias
			// family for this repository forever. A pagination cap is NOT that
			// case: the collected set would be deterministically truncated and
			// every later run would reproduce the same truncation, so it stays
			// fatal (recipe: never both capped and successful).
			if errors.Is(err, ErrPaginationCapExceeded) {
				return result, err
			}
			if fatalErr := githubWorkItemsRESTOptionalFailure(
				ctx, &counted, err,
			); fatalErr != nil {
				return result, fatalErr
			}
			// Python loses every pull request here (`prs = list(...)` never
			// binds, so its else-branch is skipped wholesale). We keep the ones
			// already fetched: under D17 a partial set is safe precisely
			// because the omission is recorded, and it matches the
			// retain-earlier-pages behaviour of the milestone and comment paths
			// above.
			result.addIncomplete("pull_requests", "", err)
		}
	}
	result.Evidence.Records = len(result.Rows.WorkItems) +
		len(result.Rows.Sprints) + len(result.PullRequests)
	return result, nil
}

type githubWorkItemsRESTRepositoryPayload struct {
	FullName string `json:"full_name"`
}

func (collector GitHubWorkItemsRESTCollector) collectIssues(
	ctx context.Context,
	claim Claim,
	client *providerfoundation.HTTPClient,
	root string,
	options githubWorkItemsRESTOptions,
	normalizedAt time.Time,
	result *GitHubWorkItemsRESTResult,
) error {
	query := url.Values{"state": {"all"}, "per_page": {"100"}}
	if claim.SinceAt != nil {
		query.Set("since", claim.SinceAt.UTC().Format(time.RFC3339))
	}
	page, err := providerfoundation.CollectGitHubLinkPages(
		ctx, client, providerfoundation.GitHubPageOptions{
			Path: root + "/issues", Query: query, MaxPages: collector.maxPages(),
		},
	)
	result.addPageEvidence(page)
	if err != nil {
		return err
	}
	if page.PageBudgetExhausted {
		return ErrPaginationCapExceeded
	}
	for _, raw := range page.Items {
		var listed githubWorkItemsRESTListIssue
		if json.Unmarshal(raw, &listed) != nil || listed.Number < 1 {
			return providerfoundation.ErrNormalizationInvalid
		}
		if len(listed.PullRequest) != 0 && string(listed.PullRequest) != "null" {
			continue
		}
		if githubWorkItemsRESTUpdatedAfterBefore(listed.UpdatedAt, claim.BeforeAt) {
			continue
		}
		events, eventPage, err := collectGitHubWorkItemChildPages(
			ctx, client, root+"/issues/"+strconv.Itoa(listed.Number)+"/events",
			collector.maxPages(), githubWorkItemEventLimit,
		)
		result.addPageEvidence(eventPage)
		if err != nil {
			return err
		}
		if eventPage.PageBudgetExhausted {
			return ErrPaginationCapExceeded
		}
		comments := []json.RawMessage{}
		if options.fetchComments && options.commentsLimit > 0 {
			commentRows, commentPage, commentErr := collectGitHubWorkItemChildPages(
				ctx, client, root+"/issues/"+strconv.Itoa(listed.Number)+"/comments",
				collector.maxPages(), options.commentsLimit,
			)
			result.addPageEvidence(commentPage)
			switch {
			case commentErr != nil:
				if fatalErr := githubWorkItemsRESTOptionalFailure(
					ctx, client, commentErr,
				); fatalErr != nil {
					return fatalErr
				}
				result.addIncomplete("issue_comments", strconv.Itoa(listed.Number), commentErr)
				comments = commentRows
			case commentPage.PageBudgetExhausted:
				return ErrPaginationCapExceeded
			default:
				comments = commentRows
			}
		}
		rows, normalizeErr := normalizeGitHubIssueBundle(
			claim, result.RepoFullName, result.RepoID, raw, events, comments,
			nil, normalizedAt,
		)
		if normalizeErr != nil && len(comments) > 0 {
			// Comment normalization is inside Python's best-effort comment loop.
			// Preserve the required issue/event facts and expose the degradation.
			result.addIncomplete("issue_comments", strconv.Itoa(listed.Number), normalizeErr)
			rows, normalizeErr = normalizeGitHubIssueBundle(
				claim, result.RepoFullName, result.RepoID, raw, events, nil,
				nil, normalizedAt,
			)
		}
		if normalizeErr != nil {
			return normalizeErr
		}
		appendGitHubWorkItemRows(&result.Rows, rows)
	}
	return nil
}

func (collector GitHubWorkItemsRESTCollector) collectPullRequests(
	ctx context.Context,
	claim Claim,
	client *providerfoundation.HTTPClient,
	root string,
	normalizedAt time.Time,
	result *GitHubWorkItemsRESTResult,
) error {
	query := url.Values{
		"state": {"all"}, "sort": {"updated"}, "direction": {"desc"},
		"per_page": {"100"},
	}
	page, err := providerfoundation.CollectGitHubLinkPages(
		ctx, client, providerfoundation.GitHubPageOptions{
			Path: root + "/pulls", Query: query, MaxPages: collector.maxPages(),
			StopAt: func(raw json.RawMessage) bool {
				return pullCrossedSinceBoundary(raw, claim)
			},
		},
	)
	result.addPageEvidence(page)
	if err != nil {
		return err
	}
	if page.PageBudgetExhausted {
		return ErrPaginationCapExceeded
	}
	numbers, err := filterGitHubPullWindow(page.Items, claim)
	if err != nil {
		return err
	}
	for _, number := range numbers {
		var raw json.RawMessage
		if err := fetchObject(
			ctx, client, root+"/pulls/"+strconv.Itoa(number), &raw,
		); err != nil {
			return err
		}
		var pull githubPullRequestWorkItemPayload
		if json.Unmarshal(raw, &pull) != nil || pull.Number != number ||
			strings.TrimSpace(pull.Title) == "" {
			return providerfoundation.ErrNormalizationInvalid
		}
		createdAt := parseGitHubWorkItemTime(pull.CreatedAt)
		if createdAt == nil {
			fallback := normalizedAt.UTC()
			createdAt = &fallback
		}
		result.PullRequests = append(result.PullRequests, GitHubWorkItemsRESTPullRequest{
			Number: number, CreatedAt: createdAt.UTC(),
			Payload: append(json.RawMessage(nil), raw...),
		})
	}
	return nil
}

func (collector GitHubWorkItemsRESTCollector) maxPages() int {
	if collector.MaxPages == 0 {
		return nativeMaxPages
	}
	return collector.MaxPages
}

func collectGitHubWorkItemChildPages(
	ctx context.Context,
	client *providerfoundation.HTTPClient,
	path string,
	maxPages int,
	itemLimit int,
) ([]json.RawMessage, providerfoundation.PageCollection, error) {
	// MaxItems stops on the exact provider iterator limit without fetching a
	// speculative next page. MaxPages remains the fail-closed traversal bound;
	// reaching it with a rel=next is never relabeled as successful partial data.
	page, err := providerfoundation.CollectGitHubLinkPages(
		ctx, client, providerfoundation.GitHubPageOptions{
			Path: path, Query: url.Values{"per_page": {"100"}},
			MaxPages: maxPages, MaxItems: itemLimit,
		},
	)
	if err != nil || page.PageBudgetExhausted {
		return page.Items, page, err
	}
	return page.Items, page, nil
}

func githubWorkItemsRESTOptionsForClaim(claim Claim) (githubWorkItemsRESTOptions, error) {
	options := githubWorkItemsRESTOptions{
		includeIssues:       true,
		includePullRequests: claim.ProcessorFlags["sync_prs"],
		fetchComments:       true,
		fetchMilestones:     true,
		commentsLimit:       githubWorkItemCommentLimit,
	}
	for name, target := range map[string]*bool{
		"include_issues":        &options.includeIssues,
		"include_pull_requests": &options.includePullRequests,
		"fetch_comments":        &options.fetchComments,
		"fetch_milestones":      &options.fetchMilestones,
	} {
		value, present := claim.DatasetOptions[name]
		if !present || value == nil {
			continue
		}
		parsed, ok := value.(bool)
		if !ok {
			return githubWorkItemsRESTOptions{}, ErrInvalidConfiguration
		}
		*target = parsed
	}
	commentsLimit, err := githubWorkItemsRESTNonNegativeIntOption(
		claim.DatasetOptions, "comments_limit", githubWorkItemCommentLimit,
	)
	if err != nil {
		return githubWorkItemsRESTOptions{}, err
	}
	options.commentsLimit = commentsLimit
	return options, nil
}

func githubWorkItemsRESTNonNegativeIntOption(
	options map[string]any,
	name string,
	fallback int,
) (int, error) {
	value, present := options[name]
	if !present || value == nil {
		return fallback, nil
	}
	var parsed int64
	switch typed := value.(type) {
	case int:
		parsed = int64(typed)
	case int64:
		parsed = typed
	case float64:
		if math.IsNaN(typed) || math.IsInf(typed, 0) || math.Trunc(typed) != typed ||
			typed < 0 || typed > float64(math.MaxInt) {
			return 0, ErrInvalidConfiguration
		}
		parsed = int64(typed)
	case json.Number:
		var err error
		parsed, err = strconv.ParseInt(string(typed), 10, 64)
		if err != nil {
			return 0, ErrInvalidConfiguration
		}
	default:
		return 0, ErrInvalidConfiguration
	}
	if parsed < 0 || uint64(parsed) > uint64(math.MaxInt) {
		return 0, ErrInvalidConfiguration
	}
	return int(parsed), nil
}

func githubWorkItemsRESTUpdatedAfterBefore(value *string, before *time.Time) bool {
	if before == nil {
		return false
	}
	updatedAt := parseGitHubWorkItemTime(value)
	return updatedAt != nil && updatedAt.After(before.UTC())
}

func githubWorkItemsRESTOptionalFailure(
	ctx context.Context,
	client *providerfoundation.HTTPClient,
	err error,
) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, providerfoundation.ErrLeaseLost) ||
		errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) ||
		isRateLimited(err) || ctx.Err() != nil {
		return err
	}
	if client == nil || client.Lease == nil {
		return ErrInvalidConfiguration
	}
	if leaseErr := client.Lease.Assert(ctx); leaseErr != nil {
		return leaseErr
	}
	return nil
}

func (result *GitHubWorkItemsRESTResult) addPageEvidence(
	page providerfoundation.PageCollection,
) {
	result.Evidence.Pages += page.Pages
	result.Evidence.CapReached = result.Evidence.CapReached || page.PageBudgetExhausted
}

func (result *GitHubWorkItemsRESTResult) addIncomplete(
	component string,
	subjectID string,
	err error,
) {
	result.Incomplete = append(result.Incomplete, GitHubWorkItemsRESTIncomplete{
		Component: component, SubjectID: subjectID,
		Cause: githubWorkItemsRESTFailureCause(err),
	})
}

func githubWorkItemsRESTFailureCause(err error) string {
	var providerErr *providerfoundation.ProviderError
	if errors.As(err, &providerErr) && providerErr != nil {
		return string(providerErr.Class)
	}
	return "invalid_response"
}

func emptyGitHubWorkItemRows() githubWorkItemRows {
	return githubWorkItemRows{
		WorkItems:         []githubWorkItemRow{},
		StatusTransitions: []githubWorkItemTransitionRow{},
		Dependencies:      []githubWorkItemDependencyRow{},
		ReopenEvents:      []githubWorkItemReopenRow{},
		Interactions:      []githubWorkItemInteractionRow{},
		Sprints:           []githubSprintRow{},
		AIAttributions:    []githubAIAttributionRow{},
	}
}

func appendGitHubWorkItemRows(target *githubWorkItemRows, source githubWorkItemRows) {
	target.WorkItems = append(target.WorkItems, source.WorkItems...)
	target.StatusTransitions = append(target.StatusTransitions, source.StatusTransitions...)
	target.Dependencies = append(target.Dependencies, source.Dependencies...)
	target.ReopenEvents = append(target.ReopenEvents, source.ReopenEvents...)
	target.Interactions = append(target.Interactions, source.Interactions...)
	target.Sprints = append(target.Sprints, source.Sprints...)
	target.AIAttributions = append(target.AIAttributions, source.AIAttributions...)
}
