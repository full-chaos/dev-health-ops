package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
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
	// githubWorkItemEventLimit is the Python producer's issue-event cap, kept
	// ONLY because the frozen selection oracle records it. No production path
	// applies it any more: issue and PR events are paged to the end under the
	// shared page bound and fail closed past it (CHAOS-8770).
	githubWorkItemEventLimit = 1000
	// githubWorkItemCommentLimit is the DEFAULT of the user option
	// comments_limit. The option stays an explicit, user-set cap; a cut it
	// makes is counted and logged, never silent (CHAOS-8770).
	githubWorkItemCommentLimit = 500
	// githubWorkItemEventsUnbounded asks the PR-social fetch for every timeline
	// event: no item cap, only the page bound.
	githubWorkItemEventsUnbounded = 1<<31 - 1
)

// githubWorkItemNestedBoundExceeded reports a nested list that still had a next
// page after the page bound. It names the repository, the owner item and the
// field, logs at ERROR, and wraps ErrPaginationCapExceeded so the unit fails
// loudly. It carries no payload text and no credential.
func githubWorkItemNestedBoundExceeded(repository string, number int, field string, pages int) error {
	slog.Error("providersync.github.nested_list_bound_exceeded",
		"repository", repository, "issue", number, "field", field, "pages", pages)
	return fmt.Errorf(
		"%w: github %s of %s#%d still had a next page after %d pages",
		ErrPaginationCapExceeded, field, repository, number, pages,
	)
}

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
	// CommentsTruncated counts issues whose comments the user's comments_limit
	// cut while more comments existed. A named, counted limit: it holds no
	// watermark and never reads as complete.
	CommentsTruncated int
}

// NoOptionalDegradation reports only whether this REST layer observed an
// optional milestone or issue-comment degradation. It is deliberately not a
// composite-completion signal: every PullRequests entry still awaits the
// GraphQL social fetch before the GitHub work-item producer can be complete.
func (result GitHubWorkItemsRESTResult) NoOptionalDegradation() bool {
	return len(result.Incomplete) == 0
}

// GitHubWorkItemsRESTCollector builds the REST half of GitHub's composite
// work-item producer, and that composition has HAPPENED.
//
// WIRING: WIRED, as a FIELD rather than by direct assignment --
// github_work_items_route.go:182-183 declares
// `REST GitHubWorkItemsRESTCollector` on GitHubWorkItemsRouteHandler, which
// internal/workerservice/provider_sync.go's
// `provider == "github" && dataset == "work-items"` case constructs and assigns
// to routeHandler alongside NewGitHubWorkItemClickHouseEffects.
//
// WIRED IS NOT EXECUTING, and this comment claims only the former. Planning
// admits a canonical work-items unit only when the descriptor satisfies
// RouteReady AND Plannable AND ExecutedProofSatisfied
// (internal/scheduler/sync/planner.go:401-423); an attempted-but-unproven
// executed-proof state returns `PlannedUnit{}, false` before BuildExecutor or
// Execute is reached -- the condition that silently vetoed github/work-items
// for two months under CHAOS-4060/CHAOS-4731.
//
// SUPERSEDED: "It remains intentionally unregistered until the PR GraphQL
// SUPERSEDED: social layer and every direct/derived effect are composed
// SUPERSEDED: atomically."
//
// Both preconditions are met: the Social fetcher is a sibling field on the same
// handler and the derived effects are wired. Kept visible as a correction
// rather than deleted (CHAOS-4848).
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
	if err := fetchObjectRoute(ctx, &counted, githubRepoRoute, root, &repo); err != nil {
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
				Route:    githubMilestonesRoute,
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
	// The list is ordered by update, oldest first, and `since` is its only server-side bound, so the reader
	// stops at the first issue updated at or after the window end: every later one is newer and belongs to a
	// later window. In GitHub's default order (created, newest first) the end could not be found without
	// paging every issue updated since the window start, which trips the page cap on a large repository
	// (CHAOS-10112). Python read the default order and skipped; this is a declared request-order divergence.
	//
	// An update-ordered list is MUTABLE while it is read: an issue edited meanwhile moves to the end and
	// shifts every later issue up by one place, so following page numbers would skip the issue that crossed
	// a page boundary. The reader therefore pages by key, not by number: each request asks for one page
	// since the update time of the last issue it saw (inclusive, so that issue repeats), and an issue read
	// twice keeps its latest reading. An issue that moved forward reappears later and is not lost. Only a
	// tie (a page whose last issue was updated at the very `since` it was asked for, a bulk edit inside one
	// second) is continued by page number within that `since`.
	issues, pages, stoppedAtWindowEnd, capReached, err := collector.listIssuesByUpdate(ctx, claim, client, root, result)
	slog.Default().InfoContext(ctx, "github_work_items_issue_list",
		"org_id", claim.OrgID, "unit_id", claim.ID, "source_id", claim.SourceID,
		"window_since", issueListBound(claim.SinceAt), "window_before", issueListBound(claim.BeforeAt),
		"pages", pages, "stopped_at_window_end", stoppedAtWindowEnd)
	if stoppedAtWindowEnd {
		client.Metrics.RecordGitHubIssueListStopped()
	}
	if capReached {
		slog.Default().WarnContext(ctx, "github_work_items_issue_list_cap_exceeded",
			"org_id", claim.OrgID, "unit_id", claim.ID, "source_id", claim.SourceID,
			"window_since", issueListBound(claim.SinceAt), "window_before", issueListBound(claim.BeforeAt),
			"pages", pages, "page_cap", collector.maxPages())
		client.Metrics.RecordGitHubIssueListCapExceeded()
	}
	if err != nil {
		return err
	}
	if capReached {
		result.Evidence.CapReached = true
		return ErrPaginationCapExceeded
	}
	for _, raw := range issues {
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
			ctx, client, githubIssueEventsRoute, root+"/issues/"+strconv.Itoa(listed.Number)+"/events",
			collector.maxPages(), 0,
		)
		result.addPageEvidence(eventPage)
		if err != nil {
			return err
		}
		if eventPage.PageBudgetExhausted {
			return githubWorkItemNestedBoundExceeded(result.RepoFullName, listed.Number, "events", eventPage.Pages)
		}
		comments := []json.RawMessage{}
		if options.fetchComments && options.commentsLimit > 0 {
			commentRows, commentPage, commentErr := collectGitHubWorkItemChildPages(
				ctx, client, githubIssueCommentRoute, root+"/issues/"+strconv.Itoa(listed.Number)+"/comments",
				// One row past the limit tells a real cut from a list that holds
				// exactly the limit.
				collector.maxPages(), githubWorkItemCommentLookahead(options.commentsLimit),
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
				return githubWorkItemNestedBoundExceeded(result.RepoFullName, listed.Number, "comments", commentPage.Pages)
			default:
				comments = commentRows
				if len(comments) > options.commentsLimit {
					comments = comments[:options.commentsLimit]
					result.CommentsTruncated++
					slog.Warn("providersync.github.issue_comments_truncated_by_option",
						"repository", result.RepoFullName, "issue", listed.Number,
						"comments_limit", options.commentsLimit)
				}
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
			Path: root + "/pulls", Route: githubPullsRoute, Query: query, MaxPages: collector.maxPages(),
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
		if err := fetchObjectRoute(
			ctx, client, githubPullRoute, root+"/pulls/"+strconv.Itoa(number), &raw,
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
	route, path string,
	maxPages int,
	itemLimit int,
) ([]json.RawMessage, providerfoundation.PageCollection, error) {
	// MaxItems stops on the exact provider iterator limit without fetching a
	// speculative next page. MaxPages remains the fail-closed traversal bound;
	// reaching it with a rel=next is never relabeled as successful partial data.
	page, err := providerfoundation.CollectGitHubLinkPages(
		ctx, client, providerfoundation.GitHubPageOptions{
			Path: path, Route: route, Query: url.Values{"per_page": {"100"}},
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
	// Half-open window [since, before): an issue updated exactly at the end belongs to the next window.
	return updatedAt != nil && !updatedAt.Before(before.UTC())
}

// issueListCrossedWindowEnd is the StopAt predicate of the issue list: the first item updated at or after
// the window end ends the read (the list is ordered by update, oldest first).
func issueListCrossedWindowEnd(raw json.RawMessage, claim Claim) bool {
	var listed githubWorkItemsRESTListIssue
	if json.Unmarshal(raw, &listed) != nil {
		return false
	}
	return githubWorkItemsRESTUpdatedAfterBefore(listed.UpdatedAt, claim.BeforeAt)
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

// githubWorkItemCommentLookahead is the comment limit plus one row, saturating:
// the extra row tells a real cut from a list that holds exactly the limit.
func githubWorkItemCommentLookahead(limit int) int {
	if limit >= math.MaxInt {
		return limit
	}
	return limit + 1
}

func issueListBound(bound *time.Time) string {
	if bound == nil {
		return ""
	}
	return bound.UTC().Format(time.RFC3339Nano)
}

// listIssuesByUpdate reads the issue list described in collectIssues: one request per step, advancing the
// `since` key to the update time of the last issue read, de-duplicating by issue number (latest reading
// wins), stopping at the first issue updated at or after the window end or when the provider names no next
// page. The page cap bounds the number of requests; running out of it is reported, never truncated.
func (collector GitHubWorkItemsRESTCollector) listIssuesByUpdate(
	ctx context.Context,
	claim Claim,
	client *providerfoundation.HTTPClient,
	root string,
	result *GitHubWorkItemsRESTResult,
) (issues []json.RawMessage, pages int, stoppedAtWindowEnd, capReached bool, err error) {
	var since *time.Time
	if claim.SinceAt != nil {
		value := claim.SinceAt.UTC().Truncate(time.Second)
		since = &value
	}
	index := map[int]int{}
	page := 1
	for step := 0; ; step++ {
		if step >= collector.maxPages() {
			return issues, pages, stoppedAtWindowEnd, true, nil
		}
		query := url.Values{"state": {"all"}, "per_page": {"100"}, "sort": {"updated"}, "direction": {"asc"}}
		if since != nil {
			query.Set("since", since.Format(time.RFC3339))
		}
		if page > 1 {
			query.Set("page", strconv.Itoa(page))
		}
		collected, collectErr := providerfoundation.CollectGitHubLinkPages(
			ctx, client, providerfoundation.GitHubPageOptions{
				Path: root + "/issues", Route: githubIssuesRoute, Query: query, MaxPages: 1,
				StopAt: func(raw json.RawMessage) bool {
					if issueListCrossedWindowEnd(raw, claim) {
						stoppedAtWindowEnd = true
						return true
					}
					return false
				},
			},
		)
		// MaxPages is 1 on purpose: one request per step. A next page left over (reported as an exhausted
		// budget) is the expected state: it means the provider named a next page.
		moreAfterThisPage := collected.PageBudgetExhausted
		collected.PageBudgetExhausted = false
		result.addPageEvidence(collected)
		pages += collected.Pages
		if collectErr != nil {
			return issues, pages, stoppedAtWindowEnd, false, collectErr
		}
		var lastUpdated *time.Time
		for _, raw := range collected.Items {
			var listed githubWorkItemsRESTListIssue
			if json.Unmarshal(raw, &listed) != nil || listed.Number < 1 {
				return issues, pages, stoppedAtWindowEnd, false, providerfoundation.ErrNormalizationInvalid
			}
			if position, seen := index[listed.Number]; seen {
				issues[position] = raw
			} else {
				index[listed.Number] = len(issues)
				issues = append(issues, raw)
			}
			lastUpdated = parseGitHubWorkItemTime(listed.UpdatedAt)
		}
		if stoppedAtWindowEnd || !moreAfterThisPage {
			return issues, pages, stoppedAtWindowEnd, false, nil
		}
		if lastUpdated != nil && (since == nil || lastUpdated.UTC().Truncate(time.Second).After(*since)) {
			next := lastUpdated.UTC().Truncate(time.Second)
			since, page = &next, 1
		} else {
			page++
		}
	}
}
