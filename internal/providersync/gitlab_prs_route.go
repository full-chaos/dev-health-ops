package providersync

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

const (
	gitLabPullRequestDefaultPerPage  = 100
	gitLabPullRequestDefaultMaxPages = 10_000
)

// GitLabPullRequestRouteHandler is the complete native producer for the
// GitLab PR-social family. GitLab calls these records merge requests, but the
// persisted contract is the provider-neutral git_pull_requests and
// git_pull_request_reviews pair. Python's _sync_gitlab_mrs_to_store runs the
// list, review enrichment, and both writes as one unit regardless of whether
// prs, pr-reviews, or pr-comments selected the unit; this handler preserves
// that boundary and deliberately emits both effects for each alias.
//
// The handler is constructible and fully tested but is not registered in the
// scheduler/descriptor matrix by this slice. Activation is a separate,
// atomic routing change so no one of the three aliases can become a second
// writer for a ReplacingMergeTree row.
type GitLabPullRequestRouteHandler struct {
	PerPage  int
	MaxPages int
}

// gitLabMergeRequestPayload contains the fields consumed by the live Python
// producer. Provider JSON is decoded through any-valued fields where the
// Python path stringifies or integer-coerces values at the point of use.
type gitLabMergeRequestPayload struct {
	IID            any            `json:"iid"`
	Title          any            `json:"title"`
	Description    any            `json:"description"`
	State          any            `json:"state"`
	Author         map[string]any `json:"author"`
	CreatedAt      any            `json:"created_at"`
	UpdatedAt      any            `json:"updated_at"`
	MergedAt       any            `json:"merged_at"`
	ClosedAt       any            `json:"closed_at"`
	SourceBranch   any            `json:"source_branch"`
	TargetBranch   any            `json:"target_branch"`
	UserNotesCount any            `json:"user_notes_count"`
}

type gitLabMergeRequestReviewFetch struct {
	Rows                  []pullRequestReviewRow
	FirstReviewAt         *time.Time
	ChangesRequestedCount int
	Pages                 int
}

// ErrGitLabPullRequestReviewsIncomplete is returned when the authoritative
// MR notes stream cannot be fetched completely. Python flushes healthy rows
// but raises PartialGitLabMrSyncError before reporting success in this case;
// the Go complete route likewise returns no batch/watermark, preventing a
// retryable review gap from being silently acknowledged.
var ErrGitLabPullRequestReviewsIncomplete = errors.New("gitlab pull request reviews incomplete")

func isPRSocialDataset(dataset string) bool {
	switch dataset {
	case "prs", "pr-reviews", "pr-comments":
		return true
	default:
		return false
	}
}

func (handler GitLabPullRequestRouteHandler) limits() (int, int, error) {
	perPage := handler.PerPage
	if perPage == 0 {
		perPage = gitLabPullRequestDefaultPerPage
	}
	maxPages := handler.MaxPages
	if maxPages == 0 {
		maxPages = gitLabPullRequestDefaultMaxPages
	}
	if perPage < 1 || perPage > gitLabPullRequestDefaultPerPage || maxPages < 1 || maxPages > gitLabPullRequestDefaultMaxPages {
		return 0, 0, ErrInvalidConfiguration
	}
	return perPage, maxPages, nil
}

func (handler GitLabPullRequestRouteHandler) Collect(
	ctx context.Context,
	claim Claim,
	_ providerfoundation.Credential,
	client *providerfoundation.HTTPClient,
	normalizedAt time.Time,
) (CompleteRouteBatch, error) {
	if ctx == nil || claim.Validate() != nil || claim.Provider != "gitlab" ||
		!isPRSocialDataset(claim.Dataset) || client == nil ||
		client.Provider != "gitlab" || client.BaseURL == nil || normalizedAt.IsZero() {
		return CompleteRouteBatch{}, ErrInvalidConfiguration
	}
	perPage, maxPages, err := handler.limits()
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	projectID, err := gitLabProjectID(claim.SourceExternalID)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	normalizedAt = normalizedAt.UTC().Truncate(time.Millisecond)
	requests := 0
	counted := *client
	counted.Doer = gitLabPullRequestCountingDoer{delegate: client.Doer, attempts: &requests}
	root := providerRelativePath(&counted, "api", "v4", "projects", projectID)
	var project repositoryPayload
	if err := fetchObject(ctx, &counted, root, &project); err != nil {
		return CompleteRouteBatch{}, err
	}
	parsedProjectID, err := project.ID.Int64()
	if err != nil || parsedProjectID < 1 || strconv.FormatInt(parsedProjectID, 10) != projectID {
		return CompleteRouteBatch{}, providerfoundation.ErrNormalizationInvalid
	}
	fullName := gitLabProjectFullName(project)
	repoID, err := repositoryIdentity(fullName)
	if err != nil {
		return CompleteRouteBatch{}, err
	}

	rows := make([]pullRequestRow, 0)
	reviews := make([]pullRequestReviewRow, 0)
	pages := 0
	consume := func(raw json.RawMessage) error {
		var payload gitLabMergeRequestPayload
		if err := decodeGitLabMergeRequest(raw, &payload); err != nil {
			return err
		}
		updatedAt := parseGitLabPullTime(payload.UpdatedAt)
		if claim.BeforeAt != nil && updatedAt != nil && updatedAt.After(claim.BeforeAt.UTC()) {
			return nil
		}
		iid, err := gitLabPullRequestIID(payload.IID)
		if err != nil {
			return err
		}
		createdAt := parseGitLabPullTime(payload.CreatedAt)
		mergedAt := parseGitLabPullTime(payload.MergedAt)
		closedAt := parseGitLabPullTime(payload.ClosedAt)
		reviewFetch, err := fetchGitLabMergeRequestReviews(
			ctx, &counted, claim, projectID, iid, payload, repoID, createdAt,
			normalizedAt, perPage, maxPages,
		)
		if err != nil {
			return err
		}
		pages += reviewFetch.Pages
		comments, err := gitLabPullRequestInt(payload.UserNotesCount)
		if err != nil {
			return err
		}
		row, err := normalizeGitLabPullRequest(
			claim, repoID, payload, createdAt, mergedAt, closedAt,
			reviewFetch, comments, normalizedAt,
		)
		if err != nil {
			return err
		}
		rows = append(rows, row)
		reviews = append(reviews, reviewFetch.Rows...)
		return nil
	}
	_, listPages, capReached, err := collectGitLabPullRequestPages(
		ctx, &counted, root+"/merge_requests", claim, perPage, maxPages, consume,
	)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	if capReached {
		return CompleteRouteBatch{}, ErrPaginationCapExceeded
	}
	pages += listPages
	// Review enrichment pages are accumulated above while consuming each MR;
	// listPages is kept separate until the stream has proved it reached its
	// ordered end, so a failed notes request cannot accidentally return a
	// partial batch or watermark.

	prEffect, err := effectBatchFromValues(
		"git_pull_requests", EffectReadbackRequired, rows,
	)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	reviewEffect, err := effectBatchFromValues(
		"git_pull_request_reviews", EffectReadbackRequired, reviews,
	)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	return CompleteRouteBatch{
		Effects: []EffectBatch{prEffect, reviewEffect},
		Result: map[string]any{
			"prs_synced":        len(rows),
			"pr_reviews_synced": len(reviews),
			"repo":              fullName,
			"project_id":        parsedProjectID,
		},
		Watermark: claim.BeforeAt,
		Evidence: FetchEvidence{
			Provider: claim.Provider, Dataset: claim.Dataset,
			Requests: requests, Pages: pages, Records: len(rows) + len(reviews),
		},
	}, nil
}

type gitLabPullRequestCountingDoer struct {
	delegate providerfoundation.HTTPDoer
	attempts *int
}

func (doer gitLabPullRequestCountingDoer) Do(request *http.Request) (*http.Response, error) {
	attempts := *doer.attempts + 1
	*doer.attempts = attempts
	return doer.delegate.Do(request)
}

func decodeGitLabMergeRequest(raw json.RawMessage, payload *gitLabMergeRequestPayload) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(payload); err != nil {
		return providerfoundation.ErrNormalizationInvalid
	}
	if decoder.Decode(&struct{}{}) != io.EOF {
		return providerfoundation.ErrNormalizationInvalid
	}
	return nil
}

func collectGitLabPullRequestPages(
	ctx context.Context,
	client *providerfoundation.HTTPClient,
	path string,
	claim Claim,
	perPage, maxPages int,
	consume func(json.RawMessage) error,
) ([]json.RawMessage, int, bool, error) {
	query := url.Values{"state": {"all"}, "order_by": {"updated_at"}, "sort": {"desc"}}
	items, pages, capped, err := collectGitLabRawPages(
		ctx, client, path, query, perPage, maxPages,
		func(raw json.RawMessage) bool {
			if claim.SinceAt == nil {
				return false
			}
			var value map[string]any
			decoder := json.NewDecoder(bytes.NewReader(raw))
			decoder.UseNumber()
			if decoder.Decode(&value) != nil {
				return false
			}
			updatedAt := parseGitLabPullTime(value["updated_at"])
			return updatedAt != nil && updatedAt.Before(claim.SinceAt.UTC())
		}, consume,
	)
	return items, pages, capped, err
}

func collectGitLabRawPages(
	ctx context.Context,
	client *providerfoundation.HTTPClient,
	path string,
	baseQuery url.Values,
	perPage, maxPages int,
	stopAt func(json.RawMessage) bool,
	consume func(json.RawMessage) error,
) ([]json.RawMessage, int, bool, error) {
	items := make([]json.RawMessage, 0)
	page := 1
	pages := 0
	for page > 0 {
		if pages >= maxPages {
			return items, pages, true, nil
		}
		query := cloneGitLabPullRequestValues(baseQuery)
		query.Set("page", strconv.Itoa(page))
		query.Set("per_page", strconv.Itoa(perPage))
		response, err := client.Do(ctx, http.MethodGet, path+"?"+query.Encode(), nil)
		if err != nil {
			return nil, pages, false, err
		}
		pageItems, err := decodeGitLabPullRequestPage(response)
		if err != nil {
			return nil, pages, false, err
		}
		pages++
		if len(pageItems) == 0 {
			return items, pages, false, nil
		}
		for _, item := range pageItems {
			if stopAt != nil && stopAt(item) {
				return items, pages, false, nil
			}
			if consume != nil {
				if err := consume(item); err != nil {
					return nil, pages, false, err
				}
			}
			items = append(items, item)
		}
		next := strings.TrimSpace(response.Header.Get("X-Next-Page"))
		if next != "" {
			nextPage, parseErr := strconv.Atoi(next)
			if parseErr != nil || nextPage < 1 {
				return items, pages, false, nil
			}
			page = nextPage
			continue
		}
		if len(pageItems) < perPage {
			return items, pages, false, nil
		}
		page++
	}
	return items, pages, false, nil
}

func cloneGitLabPullRequestValues(source url.Values) url.Values {
	clone := make(url.Values, len(source))
	for key, values := range source {
		clone[key] = append([]string(nil), values...)
	}
	return clone
}

func decodeGitLabPullRequestPage(response *http.Response) ([]json.RawMessage, error) {
	if response == nil || response.Body == nil {
		return nil, providerfoundation.ErrNormalizationInvalid
	}
	defer response.Body.Close()
	decoder := json.NewDecoder(io.LimitReader(response.Body, (32<<20)+1))
	decoder.UseNumber()
	var items []json.RawMessage
	if err := decoder.Decode(&items); err != nil || items == nil {
		return nil, providerfoundation.ErrNormalizationInvalid
	}
	if decoder.Decode(&struct{}{}) != io.EOF {
		return nil, providerfoundation.ErrNormalizationInvalid
	}
	return items, nil
}

func gitLabPullRequestIID(value any) (int, error) {
	parsed, err := strconv.Atoi(strings.TrimSpace(stringValue(value)))
	if err != nil || parsed < 1 {
		return 0, providerfoundation.ErrNormalizationInvalid
	}
	return parsed, nil
}

func gitLabPullRequestInt(value any) (int, error) {
	if value == nil {
		return 0, nil
	}
	parsed, err := strconv.Atoi(strings.TrimSpace(stringValue(value)))
	if err != nil || parsed < 0 {
		return 0, providerfoundation.ErrNormalizationInvalid
	}
	return parsed, nil
}

func parseGitLabPullTime(value any) *time.Time {
	text := stringValue(value)
	if text == "" {
		return nil
	}
	parsed, err := time.Parse(time.RFC3339Nano, text)
	if err != nil {
		return nil
	}
	parsed = parsed.UTC().Truncate(time.Millisecond)
	return &parsed
}

func optionalGitLabPullString(value any) *string {
	if value == nil {
		return nil
	}
	text := stringValue(value)
	return &text
}

func optionalGitLabPullTitle(value any) *string {
	text := stringValue(value)
	if text == "" {
		return nil
	}
	return &text
}

func normalizeGitLabPullRequest(
	claim Claim,
	repoID string,
	payload gitLabMergeRequestPayload,
	createdAt, mergedAt, closedAt *time.Time,
	review gitLabMergeRequestReviewFetch,
	comments int,
	normalizedAt time.Time,
) (pullRequestRow, error) {
	iid, err := gitLabPullRequestIID(payload.IID)
	if err != nil {
		return pullRequestRow{}, err
	}
	created := resolveCreatedAt(createdAt, mergedAt, closedAt, normalizedAt)
	author := optionalGitLabPullString(payload.Author["username"])
	authorName := "Unknown"
	if author != nil {
		authorName = *author
	}
	row := pullRequestRow{
		RepoID: repoID, Number: iid, Title: optionalGitLabPullTitle(payload.Title),
		Body:       optionalGitLabPullString(payload.Description),
		State:      normalizePRState(stringValue(payload.State), mergedAt),
		AuthorName: authorName, AuthorEmail: nil, CreatedAt: created.UTC(),
		MergedAt: mergedAt, ClosedAt: closedAt,
		HeadBranch:            optionalGitLabPullString(payload.SourceBranch),
		BaseBranch:            optionalGitLabPullString(payload.TargetBranch),
		FirstReviewAt:         review.FirstReviewAt,
		ChangesRequestedCount: review.ChangesRequestedCount,
		ReviewsCount:          len(review.Rows), CommentsCount: comments,
		LastSynced: normalizedAt, OrgID: claim.OrgID,
	}
	if err := row.validate(claim); err != nil {
		return pullRequestRow{}, err
	}
	return row, nil
}

func fetchGitLabMergeRequestReviews(
	ctx context.Context,
	client *providerfoundation.HTTPClient,
	claim Claim,
	projectID string,
	iid int,
	payload gitLabMergeRequestPayload,
	repoID string,
	createdAt *time.Time,
	normalizedAt time.Time,
	perPage, maxPages int,
) (gitLabMergeRequestReviewFetch, error) {
	result := gitLabMergeRequestReviewFetch{}
	var approvals map[string]any
	approvalPath := providerRelativePath(client, "api", "v4", "projects", projectID, "merge_requests", strconv.Itoa(iid), "approvals")
	if err := fetchObject(ctx, client, approvalPath, &approvals); err != nil {
		if controlErr := gitLabPullRequestControlError(ctx, err); controlErr != nil {
			return result, controlErr
		}
		// Python treats approvals as optional enrichment; the notes stream is
		// authoritative and still determines whether the MR is known.
		approvals = nil
	}
	notesPath := providerRelativePath(client, "api", "v4", "projects", projectID, "merge_requests", strconv.Itoa(iid), "notes")
	notes, pages, capReached, err := collectGitLabRawPages(
		ctx, client, notesPath,
		url.Values{"sort": {"asc"}, "order_by": {"created_at"}},
		perPage, maxPages, nil, nil,
	)
	if err != nil {
		if controlErr := gitLabPullRequestControlError(ctx, err); controlErr != nil {
			return result, controlErr
		}
		return result, ErrGitLabPullRequestReviewsIncomplete
	}
	if capReached {
		return result, ErrPaginationCapExceeded
	}
	result.Pages = pages
	rows, firstReviewAt, changesRequested := mapGitLabPullRequestReviews(
		claim, repoID, iid, approvals, notes, createdAt, normalizedAt,
		payload.Author,
	)
	result.Rows = rows
	result.FirstReviewAt = firstReviewAt
	result.ChangesRequestedCount = changesRequested
	return result, nil
}

func gitLabPullRequestControlError(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || isRateLimited(err) {
		return err
	}
	return nil
}

var _ CompleteRouteHandler = GitLabPullRequestRouteHandler{}
