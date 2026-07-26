package providersync

import (
	"bytes"
	"context"
	"encoding/json"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// pullRequestRow is the frozen `git_pull_requests` projection. Field order and
// JSON names mirror the Python ClickHouse sink
// (`ClickHouseStore.insert_git_pull_requests`) so the effect digest is stable
// and the row is byte-comparable during live parity.
//
// Four fields are intentionally always zero-valued here:
// first_review_at, reviews_count, and changes_requested_count are populated
// in Python only by `_enrich_prs_with_reviews_batch`'s GraphQL review fetch
// (processors/github.py), which this handler does not perform -- that is
// github/pr-reviews' job (see deploy/go-workers/provider-sync-porting-recipe.md).
// first_comment_at is `None` unconditionally in Python's own
// `_collect_github_pr_objects`, so leaving it nil here is exact parity, not a
// gap.
type pullRequestRow struct {
	RepoID                string     `json:"repo_id"`
	Number                int        `json:"number"`
	Title                 *string    `json:"title"`
	Body                  *string    `json:"body"`
	State                 string     `json:"state"`
	AuthorName            string     `json:"author_name"`
	AuthorEmail           *string    `json:"author_email"`
	CreatedAt             time.Time  `json:"created_at"`
	MergedAt              *time.Time `json:"merged_at"`
	ClosedAt              *time.Time `json:"closed_at"`
	HeadBranch            *string    `json:"head_branch"`
	BaseBranch            *string    `json:"base_branch"`
	Additions             int        `json:"additions"`
	Deletions             int        `json:"deletions"`
	ChangedFiles          int        `json:"changed_files"`
	FirstReviewAt         *time.Time `json:"first_review_at"`
	FirstCommentAt        *time.Time `json:"first_comment_at"`
	ChangesRequestedCount int        `json:"changes_requested_count"`
	ReviewsCount          int        `json:"reviews_count"`
	CommentsCount         int        `json:"comments_count"`
	LastSynced            time.Time  `json:"last_synced"`
	SourceID              *string    `json:"source_id"`
	OrgID                 string     `json:"org_id"`
}

// gitHubPullListItem is the subset of a GET /repos/{owner}/{repo}/pulls list
// item this handler reads: enough to compute the since/before window and the
// PR number to fetch in full. GitHub's list response does not include
// additions/deletions/changed_files/comments -- only the single-PR GET does
// (mirrors code_client.py's two-phase iter_pulls + get_pull_detail).
type gitHubPullListItem struct {
	Number    int    `json:"number"`
	UpdatedAt string `json:"updated_at"`
}

// gitHubPullDetailPayload is the GET /repos/{owner}/{repo}/pulls/{number}
// response fields this handler reads. Field names mirror
// providers/github/code_client.py::_pull_from_item.
type gitHubPullDetailPayload struct {
	ID           json.Number     `json:"id"`
	Number       int             `json:"number"`
	Title        *string         `json:"title"`
	Body         *string         `json:"body"`
	State        *string         `json:"state"`
	User         json.RawMessage `json:"user"`
	CreatedAt    *string         `json:"created_at"`
	UpdatedAt    *string         `json:"updated_at"`
	MergedAt     *string         `json:"merged_at"`
	ClosedAt     *string         `json:"closed_at"`
	Head         json.RawMessage `json:"head"`
	Base         json.RawMessage `json:"base"`
	Additions    int             `json:"additions"`
	Deletions    int             `json:"deletions"`
	ChangedFiles int             `json:"changed_files"`
	Comments     int             `json:"comments"`
}

type gitHubPullRef struct {
	Ref string `json:"ref"`
}

// GitHubPullRequestRouteHandler is the native Go complete-route handler for
// (github, prs). It mirrors `_collect_github_pr_objects` +
// `build_git_pull_request` + `normalize_pr_state`
// (src/dev_health_ops/processors/github.py, providers/pr_state.py): list
// every PR via GET .../pulls (state=all, sort=updated, direction=desc),
// fetch each PR's full detail via GET .../pulls/{number} for the fields the
// list endpoint omits, then filter to the claim's since/before window.
//
// Review-derived enrichment (first_review_at, reviews_count,
// changes_requested_count -- Python's `_enrich_prs_with_reviews_batch`, a
// GraphQL batch fetch) is NOT performed here; those fields are always their
// zero value. See the pullRequestRow doc comment and
// deploy/go-workers/provider-sync-porting-recipe.md.
type GitHubPullRequestRouteHandler struct {
	Now      func() time.Time
	MaxPages int
}

func (handler GitHubPullRequestRouteHandler) now() time.Time {
	if handler.Now != nil {
		return handler.Now().UTC()
	}
	return time.Now().UTC()
}

func (handler GitHubPullRequestRouteHandler) Collect(
	ctx context.Context,
	claim Claim,
	_ providerfoundation.Credential,
	client *providerfoundation.HTTPClient,
	normalizedAt time.Time,
) (CompleteRouteBatch, error) {
	if ctx == nil || claim.Validate() != nil || claim.Provider != "github" ||
		claim.Dataset != "prs" || client == nil || client.Provider != "github" ||
		client.BaseURL == nil || normalizedAt.IsZero() {
		return CompleteRouteBatch{}, ErrInvalidConfiguration
	}
	normalizedAt = normalizedAt.UTC().Truncate(time.Millisecond)
	owner, repository, err := splitGitHubRepository(claim.SourceExternalID)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	root := providerRelativePath(client, "repos", owner, repository)

	// repo_id is derived from the repository's API-reported full_name, not
	// claim.SourceExternalID: Python's db_repo.id = get_repo_uuid_from_repo
	// (repo_info.full_name) (models/git.py Repo.__init__, driven by
	// processors/github.py::process_github_repo's repo-info fetch). This is
	// the identical repositoryIdentity call GitHubRepositoryRouteHandler
	// makes for (github, repo-metadata).
	//
	// codex H4: a blank full_name MUST fail closed, not fall back to
	// claim.SourceExternalID. Python's get_repo_uuid_from_repo raises
	// ValueError("repo identifier is required") on a falsy repo string, and
	// models/git.py::Repo.__init__ only attempts that call `if
	// repo_identifier:` -- a blank full_name skips it entirely, leaving
	// `id` unset, and every downstream ClickHouse insert
	// (_normalize_uuid(repo.id)) then raises on the None. Python never
	// persists a PR under a guessed identity; falling back here would write
	// rows keyed on a repo_id nothing else in the system would derive.
	//
	// There is deliberately no separate `if fullName == ""` guard here: an
	// earlier version had one, and a mutation-harness run proved it dead --
	// repositoryIdentity already rejects an empty (or all-whitespace)
	// string with the same ErrNormalizationInvalid before it ever hashes
	// anything, so a second copy of that check only added an unreachable
	// branch for a mutation to hide behind. One fail-closed check, not two
	// that must be kept in sync.
	var repoPayload gitHubRepositoryPayload
	if err := fetchObject(ctx, client, root, &repoPayload); err != nil {
		return CompleteRouteBatch{}, err
	}
	repoID, err := repositoryIdentity(repoPayload.FullName)
	if err != nil {
		return CompleteRouteBatch{}, err
	}

	query := url.Values{
		"state": {"all"}, "sort": {"updated"}, "direction": {"desc"},
		"per_page": {"100"},
	}
	maxPages := handler.MaxPages
	if maxPages == 0 {
		maxPages = nativeMaxPages
	}
	page, err := providerfoundation.CollectGitHubLinkPages(
		ctx, client, providerfoundation.GitHubPageOptions{
			Path: root + "/pulls", Query: query, MaxPages: maxPages,
		},
	)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	// codex H2: Python's client.iter_pulls call is uncapped. A capped Go
	// fetch that still reports success and still lets Collect return
	// claim.BeforeAt as the watermark would silently and permanently lose
	// every PR past the cap -- the window never revisits them. Fail the
	// whole unit instead: never both capped and successful.
	if page.CapReached {
		return CompleteRouteBatch{}, ErrPaginationCapExceeded
	}
	listed, err := filterGitHubPullWindow(page.Items, claim)
	if err != nil {
		return CompleteRouteBatch{}, err
	}

	rows := make([]pullRequestRow, 0, len(listed))
	detailRequests := 0
	for _, number := range listed {
		var detail gitHubPullDetailPayload
		if err := fetchObject(
			ctx, client, root+"/pulls/"+strconv.Itoa(number), &detail,
		); err != nil {
			return CompleteRouteBatch{}, err
		}
		detailRequests++
		row, err := normalizeGitHubPullRequest(claim, repoID, detail, normalizedAt)
		if err != nil {
			return CompleteRouteBatch{}, err
		}
		rows = append(rows, row)
	}

	effect, err := effectBatchFromValues(
		"git_pull_requests", EffectReadbackRequired, rows,
	)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	watermark := claim.BeforeAt
	return CompleteRouteBatch{
		Effects: []EffectBatch{effect},
		Result: map[string]any{
			"prs_synced": len(rows),
			"repo":       repoPayload.FullName,
		},
		Watermark: watermark,
		Evidence: FetchEvidence{
			Provider: claim.Provider, Dataset: claim.Dataset,
			Requests:   page.Pages + 1 + detailRequests,
			Pages:      page.Pages,
			Records:    len(rows),
			CapReached: page.CapReached,
		},
	}, nil
}

// filterGitHubPullWindow applies the claim's since/before window to the
// listed PR numbers, client-side -- the same window
// `_collect_github_pr_objects` (processors/github.py) applies over
// `client.iter_pulls`'s results. GitHub's /pulls list endpoint has no
// server-side `since` parameter (unlike /issues), so this is the only
// filter, not a redundant belt-and-suspenders layer.
//
// codex H3: Python applies the since/until comparison ONLY when
// `updated_at` is a real `datetime` (`isinstance(updated_at, datetime)`);
// a missing, null, or unparseable value skips BOTH comparisons and the PR
// is unconditionally detail-fetched and included. An earlier version of
// this function dropped such items instead -- the "empty success" trap:
// a window of PRs that all have this shape silently reports zero records
// instead of including them, and nothing about that empty result looks
// wrong from the outside. windowKnown is split out as its own clause (not
// folded into the two comparisons) so each of the three conditions --
// "is the timestamp known at all", "is it before since", "is it after
// before" -- can be mutation-tested independently; see
// TestFilterGitHubPullWindowClauseCoverage.
func filterGitHubPullWindow(items []json.RawMessage, claim Claim) ([]int, error) {
	numbers := make([]int, 0, len(items))
	for _, raw := range items {
		var item gitHubPullListItem
		if json.Unmarshal(raw, &item) != nil || item.Number < 1 {
			return nil, providerfoundation.ErrNormalizationInvalid
		}
		if pullOutsideKnownWindow(firstTime(item.UpdatedAt), claim) {
			continue
		}
		numbers = append(numbers, item.Number)
	}
	return numbers, nil
}

// pullOutsideKnownWindow reports whether a PR must be excluded because its
// updated_at is both known and outside the claim's window. Each clause is
// intentionally a separate, named condition (not inlined into one compound
// boolean) so a mutation harness can kill each independently rather than
// only the disjunction as a whole.
func pullOutsideKnownWindow(updatedAt time.Time, claim Claim) bool {
	windowKnown := !updatedAt.IsZero()
	if !windowKnown {
		// Missing/null/unparseable updated_at: Python cannot compare it
		// either, so the item is unconditionally included (fetched and
		// stored), never excluded.
		return false
	}
	before := claim.SinceAt != nil && updatedAt.Before(claim.SinceAt.UTC())
	after := claim.BeforeAt != nil && updatedAt.After(claim.BeforeAt.UTC())
	return before || after
}

// normalizeGitHubPullRequest mirrors build_git_pull_request +
// normalize_pr_state (processors/base_git.py, providers/pr_state.py) exactly
// for the fields REST alone can populate. author_email is always nil: Python
// hardcodes `author_email = None` for the GitHub PR path
// (processors/github.py::_collect_github_pr_objects).
func normalizeGitHubPullRequest(
	claim Claim,
	repoID string,
	detail gitHubPullDetailPayload,
	normalizedAt time.Time,
) (pullRequestRow, error) {
	if detail.Number < 1 {
		return pullRequestRow{}, providerfoundation.ErrNormalizationInvalid
	}
	createdAt := parseGitHubPullTime(detail.CreatedAt)
	mergedAt := parseGitHubPullTime(detail.MergedAt)
	closedAt := parseGitHubPullTime(detail.ClosedAt)
	resolvedCreatedAt := resolveCreatedAt(createdAt, mergedAt, closedAt, normalizedAt)
	var rawState string
	if detail.State != nil {
		rawState = *detail.State
	}
	authorName := "Unknown"
	if login := gitHubPullUserLogin(detail.User); login != "" {
		authorName = login
	}
	row := pullRequestRow{
		RepoID: repoID, Number: detail.Number, Title: detail.Title,
		Body: detail.Body, State: normalizePRState(rawState, mergedAt),
		AuthorName: authorName, AuthorEmail: nil,
		CreatedAt: resolvedCreatedAt.UTC(), MergedAt: mergedAt, ClosedAt: closedAt,
		HeadBranch: gitHubPullRefName(detail.Head),
		BaseBranch: gitHubPullRefName(detail.Base),
		Additions:  detail.Additions, Deletions: detail.Deletions,
		ChangedFiles: detail.ChangedFiles,
		// FirstReviewAt/ReviewsCount/ChangesRequestedCount: see the
		// pullRequestRow doc comment -- not populated by this handler.
		CommentsCount: detail.Comments,
		LastSynced:    normalizedAt, OrgID: claim.OrgID,
	}
	if err := row.validate(claim); err != nil {
		return pullRequestRow{}, err
	}
	return row, nil
}

// resolveCreatedAt is a direct port of BaseGitProcessor.coerce_created_at
// (processors/base_git.py): created_at or merged_at or closed_at or now().
// created_at is NOT NULL in the ClickHouse schema. Extracted to its own
// function (rather than inlined in normalizeGitHubPullRequest) so
// TestGitHubPRSNormalizationMatchesLivePythonFunctions can call it directly
// against the live Python oracle's per-case output.
func resolveCreatedAt(
	createdAt, mergedAt, closedAt *time.Time,
	normalizedAt time.Time,
) time.Time {
	switch {
	case createdAt != nil:
		return *createdAt
	case mergedAt != nil:
		return *mergedAt
	case closedAt != nil:
		return *closedAt
	default:
		return normalizedAt
	}
}

// normalizePRState is a byte-for-byte port of providers/pr_state.py's
// normalize_pr_state: GitHub returns "closed" for both merged and unmerged
// PRs, so merged_at disambiguates.
//
// codex M7: Python's own operation is exactly `raw_state.strip().lower()` --
// strip leading/trailing whitespace ONLY (Python's default whitespace set,
// which includes \r), then lowercase. A prior version of this function
// stripped whitespace THROUGHOUT the string (turning "clo sed" into
// "closed", which Python would leave unmatched) and did not strip \r at all
// (leaving "closed\r" unmatched, which Python's strip() does match). Using
// strings.TrimSpace + strings.ToLower is deliberately no MORE aggressive
// than Python's operation, not just "also correct" for the cases tested.
func normalizePRState(rawState string, mergedAt *time.Time) string {
	if rawState == "" {
		return "open"
	}
	switch strings.ToLower(strings.TrimSpace(rawState)) {
	case "merged":
		return "merged"
	case "opened", "open":
		return "open"
	case "closed":
		if mergedAt != nil {
			return "merged"
		}
		return "closed"
	default:
		return "open"
	}
}

// parseGitHubPullTime parses a provider RFC3339 timestamp and truncates it
// to millisecond precision at construction time (codex M5): ClickHouse
// persists `DateTime64(3)`, so a provider timestamp with sub-millisecond
// precision would otherwise diverge from what a subsequent readback SELECT
// scans back -- a process death between WriteEffect's `Send` and the ledger
// CommitEffect would then compare, e.g., `.123` against the in-memory
// `.123456`, report EffectConflict on a row that is actually fine, and
// recovery could neither mark it committed nor safely replay it. Truncating
// here, once, at the point the value enters the row, is simpler and more
// robust than truncating at every comparison site downstream.
func parseGitHubPullTime(value *string) *time.Time {
	if value == nil || *value == "" {
		return nil
	}
	parsed, err := time.Parse(time.RFC3339Nano, *value)
	if err != nil {
		return nil
	}
	parsed = parsed.UTC().Truncate(time.Millisecond)
	return &parsed
}

func gitHubPullRefName(raw json.RawMessage) *string {
	if len(raw) == 0 {
		return nil
	}
	var ref gitHubPullRef
	if json.Unmarshal(raw, &ref) != nil || ref.Ref == "" {
		return nil
	}
	value := ref.Ref
	return &value
}

// gitHubPullUserLogin mirrors code_client.py::_pull_from_item's
// `str(user["login"]) if isinstance(user, Mapping) and user.get("login") is
// not None else None`: Python stringifies ANY non-null login value, not
// only a string one. codex M8: decoding directly into a Go `string` field
// makes json.Unmarshal fail (and this function return "") for a numeric
// login such as `{"login": 12345}`, silently losing attribution to the
// "Unknown" fallback where Python would have written "12345". Decoding into
// `any` (with UseNumber so a numeric login round-trips exactly, not through
// a lossy float64) and reusing the package's existing stringValue helper
// (native_rest.go) gets the same str()-like coercion Python's own
// str(user["login"]) performs.
func gitHubPullUserLogin(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var user struct {
		Login any `json:"login"`
	}
	if decoder.Decode(&user) != nil {
		return ""
	}
	return stringValue(user.Login)
}

func (row pullRequestRow) validate(claim Claim) error {
	if row.OrgID == "" || row.OrgID != claim.OrgID || row.RepoID == "" ||
		len(row.RepoID) != 36 || row.Number < 1 || row.State == "" ||
		row.CreatedAt.IsZero() || row.LastSynced.IsZero() {
		return providerfoundation.ErrInvalidScope
	}
	return nil
}

var _ CompleteRouteHandler = GitHubPullRequestRouteHandler{}
