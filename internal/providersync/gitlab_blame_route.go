package providersync

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

const (
	gitLabBlameMaxFiles       = 500
	gitLabBlameDefaultPerPage = 100
	gitLabBlameDefaultMaxPage = 10_000
)

var (
	ErrGitLabBlameTraversalFailed     = errors.New("gitlab blame traversal failed")
	ErrGitLabBlameIncomplete          = errors.New("gitlab blame inventory incomplete")
	ErrGitLabBlameCoverageUnavailable = errors.New("gitlab blame coverage unavailable")
)

// GitLabBlameCoverage is the durable selection boundary for the bounded
// backfill. Implementations must scope the returned paths by both org_id and
// repo_id; a path in another tenant is never evidence that this unit is done.
type GitLabBlameCoverage interface {
	BlamedPaths(context.Context, Claim, string) ([]string, error)
}

// GitLabBlameRouteHandler owns the active GitLab code-client blame backfill.
// It deliberately has no route-registry or matrix wiring: activation is a
// separate cutover concern. MaxFiles is test-configurable below the Python
// producer's ceiling, while page bounds mirror GitLabCodeClient.
type GitLabBlameRouteHandler struct {
	Coverage GitLabBlameCoverage
	PerPage  int
	MaxPages int
	MaxFiles int
}

// GitLabBlameResult is the concrete result contract for this provider pair.
// CompleteRouteBatch predates typed route results and exposes a map at the
// executor boundary; keeping the source result typed prevents a new key or an
// unstructured nested payload from becoming an accidental persistence/API
// contract.
type GitLabBlameResult struct {
	BlameRowsSynced       int
	InventoryStatus       string
	Repo                  string
	RemainingPaths        int
	RetryablePathFailures int
}

func (result GitLabBlameResult) routeValues() map[string]any {
	return map[string]any{
		"blame_rows_synced":       result.BlameRowsSynced,
		"inventory_status":        result.InventoryStatus,
		"repo":                    result.Repo,
		"remaining_paths":         result.RemainingPaths,
		"retryable_path_failures": result.RetryablePathFailures,
	}
}

type gitLabBlameCountingDoer struct {
	delegate providerfoundation.HTTPDoer
	attempts *int
}

func (doer gitLabBlameCountingDoer) Do(request *http.Request) (*http.Response, error) {
	if doer.attempts != nil {
		*doer.attempts++
	}
	return doer.delegate.Do(request)
}

type gitLabBlameTreePayload struct {
	Path string `json:"path"`
	Type string `json:"type"`
}

type gitLabBlameCommitRefPayload struct {
	ID string `json:"id"`
}

func (handler GitLabBlameRouteHandler) limits() (int, int, int, error) {
	perPage, maxPages, maxFiles := handler.PerPage, handler.MaxPages, handler.MaxFiles
	if perPage == 0 {
		perPage = gitLabBlameDefaultPerPage
	}
	if maxPages == 0 {
		maxPages = gitLabBlameDefaultMaxPage
	}
	if maxFiles == 0 {
		maxFiles = gitLabBlameMaxFiles
	}
	if perPage < 1 || perPage > gitLabBlameDefaultPerPage || maxPages < 1 ||
		maxPages > gitLabBlameDefaultMaxPage || maxFiles < 1 || maxFiles > gitLabBlameMaxFiles {
		return 0, 0, 0, ErrInvalidConfiguration
	}
	return perPage, maxPages, maxFiles, nil
}

func (handler GitLabBlameRouteHandler) Collect(
	ctx context.Context,
	claim Claim,
	_ providerfoundation.Credential,
	client *providerfoundation.HTTPClient,
	normalizedAt time.Time,
) (CompleteRouteBatch, error) {
	if err := validateGitLabBlameCollectInputs(ctx, claim, client, normalizedAt); err != nil {
		return CompleteRouteBatch{}, err
	}
	perPage, maxPages, maxFiles, err := handler.limits()
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	if handler.Coverage == nil {
		return CompleteRouteBatch{}, ErrGitLabBlameCoverageUnavailable
	}
	return collectGitLabBlame(ctx, claim, client, normalizedAt, handler.Coverage,
		perPage, maxPages, maxFiles, false)
}

// collectGitLabBlameFoundation keeps the provider fetch and normalization
// boundary available for differential tests. Production selection remains
// fail-closed until tenant-scoped persisted coverage is supplied above.
func collectGitLabBlameFoundation(
	ctx context.Context,
	claim Claim,
	client *providerfoundation.HTTPClient,
	normalizedAt time.Time,
) (CompleteRouteBatch, error) {
	return collectGitLabBlame(ctx, claim, client, normalizedAt, nil,
		gitLabBlameDefaultPerPage, gitLabBlameDefaultMaxPage, gitLabBlameMaxFiles, true)
}

func collectGitLabBlame(
	ctx context.Context,
	claim Claim,
	client *providerfoundation.HTTPClient,
	normalizedAt time.Time,
	coverage GitLabBlameCoverage,
	perPage, maxPages, maxFiles int,
	allowUnscopedCoverage bool,
) (CompleteRouteBatch, error) {
	if err := validateGitLabBlameCollectInputs(ctx, claim, client, normalizedAt); err != nil {
		return CompleteRouteBatch{}, err
	}
	if perPage < 1 || perPage > gitLabBlameDefaultPerPage || maxPages < 1 ||
		maxPages > gitLabBlameDefaultMaxPage || maxFiles < 1 || maxFiles > gitLabBlameMaxFiles {
		return CompleteRouteBatch{}, ErrInvalidConfiguration
	}
	normalizedAt = normalizedAt.UTC().Truncate(time.Millisecond)
	projectID, err := gitLabProjectID(claim.SourceExternalID)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	requests := 0
	counted := *client
	counted.Doer = gitLabBlameCountingDoer{delegate: client.Doer, attempts: &requests}
	root := providerRelativePath(&counted, "api", "v4", "projects", projectID)

	var project repositoryPayload
	if err := fetchObject(ctx, &counted, root, &project); err != nil {
		return CompleteRouteBatch{}, gitLabBlameTraversalError(err)
	}
	parsedProjectID, err := project.ID.Int64()
	if err != nil || parsedProjectID < 1 || strconv.FormatInt(parsedProjectID, 10) != projectID {
		return CompleteRouteBatch{}, gitLabBlameTraversalError(providerfoundation.ErrNormalizationInvalid)
	}
	fullName := gitLabProjectFullName(project)
	repoID, err := repositoryIdentity(fullName)
	if err != nil {
		return CompleteRouteBatch{}, gitLabBlameTraversalError(err)
	}
	branch := project.DefaultBranch
	if branch == "" {
		branch = "main"
	}
	treeRef, commitPages, err := gitLabBlameTreeRef(ctx, &counted, root, branch, claim.BeforeAt)
	if err != nil {
		return CompleteRouteBatch{}, gitLabBlameTraversalError(err)
	}
	if treeRef == "" {
		return gitLabBlameBatch(claim, fullName, nil, requests, commitPages, 0, 0, false, true)
	}

	tree, err := providerfoundation.CollectGitLabPageParamPages(ctx, &counted,
		providerfoundation.GitLabPageOptions{
			Path:    root + "/repository/tree",
			Query:   url.Values{"ref": {treeRef}, "recursive": {"true"}},
			PerPage: perPage, MaxPages: maxPages,
		})
	if err != nil {
		return CompleteRouteBatch{}, gitLabBlameTraversalError(err)
	}
	if tree.CapReached {
		return CompleteRouteBatch{}, fmt.Errorf("%w: tree page cap", ErrGitLabBlameIncomplete)
	}
	paths := make([]string, 0, len(tree.Items))
	for _, raw := range tree.Items {
		var object map[string]json.RawMessage
		if err := json.Unmarshal(raw, &object); err != nil || object == nil {
			return CompleteRouteBatch{}, gitLabBlameTraversalError(providerfoundation.ErrNormalizationInvalid)
		}
		var item gitLabBlameTreePayload
		if err := json.Unmarshal(raw, &item); err != nil {
			return CompleteRouteBatch{}, gitLabBlameTraversalError(providerfoundation.ErrNormalizationInvalid)
		}
		if item.Type == "blob" && strings.TrimSpace(item.Path) != "" {
			paths = append(paths, item.Path)
		}
	}
	pages := commitPages + tree.Pages
	if len(paths) == 0 {
		return gitLabBlameBatch(claim, fullName, nil, requests, pages, 0, 0, false, false)
	}

	blamedPaths := []string(nil)
	if coverage != nil {
		blamedPaths, err = coverage.BlamedPaths(ctx, claim, repoID)
		if err != nil {
			return CompleteRouteBatch{}, fmt.Errorf("%w: %w", ErrGitLabBlameCoverageUnavailable, err)
		}
	} else if !allowUnscopedCoverage {
		return CompleteRouteBatch{}, ErrGitLabBlameCoverageUnavailable
	}
	selected, remainingPaths, err := selectNextGitLabBlamePaths(paths, blamedPaths, maxFiles)
	if err != nil {
		return CompleteRouteBatch{}, err
	}

	rows := make([]gitBlameRow, 0)
	retryableFailures := 0
	emptyPaths := 0
	for _, filePath := range selected {
		ranges, fetchErr := fetchGitLabBlame(ctx, &counted, root, filePath, treeRef)
		if fetchErr != nil {
			if fatal := gitLabBlameFileControlError(ctx, fetchErr); fatal != nil {
				return CompleteRouteBatch{}, fmt.Errorf("%w for %s: %w", ErrGitLabBlameTraversalFailed, filePath, fatal)
			}
			retryableFailures++
			remainingPaths++
			continue
		}
		if len(ranges) == 0 {
			// The Python store considers a file with no blame lines unblamed on
			// the next run. Without a path-progress table, advancing its window
			// would make that path permanently disappear from the bounded retry.
			emptyPaths++
			remainingPaths++
			continue
		}
		for _, blameRange := range ranges {
			for lineNo, line := range blameRange.Lines {
				row := newGitLabBlameRow(claim, repoID, filePath,
					uint32(blameRange.StartingLine+lineNo), blameRange, line, normalizedAt)
				if err := validateGitLabBlameRow(row, claim); err != nil {
					return CompleteRouteBatch{}, err
				}
				rows = append(rows, row)
			}
		}
	}
	return gitLabBlameBatch(claim, fullName, rows, requests, pages,
		remainingPaths, retryableFailures, emptyPaths > 0, false)
}

func gitLabBlameTraversalError(err error) error {
	if err == nil {
		return ErrGitLabBlameTraversalFailed
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return fmt.Errorf("%w: %w", ErrGitLabBlameTraversalFailed, err)
}

func gitLabBlameFileControlError(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, providerfoundation.ErrLeaseLost) ||
		errors.Is(err, providerfoundation.ErrBudgetUnavailable) {
		return err
	}
	var providerErr *providerfoundation.ProviderError
	if errors.As(err, &providerErr) && providerErr.Class == providerfoundation.ErrorRateLimited {
		return err
	}
	return nil
}

func validateGitLabBlameCollectInputs(
	ctx context.Context,
	claim Claim,
	client *providerfoundation.HTTPClient,
	normalizedAt time.Time,
) error {
	if ctx == nil || claim.Validate() != nil || claim.Provider != "gitlab" ||
		claim.Dataset != "blame" || client == nil || client.Provider != "gitlab" ||
		client.BaseURL == nil || normalizedAt.IsZero() {
		return ErrInvalidConfiguration
	}
	return nil
}

func gitLabBlameTreeRef(
	ctx context.Context,
	client *providerfoundation.HTTPClient,
	root, branch string,
	beforeAt *time.Time,
) (string, int, error) {
	if beforeAt == nil {
		return branch, 0, nil
	}
	page, err := providerfoundation.CollectGitLabPageParamPages(ctx, client,
		providerfoundation.GitLabPageOptions{
			Path: root + "/repository/commits",
			Query: url.Values{
				"ref_name": {branch},
				"until":    {beforeAt.UTC().Format(time.RFC3339Nano)},
			},
			PerPage: 1, MaxPages: 1, SinglePage: true,
		})
	if err != nil {
		return "", page.Pages, err
	}
	if len(page.Items) == 0 {
		return "", page.Pages, nil
	}
	var commit gitLabBlameCommitRefPayload
	if err := json.Unmarshal(page.Items[0], &commit); err != nil || strings.TrimSpace(commit.ID) == "" {
		return "", page.Pages, providerfoundation.ErrNormalizationInvalid
	}
	return commit.ID, page.Pages, nil
}

type gitLabBlameRangePayload struct {
	StartingLine int
	CommitSHA    string
	Author       string
	AuthorEmail  string
	Lines        []string
}

func fetchGitLabBlame(
	ctx context.Context,
	client *providerfoundation.HTTPClient,
	root, filePath, ref string,
) ([]gitLabBlameRangePayload, error) {
	path := root + "/repository/files/" + url.PathEscape(filePath) + "/blame"
	response, err := client.Do(ctx, http.MethodGet, path+"?ref="+url.QueryEscape(ref), nil)
	if err != nil {
		return nil, err
	}
	body, readErr := io.ReadAll(io.LimitReader(response.Body, nativeMaxObjectBytes+1))
	closeErr := response.Body.Close()
	if readErr != nil || closeErr != nil || len(body) > nativeMaxObjectBytes {
		return nil, providerfoundation.ErrNormalizationInvalid
	}
	if bytes.Equal(bytes.TrimSpace(body), []byte("null")) {
		return nil, providerfoundation.ErrNormalizationInvalid
	}
	var items []json.RawMessage
	if err := json.Unmarshal(body, &items); err != nil {
		return nil, providerfoundation.ErrNormalizationInvalid
	}
	return normalizeGitLabBlameItems(items), nil
}

func normalizeGitLabBlameItems(items []json.RawMessage) []gitLabBlameRangePayload {
	lineNo := 1
	ranges := make([]gitLabBlameRangePayload, 0, len(items))
	for _, raw := range items {
		var object map[string]json.RawMessage
		if json.Unmarshal(raw, &object) != nil || object == nil {
			continue
		}
		var rawLines []json.RawMessage
		if rawValue, ok := object["lines"]; !ok || json.Unmarshal(rawValue, &rawLines) != nil {
			continue
		}
		lines := make([]string, 0, len(rawLines))
		for _, rawLine := range rawLines {
			var line string
			if json.Unmarshal(rawLine, &line) == nil {
				lines = append(lines, line)
			}
		}
		if len(lines) == 0 {
			continue
		}
		author, email, commit := "Unknown", "", ""
		if rawCommit, ok := object["commit"]; ok {
			var commitObject map[string]json.RawMessage
			if json.Unmarshal(rawCommit, &commitObject) == nil && commitObject != nil {
				commit = jsonStringValue(commitObject["id"])
				author = jsonStringValue(commitObject["author_name"])
				if author == "" {
					author = "Unknown"
				}
				email = jsonStringValue(commitObject["author_email"])
			}
		}
		ranges = append(ranges, gitLabBlameRangePayload{
			StartingLine: lineNo, CommitSHA: commit, Author: author,
			AuthorEmail: email, Lines: lines,
		})
		lineNo += len(lines)
	}
	return ranges
}

func jsonStringValue(raw json.RawMessage) string {
	if len(raw) == 0 || string(raw) == "null" {
		return ""
	}
	var value string
	if json.Unmarshal(raw, &value) == nil {
		return value
	}
	var number json.Number
	if json.Unmarshal(raw, &number) == nil {
		return number.String()
	}
	return ""
}

func newGitLabBlameRow(
	claim Claim,
	repoID, filePath string,
	lineNo uint32,
	blameRange gitLabBlameRangePayload,
	line string,
	normalizedAt time.Time,
) gitBlameRow {
	authorName, authorEmail, commitHash := blameRange.Author, blameRange.AuthorEmail, blameRange.CommitSHA
	return gitBlameRow{
		RepoID: repoID, Path: filePath, LineNo: lineNo,
		AuthorEmail: &authorEmail, AuthorName: &authorName,
		CommitHash: &commitHash, Line: &line,
		LastSynced: normalizedAt, OrgID: claim.OrgID,
	}
}

func validateGitLabBlameRow(row gitBlameRow, claim Claim) error {
	if claim.Validate() != nil || claim.Provider != "gitlab" || claim.Dataset != "blame" ||
		row.RepoID == "" || row.Path == "" || row.LineNo == 0 || row.LastSynced.IsZero() ||
		row.OrgID == "" || row.OrgID != claim.OrgID || row.AuthorEmail == nil ||
		row.AuthorName == nil || row.CommitHash == nil || row.Line == nil {
		return ErrInvalidConfiguration
	}
	return nil
}

func selectNextGitLabBlamePaths(filePaths, blamedPaths []string, maxFiles int) ([]string, int, error) {
	if maxFiles < 1 || maxFiles > gitLabBlameMaxFiles {
		return nil, 0, ErrInvalidConfiguration
	}
	blamed := make(map[string]struct{}, len(blamedPaths))
	for _, path := range blamedPaths {
		if strings.TrimSpace(path) != "" {
			blamed[path] = struct{}{}
		}
	}
	unblamed := make([]string, 0, len(filePaths))
	for _, path := range filePaths {
		if strings.TrimSpace(path) == "" {
			continue
		}
		if _, ok := blamed[path]; ok {
			continue
		}
		unblamed = append(unblamed, path)
	}
	remaining := 0
	if len(unblamed) > maxFiles {
		remaining = len(unblamed) - maxFiles
		unblamed = unblamed[:maxFiles]
	}
	return unblamed, remaining, nil
}

func gitLabBlameBatch(
	claim Claim,
	fullName string,
	rows []gitBlameRow,
	requests, pages, remainingPaths, retryableFailures int,
	emptyPath, noCommit bool,
) (CompleteRouteBatch, error) {
	effect, err := effectBatchFromValues("git_blame", EffectReadbackRequired, rows)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	status := "complete"
	if remainingPaths > 0 {
		status = "partial"
	}
	if len(rows) == 0 && remainingPaths == 0 {
		status = "empty"
		if noCommit {
			status = "no_commit_at_bound"
		}
	}
	watermark := claim.BeforeAt
	if status == "partial" || emptyPath {
		watermark = nil
	}
	result := GitLabBlameResult{
		BlameRowsSynced: len(rows), InventoryStatus: status, Repo: fullName,
		RemainingPaths: remainingPaths, RetryablePathFailures: retryableFailures,
	}
	return CompleteRouteBatch{
		Effects: []EffectBatch{effect}, Result: result.routeValues(), Watermark: watermark,
		Evidence: FetchEvidence{
			Provider: claim.Provider, Dataset: claim.Dataset, Requests: requests,
			Pages: pages, Records: len(rows), CapReached: false,
		},
	}, nil
}

var _ CompleteRouteHandler = GitLabBlameRouteHandler{}
