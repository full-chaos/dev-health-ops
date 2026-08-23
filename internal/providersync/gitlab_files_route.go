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
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

const (
	defaultGitLabFilesPerPage  = 100
	defaultGitLabFilesMaxPages = 10_000
	gitLabFileContentMaxBytes  = 1_000_000
	gitLabFileContentMaxFiles  = 2_000
	gitLabFileContentBatch     = 50
	gitLabGraphQLResponseLimit = 64 << 20
)

// ErrGitLabFilesTraversalFailed marks a tree traversal that did not establish
// a complete path inventory. Content traversal is deliberately softer: the
// Python producer preserves available paths, records typed incompleteness, and
// leaves the watermark nil so the content window is retried.
var ErrGitLabFilesTraversalFailed = errors.New("gitlab files traversal failed")

const gitLabFilesIncompleteResultKey = "gitlab_files_incomplete"

// GitLabFilesIncomplete is durable evidence that the path inventory landed
// but content coverage did not. It is intentionally JSON-safe because the
// complete-route snapshot persists Result before effects are committed.
type GitLabFilesIncomplete struct {
	Cause    string `json:"cause"`
	Subject  string `json:"subject"`
	Limit    int    `json:"limit,omitempty"`
	Observed int    `json:"observed,omitempty"`
}

type gitLabFilesContentStatus struct {
	Incomplete []GitLabFilesIncomplete
}

func (status *gitLabFilesContentStatus) addIncomplete(cause string, limit, observed int) {
	for _, existing := range status.Incomplete {
		if existing.Cause == cause {
			return
		}
	}
	status.Incomplete = append(status.Incomplete, GitLabFilesIncomplete{
		Cause: cause, Subject: "gitlab/files", Limit: limit, Observed: observed,
	})
}

// GitLabFilesRouteHandler owns only the gitlab/files unit. GitLab's blame
// dataset has a separate destination and remains outside this route.
//
// The handler follows the live Python producer's project -> bounded commit
// ref -> recursive tree -> scanner-filtered GraphQL blob sequence. Tree
// traversal errors fail closed so an incomplete path inventory cannot become
// a successful empty effect. Ordinary content errors preserve paths, emit
// typed incompleteness, and leave the watermark nil; rate limits still
// propagate (D16 policy is recorded on CHAOS-3188).
type GitLabFilesRouteHandler struct {
	PerPage   int
	MaxPages  int
	MaxFiles  int
	MaxBytes  int
	BatchSize int
}

type gitLabFilesCountingDoer struct {
	delegate providerfoundation.HTTPDoer
	attempts *int
}

func (doer gitLabFilesCountingDoer) Do(request *http.Request) (*http.Response, error) {
	(*doer.attempts)++
	return doer.delegate.Do(request)
}

type gitLabTreePayload struct {
	Path string `json:"path"`
	Type string `json:"type"`
}

type gitLabCommitRefPayload struct {
	ID string `json:"id"`
}

type gitLabBlobNode struct {
	Path        string          `json:"path"`
	RawSize     json.RawMessage `json:"rawSize"`
	RawTextBlob *string         `json:"rawTextBlob"`
}

func (handler GitLabFilesRouteHandler) limits() (int, int, int, int, int, error) {
	perPage, maxPages, maxFiles, maxBytes, batchSize :=
		handler.PerPage, handler.MaxPages, handler.MaxFiles, handler.MaxBytes, handler.BatchSize
	if perPage == 0 {
		perPage = defaultGitLabFilesPerPage
	}
	if maxPages == 0 {
		maxPages = defaultGitLabFilesMaxPages
	}
	if maxFiles == 0 {
		maxFiles = gitLabFileContentMaxFiles
	}
	if maxBytes == 0 {
		maxBytes = gitLabFileContentMaxBytes
	}
	if batchSize == 0 {
		batchSize = gitLabFileContentBatch
	}
	if perPage < 1 || perPage > defaultGitLabFilesPerPage || maxPages < 1 ||
		maxPages > defaultGitLabFilesMaxPages || maxFiles < 1 ||
		maxFiles > gitLabFileContentMaxFiles || maxBytes < 1 || batchSize < 1 ||
		batchSize > gitLabFileContentBatch {
		return 0, 0, 0, 0, 0, ErrInvalidConfiguration
	}
	return perPage, maxPages, maxFiles, maxBytes, batchSize, nil
}

func (handler GitLabFilesRouteHandler) Collect(
	ctx context.Context,
	claim Claim,
	_ providerfoundation.Credential,
	client *providerfoundation.HTTPClient,
	normalizedAt time.Time,
) (CompleteRouteBatch, error) {
	if ctx == nil || claim.Validate() != nil || claim.Provider != "gitlab" ||
		claim.Dataset != "files" || client == nil || client.Provider != "gitlab" ||
		client.BaseURL == nil || normalizedAt.IsZero() {
		return CompleteRouteBatch{}, ErrInvalidConfiguration
	}
	perPage, maxPages, maxFiles, maxBytes, batchSize, err := handler.limits()
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	projectID, err := gitLabProjectID(claim.SourceExternalID)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	requests := 0
	counted := *client
	counted.Doer = gitLabFilesCountingDoer{delegate: client.Doer, attempts: &requests}
	root := providerRelativePath(&counted, "api", "v4", "projects", projectID)
	var project repositoryPayload
	if err := fetchObject(ctx, &counted, root, &project); err != nil {
		return CompleteRouteBatch{}, gitLabFilesTraversalError(err)
	}
	parsedProjectID, err := project.ID.Int64()
	if err != nil || parsedProjectID < 1 || strconv.FormatInt(parsedProjectID, 10) != projectID {
		return CompleteRouteBatch{}, gitLabFilesTraversalError(providerfoundation.ErrNormalizationInvalid)
	}
	fullName := gitLabProjectFullName(project)
	repoID, err := repositoryIdentity(fullName)
	if err != nil {
		return CompleteRouteBatch{}, gitLabFilesTraversalError(err)
	}
	branch := project.DefaultBranch
	if branch == "" {
		branch = "main"
	}

	treeRef, _, err := gitLabFilesTreeRef(
		ctx, &counted, root, branch, claim.BeforeAt,
	)
	if err != nil {
		return CompleteRouteBatch{}, gitLabFilesTraversalError(err)
	}
	if treeRef == "" {
		return gitLabFilesBatch(claim, fullName, nil, nil, requests, 0)
	}

	tree, err := providerfoundation.CollectGitLabPageParamPages(ctx, &counted,
		providerfoundation.GitLabPageOptions{
			Path: root + "/repository/tree",
			Query: url.Values{
				"ref": {treeRef}, "recursive": {"true"},
			},
			PerPage: perPage, MaxPages: maxPages,
		})
	if err != nil {
		return CompleteRouteBatch{}, gitLabFilesTraversalError(err)
	}
	if tree.PageBudgetExhausted {
		return CompleteRouteBatch{}, ErrPaginationCapExceeded
	}
	paths := make([]string, 0, len(tree.Items))
	for _, raw := range tree.Items {
		var object map[string]json.RawMessage
		if err := json.Unmarshal(raw, &object); err != nil || object == nil {
			return CompleteRouteBatch{}, gitLabFilesTraversalError(providerfoundation.ErrNormalizationInvalid)
		}
		var item gitLabTreePayload
		if err := json.Unmarshal(raw, &item); err != nil {
			return CompleteRouteBatch{}, gitLabFilesTraversalError(providerfoundation.ErrNormalizationInvalid)
		}
		if item.Type == "blob" && item.Path != "" {
			paths = append(paths, item.Path)
		}
	}
	contents, contentStatus, err := fetchGitLabFileContents(
		ctx, &counted, fullName, treeRef, paths, maxFiles, maxBytes, batchSize,
	)
	if err != nil {
		return CompleteRouteBatch{}, gitLabFilesTraversalError(err)
	}
	rows := make([]gitFileRow, 0, len(paths))
	for _, filePath := range paths {
		row := newGitLabFileRow(claim, repoID, filePath, contents, normalizedAt)
		if err := row.validate(claim); err != nil {
			return CompleteRouteBatch{}, gitLabFilesTraversalError(err)
		}
		rows = append(rows, row)
	}
	return gitLabFilesBatch(
		claim, fullName, rows, contentStatus.Incomplete, requests, tree.Pages,
	)
}

func gitLabFilesTraversalError(err error) error {
	if err == nil {
		return ErrGitLabFilesTraversalFailed
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return fmt.Errorf("%w: %w", ErrGitLabFilesTraversalFailed, err)
}

func gitLabFilesTreeRef(
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
				"ref_name": {branch}, "until": {beforeAt.UTC().Format(time.RFC3339Nano)},
			},
			PerPage: 1, MaxPages: 1, SinglePage: true,
		})
	if err != nil {
		return "", 0, err
	}
	if len(page.Items) == 0 {
		return "", page.Pages, nil
	}
	var commit gitLabCommitRefPayload
	if err := json.Unmarshal(page.Items[0], &commit); err != nil || strings.TrimSpace(commit.ID) == "" {
		return "", page.Pages, providerfoundation.ErrNormalizationInvalid
	}
	return commit.ID, page.Pages, nil
}

func fetchGitLabFileContents(
	ctx context.Context,
	client *providerfoundation.HTTPClient,
	projectFullName, ref string,
	paths []string,
	maxFiles, maxBytes, batchSize int,
) (map[string]string, gitLabFilesContentStatus, error) {
	status := gitLabFilesContentStatus{}
	allScannable := make([]string, 0, len(paths))
	for _, filePath := range paths {
		if !gitLabFileContentEligible(filePath) {
			continue
		}
		allScannable = append(allScannable, filePath)
	}
	scannable := allScannable
	if len(scannable) > maxFiles {
		status.addIncomplete("content_cap", maxFiles, len(scannable))
		scannable = scannable[:maxFiles]
	}
	if len(scannable) == 0 {
		return map[string]string{}, status, nil
	}

	eligible := make([]string, 0, len(scannable))
	requests := 0
	for start := 0; start < len(scannable); start += batchSize {
		end := min(start+batchSize, len(scannable))
		nodes, err := fetchGitLabGraphQLBlobs(ctx, client, projectFullName, ref, scannable[start:end], "path rawSize")
		if err != nil {
			if !gitLabFilesContentErrorDegradable(err) {
				return nil, status, err
			}
			status.addIncomplete("content_size_fetch", 0, end-start)
			eligible = append(eligible, scannable[start:end]...)
			continue
		}
		requests++
		for _, node := range nodes {
			if node.Path == "" {
				continue
			}
			size, err := gitLabRawSize(node.RawSize)
			if err != nil {
				status.addIncomplete("content_size_decode", 0, 1)
				eligible = append(eligible, node.Path)
				continue
			}
			if size == nil || *size <= maxBytes {
				eligible = append(eligible, node.Path)
			}
		}
	}

	contents := make(map[string]string, len(eligible))
	for start := 0; start < len(eligible); start += batchSize {
		end := min(start+batchSize, len(eligible))
		nodes, err := fetchGitLabGraphQLBlobs(ctx, client, projectFullName, ref, eligible[start:end], "path rawTextBlob")
		if err != nil {
			if !gitLabFilesContentErrorDegradable(err) {
				return nil, status, err
			}
			status.addIncomplete("content_fetch", 0, end-start)
			continue
		}
		requests++
		for _, node := range nodes {
			if node.Path != "" && node.RawTextBlob != nil {
				contents[node.Path] = *node.RawTextBlob
			}
		}
	}
	return contents, status, nil
}

func gitLabFilesContentErrorDegradable(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, providerfoundation.ErrLeaseLost) || errors.Is(err, providerfoundation.ErrBudgetUnavailable) {
		return false
	}
	var providerErr *providerfoundation.ProviderError
	if errors.As(err, &providerErr) && providerErr.Class == providerfoundation.ErrorRateLimited {
		return false
	}
	return true
}

func fetchGitLabGraphQLBlobs(
	ctx context.Context,
	client *providerfoundation.HTTPClient,
	projectFullName, ref string,
	paths []string,
	fields string,
) ([]gitLabBlobNode, error) {
	body, err := json.Marshal(map[string]any{
		"query": gitLabBlobQuery(fields),
		"variables": map[string]any{
			"fullPath": projectFullName, "ref": ref, "paths": paths,
		},
	})
	if err != nil {
		return nil, providerfoundation.ErrNormalizationInvalid
	}
	response, err := client.Do(ctx, http.MethodPost, gitLabGraphQLPath(client), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	if response == nil || response.Body == nil {
		return nil, providerfoundation.ErrNormalizationInvalid
	}
	defer response.Body.Close()
	payload, err := io.ReadAll(io.LimitReader(response.Body, gitLabGraphQLResponseLimit+1))
	if err != nil || len(payload) > gitLabGraphQLResponseLimit {
		return nil, providerfoundation.ErrNormalizationInvalid
	}
	var envelope struct {
		Data struct {
			Project *struct {
				Repository *struct {
					Blobs *struct {
						Nodes []gitLabBlobNode `json:"nodes"`
					} `json:"blobs"`
				} `json:"repository"`
			} `json:"project"`
		} `json:"data"`
		Errors json.RawMessage `json:"errors"`
	}
	if err := json.Unmarshal(payload, &envelope); err != nil {
		return nil, providerfoundation.ErrNormalizationInvalid
	}
	trimmedErrors := bytes.TrimSpace(envelope.Errors)
	if len(trimmedErrors) > 0 && !bytes.Equal(trimmedErrors, []byte("null")) && !bytes.Equal(trimmedErrors, []byte("[]")) {
		return nil, providerfoundation.ErrGraphQLResponse
	}
	if envelope.Data.Project == nil || envelope.Data.Project.Repository == nil ||
		envelope.Data.Project.Repository.Blobs == nil {
		return nil, providerfoundation.ErrGraphQLResponse
	}
	return envelope.Data.Project.Repository.Blobs.Nodes, nil
}

func gitLabRawSize(raw json.RawMessage) (*int, error) {
	if len(raw) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil, nil
	}
	var value int
	if err := json.Unmarshal(raw, &value); err != nil || value < 0 {
		return nil, providerfoundation.ErrNormalizationInvalid
	}
	return &value, nil
}

func gitLabBlobQuery(fields string) string {
	return "query($fullPath: ID!, $ref: String!, $paths: [String!]!) {\n" +
		"  project(fullPath: $fullPath) {\n" +
		"    repository {\n" +
		"      blobs(ref: $ref, paths: $paths) {\n" +
		"        nodes { " + fields + " }\n" +
		"      }\n" +
		"    }\n" +
		"  }\n" +
		"}"
}

func gitLabGraphQLPath(client *providerfoundation.HTTPClient) string {
	base := strings.TrimSuffix(client.BaseURL.EscapedPath(), "/")
	if strings.HasSuffix(base, "/api/v4") {
		return strings.TrimSuffix(base, "/api/v4") + "/api/graphql"
	}
	return base + "/api/graphql"
}

func gitLabFileContentEligible(filePath string) bool {
	for _, segment := range strings.Split(filePath, "/") {
		switch segment {
		case "migrations":
			return false
		case "tests":
			return false
		case "venv":
			return false
		case ".venv":
			return false
		case "node_modules":
			return false
		case "dist":
			return false
		case "build":
			return false
		case ".next":
			return false
		case "vendor":
			return false
		}
	}
	base := path.Base(filePath)
	if base == "__init__.py" {
		return false
	}
	if strings.HasSuffix(base, ".min.js") {
		return false
	}
	if strings.HasSuffix(base, ".d.ts") {
		return false
	}
	if strings.HasSuffix(base, ".config.js") {
		return false
	}
	if strings.HasSuffix(base, ".config.ts") {
		return false
	}
	if strings.HasSuffix(base, ".config.mjs") {
		return false
	}
	// The Python producer applies the lowercase include_globs through
	// fnmatch.fnmatch, which is case-sensitive on the supported worker
	// platform. Keep the extension's source spelling instead of normalizing it:
	// Main.GO and mixed.Go must remain paths-only, just as Python emits them.
	switch path.Ext(base) {
	case ".py", ".ts", ".tsx", ".js", ".jsx", ".mjs", ".cjs", ".go", ".rs", ".java", ".kt", ".kts", ".rb", ".php", ".c", ".h", ".cpp", ".cc", ".hpp", ".cs", ".swift", ".scala", ".m", ".mm", ".lua", ".vue":
		return true
	default:
		return false
	}
}

func newGitLabFileRow(claim Claim, repoID, filePath string, contents map[string]string, normalizedAt time.Time) gitFileRow {
	row := gitFileRow{RepoID: repoID, Path: filePath, LastSynced: normalizedAt.UTC().Truncate(time.Millisecond), OrgID: claim.OrgID}
	if content, ok := contents[filePath]; ok {
		row.Contents = &content
	}
	return row
}

func gitLabFilesBatch(
	claim Claim,
	fullName string,
	rows []gitFileRow,
	incomplete []GitLabFilesIncomplete,
	requests, pages int,
) (CompleteRouteBatch, error) {
	effect, err := effectBatchFromValues("git_files", EffectReadbackRequired, rows)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	inventoryStatus := "complete"
	if len(rows) == 0 {
		inventoryStatus = "empty"
		if pages == 0 && claim.BeforeAt != nil {
			inventoryStatus = "no_commit_at_bound"
		}
	}
	result := map[string]any{
		"files_synced": len(rows), "inventory_status": inventoryStatus, "repo": fullName,
	}
	if len(incomplete) != 0 {
		result[gitLabFilesIncompleteResultKey] = append([]GitLabFilesIncomplete(nil), incomplete...)
	}
	watermark := claim.BeforeAt
	capReached := false
	if len(incomplete) != 0 {
		watermark = nil
		for _, partial := range incomplete {
			if partial.Cause == "content_cap" {
				capReached = true
				break
			}
		}
	}
	return CompleteRouteBatch{
		Effects: []EffectBatch{effect},
		Result:  result, Watermark: watermark,
		Evidence: FetchEvidence{
			Provider: claim.Provider, Dataset: claim.Dataset,
			Requests: requests, Pages: pages, Records: len(rows), CapReached: capReached,
		},
	}, nil
}

var _ CompleteRouteHandler = GitLabFilesRouteHandler{}
