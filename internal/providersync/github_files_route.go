package providersync

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

const (
	gitHubFileContentMaxBytes = 1_000_000
	gitHubFileContentMaxFiles = 2_000
	gitHubFileContentBatch    = 50
)

// ErrGitHubFilesTraversalFailed marks an inventory traversal that could not
// establish whether the repository is empty. Callers must retry it rather than
// completing the sync with a zero-row effect.
var ErrGitHubFilesTraversalFailed = errors.New("github files traversal failed")

// gitFileRow is the git_files projection produced by github/files. The Python
// producer constructs GitFile records in backfill_file_records and stamps the
// sink-owned last_synced value at ClickHouse insertion; this route carries the
// equivalent normalized timestamp explicitly for crash-safe readback.
type gitFileRow struct {
	RepoID     string    `json:"repo_id"`
	Path       string    `json:"path"`
	Executable bool      `json:"executable"`
	Contents   *string   `json:"contents"`
	LastSynced time.Time `json:"last_synced"`
	OrgID      string    `json:"org_id"`
}

type gitHubBranchPayload struct {
	Commit struct {
		SHA string `json:"sha"`
	} `json:"commit"`
}

type gitHubTreePayload struct {
	Tree []gitHubTreeEntry `json:"tree"`
}

type gitHubTreeEntry struct {
	Path string `json:"path"`
	Type string `json:"type"`
	Size *int   `json:"size"`
}

// GitHubFilesRouteHandler mirrors github.py's files branch: resolve the branch
// at the claim's upper bound, traverse its recursive tree, retain every blob
// path, and fetch scanner-eligible, <=1MB blob text in 50-path GraphQL batches.
// Python deliberately does not reject a truncated tree response, so this route
// preserves that behavior rather than inventing a stricter policy.
type GitHubFilesRouteHandler struct{}

func (GitHubFilesRouteHandler) Collect(
	ctx context.Context,
	claim Claim,
	_ providerfoundation.Credential,
	client *providerfoundation.HTTPClient,
	normalizedAt time.Time,
) (CompleteRouteBatch, error) {
	if ctx == nil || claim.Validate() != nil || claim.Provider != "github" ||
		claim.Dataset != "files" || client == nil || client.Provider != "github" ||
		client.BaseURL == nil || normalizedAt.IsZero() {
		return CompleteRouteBatch{}, ErrInvalidConfiguration
	}
	normalizedAt = normalizedAt.UTC().Truncate(time.Millisecond)
	owner, repository, err := splitGitHubRepository(claim.SourceExternalID)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	root := providerRelativePath(client, "repos", owner, repository)
	var repoPayload gitHubRepositoryPayload
	if err := fetchObject(ctx, client, root, &repoPayload); err != nil {
		return CompleteRouteBatch{}, err
	}
	repoID, err := repositoryIdentity(repoPayload.FullName)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	branch := repoPayload.DefaultBranch
	if branch == "" {
		branch = "main"
	}
	treeRef, requests, err := gitHubFilesTreeRef(ctx, client, root, branch, claim.BeforeAt)
	if err != nil {
		if err := continueGitHubFilesTraversal(err, repoPayload.FullName); err != nil {
			return CompleteRouteBatch{}, err
		}
		return gitHubFilesBatch(claim, repoPayload.FullName, nil, normalizedAt, requests, 0, false)
	}
	if treeRef == "" {
		return gitHubFilesBatch(claim, repoPayload.FullName, nil, normalizedAt, requests, 0, false)
	}
	var tree gitHubTreePayload
	if err := fetchObject(ctx, client, root+"/git/trees/"+url.PathEscape(treeRef)+"?recursive=true", &tree); err != nil {
		if err := continueGitHubFilesTraversal(err, repoPayload.FullName); err != nil {
			return CompleteRouteBatch{}, err
		}
		return gitHubFilesBatch(claim, repoPayload.FullName, nil, normalizedAt, requests, 0, false)
	}
	requests++
	paths := make([]string, 0, len(tree.Tree))
	sizes := make(map[string]*int, len(tree.Tree))
	for _, entry := range tree.Tree {
		if entry.Type != "blob" || entry.Path == "" {
			continue
		}
		paths = append(paths, entry.Path)
		sizes[entry.Path] = entry.Size
	}
	contents, contentRequests, err := fetchGitHubFileContents(ctx, client, owner, repository, treeRef, paths, sizes)
	if err != nil {
		if err := continueGitHubFilesTraversal(err, repoPayload.FullName); err != nil {
			return CompleteRouteBatch{}, err
		}
		return gitHubFilesBatch(claim, repoPayload.FullName, nil, normalizedAt, requests+contentRequests, 0, false)
	}
	requests += contentRequests
	rows := make([]gitFileRow, 0, len(paths))
	for _, filePath := range paths {
		row := newGitHubFileRow(claim, repoID, filePath, contents[filePath], normalizedAt)
		if err := row.validate(claim); err != nil {
			return CompleteRouteBatch{}, err
		}
		rows = append(rows, row)
	}
	return gitHubFilesBatch(claim, repoPayload.FullName, rows, normalizedAt, requests, 1, false)
}

func continueGitHubFilesTraversal(err error, repository string) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	var providerErr *providerfoundation.ProviderError
	if errors.As(err, &providerErr) && providerErr.Class == providerfoundation.ErrorRateLimited {
		return err
	}
	slog.Warn("github files traversal failed", "repository", repository, "inventory_status", "failed", "error", err)
	return fmt.Errorf("%w: %w", ErrGitHubFilesTraversalFailed, err)
}

func newGitHubFileRow(claim Claim, repoID, filePath, content string, normalizedAt time.Time) gitFileRow {
	row := gitFileRow{RepoID: repoID, Path: filePath, LastSynced: normalizedAt, OrgID: claim.OrgID}
	if content != "" {
		row.Contents = &content
	}
	return row
}

func gitHubFilesTreeRef(
	ctx context.Context,
	client *providerfoundation.HTTPClient,
	root, branch string,
	beforeAt *time.Time,
) (string, int, error) {
	if beforeAt != nil {
		query := url.Values{"per_page": {"1"}, "sha": {branch}, "until": {beforeAt.UTC().Format(time.RFC3339Nano)}}
		var commits []struct {
			SHA string `json:"sha"`
		}
		if err := fetchObject(ctx, client, root+"/commits?"+query.Encode(), &commits); err != nil {
			return "", 0, err
		}
		if len(commits) == 0 {
			return "", 1, nil
		}
		return commits[0].SHA, 1, nil
	}
	var branchPayload gitHubBranchPayload
	if err := fetchObject(ctx, client, root+"/branches/"+url.PathEscape(branch), &branchPayload); err != nil {
		return "", 0, err
	}
	return branchPayload.Commit.SHA, 1, nil
}

func gitHubFilesBatch(
	claim Claim,
	fullName string,
	rows []gitFileRow,
	normalizedAt time.Time,
	requests, pages int,
	capReached bool,
) (CompleteRouteBatch, error) {
	effect, err := effectBatchFromValues("git_files", EffectReadbackRequired, rows)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	inventoryStatus := "complete"
	if len(rows) == 0 {
		inventoryStatus = "empty"
		if pages == 0 {
			inventoryStatus = "no_commit_at_bound"
		}
	}
	return CompleteRouteBatch{
		Effects: []EffectBatch{effect},
		Result: map[string]any{
			"files_synced": len(rows), "inventory_status": inventoryStatus, "repo": fullName,
		},
		Watermark: claim.BeforeAt,
		Evidence: FetchEvidence{
			Provider: claim.Provider, Dataset: claim.Dataset, Requests: requests,
			Pages: pages, Records: len(rows), CapReached: capReached,
		},
	}, nil
}

func fetchGitHubFileContents(
	ctx context.Context,
	client *providerfoundation.HTTPClient,
	owner, repository, ref string,
	paths []string,
	sizes map[string]*int,
) (map[string]string, int, error) {
	scannable := make([]string, 0, min(len(paths), gitHubFileContentMaxFiles))
	for _, filePath := range paths {
		if !gitHubFileContentEligible(filePath, sizes[filePath]) {
			continue
		}
		scannable = append(scannable, filePath)
		if len(scannable) >= gitHubFileContentMaxFiles {
			break
		}
	}
	contents := make(map[string]string)
	requests := 0
	for start := 0; start < len(scannable); start += gitHubFileContentBatch {
		end := min(start+gitHubFileContentBatch, len(scannable))
		chunk := scannable[start:end]
		body, err := json.Marshal(map[string]any{
			"query":     gitHubBlobTextsQuery(ref, chunk),
			"variables": map[string]string{"owner": owner, "repo": repository},
		})
		if err != nil {
			return nil, requests, providerfoundation.ErrNormalizationInvalid
		}
		response, err := client.Do(ctx, "POST", gitHubGraphQLPath(client), bytes.NewReader(body))
		if err != nil {
			return nil, requests, err
		}
		requests++
		var envelope struct {
			Data struct {
				Repository map[string]struct {
					Text        *string `json:"text"`
					IsBinary    bool    `json:"isBinary"`
					IsTruncated bool    `json:"isTruncated"`
				} `json:"repository"`
			} `json:"data"`
			Errors json.RawMessage `json:"errors"`
		}
		decodeErr := json.NewDecoder(response.Body).Decode(&envelope)
		closeErr := response.Body.Close()
		if decodeErr != nil || closeErr != nil || len(envelope.Errors) > 0 {
			return nil, requests, providerfoundation.ErrNormalizationInvalid
		}
		for index, filePath := range chunk {
			blob, ok := envelope.Data.Repository["f"+strconv.Itoa(index)]
			if !ok || blob.IsBinary || blob.IsTruncated || blob.Text == nil {
				continue
			}
			contents[filePath] = *blob.Text
		}
	}
	return contents, requests, nil
}

func gitHubFileContentEligible(filePath string, size *int) bool {
	if size != nil && *size > gitHubFileContentMaxBytes {
		return false
	}
	for _, segment := range strings.Split(filePath, "/") {
		switch segment {
		case "migrations", "tests", "venv", ".venv", "node_modules", "dist", "build", ".next", "vendor":
			return false
		}
	}
	base := path.Base(filePath)
	if base == "__init__.py" || strings.HasSuffix(base, ".min.js") || strings.HasSuffix(base, ".d.ts") ||
		strings.HasSuffix(base, ".config.js") || strings.HasSuffix(base, ".config.ts") || strings.HasSuffix(base, ".config.mjs") {
		return false
	}
	switch strings.ToLower(path.Ext(base)) {
	case ".py", ".ts", ".tsx", ".js", ".jsx", ".mjs", ".cjs", ".go", ".rs", ".java", ".kt", ".kts", ".rb", ".php", ".c", ".h", ".cpp", ".cc", ".hpp", ".cs", ".swift", ".scala", ".m", ".mm", ".lua", ".vue":
		return true
	default:
		return false
	}
}

func gitHubBlobTextsQuery(ref string, paths []string) string {
	fields := make([]string, 0, len(paths))
	for index, filePath := range paths {
		expression, _ := json.Marshal(ref + ":" + filePath)
		fields = append(fields, "f"+strconv.Itoa(index)+": object(expression: "+string(expression)+") { ... on Blob { text isBinary isTruncated } }")
	}
	return "query($owner: String!, $repo: String!) {\n  repository(owner: $owner, name: $repo) {\n" + strings.Join(fields, "\n") + "\n  }\n}"
}

func gitHubGraphQLPath(client *providerfoundation.HTTPClient) string {
	base := strings.TrimSuffix(client.BaseURL.EscapedPath(), "/")
	if strings.HasSuffix(base, "/api/v3") {
		return strings.TrimSuffix(base, "/api/v3") + "/api/graphql"
	}
	return base + "/graphql"
}

func (row gitFileRow) validate(claim Claim) error {
	if row.OrgID == "" || row.OrgID != claim.OrgID || len(row.RepoID) != 36 || row.Path == "" || row.LastSynced.IsZero() {
		return providerfoundation.ErrInvalidScope
	}
	return nil
}

var _ CompleteRouteHandler = GitHubFilesRouteHandler{}
