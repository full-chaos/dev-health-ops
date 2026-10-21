package providersync

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// gitHubFilesCountingDoer observes actual wire attempts, including
// transport failures and retries the wrapped HTTPClient makes internally --
// unlike a logical-fetch-call tally, it increments once per Doer.Do call
// regardless of whether that call ever produced a usable response. It is
// the single source of truth for this Collect run's FetchEvidence.Requests.
type gitHubFilesCountingDoer struct {
	delegate providerfoundation.HTTPDoer
	attempts *int
}

func (doer gitHubFilesCountingDoer) Do(request *http.Request) (*http.Response, error) {
	*doer.attempts++
	return doer.delegate.Do(request)
}

const (
	gitHubFileContentMaxBytes = 1_000_000
	gitHubFileContentBatch    = 50
	// gitHubFileChunkMaxBytes bounds the declared blob text of one chunk and of
	// one GraphQL content request. A chunk (payload plus its effect ledger)
	// must fit 2 MiB (ck_sync_chunk_payload_bytes): 1.5 MiB of declared text
	// leaves room for the row fields, and gitHubFilesChunkFits has the last
	// word when escaping inflates the text. A blob of unknown size counts as
	// the largest eligible blob.
	gitHubFileChunkMaxBytes = 1_572_864
	gitHubFileChunkMaxRows  = 500
)

// GitHubFilesChunkPolicy is the chunk policy of github/files. MaxSourceItems
// equals the row bound so one emission is one prepared sub-chunk.
func GitHubFilesChunkPolicy() ChunkPolicy {
	policy := DefaultChunkPolicy()
	policy.MaxSourceItems = gitHubFileChunkMaxRows
	policy.MaxEffectRows = gitHubFileChunkMaxRows
	return policy
}

// ErrGitHubFilesTraversalFailed marks an inventory traversal that could not
// establish whether the repository is empty. Callers must retry it rather than
// completing the sync with a zero-row effect.
var ErrGitHubFilesTraversalFailed = errors.New("github files traversal failed")

// ErrGitHubFilesSubtreeTruncated marks a non-recursive subtree fetch that
// itself reports truncated during tree-walk recovery. The tree API can
// truncate a single oversized directory even without recursive=1, so the
// walk cannot treat that subtree's listing as complete either.
var ErrGitHubFilesSubtreeTruncated = errors.New("github files subtree truncated")

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
	Tree      []gitHubTreeEntry `json:"tree"`
	Truncated bool              `json:"truncated"`
}

type gitHubTreeEntry struct {
	Path string `json:"path"`
	Type string `json:"type"`
	SHA  string `json:"sha"`
	Size *int   `json:"size"`
}

// GitHubFilesRouteHandler resolves the branch at the claim's upper bound,
// traverses its tree, retains every blob path, and fetches scanner-eligible,
// <=1MB blob text in size-bounded GraphQL requests.
//
// The route is chunked (ChunkedCompleteRouteHandler): one emission is a run of
// consecutive paths of the sorted blob list whose declared eligible text is at
// most gitHubFileChunkMaxBytes. The resume cursor pins the tree sha, so one
// unit reads one snapshot whatever the branch does between attempts, and holds
// the index of the next path. Memory per attempt is one chunk plus the path
// list, whatever the repository size (CHAOS-10439).
//
// The tree API can report the recursive listing truncated once a repository
// crosses its entry-count or response-size ceiling. A truncated recursive
// response is never banked as a complete inventory: the handler discards it
// and walks the same tree non-recursively, descending into every subtree by
// its own sha until every path is enumerated. A subtree that itself reports
// truncated, or any fetch that fails partway through the walk, fails the
// whole inventory rather than writing a partial one.
//
// A repository can also produce a well-formed, NOT-truncated recursive
// listing that is simply bigger than the shared per-object response cap
// (fetchObject/nativeMaxObjectBytes); GitHub never sets its own truncated
// flag for that case, since the cap truncates the body before that field
// would even be reached. That case is treated exactly like GitHub's own
// truncated=true and recovers through the same non-recursive walk (CHAOS-6268).
type GitHubFilesRouteHandler struct{ RepoIdentity GitLabRepoIdentityReader }

// githubFilesChunkCursor is the durable position of one files unit.
type githubFilesChunkCursor struct {
	Version  int    `json:"v"`
	TreeRef  string `json:"tree,omitempty"`
	Ordinal  int    `json:"next"`
	Rows     int    `json:"rows"`
	Contents int    `json:"contents"`
	Oversize int    `json:"oversize"`
	// Unavailable counts eligible blobs the provider returned no text for
	// (binary, truncated, absent): they stay without contents.
	Unavailable int `json:"unavailable"`
	// PinLost counts restarts after the pinned tree vanished; SyncedAt is the
	// write instant of rows built after the latest restart.
	PinLost       int        `json:"pin_lost,omitempty"`
	SyncedAt      *time.Time `json:"synced_at,omitempty"`
	PathsTotal    int        `json:"paths_total"`
	EligibleTotal int        `json:"eligible_total"`
	Requests      int        `json:"requests"`
	Chunks        int        `json:"chunks"`
	Walked        bool       `json:"walked,omitempty"`
	NoCommit      bool       `json:"no_commit,omitempty"`
	Done          bool       `json:"done,omitempty"`
}

func decodeGitHubFilesChunkCursor(raw string) (githubFilesChunkCursor, error) {
	if raw == "" {
		return githubFilesChunkCursor{Version: 1}, nil
	}
	var cursor githubFilesChunkCursor
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&cursor); err != nil || cursor.Version != 1 || cursor.Ordinal < 0 ||
		cursor.Rows < 0 || cursor.Contents < 0 || cursor.Oversize < 0 || cursor.Unavailable < 0 || cursor.PinLost < 0 || cursor.PathsTotal < 0 || cursor.EligibleTotal < 0 || cursor.Requests < 0 || cursor.Chunks < 0 ||
		(cursor.TreeRef == "" && !cursor.NoCommit) {
		return githubFilesChunkCursor{}, ErrInvalidConfiguration
	}
	return cursor, nil
}

func (cursor githubFilesChunkCursor) encode() (string, error) {
	encoded, err := json.Marshal(cursor)
	if err != nil || len(encoded) > maxChunkCursorBytes {
		return "", ErrInvalidConfiguration
	}
	return string(encoded), nil
}

// Collect runs the chunked collection to the end and returns it as one batch.
// Production uses CollectChunks; this serves one-shot callers and the parity
// tests.
func (handler GitHubFilesRouteHandler) Collect(
	ctx context.Context,
	claim Claim,
	credential providerfoundation.Credential,
	client *providerfoundation.HTTPClient,
	normalizedAt time.Time,
) (CompleteRouteBatch, error) {
	var rows []json.RawMessage
	var final CompleteRouteBatch
	err := handler.CollectChunks(ctx, claim, credential, client, normalizedAt, "", func(emission ChunkRouteEmission) error {
		for _, effect := range emission.Batch.Effects {
			rows = append(rows, effect.Rows...)
		}
		if emission.Final {
			final = emission.Batch
		}
		return nil
	})
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	effect, err := BuildEffectBatch("git_files", EffectReadbackRequired, rows)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	final.Effects = []EffectBatch{effect}
	return final, nil
}

func (handler GitHubFilesRouteHandler) CollectChunks(
	ctx context.Context,
	claim Claim,
	_ providerfoundation.Credential,
	client *providerfoundation.HTTPClient,
	normalizedAt time.Time,
	resumeCursor string,
	emit func(ChunkRouteEmission) error,
) error {
	if ctx == nil || claim.Validate() != nil || claim.Provider != "github" ||
		claim.Dataset != "files" || client == nil || client.Provider != "github" ||
		client.BaseURL == nil || normalizedAt.IsZero() || emit == nil {
		return ErrInvalidConfiguration
	}
	cursor, err := decodeGitHubFilesChunkCursor(resumeCursor)
	if err != nil {
		return err
	}
	normalizedAt = normalizedAt.UTC().Truncate(time.Millisecond)
	owner, repository, err := splitGitHubRepository(claim.SourceExternalID)
	if err != nil {
		return err
	}
	requests := 0
	counted := *client
	counted.Doer = gitHubFilesCountingDoer{delegate: client.Doer, attempts: &requests}
	client = &counted
	baseRequests := cursor.Requests
	root := providerRelativePath(client, "repos", owner, repository)
	var repoPayload gitHubRepositoryPayload
	if err := fetchObjectRoute(ctx, client, githubRepoRoute, root, &repoPayload); err != nil {
		return err
	}
	repoID, err := resolveGitHubRepoID(ctx, handler.RepoIdentity, claim, repoPayload)
	if err != nil {
		return err
	}
	before := resumeCursor
	emitFinal := func(cursor githubFilesChunkCursor) error {
		cursor.Requests = baseRequests + requests
		cursor.Done = true
		after, err := cursor.encode()
		if err != nil {
			return err
		}
		batch, err := gitHubFilesFinalBatch(claim, repoPayload.FullName, cursor)
		if err != nil {
			return err
		}
		return emit(ChunkRouteEmission{Batch: batch, CursorBefore: before, CursorAfter: after, Final: true})
	}
	if cursor.Done || cursor.NoCommit {
		return emitFinal(cursor)
	}
	pin := func() (bool, error) {
		branch := repoPayload.DefaultBranch
		if branch == "" {
			branch = "main"
		}
		treeRef, err := gitHubFilesTreeRef(ctx, client, root, branch, claim.BeforeAt)
		if err != nil {
			return false, continueGitHubFilesTraversal(err, repoPayload.FullName)
		}
		if treeRef == "" {
			cursor.NoCommit = true
			return true, nil
		}
		cursor.TreeRef = treeRef
		return false, nil
	}
	resumedPin := cursor.TreeRef != ""
	if cursor.SyncedAt != nil {
		normalizedAt = *cursor.SyncedAt
	}
	if !resumedPin {
		if done, err := pin(); err != nil || done {
			if err != nil {
				return err
			}
			return emitFinal(cursor)
		}
	}
	paths, sizes, walked, err := gitHubFilesInventory(ctx, client, root, cursor.TreeRef, repoPayload.FullName)
	if err != nil && resumedPin && gitHubFilesPinGone(err) && cursor.PinLost < gitHubFilesMaxRepins {
		// The tree the cursor pinned no longer exists (its commit was
		// garbage-collected or force-pushed away), so the cursor can never
		// advance. Restart from the current head under a later write instant:
		// rows written for the old tree keep their instant, so the new rows
		// win the ReplacingMergeTree version for every path they cover.
		slog.Warn(
			"github files pinned tree is gone; restarting from the current branch head",
			"pin_lost", cursor.PinLost+1, "paths_done", cursor.Ordinal,
		)
		restart := time.Now().UTC().Truncate(time.Millisecond)
		if !restart.After(normalizedAt) {
			restart = normalizedAt.Add(time.Millisecond)
		}
		// The lost pin's partial inventory is dropped from the cursor: its
		// counters do not carry into the result of the new tree. Only the wire
		// attempts, the restart count and the write instant survive.
		cursor = githubFilesChunkCursor{
			Version: 1, Requests: cursor.Requests, PinLost: cursor.PinLost + 1, SyncedAt: &restart,
		}
		normalizedAt = restart
		if done, pinErr := pin(); pinErr != nil || done {
			if pinErr != nil {
				return pinErr
			}
			return emitFinal(cursor)
		}
		paths, sizes, walked, err = gitHubFilesInventory(ctx, client, root, cursor.TreeRef, repoPayload.FullName)
	}
	if err != nil {
		return err
	}
	cursor.PathsTotal = len(paths)
	cursor.EligibleTotal = 0
	for _, filePath := range paths {
		if gitHubFileContentEligible(filePath, sizes[filePath]) {
			cursor.EligibleTotal++
		}
	}
	cursor.Walked = cursor.Walked || walked
	sort.Strings(paths)
	if cursor.Ordinal > len(paths) {
		return ErrInvalidConfiguration
	}
	// fetched keeps the text of every blob read in this attempt, by presence, so
	// a chunk shortened below to fit is not read twice.
	fetched := map[string]string{}
	for cursor.Ordinal < len(paths) {
		end, declared := cursor.Ordinal, 0
		for end < len(paths) && end-cursor.Ordinal < gitHubFileChunkMaxRows {
			cost := 0
			if gitHubFileContentEligible(paths[end], sizes[paths[end]]) {
				cost = gitHubFileDeclaredBytes(sizes[paths[end]])
			}
			if end > cursor.Ordinal && declared+cost > gitHubFileChunkMaxBytes {
				break
			}
			declared += cost
			end++
		}
		var missing []string
		for _, filePath := range paths[cursor.Ordinal:end] {
			if _, done := fetched[filePath]; !done {
				missing = append(missing, filePath)
			}
		}
		contents, err := fetchGitHubFileContents(ctx, client, owner, repository, cursor.TreeRef, missing, sizes)
		if err != nil {
			return continueGitHubFilesTraversal(err, repoPayload.FullName)
		}
		for filePath, text := range contents {
			fetched[filePath] = text
		}
		// The chunk is sized by its persisted form: the same material the chunk
		// store builds (payload, effect ledger, cursors) must be accepted. A
		// chunk that is not is halved; one row that alone is not loses its
		// contents (counted) so it can never stop the cursor.
		dropped := map[string]bool{}
		var (
			rows   []gitFileRow
			next   githubFilesChunkCursor
			effect EffectBatch
			after  string
		)
		for {
			rows = rows[:0]
			next = cursor
			for _, filePath := range paths[cursor.Ordinal:end] {
				content, present := fetched[filePath]
				oversize := dropped[filePath]
				if oversize {
					content, present = "", false
					next.Oversize++
				}
				row := newGitHubFileRow(claim, repoID, filePath, content, normalizedAt)
				if present && row.Contents == nil {
					row.Contents = &content
				}
				if err := row.validate(claim); err != nil {
					return err
				}
				if row.Contents != nil {
					next.Contents++
				} else if !oversize && gitHubFileContentEligible(filePath, sizes[filePath]) {
					next.Unavailable++
				}
				rows = append(rows, row)
			}
			effect, err = effectBatchFromValues("git_files", EffectReadbackRequired, rows)
			if err != nil {
				return err
			}
			next.Ordinal = end
			next.Rows += len(rows)
			next.Chunks++
			next.Requests = baseRequests + requests
			after, err = next.encode()
			if err != nil {
				return err
			}
			if gitHubFilesChunkFits(claim, effect, before, after) {
				break
			}
			if len(rows) > 1 {
				end = cursor.Ordinal + len(rows)/2
				continue
			}
			if rows[0].Contents == nil {
				return ErrChunkPolicyExceeded
			}
			dropped[rows[0].Path] = true
		}
		if next.Oversize > cursor.Oversize {
			slog.Warn(
				"github files rows kept without contents: persisted size above one chunk",
				"skipped", next.Oversize-cursor.Oversize,
			)
		}
		batch := CompleteRouteBatch{Effects: []EffectBatch{effect}}
		if err := emit(ChunkRouteEmission{Batch: batch, CursorBefore: before, CursorAfter: after}); err != nil {
			return err
		}
		for _, row := range rows {
			delete(fetched, row.Path)
		}
		before, cursor = after, next
	}
	return emitFinal(cursor)
}

// gitHubFilesMaxRepins bounds the restarts of one unit: a repository whose
// every pinned tree vanishes is failing for another reason.
const gitHubFilesMaxRepins = 3

// gitHubFilesPinGone reports a provider answer that says the pinned tree
// object does not exist (404, or 422 for an unresolvable sha). Server errors,
// rate limits and transport failures are not it: they keep the pin.
func gitHubFilesPinGone(err error) bool {
	var providerErr *providerfoundation.ProviderError
	return errors.As(err, &providerErr) &&
		(providerErr.StatusCode == http.StatusNotFound || providerErr.StatusCode == http.StatusUnprocessableEntity)
}

// gitHubFileDeclaredBytes is the size a blob is charged against a chunk: its
// tree size, or the largest eligible size when the tree gave none.
func gitHubFileDeclaredBytes(size *int) int {
	if size == nil {
		return gitHubFileContentMaxBytes
	}
	return *size
}

// gitHubFilesInventory lists every blob of treeRef: the recursive listing, or
// the directory walk when that listing is truncated or above the object cap.
func gitHubFilesInventory(
	ctx context.Context,
	client *providerfoundation.HTTPClient,
	root, treeRef, fullName string,
) ([]string, map[string]*int, bool, error) {
	var tree gitHubTreePayload
	treeErr := fetchObjectRoute(ctx, client, githubTreeRoute, root+"/git/trees/"+url.PathEscape(treeRef)+"?recursive=true", &tree)
	var tooLarge *providerfoundation.ObjectTooLargeError
	treeCapExceeded := errors.As(treeErr, &tooLarge)
	if treeCapExceeded {
		slog.Warn(
			"github files tree response exceeded the shared object cap; falling back to non-recursive walk",
			"repository", fullName, "path", tooLarge.Path,
			"cap_bytes", tooLarge.CapBytes, "content_length", tooLarge.ContentLength,
		)
	} else if treeErr != nil {
		return nil, nil, false, continueGitHubFilesTraversal(treeErr, fullName)
	}
	if treeCapExceeded || tree.Truncated {
		paths, sizes, walkErr := gitHubWalkTree(ctx, client, root, treeRef, "")
		if walkErr != nil {
			return nil, nil, true, continueGitHubFilesTraversal(fmt.Errorf("ref=%s: %w", treeRef, walkErr), fullName)
		}
		return paths, sizes, true, nil
	}
	paths := make([]string, 0, len(tree.Tree))
	sizes := make(map[string]*int, len(tree.Tree))
	for _, entry := range tree.Tree {
		if entry.Type != "blob" || entry.Path == "" {
			continue
		}
		paths = append(paths, entry.Path)
		sizes[entry.Path] = entry.Size
	}
	return paths, sizes, false, nil
}

// gitHubWalkTree recovers a complete blob inventory when the recursive tree
// listing reports truncated. It re-fetches the same tree object without
// recursive=1 and descends into every "tree" entry by its own sha, joining
// each level's path onto prefix. A subtree that itself reports truncated, or
// any fetch that fails, aborts the whole walk instead of returning the paths
// already collected -- a partial walk is exactly the incomplete inventory
// this recovery exists to avoid.
func gitHubWalkTree(
	ctx context.Context,
	client *providerfoundation.HTTPClient,
	root, sha, prefix string,
) ([]string, map[string]*int, error) {
	var tree gitHubTreePayload
	if err := fetchObjectRoute(ctx, client, githubTreeRoute, root+"/git/trees/"+url.PathEscape(sha), &tree); err != nil {
		return nil, nil, fmt.Errorf("path=%q: %w", prefix, err)
	}
	if tree.Truncated {
		return nil, nil, fmt.Errorf("path=%q: %w", prefix, ErrGitHubFilesSubtreeTruncated)
	}
	paths := make([]string, 0, len(tree.Tree))
	sizes := make(map[string]*int, len(tree.Tree))
	for _, entry := range tree.Tree {
		if entry.Path == "" {
			continue
		}
		fullPath := entry.Path
		if prefix != "" {
			fullPath = prefix + "/" + entry.Path
		}
		switch entry.Type {
		case "blob":
			paths = append(paths, fullPath)
			sizes[fullPath] = entry.Size
		case "tree":
			if entry.SHA == "" {
				return nil, nil, fmt.Errorf("path=%q: %w", fullPath, providerfoundation.ErrNormalizationInvalid)
			}
			childPaths, childSizes, err := gitHubWalkTree(ctx, client, root, entry.SHA, fullPath)
			if err != nil {
				return nil, nil, err
			}
			paths = append(paths, childPaths...)
			for childPath, size := range childSizes {
				sizes[childPath] = size
			}
		}
	}
	return paths, sizes, nil
}

func continueGitHubFilesTraversal(err error, repository string) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	var providerErr *providerfoundation.ProviderError
	if errors.As(err, &providerErr) && providerErr.Class == providerfoundation.ErrorRateLimited {
		return err
	}
	slog.Error("github files traversal failed", "repository", repository, "inventory_status", "failed", "error", err)
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
) (string, error) {
	if beforeAt != nil {
		query := url.Values{"per_page": {"1"}, "sha": {branch}, "until": {beforeAt.UTC().Format(time.RFC3339Nano)}}
		var commits []struct {
			SHA string `json:"sha"`
		}
		if err := fetchObjectRoute(ctx, client, githubCommitsRoute, root+"/commits?"+query.Encode(), &commits); err != nil {
			return "", err
		}
		if len(commits) == 0 {
			return "", nil
		}
		return commits[0].SHA, nil
	}
	var branchPayload gitHubBranchPayload
	if err := fetchObjectRoute(ctx, client, githubBranchRoute, root+"/branches/"+url.PathEscape(branch), &branchPayload); err != nil {
		return "", err
	}
	return branchPayload.Commit.SHA, nil
}

func gitHubFilesFinalBatch(claim Claim, fullName string, cursor githubFilesChunkCursor) (CompleteRouteBatch, error) {
	effect, err := effectBatchFromValues("git_files", EffectReadbackRequired, []gitFileRow(nil))
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	inventoryStatus := "complete"
	pages := 1
	if cursor.Rows == 0 {
		inventoryStatus = "empty"
		if cursor.NoCommit {
			inventoryStatus, pages = "no_commit_at_bound", 0
		}
	}
	slog.Info(
		"github files inventory", "repository", fullName, "entries", cursor.Rows,
		"subtree_walk", cursor.Walked, "inventory_status", inventoryStatus,
		"chunks", cursor.Chunks, "with_contents", cursor.Contents, "contents_oversize", cursor.Oversize,
		"contents_unavailable", cursor.Unavailable,
		"paths_total", cursor.PathsTotal, "eligible_total", cursor.EligibleTotal, "pin_lost", cursor.PinLost,
	)
	return CompleteRouteBatch{
		Effects: []EffectBatch{effect},
		Result: map[string]any{
			"files_synced": cursor.Rows, "inventory_status": inventoryStatus, "repo": fullName,
			"files_with_contents": cursor.Contents, "contents_oversize": cursor.Oversize,
			"contents_unavailable": cursor.Unavailable, "paths_total": cursor.PathsTotal,
			"eligible_total": cursor.EligibleTotal, "pin_lost": cursor.PinLost,
			"content_chunks": cursor.Chunks,
		},
		Watermark: claim.BeforeAt,
		Evidence: FetchEvidence{
			Provider: claim.Provider, Dataset: claim.Dataset, Requests: cursor.Requests,
			Pages: pages, Records: cursor.Rows,
		},
	}, nil
}

func fetchGitHubFileContents(
	ctx context.Context,
	client *providerfoundation.HTTPClient,
	owner, repository, ref string,
	paths []string,
	sizes map[string]*int,
) (map[string]string, error) {
	scannable := make([]string, 0, len(paths))
	for _, filePath := range paths {
		if gitHubFileContentEligible(filePath, sizes[filePath]) {
			scannable = append(scannable, filePath)
		}
	}
	contents := make(map[string]string)
	for start := 0; start < len(scannable); {
		// A request holds at most gitHubFileContentBatch blobs and, by their
		// declared sizes, at most gitHubFileChunkMaxBytes of text (one blob
		// always fits), far below the response cap.
		end, declared := start, 0
		for end < len(scannable) && end-start < gitHubFileContentBatch {
			cost := gitHubFileDeclaredBytes(sizes[scannable[end]])
			if end > start && declared+cost > gitHubFileChunkMaxBytes {
				break
			}
			declared += cost
			end++
		}
		chunk := scannable[start:end]
		start = end
		body, err := json.Marshal(map[string]any{
			"query":     gitHubBlobTextsQuery(ref, chunk),
			"variables": map[string]string{"owner": owner, "repo": repository},
		})
		if err != nil {
			return nil, providerfoundation.ErrNormalizationInvalid
		}
		response, err := client.DoRoute(providerfoundation.WithBudgetDimension(ctx, BudgetContentsBlob), "POST", graphQLRoute, gitHubGraphQLURL(client), bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
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
		raw, readErr := providerfoundation.ReadResponseBody(response, providerResponseMaxBytes)
		closeErr := response.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if len(raw) > providerResponseMaxBytes {
			return nil, providerfoundation.ResponseInvalid(response, providerfoundation.ReasonBodyTooLarge)
		}
		if decodeErr := providerfoundation.DecodeResponseJSON(response, raw, &envelope, 0); decodeErr != nil {
			return nil, decodeErr
		}
		if closeErr != nil || len(envelope.Errors) > 0 {
			return nil, providerfoundation.ResponseInvalid(response, providerfoundation.ReasonBodyDecode)
		}
		for index, filePath := range chunk {
			blob, ok := envelope.Data.Repository["f"+strconv.Itoa(index)]
			if !ok || blob.IsBinary || blob.IsTruncated || blob.Text == nil {
				continue
			}
			contents[filePath] = *blob.Text
		}
	}
	return contents, nil
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

// gitHubGraphQLURL is the GraphQL endpoint as an absolute URL: it sits BESIDE a
// GitHub Enterprise /api/v3 base (/api/graphql), so it cannot be a path joined
// under the base path.
func gitHubGraphQLURL(client *providerfoundation.HTTPClient) string {
	return (&url.URL{Scheme: client.BaseURL.Scheme, Host: client.BaseURL.Host}).String() + gitHubGraphQLPath(client)
}

// gitHubGraphQLPath is that endpoint's path on the base URL's host.
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

// gitHubFilesWidestInstant is the longest RFC3339Nano instant (nine fractional
// digits). The executor stamps the chunk ledger with its own nanosecond clock
// while a route only knows a millisecond instant, so the pre-check runs on the
// widest encoding: it is never smaller than what production measures.
var gitHubFilesWidestInstant = time.Date(2026, 12, 31, 23, 59, 59, 999_999_999, time.UTC)

// gitHubFilesPreparedChunk is the one prepared chunk the executor builds for a
// non-final emission: the same split, the same cursor stamping and the same
// payload sizing it uses. ok is false when the split yields more than one.
func gitHubFilesPreparedChunk(effect EffectBatch, before, after string) (PreparedProviderChunk, bool) {
	chunks, err := splitCompleteRouteBatch(CompleteRouteBatch{Effects: []EffectBatch{effect}}, ShadowComparison{}, GitHubFilesChunkPolicy())
	if err != nil || len(chunks) != 1 {
		return PreparedProviderChunk{}, false
	}
	resetEmissionChunks(chunks, ChunkRouteEmission{CursorBefore: before, CursorAfter: after})
	if sizeEmissionChunks(chunks) != nil {
		return PreparedProviderChunk{}, false
	}
	chunks[0].Ordinal = 99999
	chunks[0].ManifestDigest = preparedChunkDigest(chunks[0])
	return chunks[0], true
}

// gitHubFilesChunkFits runs the chunk store's own material builder and
// validation on that chunk, with the widest clock. No estimate: what the store
// would refuse is refused here first.
func gitHubFilesChunkFits(claim Claim, effect EffectBatch, before, after string) bool {
	chunk, ok := gitHubFilesPreparedChunk(effect, before, after)
	if !ok {
		return false
	}
	_, err := buildPreparedChunkMaterial(claim, chunk, gitHubFilesWidestInstant)
	return err == nil
}
