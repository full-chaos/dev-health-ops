package providersync

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"slices"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

const gitHubBlameMaxFiles = 500

var (
	ErrGitHubBlameTraversalFailed     = errors.New("github blame traversal failed")
	ErrGitHubBlameIncomplete          = errors.New("github blame inventory incomplete")
	ErrGitHubBlameProgressUnavailable = errors.New("github blame incremental progress unavailable")
)

// gitBlameRow mirrors ClickHouseStore.insert_blame_data's complete git_blame
// projection. Nullable provider fields remain pointers so readback can
// distinguish NULL from an empty string after a crash.
type gitBlameRow struct {
	RepoID      string     `json:"repo_id"`
	Path        string     `json:"path"`
	LineNo      uint32     `json:"line_no"`
	AuthorEmail *string    `json:"author_email"`
	AuthorName  *string    `json:"author_name"`
	AuthorWhen  *time.Time `json:"author_when"`
	CommitHash  *string    `json:"commit_hash"`
	Line        *string    `json:"line"`
	LastSynced  time.Time  `json:"last_synced"`
	OrgID       string     `json:"org_id"`
}

type gitHubBlameEnvelope struct {
	Data struct {
		Repository struct {
			Object struct {
				Blame struct {
					Ranges []gitHubBlameRange `json:"ranges"`
				} `json:"blame"`
			} `json:"object"`
		} `json:"repository"`
	} `json:"data"`
	Errors []struct {
		Message string `json:"message"`
	} `json:"errors"`
}

type gitHubBlameRange struct {
	StartingLine uint32 `json:"startingLine"`
	EndingLine   uint32 `json:"endingLine"`
	Commit       struct {
		OID    string `json:"oid"`
		Author struct {
			Name  *string `json:"name"`
			Email *string `json:"email"`
		} `json:"author"`
	} `json:"commit"`
}

const (
	gitHubBlameOutcomeRows           = "rows"
	gitHubBlameOutcomeEmpty          = "empty"
	gitHubBlameOutcomeRetryableError = "retryable_error"
)

// gitHubBlamePathProgressRow is a durable path-level attempt marker. It is a
// separate effect from git_blame: empty files must advance coverage without a
// fabricated line, while failed files remain retryable and carry enough
// history to rotate behind paths that have never been attempted.
type gitHubBlamePathProgressRow struct {
	OrgID       string    `json:"org_id"`
	RepoID      string    `json:"repo_id"`
	TreeRef     string    `json:"tree_ref"`
	Path        string    `json:"path"`
	Generation  string    `json:"generation"`
	Outcome     string    `json:"outcome"`
	AttemptedAt time.Time `json:"attempted_at"`
}

func (row gitHubBlamePathProgressRow) validate(claim Claim) error {
	if claim.Validate() != nil || row.OrgID == "" || row.OrgID != claim.OrgID ||
		row.RepoID == "" || row.TreeRef == "" || row.Path == "" ||
		row.Generation != claim.GenerationKey() || row.AttemptedAt.IsZero() {
		return ErrInvalidConfiguration
	}
	switch row.Outcome {
	case gitHubBlameOutcomeRows, gitHubBlameOutcomeEmpty, gitHubBlameOutcomeRetryableError:
		return nil
	default:
		return ErrInvalidConfiguration
	}
}

type GitHubBlameProgressState struct {
	BlamedPaths      []string
	EmptyPaths       []string
	FailedAttempts   map[string]uint64
	InFlightOutcomes map[string]string
}

// GitHubBlameCoverage provides the durable, tenant-scoped progress boundary
// used to select the next bounded set of unblamed paths.
type GitHubBlameCoverage interface {
	Progress(context.Context, Claim, string, string, string) (GitHubBlameProgressState, error)
}

type GitHubBlameRouteHandler struct {
	Coverage GitHubBlameCoverage
	// MaxFiles is test-configurable below the production ceiling. Zero uses
	// gitHubBlameMaxFiles; values above that ceiling fail closed.
	MaxFiles int
}

func (handler GitHubBlameRouteHandler) Collect(
	ctx context.Context,
	claim Claim,
	_ providerfoundation.Credential,
	client *providerfoundation.HTTPClient,
	normalizedAt time.Time,
) (CompleteRouteBatch, error) {
	if err := validateGitHubBlameCollectInputs(ctx, claim, client, normalizedAt); err != nil {
		return CompleteRouteBatch{}, err
	}
	if handler.Coverage == nil {
		return CompleteRouteBatch{}, ErrGitHubBlameProgressUnavailable
	}
	maxFiles := handler.MaxFiles
	if maxFiles == 0 {
		maxFiles = gitHubBlameMaxFiles
	}
	if maxFiles < 1 || maxFiles > gitHubBlameMaxFiles {
		return CompleteRouteBatch{}, ErrInvalidConfiguration
	}
	return collectGitHubBlame(
		ctx, claim, client, normalizedAt, handler.Coverage, maxFiles, false, "",
	)
}

func (handler GitHubBlameRouteHandler) CollectRecovery(
	ctx context.Context,
	claim Claim,
	_ providerfoundation.Credential,
	client *providerfoundation.HTTPClient,
	normalizedAt time.Time,
	state EffectLedgerState,
) (CompleteRouteBatch, error) {
	if state.validate() != nil || state.Generation != claim.GenerationKey() ||
		state.Provider != claim.Provider || state.Dataset != claim.Dataset {
		return CompleteRouteBatch{}, ErrEffectLedgerConflict
	}
	if handler.Coverage == nil {
		return CompleteRouteBatch{}, ErrGitHubBlameProgressUnavailable
	}
	maxFiles := handler.MaxFiles
	if maxFiles == 0 {
		maxFiles = gitHubBlameMaxFiles
	}
	return collectGitHubBlame(
		ctx, claim, client, normalizedAt, handler.Coverage, maxFiles, false,
		state.Generation,
	)
}

// collectGitHubBlameFoundation retains the bounded fetch and normalization
// foundation for parity tests. Production must call GitHubBlameRouteHandler,
// which remains fail-closed until persisted incremental selection is wired.
func collectGitHubBlameFoundation(
	ctx context.Context,
	claim Claim,
	client *providerfoundation.HTTPClient,
	normalizedAt time.Time,
) (CompleteRouteBatch, error) {
	return collectGitHubBlame(
		ctx, claim, client, normalizedAt, nil, gitHubBlameMaxFiles, true, "",
	)
}

func collectGitHubBlame(
	ctx context.Context,
	claim Claim,
	client *providerfoundation.HTTPClient,
	normalizedAt time.Time,
	coverage GitHubBlameCoverage,
	maxFiles int,
	requireCompleteInventory bool,
	recoveryGeneration string,
) (CompleteRouteBatch, error) {
	if err := validateGitHubBlameCollectInputs(ctx, claim, client, normalizedAt); err != nil {
		return CompleteRouteBatch{}, err
	}
	normalizedAt = normalizedAt.UTC().Truncate(time.Millisecond)
	owner, repository, err := splitGitHubRepository(claim.SourceExternalID)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	root := providerRelativePath(client, "repos", owner, repository)
	var repoPayload gitHubRepositoryPayload
	if err := fetchObject(ctx, client, root, &repoPayload); err != nil {
		return CompleteRouteBatch{}, fmt.Errorf("%w: %w", ErrGitHubBlameTraversalFailed, err)
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
		return CompleteRouteBatch{}, fmt.Errorf("%w: %w", ErrGitHubBlameTraversalFailed, err)
	}
	if treeRef == "" {
		return gitHubBlameBatch(claim, repoPayload.FullName, nil, nil, requests, 0, 0, 0)
	}
	var tree gitHubTreePayload
	if err := fetchObject(ctx, client, root+"/git/trees/"+url.PathEscape(treeRef)+"?recursive=true", &tree); err != nil {
		return CompleteRouteBatch{}, fmt.Errorf("%w: %w", ErrGitHubBlameTraversalFailed, err)
	}
	requests++
	paths := make([]string, 0, len(tree.Tree))
	for _, entry := range tree.Tree {
		if entry.Type == "blob" && entry.Path != "" {
			paths = append(paths, entry.Path)
		}
	}
	if requireCompleteInventory && len(paths) > maxFiles {
		return CompleteRouteBatch{}, fmt.Errorf(
			"%w: repository has %d files, maximum complete unit is %d",
			ErrGitHubBlameIncomplete, len(paths), maxFiles,
		)
	}
	remainingPaths := 0
	progress := GitHubBlameProgressState{}
	if !requireCompleteInventory {
		if coverage == nil {
			return CompleteRouteBatch{}, ErrGitHubBlameProgressUnavailable
		}
		progress, err = coverage.Progress(
			ctx, claim, repoID, treeRef, recoveryGeneration,
		)
		if err != nil {
			return CompleteRouteBatch{}, fmt.Errorf("%w: %w", ErrGitHubBlameProgressUnavailable, err)
		}
		paths, remainingPaths, err = selectNextGitHubBlamePathsWithProgress(
			paths, progress, maxFiles, recoveryGeneration != "",
		)
		if err != nil {
			return CompleteRouteBatch{}, err
		}
	}
	rows := make([]gitBlameRow, 0)
	progressRows := make([]gitHubBlamePathProgressRow, 0, len(paths))
	retryableFailures := 0
	for _, filePath := range paths {
		if outcome := progress.InFlightOutcomes[filePath]; recoveryGeneration != "" &&
			(outcome == gitHubBlameOutcomeEmpty || outcome == gitHubBlameOutcomeRetryableError) {
			progressRows = append(progressRows, newGitHubBlamePathProgressRow(
				claim, repoID, treeRef, filePath, outcome, normalizedAt,
			))
			if outcome == gitHubBlameOutcomeRetryableError {
				retryableFailures++
			}
			continue
		}
		ranges, err := fetchGitHubBlame(ctx, client, owner, repository, treeRef, filePath)
		requests++
		if err != nil {
			if isRateLimited(err) {
				return CompleteRouteBatch{}, fmt.Errorf("%w for %s: %w", ErrGitHubBlameTraversalFailed, filePath, err)
			}
			progressRows = append(progressRows, newGitHubBlamePathProgressRow(
				claim, repoID, treeRef, filePath, gitHubBlameOutcomeRetryableError,
				normalizedAt,
			))
			retryableFailures++
			remainingPaths++
			continue
		}
		outcome := gitHubBlameOutcomeEmpty
		if len(ranges) > 0 {
			outcome = gitHubBlameOutcomeRows
		}
		progressRows = append(progressRows, newGitHubBlamePathProgressRow(
			claim, repoID, treeRef, filePath, outcome, normalizedAt,
		))
		for _, blameRange := range ranges {
			if blameRange.StartingLine == 0 || blameRange.EndingLine < blameRange.StartingLine {
				return CompleteRouteBatch{}, providerfoundation.ErrNormalizationInvalid
			}
			for lineNo := blameRange.StartingLine; lineNo <= blameRange.EndingLine; lineNo++ {
				row := newGitHubBlameRow(claim, repoID, filePath, lineNo, blameRange, normalizedAt)
				if err := row.validate(claim); err != nil {
					return CompleteRouteBatch{}, err
				}
				rows = append(rows, row)
				if lineNo == ^uint32(0) {
					break
				}
			}
		}
	}
	return gitHubBlameBatch(
		claim, repoPayload.FullName, rows, progressRows, requests, 1,
		remainingPaths, retryableFailures,
	)
}

func newGitHubBlamePathProgressRow(
	claim Claim,
	repoID, treeRef, path, outcome string,
	normalizedAt time.Time,
) gitHubBlamePathProgressRow {
	return gitHubBlamePathProgressRow{
		OrgID: claim.OrgID, RepoID: repoID, TreeRef: treeRef, Path: path,
		Generation: claim.GenerationKey(), Outcome: outcome,
		AttemptedAt: normalizedAt.UTC().Truncate(time.Millisecond),
	}
}

func selectNextGitHubBlamePaths(
	filePaths []string,
	blamedPaths []string,
	maxFiles int,
) ([]string, int, error) {
	return selectNextGitHubBlamePathsWithProgress(
		filePaths, GitHubBlameProgressState{BlamedPaths: blamedPaths}, maxFiles, false,
	)
}

func selectNextGitHubBlamePathsWithProgress(
	filePaths []string,
	progress GitHubBlameProgressState,
	maxFiles int,
	recovering bool,
) ([]string, int, error) {
	if maxFiles < 1 || maxFiles > gitHubBlameMaxFiles {
		return nil, 0, ErrInvalidConfiguration
	}
	if recovering && len(progress.InFlightOutcomes) > 0 {
		selected := make([]string, 0, len(progress.InFlightOutcomes))
		remaining := 0
		blamed := make(map[string]struct{}, len(progress.BlamedPaths))
		for _, path := range progress.BlamedPaths {
			blamed[path] = struct{}{}
		}
		empty := make(map[string]struct{}, len(progress.EmptyPaths))
		for _, path := range progress.EmptyPaths {
			empty[path] = struct{}{}
		}
		for _, path := range filePaths {
			if outcome, exists := progress.InFlightOutcomes[path]; exists {
				selected = append(selected, path)
				if outcome == gitHubBlameOutcomeRetryableError {
					remaining++
				}
				continue
			}
			if _, exists := blamed[path]; exists {
				continue
			}
			if _, exists := empty[path]; exists {
				continue
			}
			remaining++
		}
		if len(selected) != len(progress.InFlightOutcomes) || len(selected) > maxFiles {
			return nil, 0, ErrEffectLedgerConflict
		}
		return selected, remaining, nil
	}
	completed := make(map[string]struct{}, len(progress.BlamedPaths)+len(progress.EmptyPaths))
	for _, path := range progress.BlamedPaths {
		if path != "" {
			completed[path] = struct{}{}
		}
	}
	for _, path := range progress.EmptyPaths {
		if path != "" {
			completed[path] = struct{}{}
		}
	}
	neverAttempted := make([]string, 0, len(filePaths))
	failed := make([]string, 0, len(filePaths))
	for _, path := range filePaths {
		if _, exists := completed[path]; exists {
			continue
		}
		if progress.FailedAttempts[path] == 0 {
			neverAttempted = append(neverAttempted, path)
		} else {
			failed = append(failed, path)
		}
	}
	slices.SortStableFunc(failed, func(left, right string) int {
		leftAttempts, rightAttempts := progress.FailedAttempts[left], progress.FailedAttempts[right]
		if leftAttempts < rightAttempts {
			return -1
		}
		if leftAttempts > rightAttempts {
			return 1
		}
		return 0
	})
	unblamed := append(neverAttempted, failed...)
	remaining := 0
	if len(unblamed) > maxFiles {
		remaining = len(unblamed) - maxFiles
		unblamed = unblamed[:maxFiles]
	}
	return unblamed, remaining, nil
}

func validateGitHubBlameCollectInputs(
	ctx context.Context,
	claim Claim,
	client *providerfoundation.HTTPClient,
	normalizedAt time.Time,
) error {
	if ctx == nil || claim.Validate() != nil || claim.Provider != "github" ||
		claim.Dataset != "blame" || client == nil || client.Provider != "github" ||
		client.BaseURL == nil || normalizedAt.IsZero() {
		return ErrInvalidConfiguration
	}
	return nil
}

func fetchGitHubBlame(
	ctx context.Context,
	client *providerfoundation.HTTPClient,
	owner, repository, ref, filePath string,
) ([]gitHubBlameRange, error) {
	body, err := json.Marshal(map[string]any{
		"query": `query($owner: String!, $repo: String!, $path: String!, $ref: String!) {
  repository(owner: $owner, name: $repo) {
    object(expression: $ref) {
      ... on Commit {
        blame(path: $path) {
          ranges {
            startingLine
            endingLine
            commit { oid author { name email } }
          }
        }
      }
    }
  }
}`,
		"variables": map[string]string{
			"owner": owner, "repo": repository, "path": filePath, "ref": ref,
		},
	})
	if err != nil {
		return nil, providerfoundation.ErrNormalizationInvalid
	}
	response, err := client.Do(ctx, "POST", gitHubGraphQLPath(client), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	bodyBytes, decodeErr := io.ReadAll(io.LimitReader(response.Body, nativeMaxObjectBytes+1))
	var envelope gitHubBlameEnvelope
	if decodeErr == nil && len(bodyBytes) <= nativeMaxObjectBytes {
		decodeErr = json.Unmarshal(bodyBytes, &envelope)
	} else if decodeErr == nil {
		decodeErr = providerfoundation.ErrNormalizationInvalid
	}
	closeErr := response.Body.Close()
	if decodeErr != nil || closeErr != nil {
		return nil, providerfoundation.ErrNormalizationInvalid
	}
	if len(envelope.Errors) > 0 {
		return nil, providerfoundation.ErrNormalizationInvalid
	}
	return envelope.Data.Repository.Object.Blame.Ranges, nil
}

func newGitHubBlameRow(
	claim Claim,
	repoID, filePath string,
	lineNo uint32,
	blameRange gitHubBlameRange,
	normalizedAt time.Time,
) gitBlameRow {
	unknown := "Unknown"
	empty := ""
	authorName := blameRange.Commit.Author.Name
	if authorName == nil {
		authorName = &unknown
	}
	authorEmail := blameRange.Commit.Author.Email
	if authorEmail == nil {
		authorEmail = &empty
	}
	commitHash := blameRange.Commit.OID
	return gitBlameRow{
		RepoID: repoID, Path: filePath, LineNo: lineNo,
		AuthorEmail: authorEmail, AuthorName: authorName, CommitHash: &commitHash,
		LastSynced: normalizedAt, OrgID: claim.OrgID,
	}
}

func gitHubBlameBatch(
	claim Claim,
	fullName string,
	rows []gitBlameRow,
	progressRows []gitHubBlamePathProgressRow,
	requests, pages, remainingPaths, retryableFailures int,
) (CompleteRouteBatch, error) {
	progressEffect, err := effectBatchFromValues(
		"github_blame_path_progress", EffectReadbackRequired, progressRows,
	)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	blameEffect, err := effectBatchFromValues("git_blame", EffectReadbackRequired, rows)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	status := "complete"
	if remainingPaths > 0 {
		status = "partial"
	}
	if len(rows) == 0 && remainingPaths == 0 {
		status = "empty"
		if pages == 0 {
			status = "no_commit_at_bound"
		}
	}
	return CompleteRouteBatch{
		Effects: []EffectBatch{progressEffect, blameEffect},
		Result: map[string]any{
			"blame_rows_synced": len(rows), "inventory_status": status, "repo": fullName,
			"remaining_paths": remainingPaths, "retryable_path_failures": retryableFailures,
		},
		Watermark: claim.BeforeAt,
		Evidence: FetchEvidence{
			Provider: claim.Provider, Dataset: claim.Dataset, Requests: requests,
			Pages: pages, Records: len(rows), CapReached: false,
		},
	}, nil
}

func (row gitBlameRow) validate(claim Claim) error {
	if claim.Validate() != nil || claim.Provider != "github" || claim.Dataset != "blame" ||
		row.RepoID == "" || row.Path == "" || row.LineNo == 0 || row.LastSynced.IsZero() ||
		row.OrgID == "" || row.OrgID != claim.OrgID || row.AuthorEmail == nil ||
		row.AuthorName == nil || row.CommitHash == nil {
		return ErrInvalidConfiguration
	}
	return nil
}

var _ CompleteRouteHandler = GitHubBlameRouteHandler{}
var _ RecoveringCompleteRouteHandler = GitHubBlameRouteHandler{}
