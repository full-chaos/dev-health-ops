package providersync

import (
	"context"
	"encoding/json"
	"math"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

const (
	defaultGitLabCommitStatsPerPage  = 100
	defaultGitLabCommitStatsMaxPages = 10_000
	defaultGitLabCommitStatsHardCap  = 300
	defaultGitLabCommitStatsFullSync = 50
	gitLabAggregateStatsMarker       = "__AGGREGATE__"
)

type gitLabCommitStatsPayload struct {
	Stats map[string]any `json:"stats"`
}

// GitLabCommitStatsRouteHandler mirrors Python's isolated commit-stats unit:
// it obtains the accepted commit window, then expands each selected hash with
// one aggregate detail request. MaxStats is Python's COMMIT_STATS_MAX_COMMITS
// hard ceiling; full-history runs retain Python's smaller 50-commit sample.
type GitLabCommitStatsRouteHandler struct {
	PerPage  int
	MaxPages int
	MaxStats int
}

func (handler GitLabCommitStatsRouteHandler) Collect(
	ctx context.Context,
	claim Claim,
	_ providerfoundation.Credential,
	client *providerfoundation.HTTPClient,
	normalizedAt time.Time,
) (CompleteRouteBatch, error) {
	if ctx == nil || claim.Validate() != nil || claim.Provider != "gitlab" ||
		claim.Dataset != "commit-stats" || client == nil || client.Provider != "gitlab" ||
		client.BaseURL == nil || normalizedAt.IsZero() {
		return CompleteRouteBatch{}, ErrInvalidConfiguration
	}
	projectID, err := gitLabProjectID(claim.SourceExternalID)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	root := providerRelativePath(client, "api", "v4", "projects", projectID)
	var project repositoryPayload
	if err := fetchObject(ctx, client, root, &project); err != nil {
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

	perPage := handler.PerPage
	if perPage == 0 {
		perPage = defaultGitLabCommitStatsPerPage
	}
	maxPages := handler.MaxPages
	if maxPages == 0 {
		maxPages = defaultGitLabCommitStatsMaxPages
	}
	maxStats := handler.MaxStats
	if maxStats == 0 {
		maxStats = defaultGitLabCommitStatsHardCap
	}
	if perPage < 1 || maxPages < 1 || maxStats < 1 {
		return CompleteRouteBatch{}, ErrInvalidConfiguration
	}
	query := url.Values{}
	if claim.SinceAt != nil {
		query.Set("since", claim.SinceAt.UTC().Format(time.RFC3339Nano))
	}
	if claim.BeforeAt != nil {
		query.Set("until", claim.BeforeAt.UTC().Format(time.RFC3339Nano))
	}
	page, err := providerfoundation.CollectGitLabPageParamPages(
		ctx,
		client,
		providerfoundation.GitLabPageOptions{
			Path: root + "/repository/commits", Query: query,
			PerPage: perPage, MaxPages: maxPages,
		},
	)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	if page.PageBudgetExhausted {
		return CompleteRouteBatch{}, ErrPaginationCapExceeded
	}

	commitHashes := make([]string, 0, len(page.Items))
	seenCommitHashes := make(map[string]struct{}, len(page.Items))
	for _, raw := range page.Items {
		commit, decodeErr := decodeGitLabCommit(raw)
		if decodeErr != nil {
			return CompleteRouteBatch{}, decodeErr
		}
		committedWhen := parseCommitTime(commit.CommittedDate)
		if committedWhen != nil && claim.BeforeAt != nil && committedWhen.After(claim.BeforeAt.UTC()) {
			continue
		}
		if committedWhen != nil && claim.SinceAt != nil && committedWhen.Before(claim.SinceAt.UTC()) {
			break
		}
		commitHash := stringValue(commit.ID)
		if commitHash == "" {
			commitHash = stringValue(commit.ShortID)
		}
		if _, duplicate := seenCommitHashes[commitHash]; duplicate {
			continue
		}
		seenCommitHashes[commitHash] = struct{}{}
		commitHashes = append(commitHashes, commitHash)
	}
	statsLimit := defaultGitLabCommitStatsFullSync
	if claim.SinceAt != nil {
		statsLimit = len(commitHashes)
	}
	statsLimit = min(statsLimit, maxStats, len(commitHashes))

	normalizedAt = normalizedAt.UTC().Truncate(time.Millisecond)
	rows := make([]commitStatsRow, 0, statsLimit)
	detailRequests := 0
	for _, commitHash := range commitHashes[:statsLimit] {
		detailRequests++
		var detail gitLabCommitStatsPayload
		if err := fetchObject(
			ctx, client, root+"/repository/commits/"+url.PathEscape(commitHash), &detail,
		); err != nil {
			// Preserve Python's accepted soft degradation only for ordinary
			// per-commit detail failures. Authentication, rate limits, lease or
			// budget loss, and cancellation are control-plane failures: none may
			// advance the watermark through an empty or partial effect.
			if fatalErr := gitLabCommitStatsFatalDetailError(ctx, err); fatalErr != nil {
				return CompleteRouteBatch{}, fatalErr
			}
			continue
		}
		row, normalizeErr := normalizeGitLabCommitStats(
			claim, repoID, commitHash, detail, normalizedAt,
		)
		if normalizeErr != nil {
			return CompleteRouteBatch{}, normalizeErr
		}
		rows = append(rows, row)
	}
	effect, err := effectBatchFromValues("git_commit_stats", EffectReadbackRequired, rows)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	return CompleteRouteBatch{
		Effects: []EffectBatch{effect},
		Result: map[string]any{
			"commit_stats_synced": len(rows),
			"repo":                fullName,
			"project_id":          parsedProjectID,
		},
		Watermark: claim.BeforeAt,
		Evidence: FetchEvidence{
			Provider: claim.Provider, Dataset: claim.Dataset,
			Requests: page.Pages + 1 + detailRequests,
			Pages:    page.Pages,
			Records:  len(rows),
		},
	}, nil
}

func gitLabCommitStatsFatalDetailError(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	// D16 preserves malformed individual detail payloads as soft degradation,
	// but only when every leaf is that normalization sentinel. A joined budget,
	// lease, context, or other control-plane error must still fail the unit.
	if gitLabErrorTreeOnlyLeaves(err, func(leaf error) bool {
		return leaf == providerfoundation.ErrNormalizationInvalid
	}) {
		return nil
	}
	if gitLabErrorTreeOnlyProviderClasses(
		err,
		providerfoundation.ErrorNotFound,
		providerfoundation.ErrorConflict,
		providerfoundation.ErrorTransient,
		providerfoundation.ErrorPermanent,
	) {
		return nil
	}
	return err
}

func normalizeGitLabCommitStats(
	claim Claim,
	repoID string,
	commitHash string,
	payload gitLabCommitStatsPayload,
	normalizedAt time.Time,
) (commitStatsRow, error) {
	additions, err := gitLabCommitStatInt32(payload.Stats["additions"])
	if err != nil {
		return commitStatsRow{}, err
	}
	deletions, err := gitLabCommitStatInt32(payload.Stats["deletions"])
	if err != nil {
		return commitStatsRow{}, err
	}
	row := commitStatsRow{
		OrgID: claim.OrgID, RepoID: repoID, CommitHash: commitHash,
		FilePath: gitLabAggregateStatsMarker, Additions: additions, Deletions: deletions,
		OldFileMode: "unknown", NewFileMode: "unknown", LastSynced: normalizedAt,
	}
	if err := row.validate(claim); err != nil {
		return commitStatsRow{}, err
	}
	return row, nil
}

func gitLabCommitStatInt32(value any) (int32, error) {
	var number float64
	switch typed := value.(type) {
	case nil:
		return 0, nil
	case bool:
		if typed {
			return 1, nil
		}
		return 0, nil
	case json.Number:
		parsed, err := typed.Float64()
		if err != nil {
			return 0, nil
		}
		number = parsed
	case string:
		parsed, err := strconv.ParseInt(strings.TrimSpace(typed), 10, 32)
		if err != nil {
			return 0, nil
		}
		return int32(parsed), nil
	case float64:
		number = typed
	default:
		return 0, nil
	}
	if math.IsNaN(number) || math.IsInf(number, 0) ||
		number < math.MinInt32 || number > math.MaxInt32 {
		return 0, providerfoundation.ErrNormalizationInvalid
	}
	return int32(number), nil
}

var _ CompleteRouteHandler = GitLabCommitStatsRouteHandler{}
