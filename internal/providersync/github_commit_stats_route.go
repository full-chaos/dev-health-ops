package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

const (
	defaultGitHubCommitStatsMaxCommits = 300
	defaultGitHubCommitStatsSampleSize = 50
)

// commitStatsRow is the git_commit_stats projection emitted by github's
// commit-stats unit. Python's _github_commit_stat_to_model leaves file modes
// unset; its ClickHouse sink supplies the same "unknown" defaults represented
// directly here. org_id is carried from the claim because migration 027 makes
// it part of the table's replacing key.
type commitStatsRow struct {
	OrgID       string    `json:"org_id"`
	RepoID      string    `json:"repo_id"`
	CommitHash  string    `json:"commit_hash"`
	FilePath    string    `json:"file_path"`
	Additions   int32     `json:"additions"`
	Deletions   int32     `json:"deletions"`
	OldFileMode string    `json:"old_file_mode"`
	NewFileMode string    `json:"new_file_mode"`
	LastSynced  time.Time `json:"last_synced"`
}

type gitHubCommitListPayload struct {
	SHA    string `json:"sha"`
	Commit struct {
		Author struct {
			Date *string `json:"date"`
		} `json:"author"`
		Committer struct {
			Date *string `json:"date"`
		} `json:"committer"`
	} `json:"commit"`
}

type gitHubCommitDetailPayload struct {
	Files []gitHubCommitFilePayload `json:"files"`
}

type gitHubCommitFilePayload struct {
	Filename  string `json:"filename"`
	Additions int32  `json:"additions"`
	Deletions int32  `json:"deletions"`
}

// GitHubCommitStatsRouteHandler mirrors _fetch_github_commits_async followed
// by _sync_github_commit_stats: it lists commits with the Python window
// parameters, then makes one sequential detail request per retained commit.
// MaxCommits is the upstream sync cap; a zero value uses Python's configured
// hard ceiling, while an incremental window that exceeds either cap is skipped
// rather than written partially.
type GitHubCommitStatsRouteHandler struct{ MaxCommits int }

func (handler GitHubCommitStatsRouteHandler) Collect(
	ctx context.Context,
	claim Claim,
	_ providerfoundation.Credential,
	client *providerfoundation.HTTPClient,
	normalizedAt time.Time,
) (CompleteRouteBatch, error) {
	if ctx == nil || claim.Validate() != nil || claim.Provider != "github" ||
		claim.Dataset != "commit-stats" || client == nil || client.Provider != "github" ||
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

	maxCommits := handler.MaxCommits
	if maxCommits == 0 {
		maxCommits = defaultGitHubCommitStatsMaxCommits
	}
	if maxCommits < 1 {
		return CompleteRouteBatch{}, ErrInvalidConfiguration
	}
	fetchLimit := maxCommits + 1
	pages := (fetchLimit + nativePerPage - 1) / nativePerPage
	query := url.Values{"per_page": {"100"}}
	if claim.SinceAt != nil {
		query.Set("since", claim.SinceAt.UTC().Format(time.RFC3339Nano))
	}
	if claim.BeforeAt != nil {
		query.Set("until", claim.BeforeAt.UTC().Format(time.RFC3339Nano))
	}
	page, err := providerfoundation.CollectGitHubLinkPages(
		ctx,
		client,
		providerfoundation.GitHubPageOptions{
			Path: root + "/commits", Query: query, MaxPages: pages,
		},
	)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	windowTruncated := len(page.Items) >= fetchLimit
	if windowTruncated {
		page.Items = page.Items[:maxCommits]
	}
	if claim.SinceAt != nil && (windowTruncated || len(page.Items) > defaultGitHubCommitStatsMaxCommits) {
		return handler.completeBatch(claim, repoPayload.FullName, nil, page.Pages, page.Pages+1, true)
	}

	statsLimit := defaultGitHubCommitStatsSampleSize
	if claim.SinceAt != nil {
		statsLimit = len(page.Items)
	}
	if statsLimit > maxCommits {
		statsLimit = maxCommits
	}
	if statsLimit > defaultGitHubCommitStatsMaxCommits {
		statsLimit = defaultGitHubCommitStatsMaxCommits
	}
	rows := make([]commitStatsRow, 0)
	detailRequests := 0
	for _, raw := range page.Items[:min(statsLimit, len(page.Items))] {
		var commit gitHubCommitListPayload
		decoder := json.NewDecoder(strings.NewReader(string(raw)))
		decoder.UseNumber()
		if decoder.Decode(&commit) != nil || commit.SHA == "" {
			return CompleteRouteBatch{}, providerfoundation.ErrNormalizationInvalid
		}
		if commitStatsBeforeSince(commit, claim.SinceAt) {
			continue
		}
		var detail gitHubCommitDetailPayload
		if err := fetchObject(ctx, client, root+"/commits/"+url.PathEscape(commit.SHA), &detail); err != nil {
			if isRateLimited(err) {
				return CompleteRouteBatch{}, err
			}
			continue
		}
		detailRequests++
		for _, file := range detail.Files {
			if file.Filename == "" {
				continue
			}
			rows = append(rows, commitStatsRow{
				OrgID: claim.OrgID, RepoID: repoID, CommitHash: commit.SHA,
				FilePath: file.Filename, Additions: file.Additions, Deletions: file.Deletions,
				OldFileMode: "unknown", NewFileMode: "unknown", LastSynced: normalizedAt,
			})
		}
	}
	return handler.completeBatch(claim, repoPayload.FullName, rows, page.Pages, page.Pages+1+detailRequests, page.CapReached)
}

func (handler GitHubCommitStatsRouteHandler) completeBatch(
	claim Claim,
	fullName string,
	rows []commitStatsRow,
	pages int,
	requests int,
	capReached bool,
) (CompleteRouteBatch, error) {
	effect, err := effectBatchFromValues("git_commit_stats", EffectReadbackRequired, rows)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	return CompleteRouteBatch{
		Effects:   []EffectBatch{effect},
		Watermark: claim.BeforeAt,
		Result: map[string]any{
			"commit_stats_synced": len(rows),
			"repo":                fullName,
		},
		Evidence: FetchEvidence{
			Provider: claim.Provider, Dataset: claim.Dataset, Requests: requests,
			Pages: pages, Records: len(rows), CapReached: capReached,
		},
	}, nil
}

func commitStatsBeforeSince(commit gitHubCommitListPayload, since *time.Time) bool {
	if since == nil {
		return false
	}
	when := parseGitHubPullTime(commit.Commit.Committer.Date)
	if when == nil {
		when = parseGitHubPullTime(commit.Commit.Author.Date)
	}
	return when != nil && when.Before(since.UTC())
}

func isRateLimited(err error) bool {
	var providerErr *providerfoundation.ProviderError
	return errors.As(err, &providerErr) && providerErr.Class == providerfoundation.ErrorRateLimited
}

func (row commitStatsRow) validate(claim Claim) error {
	if row.OrgID == "" || row.OrgID != claim.OrgID || len(row.RepoID) != 36 ||
		row.CommitHash == "" || row.FilePath == "" || row.LastSynced.IsZero() {
		return providerfoundation.ErrInvalidScope
	}
	return nil
}

var _ CompleteRouteHandler = GitHubCommitStatsRouteHandler{}
