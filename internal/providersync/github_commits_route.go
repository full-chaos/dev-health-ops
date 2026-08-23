package providersync

import (
	"context"
	"encoding/json"
	"net/url"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

const defaultGitHubCommitsMax = 1_000

type gitCommitRow struct {
	OrgID          string    `json:"org_id"`
	RepoID         string    `json:"repo_id"`
	Hash           string    `json:"hash"`
	Message        *string   `json:"message"`
	AuthorName     string    `json:"author_name"`
	AuthorEmail    *string   `json:"author_email"`
	AuthorWhen     time.Time `json:"author_when"`
	CommitterName  string    `json:"committer_name"`
	CommitterEmail *string   `json:"committer_email"`
	CommitterWhen  time.Time `json:"committer_when"`
	Parents        uint32    `json:"parents"`
	LastSynced     time.Time `json:"last_synced"`
}

type gitHubCommitPayload struct {
	SHA       any               `json:"sha"`
	Commit    gitCommitPerson   `json:"commit"`
	Author    gitHubUser        `json:"author"`
	Committer gitHubUser        `json:"committer"`
	Parents   []json.RawMessage `json:"parents"`
}

type gitCommitPerson struct {
	Message   any               `json:"message"`
	Author    gitCommitIdentity `json:"author"`
	Committer gitCommitIdentity `json:"committer"`
}

type gitCommitIdentity struct {
	Name  any `json:"name"`
	Email any `json:"email"`
	Date  any `json:"date"`
}
type gitHubUser struct {
	Login any `json:"login"`
	Email any `json:"email"`
}

type GitHubCommitsRouteHandler struct{ MaxCommits int }

func (handler GitHubCommitsRouteHandler) Collect(ctx context.Context, claim Claim, _ providerfoundation.Credential, client *providerfoundation.HTTPClient, normalizedAt time.Time) (CompleteRouteBatch, error) {
	if ctx == nil || claim.Validate() != nil || claim.Provider != "github" || claim.Dataset != "commits" || client == nil || client.Provider != "github" || client.BaseURL == nil || normalizedAt.IsZero() {
		return CompleteRouteBatch{}, ErrInvalidConfiguration
	}
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
	max := handler.MaxCommits
	if max == 0 {
		max = defaultGitHubCommitsMax
	}
	if max < 1 {
		return CompleteRouteBatch{}, ErrInvalidConfiguration
	}
	pages := (max + nativePerPage - 1) / nativePerPage
	query := url.Values{"per_page": {"100"}}
	if claim.SinceAt != nil {
		query.Set("since", claim.SinceAt.UTC().Format(time.RFC3339))
	}
	if claim.BeforeAt != nil {
		query.Set("until", claim.BeforeAt.UTC().Format(time.RFC3339))
	}
	page, err := providerfoundation.CollectGitHubLinkPages(ctx, client, providerfoundation.GitHubPageOptions{Path: root + "/commits", Query: query, MaxPages: pages})
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	if page.PageBudgetExhausted {
		return CompleteRouteBatch{}, ErrPaginationCapExceeded
	}
	items := page.Items
	if len(items) > max {
		items = items[:max]
	}
	rows := make([]gitCommitRow, 0, len(items))
	for _, raw := range items {
		var payload gitHubCommitPayload
		decoder := json.NewDecoder(strings.NewReader(string(raw)))
		decoder.UseNumber()
		if decoder.Decode(&payload) != nil {
			return CompleteRouteBatch{}, providerfoundation.ErrNormalizationInvalid
		}
		row, ok := normalizeGitHubCommit(claim, repoID, payload, normalizedAt)
		if ok {
			rows = append(rows, row)
		}
	}
	effect, err := effectBatchFromValues("git_commits", EffectReadbackRequired, rows)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	return CompleteRouteBatch{Effects: []EffectBatch{effect}, Result: map[string]any{"commits_synced": len(rows), "repo": repoPayload.FullName}, Watermark: claim.BeforeAt, Evidence: FetchEvidence{Provider: claim.Provider, Dataset: claim.Dataset, Requests: page.Pages + 1, Pages: page.Pages, Records: len(rows), CapReached: page.PageBudgetExhausted}}, nil
}

func normalizeGitHubCommit(claim Claim, repoID string, commit gitHubCommitPayload, normalizedAt time.Time) (gitCommitRow, bool) {
	hash := stringValue(commit.SHA)
	if hash == "" {
		return gitCommitRow{}, false
	}
	authorWhen := parseCommitTime(commit.Commit.Author.Date)
	if authorWhen == nil {
		now := time.Now().UTC()
		authorWhen = &now
	}
	committerWhen := parseCommitTime(commit.Commit.Committer.Date)
	if committerWhen == nil {
		now := time.Now().UTC()
		committerWhen = &now
	}
	message := stringValue(commit.Commit.Message)
	return gitCommitRow{OrgID: claim.OrgID, RepoID: repoID, Hash: hash, Message: &message, AuthorName: preferredName(commit.Author.Login, commit.Commit.Author.Name), AuthorEmail: preferredEmail(commit.Commit.Author.Email, commit.Author.Email), AuthorWhen: *authorWhen, CommitterName: preferredName(commit.Committer.Login, commit.Commit.Committer.Name), CommitterEmail: preferredEmail(commit.Commit.Committer.Email, commit.Committer.Email), CommitterWhen: *committerWhen, Parents: uint32(len(commit.Parents)), LastSynced: normalizedAt.UTC().Truncate(time.Millisecond)}, true
}

func preferredName(primary, fallback any) string {
	if value := stringValue(primary); value != "" {
		return value
	}
	if value := stringValue(fallback); value != "" {
		return value
	}
	return "Unknown"
}
func preferredEmail(primary, fallback any) *string {
	if value := stringValue(primary); value != "" {
		return &value
	}
	if value := stringValue(fallback); value != "" {
		return &value
	}
	return nil
}
func parseCommitTime(value any) *time.Time {
	text := stringValue(value)
	if text == "" {
		return nil
	}
	parsed, err := time.Parse(time.RFC3339, strings.ReplaceAll(text, "Z", "+00:00"))
	if err != nil {
		return nil
	}
	return &parsed
}
func (row gitCommitRow) validate(claim Claim) error {
	if row.OrgID == "" || row.OrgID != claim.OrgID || len(row.RepoID) != 36 || row.Hash == "" || row.AuthorName == "" || row.CommitterName == "" || row.AuthorWhen.IsZero() || row.CommitterWhen.IsZero() || row.LastSynced.IsZero() {
		return providerfoundation.ErrInvalidScope
	}
	return nil
}

var _ CompleteRouteHandler = GitHubCommitsRouteHandler{}
