package providersync

import (
	"bytes"
	"context"
	"encoding/json"
	"net/url"
	"strconv"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

const (
	defaultGitLabCommitsPerPage  = 100
	defaultGitLabCommitsMaxPages = 10_000
)

type gitLabCommitPayload struct {
	ID            any      `json:"id"`
	ShortID       any      `json:"short_id"`
	Message       *string  `json:"message"`
	AuthorName    *string  `json:"author_name"`
	AuthoredDate  any      `json:"authored_date"`
	CommitterName *string  `json:"committer_name"`
	CommittedDate any      `json:"committed_date"`
	ParentIDs     []string `json:"parent_ids"`
}

// GitLabCommitsRouteHandler owns the complete native (gitlab, commits) unit.
// MaxPages defaults to the canonical Python client's accepted 10,000-page
// bound. Unlike Python's compatibility path, reaching that bound while more
// data is advertised fails closed so an incomplete traversal cannot advance
// the unit watermark.
type GitLabCommitsRouteHandler struct {
	PerPage  int
	MaxPages int
	Now      func() time.Time
}

func (handler GitLabCommitsRouteHandler) now() time.Time {
	if handler.Now != nil {
		return handler.Now().UTC()
	}
	return time.Now().UTC()
}

func (handler GitLabCommitsRouteHandler) Collect(
	ctx context.Context,
	claim Claim,
	_ providerfoundation.Credential,
	client *providerfoundation.HTTPClient,
	normalizedAt time.Time,
) (CompleteRouteBatch, error) {
	if ctx == nil || claim.Validate() != nil || claim.Provider != "gitlab" ||
		claim.Dataset != "commits" || client == nil || client.Provider != "gitlab" ||
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
	fullName := project.PathWithNS
	if fullName == "" {
		fullName = project.Name
	}
	repoID, err := repositoryIdentity(fullName)
	if err != nil {
		return CompleteRouteBatch{}, err
	}

	perPage := handler.PerPage
	if perPage == 0 {
		perPage = defaultGitLabCommitsPerPage
	}
	maxPages := handler.MaxPages
	if maxPages == 0 {
		maxPages = defaultGitLabCommitsMaxPages
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
	if page.CapReached {
		return CompleteRouteBatch{}, ErrPaginationCapExceeded
	}

	normalizedAt = normalizedAt.UTC().Truncate(time.Millisecond)
	rows := make([]gitCommitRow, 0, len(page.Items))
	for _, raw := range page.Items {
		payload, decodeErr := decodeGitLabCommit(raw)
		if decodeErr != nil {
			return CompleteRouteBatch{}, decodeErr
		}
		committedWhen := parseCommitTime(payload.CommittedDate)
		if committedWhen != nil && claim.BeforeAt != nil && committedWhen.After(claim.BeforeAt.UTC()) {
			continue
		}
		if committedWhen != nil && claim.SinceAt != nil && committedWhen.Before(claim.SinceAt.UTC()) {
			break
		}
		row, normalizeErr := handler.normalizeCommit(claim, repoID, payload, normalizedAt)
		if normalizeErr != nil {
			return CompleteRouteBatch{}, normalizeErr
		}
		rows = append(rows, row)
	}
	effect, err := effectBatchFromValues("git_commits", EffectReadbackRequired, rows)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	return CompleteRouteBatch{
		Effects: []EffectBatch{effect},
		Result: map[string]any{
			"commits_synced": len(rows),
			"repo":           fullName,
			"project_id":     parsedProjectID,
		},
		Watermark: claim.BeforeAt,
		Evidence: FetchEvidence{
			Provider: claim.Provider, Dataset: claim.Dataset,
			Requests: page.Pages + 1, Pages: page.Pages, Records: len(rows),
		},
	}, nil
}

func decodeGitLabCommit(raw json.RawMessage) (gitLabCommitPayload, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var payload gitLabCommitPayload
	if err := decoder.Decode(&payload); err != nil {
		return gitLabCommitPayload{}, providerfoundation.ErrNormalizationInvalid
	}
	if stringValue(payload.ID) == "" && stringValue(payload.ShortID) == "" {
		return gitLabCommitPayload{}, providerfoundation.ErrNormalizationInvalid
	}
	return payload, nil
}

func (handler GitLabCommitsRouteHandler) normalizeCommit(
	claim Claim,
	repoID string,
	payload gitLabCommitPayload,
	normalizedAt time.Time,
) (gitCommitRow, error) {
	authorWhen := parseCommitTime(payload.AuthoredDate)
	if authorWhen == nil {
		now := handler.now()
		authorWhen = &now
	}
	committerWhen := parseCommitTime(payload.CommittedDate)
	if committerWhen == nil {
		now := handler.now()
		committerWhen = &now
	}
	authorName := "Unknown"
	if payload.AuthorName != nil && *payload.AuthorName != "" {
		authorName = *payload.AuthorName
	}
	committerName := "Unknown"
	if payload.CommitterName != nil && *payload.CommitterName != "" {
		committerName = *payload.CommitterName
	}
	row := gitCommitRow{
		OrgID: claim.OrgID, RepoID: repoID,
		Hash: stringValue(payload.ID), Message: payload.Message,
		AuthorName: authorName, AuthorEmail: nil, AuthorWhen: *authorWhen,
		CommitterName: committerName, CommitterEmail: nil, CommitterWhen: *committerWhen,
		Parents: uint32(len(payload.ParentIDs)), LastSynced: normalizedAt,
	}
	if row.Hash == "" {
		row.Hash = stringValue(payload.ShortID)
	}
	if err := row.validate(claim); err != nil {
		return gitCommitRow{}, err
	}
	return row, nil
}

var _ CompleteRouteHandler = GitLabCommitsRouteHandler{}
