package providersync

import (
	"context"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// gitLabRepositorySettings preserves the insertion order of the Python
// process_gitlab_project settings dict. ClickHouse stores this document as a
// JSON string, so stable order also keeps effect digests stable across retry.
type gitLabRepositorySettings struct {
	Source            string  `json:"source"`
	ProjectID         int64   `json:"project_id"`
	URL               *string `json:"url"`
	DefaultBranch     string  `json:"default_branch"`
	GitLabInstanceURL string  `json:"gitlab_instance_url"`
}

// GitLabRepositoryRouteHandler owns the complete native
// (gitlab, repo-metadata) route. It mirrors the Python dataset adapter's
// unconditional project fetch and repository write while retaining Go's
// authoritative lease/effect lifecycle.
type GitLabRepositoryRouteHandler struct{}

func (GitLabRepositoryRouteHandler) Collect(
	ctx context.Context,
	claim Claim,
	_ providerfoundation.Credential,
	client *providerfoundation.HTTPClient,
	normalizedAt time.Time,
) (CompleteRouteBatch, error) {
	if ctx == nil || claim.Validate() != nil || claim.Provider != "gitlab" ||
		claim.Dataset != "repo-metadata" || client == nil ||
		client.Provider != "gitlab" || client.BaseURL == nil ||
		normalizedAt.IsZero() {
		return CompleteRouteBatch{}, ErrInvalidConfiguration
	}
	projectID, err := gitLabProjectID(claim.SourceExternalID)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	var payload repositoryPayload
	path := providerRelativePath(client, "api", "v4", "projects", projectID)
	if err := fetchObject(ctx, client, path, &payload); err != nil {
		return CompleteRouteBatch{}, err
	}
	parsedProjectID, err := payload.ID.Int64()
	if err != nil || parsedProjectID < 1 || strconv.FormatInt(parsedProjectID, 10) != projectID {
		return CompleteRouteBatch{}, providerfoundation.ErrNormalizationInvalid
	}
	fullName := payload.PathWithNS
	if fullName == "" {
		fullName = payload.Name
	}
	identity, err := repositoryIdentity(fullName)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	instance, ok := normalizedGitLabInstance(client.BaseURL)
	if !ok {
		return CompleteRouteBatch{}, ErrInvalidConfiguration
	}
	defaultBranch := payload.DefaultBranch
	if defaultBranch == "" {
		defaultBranch = "main"
	}
	var webURL *string
	if payload.WebURL != "" {
		value := payload.WebURL
		webURL = &value
	}
	settings, err := marshalRepositoryJSON(gitLabRepositorySettings{
		Source:            "gitlab",
		ProjectID:         parsedProjectID,
		URL:               webURL,
		DefaultBranch:     defaultBranch,
		GitLabInstanceURL: instance,
	})
	if err != nil {
		return CompleteRouteBatch{}, providerfoundation.ErrNormalizationInvalid
	}
	tags, err := marshalRepositoryJSON([]string{"gitlab"})
	if err != nil {
		return CompleteRouteBatch{}, providerfoundation.ErrNormalizationInvalid
	}
	normalizedAt = normalizedAt.UTC().Truncate(time.Millisecond)
	row := repositoryRow{
		ID: identity, OrgID: claim.OrgID, Repo: fullName,
		CreatedAt: normalizedAt, Settings: string(settings), Tags: string(tags),
		Provider: "gitlab", LastSynced: normalizedAt,
	}
	if err := row.validate(claim); err != nil {
		return CompleteRouteBatch{}, err
	}
	effect, err := effectBatchFromValues(
		"repos", EffectReadbackRequired, []repositoryRow{row},
	)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	return CompleteRouteBatch{
		Effects: []EffectBatch{effect},
		Result: map[string]any{
			"repos_synced": 1, "repo": fullName, "project_id": parsedProjectID,
			"default_branch": defaultBranch, "archived": payload.Archived,
		},
		Watermark: nil,
		Evidence: FetchEvidence{
			Provider: claim.Provider, Dataset: claim.Dataset,
			Requests: 1, Pages: 1, Records: 1,
		},
	}, nil
}

// normalizedGitLabInstance mirrors providers/gitlab/instance.py. Unlike the
// operational identity normalizer used by GitHub, this persisted discriminator
// includes the scheme because Python's GitLab work-item scope does too.
func normalizedGitLabInstance(base *url.URL) (string, bool) {
	if base == nil {
		return "", false
	}
	host := strings.ToLower(strings.TrimSpace(base.Hostname()))
	if host == "" {
		return "", false
	}
	scheme := strings.ToLower(strings.TrimSpace(base.Scheme))
	if scheme == "" {
		scheme = "https"
	}
	port := base.Port()
	if (scheme == "https" && port == "443") || (scheme == "http" && port == "80") {
		port = ""
	}
	if port != "" {
		value, err := strconv.Atoi(port)
		if err != nil || value < 1 || value > 65535 {
			return "", false
		}
		host += ":" + port
	}
	return scheme + "://" + host, true
}

var _ CompleteRouteHandler = GitLabRepositoryRouteHandler{}
