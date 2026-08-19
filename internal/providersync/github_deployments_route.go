package providersync

import (
	"context"
	"encoding/json"
	"math"
	"net/url"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

const defaultGitHubDeploymentsMax = 1_000

// deploymentRow mirrors Python's build_deployment -> insert_deployments boundary.
type deploymentRow struct {
	RepoID               string     `json:"repo_id"`
	DeploymentID         string     `json:"deployment_id"`
	Status               *string    `json:"status"`
	Environment          *string    `json:"environment"`
	StartedAt            *time.Time `json:"started_at"`
	FinishedAt           *time.Time `json:"finished_at"`
	DeployedAt           *time.Time `json:"deployed_at"`
	MergedAt             *time.Time `json:"merged_at"`
	PullRequestNumber    *int       `json:"pull_request_number"`
	ReleaseRef           string     `json:"release_ref"`
	ReleaseRefConfidence float64    `json:"release_ref_confidence"`
	OrgID                string     `json:"org_id"`
	LastSynced           time.Time  `json:"last_synced"`
}

type gitHubDeploymentPayload struct {
	ID          json.Number    `json:"id"`
	State       any            `json:"state"`
	Status      any            `json:"status"`
	Environment any            `json:"environment"`
	CreatedAt   *string        `json:"created_at"`
	SHA         *string        `json:"sha"`
	Ref         any            `json:"ref"`
	Tag         any            `json:"tag"`
	TagName     any            `json:"tag_name"`
	Payload     map[string]any `json:"payload"`
}

type gitHubReleasePayload struct {
	TagName any `json:"tag_name"`
}
type gitHubPullPayload struct {
	Number         any     `json:"number"`
	MergedAt       *string `json:"merged_at"`
	MergeCommitSHA any     `json:"merge_commit_sha"`
}

// GitHubDeploymentsRouteHandler mirrors _fetch_github_deployments_async. It
// owns only deployments, including Python's release and PR enrichment.
type GitHubDeploymentsRouteHandler struct{ MaxDeployments int }

func (handler GitHubDeploymentsRouteHandler) Collect(
	ctx context.Context,
	claim Claim,
	_ providerfoundation.Credential,
	client *providerfoundation.HTTPClient,
	normalizedAt time.Time,
) (CompleteRouteBatch, error) {
	if ctx == nil || claim.Validate() != nil || claim.Provider != "github" ||
		claim.Dataset != "deployments" || client == nil || client.Provider != "github" ||
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
	maxDeployments := handler.MaxDeployments
	if maxDeployments == 0 {
		maxDeployments = defaultGitHubDeploymentsMax
	}
	if maxDeployments < 1 {
		return CompleteRouteBatch{}, ErrInvalidConfiguration
	}
	pages := (maxDeployments + nativePerPage - 1) / nativePerPage
	releases, releasePages, err := fetchGitHubDeploymentsPage[gitHubReleasePayload](ctx, client, root+"/releases", pages)
	if err != nil {
		// Python treats release enrichment as best effort.
		releases, releasePages = nil, 0
	}
	deployments, deploymentPages, err := fetchGitHubDeploymentsPage[gitHubDeploymentPayload](ctx, client, root+"/deployments", pages)
	if err != nil {
		// Python logs and returns the successful empty batch for this optional collection.
		effect, effectErr := effectBatchFromValues("deployments", EffectReadbackRequired, []deploymentRow{})
		if effectErr != nil {
			return CompleteRouteBatch{}, effectErr
		}
		return CompleteRouteBatch{Effects: []EffectBatch{effect}, Result: map[string]any{"deployments_synced": 0, "repo": repoPayload.FullName}, Watermark: claim.BeforeAt, Evidence: FetchEvidence{Provider: claim.Provider, Dataset: claim.Dataset, Requests: releasePages + 1, Pages: releasePages, Records: 0}}, nil
	}
	if len(deployments) > maxDeployments {
		deployments = deployments[:maxDeployments]
	}
	rows := make([]deploymentRow, 0, len(deployments))
	for _, deployment := range deployments {
		row, include := normalizeGitHubDeployment(claim, repoID, deployment, releases, normalizedAt)
		if !include || deploymentOutsideWindow(row.DeployedAt, claim) {
			continue
		}
		if deployment.SHA != nil && strings.TrimSpace(*deployment.SHA) != "" {
			pulls, _, pullErr := fetchGitHubDeploymentsPage[gitHubPullPayload](ctx, client, root+"/commits/"+url.PathEscape(*deployment.SHA)+"/pulls", 1)
			if pullErr == nil {
				row.PullRequestNumber, row.MergedAt = chooseDeploymentPullRequest(pulls, *deployment.SHA)
			}
		}
		rows = append(rows, row)
	}
	effect, err := effectBatchFromValues("deployments", EffectReadbackRequired, rows)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	return CompleteRouteBatch{Effects: []EffectBatch{effect}, Result: map[string]any{"deployments_synced": len(rows), "repo": repoPayload.FullName}, Watermark: claim.BeforeAt, Evidence: FetchEvidence{Provider: claim.Provider, Dataset: claim.Dataset, Requests: releasePages + deploymentPages + 1, Pages: releasePages + deploymentPages, Records: len(rows)}}, nil
}

func fetchGitHubDeploymentsPage[T any](ctx context.Context, client *providerfoundation.HTTPClient, path string, maxPages int) ([]T, int, error) {
	page, err := providerfoundation.CollectGitHubLinkPages(ctx, client, providerfoundation.GitHubPageOptions{Path: path, Query: url.Values{"per_page": {"100"}}, MaxPages: maxPages})
	if err != nil {
		return nil, 0, err
	}
	items := make([]T, 0, len(page.Items))
	for _, raw := range page.Items {
		var item T
		decoder := json.NewDecoder(strings.NewReader(string(raw)))
		decoder.UseNumber()
		if decoder.Decode(&item) != nil {
			return nil, 0, providerfoundation.ErrNormalizationInvalid
		}
		items = append(items, item)
	}
	return items, page.Pages, nil
}

func normalizeGitHubDeployment(claim Claim, repoID string, deployment gitHubDeploymentPayload, releases []gitHubReleasePayload, normalizedAt time.Time) (deploymentRow, bool) {
	deployedAt := parseGitHubWorkflowTime(deployment.CreatedAt)
	if deployedAt == nil {
		return deploymentRow{}, false
	}
	releaseRef, confidence := deploymentReleaseRef(deployment, releases)
	return deploymentRow{RepoID: repoID, DeploymentID: stringValue(deployment.ID), Status: optionalString(deployment.State, deployment.Status), Environment: optionalString(deployment.Environment), StartedAt: deployedAt, DeployedAt: deployedAt, ReleaseRef: releaseRef, ReleaseRefConfidence: confidence, OrgID: claim.OrgID, LastSynced: normalizedAt}, true
}

func deploymentReleaseRef(deployment gitHubDeploymentPayload, releases []gitHubReleasePayload) (string, float64) {
	for _, candidate := range []any{deployment.TagName, deployment.Tag, deployment.Payload["release_ref"], deployment.Payload["release"], deployment.Payload["release_tag"], deployment.Payload["tag_name"], deployment.Payload["tag"], deployment.Payload["version"]} {
		if value := strings.TrimSpace(stringValue(candidate)); value != "" {
			return value, deploymentExplicitConfidence(deployment)
		}
	}
	candidates := map[string]struct{}{}
	for _, candidate := range []any{deployment.Ref, deployment.Tag, deployment.TagName, deployment.Payload["ref"], deployment.Payload["tag"], deployment.Payload["tag_name"], deployment.Payload["release_tag"]} {
		if value := strings.TrimSpace(stringValue(candidate)); value != "" {
			candidates[value] = struct{}{}
		}
	}
	for _, release := range releases {
		if tag := strings.TrimSpace(stringValue(release.TagName)); tag != "" {
			if _, ok := candidates[tag]; ok {
				return tag, 1
			}
		}
	}
	return stringValue(deployment.ID), 0.3
}

func deploymentExplicitConfidence(deployment gitHubDeploymentPayload) float64 {
	value, ok := deployment.Payload["release_ref_confidence"]
	if !ok {
		return 1
	}
	number, err := json.Number(stringValue(value)).Float64()
	if err != nil {
		return 1
	}
	if number < 0 {
		return 0
	}
	if number > 1 {
		return 1
	}
	return number
}

func optionalString(values ...any) *string {
	for _, value := range values {
		if text := strings.TrimSpace(stringValue(value)); text != "" {
			return &text
		}
	}
	return nil
}
func deploymentOutsideWindow(deployedAt *time.Time, claim Claim) bool {
	return deployedAt == nil || (claim.SinceAt != nil && deployedAt.Before(claim.SinceAt.UTC())) || (claim.BeforeAt != nil && deployedAt.After(claim.BeforeAt.UTC()))
}
func chooseDeploymentPullRequest(pulls []gitHubPullPayload, sha string) (*int, *time.Time) {
	var chosen *gitHubPullPayload
	for i := range pulls {
		if parseGitHubWorkflowTime(pulls[i].MergedAt) != nil && stringValue(pulls[i].MergeCommitSHA) == sha {
			chosen = &pulls[i]
			break
		}
	}
	if chosen == nil {
		for i := range pulls {
			if parseGitHubWorkflowTime(pulls[i].MergedAt) != nil {
				chosen = &pulls[i]
				break
			}
		}
	}
	if chosen == nil && len(pulls) > 0 {
		chosen = &pulls[0]
	}
	if chosen == nil {
		return nil, nil
	}
	number, err := json.Number(stringValue(chosen.Number)).Int64()
	if err != nil {
		return nil, parseGitHubWorkflowTime(chosen.MergedAt)
	}
	value := int(number)
	return &value, parseGitHubWorkflowTime(chosen.MergedAt)
}

func (row deploymentRow) validate(claim Claim) error {
	if row.OrgID == "" || row.OrgID != claim.OrgID || row.RepoID == "" || len(row.RepoID) != 36 || row.DeploymentID == "" || row.DeployedAt == nil || row.LastSynced.IsZero() {
		return providerfoundation.ErrInvalidScope
	}
	if row.PullRequestNumber != nil && uint64(*row.PullRequestNumber) > math.MaxUint32 {
		return providerfoundation.ErrInvalidScope
	}
	return nil
}

var _ CompleteRouteHandler = GitHubDeploymentsRouteHandler{}
