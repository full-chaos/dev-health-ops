package providersync

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

const (
	// Python calls _fetch_gitlab_deployments_sync with processors.gitlab's
	// BATCH_SIZE (1,000). get_deployments intentionally requests only one
	// 100-record GitLab page, however; do not turn this into a multi-page
	// traversal without a separately approved Python-boundary change.
	defaultGitLabDeploymentsMax     = 1_000
	gitLabDeploymentsMaximumPerPage = 100
)

// GitLabDeploymentsRouteHandler mirrors Python's
// _fetch_gitlab_deployments_sync -> build_deployment boundary. Releases and
// per-SHA MR enrichment are deliberately best effort, and a deployments-list
// error produces Python's accepted empty successful batch rather than a
// partial/fail-closed effect.
type GitLabDeploymentsRouteHandler struct{ MaxDeployments int }

type gitLabDeploymentCountingDoer struct {
	delegate providerfoundation.HTTPDoer
	attempts *int
}

func (doer gitLabDeploymentCountingDoer) Do(request *http.Request) (*http.Response, error) {
	*doer.attempts = *doer.attempts + 1
	return doer.delegate.Do(request)
}

func (handler GitLabDeploymentsRouteHandler) Collect(
	ctx context.Context,
	claim Claim,
	_ providerfoundation.Credential,
	client *providerfoundation.HTTPClient,
	normalizedAt time.Time,
) (CompleteRouteBatch, error) {
	if ctx == nil || claim.Validate() != nil || claim.Provider != "gitlab" ||
		claim.Dataset != "deployments" || client == nil || client.Provider != "gitlab" ||
		client.BaseURL == nil || normalizedAt.IsZero() {
		return CompleteRouteBatch{}, ErrInvalidConfiguration
	}
	maxDeployments := handler.MaxDeployments
	if maxDeployments == 0 {
		maxDeployments = defaultGitLabDeploymentsMax
	}
	if maxDeployments < 1 {
		return CompleteRouteBatch{}, ErrInvalidConfiguration
	}
	projectID, err := gitLabProjectID(claim.SourceExternalID)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	requests := 0
	counted := *client
	counted.Doer = gitLabDeploymentCountingDoer{delegate: client.Doer, attempts: &requests}
	root := providerRelativePath(&counted, "api", "v4", "projects", projectID)
	var project repositoryPayload
	if err := fetchObject(ctx, &counted, root, &project); err != nil {
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
	normalizedAt = normalizedAt.UTC().Truncate(time.Millisecond)
	perPage := min(maxDeployments, gitLabDeploymentsMaximumPerPage)

	releases, releasePages, releaseErr := fetchGitLabDeploymentObjects(
		ctx, &counted, root+"/releases", url.Values{}, perPage,
	)
	if controlErr := gitLabDeploymentControlError(ctx, releaseErr); controlErr != nil {
		return CompleteRouteBatch{}, controlErr
	}
	if releaseErr != nil {
		// Python logs and discards only this release-ref enrichment input.
		releases = nil
		releasePages = 0
	}

	deployments, deploymentPages, deploymentsErr := fetchGitLabDeploymentObjects(
		ctx,
		&counted,
		root+"/deployments",
		url.Values{"order_by": {"created_at"}, "sort": {"desc"}},
		perPage,
	)
	if controlErr := gitLabDeploymentControlError(ctx, deploymentsErr); controlErr != nil {
		return CompleteRouteBatch{}, controlErr
	}
	if deploymentsErr != nil {
		// D16: the Python producer catches an error spanning the deployments
		// collection and returns the successful empty list. It must not retain
		// release-derived partial state or manufacture partial deployment rows.
		return gitLabDeploymentsBatch(
			claim, fullName, parsedProjectID, normalizedAt, nil, requests, releasePages,
		)
	}
	if len(deployments) > maxDeployments {
		deployments = deployments[:maxDeployments]
	}
	mergeRequestsByDeploymentID := make(map[string][]map[string]any, len(deployments))
	pages := releasePages + deploymentPages
	for _, deployment := range deployments {
		sha := stringValue(deployment["sha"])
		if sha == "" {
			continue
		}
		mergeRequests, mergeRequestPages, mergeRequestErr := fetchGitLabDeploymentObjects(
			ctx,
			&counted,
			root+"/repository/commits/"+url.PathEscape(sha)+"/merge_requests",
			url.Values{},
			gitLabDeploymentsMaximumPerPage,
		)
		if controlErr := gitLabDeploymentControlError(ctx, mergeRequestErr); controlErr != nil {
			return CompleteRouteBatch{}, controlErr
		}
		if mergeRequestErr != nil {
			// Python permits a failed MR lookup for one deployed SHA to leave
			// only that deployment unattributed.
			continue
		}
		pages += mergeRequestPages
		mergeRequestsByDeploymentID[stringValue(deployment["id"])] = mergeRequests
	}

	rows := make([]deploymentRow, 0, len(deployments))
	for _, deployment := range deployments {
		row, include, normalizeErr := normalizeGitLabDeployment(
			claim, repoID, deployment, releases, normalizedAt,
		)
		if normalizeErr != nil {
			return CompleteRouteBatch{}, normalizeErr
		}
		if !include {
			continue
		}
		// The Python collector is ordered newest-created first. It breaks at
		// the first item before since, but filters an item after until while
		// continuing to inspect the accepted one-page response.
		if claim.SinceAt != nil && row.DeployedAt.Before(claim.SinceAt.UTC()) {
			break
		}
		if claim.BeforeAt != nil && row.DeployedAt.After(claim.BeforeAt.UTC()) {
			continue
		}
		row.PullRequestNumber, row.MergedAt = resolveGitLabDeploymentMergeRequest(
			mergeRequestsByDeploymentID[row.DeploymentID],
		)
		rows = append(rows, row)
	}
	return gitLabDeploymentsBatch(
		claim, fullName, parsedProjectID, normalizedAt, rows, requests, pages,
	)
}

func gitLabDeploymentControlError(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return nil
}

func gitLabDeploymentsBatch(
	claim Claim,
	fullName string,
	projectID int64,
	normalizedAt time.Time,
	rows []deploymentRow,
	requests int,
	pages int,
) (CompleteRouteBatch, error) {
	effect, err := effectBatchFromValues("deployments", EffectReadbackRequired, rows)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	return CompleteRouteBatch{
		Effects: []EffectBatch{effect},
		Result: map[string]any{
			"deployments_synced": len(rows),
			"repo":               fullName,
			"project_id":         projectID,
		},
		Watermark: claim.BeforeAt,
		Evidence: FetchEvidence{
			Provider: claim.Provider, Dataset: claim.Dataset,
			Requests: requests, Pages: pages, Records: len(rows),
		},
	}, nil
}

func fetchGitLabDeploymentObjects(
	ctx context.Context,
	client *providerfoundation.HTTPClient,
	path string,
	query url.Values,
	perPage int,
) ([]map[string]any, int, error) {
	page, err := providerfoundation.CollectGitLabPageParamPages(
		ctx,
		client,
		providerfoundation.GitLabPageOptions{
			Path: path, Query: query, PerPage: perPage, MaxPages: 1, SinglePage: true,
		},
	)
	if err != nil {
		return nil, 0, err
	}
	objects := make([]map[string]any, 0, len(page.Items))
	for _, raw := range page.Items {
		object, include, decodeErr := decodeGitLabDeploymentObject(raw)
		if decodeErr != nil {
			return nil, 0, decodeErr
		}
		if include {
			objects = append(objects, object)
		}
	}
	return objects, page.Pages, nil
}

func decodeGitLabDeploymentObject(raw json.RawMessage) (map[string]any, bool, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, false, providerfoundation.ErrNormalizationInvalid
	}
	object, ok := value.(map[string]any)
	return object, ok, nil
}

func normalizeGitLabDeployment(
	claim Claim,
	repoID string,
	payload map[string]any,
	releases []map[string]any,
	normalizedAt time.Time,
) (deploymentRow, bool, error) {
	createdAt := parseGitLabDeploymentTime(payload["created_at"])
	if createdAt == nil {
		return deploymentRow{}, false, nil
	}
	deploymentID := stringValue(payload["id"])
	if deploymentID == "" {
		return deploymentRow{}, false, providerfoundation.ErrNormalizationInvalid
	}
	var environment any
	if rawEnvironment, ok := payload["environment"].(map[string]any); ok {
		environment = rawEnvironment["name"]
	}
	releaseRef, confidence := gitLabDeploymentReleaseRef(payload, deploymentID, releases)
	row := deploymentRow{
		RepoID:               repoID,
		DeploymentID:         deploymentID,
		Status:               optionalString(payload["status"]),
		Environment:          optionalString(environment),
		StartedAt:            createdAt,
		FinishedAt:           parseGitLabDeploymentTime(payload["finished_at"]),
		DeployedAt:           createdAt,
		ReleaseRef:           releaseRef,
		ReleaseRefConfidence: confidence,
		OrgID:                claim.OrgID,
		LastSynced:           normalizedAt,
	}
	if err := row.validate(claim); err != nil {
		return deploymentRow{}, false, err
	}
	return row, true, nil
}

func parseGitLabDeploymentTime(value any) *time.Time {
	text, ok := value.(string)
	if !ok || text == "" {
		return nil
	}
	parsed, err := time.Parse(time.RFC3339Nano, text)
	if err != nil {
		return nil
	}
	parsed = parsed.UTC().Truncate(time.Millisecond)
	return &parsed
}

func gitLabDeploymentReleaseRef(
	payload map[string]any,
	deploymentID string,
	releases []map[string]any,
) (string, float64) {
	if explicit := gitLabDeploymentExplicitReleaseRef(payload); explicit != "" {
		return explicit, gitLabDeploymentExplicitConfidence(payload)
	}
	candidates := map[string]struct{}{}
	for _, key := range []string{"ref", "tag", "tag_name"} {
		if candidate := strings.TrimSpace(stringValue(payload[key])); candidate != "" {
			candidates[candidate] = struct{}{}
		}
	}
	for _, release := range releases {
		if tag := strings.TrimSpace(stringValue(release["tag_name"])); tag != "" {
			if _, ok := candidates[tag]; ok {
				return tag, 1
			}
		}
	}
	for _, candidate := range []any{payload["iid"], deploymentID, payload["id"]} {
		if value := strings.TrimSpace(stringValue(candidate)); value != "" {
			return value, 0.3
		}
	}
	return "", 0.3
}

func gitLabDeploymentExplicitReleaseRef(payload map[string]any) string {
	for _, key := range []string{"release_ref", "release", "release_tag", "tag_name", "tag", "version"} {
		if value := strings.TrimSpace(stringValue(payload[key])); value != "" {
			return value
		}
	}
	nested, _ := payload["payload"].(map[string]any)
	for _, key := range []string{"release_ref", "release", "release_tag", "tag_name", "tag", "version"} {
		if value := strings.TrimSpace(stringValue(nested[key])); value != "" {
			return value
		}
	}
	return ""
}

func gitLabDeploymentExplicitConfidence(payload map[string]any) float64 {
	value, ok := payload["release_ref_confidence"]
	if !ok {
		if nested, nestedOK := payload["payload"].(map[string]any); nestedOK {
			value, ok = nested["release_ref_confidence"]
		}
	}
	if !ok {
		return 1
	}
	confidence, err := strconv.ParseFloat(strings.TrimSpace(stringValue(value)), 64)
	if err != nil {
		return 1
	}
	if confidence < 0 {
		return 0
	}
	if confidence > 1 {
		return 1
	}
	return confidence
}

func resolveGitLabDeploymentMergeRequest(
	mergeRequests []map[string]any,
) (*int, *time.Time) {
	var chosen map[string]any
	for _, mergeRequest := range mergeRequests {
		if stringValue(mergeRequest["state"]) == "merged" {
			chosen = mergeRequest
			break
		}
	}
	if chosen == nil && len(mergeRequests) > 0 {
		chosen = mergeRequests[0]
	}
	if chosen == nil {
		return nil, nil
	}
	var number *int
	if parsed, err := strconv.Atoi(strings.TrimSpace(stringValue(chosen["iid"]))); err == nil {
		number = &parsed
	}
	return number, parseGitLabDeploymentTime(chosen["merged_at"])
}

var _ CompleteRouteHandler = GitLabDeploymentsRouteHandler{}
