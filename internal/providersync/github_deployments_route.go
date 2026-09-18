package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"math"
	"net/url"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

const defaultGitHubDeploymentsMax = 1_000

// maxDeploymentStatusPages bounds the per-deployment statuses lookup the
// same way the existing per-SHA pull-request lookup already bounds itself
// (one deployment, one best-effort enrichment call) -- 100 statuses per page
// at up to 3 pages covers every real deployment's status history without an
// unbounded per-deployment request budget.
const maxDeploymentStatusPages = 3

// githubDeploymentTerminalStates are the states that represent THIS
// deployment's own pipeline actually finishing (docs.github.com/en/rest/
// deployments/statuses, "state" enum): success, failure, error. "inactive"
// and "pending"/"queued"/"in_progress" are deliberately excluded --
// "inactive" fires when a NEWER deployment on the same environment
// supersedes this one, an environment-lifecycle event unrelated to when
// THIS deployment's own work concluded; including it would let a later
// supersession retroactively push finished_at past the real completion a
// success/failure/error entry already recorded.
var githubDeploymentTerminalStates = map[string]struct{}{
	"success": {},
	"failure": {},
	"error":   {},
}

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
	// LifecycleLookupFailed is writer-internal signaling, never an INSERT
	// column: true exactly when this row's StartedAt/FinishedAt are nil
	// because the statuses lookup FAILED or was skipped by the rate-limit
	// path this pass, as opposed to a successful lookup that honestly
	// found no signal. The effects sink's write-once lifecycle guard reads
	// this to decide whether a nil is worth carrying a prior value forward
	// over -- see guardDeploymentLifecycleRegressions.
	LifecycleLookupFailed bool `json:"lifecycle_lookup_failed,omitempty"`
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

// gitHubDeploymentStatusPayload mirrors one entry of GitHub's "List
// deployment statuses" response (docs.github.com/en/rest/deployments/
// statuses): only the two fields deploymentLifecycleFromStatuses reads.
type gitHubDeploymentStatusPayload struct {
	State     any     `json:"state"`
	CreatedAt *string `json:"created_at"`
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
	releases, releasePages, _, err := fetchGitHubDeploymentsPage[gitHubReleasePayload](ctx, client, root+"/releases", pages)
	if err != nil {
		// Python treats release enrichment as best effort.
		releases, releasePages = nil, 0
	}
	deployments, deploymentPages, _, err := fetchGitHubDeploymentsPage[gitHubDeploymentPayload](ctx, client, root+"/deployments", pages)
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
	statusesRateLimited := false
	statusLookupsSkippedForRateLimit := 0
	enrichmentPages := 0
	for _, deployment := range deployments {
		row, include := normalizeGitHubDeployment(claim, repoID, deployment, releases, normalizedAt)
		if !include || deploymentOutsideWindow(row.DeployedAt, claim) {
			continue
		}
		if deployment.SHA != nil && strings.TrimSpace(*deployment.SHA) != "" {
			pulls, pullPages, _, pullErr := fetchGitHubDeploymentsPage[gitHubPullPayload](ctx, client, root+"/commits/"+url.PathEscape(*deployment.SHA)+"/pulls", 1)
			enrichmentPages += pullPages
			if pullErr == nil {
				row.PullRequestNumber, row.MergedAt = chooseDeploymentPullRequest(pulls, *deployment.SHA)
			}
		}
		switch {
		case statusesRateLimited:
			// The statuses endpoint already exhausted its retry budget once
			// in this unit -- a second, third, ... call would each pay the
			// same full backoff again for a deployment count that can run to
			// maxDeployments. Skip the remaining lookups outright rather
			// than multiply that cost; the summary line below reports how
			// many were skipped this way.
			statusLookupsSkippedForRateLimit++
			row.LifecycleLookupFailed = true
		default:
			statuses, statusPages, statusesTruncated, statusErr := fetchGitHubDeploymentsPage[gitHubDeploymentStatusPayload](ctx, client, root+"/deployments/"+stringValue(deployment.ID)+"/statuses", maxDeploymentStatusPages)
			enrichmentPages += statusPages
			switch {
			case statusErr != nil:
				slog.Warn("github_deployments.status_lookup_failed", "deployment_id", row.DeploymentID, "cause", statusErr.Error())
				row.LifecycleLookupFailed = true
				if isRateLimitExhausted(statusErr) {
					statusesRateLimited = true
				}
			case len(statuses) == 0:
				slog.Warn("github_deployments.status_lookup_empty", "deployment_id", row.DeploymentID, "cause", "no deployment statuses returned")
			default:
				row.StartedAt, row.FinishedAt = deploymentLifecycleFromStatuses(statuses)
				row.Status = deploymentLatestStatus(statuses)
				if statusesTruncated {
					// The provider had more status pages than
					// maxDeploymentStatusPages fetched -- an earlier
					// in_progress entry (or a later terminal one) may exist
					// past the budget, so the derived value above is
					// honest but possibly not the true earliest/latest.
					slog.Warn("github_deployments.status_lookup_truncated", "deployment_id", row.DeploymentID, "cause", "statuses page budget exhausted before the provider ran out of pages")
				}
			}
		}
		rows = append(rows, row)
	}
	if statusLookupsSkippedForRateLimit > 0 {
		slog.Warn("github_deployments.status_lookup_rate_limited_skip", "count", statusLookupsSkippedForRateLimit, "cause", "statuses lookup rate limited; remaining deployments in this sync unit skipped")
	}
	effect, err := effectBatchFromValues("deployments", EffectReadbackRequired, rows)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	return CompleteRouteBatch{Effects: []EffectBatch{effect}, Result: map[string]any{"deployments_synced": len(rows), "repo": repoPayload.FullName}, Watermark: claim.BeforeAt, Evidence: FetchEvidence{Provider: claim.Provider, Dataset: claim.Dataset, Requests: releasePages + deploymentPages + enrichmentPages + 1, Pages: releasePages + deploymentPages + enrichmentPages, Records: len(rows)}}, nil
}

// fetchGitHubDeploymentsPage's fourth return value reports whether the
// provider had more pages than maxPages allowed to fetch (page.
// PageBudgetExhausted) -- the releases and deployments-list callers don't
// need it and discard it with `_`; the statuses lookup uses it to tell an
// honestly-derived-but-possibly-incomplete lifecycle timestamp from a
// complete one. Its second return value (page count) is folded into every
// caller's own request-accounting total, including the per-SHA PR lookup
// and the statuses lookup, both of which fire once per in-window
// deployment.
func fetchGitHubDeploymentsPage[T any](ctx context.Context, client *providerfoundation.HTTPClient, path string, maxPages int) ([]T, int, bool, error) {
	page, err := providerfoundation.CollectGitHubLinkPages(ctx, client, providerfoundation.GitHubPageOptions{Path: path, Query: url.Values{"per_page": {"100"}}, MaxPages: maxPages})
	if err != nil {
		return nil, 0, false, err
	}
	items := make([]T, 0, len(page.Items))
	for _, raw := range page.Items {
		var item T
		decoder := json.NewDecoder(strings.NewReader(string(raw)))
		decoder.UseNumber()
		if decoder.Decode(&item) != nil {
			return nil, 0, false, providerfoundation.ErrNormalizationInvalid
		}
		items = append(items, item)
	}
	return items, page.Pages, page.PageBudgetExhausted, nil
}

// normalizeGitHubDeployment's own Status candidates (deployment.State,
// deployment.Status) always resolve nil against GitHub's real List
// Deployments response (docs.github.com/en/rest/deployments/deployments):
// that endpoint carries no state/status field on the deployment object
// itself, only a statuses_url -- deploymentLatestStatus, read from the
// separate statuses sub-resource in the Collect loop below, is the only
// source that ever sets a real Status for a GitHub row. This list-level
// read is kept, harmlessly, because it is always overwritten once that
// lookup succeeds, and because it is what this package's own oracle
// parity harness (testdata/oracle_pairs/github_deployments_row.py) relies
// on to match the Python oracle's identical read of its own synthetic
// fixture -- neither side's fixture supplies a statuses history, so both
// read the (equally synthetic) list-level field instead.
func normalizeGitHubDeployment(claim Claim, repoID string, deployment gitHubDeploymentPayload, releases []gitHubReleasePayload, normalizedAt time.Time) (deploymentRow, bool) {
	deployedAt := parseGitHubWorkflowTime(deployment.CreatedAt)
	if deployedAt == nil {
		return deploymentRow{}, false
	}
	releaseRef, confidence := deploymentReleaseRef(deployment, releases)
	return deploymentRow{RepoID: repoID, DeploymentID: stringValue(deployment.ID), Status: optionalString(deployment.State, deployment.Status), Environment: optionalString(deployment.Environment), DeployedAt: deployedAt, ReleaseRef: releaseRef, ReleaseRefConfidence: confidence, OrgID: claim.OrgID, LastSynced: normalizedAt}, true
}

// deploymentLifecycleFromStatuses derives started_at/finished_at from a
// deployment's status history (docs.github.com/en/rest/deployments/
// statuses). It compares every entry's own created_at rather than relying
// on response order, so a second page or a reversed-order response yields
// the same result: started_at is the earliest "in_progress" entry (the
// first signal that work actually began, distinct from the deployment's
// own creation instant); finished_at is the latest entry in
// githubDeploymentTerminalStates. Either stays nil when no matching entry
// is found -- never a value copied from another column.
func deploymentLifecycleFromStatuses(statuses []gitHubDeploymentStatusPayload) (startedAt, finishedAt *time.Time) {
	for _, status := range statuses {
		at := parseGitHubWorkflowTime(status.CreatedAt)
		if at == nil {
			continue
		}
		state := strings.ToLower(strings.TrimSpace(stringValue(status.State)))
		if state == "in_progress" && (startedAt == nil || at.Before(*startedAt)) {
			startedAt = at
		}
		if _, terminal := githubDeploymentTerminalStates[state]; terminal && (finishedAt == nil || at.After(*finishedAt)) {
			finishedAt = at
		}
	}
	return startedAt, finishedAt
}

// deploymentLatestStatus derives the deployment's own current status from
// the same status history deploymentLifecycleFromStatuses reads --
// GitHub's List Deployments response (docs.github.com/en/rest/deployments/
// deployments) carries no state/status field on the deployment object
// itself, only a statuses_url; the deployment's current status lives on
// its separate statuses sub-resource (docs.github.com/en/rest/deployments/
// statuses), the same one this writer already fetches for started_at/
// finished_at. The latest entry by its own created_at (never response
// order, for the same reordering reasons deploymentLifecycleFromStatuses
// documents) is the deployment's current status; nil when the history is
// empty or every entry's timestamp fails to parse.
func deploymentLatestStatus(statuses []gitHubDeploymentStatusPayload) *string {
	var latestAt *time.Time
	var latestState string
	for _, status := range statuses {
		at := parseGitHubWorkflowTime(status.CreatedAt)
		if at == nil {
			continue
		}
		if latestAt == nil || at.After(*latestAt) {
			latestAt = at
			latestState = strings.TrimSpace(stringValue(status.State))
		}
	}
	if latestAt == nil || latestState == "" {
		return nil
	}
	return &latestState
}

// isRateLimitExhausted reports whether err is the HTTP client's own retry
// budget running out on a rate-limited response (providerfoundation.
// ErrorRateLimited) -- as opposed to any other request failure (network
// error, 404, etc.), which does not trigger the remaining-lookups skip.
func isRateLimitExhausted(err error) bool {
	var providerErr *providerfoundation.ProviderError
	if errors.As(err, &providerErr) {
		return providerErr.Class == providerfoundation.ErrorRateLimited
	}
	return false
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
