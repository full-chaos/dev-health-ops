package providersync

import (
	"context"
	"encoding/json"
	"net/url"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

const defaultGitHubCICDMaxRuns = 1_000

// ciPipelineRunRow is the frozen ci_pipeline_runs projection written by the
// github/cicd sync unit. Its fields and JSON names mirror Python's
// build_ci_pipeline_run -> ClickHouseStore.insert_ci_pipeline_runs boundary.
type ciPipelineRunRow struct {
	OrgID      string     `json:"org_id"`
	RepoID     string     `json:"repo_id"`
	RunID      string     `json:"run_id"`
	Status     *string    `json:"status"`
	QueuedAt   *time.Time `json:"queued_at"`
	StartedAt  time.Time  `json:"started_at"`
	FinishedAt *time.Time `json:"finished_at"`
	RetryCount uint32     `json:"retry_count"`
	LastSynced time.Time  `json:"last_synced"`
}

type gitHubWorkflowRunsPayload struct {
	Runs []gitHubWorkflowRunPayload `json:"workflow_runs"`
}

type gitHubWorkflowRunPayload struct {
	ID           json.Number `json:"id"`
	Conclusion   any         `json:"conclusion"`
	Status       any         `json:"status"`
	CreatedAt    *string     `json:"created_at"`
	RunStartedAt *string     `json:"run_started_at"`
	UpdatedAt    *string     `json:"updated_at"`
	RunAttempt   any         `json:"run_attempt"`
}

// GitHubCICDRouteHandler mirrors _fetch_github_workflow_runs_async followed
// by its caller's _filter_after(..., until, "started_at") in github.py. It
// owns exactly ci_pipeline_runs; tests, deployments, and PR datasets remain
// distinct Python/Go units.
type GitHubCICDRouteHandler struct{ MaxRuns int }

func (handler GitHubCICDRouteHandler) Collect(
	ctx context.Context,
	claim Claim,
	_ providerfoundation.Credential,
	client *providerfoundation.HTTPClient,
	normalizedAt time.Time,
) (CompleteRouteBatch, error) {
	if ctx == nil || claim.Validate() != nil || claim.Provider != "github" ||
		claim.Dataset != "cicd" || client == nil || client.Provider != "github" ||
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
	maxRuns := handler.MaxRuns
	if maxRuns == 0 {
		maxRuns = defaultGitHubCICDMaxRuns
	}
	if maxRuns < 1 {
		return CompleteRouteBatch{}, ErrInvalidConfiguration
	}
	pages := (maxRuns + nativePerPage - 1) / nativePerPage
	page, err := providerfoundation.CollectGitHubLinkPages(
		ctx, client, providerfoundation.GitHubPageOptions{
			Path:    root + "/actions/runs",
			Query:   url.Values{"per_page": {"100"}},
			DataKey: "workflow_runs", MaxPages: pages,
		},
	)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	if page.CapReached {
		return CompleteRouteBatch{}, ErrPaginationCapExceeded
	}
	items := page.Items
	if len(items) > maxRuns {
		items = items[:maxRuns]
	}
	rows := make([]ciPipelineRunRow, 0, len(items))
	for _, raw := range items {
		var workflow gitHubWorkflowRunPayload
		decoder := json.NewDecoder(strings.NewReader(string(raw)))
		decoder.UseNumber()
		if decoder.Decode(&workflow) != nil {
			return CompleteRouteBatch{}, providerfoundation.ErrNormalizationInvalid
		}
		row, include := normalizeGitHubWorkflowRun(claim, repoID, workflow, normalizedAt)
		if !include || ciPipelineRunOutsideWindow(row.StartedAt, claim) {
			continue
		}
		rows = append(rows, row)
	}
	effect, err := effectBatchFromValues("ci_pipeline_runs", EffectReadbackRequired, rows)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	watermark := claim.BeforeAt
	return CompleteRouteBatch{
		Effects: []EffectBatch{effect},
		Result: map[string]any{
			"pipeline_runs_synced": len(rows),
			"repo":                 repoPayload.FullName,
		},
		Watermark: watermark,
		Evidence: FetchEvidence{
			Provider: claim.Provider, Dataset: claim.Dataset,
			Requests: page.Pages + 1, Pages: page.Pages, Records: len(rows),
			CapReached: page.CapReached,
		},
	}, nil
}

func normalizeGitHubWorkflowRun(
	claim Claim,
	repoID string,
	workflow gitHubWorkflowRunPayload,
	normalizedAt time.Time,
) (ciPipelineRunRow, bool) {
	queuedAt := parseGitHubWorkflowTime(workflow.CreatedAt)
	startedAt := parseGitHubWorkflowTime(workflow.RunStartedAt)
	if startedAt == nil {
		startedAt = queuedAt
	}
	if startedAt == nil {
		return ciPipelineRunRow{}, false
	}
	status := workflowRunStatus(workflow.Conclusion, workflow.Status)
	return ciPipelineRunRow{
		OrgID: claim.OrgID, RepoID: repoID, RunID: stringValue(workflow.ID), Status: status,
		QueuedAt: queuedAt, StartedAt: *startedAt,
		FinishedAt: parseGitHubWorkflowTime(workflow.UpdatedAt),
		RetryCount: workflowRetryCount(workflow.RunAttempt), LastSynced: normalizedAt,
	}, true
}

func parseGitHubWorkflowTime(value *string) *time.Time {
	return parseGitHubPullTime(value)
}

func workflowRunStatus(conclusion, status any) *string {
	value := stringValue(conclusion)
	if value == "" {
		value = stringValue(status)
	}
	if value == "" {
		return nil
	}
	return &value
}

func workflowRetryCount(value any) uint32 {
	attempt, err := json.Number(stringValue(value)).Int64()
	if err != nil || attempt < 2 {
		return 0
	}
	return uint32(attempt - 1)
}

func ciPipelineRunOutsideWindow(startedAt time.Time, claim Claim) bool {
	return (claim.SinceAt != nil && startedAt.Before(claim.SinceAt.UTC())) ||
		(claim.BeforeAt != nil && startedAt.After(claim.BeforeAt.UTC()))
}

func (row ciPipelineRunRow) validate(claim Claim) error {
	if row.OrgID == "" || row.OrgID != claim.OrgID || row.RepoID == "" ||
		len(row.RepoID) != 36 || row.RunID == "" ||
		row.StartedAt.IsZero() || row.LastSynced.IsZero() {
		return providerfoundation.ErrInvalidScope
	}
	return nil
}

var _ CompleteRouteHandler = GitHubCICDRouteHandler{}
