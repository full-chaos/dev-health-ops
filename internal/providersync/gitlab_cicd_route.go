package providersync

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

const (
	defaultGitLabCICDPerPage      = 100
	defaultGitLabCICDMaxPipelines = 1_000
)

type gitLabCICDPipelinePayload struct {
	ID         any     `json:"id"`
	Status     any     `json:"status"`
	CreatedAt  *string `json:"created_at"`
	StartedAt  *string `json:"started_at"`
	FinishedAt *string `json:"finished_at"`
}

// gitLabCICDPipelineRow is the complete row owned by Python's isolated
// sync_cicd path. TestOps-only columns belong to the separate gitlab/tests
// unit and are deliberately outside this D16 port.
type gitLabCICDPipelineRow struct {
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

func (row gitLabCICDPipelineRow) validate(claim Claim) error {
	if claim.Provider != "gitlab" || claim.Dataset != "cicd" ||
		row.OrgID == "" || row.OrgID != claim.OrgID || row.RepoID == "" ||
		row.RunID == "" || row.StartedAt.IsZero() || row.LastSynced.IsZero() {
		return providerfoundation.ErrInvalidScope
	}
	return nil
}

// GitLabCICDRouteHandler ports only the active Python gitlab/cicd pipeline
// producer. Its intentional 1,000-item window is accepted even when GitLab
// advertises more data; a shorter traversal that exhausts its page bound is
// incomplete and cannot emit effects or advance the watermark.
type GitLabCICDRouteHandler struct {
	PerPage      int
	MaxPipelines int
}

type gitLabCICDCountingDoer struct {
	delegate providerfoundation.HTTPDoer
	attempts *int
}

func (doer gitLabCICDCountingDoer) Do(request *http.Request) (*http.Response, error) {
	attempts := doer.attempts
	*attempts = *attempts + 1
	return doer.delegate.Do(request)
}

func (handler GitLabCICDRouteHandler) Collect(
	ctx context.Context,
	claim Claim,
	_ providerfoundation.Credential,
	client *providerfoundation.HTTPClient,
	normalizedAt time.Time,
) (CompleteRouteBatch, error) {
	if ctx == nil || claim.Validate() != nil || claim.Provider != "gitlab" ||
		claim.Dataset != "cicd" || client == nil || client.Provider != "gitlab" ||
		client.BaseURL == nil || normalizedAt.IsZero() {
		return CompleteRouteBatch{}, ErrInvalidConfiguration
	}
	projectID, err := gitLabProjectID(claim.SourceExternalID)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	requestAttempts := 0
	countedClient := *client
	countedClient.Doer = gitLabCICDCountingDoer{
		delegate: client.Doer,
		attempts: &requestAttempts,
	}
	root := providerRelativePath(client, "api", "v4", "projects", projectID)
	var project repositoryPayload
	if err := fetchObject(ctx, &countedClient, root, &project); err != nil {
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
		perPage = defaultGitLabCICDPerPage
	}
	maxPipelines := handler.MaxPipelines
	if maxPipelines == 0 {
		maxPipelines = defaultGitLabCICDMaxPipelines
	}
	if perPage < 1 || perPage > defaultGitLabCICDPerPage ||
		maxPipelines < 1 || maxPipelines > defaultGitLabCICDMaxPipelines {
		return CompleteRouteBatch{}, ErrInvalidConfiguration
	}
	maxPages := (maxPipelines + perPage - 1) / perPage
	page, err := providerfoundation.CollectGitLabPageParamPages(
		ctx,
		&countedClient,
		providerfoundation.GitLabPageOptions{
			Path: root + "/pipelines",
			Query: url.Values{
				"order_by": {"updated_at"},
				"sort":     {"desc"},
			},
			PerPage: perPage, MaxPages: maxPages,
		},
	)
	if err != nil {
		if fatal := gitLabCICDFatalListError(ctx, err); fatal != nil {
			return CompleteRouteBatch{}, fatal
		}
		return gitLabCICDSoftEmptyBatch(
			claim, fullName, parsedProjectID, requestAttempts,
		), nil
	}
	if page.CapReached && len(page.Items) < maxPipelines {
		return CompleteRouteBatch{}, ErrPaginationCapExceeded
	}
	items := page.Items
	if len(items) > maxPipelines {
		items = items[:maxPipelines]
	}
	payloads := make([]gitLabCICDPipelinePayload, 0, len(items))
	for _, raw := range items {
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.UseNumber()
		var payload gitLabCICDPipelinePayload
		if decoder.Decode(&payload) != nil {
			return CompleteRouteBatch{}, providerfoundation.ErrNormalizationInvalid
		}
		payloads = append(payloads, payload)
	}
	rows, err := normalizeGitLabCICDPipelines(claim, repoID, payloads, claim.SinceAt, normalizedAt)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	effect, err := effectBatchFromValues("ci_pipeline_runs", EffectReadbackRequired, rows)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	return CompleteRouteBatch{
		Effects: []EffectBatch{effect},
		Result: map[string]any{
			"pipelines_synced": len(rows), "repo": fullName,
			"project_id": parsedProjectID,
		},
		Watermark: claim.BeforeAt,
		Evidence: FetchEvidence{
			Provider: claim.Provider, Dataset: claim.Dataset,
			Requests: requestAttempts, Pages: page.Pages, Records: len(rows),
			CapReached: page.CapReached,
		},
	}, nil
}

func gitLabCICDSoftEmptyBatch(
	claim Claim, fullName string, projectID int64, requests int,
) CompleteRouteBatch {
	effect, _ := effectBatchFromValues("ci_pipeline_runs", EffectReadbackRequired, []gitLabCICDPipelineRow{})
	return CompleteRouteBatch{
		Effects: []EffectBatch{effect},
		Result: map[string]any{
			"pipelines_synced": 0, "repo": fullName, "project_id": projectID,
			"soft_failure": true,
		},
		Watermark: claim.BeforeAt,
		Evidence:  FetchEvidence{Provider: claim.Provider, Dataset: claim.Dataset, Requests: requests},
	}
}

func gitLabCICDFatalListError(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
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

func normalizeGitLabCICDPipelines(
	claim Claim,
	repoID string,
	payloads []gitLabCICDPipelinePayload,
	since *time.Time,
	normalizedAt time.Time,
) ([]gitLabCICDPipelineRow, error) {
	if claim.Provider != "gitlab" || claim.Dataset != "cicd" || repoID == "" || normalizedAt.IsZero() {
		return nil, ErrInvalidConfiguration
	}
	normalizedAt = normalizedAt.UTC().Truncate(time.Millisecond)
	rows := make([]gitLabCICDPipelineRow, 0, len(payloads))
	for _, payload := range payloads {
		createdAt := parseGitLabCICDTime(payload.CreatedAt)
		if createdAt == nil {
			continue
		}
		// Python compares the producer timestamps at their original precision.
		// ClickHouse stores milliseconds, but truncating before these boundary
		// checks changes membership for sub-millisecond values.
		createdInstant := createdAt.UTC()
		if since != nil && createdInstant.Before(since.UTC()) {
			break
		}
		startedInstant := createdInstant
		if value := parseGitLabCICDTime(payload.StartedAt); value != nil {
			startedInstant = value.UTC()
		}
		if claim.BeforeAt != nil && startedInstant.After(claim.BeforeAt.UTC()) {
			continue
		}
		created := createdInstant.Truncate(time.Millisecond)
		started := startedInstant.Truncate(time.Millisecond)
		var finished *time.Time
		if value := parseGitLabCICDTime(payload.FinishedAt); value != nil {
			canonical := value.UTC().Truncate(time.Millisecond)
			finished = &canonical
		}
		runID := stringValue(payload.ID)
		if runID == "" {
			return nil, providerfoundation.ErrNormalizationInvalid
		}
		var status *string
		if payload.Status != nil {
			value, ok := payload.Status.(string)
			if !ok {
				return nil, providerfoundation.ErrNormalizationInvalid
			}
			status = &value
		}
		queued := created
		row := gitLabCICDPipelineRow{
			OrgID: claim.OrgID, RepoID: repoID, RunID: runID, Status: status,
			QueuedAt: &queued, StartedAt: started, FinishedAt: finished,
			RetryCount: 0, LastSynced: normalizedAt,
		}
		if err := row.validate(claim); err != nil {
			return nil, err
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func parseGitLabCICDTime(value *string) *time.Time {
	if value == nil || *value == "" {
		return nil
	}
	parsed, err := time.Parse(time.RFC3339Nano, *value)
	if err != nil {
		return nil
	}
	return &parsed
}

var _ CompleteRouteHandler = GitLabCICDRouteHandler{}
