package providersync

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

const (
	// GitHub Actions returns at most nativePerPage workflow runs per page. The
	// configured 2.5-month backfill window can exceed two pages, so the route
	// permits the package-wide nativeMaxPages ceiling (10,000 runs) while still
	// failing closed when GitHub advertises a page beyond that bound.
	githubTestsMaxRuns         = nativeMaxPages * nativePerPage
	githubTestsMaxArtifacts    = 25
	githubTestsMaxDownloadSize = 100 << 20
	githubTestsRuleVersion     = "ci-acceptance.v1"
)

var ErrGitHubTestsIncomplete = errors.New("github tests inventory incomplete")

type githubTestsPipelineRow struct {
	OrgID           string     `json:"org_id"`
	RepoID          string     `json:"repo_id"`
	RunID           string     `json:"run_id"`
	PipelineName    *string    `json:"pipeline_name"`
	Provider        string     `json:"provider"`
	Status          *string    `json:"status"`
	QueuedAt        *time.Time `json:"queued_at"`
	StartedAt       time.Time  `json:"started_at"`
	FinishedAt      *time.Time `json:"finished_at"`
	DurationSeconds *float64   `json:"duration_seconds"`
	QueueSeconds    *float64   `json:"queue_seconds"`
	RetryCount      uint32     `json:"retry_count"`
	CancelReason    *string    `json:"cancel_reason"`
	TriggerSource   *string    `json:"trigger_source"`
	CommitHash      *string    `json:"commit_hash"`
	Branch          *string    `json:"branch"`
	PRNumber        *uint32    `json:"pr_number"`
	TeamID          *string    `json:"team_id"`
	ServiceID       *string    `json:"service_id"`
	LastSynced      time.Time  `json:"last_synced"`
}

type githubTestsJobRow struct {
	OrgID           string     `json:"org_id"`
	RepoID          string     `json:"repo_id"`
	RunID           string     `json:"run_id"`
	JobID           string     `json:"job_id"`
	JobName         string     `json:"job_name"`
	Stage           *string    `json:"stage"`
	Status          *string    `json:"status"`
	StartedAt       *time.Time `json:"started_at"`
	FinishedAt      *time.Time `json:"finished_at"`
	DurationSeconds *float64   `json:"duration_seconds"`
	RunnerType      *string    `json:"runner_type"`
	RetryAttempt    uint32     `json:"retry_attempt"`
	LastSynced      time.Time  `json:"last_synced"`
}

type githubTestsAcceptanceRow struct {
	OrgID        string    `json:"org_id"`
	RepoID       string    `json:"repo_id"`
	RunID        string    `json:"run_id"`
	CheckKey     string    `json:"check_key"`
	CheckName    string    `json:"check_name"`
	Provider     string    `json:"provider"`
	Requirement  string    `json:"requirement"`
	Result       string    `json:"result"`
	RuleVersion  string    `json:"rule_version"`
	Provenance   string    `json:"provenance"`
	ObservedAt   time.Time `json:"observed_at"`
	TargetBranch *string   `json:"target_branch"`
	PRNumber     *uint32   `json:"pr_number"`
	SourceURL    *string   `json:"source_url"`
	LastSynced   time.Time `json:"last_synced"`
}

type githubTestsJobsPayload struct {
	Jobs []githubTestsJobPayload `json:"jobs"`
}
type githubTestsJobPayload struct {
	ID          json.Number `json:"id"`
	Name        any         `json:"name"`
	Status      any         `json:"status"`
	Conclusion  any         `json:"conclusion"`
	StartedAt   *string     `json:"started_at"`
	CompletedAt *string     `json:"completed_at"`
	Labels      []any       `json:"labels"`
}
type githubTestsArtifactsPayload struct {
	Artifacts []githubTestsArtifactPayload `json:"artifacts"`
}
type githubTestsArtifactPayload struct {
	ID      json.Number `json:"id"`
	Expired bool        `json:"expired"`
}
type githubTestsRequiredPayload struct {
	Contexts []any `json:"contexts"`
	Checks   []struct {
		Context any `json:"context"`
	} `json:"checks"`
}

// GitHubTestsRouteHandler ports the complete active Python sync_tests unit:
// Actions runs, jobs, acceptance policy, and bounded report artifacts. Every
// bounded collection fails the unit when a next page exists, so incomplete
// work never commits effects or advances the watermark.
type GitHubTestsRouteHandler struct {
	MaxRuns            int
	MaxArtifactsPerRun int
	MaxJobPages        int
}

func (handler GitHubTestsRouteHandler) Collect(
	ctx context.Context, claim Claim, _ providerfoundation.Credential,
	client *providerfoundation.HTTPClient, normalizedAt time.Time,
) (CompleteRouteBatch, error) {
	if ctx == nil || claim.Validate() != nil || claim.Provider != "github" || (claim.Dataset != "tests" && claim.Dataset != "cicd") ||
		client == nil || client.Provider != "github" || client.BaseURL == nil || normalizedAt.IsZero() {
		return CompleteRouteBatch{}, ErrInvalidConfiguration
	}
	normalizedAt = normalizedAt.UTC().Truncate(time.Millisecond)
	owner, repository, err := splitGitHubRepository(claim.SourceExternalID)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	root := providerRelativePath(client, "repos", owner, repository)
	var repo gitHubRepositoryPayload
	if err := fetchObject(ctx, client, root, &repo); err != nil {
		return CompleteRouteBatch{}, err
	}
	repoID, err := repositoryIdentity(repo.FullName)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	maxRuns := handler.MaxRuns
	if maxRuns == 0 {
		maxRuns = githubTestsMaxRuns
	}
	maxArtifacts := handler.MaxArtifactsPerRun
	if maxArtifacts == 0 {
		maxArtifacts = githubTestsMaxArtifacts
	}
	jobPages := handler.MaxJobPages
	if jobPages == 0 {
		jobPages = nativeMaxPages
	}
	if maxRuns < 1 || maxRuns > githubTestsMaxRuns || maxArtifacts < 1 || maxArtifacts > githubTestsMaxArtifacts || jobPages < 1 || jobPages > nativeMaxPages {
		return CompleteRouteBatch{}, ErrInvalidConfiguration
	}
	runQuery := url.Values{"per_page": {"100"}}
	if claim.SinceAt != nil || claim.BeforeAt != nil {
		start, end := "*", "*"
		if claim.SinceAt != nil {
			start = claim.SinceAt.UTC().Format(time.RFC3339)
		}
		if claim.BeforeAt != nil {
			end = claim.BeforeAt.UTC().Format(time.RFC3339)
		}
		runQuery.Set("created", start+".."+end)
	}
	runPages := (maxRuns + nativePerPage - 1) / nativePerPage
	runsPage, err := providerfoundation.CollectGitHubLinkPages(ctx, client, providerfoundation.GitHubPageOptions{
		Path: root + "/actions/runs", Query: runQuery, DataKey: "workflow_runs", MaxPages: runPages,
	})
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	if runsPage.CapReached || len(runsPage.Items) > maxRuns {
		return CompleteRouteBatch{}, ErrPaginationCapExceeded
	}
	// The active Python report producer performs a second, server-side run
	// selection scoped to the repository default branch. Pipeline/job rows are
	// intentionally broader; report artifacts from feature and PR branches are
	// outside the sync_tests report boundary.
	artifactRunQuery := url.Values{"per_page": {"100"}}
	if repo.DefaultBranch != "" {
		artifactRunQuery.Set("branch", repo.DefaultBranch)
	}
	if claim.SinceAt != nil {
		artifactRunQuery.Set("created", ">="+claim.SinceAt.UTC().Format(time.DateOnly))
	}
	artifactRunsPage, err := providerfoundation.CollectGitHubLinkPages(ctx, client, providerfoundation.GitHubPageOptions{
		Path: root + "/actions/runs", Query: artifactRunQuery, DataKey: "workflow_runs", MaxPages: runPages,
	})
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	if artifactRunsPage.CapReached || len(artifactRunsPage.Items) > maxRuns {
		return CompleteRouteBatch{}, ErrPaginationCapExceeded
	}
	pipelines := make([]githubTestsPipelineRow, 0, len(runsPage.Items))
	jobs := make([]githubTestsJobRow, 0)
	acceptance := make([]githubTestsAcceptanceRow, 0)
	suites := make([]testSuiteResultRow, 0)
	cases := make([]testCaseResultRow, 0)
	coverage := make([]coverageSnapshotRow, 0)
	incomplete := make([]GitHubTestsIncomplete, 0)
	requests, pages := 1+runsPage.Pages+artifactRunsPage.Pages, runsPage.Pages+artifactRunsPage.Pages
	policyCache := map[string]githubTestsPolicy{}
	for _, raw := range runsPage.Items {
		var run gitHubWorkflowRunPayload
		decoder := json.NewDecoder(strings.NewReader(string(raw)))
		decoder.UseNumber()
		if decoder.Decode(&run) != nil {
			return CompleteRouteBatch{}, providerfoundation.ErrNormalizationInvalid
		}
		pipeline, include := normalizeGitHubTestsPipeline(claim, repoID, run, normalizedAt)
		if !include || ciPipelineRunOutsideWindow(pipeline.StartedAt, claim) {
			continue
		}
		pipelines = append(pipelines, pipeline)
		jobPage, pageErr := providerfoundation.CollectGitHubLinkPages(ctx, client, providerfoundation.GitHubPageOptions{
			Path: root + "/actions/runs/" + url.PathEscape(pipeline.RunID) + "/jobs", Query: url.Values{"per_page": {"100"}}, DataKey: "jobs", MaxPages: jobPages,
		})
		if pageErr != nil {
			return CompleteRouteBatch{}, pageErr
		}
		requests += jobPage.Pages
		pages += jobPage.Pages
		if jobPage.CapReached {
			return CompleteRouteBatch{}, ErrPaginationCapExceeded
		}
		runJobs := make([]githubTestsJobRow, 0, len(jobPage.Items))
		for _, jobRaw := range jobPage.Items {
			var job githubTestsJobPayload
			decoder := json.NewDecoder(strings.NewReader(string(jobRaw)))
			decoder.UseNumber()
			if decoder.Decode(&job) != nil {
				return CompleteRouteBatch{}, providerfoundation.ErrNormalizationInvalid
			}
			row, ok := normalizeGitHubTestsJob(claim, repoID, pipeline.RunID, pipeline.RetryCount, job, normalizedAt)
			if ok {
				runJobs = append(runJobs, row)
				jobs = append(jobs, row)
			}
		}
		targetBranch, prNumber := gitHubTestsTarget(run)
		policy := githubTestsPolicy{provenance: "github.branch_protection.target_branch_unavailable"}
		if targetBranch != nil {
			cached, ok := policyCache[*targetBranch]
			if !ok {
				cached, err = fetchGitHubTestsPolicy(ctx, client, root, *targetBranch)
				requests++
				if err != nil {
					return CompleteRouteBatch{}, err
				}
				policyCache[*targetBranch] = cached
			}
			policy = cached
		}
		acceptance = append(acceptance, projectGitHubTestsChecks(
			claim, repoID, pipeline, runJobs, policy, targetBranch, prNumber,
			testsOptionalString(stringValue(run.HTMLURL)), normalizedAt,
		)...)
	}
	for _, raw := range artifactRunsPage.Items {
		var run gitHubWorkflowRunPayload
		decoder := json.NewDecoder(strings.NewReader(string(raw)))
		decoder.UseNumber()
		if decoder.Decode(&run) != nil {
			return CompleteRouteBatch{}, providerfoundation.ErrNormalizationInvalid
		}
		pipeline, include := normalizeGitHubTestsPipeline(claim, repoID, run, normalizedAt)
		if !include || (claim.BeforeAt != nil && pipeline.StartedAt.After(claim.BeforeAt.UTC())) {
			continue
		}
		artPage, pageErr := providerfoundation.CollectGitHubLinkPages(ctx, client, providerfoundation.GitHubPageOptions{
			Path: root + "/actions/runs/" + url.PathEscape(pipeline.RunID) + "/artifacts", Query: url.Values{"per_page": {"100"}}, DataKey: "artifacts", MaxPages: 1,
		})
		if pageErr != nil {
			return CompleteRouteBatch{}, pageErr
		}
		requests += artPage.Pages
		pages += artPage.Pages
		if artPage.CapReached || len(artPage.Items) > maxArtifacts {
			return CompleteRouteBatch{}, ErrPaginationCapExceeded
		}
		for _, artifactRaw := range artPage.Items {
			var artifact githubTestsArtifactPayload
			decoder := json.NewDecoder(strings.NewReader(string(artifactRaw)))
			decoder.UseNumber()
			if decoder.Decode(&artifact) != nil || artifact.ID == "" {
				return CompleteRouteBatch{}, providerfoundation.ErrNormalizationInvalid
			}
			if artifact.Expired {
				continue
			}
			archive, used, downloadErr := downloadGitHubTestsArtifact(ctx, client, root, string(artifact.ID))
			requests += used
			if downloadErr != nil {
				return CompleteRouteBatch{}, downloadErr
			}
			if len(archive) == 0 {
				continue
			}
			rows, parseErr := parseGitHubTestsArtifact(archive, repoID, pipeline.RunID, claim.OrgID, pipeline.StartedAtPtr(), pipeline.FinishedAt, normalizedAt)
			if parseErr != nil {
				return CompleteRouteBatch{}, fmt.Errorf("%w: reports skipped=%d: %v", ErrGitHubTestsIncomplete, rows.Skipped, parseErr)
			}
			reportIncomplete, optional := rows.optionalIncomplete()
			if !optional {
				return CompleteRouteBatch{}, fmt.Errorf("%w: reports skipped=%d: unsafe archive bounds", ErrGitHubTestsIncomplete, rows.Skipped)
			}
			for _, observation := range reportIncomplete {
				incomplete = mergeGitHubTestsIncomplete(incomplete, observation)
			}
			suites = append(suites, rows.Suites...)
			cases = append(cases, rows.Cases...)
			coverage = append(coverage, rows.Coverage...)
		}
	}
	effects, err := testOpsEffects(pipelines, jobs, acceptance, suites, cases, coverage)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	watermark := claim.BeforeAt
	if len(incomplete) > 0 {
		watermark = nil
	}
	return CompleteRouteBatch{Effects: effects, Watermark: watermark, Result: map[string]any{
		"pipeline_runs_synced": len(pipelines), "job_runs_synced": len(jobs), "acceptance_checks_synced": len(acceptance),
		"test_suites_synced": len(suites), "test_cases_synced": len(cases), "coverage_snapshots_synced": len(coverage), "repo": repo.FullName,
		"reports_complete": len(incomplete) == 0,
		"reports_skipped":  githubTestsIncompleteCount(incomplete),
		"incomplete":       incomplete,
	}, Evidence: FetchEvidence{Provider: claim.Provider, Dataset: claim.Dataset, Requests: requests, Pages: pages, Records: len(pipelines) + len(jobs) + len(acceptance) + len(suites) + len(cases) + len(coverage)}}, nil
}

func mergeGitHubTestsIncomplete(
	current []GitHubTestsIncomplete,
	observation GitHubTestsIncomplete,
) []GitHubTestsIncomplete {
	for index := range current {
		if current[index].Component == observation.Component && current[index].Cause == observation.Cause {
			current[index].Count += observation.Count
			return current
		}
	}
	return append(current, observation)
}

func githubTestsIncompleteCount(incomplete []GitHubTestsIncomplete) int {
	total := 0
	for _, observation := range incomplete {
		total += observation.Count
	}
	return total
}

func (row githubTestsPipelineRow) StartedAtPtr() *time.Time { value := row.StartedAt; return &value }

func normalizeGitHubTestsPipeline(claim Claim, repoID string, run gitHubWorkflowRunPayload, at time.Time) (githubTestsPipelineRow, bool) {
	queuedRaw := parseGitHubWorkflowTime(run.CreatedAt)
	startedRaw := parseGitHubWorkflowTime(run.RunStartedAt)
	if startedRaw == nil {
		startedRaw = queuedRaw
	}
	if startedRaw == nil {
		return githubTestsPipelineRow{}, false
	}
	finishedRaw := parseGitHubWorkflowTime(run.UpdatedAt)
	queued := queuedRaw
	started := *startedRaw
	finished := finishedRaw
	return githubTestsPipelineRow{OrgID: claim.OrgID, RepoID: repoID, RunID: stringValue(run.ID), PipelineName: testsOptionalString(stringValue(run.Name)),
		Provider: "github_actions", Status: testsOptionalString(mapGitHubTestsStatus(stringValue(run.Status), stringValue(run.Conclusion))), QueuedAt: queued, StartedAt: started,
		FinishedAt: finished, DurationSeconds: secondsBetween(startedRaw, finishedRaw), QueueSeconds: secondsBetween(queuedRaw, startedRaw), RetryCount: workflowRetryCount(run.RunAttempt),
		TriggerSource: testsOptionalString(gitHubTestsTrigger(stringValue(run.Event))), CommitHash: testsOptionalString(stringValue(run.HeadSHA)), Branch: testsOptionalString(stringValue(run.HeadBranch)),
		PRNumber: gitHubTestsPRNumber(run), LastSynced: at}, true
}

func normalizeGitHubTestsJob(claim Claim, repoID, runID string, retry uint32, job githubTestsJobPayload, at time.Time) (githubTestsJobRow, bool) {
	if stringValue(job.ID) == "" {
		return githubTestsJobRow{}, false
	}
	startedRaw, finishedRaw := parseGitHubWorkflowTime(job.StartedAt), parseGitHubWorkflowTime(job.CompletedAt)
	started, finished := startedRaw, finishedRaw
	labels := make([]string, 0, len(job.Labels))
	for _, label := range job.Labels {
		labels = append(labels, strings.ToLower(stringValue(label)))
	}
	var runner *string
	if slices.Contains(labels, "self-hosted") {
		runner = testsOptionalString("self-hosted")
	} else if len(labels) > 0 {
		runner = testsOptionalString("hosted")
	}
	return githubTestsJobRow{OrgID: claim.OrgID, RepoID: repoID, RunID: runID, JobID: stringValue(job.ID), JobName: firstNonEmpty(stringValue(job.Name), "job"),
		Status: testsOptionalString(mapGitHubTestsJobStatus(stringValue(job.Status), stringValue(job.Conclusion))), StartedAt: started, FinishedAt: finished,
		DurationSeconds: secondsBetween(startedRaw, finishedRaw), RunnerType: runner, RetryAttempt: retry, LastSynced: at}, true
}

func secondsBetween(start, end *time.Time) *float64 {
	if start == nil || end == nil {
		return nil
	}
	value := max(0, end.Sub(*start).Seconds())
	return &value
}
func mapGitHubTestsStatus(status, conclusion string) string {
	switch conclusion {
	case "success":
		return "success"
	case "failure", "startup_failure", "action_required":
		return "failure"
	case "cancelled", "neutral":
		return "cancelled"
	case "timed_out":
		return "timeout"
	}
	switch status {
	case "in_progress", "requested", "waiting", "pending":
		return "running"
	case "queued":
		return "queued"
	}
	return firstNonEmpty(conclusion, status)
}
func mapGitHubTestsJobStatus(status, conclusion string) string {
	if conclusion == "skipped" {
		return "skipped"
	}
	return mapGitHubTestsStatus(status, conclusion)
}
func gitHubTestsTrigger(value string) string {
	switch strings.ToLower(value) {
	case "pull_request":
		return "pr"
	case "workflow_dispatch":
		return "manual"
	case "repository_dispatch":
		return "api"
	default:
		return strings.ToLower(value)
	}
}
func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
func gitHubTestsPRNumber(run gitHubWorkflowRunPayload) *uint32 {
	if len(run.PullRequests) == 0 {
		return nil
	}
	value, err := json.Number(stringValue(run.PullRequests[0].Number)).Int64()
	if err != nil || value < 0 {
		return nil
	}
	result := uint32(value)
	return &result
}
func gitHubTestsTarget(run gitHubWorkflowRunPayload) (*string, *uint32) {
	var branch *string
	if len(run.PullRequests) > 0 {
		branch = testsOptionalString(stringValue(run.PullRequests[0].Base.Ref))
	}
	return branch, gitHubTestsPRNumber(run)
}

type githubTestsPolicy struct {
	required   map[string]struct{}
	known      bool
	provenance string
}

func fetchGitHubTestsPolicy(ctx context.Context, client *providerfoundation.HTTPClient, root, branch string) (githubTestsPolicy, error) {
	path := root + "/branches/" + url.PathEscape(branch) + "/protection/required_status_checks"
	response, err := client.Do(ctx, http.MethodGet, path, nil)
	if err != nil {
		var providerErr *providerfoundation.ProviderError
		if errors.As(err, &providerErr) && providerErr.StatusCode > 0 {
			return githubTestsPolicy{provenance: fmt.Sprintf("github.branch_protection.http_%d", providerErr.StatusCode)}, nil
		}
		return githubTestsPolicy{}, err
	}
	defer response.Body.Close()
	var payload githubTestsRequiredPayload
	if json.NewDecoder(io.LimitReader(response.Body, nativeMaxObjectBytes+1)).Decode(&payload) != nil {
		return githubTestsPolicy{provenance: "github.branch_protection.invalid_payload"}, nil
	}
	required := map[string]struct{}{}
	for _, value := range payload.Contexts {
		if name := stringValue(value); name != "" {
			required[name] = struct{}{}
		}
	}
	for _, check := range payload.Checks {
		if name := stringValue(check.Context); name != "" {
			required[name] = struct{}{}
		}
	}
	return githubTestsPolicy{required: required, known: true, provenance: "github.branch_protection.required_status_checks"}, nil
}

func projectGitHubTestsChecks(claim Claim, repoID string, pipeline githubTestsPipelineRow, jobs []githubTestsJobRow, policy githubTestsPolicy, branch *string, pr *uint32, sourceURL *string, at time.Time) []githubTestsAcceptanceRow {
	byName := map[string]githubTestsJobRow{}
	names := make([]string, 0, len(jobs))
	for _, job := range jobs {
		if _, ok := byName[job.JobName]; !ok {
			names = append(names, job.JobName)
		}
		byName[job.JobName] = job
	}
	if policy.known {
		for name := range policy.required {
			if _, ok := byName[name]; !ok {
				names = append(names, name)
			}
		}
	}
	sortTestOpsAcceptanceNames(names)
	result := make([]githubTestsAcceptanceRow, 0, len(names))
	for _, name := range names {
		job, exists := byName[name]
		requirement := "unknown"
		if policy.known {
			requirement = "optional"
			if _, ok := policy.required[name]; ok {
				requirement = "required"
			}
		}
		status := "unknown"
		if exists {
			status = canonicalGitHubTestsResult(job.Status)
		}
		observed := pipeline.StartedAt
		if pipeline.FinishedAt != nil {
			observed = *pipeline.FinishedAt
		}
		result = append(result, githubTestsAcceptanceRow{OrgID: claim.OrgID, RepoID: repoID, RunID: pipeline.RunID, CheckKey: gitHubTestsCheckKey("github_actions", name), CheckName: name, Provider: "github_actions", Requirement: requirement, Result: status, RuleVersion: githubTestsRuleVersion, Provenance: policy.provenance, ObservedAt: observed, TargetBranch: branch, PRNumber: pr, SourceURL: sourceURL, LastSynced: at})
	}
	return result
}
func canonicalGitHubTestsResult(value *string) string {
	raw := ""
	if value != nil {
		raw = strings.ToLower(*value)
	}
	switch raw {
	case "success", "passed", "pass", "green", "succeeded":
		return "passed"
	case "failure", "failed", "error", "timed_out", "timeout", "action_required", "startup_failure":
		return "failed"
	case "skipped", "manual":
		return "skipped"
	case "queued", "pending", "requested", "waiting", "created", "preparing", "running", "in_progress":
		return "pending"
	default:
		return "unknown"
	}
}
func gitHubTestsCheckKey(provider, name string) string {
	digest := sha256.Sum256([]byte(provider + "\x00" + name))
	return provider + ":" + hex.EncodeToString(digest[:])[:24]
}

func downloadGitHubTestsArtifact(ctx context.Context, client *providerfoundation.HTTPClient, root, artifactID string) ([]byte, int, error) {
	response, err := client.Do(ctx, http.MethodGet, root+"/actions/artifacts/"+url.PathEscape(artifactID)+"/zip", nil)
	if err != nil {
		var providerErr *providerfoundation.ProviderError
		if errors.As(err, &providerErr) && (providerErr.StatusCode == http.StatusNotFound || providerErr.StatusCode == http.StatusGone) {
			return nil, 1, nil
		}
		return nil, 1, err
	}
	requests := 1
	if response.StatusCode >= 300 && response.StatusCode < 400 {
		location := response.Header.Get("Location")
		response.Body.Close()
		if location == "" {
			return nil, requests, ErrGitHubTestsIncomplete
		}
		response, err = client.DoUnauthenticated(ctx, http.MethodGet, location)
		requests++
		if err != nil {
			return nil, requests, err
		}
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusNotFound || response.StatusCode == http.StatusGone {
		return nil, requests, nil
	}
	if response.StatusCode >= 400 {
		return nil, requests, &providerfoundation.ProviderError{Class: providerfoundation.ErrorPermanent, StatusCode: response.StatusCode}
	}
	body, readErr := io.ReadAll(io.LimitReader(response.Body, githubTestsMaxDownloadSize+1))
	if readErr != nil || len(body) > githubTestsMaxDownloadSize {
		return nil, requests, ErrGitHubTestsIncomplete
	}
	return body, requests, nil
}

var _ CompleteRouteHandler = GitHubTestsRouteHandler{}
