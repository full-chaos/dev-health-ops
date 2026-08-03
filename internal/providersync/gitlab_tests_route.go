package providersync

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

const (
	gitLabTestsMaxPipelines = 200
	gitLabTestsMaxArtifacts = 25
	gitLabTestsMaxPages     = 30
	gitLabTestsMaxDownload  = 100 << 20
)

var ErrGitLabTestsIncomplete = errors.New("gitlab tests inventory incomplete")

type gitLabTestsPipelinePayload struct {
	ID           any            `json:"id"`
	Name         any            `json:"name"`
	Ref          any            `json:"ref"`
	Status       any            `json:"status"`
	CreatedAt    *string        `json:"created_at"`
	StartedAt    *string        `json:"started_at"`
	FinishedAt   *string        `json:"finished_at"`
	Source       any            `json:"source"`
	SHA          any            `json:"sha"`
	WebURL       any            `json:"web_url"`
	MergeRequest map[string]any `json:"merge_request"`
}

type gitLabTestsJobPayload struct {
	ID            any            `json:"id"`
	Name          any            `json:"name"`
	Stage         any            `json:"stage"`
	Status        any            `json:"status"`
	StartedAt     *string        `json:"started_at"`
	FinishedAt    *string        `json:"finished_at"`
	Runner        map[string]any `json:"runner"`
	TagList       []any          `json:"tag_list"`
	Retried       any            `json:"retried"`
	ArtifactsFile any            `json:"artifacts_file"`
	Artifacts     any            `json:"artifacts"`
}

type gitLabTestsReportPayload struct {
	Suites []gitLabTestsSuitePayload `json:"test_suites"`
}

type gitLabTestsSuitePayload struct {
	Name      any                      `json:"name"`
	TotalTime any                      `json:"total_time"`
	Cases     []gitLabTestsCasePayload `json:"test_cases"`
}

type gitLabTestsCasePayload struct {
	Name          any `json:"name"`
	ClassName     any `json:"classname"`
	Status        any `json:"status"`
	ExecutionTime any `json:"execution_time"`
	StackTrace    any `json:"stack_trace"`
	SystemOutput  any `json:"system_output"`
}

// GitLabTestsRouteHandler is the one canonical complete TestOps producer for
// both gitlab/cicd and gitlab/tests. The aliases intentionally execute the
// same fetch, normalization, and six-effect boundary; only request evidence
// retains the claimed dataset identity.
type GitLabTestsRouteHandler struct {
	MaxPipelines int
	MaxPages     int
}

type gitLabTestsCountingDoer struct {
	delegate providerfoundation.HTTPDoer
	attempts *int
}

func (doer gitLabTestsCountingDoer) Do(request *http.Request) (*http.Response, error) {
	attempts := doer.attempts
	*attempts = *attempts + 1
	return doer.delegate.Do(request)
}

func (handler GitLabTestsRouteHandler) Collect(
	ctx context.Context,
	claim Claim,
	_ providerfoundation.Credential,
	client *providerfoundation.HTTPClient,
	normalizedAt time.Time,
) (CompleteRouteBatch, error) {
	if ctx == nil || claim.Validate() != nil || claim.Provider != "gitlab" ||
		(claim.Dataset != "cicd" && claim.Dataset != "tests") || client == nil ||
		client.Provider != "gitlab" || client.BaseURL == nil || normalizedAt.IsZero() {
		return CompleteRouteBatch{}, ErrInvalidConfiguration
	}
	normalizedAt = normalizedAt.UTC().Truncate(time.Millisecond)
	maxPipelines := handler.MaxPipelines
	if maxPipelines == 0 {
		maxPipelines = gitLabTestsMaxPipelines
	}
	maxPages := handler.MaxPages
	if maxPages == 0 {
		maxPages = gitLabTestsMaxPages
	}
	if maxPipelines < 1 || maxPipelines > gitLabTestsMaxPipelines || maxPages < 1 || maxPages > gitLabTestsMaxPages {
		return CompleteRouteBatch{}, ErrInvalidConfiguration
	}
	projectID, err := gitLabProjectID(claim.SourceExternalID)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	requests := 0
	counted := *client
	counted.Doer = gitLabTestsCountingDoer{delegate: client.Doer, attempts: &requests}
	root := providerRelativePath(client, "api", "v4", "projects", projectID)
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
	required, provenance := projectGitLabTestsRequirement(project)
	query := url.Values{"order_by": {"updated_at"}, "sort": {"desc"}}
	if claim.SinceAt != nil {
		query.Set("updated_after", claim.SinceAt.UTC().Format(time.RFC3339Nano))
	}
	if claim.BeforeAt != nil {
		query.Set("updated_before", claim.BeforeAt.UTC().Format(time.RFC3339Nano))
	}
	adapterPage, err := providerfoundation.CollectGitLabPageParamPages(ctx, &counted, providerfoundation.GitLabPageOptions{
		Path: root + "/pipelines", Query: query, PerPage: nativePerPage, MaxPages: maxPages,
	})
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	if adapterPage.CapReached {
		return CompleteRouteBatch{}, ErrPaginationCapExceeded
	}
	pipelines := make([]githubTestsPipelineRow, 0, len(adapterPage.Items))
	jobs := make([]githubTestsJobRow, 0)
	acceptance := make([]githubTestsAcceptanceRow, 0)
	pages := adapterPage.Pages
	for _, raw := range adapterPage.Items {
		payload, err := decodeGitLabTestsPipeline(raw)
		if err != nil {
			return CompleteRouteBatch{}, err
		}
		if stringValue(payload.ID) == "" {
			return CompleteRouteBatch{}, providerfoundation.ErrNormalizationInvalid
		}
		startedAt := gitLabTestsPipelineStartedAt(payload)
		if startedAt == nil {
			continue
		}
		if ciPipelineRunOutsideWindow(*startedAt, claim) {
			continue
		}
		pipeline, ok := normalizeGitLabTestsPipeline(claim, repoID, payload, normalizedAt)
		if !ok {
			return CompleteRouteBatch{}, providerfoundation.ErrNormalizationInvalid
		}
		pipeline.PRNumber, err = resolveGitLabTestsMergeRequest(ctx, &counted, root, payload)
		if err != nil {
			return CompleteRouteBatch{}, err
		}
		jobPage, err := providerfoundation.CollectGitLabPageParamPages(ctx, &counted, providerfoundation.GitLabPageOptions{
			Path:  root + "/pipelines/" + url.PathEscape(pipeline.RunID) + "/jobs",
			Query: url.Values{"include_retried": {"true"}}, PerPage: nativePerPage, MaxPages: maxPages,
		})
		if err != nil {
			return CompleteRouteBatch{}, err
		}
		pages += jobPage.Pages
		if jobPage.CapReached {
			return CompleteRouteBatch{}, ErrPaginationCapExceeded
		}
		runJobs := make([]githubTestsJobRow, 0, len(jobPage.Items))
		for _, jobRaw := range jobPage.Items {
			job, err := decodeGitLabTestsJob(jobRaw)
			if err != nil {
				return CompleteRouteBatch{}, err
			}
			row, ok := normalizeGitLabTestsJob(claim, repoID, pipeline.RunID, job, normalizedAt)
			if !ok {
				return CompleteRouteBatch{}, providerfoundation.ErrNormalizationInvalid
			}
			runJobs = append(runJobs, row)
			jobs = append(jobs, row)
		}
		pipelines = append(pipelines, pipeline)
		acceptance = append(acceptance, projectGitLabTestsChecks(
			claim, repoID, pipeline, payload, runJobs, required, provenance, normalizedAt,
		)...)
	}

	reportQuery := url.Values{"order_by": {"updated_at"}, "sort": {"desc"}}
	if claim.SinceAt != nil {
		reportQuery.Set("updated_after", claim.SinceAt.UTC().Format(time.RFC3339Nano))
	}
	reportPage, err := providerfoundation.CollectGitLabPageParamPages(ctx, &counted, providerfoundation.GitLabPageOptions{
		Path: root + "/pipelines", Query: reportQuery, PerPage: nativePerPage, MaxPages: maxPages,
	})
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	pages += reportPage.Pages
	if reportPage.CapReached {
		return CompleteRouteBatch{}, ErrPaginationCapExceeded
	}
	reportPipelines, err := selectGitLabTestsReportPipelines(
		reportPage.Items, project.DefaultBranch, maxPipelines, claim.BeforeAt,
	)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	suites := make([]testSuiteResultRow, 0)
	cases := make([]testCaseResultRow, 0)
	coverage := make([]coverageSnapshotRow, 0)
	for _, payload := range reportPipelines {
		runID := stringValue(payload.ID)
		started := gitLabTestsPipelineStartedAt(payload)
		finished := parseGitLabTestsTime(payload.FinishedAt)
		report, present, err := fetchGitLabTestsReport(ctx, &counted, root, runID)
		if err != nil {
			return CompleteRouteBatch{}, err
		}
		if present {
			reportSuites, reportCases, err := normalizeGitLabNativeTestReport(claim, repoID, runID, report, started, finished, normalizedAt)
			if err != nil {
				return CompleteRouteBatch{}, err
			}
			suites = append(suites, reportSuites...)
			cases = append(cases, reportCases...)
		}
		jobPage, err := providerfoundation.CollectGitLabPageParamPages(ctx, &counted, providerfoundation.GitLabPageOptions{
			Path: root + "/pipelines/" + url.PathEscape(runID) + "/jobs", PerPage: nativePerPage, MaxPages: 1, SinglePage: true,
		})
		if err != nil {
			if fatal := gitLabTestsOptionalError(ctx, err); fatal != nil {
				return CompleteRouteBatch{}, fatal
			}
			continue
		}
		pages += jobPage.Pages
		if jobPage.CapReached {
			return CompleteRouteBatch{}, ErrPaginationCapExceeded
		}
		artifactJobs, err := selectGitLabTestsArtifactJobs(jobPage.Items, gitLabTestsMaxArtifacts)
		if err != nil {
			return CompleteRouteBatch{}, err
		}
		for _, job := range artifactJobs {
			jobID := stringValue(job.ID)
			archive, err := downloadGitLabTestsArtifact(ctx, &counted, root, jobID)
			if err != nil {
				return CompleteRouteBatch{}, err
			}
			if len(archive) == 0 {
				continue
			}
			rows, err := parseGitHubTestsArtifact(archive, repoID, runID, claim.OrgID, started, finished, normalizedAt)
			if err != nil || rows.Skipped != 0 {
				return CompleteRouteBatch{}, fmt.Errorf("%w: reports skipped=%d: %v", ErrGitLabTestsIncomplete, rows.Skipped, err)
			}
			// GitLab's native test_report JSON is authoritative for suites/cases.
			coverage = append(coverage, rows.Coverage...)
		}
	}
	effects, err := testOpsEffects(pipelines, jobs, acceptance, suites, cases, coverage)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	actualRouteFamily := gitLabTestsActualRouteFamily(claim.Dataset)
	return CompleteRouteBatch{
		Effects: effects, Watermark: claim.BeforeAt,
		Result: map[string]any{
			"pipeline_runs_synced": len(pipelines), "job_runs_synced": len(jobs),
			"acceptance_checks_synced": len(acceptance), "test_suites_synced": len(suites),
			"test_cases_synced": len(cases), "coverage_snapshots_synced": len(coverage),
			"repo": fullName, "project_id": parsedProjectID,
			"actual_route_family": actualRouteFamily,
			"observations": map[string]any{
				"provider_usage": []any{map[string]any{
					"transport": "rest", "route_family": actualRouteFamily,
					"dimension": "rest_core", "request_count": requests,
				}},
			},
		},
		Evidence: FetchEvidence{Provider: claim.Provider, Dataset: claim.Dataset, Requests: requests, Pages: pages,
			Records: len(pipelines) + len(jobs) + len(acceptance) + len(suites) + len(cases) + len(coverage)},
	}, nil
}

func gitLabTestsPipelineStartedAt(payload gitLabTestsPipelinePayload) *time.Time {
	started := parseGitLabTestsTime(payload.StartedAt)
	if started == nil {
		started = parseGitLabTestsTime(payload.CreatedAt)
	}
	return started
}

func parseGitLabTestsTime(value *string) *time.Time {
	if value == nil || *value == "" {
		return nil
	}
	parsed, err := time.Parse(time.RFC3339Nano, *value)
	if err != nil {
		return nil
	}
	return &parsed
}

func gitLabTestsActualRouteFamily(dataset string) string {
	switch dataset {
	case "cicd":
		return "pipelines"
	case "tests":
		return "tests"
	default:
		return ""
	}
}

func selectGitLabTestsReportPipelines(
	rawItems []json.RawMessage,
	defaultBranch string,
	maxPipelines int,
	before *time.Time,
) ([]gitLabTestsPipelinePayload, error) {
	if maxPipelines < 1 || maxPipelines > gitLabTestsMaxPipelines {
		return nil, ErrInvalidConfiguration
	}
	selected := make([]gitLabTestsPipelinePayload, 0, min(len(rawItems), maxPipelines))
	accepted := 0
	for _, raw := range rawItems {
		if accepted >= maxPipelines {
			break
		}
		payload, err := decodeGitLabTestsPipeline(raw)
		if err != nil {
			return nil, err
		}
		ref := stringValue(payload.Ref)
		if defaultBranch != "" && ref != "" && ref != defaultBranch {
			continue
		}
		if stringValue(payload.ID) == "" {
			return nil, providerfoundation.ErrNormalizationInvalid
		}
		// Python applies the cap after the default-branch filter but before its
		// inclusive upper-window check. Preserve that ordering exactly.
		accepted++
		started := parseGitLabTestsTime(payload.StartedAt)
		if started == nil {
			started = parseGitLabTestsTime(payload.CreatedAt)
		}
		if before != nil && started != nil && started.UTC().After(before.UTC()) {
			continue
		}
		selected = append(selected, payload)
	}
	return selected, nil
}

func selectGitLabTestsArtifactJobs(rawItems []json.RawMessage, maxArtifacts int) ([]gitLabTestsJobPayload, error) {
	if maxArtifacts < 1 || maxArtifacts > gitLabTestsMaxArtifacts {
		return nil, ErrInvalidConfiguration
	}
	selected := make([]gitLabTestsJobPayload, 0, min(len(rawItems), maxArtifacts))
	for _, raw := range rawItems {
		if len(selected) >= maxArtifacts {
			break
		}
		job, err := decodeGitLabTestsJob(raw)
		if err != nil {
			return nil, err
		}
		if !gitLabTestsJobHasArtifacts(job) {
			continue
		}
		if stringValue(job.ID) == "" {
			return nil, providerfoundation.ErrNormalizationInvalid
		}
		selected = append(selected, job)
	}
	return selected, nil
}

func decodeGitLabTestsPipeline(raw json.RawMessage) (gitLabTestsPipelinePayload, error) {
	var result gitLabTestsPipelinePayload
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&result); err != nil {
		return result, providerfoundation.ErrNormalizationInvalid
	}
	return result, nil
}

func decodeGitLabTestsJob(raw json.RawMessage) (gitLabTestsJobPayload, error) {
	var result gitLabTestsJobPayload
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&result); err != nil {
		return result, providerfoundation.ErrNormalizationInvalid
	}
	return result, nil
}

func normalizeGitLabTestsPipeline(claim Claim, repoID string, payload gitLabTestsPipelinePayload, at time.Time) (githubTestsPipelineRow, bool) {
	createdRaw := parseGitLabTestsTime(payload.CreatedAt)
	startedRaw := parseGitLabTestsTime(payload.StartedAt)
	if startedRaw == nil {
		startedRaw = createdRaw
	}
	if startedRaw == nil {
		return githubTestsPipelineRow{}, false
	}
	finishedRaw := parseGitLabTestsTime(payload.FinishedAt)
	runID := stringValue(payload.ID)
	if runID == "" {
		return githubTestsPipelineRow{}, false
	}
	created, started, finished := cloneTime(createdRaw), *cloneTime(startedRaw), cloneTime(finishedRaw)
	return githubTestsPipelineRow{
		OrgID: claim.OrgID, RepoID: repoID, RunID: runID,
		PipelineName: testsOptionalString(firstNonEmpty(stringValue(payload.Name), stringValue(payload.Ref))),
		Provider:     "gitlab_ci", Status: testsOptionalString(mapGitLabTestsPipelineStatus(stringValue(payload.Status))),
		QueuedAt: created, StartedAt: started, FinishedAt: finished,
		DurationSeconds: secondsBetween(startedRaw, finishedRaw), QueueSeconds: secondsBetween(createdRaw, startedRaw),
		TriggerSource: testsOptionalString(coerceGitLabTestsTrigger(stringValue(payload.Source))),
		CommitHash:    testsOptionalString(stringValue(payload.SHA)), Branch: testsOptionalString(stringValue(payload.Ref)),
		LastSynced: at,
	}, true
}

func normalizeGitLabTestsJob(claim Claim, repoID, runID string, payload gitLabTestsJobPayload, at time.Time) (githubTestsJobRow, bool) {
	jobID := stringValue(payload.ID)
	if jobID == "" {
		return githubTestsJobRow{}, false
	}
	started, finished := parseGitLabTestsTime(payload.StartedAt), parseGitLabTestsTime(payload.FinishedAt)
	var runner *string
	if value := stringValue(payload.Runner["runner_type"]); value != "" {
		runner = testsOptionalString(value)
	} else {
		tags := make([]string, 0, len(payload.TagList))
		for _, tag := range payload.TagList {
			tags = append(tags, strings.ToLower(stringValue(tag)))
		}
		if slices.Contains(tags, "self-hosted") {
			runner = testsOptionalString("self-hosted")
		} else if len(tags) > 0 {
			runner = testsOptionalString("hosted")
		}
	}
	retry := uint32(0)
	if value, ok := payload.Retried.(bool); ok && value {
		retry = 1
	}
	return githubTestsJobRow{
		OrgID: claim.OrgID, RepoID: repoID, RunID: runID, JobID: jobID,
		JobName: firstNonEmpty(stringValue(payload.Name), "job"), Stage: testsOptionalString(stringValue(payload.Stage)),
		Status:    testsOptionalString(mapGitLabTestsJobStatus(stringValue(payload.Status))),
		StartedAt: cloneTime(started), FinishedAt: cloneTime(finished), DurationSeconds: secondsBetween(started, finished),
		RunnerType: runner, RetryAttempt: retry, LastSynced: at,
	}, true
}

func mapGitLabTestsPipelineStatus(value string) string {
	switch value {
	case "failed":
		return "failure"
	case "canceled", "cancelled", "skipped":
		return "cancelled"
	case "manual", "scheduled", "pending", "created", "waiting_for_resource", "preparing":
		return "queued"
	default:
		return value
	}
}

func mapGitLabTestsJobStatus(value string) string {
	switch value {
	case "failed":
		return "failure"
	case "canceled", "cancelled":
		return "cancelled"
	case "manual", "skipped":
		return "skipped"
	case "pending", "created", "waiting_for_resource", "preparing":
		return "running"
	default:
		return value
	}
}

func coerceGitLabTestsTrigger(value string) string {
	switch strings.ToLower(value) {
	case "merge_request_event", "merge_request":
		return "pr"
	case "web", "manual":
		return "manual"
	case "api", "trigger":
		return "api"
	default:
		return strings.ToLower(value)
	}
}

func projectGitLabTestsRequirement(project repositoryPayload) (map[string]struct{}, string) {
	if project.OnlyAllowMergeIfPipelineSucceeds == nil {
		return nil, "gitlab.project_merge_policy.missing_field"
	}
	required := map[string]struct{}{}
	if *project.OnlyAllowMergeIfPipelineSucceeds {
		required["pipeline"] = struct{}{}
	}
	return required, "gitlab.project_merge_policy"
}

func resolveGitLabTestsMergeRequest(ctx context.Context, client *providerfoundation.HTTPClient, root string, pipeline gitLabTestsPipelinePayload) (*uint32, error) {
	if stringValue(pipeline.Source) != "merge_request_event" {
		return nil, nil
	}
	if value := stringValue(pipeline.MergeRequest["iid"]); value != "" {
		parsed, err := strconv.ParseUint(value, 10, 32)
		if err != nil {
			return nil, nil
		}
		result := uint32(parsed)
		return &result, nil
	}
	sha := stringValue(pipeline.SHA)
	if sha == "" {
		return nil, nil
	}
	response, err := client.Do(ctx, http.MethodGet, root+"/repository/commits/"+url.PathEscape(sha)+"/merge_requests", nil)
	if err != nil {
		if fatal := gitLabTestsOptionalError(ctx, err); fatal != nil {
			return nil, fatal
		}
		return nil, nil
	}
	defer response.Body.Close()
	var items []map[string]any
	decoder := json.NewDecoder(io.LimitReader(response.Body, nativeMaxObjectBytes+1))
	decoder.UseNumber()
	if err := decoder.Decode(&items); err != nil {
		return nil, providerfoundation.ErrNormalizationInvalid
	}
	candidates := map[uint32]struct{}{}
	exact := map[uint32]struct{}{}
	for _, item := range items {
		value, err := strconv.ParseUint(stringValue(item["iid"]), 10, 32)
		if err != nil {
			continue
		}
		iid := uint32(value)
		candidates[iid] = struct{}{}
		diff, _ := item["diff_refs"].(map[string]any)
		if stringValue(item["sha"]) == sha || stringValue(diff["head_sha"]) == sha {
			exact[iid] = struct{}{}
		}
	}
	selected := candidates
	if len(exact) > 0 {
		selected = exact
	}
	if len(selected) != 1 {
		return nil, nil
	}
	for iid := range selected {
		result := iid
		return &result, nil
	}
	return nil, nil
}

func projectGitLabTestsChecks(claim Claim, repoID string, pipeline githubTestsPipelineRow, payload gitLabTestsPipelinePayload, jobs []githubTestsJobRow, required map[string]struct{}, provenance string, at time.Time) []githubTestsAcceptanceRow {
	byName := map[string]*string{"pipeline": testsOptionalString(stringValue(payload.Status))}
	for _, job := range jobs {
		byName[job.JobName] = job.Status
	}
	names := make([]string, 0, len(byName)+len(required))
	for name := range byName {
		names = append(names, name)
	}
	if required != nil {
		for name := range required {
			if _, ok := byName[name]; !ok {
				names = append(names, name)
			}
		}
	}
	sortTestOpsAcceptanceNames(names)
	result := make([]githubTestsAcceptanceRow, 0, len(names))
	for _, name := range names {
		requirement := "unknown"
		if required != nil {
			requirement = "optional"
			if _, ok := required[name]; ok {
				requirement = "required"
			}
		}
		observed := pipeline.StartedAt
		if pipeline.FinishedAt != nil {
			observed = *pipeline.FinishedAt
		}
		result = append(result, githubTestsAcceptanceRow{
			OrgID: claim.OrgID, RepoID: repoID, RunID: pipeline.RunID,
			CheckKey: gitHubTestsCheckKey("gitlab_ci", name), CheckName: name, Provider: "gitlab_ci",
			Requirement: requirement, Result: canonicalGitHubTestsResult(byName[name]), RuleVersion: githubTestsRuleVersion,
			Provenance: provenance, ObservedAt: observed, TargetBranch: testsOptionalString(stringValue(payload.Ref)),
			PRNumber: pipeline.PRNumber, SourceURL: testsOptionalString(stringValue(payload.WebURL)), LastSynced: at,
		})
	}
	return result
}

func fetchGitLabTestsReport(ctx context.Context, client *providerfoundation.HTTPClient, root, runID string) (gitLabTestsReportPayload, bool, error) {
	var result gitLabTestsReportPayload
	response, err := client.Do(ctx, http.MethodGet, root+"/pipelines/"+url.PathEscape(runID)+"/test_report", nil)
	if err != nil {
		if fatal := gitLabTestsOptionalError(ctx, err); fatal != nil {
			return result, false, fatal
		}
		return result, false, nil
	}
	defer response.Body.Close()
	decoder := json.NewDecoder(io.LimitReader(response.Body, nativeMaxObjectBytes+1))
	decoder.UseNumber()
	if err := decoder.Decode(&result); err != nil {
		return result, false, providerfoundation.ErrNormalizationInvalid
	}
	return result, len(result.Suites) > 0, nil
}

func normalizeGitLabNativeTestReport(claim Claim, repoID, runID string, report gitLabTestsReportPayload, started, finished *time.Time, at time.Time) ([]testSuiteResultRow, []testCaseResultRow, error) {
	if claim.Provider != "gitlab" || (claim.Dataset != "cicd" && claim.Dataset != "tests") || repoID == "" || runID == "" || at.IsZero() {
		return nil, nil, ErrInvalidConfiguration
	}
	at = at.UTC().Truncate(time.Millisecond)
	suites := make([]testSuiteResultRow, 0, len(report.Suites))
	cases := make([]testCaseResultRow, 0)
	for _, rawSuite := range report.Suites {
		if len(rawSuite.Cases) == 0 {
			continue
		}
		name := firstNonEmpty(stringValue(rawSuite.Name), "unnamed")
		suiteID := hashTestIdentifier(runID, name, "")
		row := testSuiteResultRow{OrgID: claim.OrgID, RepoID: repoID, RunID: runID, SuiteID: suiteID,
			SuiteName: name, Framework: testsOptionalString("gitlab_ci"), TotalCount: int64(len(rawSuite.Cases)),
			DurationSeconds: optionalGitLabTestsFloat(rawSuite.TotalTime), StartedAt: cloneTime(started), FinishedAt: cloneTime(finished), LastSynced: at}
		for _, rawCase := range rawSuite.Cases {
			status := mapGitLabNativeCaseStatus(stringValue(rawCase.Status))
			switch status {
			case "passed":
				row.PassedCount++
			case "failed":
				row.FailedCount++
			case "skipped":
				row.SkippedCount++
			case "quarantined":
				row.QuarantinedCount++
			default:
				row.ErrorCount++
			}
			caseName := firstNonEmpty(stringValue(rawCase.Name), "unnamed")
			stack := firstNonEmpty(stringValue(rawCase.StackTrace), stringValue(rawCase.SystemOutput))
			if output := stringValue(rawCase.SystemOutput); output != "" {
				if stack != "" {
					stack += "\n"
				}
				stack += output
			}
			if len(stack) > 4096 {
				stack = stack[:4096]
			}
			cases = append(cases, testCaseResultRow{OrgID: claim.OrgID, RepoID: repoID, RunID: runID, SuiteID: suiteID,
				CaseID: hashTestIdentifier(suiteID, caseName), CaseName: caseName, ClassName: testsOptionalString(stringValue(rawCase.ClassName)),
				Status: status, DurationSeconds: optionalGitLabTestsFloat(rawCase.ExecutionTime), StackTrace: testsOptionalString(stack),
				IsQuarantined: status == "quarantined", LastSynced: at})
		}
		suites = append(suites, row)
	}
	return suites, cases, nil
}

func mapGitLabNativeCaseStatus(value string) string {
	switch strings.ToLower(value) {
	case "success":
		return "passed"
	case "failed":
		return "failed"
	case "skipped":
		return "skipped"
	case "error":
		return "error"
	default:
		return "error"
	}
}

func optionalGitLabTestsFloat(value any) *float64 {
	text := stringValue(value)
	if text == "" {
		return nil
	}
	parsed, err := strconv.ParseFloat(text, 64)
	if err != nil {
		return nil
	}
	return &parsed
}

func gitLabTestsJobHasArtifacts(job gitLabTestsJobPayload) bool {
	return gitLabTestsJSONTruthy(job.ArtifactsFile) || gitLabTestsJSONTruthy(job.Artifacts)
}

func gitLabTestsJSONTruthy(value any) bool {
	switch typed := value.(type) {
	case nil:
		return false
	case bool:
		return typed
	case string:
		return typed != ""
	case json.Number:
		parsed, err := typed.Float64()
		return err == nil && parsed != 0
	case []any:
		return len(typed) > 0
	case map[string]any:
		return len(typed) > 0
	default:
		return true
	}
}

func downloadGitLabTestsArtifact(ctx context.Context, client *providerfoundation.HTTPClient, root, jobID string) ([]byte, error) {
	response, err := client.Do(ctx, http.MethodGet, root+"/jobs/"+url.PathEscape(jobID)+"/artifacts", nil)
	if err != nil {
		var providerErr *providerfoundation.ProviderError
		if errors.As(err, &providerErr) && (providerErr.StatusCode == http.StatusNotFound || providerErr.StatusCode == http.StatusGone) {
			return nil, nil
		}
		return nil, err
	}
	if response.StatusCode >= 300 && response.StatusCode < 400 {
		location := response.Header.Get("Location")
		_ = response.Body.Close()
		if location == "" {
			return nil, ErrGitLabTestsIncomplete
		}
		response, err = client.DoUnauthenticated(ctx, http.MethodGet, location)
		if err != nil {
			return nil, err
		}
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusNotFound || response.StatusCode == http.StatusGone {
		return nil, nil
	}
	if response.StatusCode >= 400 {
		message, readErr := io.ReadAll(io.LimitReader(response.Body, 1<<20))
		if readErr != nil {
			return nil, readErr
		}
		if classified := providerfoundation.ClassifyHTTPWithMessage("gitlab", response.StatusCode, response.Header, string(message)); classified != nil {
			return nil, classified
		}
		return nil, &providerfoundation.ProviderError{Class: providerfoundation.ErrorPermanent, StatusCode: response.StatusCode}
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, gitLabTestsMaxDownload+1))
	if err != nil {
		return nil, err
	}
	if len(body) > gitLabTestsMaxDownload {
		return nil, ErrGitLabTestsIncomplete
	}
	return body, nil
}

func gitLabTestsOptionalError(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	// The active Python producer treats a missing optional report/policy/
	// artifact as absence. Every other class is an incomplete traversal and
	// must fail the combined unit; otherwise one alias could commit a partial
	// replacement row and advance its watermark.
	if gitLabErrorTreeOnlyProviderClasses(err, providerfoundation.ErrorNotFound) {
		return nil
	}
	return err
}

var _ CompleteRouteHandler = GitLabTestsRouteHandler{}
