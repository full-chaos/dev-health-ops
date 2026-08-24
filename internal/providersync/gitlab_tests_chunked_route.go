package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// gitLabTestsPerRunJobPages is the page budget for the PER-RUN jobs walk, and
// is deliberately NOT handler.MaxPages (CHAOS-4142, codex round 2).
//
// handler.MaxPages was doing two jobs at once: bounding the pipeline INVENTORY
// walk, where a small budget is a legitimate and useful setting, and bounding
// the per-run jobs walk, where a small budget is a permanent source stall --
// the page-budget branch fires, withholds the watermark, and does so identically
// on every future window. One knob with two correct ranges is the same
// one-thing-meaning-two-things shape that caused the original defect, so the
// two budgets are now separate.
//
// The value is DERIVED from the cap rather than chosen: the route's item cap
// tests `len(items) > githubTestsMaxJobsPerRun`, so it needs strictly more than
// the cap in hand, and cap/perPage+1 pages is the smallest budget that can
// deliver that. Deriving it means the item cap always binds first for as long
// as pages are full, without any configuration being able to say otherwise.
const gitLabTestsPerRunJobPages = githubTestsMaxJobsPerRun/nativePerPage + 1

type gitLabTestsChunkCursor struct {
	Phase      string `json:"phase"`
	Page       int    `json:"page,omitempty"`
	Index      int    `json:"index,omitempty"`
	Pipelines  int    `json:"pipelines"`
	Jobs       int    `json:"jobs"`
	Acceptance int    `json:"acceptance"`
	Suites     int    `json:"suites"`
	Cases      int    `json:"cases"`
	Coverage   int    `json:"coverage"`
	Requests   int    `json:"requests"`
	Pages      int    `json:"pages"`
	// Cumulative per-phase inventory-page counts. See CHAOS-3822 and the
	// matching fields on githubTestsChunkCursor: the paginator cap is local to
	// one invocation, so a continuation would otherwise renew the budget.
	PipelinePages int `json:"pipeline_pages"`
	ReportPages   int `json:"report_pages"`
	// Truncated names the inventory phases that stopped at their cumulative
	// page budget before the provider ran out of pages (CHAOS-4130). It is a
	// bounded, closed vocabulary -- at most one entry per phase -- and its
	// presence is what makes the unit finalize with lower-bound coverage
	// instead of being cancelled.
	Truncated []string `json:"truncated,omitempty"`
	// Repo and ProjectID let the terminal `done` resume publish completion
	// metadata without re-fetching the project object (CHAOS-3820).
	Repo      string `json:"repo,omitempty"`
	ProjectID int64  `json:"project_id,omitempty"`
}

const (
	gitLabPipelineInventoryComponent = "pipeline_inventory"
	gitLabReportInventoryComponent   = "report_inventory"
	// The per-run components are the GitLab twins of run_jobs/run_artifacts on
	// the github route: ONE pipeline held more items than its cap allows and
	// was committed with only the first cap-worth (CHAOS-4142).
	gitLabRunJobsComponent      = "run_jobs"
	gitLabRunArtifactsComponent = "run_artifacts"
	// GitLab's truncation evidence is a set of component names with no cause
	// field, so the page-budget variants are their own components rather than
	// a second cause. They mean the nested paginator ran out of page allowance
	// inside one pipeline, leaving an UNKNOWN remainder -- unlike the item cap,
	// which was positively observed (CHAOS-4142, codex round 1).
	gitLabRunJobsPageBudgetComponent      = "run_jobs_page_budget"
	gitLabRunArtifactsPageBudgetComponent = "run_artifacts_page_budget"
)

// gitLabTestsTruncationComponents is the CLOSED vocabulary of truncation
// components, and gitLabTestsWindowBlocking is the subset that leaves part of
// the requested window unwalked.
//
// GitLab lists pipelines order_by=updated_at&sort=desc, so an inventory phase
// that stops at its page budget covers the NEW end of the window and never
// reaches the old one; advancing over that remainder would make the gap
// permanent (CHAOS-2587). A per-run cap walks the whole window and truncates
// only inside an already-committed pipeline, so it has no unreached remainder
// and must NOT withhold the watermark -- withholding it there is what pinned
// since_at forever on the github side (CHAOS-4142).
var (
	gitLabTestsTruncationComponents = map[string]struct{}{
		gitLabPipelineInventoryComponent: {}, gitLabReportInventoryComponent: {},
		gitLabRunJobsComponent: {}, gitLabRunArtifactsComponent: {},
		gitLabRunJobsPageBudgetComponent: {}, gitLabRunArtifactsPageBudgetComponent: {},
	}
	// The advancing set is an ALLOWLIST, so anything added to the vocabulary
	// and forgotten here withholds rather than silently advancing over data
	// nobody looked at. Same single rule as the github route: a page budget
	// stop withholds the watermark; a positively observed item cap advances it.
	gitLabTestsWatermarkAdvancing = map[string]struct{}{
		gitLabRunJobsComponent: {}, gitLabRunArtifactsComponent: {},
	}
)

func gitLabTestsTruncationKnown(component string) bool {
	_, known := gitLabTestsTruncationComponents[component]
	return known
}

// gitLabTestsBlocksWatermark reports whether these components leave part of the
// requested window unwalked. coverage_complete is a SEPARATE claim and stays
// false for any truncation, blocking or not.
func gitLabTestsBlocksWatermark(components []string) bool {
	for _, component := range components {
		if _, advancing := gitLabTestsWatermarkAdvancing[component]; !advancing {
			return true
		}
	}
	return false
}

// recordGitLabTestsPerRunTruncation keeps a pipeline whose items exceeded a
// PER-RUN cap instead of failing the unit. See
// recordGitHubTestsPerRunTruncation for the full rationale; the two routes are
// kept symmetric deliberately, because a cap that finalizes on one provider
// and cancels on the other is worse than either choice consistently applied.
// gitLabTestsMetricLabels splits GitLab's single truncation component name
// into the (component, cause) pair the shared metric uses.
func gitLabTestsMetricLabels(component string) (string, string) {
	switch component {
	case gitLabRunJobsPageBudgetComponent:
		return "run_jobs", "per_run_page_budget"
	case gitLabRunArtifactsPageBudgetComponent:
		return "run_artifacts", "per_run_page_budget"
	default:
		return component, "per_run_cap"
	}
}

func recordGitLabTestsPerRunTruncation(
	cursor gitLabTestsChunkCursor,
	client *providerfoundation.HTTPClient,
	claim Claim,
	component string,
	runID string,
	kept int,
) gitLabTestsChunkCursor {
	present := false
	for _, existing := range cursor.Truncated {
		if existing == component {
			present = true
		}
	}
	if !present {
		cursor.Truncated = append(cursor.Truncated, component)
	}
	if client != nil {
		// GitLab encodes the reason in the component name; the metric keeps the
		// same (component, cause) shape as github so one dashboard serves both.
		metricComponent, metricCause := gitLabTestsMetricLabels(component)
		client.Metrics.RecordPerRunTruncation(
			claim.Provider, claim.Dataset, metricComponent, metricCause,
		)
	}
	slog.Warn(
		"provider per-run item cap reached; run committed with partial items",
		"provider", claim.Provider, "dataset", claim.Dataset, "unit", claim.ID,
		"repository", cursor.Repo, "component", component, "run", runID, "kept", kept,
	)
	return cursor
}

// recordGitLabTestsInventoryTruncation is the GitLab counterpart of
// recordGitHubTestsInventoryTruncation: a budget stop finalizes the unit with
// recorded lower-bound coverage instead of returning ErrPaginationCapExceeded,
// which providerunit maps to a deterministic-terminal category that cancels
// the unit and discards a checkpoint holding durable rows. GitLab lists
// pipelines order_by=updated_at&sort=desc, so a truncated walk -- exactly like
// GitHub's -- covers the NEW end of the window and must not advance the
// watermark over the old one (CHAOS-2587).
func recordGitLabTestsInventoryTruncation(
	cursor gitLabTestsChunkCursor,
	client *providerfoundation.HTTPClient,
	claim Claim,
	component string,
	pagesSpent int,
) gitLabTestsChunkCursor {
	present := false
	for _, existing := range cursor.Truncated {
		if existing == component {
			present = true
		}
	}
	if !present {
		cursor.Truncated = append(cursor.Truncated, component)
	}
	if client != nil {
		client.Metrics.RecordInventoryPageCap(claim.Provider, claim.Dataset)
	}
	slog.Warn(
		"provider inventory page budget exhausted",
		"provider", claim.Provider, "dataset", claim.Dataset, "unit", claim.ID,
		"repository", cursor.Repo, "component", component, "pages", pagesSpent,
	)
	return cursor
}

// emitGitLabCursorPair publishes the terminal metadata emission, whose before
// and after cursors are the same value because it advances no provider
// position.
func emitGitLabCursorPair(
	cursor gitLabTestsChunkCursor, batch CompleteRouteBatch, emit func(ChunkRouteEmission) error,
) error {
	raw, err := encodeGitLabTestsChunkCursor(cursor)
	if err != nil {
		return err
	}
	return emit(ChunkRouteEmission{Batch: batch, CursorBefore: raw, CursorAfter: raw, Final: true})
}

func decodeGitLabTestsChunkCursor(raw string) (gitLabTestsChunkCursor, error) {
	if strings.TrimSpace(raw) == "" {
		return gitLabTestsChunkCursor{Phase: "pipelines"}, nil
	}
	var cursor gitLabTestsChunkCursor
	if json.Unmarshal([]byte(raw), &cursor) != nil ||
		(cursor.Phase != "pipelines" && cursor.Phase != "reports" && cursor.Phase != "done") ||
		cursor.Page < 0 || cursor.Index < 0 ||
		cursor.PipelinePages < 0 || cursor.ReportPages < 0 ||
		len(cursor.Truncated) > len(gitLabTestsTruncationComponents) {
		return gitLabTestsChunkCursor{}, ErrChunkCheckpointConflict
	}
	seen := map[string]bool{}
	for _, component := range cursor.Truncated {
		if !gitLabTestsTruncationKnown(component) || seen[component] {
			return gitLabTestsChunkCursor{}, ErrChunkCheckpointConflict
		}
		seen[component] = true
	}
	return cursor, nil
}

func encodeGitLabTestsChunkCursor(cursor gitLabTestsChunkCursor) (string, error) {
	encoded, err := json.Marshal(cursor)
	if err != nil || len(encoded) > maxChunkCursorBytes {
		return "", ErrChunkCheckpointConflict
	}
	return string(encoded), nil
}

// CollectChunks streams GitLab TestOps pipeline and report pages. It keeps
// the same normalization functions as Collect while persisting the page and
// item index after each bounded emission.
func (handler GitLabTestsRouteHandler) CollectChunks(
	ctx context.Context,
	claim Claim,
	_ providerfoundation.Credential,
	client *providerfoundation.HTTPClient,
	normalizedAt time.Time,
	resumeCursor string,
	emit func(ChunkRouteEmission) error,
) error {
	if ctx == nil || claim.Validate() != nil || claim.Provider != "gitlab" ||
		(claim.Dataset != "cicd" && claim.Dataset != "tests") || client == nil ||
		client.Provider != "gitlab" || client.BaseURL == nil || normalizedAt.IsZero() || emit == nil {
		return ErrInvalidConfiguration
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
		return ErrInvalidConfiguration
	}
	projectID, err := gitLabProjectID(claim.SourceExternalID)
	if err != nil {
		return err
	}
	// Decode BEFORE any provider call: a terminal cursor must re-publish
	// completion metadata, never re-enter pagination (CHAOS-3820).
	cursor, err := decodeGitLabTestsChunkCursor(resumeCursor)
	if err != nil {
		return err
	}
	emitFinalMetadata := func(cursor gitLabTestsChunkCursor) error {
		cursor.Phase = "done"
		effects, effectErr := testOpsEffects(nil, nil, nil, nil, nil, nil)
		if effectErr != nil {
			return effectErr
		}
		return emitGitLabCursorPair(cursor, CompleteRouteBatch{
			Effects: effects,
			Result: map[string]any{
				"pipeline_runs_synced": cursor.Pipelines, "job_runs_synced": cursor.Jobs,
				"acceptance_checks_synced": cursor.Acceptance, "test_suites_synced": cursor.Suites,
				"test_cases_synced": cursor.Cases, "coverage_snapshots_synced": cursor.Coverage,
				"repo": cursor.Repo, "project_id": cursor.ProjectID,
				// NOT named inventory_complete: the chunk checkpoint already
				// owns that name for "the route finished scanning". This is
				// about COVERAGE -- whether what it scanned was the whole
				// window (CHAOS-4130).
				"coverage_complete": len(cursor.Truncated) == 0,
				"inventory_truncated": append(
					make([]string, 0, len(cursor.Truncated)), cursor.Truncated...),
				"actual_route_family": gitLabTestsActualRouteFamily(claim.Dataset),
				"observations": map[string]any{"provider_usage": []any{map[string]any{
					"transport": "rest", "route_family": gitLabTestsActualRouteFamily(claim.Dataset),
					"dimension": "rest_core", "request_count": cursor.Requests,
				}}},
			},
			// An INVENTORY-truncated walk covered only the newest part of
			// the window. Advancing the watermark over the unreached old end
			// would make the gap permanent, so the phase that stopped short
			// refuses it. A per-run truncation walked the whole window and
			// does not (CHAOS-4142).
			Watermark: func() *time.Time {
				if gitLabTestsBlocksWatermark(cursor.Truncated) {
					return nil
				}
				return claim.BeforeAt
			}(),
			Evidence: FetchEvidence{
				Provider: claim.Provider, Dataset: claim.Dataset,
				Requests: cursor.Requests, Pages: cursor.Pages,
				Records: cursor.Pipelines + cursor.Jobs + cursor.Acceptance +
					cursor.Suites + cursor.Cases + cursor.Coverage,
			},
		}, emit)
	}
	if cursor.Phase == "done" {
		return emitFinalMetadata(cursor)
	}
	requests := cursor.Requests
	counted := *client
	counted.Doer = gitLabTestsCountingDoer{delegate: client.Doer, attempts: &requests}
	root := providerRelativePath(client, "api", "v4", "projects", projectID)
	var project repositoryPayload
	if err := fetchObject(ctx, &counted, root, &project); err != nil {
		return err
	}
	cursor.Requests = requests
	parsedProjectID, err := project.ID.Int64()
	if err != nil || parsedProjectID < 1 || strconv.FormatInt(parsedProjectID, 10) != projectID {
		return providerfoundation.ErrNormalizationInvalid
	}
	fullName := gitLabProjectFullName(project)
	cursor.Repo, cursor.ProjectID = fullName, parsedProjectID
	repoID, err := repositoryIdentity(fullName)
	if err != nil {
		return err
	}
	required, provenance := projectGitLabTestsRequirement(project)
	emitCursor := func(before, after gitLabTestsChunkCursor, batch CompleteRouteBatch, final bool) error {
		before.Requests = requests
		after.Requests = requests
		beforeRaw, beforeErr := encodeGitLabTestsChunkCursor(before)
		if beforeErr != nil {
			return beforeErr
		}
		afterRaw, afterErr := encodeGitLabTestsChunkCursor(after)
		if afterErr != nil {
			return afterErr
		}
		return emit(ChunkRouteEmission{Batch: batch, CursorBefore: beforeRaw, CursorAfter: afterRaw, Final: final})
	}
	query := url.Values{"order_by": {"updated_at"}, "sort": {"desc"}}
	if claim.SinceAt != nil {
		query.Set("updated_after", claim.SinceAt.UTC().Format(time.RFC3339Nano))
	}
	if claim.BeforeAt != nil {
		query.Set("updated_before", claim.BeforeAt.UTC().Format(time.RFC3339Nano))
	}

	if cursor.Phase == "pipelines" {
		visit := func(page providerfoundation.PageVisit) error {
			cursor.Pages++
			pageNumber, _ := strconv.Atoi(page.CursorBefore)
			// First-entry-only counting against the CUMULATIVE budget. This is
			// the GitLab twin of the GitHub defect measured in CHAOS-4130: a
			// continuation re-requests the page it stopped inside, and
			// counting that re-visit shrank an N-page budget to N/visits-per-
			// page real pages, cancelling every busy project. cursor.Page ==
			// pageNumber is also true for a FRESH page entered at index 0, so
			// cursor.Index > 0 is what identifies the re-entry.
			if !(cursor.Page == pageNumber && cursor.Index > 0) {
				cursor.PipelinePages++
			}
			start := 0
			if cursor.Page == pageNumber {
				start = cursor.Index
			}
			if start > len(page.Items) {
				return ErrChunkCheckpointConflict
			}
			for index := start; index < len(page.Items); index++ {
				before := cursor
				payload, decodeErr := decodeGitLabTestsPipeline(page.Items[index])
				if decodeErr != nil {
					return decodeErr
				}
				jobs := []githubTestsJobRow(nil)
				acceptance := []githubTestsAcceptanceRow(nil)
				pipelines := []githubTestsPipelineRow(nil)
				startedAt := gitLabTestsPipelineStartedAt(payload)
				if startedAt != nil && !ciPipelineRunOutsideWindow(*startedAt, claim) {
					pipeline, ok := normalizeGitLabTestsPipeline(claim, repoID, payload, normalizedAt)
					if !ok {
						return providerfoundation.ErrNormalizationInvalid
					}
					pipeline.PRNumber, err = resolveGitLabTestsMergeRequest(ctx, &counted, root, payload)
					if err != nil {
						return err
					}
					jobPage, pageErr := providerfoundation.CollectGitLabPageParamPages(ctx, &counted, providerfoundation.GitLabPageOptions{
						Path:  root + "/pipelines/" + url.PathEscape(pipeline.RunID) + "/jobs",
						Query: url.Values{"include_retried": {"true"}}, PerPage: nativePerPage,
						MaxPages: gitLabTestsPerRunJobPages,
					})
					if pageErr != nil {
						return pageErr
					}
					cursor.Pages += jobPage.Pages
					// CollectGitLabPageParamPages passes no MaxItems, so its
					// CapReached can ONLY mean page-budget exhaustion. The item
					// cap here is therefore len-based, and the two are recorded
					// as different components because they classify oppositely.
					jobItems := jobPage.Items
					switch {
					case len(jobItems) > githubTestsMaxJobsPerRun:
						jobItems = jobItems[:githubTestsMaxJobsPerRun]
						cursor = recordGitLabTestsPerRunTruncation(
							cursor, client, claim, gitLabRunJobsComponent,
							pipeline.RunID, len(jobItems),
						)
					// UNREACHABLE with full pages, and KEPT anyway -- same
					// disposition as the github twin, reached structurally
					// rather than by validation: gitLabTestsPerRunJobPages is
					// derived from the cap, so no configuration can make this
					// budget bind first. Short pages could still land here, and
					// deleting the branch would silently change semantics if
					// that constant is ever lowered.
					case jobPage.PageBudgetExhausted:
						cursor = recordGitLabTestsPerRunTruncation(
							cursor, client, claim, gitLabRunJobsPageBudgetComponent,
							pipeline.RunID, len(jobItems),
						)
					}
					for _, rawJob := range jobItems {
						job, jobErr := decodeGitLabTestsJob(rawJob)
						if jobErr != nil {
							return jobErr
						}
						row, include := normalizeGitLabTestsJob(claim, repoID, pipeline.RunID, job, normalizedAt)
						if include {
							jobs = append(jobs, row)
						}
					}
					pipelines = append(pipelines, pipeline)
					acceptance = projectGitLabTestsChecks(claim, repoID, pipeline, payload, jobs, required, provenance, normalizedAt)
					cursor.Pipelines++
					cursor.Jobs += len(jobs)
					cursor.Acceptance += len(acceptance)
				}
				effects, effectErr := testOpsEffects(pipelines, jobs, acceptance, nil, nil, nil)
				if effectErr != nil {
					return effectErr
				}
				after := cursor
				after.Page, after.Index = pageNumber, index+1
				if after.Index >= len(page.Items) {
					nextPage, _ := strconv.Atoi(page.CursorAfter)
					after.Page, after.Index = nextPage, 0
					if nextPage == 0 {
						after.Phase = "reports"
					}
				}
				if err := emitCursor(before, after, CompleteRouteBatch{Effects: effects}, false); err != nil {
					return err
				}
				cursor = after
			}
			if len(page.Items) == 0 {
				nextPage, _ := strconv.Atoi(page.CursorAfter)
				cursor.Page, cursor.Index = nextPage, 0
				if nextPage == 0 {
					cursor.Phase = "reports"
				}
			}
			return nil
		}
		allowance, budgetErr := remainingPageBudget(maxPages, cursor.PipelinePages)
		switch {
		case errors.Is(budgetErr, ErrPaginationCapExceeded):
			cursor = recordGitLabTestsInventoryTruncation(
				cursor, client, claim, gitLabPipelineInventoryComponent, cursor.PipelinePages)
		case budgetErr != nil:
			return budgetErr
		default:
			collection, visitErr := providerfoundation.VisitGitLabPageParamPages(ctx, &counted, providerfoundation.GitLabPageOptions{
				Path: root + "/pipelines", Query: query, PerPage: nativePerPage, MaxPages: allowance, InitialPage: cursor.Page,
			}, visit)
			if visitErr != nil {
				return visitErr
			}
			if collection.PageBudgetExhausted {
				cursor = recordGitLabTestsInventoryTruncation(
					cursor, client, claim, gitLabPipelineInventoryComponent, cursor.PipelinePages)
			}
		}
	}
	if cursor.Phase != "reports" {
		cursor.Phase, cursor.Page, cursor.Index = "reports", 0, 0
	}

	reportQuery := url.Values{"order_by": {"updated_at"}, "sort": {"desc"}}
	if claim.SinceAt != nil {
		reportQuery.Set("updated_after", claim.SinceAt.UTC().Format(time.RFC3339Nano))
	}
	if claim.BeforeAt != nil {
		reportQuery.Set("updated_before", claim.BeforeAt.UTC().Format(time.RFC3339Nano))
	}
	reportVisit := func(page providerfoundation.PageVisit) error {
		cursor.Pages++
		pageNumber, _ := strconv.Atoi(page.CursorBefore)
		// First-entry-only counting, exactly as in the pipelines visit above.
		if !(cursor.Page == pageNumber && cursor.Index > 0) {
			cursor.ReportPages++
		}
		start := 0
		if cursor.Page == pageNumber {
			start = cursor.Index
		}
		if start > len(page.Items) {
			return ErrChunkCheckpointConflict
		}
		for index := start; index < len(page.Items); index++ {
			before := cursor
			selected, selectErr := selectGitLabTestsReportPipelines([]json.RawMessage{page.Items[index]}, project.DefaultBranch, maxPipelines, claim.BeforeAt)
			if selectErr != nil {
				return selectErr
			}
			suites := []testSuiteResultRow(nil)
			cases := []testCaseResultRow(nil)
			coverage := []coverageSnapshotRow(nil)
			if len(selected) == 1 {
				payload := selected[0]
				runID := stringValue(payload.ID)
				started := gitLabTestsPipelineStartedAt(payload)
				finished := parseGitLabTestsTime(payload.FinishedAt)
				report, present, reportErr := fetchGitLabTestsReport(ctx, &counted, root, runID)
				if reportErr != nil {
					return reportErr
				}
				if present {
					reportSuites, reportCases, normalizeErr := normalizeGitLabNativeTestReport(claim, repoID, runID, report, started, finished, normalizedAt)
					if normalizeErr != nil {
						return normalizeErr
					}
					suites, cases = reportSuites, reportCases
				}
				jobPage, jobErr := providerfoundation.CollectGitLabPageParamPages(ctx, &counted, providerfoundation.GitLabPageOptions{
					Path: root + "/pipelines/" + url.PathEscape(runID) + "/jobs", PerPage: nativePerPage, MaxPages: 1, SinglePage: true,
				})
				if jobErr != nil {
					if fatal := gitLabTestsOptionalError(ctx, jobErr); fatal != nil {
						return fatal
					}
				} else {
					cursor.Pages += jobPage.Pages
					// CURRENTLY UNREACHABLE, kept consistent on purpose. This
					// collection passes SinglePage:true, and
					// CollectGitLabPageParamPages sets CapReached only on a
					// SECOND loop iteration (pagination.go: the check sits at
					// the top of the loop), while SinglePage returns right
					// after the first page. Verified empirically: with
					// MaxPages:1+SinglePage:true a full page advertising
					// X-Next-Page yields CapReached=false, and the same options
					// without SinglePage yield true. So this branch never fired
					// and the pre-CHAOS-4142 ErrPaginationCapExceeded here was
					// never a live cancellation. It is converted anyway so the
					// site cannot become a landmine if SinglePage is ever
					// dropped -- but it carries no test, because a test that
					// cannot reach its site asserts nothing.
					if jobPage.PageBudgetExhausted {
						cursor = recordGitLabTestsPerRunTruncation(
							cursor, client, claim, gitLabRunArtifactsPageBudgetComponent,
							runID, len(jobPage.Items),
						)
					}
					artifactJobs, selectJobErr := selectGitLabTestsArtifactJobs(jobPage.Items, gitLabTestsMaxArtifacts)
					if selectJobErr != nil {
						return selectJobErr
					}
					for _, job := range artifactJobs {
						archive, downloadErr := downloadGitLabTestsArtifact(ctx, &counted, root, stringValue(job.ID))
						if downloadErr != nil {
							return downloadErr
						}
						if len(archive) == 0 {
							continue
						}
						rows, parseErr := parseGitHubTestsArtifact(archive, stringValue(job.ID), repoID, runID, claim.OrgID, started, finished, normalizedAt)
						if parseErr != nil || rows.Skipped != 0 {
							return fmt.Errorf("%w: reports skipped=%d: %v", ErrGitLabTestsIncomplete, rows.Skipped, parseErr)
						}
						coverage = append(coverage, rows.Coverage...)
					}
				}
			}
			cursor.Suites += len(suites)
			cursor.Cases += len(cases)
			cursor.Coverage += len(coverage)
			effects, effectErr := testOpsEffects(nil, nil, nil, suites, cases, coverage)
			if effectErr != nil {
				return effectErr
			}
			after := cursor
			after.Page, after.Index = pageNumber, index+1
			if after.Index >= len(page.Items) {
				nextPage, _ := strconv.Atoi(page.CursorAfter)
				after.Page, after.Index = nextPage, 0
				// Publish the terminal phase, exactly as the pipelines phase
				// publishes "reports". A cursor at {phase:reports, page:0} is
				// otherwise indistinguishable from a phase that has not
				// started, so a continuation landing on the last item of the
				// last page re-walked the WHOLE reports phase and spent fresh
				// budget on every lap (the GitLab twin of CHAOS-4130).
				if nextPage == 0 {
					after.Phase = "done"
				}
			}
			if err := emitCursor(before, after, CompleteRouteBatch{Effects: effects}, false); err != nil {
				return err
			}
			cursor = after
		}
		if len(page.Items) == 0 {
			nextPage, _ := strconv.Atoi(page.CursorAfter)
			cursor.Page, cursor.Index = nextPage, 0
			if nextPage == 0 {
				cursor.Phase = "done"
			}
		}
		return nil
	}
	reportAllowance, reportBudgetErr := remainingPageBudget(maxPages, cursor.ReportPages)
	switch {
	case errors.Is(reportBudgetErr, ErrPaginationCapExceeded):
		cursor = recordGitLabTestsInventoryTruncation(
			cursor, client, claim, gitLabReportInventoryComponent, cursor.ReportPages)
	case reportBudgetErr != nil:
		return reportBudgetErr
	default:
		collection, visitErr := providerfoundation.VisitGitLabPageParamPages(ctx, &counted, providerfoundation.GitLabPageOptions{
			Path: root + "/pipelines", Query: reportQuery, PerPage: nativePerPage, MaxPages: reportAllowance, InitialPage: cursor.Page,
		}, reportVisit)
		if visitErr != nil {
			return visitErr
		}
		if collection.PageBudgetExhausted {
			cursor = recordGitLabTestsInventoryTruncation(
				cursor, client, claim, gitLabReportInventoryComponent, cursor.ReportPages)
		}
	}
	cursor.Requests = requests
	return emitFinalMetadata(cursor)
}

var _ ChunkedCompleteRouteHandler = GitLabTestsRouteHandler{}
