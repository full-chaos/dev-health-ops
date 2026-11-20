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

	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
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
	// LastID is the id of the last pipeline the active phase emitted: the resume key of the id-ordered listing
	// (gitLabTestsSeek). Page and Index only say where to look for it. 0 = the phase has not emitted yet.
	LastID int64 `json:"li,omitempty"`
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
	// The size-cap skips: ONE test report over nativeMaxObjectBytes, or ONE
	// artifact over gitLabTestsMaxDownload, is skipped and counted instead of
	// failing the unit. The run keeps no rows for the skipped item (missing,
	// never zero); the unit's coverage_complete is false and names the component.
	gitLabReportTooLargeComponent    = "report_too_large"
	gitLabArtifactOversizedComponent = "artifact_oversized"
	// gitLabReportMemberDroppedComponent: report members of a job artifact the archive parse did not read because they
	// were over the compression-ratio guard or past the entry cap (CHAOS-14030). The bytes are the same on every walk, so it advances.
	gitLabReportMemberDroppedComponent = "report_member_dropped"
)

// gitLabTestsTruncationComponents is the CLOSED vocabulary of truncation
// components, and gitLabTestsWindowBlocking is the subset that leaves part of
// the requested window unwalked.
//
// The chunked route lists the window's pipelines order_by=id&sort=desc, so an
// inventory phase that stops at its page budget covers only part of the window
// and never reaches the rest; advancing over that remainder would make the gap
// permanent (CHAOS-2587). A per-run cap walks the whole window and truncates
// only inside an already-committed pipeline, so it has no unreached remainder
// and must NOT withhold the watermark -- withholding it there is what pinned
// since_at forever on the github side (CHAOS-4142).
var (
	gitLabTestsTruncationComponents = map[string]struct{}{
		gitLabPipelineInventoryComponent: {}, gitLabReportInventoryComponent: {},
		gitLabRunJobsComponent: {}, gitLabRunArtifactsComponent: {},
		gitLabRunJobsPageBudgetComponent: {}, gitLabRunArtifactsPageBudgetComponent: {},
		gitLabReportTooLargeComponent: {}, gitLabArtifactOversizedComponent: {}, gitLabReportMemberDroppedComponent: {},
	}
	// The advancing set is an ALLOWLIST, so anything added to the vocabulary
	// and forgotten here withholds rather than silently advancing over data
	// nobody looked at. Same single rule as the github route: a page budget
	// stop withholds the watermark; a positively observed item cap advances it.
	gitLabTestsWatermarkAdvancing = map[string]struct{}{
		gitLabRunJobsComponent: {}, gitLabRunArtifactsComponent: {},
		gitLabReportTooLargeComponent: {}, gitLabArtifactOversizedComponent: {}, gitLabReportMemberDroppedComponent: {},
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
		"repository", logging.ProviderText(cursor.Repo), "component", component, logging.ProviderIDAttr("run", runID), "kept", kept,
	)
	return cursor
}

// recordGitLabTestsSkippedItem keeps a run whose ONE test report or ONE artifact
// was over its size cap: the item is skipped, counted (the artifact-skip metric,
// reason report_too_large or artifact_oversized) and logged with the run id, and
// the walk goes on. The component joins the unit's truncated set, so
// coverage_complete is false; both components advance the watermark (the whole
// window was walked and the boundary was positively observed).
func recordGitLabTestsSkippedItem(
	cursor gitLabTestsChunkCursor,
	client *providerfoundation.HTTPClient,
	claim Claim,
	component string,
	runID string,
	capBytes int,
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
		client.Metrics.RecordArtifactSkipped(claim.Provider, claim.Dataset, component)
	}
	slog.Warn(
		"gitlab test item skipped: over the size cap",
		"provider", claim.Provider, "dataset", claim.Dataset, "unit", claim.ID,
		"repository", logging.ProviderText(cursor.Repo), "component", component, logging.ProviderIDAttr("run", runID), "cap_bytes", capBytes,
	)
	return cursor
}

// recordGitLabTestsInventoryTruncation is the GitLab counterpart of
// recordGitHubTestsInventoryTruncation: a budget stop finalizes the unit with
// recorded lower-bound coverage instead of returning ErrPaginationCapExceeded,
// which providerunit maps to a deterministic-terminal category that cancels
// the unit and discards a checkpoint holding durable rows. A truncated walk --
// exactly like GitHub's -- covers only part of the window and must not advance
// the watermark over the rest (CHAOS-2587).
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
		"repository", logging.ProviderText(cursor.Repo), "component", component, "pages", pagesSpent,
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
		cursor.Page < 0 || cursor.Index < 0 || cursor.LastID < 0 ||
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

// gitLabTestsSeek resumes one phase of the pipelines listing by pipeline id, not by position (CHAOS-14374). The project
// pipelines endpoint has offset pages only (no keyset, no id filter), and between two chunks a pipeline can leave the
// window listing (deleted, or updated past BeforeAt) and shift every later page forward. The listing is order_by=id&
// sort=desc, so ids are a strict order that no update changes, and the seek is exact:
//   - resumed (LastID > 0 at invocation start = seekID): start on the page that held it (Page, or Page-1 after a page
//     end). On the FIRST page of a visit, if the page is empty or its first id < seekID and it is not page 1, the
//     boundary may sit on an earlier page: step back one page and visit again, until a page's first id >= seekID or
//     page 1. Then skip every id >= seekID (only the resume-time id: inside one invocation every listed item is read).
//   - legacy (Page > 0 with no LastID: a cursor written by the updated_at-ordered binary): its position names another
//     order, so the phase starts again at page 1. Rows read twice replace the stored ones (every destination is a
//     ReplacingMergeTree on its natural key); nothing is skipped.
//
// A page at or before chargedThrough was charged to the phase's page budget by an earlier invocation, so a step back, a
// page-end re-read or a legacy restart up to the legacy Page is free; every later page is charged once on entry.
type gitLabTestsSeek struct {
	seekID         int64
	legacy         bool
	startPage      int
	chargedThrough int
	firstPage      bool
}

func newGitLabTestsSeek(cursor gitLabTestsChunkCursor) gitLabTestsSeek {
	switch {
	case cursor.LastID > 0:
		through := cursor.Page
		if cursor.Index == 0 {
			through = cursor.Page - 1
		}
		through = max(through, 1)
		return gitLabTestsSeek{seekID: cursor.LastID, startPage: through, chargedThrough: through}
	case cursor.Page > 0 || cursor.Index > 0:
		through := cursor.Page
		if cursor.Index == 0 {
			through = cursor.Page - 1
		}
		return gitLabTestsSeek{legacy: true, startPage: 1, chargedThrough: max(through, 0)}
	default:
		return gitLabTestsSeek{startPage: 1}
	}
}

// gitLabTestsStepBack ends a visit whose first page may not hold the resume boundary; the visit starts again at page.
type gitLabTestsStepBack struct{ page int }

func (step gitLabTestsStepBack) Error() string {
	return "gitlab tests listing moved under a resume; stepping back to page " + strconv.Itoa(step.page)
}

// enter checks one visited page against the seek: a step back (the boundary may be on an earlier page), or whether the
// page is charged to the budget.
func (seek *gitLabTestsSeek) enter(pageNumber int, items []json.RawMessage) (charge bool, err error) {
	first := seek.firstPage
	seek.firstPage = false
	if first && seek.seekID > 0 && pageNumber > 1 {
		if len(items) == 0 {
			return false, gitLabTestsStepBack{page: pageNumber - 1}
		}
		id, idErr := gitLabTestsListedPipelineID(items[0])
		if idErr != nil {
			return false, idErr
		}
		if id < seek.seekID {
			return false, gitLabTestsStepBack{page: pageNumber - 1}
		}
	}
	if pageNumber > seek.chargedThrough {
		seek.chargedThrough = pageNumber
		return true, nil
	}
	return false, nil
}

// gitLabTestsListedPipelineID is the id of one listed pipeline (GitLab ids are positive integers).
func gitLabTestsListedPipelineID(raw json.RawMessage) (int64, error) {
	var item struct {
		ID json.Number `json:"id"`
	}
	if err := json.Unmarshal(raw, &item); err != nil {
		return 0, providerfoundation.RouteFailure(gitlabPipelinesRoute, providerfoundation.ReasonBodyDecode, err)
	}
	id, err := item.ID.Int64()
	if err != nil || id < 1 {
		return 0, providerfoundation.RouteFailure(gitlabPipelinesRoute, providerfoundation.ReasonBodyDecode, providerfoundation.ErrNormalizationInvalid)
	}
	return id, nil
}

// visitGitLabTestsListing walks one phase of the id-ordered pipelines listing from the seek's start page, stepping back
// while the seek asks for it. MaxPages is the phase's remaining budget plus the free pages the visit re-reads.
func visitGitLabTestsListing(
	ctx context.Context, client *providerfoundation.HTTPClient, claim Claim, repo, phase, root string, query url.Values, seek *gitLabTestsSeek, allowance int,
	visit func(providerfoundation.PageVisit) error,
) (providerfoundation.PageCollection, error) {
	if seek.legacy {
		recordGitLabTestsReseek(client, claim, repo, phase, "legacy cursor without a pipeline id: the phase starts again", seek.chargedThrough)
	}
	for {
		seek.firstPage = true
		free := max(0, seek.chargedThrough-seek.startPage+1)
		collection, err := providerfoundation.VisitGitLabPageParamPages(ctx, client, providerfoundation.GitLabPageOptions{
			Path: root + "/pipelines", Route: gitlabPipelinesRoute, Query: query, PerPage: nativePerPage,
			MaxPages: allowance + free, InitialPage: seek.startPage,
		}, visit)
		var step gitLabTestsStepBack
		if !errors.As(err, &step) {
			return collection, err
		}
		recordGitLabTestsReseek(client, claim, repo, phase, "listing moved under a resume: stepping back one page", step.page)
		seek.startPage = step.page
	}
}

func recordGitLabTestsReseek(client *providerfoundation.HTTPClient, claim Claim, repo, phase, reason string, page int) {
	if client != nil {
		client.Metrics.RecordResumeReanchor(claim.Provider, claim.Dataset, phase)
	}
	slog.Warn(
		"provider page moved under a resume; seeking the listing by pipeline id",
		"provider", claim.Provider, "dataset", claim.Dataset, "unit", claim.ID,
		"repository", logging.ProviderText(repo), "phase", phase, "reason", reason, "page", page,
	)
}

// CollectChunks streams GitLab TestOps pipeline and report pages. It keeps
// the same normalization functions as Collect while persisting the page,
// item index and last pipeline id after each bounded emission.
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
				// The same key the sync coverage reader reads for GitHub tests units: a successful unit
				// that skipped something is a gap, not a covered range.
				"reports_complete": len(cursor.Truncated) == 0,
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
	if err := fetchObjectRoute(ctx, &counted, gitlabProjectRoute, root, &project); err != nil {
		return err
	}
	cursor.Requests = requests
	parsedProjectID, err := project.ID.Int64()
	if err != nil || parsedProjectID < 1 || strconv.FormatInt(parsedProjectID, 10) != projectID {
		return providerfoundation.ErrNormalizationInvalid
	}
	fullName := gitLabProjectFullName(project)
	cursor.Repo, cursor.ProjectID = fullName, parsedProjectID
	repoID, err := resolveGitLabRepoID(ctx, handler.RepoIdentity, claim, parsedProjectID, fullName)
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
	query := url.Values{"order_by": {"id"}, "sort": {"desc"}}
	if claim.SinceAt != nil {
		query.Set("updated_after", claim.SinceAt.UTC().Format(time.RFC3339Nano))
	}
	if claim.BeforeAt != nil {
		query.Set("updated_before", claim.BeforeAt.UTC().Format(time.RFC3339Nano))
	}

	if cursor.Phase == "pipelines" {
		seek := newGitLabTestsSeek(cursor)
		visit := func(page providerfoundation.PageVisit) error {
			cursor.Pages++
			pageNumber, _ := strconv.Atoi(page.CursorBefore)
			// First-entry-only counting against the CUMULATIVE budget (the GitLab
			// twin of CHAOS-4130): a page an earlier invocation already charged is
			// free when it is visited again (gitLabTestsSeek).
			charge, seekErr := seek.enter(pageNumber, page.Items)
			if seekErr != nil {
				return seekErr
			}
			if charge {
				cursor.PipelinePages++
			}
			for index := 0; index < len(page.Items); index++ {
				id, idErr := gitLabTestsListedPipelineID(page.Items[index])
				if idErr != nil {
					return idErr
				}
				if seek.seekID > 0 && id >= seek.seekID {
					continue
				}
				before := cursor
				payload, decodeErr := decodeGitLabTestsPipeline(page.Items[index])
				if decodeErr != nil {
					return providerfoundation.RouteFailure(gitlabPipelinesRoute, providerfoundation.ReasonBodyDecode, decodeErr)
				}
				jobs := []githubTestsJobRow(nil)
				acceptance := []githubTestsAcceptanceRow(nil)
				pipelines := []githubTestsPipelineRow(nil)
				startedAt := gitLabTestsPipelineStartedAt(payload)
				if startedAt != nil && !gitLabPipelineOutsideWindow(*startedAt, claim) {
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
						Route: gitlabPipelineJobsRoute,
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
							return providerfoundation.RouteFailure(gitlabPipelineJobsRoute, providerfoundation.ReasonBodyDecode, jobErr)
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
				after.Page, after.Index, after.LastID = pageNumber, index+1, id
				if after.Index >= len(page.Items) {
					nextPage, _ := strconv.Atoi(page.CursorAfter)
					after.Page, after.Index = nextPage, 0
					if nextPage == 0 {
						after.Phase, after.LastID = "reports", 0
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
					cursor.Phase, cursor.LastID = "reports", 0
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
			collection, visitErr := visitGitLabTestsListing(ctx, &counted, claim, cursor.Repo, "pipelines", root, query, &seek, allowance, visit)
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
		cursor.Phase, cursor.Page, cursor.Index, cursor.LastID = "reports", 0, 0, 0
	}

	reportQuery := url.Values{"order_by": {"id"}, "sort": {"desc"}}
	if claim.SinceAt != nil {
		reportQuery.Set("updated_after", claim.SinceAt.UTC().Format(time.RFC3339Nano))
	}
	if claim.BeforeAt != nil {
		reportQuery.Set("updated_before", claim.BeforeAt.UTC().Format(time.RFC3339Nano))
	}
	reportSeek := newGitLabTestsSeek(cursor)
	reportVisit := func(page providerfoundation.PageVisit) error {
		cursor.Pages++
		pageNumber, _ := strconv.Atoi(page.CursorBefore)
		// First-entry-only counting and the id seek, exactly as in the pipelines visit above.
		charge, seekErr := reportSeek.enter(pageNumber, page.Items)
		if seekErr != nil {
			return seekErr
		}
		if charge {
			cursor.ReportPages++
		}
		for index := 0; index < len(page.Items); index++ {
			id, idErr := gitLabTestsListedPipelineID(page.Items[index])
			if idErr != nil {
				return idErr
			}
			if reportSeek.seekID > 0 && id >= reportSeek.seekID {
				continue
			}
			before := cursor
			selected, selectErr := selectGitLabTestsReportPipelines([]json.RawMessage{page.Items[index]}, maxPipelines, claim.BeforeAt)
			if selectErr != nil {
				return selectErr
			}
			suites := []testSuiteResultRow(nil)
			cases := []testCaseResultRow(nil)
			coverage := []coverageSnapshotRow(nil)
			var completeness []testOpsRunReportCompletenessRow
			if len(selected) == 1 {
				payload := selected[0]
				runReport := newGitLabTestsRunReport()
				runID := stringValue(payload.ID)
				started := gitLabTestsPipelineStartedAt(payload)
				finished := parseGitLabTestsTime(payload.FinishedAt)
				jobPage, jobErr := providerfoundation.CollectGitLabPageParamPages(ctx, &counted, providerfoundation.GitLabPageOptions{
					Path:  root + "/pipelines/" + url.PathEscape(runID) + "/jobs",
					Route: gitlabPipelineJobsRoute,
					Query: url.Values{"include_retried": {"true"}}, PerPage: nativePerPage,
					MaxPages: gitLabTestsPerRunJobPages,
				})
				runJobs, jobItems := gitLabTestsRunJobs{}, []json.RawMessage(nil)
				attempt := int64(1)
				if jobErr != nil {
					if fatal := gitLabTestsOptionalError(ctx, jobErr); fatal != nil {
						return fatal
					}
					// The job list answered 404: the pipeline's artifacts are unknown, a known loss (CHAOS-13981).
					runReport.lose(githubTestsArtifactUnavailableCause)
				} else {
					cursor.Pages += jobPage.Pages
					jobItems = jobPage.Items
					// The attempt counts the retries of the whole listing: a retry past the per-run cap is still a new key.
					var attemptErr error
					if attempt, attemptErr = gitLabTestsRunAttemptOf(jobItems); attemptErr != nil {
						return attemptErr
					}
					switch {
					case len(jobItems) > githubTestsMaxJobsPerRun:
						jobItems = jobItems[:githubTestsMaxJobsPerRun]
						cursor = recordGitLabTestsPerRunTruncation(
							cursor, client, claim, gitLabRunArtifactsComponent, runID, len(jobItems))
						runReport.lose(gitLabTestsRunReportComponentReasons[gitLabRunArtifactsComponent])
					case jobPage.PageBudgetExhausted:
						cursor = recordGitLabTestsPerRunTruncation(
							cursor, client, claim, gitLabRunArtifactsPageBudgetComponent, runID, len(jobItems))
						runReport.lose(gitLabTestsRunReportComponentReasons[gitLabRunArtifactsPageBudgetComponent])
					}
					var rankErr error
					if runJobs, rankErr = newGitLabTestsRunJobs(jobItems); rankErr != nil {
						return rankErr
					}
				}
				var summary map[string][]string
				if runJobs.anyRetried {
					var summaryErr error
					summary, summaryErr = fetchGitLabTestsReportSummary(ctx, &counted, root, runID)
					if summaryErr != nil {
						if fatal := gitLabTestsSummaryFallback(ctx, summaryErr); fatal != nil {
							return fatal
						}
						summary = nil
					}
				}
				report, present, reportErr := fetchGitLabTestsReport(ctx, &counted, root, runID)
				if errors.Is(reportErr, errGitLabTestsReportTooLarge) {
					cursor = recordGitLabTestsSkippedItem(
						cursor, client, claim, gitLabReportTooLargeComponent, runID, nativeMaxObjectBytes)
					report, present, reportErr = gitLabTestsReportPayload{}, false, nil
					runReport.expected++
					runReport.lose(gitLabTestsRunReportComponentReasons[gitLabReportTooLargeComponent])
				}
				if reportErr != nil {
					return reportErr
				}
				if present {
					reportSuites, reportCases, nonFinite, normalizeErr := normalizeGitLabNativeTestReportAttempts(claim, repoID, runID, report, started, finished, normalizedAt,
						func(suiteName string) int64 { return runJobs.suiteAttempt(suiteName, summary) })
					if normalizeErr != nil {
						return normalizeErr
					}
					if nonFinite > 0 {
						client.Metrics.RecordNonFiniteTestTime(claim.Provider, claim.Dataset, nonFinite)
						slog.Info(
							"non-finite test time stored as NULL",
							"provider", claim.Provider, "dataset", claim.Dataset, "unit", claim.ID,
							"repository", logging.ProviderText(cursor.Repo), logging.ProviderIDAttr("run", runID), "count", nonFinite,
						)
					}
					if duplicates := countDuplicateTestCases(reportCases); duplicates > 0 {
						client.Metrics.RecordDuplicateTestCase(claim.Provider, claim.Dataset, duplicates)
						slog.Info(
							"within-suite duplicate test-case names disambiguated with an ordinal suffix",
							"provider", claim.Provider, "dataset", claim.Dataset, "unit", claim.ID,
							"repository", logging.ProviderText(cursor.Repo), logging.ProviderIDAttr("run", runID), "count", duplicates,
						)
					}
					if duplicates := countDuplicateTestSuites(reportSuites); duplicates > 0 {
						client.Metrics.RecordDuplicateTestSuite(claim.Provider, claim.Dataset, duplicates)
						slog.Info(
							"sibling suite collision resolved: same-named suite objects disambiguated with an ordinal suffix",
							"provider", claim.Provider, "dataset", claim.Dataset, "unit", claim.ID,
							"repository", logging.ProviderText(cursor.Repo), logging.ProviderIDAttr("run", runID), "count", duplicates,
						)
					}
					suites, cases = reportSuites, reportCases
					runReport.expected++
					runReport.admitted++
				}
				if jobErr == nil {
					artifactJobs, selectJobErr := selectGitLabTestsArtifactJobs(jobItems, gitLabTestsMaxArtifacts)
					if selectJobErr != nil {
						return selectJobErr
					}
					qualifying, qualifyErr := gitLabTestsQualifyingArtifactJobs(jobItems)
					if qualifyErr != nil {
						return qualifyErr
					}
					if qualifying > gitLabTestsMaxArtifacts {
						reportGitLabTestsArtifactJobsOverCap(ctx, claim.Provider, claim.Dataset, claim.ID, cursor.Repo, runID, qualifying, gitLabTestsMaxArtifacts)
						// The selection reads the first gitLabTestsMaxArtifacts and none of the others; the unit result does not
						// record this cut (ticket 14144), the pipeline's verdict does.
						runReport.lose(githubTestsPerRunCapCause)
					}
					for _, job := range artifactJobs {
						archive, found, downloadErr := downloadGitLabTestsArtifactFound(ctx, &counted, root, stringValue(job.ID))
						if errors.Is(downloadErr, errGitLabTestsArtifactTooLarge) {
							cursor = recordGitLabTestsSkippedItem(
								cursor, client, claim, gitLabArtifactOversizedComponent, runID, gitLabTestsMaxDownload)
							runReport.expected++
							runReport.lose(gitLabTestsRunReportComponentReasons[gitLabArtifactOversizedComponent])
							continue
						}
						if downloadErr != nil {
							return downloadErr
						}
						if !found {
							runReport.expected++
							runReport.lose(githubTestsArtifactNotFoundReason)
							continue
						}
						if len(archive) == 0 {
							continue
						}
						rows, parseErr := parseGitLabTestsJobArtifact(archive, job, runJobs, repoID, runID, claim.OrgID, started, finished, normalizedAt)
						if parseErr != nil || rows.Skipped != 0 {
							return fmt.Errorf("%w: reports skipped=%d: %v", ErrGitLabTestsIncomplete, rows.Skipped, parseErr)
						}
						if rows.GuardDropped > 0 {
							cursor = recordGitLabTestsSkippedItem(cursor, client, claim, gitLabReportMemberDroppedComponent, runID, 0)
							for _, cause := range rows.GuardCauses {
								runReport.lose(cause)
							}
						}
						if rows.ReportsParsed > 0 || rows.GuardDropped > 0 {
							runReport.expected++
						}
						// A source is admitted only when it was read whole: a guard-dropped member leaves it out.
						if rows.ReportsRead > 0 && rows.GuardDropped == 0 {
							runReport.admitted++
						}
						coverage = append(coverage, rows.Coverage...)
					}
				}
				// One verdict row per pipeline, in the emission that carries its report rows (CHAOS-13981).
				completeness = gitLabTestsRunReportCompleteness(claim, repoID, payload, attempt, runReport, normalizedAt)
			}
			cursor.Suites += len(suites)
			cursor.Cases += len(cases)
			cursor.Coverage += len(coverage)
			effects, effectErr := testOpsEffects(nil, nil, nil, suites, cases, coverage, completeness...)
			if effectErr != nil {
				return effectErr
			}
			after := cursor
			after.Page, after.Index, after.LastID = pageNumber, index+1, id
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
					after.Phase, after.LastID = "done", 0
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
				cursor.Phase, cursor.LastID = "done", 0
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
		collection, visitErr := visitGitLabTestsListing(ctx, &counted, claim, cursor.Repo, "reports", root, reportQuery, &reportSeek, reportAllowance, reportVisit)
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
