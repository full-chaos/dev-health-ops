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

// gitLabTestsCursorNonRepoMaxBytes is the widest GitLab cursor without Repo: every counter at its type's maximum, the
// whole truncation vocabulary, LastID, ProjectID and WindowStart at MaxInt64 encode in 666 bytes (a cell pins it).
const gitLabTestsCursorNonRepoMaxBytes = 666

// gitLabTestsCursorRepoMaxBytes is the longest repository path the done cursor keeps: an eighth of the checkpoint budget
// (maxChunkCursorBytes), so the widest done cursor is 1183 bytes (666 + 512 + the "repo" key) and keeps a margin of 2913 bytes of the
// budget whatever the path length (GitLab allows deep groups). A longer path is dropped and the done resume reads the
// project again.
const gitLabTestsCursorRepoMaxBytes = maxChunkCursorBytes / 8

// gitLabTestsWindowStartMargin is how far before GitLab's clock at the walk start the unit's coverage claim stops
// (gitLabTestsWatermark). It covers the clock skew between the Date header (one-second resolution, possibly written by a
// proxy in front of GitLab) and the clock that stamps updated_at, and an updated_at stamped before its row commits.
const gitLabTestsWindowStartMargin = 60 * time.Second

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
	// metadata without re-fetching the project object (CHAOS-3820). Repo is
	// persisted only in the done cursor and only up to gitLabTestsCursorRepoMaxBytes
	// (gitLabTestsPersistedCursor); a cursor an older binary wrote may carry any path.
	Repo      string `json:"repo,omitempty"`
	ProjectID int64  `json:"project_id,omitempty"`
	// LastID is the id of the last pipeline the active phase emitted: the resume key of the id-ordered listing
	// (gitLabTestsSeek). Page and Index only say where to look for it. 0 = the phase has not emitted yet.
	LastID int64 `json:"li,omitempty"`
	// WindowStart is the unix second up to which the walk can claim coverage: GitLab's clock when the walk started (the
	// Date of the project read, which comes before any listing) minus gitLabTestsWindowStartMargin. It is set once, by the
	// invocation that starts the walk, and never changes. 0 = a cursor an older binary wrote (gitLabTestsWatermark).
	WindowStart int64 `json:"ws,omitempty"`
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

// gitLabTestsWatermark is the coverage claim of a finished walk. The listing is read once, from the walk start on, so a
// pipeline GitLab stamps after that start (a new one, or one updated again, with an id the walk has passed) may be
// unread by the walk even when its updated_at is at or before BeforeAt: GitLab's clock behind ours, or an explicit future
// BeforeAt (a backfill). The claim therefore stops at WindowStart when that is earlier than BeforeAt, and the next window
// (updated_after = the stored watermark, inclusive) lists every such pipeline. Contract: a pipeline in the window is
// never skipped by both windows; when a pipeline enters above the page boundary inside one invocation, a pipeline can be
// read twice by that walk (gitLabTestsSeek), and the next window reads again what this walk read after WindowStart
// (idempotent: every destination is a ReplacingMergeTree on its natural key).
//
// A window-blocking truncation withholds the claim (CHAOS-2587), as does a cursor without WindowStart (an older binary
// started the walk, so its start is unknown): the next window walks the range again rather than skip a pipeline.
//
// The walk start is the Date of the project read only (GitLab sends Date on every response: RFC 9110, an origin server
// with a clock sends it). NOT closed: when the project read carries no usable Date (none, unparseable, or less than the
// margin after the epoch), WindowStart comes from our clock (normalizedAt), so a GitLab clock behind ours by more than the
// margin still loses what it stamps between its walk-start time and ours; a Date written by a proxy whose clock is behind
// GitLab's by more than the margin is the same case.
func gitLabTestsWatermark(cursor gitLabTestsChunkCursor, claim Claim) *time.Time {
	if gitLabTestsBlocksWatermark(cursor.Truncated) || claim.BeforeAt == nil {
		return nil
	}
	if cursor.WindowStart == 0 {
		slog.Warn("gitlab tests/cicd window start unknown",
			"provider", claim.Provider, "dataset", claim.Dataset, "unit", claim.ID,
			"reason", "cursor without a window start: watermark withheld")
		return nil
	}
	watermark := claim.BeforeAt.UTC()
	if start := time.Unix(cursor.WindowStart, 0).UTC(); start.Before(watermark) {
		watermark = start
	}
	return &watermark
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
		cursor.Page < 0 || cursor.Index < 0 || cursor.LastID < 0 || cursor.WindowStart < 0 ||
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

// gitLabTestsPersistedCursor is the cursor as it is written: a phase that reads the project again on resume never
// persists Repo, and the done cursor persists it only when it is short.
func gitLabTestsPersistedCursor(cursor gitLabTestsChunkCursor) gitLabTestsChunkCursor {
	if cursor.Phase != "done" || len(cursor.Repo) > gitLabTestsCursorRepoMaxBytes {
		cursor.Repo = ""
	}
	return cursor
}

func encodeGitLabTestsChunkCursor(cursor gitLabTestsChunkCursor) (string, error) {
	encoded, err := json.Marshal(gitLabTestsPersistedCursor(cursor))
	if err != nil || len(encoded) > maxChunkCursorBytes {
		return "", ErrChunkCheckpointConflict
	}
	return string(encoded), nil
}

// gitLabTestsMaxListingStepBacks bounds the step backs of one phase in one invocation (gitLabTestsSeek). A listing that keeps
// moving past it ends the phase with the phase's inventory component: the watermark is withheld and coverage_complete is false.
const gitLabTestsMaxListingStepBacks = 30

// gitLabTestsSeek walks one phase of the pipelines listing so that no listed pipeline is skipped when the listing moves between
// two requests (CHAOS-14374 across invocations, CHAOS-14438 inside one). The project pipelines endpoint has offset pages only
// (page p, per_page m = offsets [(p-1)*m, p*m); no keyset, no id filter), and a pipeline can leave the window listing (deleted,
// or updated past BeforeAt) or enter it between two requests. The listing is order_by=id&sort=desc, so ids are a strict order
// that no update changes, and one response is one snapshot of it.
//
// Every request after the first holds the last listed offset (end-1) and the items past it: per_page varies per request
// (gitLabTestsListingRequest), because a fixed per_page never puts offsets end-1 and end in one response. A response whose first
// id is at or above the anchor (the id of the last emitted pipeline) proves that no unread pipeline sits before it, since every
// id below the anchor comes after it. A response that does not (empty, or its first id below the anchor, and not offset 0) means
// the listing shrank above the boundary: it is discarded and the walk steps back to the 100-item page that holds the offset
// before it, until a response passes or offset 0 (the list head). In a passing response a pipeline is read when its id is below
// the anchor (not read yet: the listing moved it up) or when its offset is past every offset the visit listed before (high). The
// second rule reads what enters the listing above the boundary inside one invocation; it can read a pipeline twice (an entering
// pipeline shifts the last read one past high), and the rows replace the stored ones (every destination is a
// ReplacingMergeTree on its natural key). A pipeline that enters above the boundary at an offset the walk passed is not read by
// this walk; its updated_at is at or after the walk start, so the next window reads it (gitLabTestsWatermark).
//   - resumed (LastID > 0 at invocation start = seekID): end = the cursor's offset (Page, Index on the 100-item grid), the
//     anchor = seekID, and ids at or above seekID are never read by the visit (only the resume-time id).
//   - legacy (Page > 0 with no LastID: a cursor written by the updated_at-ordered binary): its position names another order, so
//     the phase starts again at offset 0 and reads every listed pipeline.
//
// Pages are charged on the 100-item grid, once each, by the grid page of the last offset a response holds: a stable walk of N
// pipelines charges ceil(N/100) pages whatever per_page the requests use, and an empty response charges none. A grid page at or before the phase's charged count is
// free (an overlap, a step back, a resume re-read), even when the whole budget is spent; a phase with no position yet has no page
// to re-read, so a spent budget stops it before any request. No request passes the last chargeable grid page; at the budget
// one request of the charged grid page that holds end-1 (at most one per phase per invocation) finds a
// shrink or the end, so the watermark stays true at the cap. Every request goes through the client's backoff gate and is
// counted by its doers, like any other.
type gitLabTestsSeek struct {
	seekID         int64
	anchor         int64
	legacy         bool
	end            int
	high           int
	startPage      int
	chargedThrough int
	stepBacks      int
}

func newGitLabTestsSeek(cursor gitLabTestsChunkCursor, charged int) gitLabTestsSeek {
	switch {
	case cursor.LastID > 0:
		end := max(0, (cursor.Page-1)*nativePerPage+cursor.Index)
		return gitLabTestsSeek{
			seekID: cursor.LastID, anchor: cursor.LastID, end: end, high: end,
			startPage: max(end-1, 0)/nativePerPage + 1, chargedThrough: charged,
		}
	case cursor.Page > 0 || cursor.Index > 0:
		return gitLabTestsSeek{legacy: true, startPage: 1, chargedThrough: charged}
	default:
		return gitLabTestsSeek{startPage: 1}
	}
}

// free is the number of already-charged pages a visit from startPage re-reads before it needs budget.
func (seek *gitLabTestsSeek) free() int {
	return max(0, seek.chargedThrough-seek.startPage+1)
}

// gitLabTestsPhaseAllowance is the new-page budget of a phase. A spent budget stops the phase only when the seek has no
// charged page to re-read; otherwise the visit reads the charged pages and stops at the first uncharged one.
func gitLabTestsPhaseAllowance(maxPages, charged int, seek *gitLabTestsSeek) (int, error) {
	allowance, err := remainingPageBudget(maxPages, charged)
	if errors.Is(err, ErrPaginationCapExceeded) && seek.free() > 0 {
		return 0, nil
	}
	return allowance, err
}

// gitLabTestsListingRequest picks the next request of a walk whose last response ended at offset end: the page and per_page
// (1..nativePerPage) whose offsets [start, start+perPage) hold end-1 and the most offsets past end, within limit. ok is false
// when no request within limit reads an offset past end.
func gitLabTestsListingRequest(end, limit int) (page, perPage, start int, ok bool) {
	if end <= 0 {
		perPage = min(nativePerPage, limit)
		return 1, perPage, 0, perPage > 0
	}
	gain := 0
	for size := nativePerPage; size >= 1; size-- {
		first := (end - 1) / size * size
		if first+size > limit || first+size-end <= gain {
			continue
		}
		gain, page, perPage, start = first+size-end, first/size+1, size, first
	}
	return page, perPage, start, gain > 0
}

// gitLabTestsListingPosition is the cursor position of offset end on the 100-item grid (Page, Index), the form every binary
// reads: a page end is (next page, 0).
func gitLabTestsListingPosition(end int) (page, index int) {
	return end/nativePerPage + 1, end % nativePerPage
}

// gitLabTestsListingPage is one response of the walk. Discarded: the response failed the anchor check and is not read. Read
// says which items the phase reads (gitLabTestsSeek); Charged is the number of grid pages it newly charges; Last: the listing
// ended with it (GitLab's X-Next-Page, or an empty response).
type gitLabTestsListingPage struct {
	Items     []json.RawMessage
	IDs       []int64
	Start     int
	Read      []bool
	Charged   int
	Last      bool
	Discarded bool
}

// gitLabTestsListingResult says why a walk stopped before the end of the listing: the page budget, or the step-back bound.
type gitLabTestsListingResult struct {
	PageBudgetExhausted bool
	StepBacksExhausted  bool
}

// gitLabTestsListedPipelineID is the id of one listed pipeline: the exact "id" key, a positive JSON integer (a quoted id
// or a case variant of the key is refused; encoding/json would match keys case-insensitively).
func gitLabTestsListedPipelineID(raw json.RawMessage) (int64, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return 0, providerfoundation.RouteFailure(gitlabPipelinesRoute, providerfoundation.ReasonBodyDecode, err)
	}
	value, present := fields["id"]
	var id int64
	if !present || json.Unmarshal(value, &id) != nil || id < 1 {
		return 0, providerfoundation.RouteFailure(gitlabPipelinesRoute, providerfoundation.ReasonBodyDecode, providerfoundation.ErrNormalizationInvalid)
	}
	return id, nil
}

// visitGitLabTestsListing walks one phase of the id-ordered pipelines listing (gitLabTestsSeek), one request at a time. The
// phase may charge allowance grid pages past the ones it already charged.
func visitGitLabTestsListing(
	ctx context.Context, client *providerfoundation.HTTPClient, claim Claim, repo, phase, root string, query url.Values, seek *gitLabTestsSeek, allowance int,
	visit func(gitLabTestsListingPage) error,
) (gitLabTestsListingResult, error) {
	if seek.legacy {
		recordGitLabTestsReseek(client, claim, repo, phase, "legacy cursor without a pipeline id: the phase starts again", seek.chargedThrough)
	}
	limit := (seek.chargedThrough + allowance) * nativePerPage
	stepBack, budgetChecked := 0, false
	for {
		page, perPage, start, ok := stepBack, nativePerPage, (stepBack-1)*nativePerPage, true
		if stepBack == 0 {
			page, perPage, start, ok = gitLabTestsListingRequest(seek.end, limit)
		}
		// No request past end fits the budget: one free request of the grid page that holds end-1 still finds whether the
		// listing shrank (step back) or ends there; only a listing that goes on past it stops at the budget. One such request
		// per visit.
		atBudget := !ok
		if atBudget {
			if seek.end <= 0 || budgetChecked {
				return gitLabTestsListingResult{PageBudgetExhausted: true}, nil
			}
			budgetChecked = true
			page = (seek.end-1)/nativePerPage + 1
			perPage, start = nativePerPage, (page-1)*nativePerPage
		}
		var response providerfoundation.PageVisit
		if _, err := providerfoundation.VisitGitLabPageParamPages(ctx, client, providerfoundation.GitLabPageOptions{
			Path: root + "/pipelines", Route: gitlabPipelinesRoute, Query: query, PerPage: perPage, MaxPages: 1, InitialPage: page,
		}, func(visited providerfoundation.PageVisit) error {
			response = visited
			return nil
		}); err != nil {
			return gitLabTestsListingResult{}, err
		}
		ids := make([]int64, len(response.Items))
		for index, raw := range response.Items {
			id, err := gitLabTestsListedPipelineID(raw)
			if err != nil {
				return gitLabTestsListingResult{}, err
			}
			ids[index] = id
		}
		last := len(ids) == 0 || response.CursorAfter == "0"
		moved := seek.anchor > 0 && start > 0 && (len(ids) == 0 || ids[0] < seek.anchor)
		// A forward response that ends at or before the last one (a short page GitLab says is not the last) makes no progress:
		// it counts against the same bound, so no listing can hold the walk in a loop.
		stalled := !moved && !last && stepBack == 0 && !atBudget && start+len(ids) <= seek.end
		if moved || stalled {
			if moved {
				if err := visit(gitLabTestsListingPage{Discarded: true}); err != nil {
					return gitLabTestsListingResult{}, err
				}
			}
			seek.stepBacks++
			if seek.stepBacks > gitLabTestsMaxListingStepBacks {
				return gitLabTestsListingResult{StepBacksExhausted: true}, nil
			}
			if moved {
				stepBack = (start-1)/nativePerPage + 1
				recordGitLabTestsReseek(client, claim, repo, phase, "listing moved between two requests: stepping back one page", stepBack)
				continue
			}
		}
		// An empty response holds no offset, so it charges no page; it is still a request (counted by the client's doers).
		reached := seek.chargedThrough
		if len(ids) > 0 {
			reached = (start+len(ids)-1)/nativePerPage + 1
		}
		listed := gitLabTestsListingPage{
			Items: response.Items, IDs: ids, Start: start, Read: make([]bool, len(ids)),
			Charged: max(0, reached-seek.chargedThrough), Last: last,
		}
		seek.chargedThrough = max(seek.chargedThrough, reached)
		lastRead := -1
		for index, id := range ids {
			below := seek.anchor > 0 && id < seek.anchor
			past := start+index >= seek.high && (seek.seekID == 0 || id < seek.seekID)
			listed.Read[index] = below || past
			if listed.Read[index] {
				lastRead = index
			}
		}
		if err := visit(listed); err != nil {
			return gitLabTestsListingResult{}, err
		}
		if lastRead >= 0 {
			seek.anchor = ids[lastRead]
		}
		seek.end = start + len(ids)
		seek.high = max(seek.high, seek.end)
		stepBack = 0
		if last {
			return gitLabTestsListingResult{}, nil
		}
		if atBudget {
			return gitLabTestsListingResult{PageBudgetExhausted: true}, nil
		}
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

// recordGitLabTestsListingMoved ends a phase whose listing kept moving under the walk (gitLabTestsMaxListingStepBacks step
// backs in one invocation). It records the phase's inventory component, which withholds the watermark and makes
// coverage_complete false, so the next window walks the range again.
func recordGitLabTestsListingMoved(cursor gitLabTestsChunkCursor, claim Claim, component string) gitLabTestsChunkCursor {
	present := false
	for _, existing := range cursor.Truncated {
		if existing == component {
			present = true
		}
	}
	if !present {
		cursor.Truncated = append(cursor.Truncated, component)
	}
	slog.Warn(
		"gitlab tests listing kept moving between requests; phase ended without coverage",
		"provider", claim.Provider, "dataset", claim.Dataset, "unit", claim.ID,
		"repository", logging.ProviderText(cursor.Repo), "component", component, "reason", "listing_moved",
		"step_backs", gitLabTestsMaxListingStepBacks,
	)
	return cursor
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
			// does not (CHAOS-4142). A whole walk claims coverage only up to
			// its start (gitLabTestsWatermark).
			Watermark: gitLabTestsWatermark(cursor, claim),
			Evidence: FetchEvidence{
				Provider: claim.Provider, Dataset: claim.Dataset,
				Requests: cursor.Requests, Pages: cursor.Pages,
				Records: cursor.Pipelines + cursor.Jobs + cursor.Acceptance +
					cursor.Suites + cursor.Cases + cursor.Coverage,
			},
		}, emit)
	}
	requests := cursor.Requests
	var gitLabDate time.Time
	counted := *client
	counted.Doer = gitLabTestsCountingDoer{delegate: client.Doer, attempts: &requests, date: &gitLabDate}
	root := providerRelativePath(client, "api", "v4", "projects", projectID)
	fetchProject := func() (repositoryPayload, int64, error) {
		var project repositoryPayload
		if err := fetchObjectRoute(ctx, &counted, gitlabProjectRoute, root, &project); err != nil {
			return project, 0, err
		}
		cursor.Requests = requests
		parsedProjectID, err := project.ID.Int64()
		if err != nil || parsedProjectID < 1 || strconv.FormatInt(parsedProjectID, 10) != projectID {
			return project, 0, providerfoundation.ErrNormalizationInvalid
		}
		return project, parsedProjectID, nil
	}
	if cursor.Phase == "done" {
		if (cursor.Repo == "" || len(cursor.Repo) > gitLabTestsCursorRepoMaxBytes) && cursor.ProjectID > 0 {
			project, _, fetchErr := fetchProject()
			switch {
			case fetchErr == nil:
				cursor.Repo = gitLabProjectFullName(project)
			case ctx.Err() != nil:
				return fetchErr
			default:
				// Main published the stored path with no project read, so a project that cannot be read now (deleted,
				// moved, rate limited) must not leave a finished scan unfinalized: keep the stored path, or the source
				// name when the cursor holds none.
				slog.Warn("gitlab tests/cicd done resume could not read the project; publishing without a refreshed repo",
					"provider", claim.Provider, "dataset", claim.Dataset, "unit", claim.ID)
				if cursor.Repo == "" {
					cursor.Repo = claim.SourceName
				}
			}
		}
		return emitFinalMetadata(cursor)
	}
	project, parsedProjectID, err := fetchProject()
	if err != nil {
		return err
	}
	if strings.TrimSpace(resumeCursor) == "" {
		// The walk starts here: the project read came before any listing, so its Date is GitLab's clock at or before the
		// first listed page. normalizedAt (the attempt start, before any request) stands in when the project read had no
		// usable Date; a window start below 1 s is not usable (it cannot be persisted: 0 means an older binary's cursor).
		windowStart := gitLabDate.Add(-gitLabTestsWindowStartMargin).Unix()
		if gitLabDate.IsZero() || windowStart < 1 {
			windowStart = normalizedAt.Add(-gitLabTestsWindowStartMargin).Unix()
			slog.Warn("gitlab tests/cicd window start unknown",
				"provider", claim.Provider, "dataset", claim.Dataset, "unit", claim.ID,
				"reason", "no usable Date header on the project read: the window start uses our clock")
			client.Metrics.RecordGitLabTestsWindowStartFallback(claim.Dataset)
		}
		cursor.WindowStart = windowStart
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
		seek := newGitLabTestsSeek(cursor, cursor.PipelinePages)
		visit := func(page gitLabTestsListingPage) error {
			cursor.Pages++
			if page.Discarded {
				return nil
			}
			// First-entry-only counting against the CUMULATIVE budget (the GitLab
			// twin of CHAOS-4130): a page an earlier invocation already charged is
			// free when it is visited again (gitLabTestsSeek).
			cursor.PipelinePages += page.Charged
			for index := 0; index < len(page.Items); index++ {
				if !page.Read[index] {
					continue
				}
				id := page.IDs[index]
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
				after.Page, after.Index = gitLabTestsListingPosition(page.Start + index + 1)
				after.LastID = id
				if page.Last && index == len(page.Items)-1 {
					after.Page, after.Index, after.Phase, after.LastID = 0, 0, "reports", 0
				}
				if err := emitCursor(before, after, CompleteRouteBatch{Effects: effects}, false); err != nil {
					return err
				}
				cursor = after
			}
			if len(page.Items) == 0 && page.Last {
				cursor.Page, cursor.Index, cursor.Phase, cursor.LastID = 0, 0, "reports", 0
			}
			return nil
		}
		allowance, budgetErr := gitLabTestsPhaseAllowance(maxPages, cursor.PipelinePages, &seek)
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
			switch {
			case collection.PageBudgetExhausted:
				cursor = recordGitLabTestsInventoryTruncation(
					cursor, client, claim, gitLabPipelineInventoryComponent, cursor.PipelinePages)
			case collection.StepBacksExhausted:
				cursor = recordGitLabTestsListingMoved(cursor, claim, gitLabPipelineInventoryComponent)
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
	reportSeek := newGitLabTestsSeek(cursor, cursor.ReportPages)
	reportVisit := func(page gitLabTestsListingPage) error {
		cursor.Pages++
		if page.Discarded {
			return nil
		}
		// First-entry-only counting and the id seek, exactly as in the pipelines visit above.
		cursor.ReportPages += page.Charged
		for index := 0; index < len(page.Items); index++ {
			if !page.Read[index] {
				continue
			}
			id := page.IDs[index]
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
			after.Page, after.Index = gitLabTestsListingPosition(page.Start + index + 1)
			after.LastID = id
			// Publish the terminal phase, exactly as the pipelines phase
			// publishes "reports". A cursor at {phase:reports, page:0} is
			// otherwise indistinguishable from a phase that has not
			// started, so a continuation landing on the last item of the
			// last page re-walked the WHOLE reports phase and spent fresh
			// budget on every lap (the GitLab twin of CHAOS-4130).
			if page.Last && index == len(page.Items)-1 {
				after.Page, after.Index, after.Phase, after.LastID = 0, 0, "done", 0
			}
			if err := emitCursor(before, after, CompleteRouteBatch{Effects: effects}, false); err != nil {
				return err
			}
			cursor = after
		}
		if len(page.Items) == 0 && page.Last {
			cursor.Page, cursor.Index, cursor.Phase, cursor.LastID = 0, 0, "done", 0
		}
		return nil
	}
	reportAllowance, reportBudgetErr := gitLabTestsPhaseAllowance(maxPages, cursor.ReportPages, &reportSeek)
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
		switch {
		case collection.PageBudgetExhausted:
			cursor = recordGitLabTestsInventoryTruncation(
				cursor, client, claim, gitLabReportInventoryComponent, cursor.ReportPages)
		case collection.StepBacksExhausted:
			cursor = recordGitLabTestsListingMoved(cursor, claim, gitLabReportInventoryComponent)
		}
	}
	cursor.Requests = requests
	return emitFinalMetadata(cursor)
}

var _ ChunkedCompleteRouteHandler = GitLabTestsRouteHandler{}
