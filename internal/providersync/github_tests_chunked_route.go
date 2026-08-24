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

// githubTestsMaxJobsPerRun bounds the items committed for ONE run: jobs, and
// separately the suites+cases+coverage rows parsed from its artifacts.
//
// For jobs it is a hard bound. For report rows it is a SOFT bound of
// max(cap, one artifact): rows are only committed in whole-artifact units, so
// a single artifact larger than the cap is kept intact rather than split,
// which would separate a suite from its cases (CHAOS-4142, codex round 1).
const githubTestsMaxJobsPerRun = 500

// githubTestsPerRunPerPage is the per_page this route asks for on its per-run
// collections. It is a named constant rather than a literal because the
// configuration check below multiplies it by the page budget: if the request
// and the check could drift apart, the check would validate an arithmetic
// relationship the requests do not actually have (CHAOS-4142, codex round 2).
const githubTestsPerRunPerPage = 100

// githubTestsChunkCursor is deliberately route-owned. A provider Link URL is
// opaque, while the run index makes a crash between two runs on one page
// resumable without replaying the already committed prefix.
type githubTestsChunkCursor struct {
	Phase      string                  `json:"phase"`
	NextURL    string                  `json:"next_url,omitempty"`
	Index      int                     `json:"index,omitempty"`
	Pipelines  int                     `json:"pipelines"`
	Jobs       int                     `json:"jobs"`
	Acceptance int                     `json:"acceptance"`
	Suites     int                     `json:"suites"`
	Cases      int                     `json:"cases"`
	Coverage   int                     `json:"coverage"`
	Requests   int                     `json:"requests"`
	Pages      int                     `json:"pages"`
	Incomplete []GitHubTestsIncomplete `json:"incomplete,omitempty"`
	// RunPages and ArtifactPages are CUMULATIVE inventory-page counts per
	// phase. The paginator's own cap is local to one invocation, and a
	// continuation starts a new invocation, so without these a route crosses
	// its page budget once per attempt-neutral resume and never reports
	// ErrPaginationCapExceeded (CHAOS-3822).
	RunPages      int `json:"run_pages"`
	ArtifactPages int `json:"artifact_pages"`
	// Repo lets the terminal `done` resume publish completion metadata without
	// re-fetching the repository object.
	Repo string `json:"repo,omitempty"`
	// ArchivesSeen and ArchivesUnreadable feed the total-unreadability gate
	// (CHAOS-4185). They are pointers so a cursor written BEFORE this pair
	// existed decodes them as UNKNOWN (nil), never zero: reading an absent
	// counter as zero was the exact false-positive a reverted CHAOS-4177
	// attempt shipped -- a walk spanning the deploy would restore 0/0 after
	// several real successes, then fail on the very next ordinary skip. A
	// fresh walk (empty resume cursor) initializes both to a known 0 in
	// decodeGitHubTestsChunkCursor; once a cursor decodes them as unknown,
	// every subsequent cursor derived from it stays unknown for the rest of
	// that walk's lifetime -- see bumpGitHubTestsArchiveCounter.
	ArchivesSeen       *int `json:"archives_seen,omitempty"`
	ArchivesUnreadable *int `json:"archives_unreadable,omitempty"`
}

// bumpGitHubTestsArchiveCounter returns a NEW pointer one greater than the
// input, or nil if the input is nil. It never mutates through the input
// pointer: githubTestsChunkCursor is copied by value throughout this route
// (before := cursor, after := cursor), and mutating *counter in place would
// silently corrupt every earlier copy that shares the same pointer, since a
// struct copy duplicates the pointer, not its target.
func bumpGitHubTestsArchiveCounter(counter *int) *int {
	if counter == nil {
		return nil
	}
	next := *counter + 1
	return &next
}

// githubTestsAllArtifactsUnreadableFloor is the minimum number of archives
// this walk must have observed before total unreadability is treated as
// evidence of a systematic condition rather than ordinary single-item noise.
// A repository with one workflow run and one corrupt archive would otherwise
// satisfy unreadable==seen at 1/1 and fail a healthy, quiet repository -- a
// regression against the incident CHAOS-4177 fixed (CHAOS-4185).
const githubTestsAllArtifactsUnreadableFloor = 2

// ErrGitHubTestsAllArtifactsUnreadable narrows ErrGitHubTestsIncomplete to
// the case where every archive this walk observed failed to read, on a
// sample large enough to rule out ordinary single-item noise. Unlike
// ErrGitHubTestsArchiveUnreadable / ErrGitHubTestsArtifactUnavailable, which
// each skip ONE bad artifact and keep walking, this means the SOURCE itself
// -- a proxy or auth edge answering every artifact request with a 2xx error
// document -- produced a unit that ingested nothing and would repeat the
// same outcome forever. It must terminalize on the FIRST attempt: retrying
// only re-observes the identical total failure, burning the unit's retry
// budget before finally recording the generic provider_unit_exhausted
// category and losing the specific cause (CHAOS-4185).
var ErrGitHubTestsAllArtifactsUnreadable = fmt.Errorf("%w: all observed artifacts unreadable", ErrGitHubTestsIncomplete)

// githubTestsCheckAllArtifactsUnreadable evaluates the totality gate against
// a fully-formed cursor. It is called from exactly one place --
// emitFinalMetadata, which serves both the natural end of a walk and a
// `done`-cursor resume -- so a totality failure fires at most once per walk
// and always on the FIRST attempt that reaches it, never once per retry.
//
// The counters are UNKNOWN (nil) whenever this cursor -- or the cursor it
// resumed from -- was ever decoded without them. Skipping the gate on
// UNKNOWN is deliberate: a walk spanning the deploy is then bounded and
// self-healing (the very next fresh walk starts with known zero counters)
// rather than a false failure on a unit that already read good archives
// before this code existed.
func githubTestsCheckAllArtifactsUnreadable(
	cursor githubTestsChunkCursor, client *providerfoundation.HTTPClient, claim Claim,
) error {
	if cursor.ArchivesSeen == nil || cursor.ArchivesUnreadable == nil {
		return nil
	}
	seen, unreadable := *cursor.ArchivesSeen, *cursor.ArchivesUnreadable
	if seen < githubTestsAllArtifactsUnreadableFloor || unreadable != seen {
		return nil
	}
	if client != nil {
		client.Metrics.RecordAllArtifactsUnreadable(claim.Provider, claim.Dataset)
	}
	slog.Error(
		"provider unit failing: every observed cicd artifact was unreadable",
		"provider", claim.Provider, "dataset", claim.Dataset, "unit", claim.ID,
		"repository", cursor.Repo, "seen", seen, "unreadable", unreadable,
	)
	return fmt.Errorf("%w: seen=%d unreadable=%d", ErrGitHubTestsAllArtifactsUnreadable, seen, unreadable)
}

// emitCursorPair publishes one emission whose before and after cursors are the
// same value. Used for the terminal metadata emission, which advances no
// provider position.
func emitCursorPair(
	cursor githubTestsChunkCursor, batch CompleteRouteBatch, emit func(ChunkRouteEmission) error,
) error {
	raw, err := encodeGitHubTestsChunkCursor(cursor)
	if err != nil {
		return err
	}
	return emit(ChunkRouteEmission{Batch: batch, CursorBefore: raw, CursorAfter: raw, Final: true})
}

// remainingPageBudget converts a total inventory budget plus the pages already
// spent on earlier attempts into the allowance for this invocation. A budget
// that is already exhausted must fail, not silently fetch one more page.
func remainingPageBudget(budget, spent int) (int, error) {
	if budget < 1 {
		return 0, ErrInvalidConfiguration
	}
	if spent >= budget {
		return 0, ErrPaginationCapExceeded
	}
	return budget - spent, nil
}

// recordGitHubTestsInventoryTruncation finalizes an inventory phase that hit
// its cumulative page budget WITHOUT failing the unit.
//
// Before CHAOS-4130 a budget stop returned ErrPaginationCapExceeded, which
// providerunit maps to the deterministic-terminal `pagination_incomplete`
// category: the unit was permanently cancelled and its checkpoint discarded,
// throwing away the cursor position of a unit that already held thousands of
// durable rows. The Python precedent for a capped newest-first fetch is
// providers/github/code_client.py:735-745 and :965-975 -- WARN, break, and
// return the partial list, so the sync completes successfully with truncated
// coverage. This adopts Python's disposition (never cancel) but not its
// silence: the truncation is recorded as bounded evidence, which makes
// githubTestsFinalMetadataBatch report reports_complete=false and, crucially,
// a nil Watermark. Refusing to advance the watermark is what keeps GitHub's
// newest-first ordering from turning the unreached OLD end of the window into
// the permanent lower-bound hole CHAOS-2587 describes. That refusal is now
// carried by githubTestsWindowBlockingComponents, which lists these inventory
// components and deliberately excludes the per-run ones below (CHAOS-4142).
func recordGitHubTestsInventoryTruncation(
	cursor githubTestsChunkCursor,
	client *providerfoundation.HTTPClient,
	claim Claim,
	component string,
	pagesSpent int,
) githubTestsChunkCursor {
	cursor.Incomplete = mergeGitHubTestsIncomplete(cursor.Incomplete, GitHubTestsIncomplete{
		Component: component, Cause: githubTestsPageBudgetCause, Count: 1,
	})
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

// recordGitHubTestsPerRunTruncation keeps a run whose items exceeded a PER-RUN
// cap instead of failing the unit (CHAOS-4142).
//
// The three per-run caps -- jobs, artifacts, and report rows -- each returned a
// raw ErrPaginationCapExceeded, which providerunit maps to the
// deterministic-terminal `pagination_incomplete` category. Because the cap is a
// property of the RUN and not of the attempt, every retry and every later
// window that still contains that run refused identically: since_at never
// advanced past it, the next hourly window (which only grows) re-included it,
// and three of this org's four github sources sat at zero cicd coverage for
// four days. A bounded skip would have been survivable; a permanent one was
// not.
//
// The disposition chris ruled is the one CHAOS-4130 already applies one level
// up, and the one Python has always had for a capped fetch
// (providers/github/code_client.py:735-745, :965-975 -- WARN, break, return the
// partial list): keep the first cap-worth, record the truncation as bounded
// evidence, and CONTINUE the walk so the unit finalizes.
//
// Unlike its inventory sibling above, this does NOT withhold the watermark.
// The listing walk completes here -- every run in the window is seen and
// committed, and only items inside an already-committed run are dropped -- so
// there is no unreached remainder to protect, and the cap is deterministic, so
// re-walking the window would drop exactly the same items. See
// githubTestsWindowBlockingComponents.
func recordGitHubTestsPerRunTruncation(
	cursor githubTestsChunkCursor,
	client *providerfoundation.HTTPClient,
	claim Claim,
	component string,
	cause string,
	runID string,
	kept int,
) githubTestsChunkCursor {
	cursor.Incomplete = mergeGitHubTestsIncomplete(cursor.Incomplete, GitHubTestsIncomplete{
		Component: component, Cause: cause, Count: 1,
	})
	if client != nil {
		client.Metrics.RecordPerRunTruncation(claim.Provider, claim.Dataset, component, cause)
	}
	// The run id is provider-supplied and unbounded, so it belongs in the log
	// line, never in the durable observation.
	slog.Warn(
		"provider per-run item cap reached; run committed with partial items",
		"provider", claim.Provider, "dataset", claim.Dataset, "unit", claim.ID,
		"repository", cursor.Repo, "component", component, "cause", cause,
		"run", runID, "kept", kept,
	)
	return cursor
}

// recordGitHubTestsSkippedArtifact records ONE skipped artifact under `cause`
// and returns the updated observation slice. It serves BOTH tests routes --
// the chunked one keeps its observations on the cursor, the non-chunked
// oracle on a local slice -- so the vocabulary, the counter and the log line
// cannot drift apart between them (CHAOS-4177). It also serves BOTH skip
// mechanisms within report_member: an archive that downloaded but would not
// open (unreadable_archive) and an artifact whose bytes could never be
// downloaded at all (artifact_unavailable, CHAOS-4191) -- the caller supplies
// which one happened so the durable evidence and the caller-visible fact
// never drift apart.
func recordGitHubTestsSkippedArtifact(
	incomplete []GitHubTestsIncomplete,
	client *providerfoundation.HTTPClient,
	claim Claim,
	repo string,
	runID string,
	cause string,
) []GitHubTestsIncomplete {
	incomplete = mergeGitHubTestsIncomplete(incomplete, GitHubTestsIncomplete{
		Component: githubTestsReportMemberComponent,
		Cause:     cause,
		Count:     1,
	})
	if client != nil {
		client.Metrics.RecordArtifactSkipped(claim.Provider, claim.Dataset, cause)
	}
	// The run id is provider-supplied and unbounded, so it belongs in the log
	// line, never in the durable observation.
	slog.Warn(
		"provider artifact skipped; inventory continued",
		"provider", claim.Provider, "dataset", claim.Dataset, "unit", claim.ID,
		"repository", repo, "component", githubTestsReportMemberComponent,
		"cause", cause, "run", runID,
	)
	return incomplete
}

// The two phases whose resume cursors carry a positional index. Closed set:
// the phase reaches a metric label.
const (
	githubTestsRunsPhase      = "runs"
	githubTestsArtifactsPhase = "artifacts"
)

// githubTestsResumeStart resolves where to resume inside a re-fetched page.
//
// A resume cursor stores an ORDINAL index into a page identified by its URL.
// GitHub serves Actions runs newest-first, so between two attempts new runs
// shift what any given page holds, and the page a cursor named can come back
// shorter than the index recorded against it. That is the provider moving, not
// a corrupt checkpoint: the unit's durable state is intact and every prepared
// chunk is still committed.
//
// Such a mismatch -- a stored index that no longer addresses an item --
// therefore RE-ANCHORS: walk the page again from the start, rather than
// failing.
//
// This detects only the case where the index addresses nothing. A page
// RESHUFFLED to the same length still satisfies the index and is walked from
// it, so an ordinal cursor cannot tell "same items, moved" from "different
// items, same count". That case is silent and undetectable without anchoring
// on item identity (CHAOS-4182), which is why the re-anchor counter is a LOWER
// BOUND on page movement, not a measure of it. Re-walking is safe because every cicd destination is a
// ReplacingMergeTree keyed on the row's natural identity
// (migrations/clickhouse/000_raw_tables.sql:97-106 for ci_pipeline_runs,
// 029_testops_tables.sql:33-102 for the testops tables), so re-processing an
// item replaces its row instead of duplicating it. Clamping to the end of the
// page instead would silently drop whatever moved off it -- data loss reported
// as progress.
//
// Failing here cost one of the unit's five attempts per occurrence, which is
// why a deploy or any other gap used to burn attempts on exactly the busiest
// repositories -- the ones whose pages shift most (CHAOS-4177).
func githubTestsResumeStart(
	cursor githubTestsChunkCursor,
	page providerfoundation.PageVisit,
	client *providerfoundation.HTTPClient,
	claim Claim,
	phase string,
) int {
	if cursor.NextURL != page.CursorBefore {
		return 0
	}
	// A cursor is never persisted with Index == len(page.Items): both item
	// write sites (the runs loop and its artifacts twin) assign index+1 and
	// then normalise every >= len case to index 0 with CursorAfter, and both
	// empty-page branches set index 0. A persisted Index > 0 therefore always
	// addressed an item that
	// existed, so an index at or past the re-fetched length now addresses
	// NOTHING -- walking from there would process zero items and advance past
	// whatever the page still held, silently. Index 0 is always a legitimate
	// start, including on an empty page.
	if cursor.Index == 0 || cursor.Index < len(page.Items) {
		return cursor.Index
	}
	if client != nil {
		client.Metrics.RecordResumeReanchor(claim.Provider, claim.Dataset, phase)
	}
	slog.Warn(
		"provider page moved under a resume; re-anchoring and re-walking the page",
		"provider", claim.Provider, "dataset", claim.Dataset, "unit", claim.ID,
		"repository", cursor.Repo, "phase", phase,
		"stored_index", cursor.Index, "page_items", len(page.Items),
	)
	return 0
}

func decodeGitHubTestsChunkCursor(raw string) (githubTestsChunkCursor, error) {
	if strings.TrimSpace(raw) == "" {
		// A brand-new walk starts the totality counters at a KNOWN zero: only
		// a cursor that predates these fields decodes them as unknown below.
		// Two separate variables: the two fields must never alias one int.
		seenZero, unreadableZero := 0, 0
		return githubTestsChunkCursor{
			Phase: "runs", ArchivesSeen: &seenZero, ArchivesUnreadable: &unreadableZero,
		}, nil
	}
	var cursor githubTestsChunkCursor
	if json.Unmarshal([]byte(raw), &cursor) != nil ||
		(cursor.Phase != "runs" && cursor.Phase != "artifacts" && cursor.Phase != "done") ||
		cursor.Index < 0 || cursor.NextURL == "" && cursor.Index != 0 ||
		cursor.RunPages < 0 || cursor.ArtifactPages < 0 ||
		// The counter pair is either both known or both unknown, never split:
		// a cursor carrying exactly one is corrupt, not a legacy shape (a
		// legacy cursor predates the field entirely, so it is missing BOTH).
		(cursor.ArchivesSeen == nil) != (cursor.ArchivesUnreadable == nil) ||
		(cursor.ArchivesSeen != nil && *cursor.ArchivesSeen < 0) ||
		(cursor.ArchivesUnreadable != nil && *cursor.ArchivesUnreadable < 0) ||
		(cursor.ArchivesSeen != nil && cursor.ArchivesUnreadable != nil &&
			*cursor.ArchivesUnreadable > *cursor.ArchivesSeen) {
		return githubTestsChunkCursor{}, ErrChunkCheckpointConflict
	}
	return cursor, nil
}

func encodeGitHubTestsChunkCursor(cursor githubTestsChunkCursor) (string, error) {
	encoded, err := json.Marshal(cursor)
	if err != nil || len(encoded) > maxChunkCursorBytes {
		return "", ErrChunkCheckpointConflict
	}
	return string(encoded), nil
}

// CollectChunks streams GitHub TestOps pages. It mirrors Collect's fetch and
// normalization rules, but emits one bounded run/report unit at a time and
// persists an opaque page URL plus item index after every emission.
// githubTestsFinalMetadataBatch builds the terminal completion batch for one
// chunked github tests/cicd unit from its cursor.
//
// The cursor's Incomplete slice is typed nil on every healthy unit: a clean
// first pass never appends to it, and a resumed cursor decodes it from JSON
// with omitempty. A typed-nil slice marshals to JSON null, which the
// production comparator — like every durable optional-evidence reader since
// CHAOS-3940 — refuses. Normalize to a non-nil empty slice here, at the one
// writer, so the durable form is always [] and readers stay strict.
func githubTestsFinalMetadataBatch(claim Claim, cursor githubTestsChunkCursor) (CompleteRouteBatch, error) {
	effects, err := testOpsEffects(nil, nil, nil, nil, nil, nil)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	incomplete := append(
		make([]GitHubTestsIncomplete, 0, len(cursor.Incomplete)), cursor.Incomplete...,
	)
	return CompleteRouteBatch{
		Effects: effects,
		Result: map[string]any{
			"pipeline_runs_synced": cursor.Pipelines, "job_runs_synced": cursor.Jobs,
			"acceptance_checks_synced": cursor.Acceptance, "test_suites_synced": cursor.Suites,
			"test_cases_synced": cursor.Cases, "coverage_snapshots_synced": cursor.Coverage,
			"repo": cursor.Repo, "reports_complete": len(incomplete) == 0,
			"reports_skipped": githubTestsIncompleteCount(incomplete),
			"incomplete":      incomplete,
		},
		// Only a WINDOW-BLOCKING observation withholds the watermark. An
		// inventory page cap leaves the old end of a newest-first window
		// unreached, so advancing would make that gap permanent (CHAOS-2587).
		// A per-run truncation walked the whole window and dropped items only
		// inside runs it already committed, so withholding there buys no
		// recovered coverage -- the cap is deterministic -- and pins since_at
		// forever, which is the CHAOS-4142 defect.
		Watermark: func() *time.Time {
			if githubTestsBlocksWatermark(incomplete) {
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
	}, nil
}

func (handler GitHubTestsRouteHandler) CollectChunks(
	ctx context.Context,
	claim Claim,
	_ providerfoundation.Credential,
	client *providerfoundation.HTTPClient,
	normalizedAt time.Time,
	resumeCursor string,
	emit func(ChunkRouteEmission) error,
) error {
	if ctx == nil || claim.Validate() != nil || claim.Provider != "github" ||
		(claim.Dataset != "tests" && claim.Dataset != "cicd") || client == nil ||
		client.Provider != "github" || client.BaseURL == nil || normalizedAt.IsZero() || emit == nil {
		return ErrInvalidConfiguration
	}
	normalizedAt = normalizedAt.UTC().Truncate(time.Millisecond)
	owner, repository, err := splitGitHubRepository(claim.SourceExternalID)
	if err != nil {
		return err
	}
	// Decode BEFORE any provider call. A cursor in the terminal `done` phase
	// means the inventory scan already finished on an earlier attempt; the only
	// thing left is to re-publish the completion metadata so the unit can
	// finalize. Re-entering pagination there refetched the whole final phase,
	// re-downloaded artifacts, and double-counted the cursor's own counters
	// (CHAOS-3820).
	cursor, err := decodeGitHubTestsChunkCursor(resumeCursor)
	if err != nil {
		return err
	}
	emitFinalMetadata := func(cursor githubTestsChunkCursor) error {
		cursor.Phase = "done"
		// The totality gate runs here, not inline in the artifacts loop below,
		// so it evaluates exactly once per walk regardless of which of the two
		// callers below reached it -- the natural end of pagination, or a
		// resume that landed directly on an already-`done` cursor.
		if err := githubTestsCheckAllArtifactsUnreadable(cursor, client, claim); err != nil {
			return err
		}
		batch, batchErr := githubTestsFinalMetadataBatch(claim, cursor)
		if batchErr != nil {
			return batchErr
		}
		return emitCursorPair(cursor, batch, emit)
	}
	if cursor.Phase == "done" {
		return emitFinalMetadata(cursor)
	}
	// Configuration is settled BEFORE the first provider call. None of these
	// bounds depends on the repository, and a configuration that can never
	// produce a correct walk should not spend a request discovering that
	// (CHAOS-4142, codex round 2). It stays BELOW the `done` early return: a
	// cursor that already finished its scan only republishes metadata, and
	// refusing there would strand an otherwise complete unit.
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
		return ErrInvalidConfiguration
	}
	if err := validatePerRunPageBudget(
		"MaxJobPages", jobPages, githubTestsPerRunPerPage, githubTestsMaxJobsPerRun,
	); err != nil {
		return err
	}
	root := providerRelativePath(client, "repos", owner, repository)
	var repo gitHubRepositoryPayload
	if err := fetchObject(ctx, client, root, &repo); err != nil {
		return err
	}
	cursor.Repo = repo.FullName
	repoID, err := repositoryIdentity(repo.FullName)
	if err != nil {
		return err
	}
	if cursor.Requests == 0 {
		cursor.Requests = 1 // repository lookup above
	}
	policyCache := map[string]githubTestsPolicy{}
	emitCursor := func(before, after githubTestsChunkCursor, batch CompleteRouteBatch, final bool) error {
		beforeRaw, beforeErr := encodeGitHubTestsChunkCursor(before)
		if beforeErr != nil {
			return beforeErr
		}
		afterRaw, afterErr := encodeGitHubTestsChunkCursor(after)
		if afterErr != nil {
			return afterErr
		}
		return emit(ChunkRouteEmission{Batch: batch, CursorBefore: beforeRaw, CursorAfter: afterRaw, Final: final})
	}
	emitRunPage := func(page providerfoundation.PageVisit) error {
		cursor.Pages++
		// Count a page against the CUMULATIVE budget only on first entry.
		// A continuation re-GETs the page it stopped inside and discards the
		// already-consumed prefix, so at MaxChunksPerAttempt=8 a 100-item page
		// is visited about 12.5 times. Counting visits turned a 100-page
		// budget into ~7.6 real pages (~760 runs), which every busy repo
		// exceeded (CHAOS-4130). cursor.Pages stays unconditional: it is fetch
		// evidence, and a re-visit really is a page this route downloaded.
		//
		// SUBTLETY: cursor.NextURL == page.CursorBefore is also true for a
		// FRESH page entered at index 0 -- it is simply the cursor we resumed
		// from. cursor.Index > 0 is the half that identifies a re-entry into a
		// partially consumed page; without it the budget would never advance
		// and the cap would be disabled entirely.
		if !(cursor.NextURL == page.CursorBefore && cursor.Index > 0) {
			cursor.RunPages++
		}
		start := githubTestsResumeStart(cursor, page, client, claim, githubTestsRunsPhase)
		for index := start; index < len(page.Items); index++ {
			before := cursor
			var run gitHubWorkflowRunPayload
			decoder := json.NewDecoder(strings.NewReader(string(page.Items[index])))
			decoder.UseNumber()
			if decoder.Decode(&run) != nil {
				return providerfoundation.ErrNormalizationInvalid
			}
			pipeline, include := normalizeGitHubTestsPipeline(claim, repoID, run, normalizedAt)
			jobs := make([]githubTestsJobRow, 0)
			acceptance := make([]githubTestsAcceptanceRow, 0)
			if include && !ciPipelineRunOutsideWindow(pipeline.StartedAt, claim) {
				jobPage, pageErr := providerfoundation.CollectGitHubLinkPages(ctx, client, providerfoundation.GitHubPageOptions{
					Path:     root + "/actions/runs/" + url.PathEscape(pipeline.RunID) + "/jobs",
					Query:    url.Values{"per_page": {strconv.Itoa(githubTestsPerRunPerPage)}},
					DataKey:  "jobs",
					MaxPages: jobPages,
					MaxItems: githubTestsMaxJobsPerRun + 1,
				})
				if pageErr != nil {
					return pageErr
				}
				cursor.Requests += jobPage.Pages
				cursor.Pages += jobPage.Pages
				// The paginator reports WHICH bound stopped it, so this reads
				// the reason instead of inferring one from len(). MaxItems is
				// cap+1, so ItemCapReached means the provider had at least one
				// job beyond the cap: the boundary was positively observed.
				// PageBudgetExhausted means MaxPages ran out first and the
				// remainder was never seen, which must not advance the
				// watermark. Item cap is checked first: if both are somehow
				// set, the observed boundary is the more specific fact.
				jobItems := jobPage.Items
				switch {
				case jobPage.ItemCapReached:
					if len(jobItems) > githubTestsMaxJobsPerRun {
						jobItems = jobItems[:githubTestsMaxJobsPerRun]
					}
					cursor = recordGitHubTestsPerRunTruncation(
						cursor, client, claim, githubTestsRunJobsComponent,
						githubTestsPerRunCapCause, pipeline.RunID, len(jobItems),
					)
				// UNREACHABLE through configuration, and KEPT anyway.
				// validatePerRunPageBudget refuses any jobPages that cannot
				// outrun the item cap, so with full pages the cap above always
				// fires first. It is kept, still withholding, for two reasons:
				// a provider that advertises a next page while returning SHORT
				// pages can still land here, and deleting it would silently
				// change this route's semantics if anyone later relaxes that
				// validation. A branch that cannot fire is honest; a missing
				// branch is a trapdoor (CHAOS-4142, codex round 2).
				case jobPage.PageBudgetExhausted:
					cursor = recordGitHubTestsPerRunTruncation(
						cursor, client, claim, githubTestsRunJobsComponent,
						githubTestsPerRunPageBudgetCause, pipeline.RunID, len(jobItems),
					)
				}
				for _, jobRaw := range jobItems {
					var job githubTestsJobPayload
					decoder := json.NewDecoder(strings.NewReader(string(jobRaw)))
					decoder.UseNumber()
					if decoder.Decode(&job) != nil {
						return providerfoundation.ErrNormalizationInvalid
					}
					row, ok := normalizeGitHubTestsJob(claim, repoID, pipeline.RunID, pipeline.RetryCount, job, normalizedAt)
					if ok {
						jobs = append(jobs, row)
					}
				}
				targetBranch, prNumber := gitHubTestsTarget(run)
				policy := githubTestsPolicy{provenance: "github.branch_protection.target_branch_unavailable"}
				if targetBranch != nil {
					cached, ok := policyCache[*targetBranch]
					if !ok {
						cached, err = fetchGitHubTestsPolicy(ctx, client, root, *targetBranch)
						cursor.Requests++
						if err != nil {
							return err
						}
						policyCache[*targetBranch] = cached
					}
					policy = cached
				}
				acceptance = projectGitHubTestsChecks(claim, repoID, pipeline, jobs, policy, targetBranch, prNumber, testsOptionalString(stringValue(run.HTMLURL)), normalizedAt)
				cursor.Pipelines++
				cursor.Jobs += len(jobs)
				cursor.Acceptance += len(acceptance)
			}
			effects, effectErr := testOpsEffects(
				func() []githubTestsPipelineRow {
					if include && !ciPipelineRunOutsideWindow(pipeline.StartedAt, claim) {
						return []githubTestsPipelineRow{pipeline}
					}
					return nil
				}(),
				jobs, acceptance, nil, nil, nil,
			)
			if effectErr != nil {
				return effectErr
			}
			after := cursor
			after.Index = index + 1
			after.NextURL = page.CursorBefore
			if after.Index >= len(page.Items) {
				after.Index = 0
				after.NextURL = page.CursorAfter
				if after.NextURL == "" {
					after.Phase = "artifacts"
				}
			}
			if err := emitCursor(before, after, CompleteRouteBatch{Effects: effects}, false); err != nil {
				return err
			}
			cursor = after
		}
		if len(page.Items) == 0 {
			cursor.NextURL = page.CursorAfter
			cursor.Index = 0
			if cursor.NextURL == "" {
				cursor.Phase = "artifacts"
			}
		}
		return nil
	}

	if cursor.Phase == "runs" {
		query := url.Values{"per_page": {"100"}}
		if claim.SinceAt != nil || claim.BeforeAt != nil {
			start, end := "*", "*"
			if claim.SinceAt != nil {
				start = claim.SinceAt.UTC().Format(time.RFC3339)
			}
			if claim.BeforeAt != nil {
				end = claim.BeforeAt.UTC().Format(time.RFC3339)
			}
			query.Set("created", start+".."+end)
		}
		allowance, budgetErr := remainingPageBudget(
			(maxRuns+nativePerPage-1)/nativePerPage, cursor.RunPages)
		switch {
		case errors.Is(budgetErr, ErrPaginationCapExceeded):
			cursor = recordGitHubTestsInventoryTruncation(
				cursor, client, claim, githubTestsRunInventoryComponent, cursor.RunPages)
		case budgetErr != nil:
			return budgetErr
		default:
			pageOptions := providerfoundation.GitHubPageOptions{Path: root + "/actions/runs", Query: query, DataKey: "workflow_runs", MaxPages: allowance, InitialURL: cursor.NextURL}
			collection, visitErr := providerfoundation.VisitGitHubLinkPages(ctx, client, pageOptions, emitRunPage)
			if visitErr != nil {
				return visitErr
			}
			if collection.PageBudgetExhausted {
				cursor = recordGitHubTestsInventoryTruncation(
					cursor, client, claim, githubTestsRunInventoryComponent, cursor.RunPages)
			}
		}
	}

	if cursor.Phase != "artifacts" {
		cursor.Phase = "artifacts"
		cursor.NextURL, cursor.Index = "", 0
	}
	if cursor.Phase == "artifacts" {
		artifactQuery := url.Values{"per_page": {"100"}}
		if repo.DefaultBranch != "" {
			artifactQuery.Set("branch", repo.DefaultBranch)
		}
		if claim.SinceAt != nil {
			artifactQuery.Set("created", ">="+claim.SinceAt.UTC().Format(time.DateOnly))
		}
		artifactPage := func(page providerfoundation.PageVisit) error {
			cursor.Pages++
			// First-entry-only counting, exactly as in emitRunPage above. This
			// twin has never fired in production only because no unit has ever
			// survived the runs phase to reach it (CHAOS-4130).
			if !(cursor.NextURL == page.CursorBefore && cursor.Index > 0) {
				cursor.ArtifactPages++
			}
			start := githubTestsResumeStart(cursor, page, client, claim, githubTestsArtifactsPhase)
			for index := start; index < len(page.Items); index++ {
				before := cursor
				var run gitHubWorkflowRunPayload
				decoder := json.NewDecoder(strings.NewReader(string(page.Items[index])))
				decoder.UseNumber()
				if decoder.Decode(&run) != nil {
					return providerfoundation.ErrNormalizationInvalid
				}
				pipeline, include := normalizeGitHubTestsPipeline(claim, repoID, run, normalizedAt)
				suites := []testSuiteResultRow(nil)
				cases := []testCaseResultRow(nil)
				coverage := []coverageSnapshotRow(nil)
				if include && (claim.BeforeAt == nil || !pipeline.StartedAt.After(claim.BeforeAt.UTC())) {
					artPage, pageErr := providerfoundation.CollectGitHubLinkPages(ctx, client, providerfoundation.GitHubPageOptions{
						Path:  root + "/actions/runs/" + url.PathEscape(pipeline.RunID) + "/artifacts",
						Query: url.Values{"per_page": {"100"}}, DataKey: "artifacts", MaxPages: 1,
					})
					if pageErr != nil {
						return pageErr
					}
					cursor.Requests += artPage.Pages
					cursor.Pages += artPage.Pages
					// Same two-facts split as the jobs cap above, except this
					// collection passes no MaxItems, so ItemCapReached can
					// never fire here and the item cap is necessarily
					// len-based. MaxPages is 1, so PageBudgetExhausted means a
					// second artifact page existed and was never fetched.
					artifactItems := artPage.Items
					switch {
					case len(artifactItems) > maxArtifacts:
						artifactItems = artifactItems[:maxArtifacts]
						cursor = recordGitHubTestsPerRunTruncation(
							cursor, client, claim, githubTestsRunArtifactsComponent,
							githubTestsPerRunCapCause, pipeline.RunID, len(artifactItems),
						)
					case artPage.PageBudgetExhausted:
						cursor = recordGitHubTestsPerRunTruncation(
							cursor, client, claim, githubTestsRunArtifactsComponent,
							githubTestsPerRunPageBudgetCause, pipeline.RunID, len(artifactItems),
						)
					}
					for _, artifactRaw := range artifactItems {
						var artifact githubTestsArtifactPayload
						decoder := json.NewDecoder(strings.NewReader(string(artifactRaw)))
						decoder.UseNumber()
						if decoder.Decode(&artifact) != nil || artifact.ID == "" {
							return providerfoundation.ErrNormalizationInvalid
						}
						if artifact.Expired {
							continue
						}
						// Counted as SEEN the moment a real read is attempted --
						// before the outcome is known -- so every downstream
						// disposition (healthy, unreadable, or a fatal parse
						// bound) is reflected in the denominator the totality
						// gate divides by (CHAOS-4185).
						cursor.ArchivesSeen = bumpGitHubTestsArchiveCounter(cursor.ArchivesSeen)
						archive, used, downloadErr := downloadGitHubTestsArtifact(ctx, client, root, string(artifact.ID))
						cursor.Requests += used
						if downloadErr != nil {
							// An artifact whose bytes could never be
							// downloaded is provider data, not a fault of
							// this unit. Skip it exactly as an unreadable
							// container is skipped below, and keep walking:
							// one bad artifact must not cost the healthy ones
							// or the whole unit (CHAOS-4191, extending
							// CHAOS-4177).
							if errors.Is(downloadErr, ErrGitHubTestsArtifactUnavailable) {
								cursor.Incomplete = recordGitHubTestsSkippedArtifact(
									cursor.Incomplete, client, claim, cursor.Repo, pipeline.RunID,
									githubTestsArtifactUnavailableCause,
								)
								cursor.ArchivesUnreadable = bumpGitHubTestsArchiveCounter(cursor.ArchivesUnreadable)
								continue
							}
							return downloadErr
						}
						if len(archive) == 0 {
							continue
						}
						rows, parseErr := parseGitHubTestsArtifact(archive, string(artifact.ID), repoID, pipeline.RunID, claim.OrgID, pipeline.StartedAtPtr(), pipeline.FinishedAt, normalizedAt)
						if parseErr != nil {
							// An archive that will not open is provider data, not
							// a fault of this unit. Skip it exactly as an expired
							// or empty artifact is skipped a few lines above, and
							// keep walking: one bad artifact must not cost the
							// healthy ones or the whole unit (CHAOS-4177).
							if errors.Is(parseErr, ErrGitHubTestsArchiveUnreadable) {
								cursor.Incomplete = recordGitHubTestsSkippedArtifact(
									cursor.Incomplete, client, claim, cursor.Repo, pipeline.RunID,
									githubTestsUnreadableArchiveCause,
								)
								cursor.ArchivesUnreadable = bumpGitHubTestsArchiveCounter(cursor.ArchivesUnreadable)
								continue
							}
							return fmt.Errorf("%w: artifact parse failed: %v", ErrGitHubTestsIncomplete, parseErr)
						}
						reportIncomplete, optional := rows.optionalIncomplete()
						if !optional {
							return fmt.Errorf("%w: reports skipped=%d: unsafe archive bounds", ErrGitHubTestsIncomplete, rows.Skipped)
						}
						for _, observation := range reportIncomplete {
							cursor.Incomplete = mergeGitHubTestsIncomplete(cursor.Incomplete, observation)
						}
						// Bound the run's committed report rows WITHOUT splitting
						// an artifact. Rows are checked for fit BEFORE they are
						// appended, so the aggregate cannot creep past the cap
						// one artifact at a time; a suite is never separated
						// from its cases, because incoherent rows are wrong
						// data rather than partial data.
						//
						// The bound is therefore max(cap, first artifact's rows)
						// and not the cap flat: an artifact that alone exceeds
						// the cap is still kept, because committing ZERO rows
						// for a run that has reports is a worse outcome than
						// overshooting once. See githubTestsMaxJobsPerRun.
						committed := len(suites) + len(cases) + len(coverage)
						incoming := len(rows.Suites) + len(rows.Cases) + len(rows.Coverage)
						if committed > 0 && committed+incoming > githubTestsMaxJobsPerRun {
							cursor = recordGitHubTestsPerRunTruncation(
								cursor, client, claim, githubTestsRunReportsComponent,
								githubTestsPerRunCapCause, pipeline.RunID, committed,
							)
							break
						}
						suites = append(suites, rows.Suites...)
						cases = append(cases, rows.Cases...)
						coverage = append(coverage, rows.Coverage...)
						// Reachable only when the FIRST artifact alone exceeds
						// the cap: it is kept whole, and the run stops here.
						if len(suites)+len(cases)+len(coverage) > githubTestsMaxJobsPerRun {
							cursor = recordGitHubTestsPerRunTruncation(
								cursor, client, claim, githubTestsRunReportsComponent,
								githubTestsPerRunCapCause, pipeline.RunID,
								len(suites)+len(cases)+len(coverage),
							)
							break
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
				after.Index = index + 1
				after.NextURL = page.CursorBefore
				if after.Index >= len(page.Items) {
					after.Index = 0
					after.NextURL = page.CursorAfter
					// Publish the terminal phase the moment the last artifact
					// page is consumed, exactly as the runs phase publishes
					// "artifacts". Without it a cursor at {phase:artifacts,
					// next_url:"", index:0} is indistinguishable from a phase
					// that has not started, so a continuation landing on the
					// last item of the last page re-walked the WHOLE artifacts
					// phase -- spending two fresh pages of budget per lap
					// until the cap stopped it (CHAOS-4130).
					if after.NextURL == "" {
						after.Phase = "done"
					}
				}
				if err := emitCursor(before, after, CompleteRouteBatch{Effects: effects}, false); err != nil {
					return err
				}
				cursor = after
			}
			if len(page.Items) == 0 {
				cursor.NextURL = page.CursorAfter
				cursor.Index = 0
				if cursor.NextURL == "" {
					cursor.Phase = "done"
				}
			}
			return nil
		}
		allowance, budgetErr := remainingPageBudget(
			(maxRuns+nativePerPage-1)/nativePerPage, cursor.ArtifactPages)
		switch {
		case errors.Is(budgetErr, ErrPaginationCapExceeded):
			cursor = recordGitHubTestsInventoryTruncation(
				cursor, client, claim, githubTestsArtifactInventoryComponent, cursor.ArtifactPages)
		case budgetErr != nil:
			return budgetErr
		default:
			collection, visitErr := providerfoundation.VisitGitHubLinkPages(ctx, client, providerfoundation.GitHubPageOptions{Path: root + "/actions/runs", Query: artifactQuery, DataKey: "workflow_runs", MaxPages: allowance, InitialURL: cursor.NextURL}, artifactPage)
			if visitErr != nil {
				return visitErr
			}
			if collection.PageBudgetExhausted {
				cursor = recordGitHubTestsInventoryTruncation(
					cursor, client, claim, githubTestsArtifactInventoryComponent, cursor.ArtifactPages)
			}
		}
	}

	// The inventory scan is complete. Publishing the terminal phase in the
	// SAME emission that carries the completion metadata means a crash between
	// this commit and MarkInventoryComplete resumes into emitFinalMetadata
	// rather than back into pagination.
	return emitFinalMetadata(cursor)
}

var _ ChunkedCompleteRouteHandler = GitHubTestsRouteHandler{}
