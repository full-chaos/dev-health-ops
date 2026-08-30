package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

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
	// SkippedArtifacts is the bounded per-artifact marker sample for
	// oversized-artifact skips (CHAOS-4315) -- see GitHubTestsSkippedArtifact
	// and githubTestsMaxSkippedArtifactRecords. SkippedArtifactsOverflow
	// counts skips beyond the cap that could not get a record; it is always
	// consistent with (and never double-counted against)
	// githubTestsArtifactOversizedCause's Incomplete Count, which reflects
	// every skip regardless of the cap.
	SkippedArtifacts         []GitHubTestsSkippedArtifact `json:"skipped_artifacts,omitempty"`
	SkippedArtifactsOverflow int                          `json:"skipped_artifacts_overflow,omitempty"`
	// SkippedArtifactCauseOverflow records WHICH report_member causes had at
	// least one marker dropped by the githubTestsMaxSkippedArtifactRecords
	// cap (CHAOS-4592 codex review round 2, P1). SkippedArtifactsOverflow
	// above is a single aggregate count shared by every cause recorded
	// through appendGitHubTestsSkippedArtifact, so "overflow>0" alone cannot
	// prove any SPECIFIC cause's marker-writing path ran to completion: a
	// heavy run of one cause filling the shared 20-record sample can make an
	// UNRELATED unmarked cause look excused (over-permissive), and
	// symmetrically a heavy run of the three original causes can starve a
	// later malformed/unreadable skip out of ever landing in the sample,
	// permanently withholding on that one unit even under this binary
	// (over-restrictive -- recreating the exact permanent-stall class this
	// ticket exists to close, just narrower). This is the per-cause ledger
	// githubTestsReportMemberSkippedWithoutDurableMarker actually needs; see
	// markGitHubTestsSkippedArtifactCauseOverflow for why it is never
	// mutated in place.
	SkippedArtifactCauseOverflow map[string]bool `json:"skipped_artifact_cause_overflow,omitempty"`
	// SkippedArtifactCauseCount is the EXACT, sample-cap-independent count of
	// report_member skips THIS accumulation has recorded for each cause,
	// incremented unconditionally by appendGitHubTestsSkippedArtifact on
	// every call regardless of whether the record fit the bounded
	// SkippedArtifacts sample or overflowed past it (codex review gate round
	// 2, P1). SkippedArtifactCauseOverflow above is a boolean -- "at least
	// one marker for this cause was dropped" -- which proves a cause has
	// SOME durable evidence but not that ALL of an Incomplete observation's
	// Count is accounted for: a walk whose cursor was resumed from a binary
	// that predates ANY marker-writing for a cause (true for
	// malformed/unreadable before this ticket; a cursor can carry
	// Incomplete{malformed, Count:5} with zero markers from such a binary)
	// would satisfy "cause has >=1 marker" the moment THIS binary appends
	// just ONE new marker for that cause, incorrectly treating all 5 as
	// covered. SkippedArtifactCauseCount closes that gap: the guard requires
	// causeCount[cause] >= observation.Count, not mere presence. A cursor
	// decoded before this field existed carries causeCount as nil/absent for
	// every cause, which is the conservative (over-restrictive, self-healing
	// per the doc comment on githubTestsReportMemberSkippedWithoutDurableMarker)
	// direction -- never the unsafe one.
	SkippedArtifactCauseCount map[string]int `json:"skipped_artifact_cause_count,omitempty"`
	// ExcludedNonReportSuffix/Prefix count artifacts the selection seam
	// (githubTestsArtifactSelectionSeam, CHAOS-4588/CHAOS-4591) excluded
	// BEFORE any download -- never counted toward ArchivesSeen/Unreadable,
	// never part of the closed Incomplete vocabulary, never watermark-
	// blocking. Plain counters, not a GitHubTestsSkippedArtifact-shaped
	// bounded list: the reason is always one of exactly two closed values, so
	// nothing is lost by not keeping a per-record Cause. ExcludedArtifactSample
	// is a SMALL bounded sample of "name (reason)" strings (cap
	// githubTestsMaxExcludedArtifactSampleRecords, well under
	// githubTestsMaxSkippedArtifactRecords) purely so an operator or a future
	// CHAOS-4591 admin view can see WHICH artifacts were excluded, not just
	// how many -- kept deliberately small against the same maxChunkCursorBytes
	// budget GitHubTestsSkippedArtifact's own doc comment describes.
	ExcludedNonReportSuffix int      `json:"excluded_non_report_suffix,omitempty"`
	ExcludedNonReportPrefix int      `json:"excluded_non_report_prefix,omitempty"`
	ExcludedArtifactSample  []string `json:"excluded_artifact_sample,omitempty"`
}

// markGitHubTestsSkippedArtifactCauseOverflow returns a NEW map with cause
// marked true, never mutating existing. githubTestsChunkCursor is copied by
// value throughout this route (before := cursor; after := cursor), and Go
// maps are reference types, so mutating a shared map in place would corrupt
// an earlier snapshot that still holds the same map -- the identical hazard
// bumpGitHubTestsArchiveCounter's own doc comment describes for its pointer
// field.
func markGitHubTestsSkippedArtifactCauseOverflow(existing map[string]bool, cause string) map[string]bool {
	next := make(map[string]bool, len(existing)+1)
	for key, value := range existing {
		next[key] = value
	}
	next[cause] = true
	return next
}

// markGitHubTestsSkippedArtifactCauseCount returns a NEW map with cause's
// count incremented by one, never mutating existing -- the identical
// clone-then-add discipline markGitHubTestsSkippedArtifactCauseOverflow
// documents, for the identical reason (githubTestsChunkCursor is copied by
// value throughout this route, and Go maps are reference types).
func markGitHubTestsSkippedArtifactCauseCount(existing map[string]int, cause string) map[string]int {
	next := make(map[string]int, len(existing)+1)
	for key, value := range existing {
		next[key] = value
	}
	next[cause]++
	return next
}

// appendGitHubTestsSkippedArtifact records ONE oversized-artifact marker,
// capping the retained sample at githubTestsMaxSkippedArtifactRecords and
// counting the rest via overflow rather than growing the list unbounded
// (CHAOS-4315 -- see the cap's own doc comment for why this must stay
// bounded). causeOverflow tracks per-cause overflow (CHAOS-4592 codex round
// 2, P1) alongside the aggregate overflow count -- see
// githubTestsChunkCursor.SkippedArtifactCauseOverflow for why both must
// exist.
func appendGitHubTestsSkippedArtifact(
	records []GitHubTestsSkippedArtifact, overflow int, causeOverflow map[string]bool, causeCount map[string]int,
	record GitHubTestsSkippedArtifact,
) ([]GitHubTestsSkippedArtifact, int, map[string]bool, map[string]int) {
	// Unconditional: this call IS one real skip event for record.Cause,
	// whether or not it fits the bounded sample below (codex review gate
	// round 2, P1 -- see githubTestsChunkCursor.SkippedArtifactCauseCount).
	causeCount = markGitHubTestsSkippedArtifactCauseCount(causeCount, record.Cause)
	if len(records) >= githubTestsMaxSkippedArtifactRecords {
		return records, overflow + 1, markGitHubTestsSkippedArtifactCauseOverflow(causeOverflow, record.Cause), causeCount
	}
	// Name is provider-supplied and unbounded (CHAOS-4588/CHAOS-4591, codex
	// round 1, P1): GitHub's own limit is 255 bytes, and up to
	// githubTestsMaxSkippedArtifactRecords of these live in the same
	// maxChunkCursorBytes (4KiB) budget as everything else on the cursor.
	// Truncated here, at the one append site, rather than at each of the
	// four call sites, so the bound can never be forgotten at a new one.
	record.Name = githubTestsTruncateArtifactName(record.Name)
	return append(records, record), overflow, causeOverflow, causeCount
}

// githubTestsMaxArtifactNameBytes bounds any provider-supplied artifact name
// stored on the cursor (GitHubTestsSkippedArtifact.Name and
// ExcludedArtifactSample entries).
//
// CHAOS-4592 codex review (P1, on merged CHAOS-4588/CHAOS-4591 code): the
// value this replaced (48) was sized against githubTestsMaxSkippedArtifactRecords's
// OWN stale "~90-120 bytes/record" estimate, written before this Name field
// existed at all -- with Name added, one record actually serializes to
// ~206 bytes at the old 48-byte name, so
// githubTestsMaxSkippedArtifactRecords's OLD cap of 20 records alone
// encoded to ~4.1KB, already exceeding the WHOLE cursor's maxChunkCursorBytes
// (4KiB) budget before Repo/NextURL/Incomplete/ExcludedArtifactSample/
// everything else on githubTestsChunkCursor is added -- turning a
// documented, harmless truncation (records beyond the cap collapse into
// SkippedArtifactsOverflow) into an undocumented, harmful one
// (encodeGitHubTestsChunkCursor fails ErrChunkCheckpointConflict, losing a
// unit's progress instead of merely trimming its operator-triage sample).
// 24 bytes here alongside githubTestsMaxSkippedArtifactRecords's 8 keeps a
// full worst-case cursor (every field at a realistic maximum, computed and
// pinned by TestGitHubTestsChunkCursorWorstCaseStaysWithinBudget) at ~3.3KB
// -- comfortable headroom under the 4KiB budget rather than a value that
// merely happens to fit today. Still enough to recognize a pattern (e.g.
// the ".dockerbuild" or "digests-" run CHAOS-4588 is about), not to
// reproduce the full name.
const githubTestsMaxArtifactNameBytes = 24

// githubTestsTruncateArtifactName bounds a provider-supplied name to
// githubTestsMaxArtifactNameBytes, byte-safe (never splits a UTF-8
// codepoint) since a provider name is untrusted input.
func githubTestsTruncateArtifactName(name string) string {
	if len(name) <= githubTestsMaxArtifactNameBytes {
		return name
	}
	truncated := name[:githubTestsMaxArtifactNameBytes]
	for len(truncated) > 0 && !utf8.ValidString(truncated) {
		truncated = truncated[:len(truncated)-1]
	}
	return truncated + "…"
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
// resumed from -- was ever decoded without them, or a genuine artifacts-phase
// re-anchor replayed a page this walk had already partly counted. Skipping
// the gate on UNKNOWN is deliberate: a walk spanning the deploy, or one whose
// counters a replay can no longer be trusted to represent, is then bounded
// and self-healing rather than a false failure.
//
// Deliberately does NOT record the RecordAllArtifactsUnreadable metric: that
// happens in providerunit.Handler.observeAllArtifactsUnreadable, ONLY after
// the durable Fail transition succeeds. Recording it here, before the error
// even leaves this route, would double-count if Repository.Fail itself
// errors and a later attempt re-detects the identical condition (CHAOS-4185
// codex round 1) -- the same reasoning providerunit's
// observeTerminalWithCommittedRows already documents for CHAOS-4130's row
// counter.
func githubTestsCheckAllArtifactsUnreadable(cursor githubTestsChunkCursor, claim Claim) error {
	if cursor.ArchivesSeen == nil || cursor.ArchivesUnreadable == nil {
		return nil
	}
	seen, unreadable := *cursor.ArchivesSeen, *cursor.ArchivesUnreadable
	if seen < githubTestsAllArtifactsUnreadableFloor || unreadable != seen {
		return nil
	}
	attrs := []any{
		"provider", claim.Provider, "dataset", claim.Dataset,
		"org", claim.OrgID, "sync_run_id", claim.SyncRunID, "unit", claim.ID,
		"repository", cursor.Repo, "seen", seen, "unreadable", unreadable,
	}
	// This failure path returns BEFORE githubTestsLogArtifactSkipSummary ever
	// runs (codex review round 6, P2), so the run/artifact ids and cap an
	// operator needs to find the exact unreadable artifacts would otherwise
	// never reach a log line at all -- the durable SkippedArtifacts markers
	// still carry them, but nothing here pointed at them.
	if sample := githubTestsSkippedArtifactLogSample(cursor.SkippedArtifacts); len(sample) > 0 {
		attrs = append(attrs, "skipped_sample", sample)
	}
	slog.Error("provider unit failing: every observed cicd artifact was unreadable", attrs...)
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
	// CHAOS-4588: this used to slog.Warn once per artifact here -- for a repo
	// whose runs mix many non-report artifacts, that fired ~14 times per unit
	// attempt at ~300ms intervals, a log storm with no per-line information
	// an operator could act on beyond "one more". The run id/artifact id an
	// operator actually needs for backfill still lands in the bounded durable
	// marker (GitHubTestsSkippedArtifact, appended by the caller just below
	// this return) and in the dev_health_provider_artifact_skipped_total{cause}
	// counter (RecordArtifactSkipped above) -- both unchanged. What is gone is
	// ONLY the per-artifact log line; it is replaced by one summary line per
	// unit at finalization (githubTestsLogArtifactSkipSummary), built from
	// this same accumulated `incomplete` slice, once the whole walk's counts
	// by cause are known.
	return incomplete
}

// githubTestsLogArtifactSkipSummary emits AT MOST ONE structured log line per
// unit attempt, replacing the per-artifact "provider artifact skipped;
// inventory continued" WARN that recordGitHubTestsSkippedArtifact used to
// emit on every call (CHAOS-4588: 297 lines/30min locally, 14 per unit
// attempt, ~300ms apart, for a repository whose CI runs mix report artifacts
// with GitHub-generated non-report ones). Silent when nothing was skipped --
// a line on every healthy sync is noise operators learn to filter, which
// costs the counter the attention it exists to buy (same principle as
// githubProjectV2's membership_skips log). The bounded per-artifact evidence
// (GitHubTestsSkippedArtifact markers) and the
// dev_health_provider_artifact_skipped_total{cause} counter are untouched --
// this is presentation only, not a reduction in observability.
//
// Extended (CHAOS-4591 prep): also folds in the pre-download exclusion
// counters/sample from the selection seam and a small name/size/cause sample
// from the durable SkippedArtifacts markers, so the one line an operator
// already reads carries name+size+selected-or-skipped+reason for CHAOS-4591's
// admin view to reuse without a second data source.
func githubTestsLogArtifactSkipSummary(
	claim Claim, repo string, incomplete []GitHubTestsIncomplete,
	skippedArtifacts []GitHubTestsSkippedArtifact, causeOverflow map[string]bool,
	excludedSuffix, excludedPrefix int, excludedSample []string,
) {
	// Gate on an actual artifact/member disposition, not on incomplete being
	// non-empty (CHAOS-4592 codex review, on merged CHAOS-4588 code): incomplete
	// ALSO carries run-level page-budget/per-run-cap truncations
	// (run_inventory, artifact_inventory, run_jobs, run_artifacts, run_reports)
	// that never skipped a single artifact or report member -- those already
	// get their own dedicated line at the truncation site
	// (recordGitHubTestsInventoryTruncation, recordGitHubTestsPerRunTruncation).
	// Firing THIS "provider artifacts skipped this unit" line for a unit whose
	// only incompleteness was run-level claimed a skip that never happened,
	// with a misleading artifact_skip_total=0 right next to the message.
	hasArtifactDisposition := excludedSuffix > 0 || excludedPrefix > 0
	for _, observation := range incomplete {
		if observation.Component == githubTestsReportMemberComponent && observation.Count > 0 {
			hasArtifactDisposition = true
			break
		}
	}
	if !hasArtifactDisposition {
		return
	}
	// incomplete mixes several different kinds of observation under one
	// closed vocabulary (codex round 1 P2, round 2 P2): run-level
	// inventory/per-run truncations (run_inventory, run_reports,
	// run_artifacts) are not artifact skips at all; and even within
	// report_member, "malformed"/"unreadable" mean ONE MEMBER inside an
	// otherwise-valid, fully-read archive was skipped -- the artifact itself
	// was not. Only artifact_oversized/artifact_unavailable/unreadable_archive
	// mean the WHOLE artifact was skipped. artifactSkipTotal isolates exactly
	// that closed set so the headline number an operator reads first means
	// what it says; incomplete_total keeps the full closed-vocabulary count
	// available alongside it, broken down per (component, cause) in the
	// attrs loop below regardless of kind.
	artifactSkipTotal := 0
	for _, observation := range incomplete {
		if observation.Component != githubTestsReportMemberComponent {
			continue
		}
		switch observation.Cause {
		case githubTestsArtifactOversizedCause, githubTestsArtifactUnavailableCause, githubTestsUnreadableArchiveCause:
			artifactSkipTotal += observation.Count
		}
	}
	attrs := make([]any, 0, 16+2*len(incomplete))
	attrs = append(attrs,
		"provider", claim.Provider, "dataset", claim.Dataset, "unit", claim.ID,
		"repository", repo, "artifact_skip_total", artifactSkipTotal,
		"incomplete_total", githubTestsIncompleteCount(incomplete),
	)
	for _, observation := range incomplete {
		attrs = append(attrs, observation.Component+"_"+observation.Cause, observation.Count)
	}
	if excludedSuffix > 0 || excludedPrefix > 0 {
		attrs = append(attrs,
			"excluded_total", excludedSuffix+excludedPrefix,
			"excluded_"+githubTestsExclusionReasonSuffix, excludedSuffix,
			"excluded_"+githubTestsExclusionReasonPrefix, excludedPrefix,
		)
		if len(excludedSample) > 0 {
			attrs = append(attrs, "excluded_sample", excludedSample)
		}
	}
	if sample := githubTestsSkippedArtifactLogSample(skippedArtifacts); len(sample) > 0 {
		attrs = append(attrs, "skipped_sample", sample)
	}
	// Per-cause marker overflow (codex review gate round 2, P2): durably
	// tracked on the cursor (SkippedArtifactCauseOverflow) since round 2, but
	// previously observable only by inspecting completion JSON directly --
	// an operator reading this line had no way to know the bounded sample
	// above is missing entries for a cause. Sorted for deterministic output.
	if len(causeOverflow) > 0 {
		overflowedCauses := make([]string, 0, len(causeOverflow))
		for cause, overflowed := range causeOverflow {
			if overflowed && cause != githubTestsLegacyReportOverflowSentinel {
				overflowedCauses = append(overflowedCauses, cause)
			}
		}
		if len(overflowedCauses) > 0 {
			sort.Strings(overflowedCauses)
			attrs = append(attrs, "skipped_sample_cause_overflow", overflowedCauses)
		}
	}
	slog.Warn("provider artifacts skipped this unit; inventory continued", attrs...)
}

// githubTestsSkippedArtifactLogSample formats a small, bounded
// "name (cause, sizeB)" sample from the durable SkippedArtifacts marker list
// for the per-unit summary line, capped the same as
// githubTestsMaxExcludedArtifactSampleRecords -- CHAOS-4591 wants
// name/size/reason visible in the line an operator already reads, not only
// in the durable JSON.
func githubTestsSkippedArtifactLogSample(markers []GitHubTestsSkippedArtifact) []string {
	limit := githubTestsMaxExcludedArtifactSampleRecords
	if len(markers) < limit {
		limit = len(markers)
	}
	sample := make([]string, 0, limit)
	for _, marker := range markers[:limit] {
		name := marker.Name
		if name == "" {
			name = "unknown"
		}
		entry := name + " (" + marker.Cause
		if marker.SizeBytes > 0 {
			entry += ", " + strconv.FormatInt(marker.SizeBytes, 10) + "B"
			if marker.CapBytes > 0 {
				entry += "/" + strconv.FormatInt(marker.CapBytes, 10) + "B cap"
			}
		}
		// run/artifact IDs (codex review round 6, P2): the old per-artifact
		// oversized WARN this summary line replaced (CHAOS-4592) carried the
		// exact run and artifact id an operator needs as a retry/backfill
		// target; folding every cause onto one summary line must not lose
		// that identifying detail, only the per-event log volume. Both are
		// already on the durable marker (RunID/ArtifactID) -- this renders
		// what was already collected, not new data.
		if marker.RunID != "" {
			entry += ", run=" + marker.RunID
		}
		if marker.ArtifactID != "" {
			entry += ", artifact=" + marker.ArtifactID
		}
		entry += ")"
		sample = append(sample, entry)
	}
	return sample
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
// The bool return reports whether this call is a GENUINE re-anchor (the
// stored index no longer addresses an item, so the page is about to be
// re-walked from 0) as opposed to a fresh page never resumed at all. The
// artifacts phase needs this distinction: a genuine re-anchor re-downloads
// and re-counts every artifact on the page, including ones an earlier
// attempt already reflected in ArchivesSeen/ArchivesUnreadable, which can
// otherwise cross the totality floor on a truly small sample (CHAOS-4185
// codex round 1) -- see the caller in the artifacts phase below.
func githubTestsResumeStart(
	cursor githubTestsChunkCursor,
	page providerfoundation.PageVisit,
	client *providerfoundation.HTTPClient,
	claim Claim,
	phase string,
) (int, bool) {
	if cursor.NextURL != page.CursorBefore {
		return 0, false
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
		return cursor.Index, false
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
	return 0, true
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
	// A cursor already carrying aggregate overflow with no per-cause ledger
	// at all predates SkippedArtifactCauseOverflow (CHAOS-4592 codex review
	// round 3, P2) -- captured from the RAW decoded shape, BEFORE the
	// legacy-sample normalization below runs (codex review round 6, P1):
	// normalization's own trim can turn SkippedArtifactsOverflow from 0 into
	// a positive number purely as an artifact of THIS migration, which is
	// not evidence that any binary's marker-writing path for a DIFFERENT,
	// unrelated cause ever ran or overflowed. Deciding the sentinel from the
	// pre-normalization signal, then merging it with whatever
	// normalization's own precise per-cause attribution adds, keeps the two
	// provenances (genuinely legacy-ambiguous vs migration-trimmed-and-
	// therefore-precisely-known) from being conflated into one
	// blanket-covers-everything signal -- see
	// githubTestsLegacyReportOverflowSentinel and
	// normalizeLegacyGitHubTestsSkippedArtifacts.
	rawOverflow, rawCauseOverflow := cursor.SkippedArtifactsOverflow, cursor.SkippedArtifactCauseOverflow
	// A cursor written under EARLIER caps can legally carry a
	// SkippedArtifacts sample this binary's own githubTestsMaxSkippedArtifactRecords/
	// githubTestsMaxArtifactNameBytes would never have produced -- normalize
	// it to the current bounded shape BEFORE anything else touches the
	// cursor (codex review round 5, P1): a heavier legacy sample can already
	// sit near or over maxChunkCursorBytes on its own, and the very next
	// re-encode -- this attempt's own checkpoint write, unrelated to any NEW
	// skip -- would otherwise fail ErrChunkCheckpointConflict outright,
	// losing committed progress instead of degrading into
	// SkippedArtifactsOverflow the same way appendGitHubTestsSkippedArtifact
	// already does for a newly appended record.
	cursor = normalizeLegacyGitHubTestsSkippedArtifacts(cursor)
	if rawOverflow > 0 && rawCauseOverflow == nil {
		cursor.SkippedArtifactCauseOverflow = markGitHubTestsSkippedArtifactCauseOverflow(
			cursor.SkippedArtifactCauseOverflow, githubTestsLegacyReportOverflowSentinel,
		)
	}
	return cursor, nil
}

// normalizeLegacyGitHubTestsSkippedArtifacts trims a decoded SkippedArtifacts
// sample down to the CURRENT githubTestsMaxSkippedArtifactRecords count and
// re-truncates any name still over the CURRENT githubTestsMaxArtifactNameBytes
// bound, moving any trimmed records into SkippedArtifactsOverflow -- exactly
// what appendGitHubTestsSkippedArtifact already does for a newly appended
// record, applied once here for a slice a PRIOR (possibly less-bounded)
// binary version already wrote (codex review round 5, P1). A no-op for a
// cursor that already fits: same length or shorter than the current cap,
// every name already within the current bound.
func normalizeLegacyGitHubTestsSkippedArtifacts(cursor githubTestsChunkCursor) githubTestsChunkCursor {
	kept := cursor.SkippedArtifacts
	if len(kept) > githubTestsMaxSkippedArtifactRecords {
		dropped := kept[githubTestsMaxSkippedArtifactRecords:]
		cursor.SkippedArtifactsOverflow += len(dropped)
		// Mark each DROPPED record's own cause precisely (codex review round
		// 6, P1), rather than leaving this trim to be covered only by the
		// generic legacy sentinel below. A pre-CHAOS-4394 cursor can legally
		// have exactly githubTestsMaxSkippedArtifactRecords(old)=20
		// artifact_oversized markers with SkippedArtifactsOverflow==0 (never
		// exceeded the old cap) alongside an UNMARKED artifact_unavailable
		// observation (that binary never wrote markers for that cause at
		// all). Trimming those 20 down to the current cap here creates
		// overflow>0 for the FIRST time -- purely an artifact of THIS
		// migration, not evidence that any binary's marker-writing path for
		// artifact_unavailable ever ran or overflowed. If that
		// migration-only overflow were left aggregate/unattributed, the
		// legacy-sentinel fallback below would wrongly treat it as
		// generic proof covering ALL three original causes, letting the
		// watermark advance over artifact_unavailable with zero durable
		// backfill evidence for it. Attributing the overflow to the ACTUAL
		// dropped cause (artifact_oversized here) keeps artifact_unavailable
		// correctly unmarked and unmarked-uncovered.
		for _, record := range dropped {
			cursor.SkippedArtifactCauseOverflow = markGitHubTestsSkippedArtifactCauseOverflow(
				cursor.SkippedArtifactCauseOverflow, record.Cause,
			)
		}
		kept = kept[:githubTestsMaxSkippedArtifactRecords]
	}
	changed := len(kept) != len(cursor.SkippedArtifacts)
	renamed := make([]GitHubTestsSkippedArtifact, len(kept))
	for index, record := range kept {
		renamed[index] = record
		if truncated := githubTestsTruncateArtifactName(record.Name); truncated != record.Name {
			renamed[index].Name = truncated
			changed = true
		}
	}
	if changed {
		cursor.SkippedArtifacts = renamed
	}
	return cursor
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
	// Same typed-nil-to-empty-slice normalization as `incomplete` above, and
	// for the identical reason: a cursor that never skipped an oversized
	// artifact carries a nil SkippedArtifacts, which marshals to JSON null,
	// which decodeCompletionValue-style strict readers refuse (CHAOS-3940).
	skippedArtifacts := append(
		make([]GitHubTestsSkippedArtifact, 0, len(cursor.SkippedArtifacts)), cursor.SkippedArtifacts...,
	)
	// Same typed-nil-to-empty-slice normalization, same reason (CHAOS-3940
	// discipline): a cursor that never excluded a non-report artifact carries
	// a nil ExcludedArtifactSample.
	excludedArtifactSample := append(
		make([]string, 0, len(cursor.ExcludedArtifactSample)), cursor.ExcludedArtifactSample...,
	)
	// Same typed-nil-to-empty-map normalization, same CHAOS-3940 reason: a
	// cursor that never overflowed the skipped-artifact sample carries a nil
	// SkippedArtifactCauseOverflow.
	causeOverflow := make(map[string]bool, len(cursor.SkippedArtifactCauseOverflow))
	for cause, overflowed := range cursor.SkippedArtifactCauseOverflow {
		causeOverflow[cause] = overflowed
	}
	// Same typed-nil-to-empty-map normalization, same CHAOS-3940 reason: a
	// cursor that never recorded a report_member skip at all carries a nil
	// SkippedArtifactCauseCount.
	causeCount := make(map[string]int, len(cursor.SkippedArtifactCauseCount))
	for cause, count := range cursor.SkippedArtifactCauseCount {
		causeCount[cause] = count
	}
	githubTestsLogArtifactSkipSummary(
		claim, cursor.Repo, incomplete, skippedArtifacts, causeOverflow,
		cursor.ExcludedNonReportSuffix, cursor.ExcludedNonReportPrefix, cursor.ExcludedArtifactSample,
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
			// Durable per-artifact marker for oversized skips (CHAOS-4315):
			// the run id/artifact id/size/cap an operator needs to find the
			// exact GitHub artifact behind an artifact_oversized count,
			// bounded per githubTestsMaxSkippedArtifactRecords with overflow
			// tracked separately rather than silently dropped.
			"skipped_artifacts":          skippedArtifacts,
			"skipped_artifacts_overflow": cursor.SkippedArtifactsOverflow,
			// Per-cause overflow ledger (CHAOS-4592 codex review round 2,
			// P1): which report_member causes had a marker dropped by the
			// cap, so githubTestsReportMemberSkippedWithoutDurableMarker can
			// tell overflow apart per cause instead of trusting the single
			// aggregate count above for every cause uniformly.
			"skipped_artifact_cause_overflow": causeOverflow,
			// Exact per-cause skip count (codex review gate round 2, P1):
			// sample-cap-independent, so githubTestsReportMemberSkippedWithoutDurableMarker
			// can require the FULL observed Incomplete count for a cause be
			// accounted for, not just cause presence -- see
			// githubTestsChunkCursor.SkippedArtifactCauseCount.
			"skipped_artifact_cause_count": causeCount,
			// Pre-download exclusions (CHAOS-4588/CHAOS-4591): artifacts the
			// selection seam never even requested. Not part of "incomplete" --
			// nothing readable was lost -- surfaced separately so CHAOS-4591's
			// admin view has the same name/reason data the per-unit log line
			// carries, durably.
			"excluded_non_report_suffix": cursor.ExcludedNonReportSuffix,
			"excluded_non_report_prefix": cursor.ExcludedNonReportPrefix,
			"excluded_artifact_sample":   excludedArtifactSample,
		},
		// Only a WINDOW-BLOCKING observation withholds the watermark. An
		// inventory page cap leaves the old end of a newest-first window
		// unreached, so advancing would make that gap permanent (CHAOS-2587).
		// A per-run truncation walked the whole window and dropped items only
		// inside runs it already committed, so withholding there buys no
		// recovered coverage -- the cap is deterministic -- and pins since_at
		// forever, which is the CHAOS-4142 defect.
		Watermark: func() *time.Time {
			// githubTestsBlocksWatermark is the single source of truth for
			// this decision -- validateGitHubTestsCompletion (the production
			// comparator) must reach the identical verdict from the same
			// evidence, or a legacy cursor that correctly withholds here
			// fails comparator validation instead of completing safely
			// (codex review round 3, P1).
			if githubTestsBlocksWatermark(incomplete, skippedArtifacts, cursor.SkippedArtifactsOverflow, causeOverflow, causeCount) {
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
		if err := githubTestsCheckAllArtifactsUnreadable(cursor, claim); err != nil {
			return err
		}
		batch, batchErr := githubTestsFinalMetadataBatch(claim, cursor)
		if batchErr != nil {
			return batchErr
		}
		// CHAOS-4394 telemetry (dev_health_cicd_partial_success_total) is NOT
		// recorded here. This closure runs once per attempt, including attempts
		// whose completion later fails to commit (codex review round 1, P2):
		// emit() only stages the batch for the executor to persist, and the
		// durable transition happens later in PostgresRepository.Complete. A
		// counter fired here would over-count a unit that gets recollected
		// after a completion failure, and under-fire nothing on a genuine
		// success -- both wrong. See
		// internal/jobs/providerunit.Handler's post-Complete success branch,
		// which fires this exactly once per unit, the same discipline
		// observeTerminalWithCommittedRows/observeAllArtifactsUnreadable use.
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
		start, _ := githubTestsResumeStart(cursor, page, client, claim, githubTestsRunsPhase)
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
			start, reanchored := githubTestsResumeStart(cursor, page, client, claim, githubTestsArtifactsPhase)
			if reanchored {
				// A genuine re-anchor re-walks this WHOLE page from index 0,
				// which re-downloads and re-counts every artifact on it --
				// including ones an earlier attempt already reflected in
				// these counters. That can cross the totality floor on a
				// truly small real sample (e.g. a genuine 1-artifact repo
				// whose page shrank under a resume): the SAME artifact would
				// be counted twice, satisfying seen>=2 with only one
				// distinct observation (CHAOS-4185 codex round 1).
				//
				// Poisoning to UNKNOWN here is the same trade-off already
				// accepted for a legacy pre-deploy cursor: bounded and
				// self-healing (the very next fresh walk starts with known
				// counters again), rather than risk a false positive from
				// state a replay can no longer be trusted to represent.
				cursor.ArchivesSeen, cursor.ArchivesUnreadable = nil, nil
				// The exclusion bookkeeping (CHAOS-4588/CHAOS-4591) is
				// DELIBERATELY NOT reset here (codex round 2, P2 -- round 1
				// had this wrong). ArchivesSeen/Unreadable above are
				// per-WALK gate inputs where "unknown" is a safe default a
				// fresh pass can restore; ExcludedNonReportSuffix/Prefix and
				// ExcludedArtifactSample are a cursor-wide running total
				// across every page this walk has already completed, not
				// just the re-anchored one. Zeroing them here would discard
				// every EARLIER page's legitimate exclusion count along with
				// the replayed page's, silently undercounting the final
				// result and summary. Leaving them alone means the replayed
				// prefix's exclusions can be double-counted on the rare
				// re-anchor -- a bounded, purely cosmetic over-count (these
				// never gate the watermark or Incomplete) -- which is a far
				// safer failure mode than erasing real history.
			}
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
					// Decode-then-filter BEFORE the per-run cap: a name-filtered
					// non-report artifact (CHAOS-4588) must not consume the
					// cap ahead of a real report artifact in the same run.
					artifactItems := make([]githubTestsArtifactPayload, 0, len(artPage.Items))
					for _, artifactRaw := range artPage.Items {
						var artifact githubTestsArtifactPayload
						decoder := json.NewDecoder(strings.NewReader(string(artifactRaw)))
						decoder.UseNumber()
						if decoder.Decode(&artifact) != nil || artifact.ID == "" {
							return providerfoundation.ErrNormalizationInvalid
						}
						if selected, reason := githubTestsArtifactSelectionSeam(artifact.Name); !selected {
							switch reason {
							case githubTestsExclusionReasonSuffix:
								cursor.ExcludedNonReportSuffix++
							case githubTestsExclusionReasonPrefix:
								cursor.ExcludedNonReportPrefix++
							}
							if len(cursor.ExcludedArtifactSample) < githubTestsMaxExcludedArtifactSampleRecords {
								// Bounded the same way as GitHubTestsSkippedArtifact.Name
								// (CHAOS-4588/CHAOS-4591, codex round 1, P1): a
								// provider name is unbounded, and this sample
								// shares the same cursor byte budget.
								cursor.ExcludedArtifactSample = append(
									cursor.ExcludedArtifactSample,
									githubTestsTruncateArtifactName(artifact.Name)+" ("+reason+")",
								)
							}
							continue
						}
						artifactItems = append(artifactItems, artifact)
					}
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
					for _, artifact := range artifactItems {
						if artifact.Expired {
							continue
						}
						archive, used, notFound, downloadErr := downloadGitHubTestsArtifact(ctx, client, root, string(artifact.ID))
						cursor.Requests += used
						if downloadErr != nil {
							// An artifact whose bytes could never be
							// downloaded is provider data, not a fault of
							// this unit. Skip it exactly as an unreadable
							// container is skipped below, and keep walking:
							// one bad artifact must not cost the healthy ones
							// or the whole unit (CHAOS-4191, extending
							// CHAOS-4177). Counted as SEEN here (not before
							// the call): a fetch that never even reached this
							// branch commits nothing, so nothing should be
							// attributed to the totality denominator either.
							if errors.Is(downloadErr, ErrGitHubTestsArtifactUnavailable) {
								cursor.Incomplete = recordGitHubTestsSkippedArtifact(
									cursor.Incomplete, client, claim, cursor.Repo, pipeline.RunID,
									githubTestsArtifactUnavailableCause,
								)
								// Durable per-artifact marker (CHAOS-4394, extending
								// CHAOS-4315's oversized-only marker to every
								// report_member cause that now advances the
								// watermark): an operator needs the run/artifact id
								// to target a backfill regardless of which of the
								// three causes skipped it.
								cursor.SkippedArtifacts, cursor.SkippedArtifactsOverflow, cursor.SkippedArtifactCauseOverflow, cursor.SkippedArtifactCauseCount = appendGitHubTestsSkippedArtifact(
									cursor.SkippedArtifacts, cursor.SkippedArtifactsOverflow, cursor.SkippedArtifactCauseOverflow, cursor.SkippedArtifactCauseCount,
									GitHubTestsSkippedArtifact{
										RunID: pipeline.RunID, ArtifactID: string(artifact.ID), Name: artifact.Name,
										Cause: githubTestsArtifactUnavailableCause,
									},
								)
								cursor.ArchivesSeen = bumpGitHubTestsArchiveCounter(cursor.ArchivesSeen)
								cursor.ArchivesUnreadable = bumpGitHubTestsArchiveCounter(cursor.ArchivesUnreadable)
								continue
							}
							// An artifact whose body exceeded
							// githubTestsMaxDownloadSize is provider data
							// too: the same repository produces the same
							// oversized bytes on every future attempt, so
							// failing the whole unit here pinned
							// sync_watermarks forever on this one run
							// instead of just losing this one artifact's
							// reports (CHAOS-4315 -- reverses the prior
							// UNIT-level-failure disposition documented on
							// ErrGitHubTestsArtifactOversized before this
							// fix). Skipped the same way as an unavailable
							// artifact, with its own cause and its own
							// counter reason so an operator can tell the two
							// apart. The artifact id and the observed
							// size/cap (carried in downloadErr, a
							// *githubTestsArtifactOversizedError) go to a
							// bounded durable marker (cursor.SkippedArtifacts,
							// just below) AND the log line -- both are
							// provider-supplied and unbounded, which is why
							// neither belongs on GitHubTestsIncomplete's own
							// closed Component/Cause/Count shape.
							if errors.Is(downloadErr, ErrGitHubTestsArtifactOversized) {
								cursor.Incomplete = recordGitHubTestsSkippedArtifact(
									cursor.Incomplete, client, claim, cursor.Repo, pipeline.RunID,
									githubTestsArtifactOversizedCause,
								)
								var sizeErr *githubTestsArtifactOversizedError
								if errors.As(downloadErr, &sizeErr) {
									cursor.SkippedArtifacts, cursor.SkippedArtifactsOverflow, cursor.SkippedArtifactCauseOverflow, cursor.SkippedArtifactCauseCount = appendGitHubTestsSkippedArtifact(
										cursor.SkippedArtifacts, cursor.SkippedArtifactsOverflow, cursor.SkippedArtifactCauseOverflow, cursor.SkippedArtifactCauseCount,
										GitHubTestsSkippedArtifact{
											RunID: pipeline.RunID, ArtifactID: string(artifact.ID), Name: artifact.Name,
											Cause:     githubTestsArtifactOversizedCause,
											SizeBytes: sizeErr.SizeBytes, CapBytes: sizeErr.CapBytes,
										},
									)
								}
								// No per-artifact log line here (CHAOS-4592
								// codex review, folding CHAOS-4588's already-
								// established contract onto this one branch it
								// missed): unavailable/unreadable_archive
								// skips a few lines above and below stopped
								// logging per event once githubTestsLogArtifactSkipSummary
								// existed to report the same evidence once per
								// unit -- this oversized branch kept its own
								// direct slog.Warn, so a unit with even one
								// oversized artifact got BOTH a per-artifact
								// line here AND the summary line at
								// finalization, violating the at-most-one-
								// line-per-unit contract and duplicating the
								// summary's skipped_sample. The size/cap this
								// line used to carry are already durable on
								// the GitHubTestsSkippedArtifact marker just
								// above (SizeBytes/CapBytes) and rendered into
								// the summary's skipped_sample
								// (githubTestsSkippedArtifactLogSample).
								cursor.ArchivesSeen = bumpGitHubTestsArchiveCounter(cursor.ArchivesSeen)
								cursor.ArchivesUnreadable = bumpGitHubTestsArchiveCounter(cursor.ArchivesUnreadable)
								continue
							}
							return downloadErr
						}
						if notFound {
							// A ROUTINE provider-side disappearance: the
							// artifact expired or was deleted between listing
							// and download (GitHub answers 404/410 for this,
							// a documented, ordinary outcome -- see
							// downloadGitHubTestsArtifact's doc comment).
							// Provider-side ephemeral state is not evidence
							// about whether OUR channel to read artifacts is
							// broken, so it is excluded from totality
							// accounting entirely -- neither seen nor
							// unreadable -- matching this route's
							// pre-CHAOS-4185 disposition for this exact case.
							// Without this exclusion, two artifacts that
							// simply expired between listing and download
							// would satisfy the totality floor and
							// terminalize a healthy unit (CHAOS-4185 codex
							// round 3).
							continue
						}
						// Counted as SEEN the moment a real read is attempted
						// AND the artifact is confirmed present -- so every
						// downstream disposition (healthy, unreadable, or a
						// fatal parse bound) is reflected in the denominator
						// the totality gate divides by, while a routine
						// not-found above never was (CHAOS-4185).
						cursor.ArchivesSeen = bumpGitHubTestsArchiveCounter(cursor.ArchivesSeen)
						if len(archive) == 0 {
							// A 2xx download with a truly empty body is not a
							// legitimate GitHub response (a real artifact
							// either has bytes or the download 404s/410s,
							// handled above); it is the same "answers every
							// request with an empty document" edge condition
							// the totality gate exists to catch, just without
							// even a malformed payload to reject. Recording
							// it as unreadable -- exactly like a container
							// that downloaded but would not open -- closes
							// the gap where a broken proxy returning empty
							// bodies for every artifact would otherwise
							// finalize the unit as healthy with zero report
							// rows (CHAOS-4185 codex round 2).
							cursor.Incomplete = recordGitHubTestsSkippedArtifact(
								cursor.Incomplete, client, claim, cursor.Repo, pipeline.RunID,
								githubTestsUnreadableArchiveCause,
							)
							// Durable per-artifact marker (CHAOS-4394): see the
							// identical comment on the artifact_unavailable branch
							// above.
							cursor.SkippedArtifacts, cursor.SkippedArtifactsOverflow, cursor.SkippedArtifactCauseOverflow, cursor.SkippedArtifactCauseCount = appendGitHubTestsSkippedArtifact(
								cursor.SkippedArtifacts, cursor.SkippedArtifactsOverflow, cursor.SkippedArtifactCauseOverflow, cursor.SkippedArtifactCauseCount,
								GitHubTestsSkippedArtifact{
									RunID: pipeline.RunID, ArtifactID: string(artifact.ID), Name: artifact.Name,
									Cause: githubTestsUnreadableArchiveCause,
								},
							)
							cursor.ArchivesUnreadable = bumpGitHubTestsArchiveCounter(cursor.ArchivesUnreadable)
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
								// Durable per-artifact marker (CHAOS-4394): see the
								// identical comment on the artifact_unavailable
								// branch above.
								cursor.SkippedArtifacts, cursor.SkippedArtifactsOverflow, cursor.SkippedArtifactCauseOverflow, cursor.SkippedArtifactCauseCount = appendGitHubTestsSkippedArtifact(
									cursor.SkippedArtifacts, cursor.SkippedArtifactsOverflow, cursor.SkippedArtifactCauseOverflow, cursor.SkippedArtifactCauseCount,
									GitHubTestsSkippedArtifact{
										RunID: pipeline.RunID, ArtifactID: string(artifact.ID), Name: artifact.Name,
										Cause: githubTestsUnreadableArchiveCause,
									},
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
						// Durable per-report markers (CHAOS-4592): malformed/unreadable
						// report members now advance the watermark like the three
						// whole-artifact causes, and githubTestsReportMemberSkippedWithoutDurableMarker
						// requires one of these for that cause to exist before it will.
						for _, marker := range rows.SkippedArtifacts {
							cursor.SkippedArtifacts, cursor.SkippedArtifactsOverflow, cursor.SkippedArtifactCauseOverflow, cursor.SkippedArtifactCauseCount = appendGitHubTestsSkippedArtifact(
								cursor.SkippedArtifacts, cursor.SkippedArtifactsOverflow, cursor.SkippedArtifactCauseOverflow, cursor.SkippedArtifactCauseCount, marker,
							)
						}
						client.Metrics.RecordDuplicateTestCase(claim.Provider, claim.Dataset, rows.DuplicateCases)
						if rows.DuplicateCases > 0 {
							slog.Info(
								"within-suite duplicate test-case names disambiguated with an ordinal suffix",
								"provider", claim.Provider, "dataset", claim.Dataset, "unit", claim.ID,
								"repository", cursor.Repo, "run", pipeline.RunID, "count", rows.DuplicateCases,
							)
						}
						client.Metrics.RecordDuplicateTestSuite(claim.Provider, claim.Dataset, rows.DuplicateSuites)
						if rows.DuplicateSuites > 0 {
							slog.Info(
								"sibling suite collision resolved: same-named suite objects disambiguated with an ordinal suffix",
								"provider", claim.Provider, "dataset", claim.Dataset, "unit", claim.ID,
								"repository", cursor.Repo, "run", pipeline.RunID, "count", rows.DuplicateSuites,
							)
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
