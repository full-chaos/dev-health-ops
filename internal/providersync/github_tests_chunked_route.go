package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// githubTestsChunkCountingDoer observes actual wire attempts, including
// transport failures and retries the wrapped HTTPClient makes internally --
// unlike a decoded-page or per-logical-call tally, it increments once per
// Doer.Do call regardless of whether that call ever produced a usable
// response. It also sees every DoUnauthenticated call (artifact downloads),
// since HTTPClient.DoUnauthenticated shares the same underlying Doer. It is
// the single source of truth for this chunk invocation's own contribution
// to cursor.Requests.
type githubTestsChunkCountingDoer struct {
	delegate providerfoundation.HTTPDoer
	attempts *int
	observed *githubTestsRateObserver
}

func (doer githubTestsChunkCountingDoer) Do(request *http.Request) (*http.Response, error) {
	*doer.attempts++
	response, err := doer.delegate.Do(request)
	if response != nil && doer.observed != nil {
		doer.observed.observe(response.Header)
	}
	return response, err
}

// githubTestsMaxJobsPerRun bounds the jobs committed for ONE run (a hard bound). It is not the report-row bound: that is
// githubTestsMaxReportRowsPerRun (CHAOS-13794; before it, the same 500 also capped the report rows and dropped whole artifacts).
const githubTestsMaxJobsPerRun = 500

// githubTestsMaxReportRowsPerRun bounds the suites+cases+coverage rows admitted for ONE run, and
// githubTestsMaxReportPayloadBytesPerDestination the JSON bytes of any one destination's rows (CHAOS-13794). They sit under the
// effect contract that one run's rows go through as ONE batch per destination before the split into chunks (maxEffectRows,
// maxEffectPayloadBytes): a run over either is a failing unit, not a truncated one, so the bound is the contract ceiling, not a
// guess. Sized from prod: the largest run is 71,896 rows (7b9583ee), the web run with vitest is about 17,300. The payload bound
// keeps the same 9/10 headroom as the blame route.
const (
	githubTestsMaxReportRowsPerRun                 = maxEffectRows
	githubTestsMaxReportPayloadBytesPerDestination = 57 << 20
)

// githubTestsPerRunPerPage is the per_page this route asks for on its per-run
// collections. It is a named constant rather than a literal because the
// configuration check below multiplies it by the page budget: if the request
// and the check could drift apart, the check would validate an arithmetic
// relationship the requests do not actually have (CHAOS-4142, codex round 2).
const githubTestsPerRunPerPage = 100

type githubTestsOpenSkipEvent struct {
	RunID  string `json:"r"`
	Reason string `json:"why"`
	Status int    `json:"st,omitempty"`
	Route  string `json:"rt,omitempty"`
}

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
	// RangeHi, RangeItems and RangeOldest peel the listing of the active phase
	// (runs, then artifacts; never both at once) past GitHub's cap of 1,000
	// results per created search. RangeHi is the end of the current range once
	// a saturated range moved it (empty: the window end), RangeItems the runs
	// consumed from the current range, RangeOldest the oldest created time seen
	// in it. A range that consumed the cap ends at its oldest time.
	RangeHi     string `json:"rh,omitempty"`
	RangeItems  int    `json:"ri,omitempty"`
	RangeOldest string `json:"ro,omitempty"`
	// RangeOldestItems counts the runs of the current runs-phase range created in the second RangeOldest: a range whose
	// cap of runs all share that second is stuck at once, as any range ending at it lists the same runs (CHAOS-14321).
	RangeOldestItems int `json:"rq,omitempty"`
	// LateRuns marks the deep range of the runs phase (the late re-run relist
	// of the claim that holds a UTC midnight), walked after the runs range;
	// LateRunPages is its cumulative page count against its own budget.
	LateRuns     bool `json:"lr,omitempty"`
	LateRunPages int  `json:"lrp,omitempty"`
	// LateRunsBudget and LateRunsSaturated record that the deep range stopped
	// at its page budget (or the walk bound) or at a saturated second; the
	// unit result carries them as the late_rerun_runs observations.
	LateRunsBudget    bool `json:"lrb,omitempty"`
	LateRunsSaturated bool `json:"lrs,omitempty"`
	// LateRecords counts the records the deep range wrote (githubTestsWindowRecords).
	LateRecords int `json:"lrr,omitempty"`
	// The open phase re-reads, by id, stored pipeline rows that are still open
	// and older than the runs listing reaches. OpenAfterStarted/OpenAfterRunID
	// are the keyset position (rows finalized by earlier chunks drop out of the
	// reader's answer, so an index would shift), OpenCap the cap fixed at the
	// phase start, OpenDone the runs re-read, OpenCapReached that the cap, not
	// the end of the open rows, stopped the phase.
	OpenAfterStarted string `json:"oas,omitempty"`
	OpenAfterRunID   string `json:"oar,omitempty"`
	OpenCap          int    `json:"oc,omitempty"`
	OpenDone         int    `json:"od,omitempty"`
	OpenCapReached   bool   `json:"ocr,omitempty"`
	// OpenSkipped counts runs whose re-read failed with an error of that run
	// alone: the keyset moves past them, they use no cap slot, and the phase
	// stops when they reach the cap (OpenSkipLimitHit). OpenTransientRun is
	// the transient skips since the last run re-read (the breaker).
	OpenSkipped      int  `json:"osk,omitempty"`
	OpenTransientRun int  `json:"otr,omitempty"`
	OpenSkipLimitHit bool `json:"osl,omitempty"`
	// OpenSkipEvent is the skip of this cursor's own emission, carried so the
	// skip is recorded (ObserveCommittedChunk) when the executor confirms that
	// chunk committed, also by recovery. Later cursors never carry it.
	OpenSkipEvent *githubTestsOpenSkipEvent `json:"ose,omitempty"`
	// OpenFloorHit and OpenFloorRemaining record that the rate-limit governor
	// stopped the open phase: remaining requests of the installation at that
	// time (-1 when the response carried no header).
	OpenFloorHit       bool  `json:"ofh,omitempty"`
	OpenFloorRemaining int64 `json:"ofr,omitempty"`
	// SaturatedRuns and SaturatedArtifacts record, in two bytes of cursor each,
	// that the runs or artifacts listing could not be walked past GitHub's cap
	// of results. The unit result carries them as the long closed-vocabulary
	// observations (run_inventory_saturated, artifact_inventory_saturated).
	SaturatedRuns      bool `json:"sr,omitempty"`
	SaturatedArtifacts bool `json:"sa,omitempty"`
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
	// ArtifactsExcludedNotFound counts routine 404/410 download-time
	// disappearances (see the notFound branch below), which -- like an
	// oversized skip -- are excluded from the ArchivesSeen/ArchivesUnreadable
	// totality gate entirely. A plain additive counter, not a pointer like
	// ArchivesSeen/Unreadable: it never gates anything, so a cursor that
	// predates this field decoding it as zero is not the CHAOS-4177-class
	// false-positive hazard bumpGitHubTestsArchiveCounter's doc comment
	// describes -- it only ever under-reports one telemetry field on a
	// resumed walk, never mis-terminalizes a unit.
	ArtifactsExcludedNotFound int `json:"artifacts_excluded_not_found,omitempty"`
	// ActiveCutKeys and CarriedCutKeys (the runs or the artifacts phase; cleared when either ends) keep a peeled range from
	// reading again the runs the previous range read whole at its cut second (github_tests_cut_runs.go). A cursor without
	// them skips nothing.
	ActiveCutKeys  string `json:"ak,omitempty"`
	CarriedCutKeys string `json:"ck,omitempty"`
	// Relist is the state of the late re-run re-list (github_tests_late_rerun_reports.go): walking its deep range, or why it
	// stopped before its oldest runs. The deep range starts ArtifactPages again from zero: the regular range is done by then.
	Relist int `json:"rl,omitempty"`
	// AnchorID is the id (1-20 digits) of the last run the active runs or artifacts listing listed, read or not. A resume
	// checks that the run before its stored index is still this run (githubTestsResumeStart): a run deleted before it moves
	// it, and the listing is then sought again by created time (githubTestsReseek) instead of resumed by position.
	AnchorID string `json:"ra,omitempty"`
	// ReseekPages is the count of free pages of a re-seek search (githubTestsReseek): the pages its moved search had entered, at
	// least one, so the re-seek reads again what they charged without charging it twice (CHAOS-14385). A page of the active
	// range is free while PagesEntered is below it. It is zero outside a re-seek range: every range end clears it.
	ReseekPages int `json:"rp,omitempty"`
	// PagesEntered counts the pages the active range entered (charged or free; a resume inside a page re-enters none). GitHub
	// may serve fewer runs than per_page on a page that is not the last, so the count is kept, not derived from RangeItems.
	PagesEntered int `json:"pe,omitempty"`
	// RecordCredit is the records of the runs a re-seek range read in its boundary second (its RangeHi): they are, for the most
	// part, runs read again, and the record bounds do not charge them. It stays for the whole walk. A credit earned in the deep
	// range is also taken off LateRecords, so the window records less this credit leave the deep range out as before.
	RecordCredit int `json:"rc,omitempty"`
	// DeletionProven is 1 once a resume of this walk proved a run deleted before its anchor (githubTestsResumeDeleted), with
	// or without credit: a deep stop at the record bound then holds the watermark (githubTestsHoldDeepStopAfterDeletion).
	// It stays for the whole walk; an older binary ignores it and behaves as main (CHAOS-14434).
	DeletionProven int `json:"d,omitempty"`
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
	// One marker per (run, artifact, cause): a second skip of the same artifact and cause (another member of it, or a
	// replayed read) is already located by the first marker, and a duplicate would push other artifacts out of the sample.
	if githubTestsSkippedMarkerRecorded(records, record.RunID, record.ArtifactID, record.Cause) {
		return records, overflow, causeOverflow, causeCount
	}
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
	// RunID/ArtifactID are decoded via json.Number (CHAOS-4592 codex review
	// gate round 5, P2): a JSON number token is syntactically valid at any
	// length, so nothing upstream bounds it the way Name was already bounded
	// above -- a provider response (malicious, buggy, or simply corrupted in
	// transit) carrying an oversized numeric ID alongside a malformed report
	// member would make the very next checkpoint exceed maxChunkCursorBytes
	// and fail ErrChunkCheckpointConflict, repeatedly abandoning the current
	// chunk's progress instead of degrading into SkippedArtifactsOverflow
	// the way this whole mechanism exists to do. Real GitHub run/artifact
	// IDs are int64 (<=19 decimal digits); githubTestsMaxArtifactIDBytes
	// gives comfortable headroom over that without reopening the same
	// unbounded-budget hazard Name's own fix (round 1, P1) closed.
	record.RunID = githubTestsTruncateArtifactID(record.RunID)
	record.ArtifactID = githubTestsTruncateArtifactID(record.ArtifactID)
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

// githubTestsMaxArtifactIDBytes bounds GitHubTestsSkippedArtifact.RunID and
// .ArtifactID (codex review gate round 5, P2). Both are decoded via
// json.Number, which accepts any syntactically valid JSON number token
// regardless of length -- unlike Name, nothing upstream bounded them before
// this. Real GitHub run/artifact IDs are int64 (<=19 decimal digits); 24
// gives 5 bytes of headroom over that -- the same margin
// githubTestsMaxArtifactNameBytes uses for its own realistic maximum --
// without being large enough to reopen the unbounded-budget hazard Name's
// own fix closed (up to githubTestsMaxSkippedArtifactRecords of these live
// in the same maxChunkCursorBytes budget as everything else on the cursor;
// TWO ID fields per record here, versus Name's one, is why this stays
// exactly at Name's bound rather than above it).
const githubTestsMaxArtifactIDBytes = 24

// githubTestsTruncateArtifactID bounds a provider-supplied RunID/ArtifactID
// to githubTestsMaxArtifactIDBytes, byte-safe for the same reason
// githubTestsTruncateArtifactName is: both are untrusted provider input,
// even though in practice a json.Number-decoded value is ASCII-only.
func githubTestsTruncateArtifactID(id string) string {
	if len(id) <= githubTestsMaxArtifactIDBytes {
		return id
	}
	truncated := id[:githubTestsMaxArtifactIDBytes]
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
		"repository", logging.ProviderText(cursor.Repo), "seen", seen, "unreadable", unreadable,
	}
	// This failure path returns BEFORE githubTestsLogArtifactSkipSummary ever
	// runs (codex review round 6, P2), so the run/artifact ids and cap an
	// operator needs to find the exact unreadable artifacts would otherwise
	// never reach a log line at all -- the durable SkippedArtifacts markers
	// still carry them, but nothing here pointed at them. Shares
	// githubTestsSkippedArtifactMarkerAttrs with that function (codex review
	// gate round 5, P2) rather than keeping its own copy, which is exactly
	// what let this path silently miss "skipped_sample_cause_overflow" when
	// round 2 added that field to the OTHER copy.
	attrs = append(attrs, githubTestsSkippedArtifactMarkerAttrs(cursor.SkippedArtifacts, cursor.SkippedArtifactCauseOverflow)...)
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

// githubTestsListingAllowance is the visitor allowance of one listing range
// under a cumulative page budget. A page is charged when a walk enters it; the
// free pages (githubTestsFreePages) are fetched on top of the budget, even when
// it is spent.
func githubTestsListingAllowance(budget, spent, free int) (int, error) {
	if free == 0 {
		return remainingPageBudget(budget, spent)
	}
	if budget < 1 {
		return 0, ErrInvalidConfiguration
	}
	return max(0, budget-spent) + free, nil
}

// githubTestsFreePages is the pages a visit of the active range may fetch without charging the page budget: the page a
// resume inside a page re-fetches (it was charged when the walk entered it), and the re-seek's free pages not entered yet.
func githubTestsFreePages(cursor githubTestsChunkCursor) int {
	free := max(0, cursor.ReseekPages-cursor.PagesEntered)
	if cursor.NextURL != "" && cursor.Index > 0 {
		free++
	}
	return free
}

// githubTestsRestorePagesEntered gives a cursor of an older binary (inside a range with runs listed, but no PagesEntered:
// this binary counts a page before it lists a run of it) the pages its range entered, from the page number GitHub's Link
// URL carries: a cursor inside page N entered N pages, a page-end cursor naming page N+1 entered N. Short pages make
// RangeItems/100 too low, so it is only the floor, and the count when the URL has no page number (the first page).
func githubTestsRestorePagesEntered(cursor githubTestsChunkCursor) githubTestsChunkCursor {
	if cursor.PagesEntered != 0 || cursor.RangeItems == 0 ||
		cursor.Phase != githubTestsRunsPhase && cursor.Phase != githubTestsArtifactsPhase {
		return cursor
	}
	entered := (cursor.RangeItems + nativePerPage - 1) / nativePerPage
	if parsed, err := url.Parse(cursor.NextURL); err == nil && cursor.NextURL != "" {
		query := parsed.Query()
		if page, atoiErr := strconv.Atoi(query.Get("page")); atoiErr == nil && page >= 1 && len(query["page"]) == 1 {
			if cursor.Index == 0 {
				page--
			}
			entered = max(entered, page)
		}
	}
	cursor.PagesEntered = entered
	return cursor
}

// githubTestsEnterPage enters the next page of the active range (at index 0): it counts the page and charges it to the page
// budget unless it is a free page of the range's re-seek.
func githubTestsEnterPage(cursor githubTestsChunkCursor, charge func(*githubTestsChunkCursor)) githubTestsChunkCursor {
	if cursor.PagesEntered >= cursor.ReseekPages {
		charge(&cursor)
	}
	cursor.PagesEntered++
	return cursor
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

// githubTestsMaxWalkRecords bounds the rows one chunked walk writes, summed over every phase and every attempt
// (githubTestsWalkRecords). The page budgets bound the runs a walk lists, not the rows: one run's reports go up to
// maxEffectRows, which the executor splits into 100-row chunks, so a walk of a busy repository could prepare chunks for many
// hours while it held its cost-class slot. The value is 2x the largest successful backfill walk on prod, Nov 2026 (2,002,076
// records; the largest incremental walk was 593,667), rounded up to the next 100,000. Two prod backfill walks that ran for 13+
// hours without an end held about 8,000,000.
const githubTestsMaxWalkRecords = 4_100_000

// errGitHubTestsWalkBound stops a listing visit at the run where the walk reached githubTestsMaxWalkRecords.
var errGitHubTestsWalkBound = errors.New("github tests walk record bound reached")

// githubTestsWalkRecords is the rows the walk has written so far. Every addend is a cumulative cursor counter, so a stored
// cursor of an earlier attempt or an earlier release already carries the total.
func githubTestsWalkRecords(cursor githubTestsChunkCursor) int {
	return cursor.Pipelines + cursor.Jobs + cursor.Acceptance + cursor.Suites + cursor.Cases + cursor.Coverage
}

// githubTestsWindowRecords is the walk's records without those of the deep range: the phases after the deep range (the
// artifacts and open phases) check the bound on it, so a deep range stopped at the bound leaves them their own allowance and
// its one advancing observation never turns into a blocking one. The deep range itself stops at the whole walk's bound.
func githubTestsWindowRecords(cursor githubTestsChunkCursor) int {
	return githubTestsWalkRecords(cursor) - cursor.LateRecords
}

// markGitHubTestsInventoryTruncated records that an inventory phase ended before the end of its listing: the unit finalizes
// with reports_complete=false and no watermark (githubTestsWindowBlockingComponents).
func markGitHubTestsInventoryTruncated(
	cursor githubTestsChunkCursor, client *providerfoundation.HTTPClient, claim Claim, component string,
) githubTestsChunkCursor {
	cursor.Incomplete = mergeGitHubTestsIncomplete(cursor.Incomplete, GitHubTestsIncomplete{
		Component: component, Cause: githubTestsPageBudgetCause, Count: 1,
	})
	if client != nil {
		client.Metrics.RecordInventoryPageCap(claim.Provider, claim.Dataset)
	}
	return cursor
}

// recordGitHubTestsWalkBound ends an inventory phase at the walk record bound through the page-budget truncation: the same
// observation, so the closed vocabulary and the coverage readers are unchanged; the WARN line names the bound.
func recordGitHubTestsWalkBound(
	cursor githubTestsChunkCursor, client *providerfoundation.HTTPClient, claim Claim, component string, bound int,
) githubTestsChunkCursor {
	cursor = markGitHubTestsInventoryTruncated(cursor, client, claim, component)
	logGitHubTestsWalkBound(cursor, claim, component, bound)
	return cursor
}

// githubTestsHoldDeepStopAfterDeletion: a deep listing stopped at the record bound in a walk that proved a deletion
// (DeletionProven) may stop before a survivor the deletion credit did not cover (CHAOS-14434). Its advancing stop then
// also records a window-blocking truncation (run_inventory for the deep runs, artifact_inventory for the deep reports): the
// watermark holds, and the next walk reads the deep range again. Re-seek credit alone (an inserted run) holds nothing.
func githubTestsHoldDeepStopAfterDeletion(
	cursor githubTestsChunkCursor, client *providerfoundation.HTTPClient, claim Claim, component string,
) githubTestsChunkCursor {
	if cursor.DeletionProven == 0 {
		return cursor
	}
	return markGitHubTestsInventoryTruncated(cursor, client, claim, component)
}

// githubTestsOpenRunsWalkBoundComponent names the open phase in the walk-bound WARN line only; it is not an inventory component.
const githubTestsOpenRunsWalkBoundComponent = "open_runs"

func logGitHubTestsWalkBound(cursor githubTestsChunkCursor, claim Claim, component string, bound int) {
	slog.Warn(
		"provider walk record bound reached",
		"provider", claim.Provider, "dataset", claim.Dataset, "mode", claim.Mode, "unit", claim.ID,
		"repository", logging.ProviderText(cursor.Repo), "component", component,
		"records", githubTestsWalkRecords(cursor), "bound", bound,
	)
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
	cursor = markGitHubTestsInventoryTruncated(cursor, client, claim, component)
	slog.Warn(
		"provider inventory page budget exhausted",
		"provider", claim.Provider, "dataset", claim.Dataset, "unit", claim.ID,
		"repository", logging.ProviderText(cursor.Repo), "component", component, "pages", pagesSpent,
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
		"repository", logging.ProviderText(cursor.Repo), "component", component, "cause", cause,
		logging.ProviderIDAttr("run", runID), "kept", kept,
	)
	return cursor
}

// recordGitHubTestsRunReportsDuplicate records, once per run and cause, that report artifacts of the run were dropped as the aggregate
// of their parts (aggregate_of_parts: nothing is lost, the watermark advances) or kept although an aggregate disagrees with its parts
// on outcomes (aggregate_mismatch: the run is not advanced past). The artifact ids go to the markers, the run id to the log line.
func recordGitHubTestsRunReportsDuplicate(
	cursor githubTestsChunkCursor,
	client *providerfoundation.HTTPClient,
	claim Claim,
	cause string,
	runID string,
	rows int,
) githubTestsChunkCursor {
	cursor.Incomplete = mergeGitHubTestsIncomplete(cursor.Incomplete, GitHubTestsIncomplete{
		Component: githubTestsRunReportsComponent, Cause: cause, Count: 1,
	})
	if client != nil {
		client.Metrics.RecordPerRunTruncation(claim.Provider, claim.Dataset, githubTestsRunReportsComponent, cause)
	}
	slog.Warn(
		"provider run report artifacts overlap by content; see the artifact markers",
		"provider", claim.Provider, "dataset", claim.Dataset, "unit", claim.ID,
		"repository", logging.ProviderText(cursor.Repo), "component", githubTestsRunReportsComponent, "cause", cause,
		logging.ProviderIDAttr("run", runID), "rows", rows,
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
	// artifact_expired is NOT counted here (CHAOS-13980): this runs once per attempt, including attempts that later fail (a
	// readable download exhausting its retries) and are recollected, so a counter fired here would charge the same expired
	// artifact again on every retry. providerunit counts it once from the committed unit result, after Complete returns nil.
	if client != nil && cause != githubTestsArtifactExpiredCause {
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
	notFoundExcluded int,
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
	// notFoundExcluded is included here (codex review r1, CHAOS-5427 follow-up
	// N-1): a unit whose ONLY skip this attempt is routine not-found never
	// appends anything to `incomplete` (by design -- see the notFound branch's
	// own doc comment in CollectChunks) and never touches excludedSuffix/
	// excludedPrefix either, so without this it silently logged nothing even
	// though a real skip -- and its totality_excluded_not_found count -- did
	// happen this attempt.
	hasArtifactDisposition := excludedSuffix > 0 || excludedPrefix > 0 || notFoundExcluded > 0
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
	// totalityExcludedOversized isolates the oversized share of
	// artifactSkipTotal: every oversized skip (CHAOS-4315) is now excluded
	// from the ArchivesSeen/ArchivesUnreadable totality gate (see the
	// oversized branch's doc comment in CollectChunks), so this is exactly
	// the count of report_member skips this unit had that never touched the
	// gate's denominator or numerator -- surfaced here so that fact is
	// readable from the log line without cross-referencing the cursor JSON.
	totalityExcludedOversized := 0
	for _, observation := range incomplete {
		if observation.Component != githubTestsReportMemberComponent {
			continue
		}
		switch observation.Cause {
		case githubTestsArtifactOversizedCause, githubTestsArtifactUnavailableCause, githubTestsUnreadableArchiveCause, githubTestsArtifactExpiredCause:
			artifactSkipTotal += observation.Count
		}
		if observation.Cause == githubTestsArtifactOversizedCause {
			totalityExcludedOversized += observation.Count
		}
	}
	attrs := make([]any, 0, 20+2*len(incomplete))
	attrs = append(attrs,
		"provider", claim.Provider, "dataset", claim.Dataset, "unit", claim.ID,
		"repository", logging.ProviderText(repo), "artifact_skip_total", artifactSkipTotal,
		"incomplete_total", githubTestsIncompleteCount(incomplete),
		// totality_excluded_* (CHAOS-4315/CHAOS-4185 interaction fix): the two
		// skip causes that never feed the ArchivesSeen/ArchivesUnreadable
		// totality gate, so an operator can see the numbers the gate divides
		// by are NOT what this line's artifact_skip_total/incomplete_total
		// otherwise suggest -- both can be nonzero on a unit the gate never
		// looks twice at.
		"totality_excluded_oversized", totalityExcludedOversized,
		"totality_excluded_not_found", notFoundExcluded,
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
	attrs = append(attrs, githubTestsSkippedArtifactMarkerAttrs(skippedArtifacts, causeOverflow)...)
	slog.Warn("provider artifacts skipped this unit; inventory continued", attrs...)
}

// githubTestsSkippedArtifactMarkerAttrs builds the "skipped_sample"/
// "skipped_sample_cause_overflow" attr pair shared by every log line that
// reports on the durable SkippedArtifacts markers (codex review gate round
// 5, P2, fixing the sibling of round 2 P2's fix: the totality-unreadable
// failure path (githubTestsCheckAllArtifactsUnreadable) returns before
// githubTestsLogArtifactSkipSummary ever runs, so it had its OWN copy of
// "skipped_sample" that silently never received "skipped_sample_cause_overflow"
// when that field was added -- two independent copies of the same
// presentation logic drifted the moment only one of them was updated. A
// THIRD caller now shares this exact function instead of a third copy, so
// it cannot drift the same way again.
func githubTestsSkippedArtifactMarkerAttrs(
	skippedArtifacts []GitHubTestsSkippedArtifact, causeOverflow map[string]bool,
) []any {
	var attrs []any
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
	return attrs
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
		// Name, run and artifact ids are provider-assigned: each is logged
		// only in the id shape, otherwise as "[id_dropped]".
		name := sampleID(marker.Name)
		if marker.Name == "" {
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
			entry += ", run=" + sampleID(marker.RunID)
		}
		if marker.ArtifactID != "" {
			entry += ", artifact=" + sampleID(marker.ArtifactID)
		}
		entry += ")"
		sample = append(sample, entry)
	}
	return sample
}

// sampleID is a provider-assigned id or name in a skipped-artifact sample
// entry: the value when it passes logging.ProviderAssignedID, otherwise
// "[id_dropped]".
func sampleID(raw string) string {
	id, dropped := logging.ProviderAssignedID(raw)
	if dropped {
		return "[id_dropped]"
	}
	return id
}

// The two phases whose resume cursors carry a positional index. Closed set:
// the phase reaches a metric label.
const (
	githubTestsRunsPhase      = "runs"
	githubTestsArtifactsPhase = "artifacts"
)

// The ways a resume goes on from its cursor (githubTestsResumeStart).
const (
	githubTestsResumeAt     = iota // at the returned index of the re-fetched page
	githubTestsResumeReseek        // the listing moved: seek it again (githubTestsReseek)
	githubTestsResumeRewalk        // a positional artifacts cursor whose index addresses nothing: walk the page from 0
)

// githubTestsPageEnd is what the resume of a page-end cursor learned about the finished page before it: whether its URL
// could be derived (githubTestsPreviousPageURL) and the id of its last run.
type githubTestsPageEnd struct {
	known   bool
	lastRun string
	items   []json.RawMessage
}

// githubTestsResumeStart resolves where the first page of an invocation resumes (CHAOS-14316).
//
// A resume cursor names a page by its URL and an index into it, plus the id of the last run the listing listed (AnchorID).
// GitHub serves Actions runs newest-first and pages them by position, so a run deleted BEFORE that run between two attempts
// moves every later run one place up: the page holds different runs at the same index, and a page end moves the next page's
// first run onto the finished page. A position alone resumed past such a run and reported the listing complete.
//
//   - Inside a page: the run before the stored index must still be the anchor. A deletion after it moves only runs not yet
//     listed, so it passes.
//   - At a page end the cursor names the next page at index 0 (the short next URL, as before); the anchor must still be the
//     last run of the page before it, which the caller re-reads (pageEnd).
//   - Anything else (the anchor moved, an index past the page, a page end whose previous page cannot be named, a cursor
//     without an anchor) in the runs phases seeks the listing again from its oldest listed created second
//     (githubTestsReseek): a moved page costs a re-read, never a skip, and never fails the unit (CHAOS-4177).
//   - In the artifacts phases a cursor without a checked anchor (written before AnchorID existed, or the fit dropped it, or
//     a page end whose previous page cannot be named) resumes by position as before: a re-seek there reads runs again,
//     which turns the totality counters unknown, and on an unchanged listing that would silence the all-unreadable gate. An
//     index that addresses nothing walks the page again from 0 (the old re-anchor).
//
// A page that is not the invocation's first, or another URL, starts at 0. A re-seek or re-walk is counted as a re-anchor
// (the label set is unchanged).
func githubTestsResumeStart(
	cursor githubTestsChunkCursor,
	page providerfoundation.PageVisit,
	first bool,
	pageEnd githubTestsPageEnd,
	client *providerfoundation.HTTPClient,
	claim Claim,
	phase string,
) (int, int) {
	if !first || cursor.NextURL != page.CursorBefore {
		return 0, githubTestsResumeAt
	}
	positional := phase == githubTestsArtifactsPhase &&
		(cursor.AnchorID == "" || cursor.Index == 0 && !pageEnd.known)
	start, action := 0, githubTestsResumeReseek
	switch {
	case positional && (cursor.Index == 0 || cursor.Index < len(page.Items)):
		start, action = cursor.Index, githubTestsResumeAt
	case positional:
		action = githubTestsResumeRewalk
	case cursor.AnchorID == "":
		if cursor.Index == 0 && cursor.RangeItems == 0 {
			action = githubTestsResumeAt
		}
	case cursor.Index == 0:
		if pageEnd.known && pageEnd.lastRun == cursor.AnchorID {
			action = githubTestsResumeAt
		}
	case cursor.Index <= len(page.Items) && githubTestsListedRunID(page.Items[cursor.Index-1]) == cursor.AnchorID:
		start, action = cursor.Index, githubTestsResumeAt
	}
	if action == githubTestsResumeAt {
		return start, action
	}
	if client != nil {
		client.Metrics.RecordResumeReanchor(claim.Provider, claim.Dataset, phase)
	}
	slog.Warn(
		"provider page moved under a resume; seeking the listing again from its oldest listed created second, or re-walking a positional page",
		"provider", claim.Provider, "dataset", claim.Dataset, "unit", claim.ID,
		"repository", logging.ProviderText(cursor.Repo), "phase", phase, "reseek", action == githubTestsResumeReseek,
		"stored_index", cursor.Index, "page_items", len(page.Items), "anchored", cursor.AnchorID != "",
	)
	return 0, action
}

// githubTestsDeletedBeforeAnchor is the runs proven deleted before the anchor of a moved resume (CHAOS-14434). items is the
// page the anchor was listed on, re-fetched now, and index the stored position after the anchor (the anchor was at
// index-1; a page-end resume passes the page before the one it names and its length). GitHub lists newest first:
//   - the anchor found at j < index-1: (index-1)-j runs before it are gone (exact);
//   - found at j > index-1: runs appeared ahead of it, nothing is proven gone: 0;
//   - not on the page, whose first run is created before the anchor's second (anchorSecond, the range's oldest listed
//     second): the anchor and every run up to it moved to an earlier page, so at least index runs are gone;
//   - not on the page otherwise (moved later, or deleted or reordered inside its second): 0.
//
// The contract does not order runs inside one created second, so a found anchor that shares its second with another run
// of the page proves nothing: 0.
func githubTestsDeletedBeforeAnchor(items []json.RawMessage, index int, anchor, anchorSecond string) int {
	if anchor == "" || index < 1 || len(items) == 0 {
		return 0
	}
	seconds := make([]int64, len(items))
	found := -1
	for position, item := range items {
		if run, err := decodeGitHubWorkflowRun(item); err == nil {
			if created := parseGitHubWorkflowTime(run.CreatedAt); created != nil {
				seconds[position] = created.UTC().Unix()
			}
		}
		if found < 0 && githubTestsListedRunID(item) == anchor {
			found = position
		}
	}
	if found >= 0 {
		for position, second := range seconds {
			if position != found && second == seconds[found] {
				return 0
			}
		}
		return max(0, index-1-found)
	}
	second, ok := githubTestsCutSecond(anchorSecond)
	if !ok {
		return 0
	}
	first, err := decodeGitHubWorkflowRun(items[0])
	if err != nil {
		return 0
	}
	if created := parseGitHubWorkflowTime(first.CreatedAt); created != nil && created.UTC().Unix() < second {
		return index
	}
	return 0
}

// githubTestsResumeDeleted is githubTestsDeletedBeforeAnchor for the first page of a resume that seeks again: inside a
// page on the page itself, at a page end on the page before it (pageEnd). A position is only proof on full pages: the
// range's runs listed must be whole pages of per_page up to the stored index, and the re-fetched page must be full, else a
// page that GitHub packed differently between two attempts moves the anchor with nothing deleted: 0.
func githubTestsResumeDeleted(cursor githubTestsChunkCursor, page providerfoundation.PageVisit, pageEnd githubTestsPageEnd) int {
	if cursor.NextURL != page.CursorBefore {
		return 0
	}
	if cursor.Index > 0 {
		if cursor.RangeItems != (cursor.PagesEntered-1)*nativePerPage+cursor.Index || len(page.Items) != nativePerPage {
			return 0
		}
		return githubTestsDeletedBeforeAnchor(page.Items, cursor.Index, cursor.AnchorID, cursor.RangeOldest)
	}
	if !pageEnd.known || cursor.RangeItems != cursor.PagesEntered*nativePerPage || len(pageEnd.items) != nativePerPage {
		return 0
	}
	return githubTestsDeletedBeforeAnchor(pageEnd.items, len(pageEnd.items), cursor.AnchorID, cursor.RangeOldest)
}

// githubTestsNeedsPageEnd reports a page-end cursor whose anchor the resume checks on the page before the one it names.
func githubTestsNeedsPageEnd(cursor githubTestsChunkCursor) bool {
	return cursor.NextURL != "" && cursor.Index == 0 && cursor.AnchorID != "" &&
		(cursor.Phase == githubTestsRunsPhase || cursor.Phase == githubTestsArtifactsPhase)
}

// githubTestsPreviousPageURL is the URL of the page before a GitHub Link page URL: the same URL with its page number one
// lower. "" when the URL carries no page number above 1 (that page cannot be named; the caller treats the anchor as
// unchecked).
func githubTestsPreviousPageURL(next string) string {
	parsed, err := url.Parse(next)
	if err != nil {
		return ""
	}
	query := parsed.Query()
	page, err := strconv.Atoi(query.Get("page"))
	if err != nil || page < 2 || len(query["page"]) != 1 {
		return ""
	}
	query.Set("page", strconv.Itoa(page-1))
	parsed.RawQuery = query.Encode()
	return parsed.String()
}

// githubTestsListedRunID is the anchor form of one listed run: its id when it is 1-20 digits, else "" (never an anchor).
func githubTestsListedRunID(raw json.RawMessage) string {
	run, err := decodeGitHubWorkflowRun(raw)
	if err != nil {
		return ""
	}
	return githubTestsAnchorID(run)
}

func githubTestsAnchorID(run gitHubWorkflowRunPayload) string {
	id := run.ID.String()
	if !githubTestsCutRunDigits(id, 20) {
		return ""
	}
	return id
}

// errGitHubTestsReseek ends a page visit whose resume position moved (githubTestsResumeStart); the phase loop seeks the
// listing again (githubTestsReseek).
var errGitHubTestsReseek = errors.New("github tests listing moved under a resume")

// githubTestsReseek starts the active range again as a new search that ends at the created second of its oldest listed run
// (RangeOldest, inclusive). GitHub lists newest-created first, so every run not yet listed was created at or before that
// second, and every run created after it was listed. The runs of that second listed in this range are listed and read
// again: a run is never skipped inside the range that read it (the cut-key rule, github_tests_cut_runs.go). That dup is
// idempotent: every destination is a ReplacingMergeTree on the row's natural key (internal/chmigrate/history/
// 000_raw_tables.sql:97-106 for ci_pipeline_runs, 029_testops_tables.sql:33-102 for the testops tables), so a row read again
// replaces the stored one. Only the carried list stays a skip source, when the range already ended at that second: those
// runs were read whole by the previous range. With no dated run listed yet the range starts again from its start. The new
// search has its own count. A moved page never fails the unit (CHAOS-4177: a failure cost one of its five attempts).
//
// The new search's first pages re-list what the moved search listed: it gets as many free pages as the moved search entered
// (PagesEntered; a cursor written before that field counts ceil(RangeItems / per page)), at least one, or the free pages it
// already had when the moved search was itself a re-seek, whichever is more (CHAOS-14385). Every run the moved search listed sits at or above the same position in the new search
// (the new search lists a subset, newest first), so a walk at the page budget edge then reads every run an uninterrupted
// walk of the surviving runs reads.
func githubTestsReseek(cursor githubTestsChunkCursor) githubTestsChunkCursor {
	cursor.ReseekPages = max(cursor.ReseekPages, cursor.PagesEntered, (cursor.RangeItems+nativePerPage-1)/nativePerPage, 1)
	cursor.PagesEntered = 0
	if second, ok := githubTestsCutSecond(cursor.RangeOldest); ok {
		if hi, hiOK := githubTestsCutSecond(cursor.RangeHi); !hiOK || hi != second {
			cursor.CarriedCutKeys = ""
		}
		cursor.RangeHi = cursor.RangeOldest
	}
	cursor.ActiveCutKeys = ""
	cursor.RangeItems, cursor.RangeOldest, cursor.RangeOldestItems = 0, "", 0
	cursor.NextURL, cursor.Index, cursor.AnchorID = "", 0, ""
	return cursor
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
		(cursor.Phase != "runs" && cursor.Phase != "artifacts" && cursor.Phase != githubTestsOpenPhase && cursor.Phase != "done") ||
		cursor.RangeItems < 0 || cursor.RangeOldestItems < 0 || cursor.RangeOldestItems > cursor.RangeItems ||
		cursor.RangeOldestItems > 0 && cursor.RangeOldest == "" || cursor.OpenDone < 0 || cursor.OpenCap < 0 ||
		cursor.OpenSkipped < 0 || cursor.OpenTransientRun < 0 ||
		cursor.OpenSkipEvent != nil && (cursor.OpenSkipEvent.RunID == "" || cursor.OpenSkipEvent.Reason == "") ||
		cursor.Index < 0 || cursor.NextURL == "" && cursor.Index != 0 ||
		cursor.AnchorID != "" && !githubTestsCutRunDigits(cursor.AnchorID, 20) ||
		cursor.RunPages < 0 || cursor.ArtifactPages < 0 || cursor.ReseekPages < 0 || cursor.PagesEntered < 0 || cursor.RecordCredit < 0 || cursor.DeletionProven < 0 || cursor.DeletionProven > 1 || cursor.Relist < 0 || cursor.Relist > githubTestsRelistStoppedSaturated ||
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
	if !githubTestsCutKeysValid(cursor) {
		return githubTestsChunkCursor{}, chunkConflictBecause(githubTestsCutRunsConflict)
	}
	if cursor.Phase != githubTestsRunsPhase && cursor.Phase != githubTestsArtifactsPhase {
		cursor = githubTestsClearCutKeys(cursor)
		cursor.AnchorID = ""
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
		// The full literal sample proves a per-cause count the trim is about to lose: a checkpoint written before the count ledger
		// held every marker, so six literal markers prove six skips. Keep the greater of that and any ledger count, so evidence
		// that overlaps is counted once (CHAOS-14030).
		proven := map[string]int{}
		for _, record := range kept {
			if cause := githubTestsSkippedArtifactCause(record); cause != "" {
				proven[cause]++
			}
		}
		for cause, count := range proven {
			if cursor.SkippedArtifactCauseCount[cause] >= count {
				continue
			}
			ledger := make(map[string]int, len(cursor.SkippedArtifactCauseCount)+1)
			for key, value := range cursor.SkippedArtifactCauseCount {
				ledger[key] = value
			}
			ledger[cause] = count
			cursor.SkippedArtifactCauseCount = ledger
		}
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
			// githubTestsSkippedArtifactCause, not record.Cause directly
			// (codex review gate round 3, P2): a genuinely pre-CHAOS-4394
			// marker's Cause field decodes as "" -- that field did not exist
			// on the binary that wrote it -- so record.Cause alone would
			// attribute the drop to the empty string, not artifact_oversized,
			// leaving githubTestsReportMemberSkippedWithoutDurableMarker's
			// causeOverflow["artifact_oversized"] check unable to see it and
			// withholding a unit whose overflow evidence is real, just
			// mis-keyed.
			cause := githubTestsSkippedArtifactCause(record)
			cursor.SkippedArtifactCauseOverflow = markGitHubTestsSkippedArtifactCauseOverflow(
				cursor.SkippedArtifactCauseOverflow, cause,
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
		// RunID/ArtifactID re-truncated here too (codex review gate round 6,
		// P2): normalizeLegacyGitHubTestsSkippedArtifacts re-bounds Name for
		// exactly this reason -- a cursor a PRIOR binary wrote can already
		// carry a shape THIS binary's own caps would never produce -- but a
		// legacy binary that predates githubTestsMaxArtifactIDBytes could
		// equally have carried an unbounded RunID/ArtifactID (round 5's fix
		// only bounds a NEWLY appended record). Left untouched, a resumed
		// cursor with a syntactically valid oversized legacy ID could sit
		// just under maxChunkCursorBytes, then fail the very next checkpoint
		// after any new skip -- repeating on every resume of that same
		// cursor, exactly the hazard round 5's fix closed for new records.
		if truncated := githubTestsTruncateArtifactID(record.RunID); truncated != record.RunID {
			renamed[index].RunID = truncated
			changed = true
		}
		if truncated := githubTestsTruncateArtifactID(record.ArtifactID); truncated != record.ArtifactID {
			renamed[index].ArtifactID = truncated
			changed = true
		}
	}
	if changed {
		cursor.SkippedArtifacts = renamed
	}
	return cursor
}

// recordGitHubTestsCutListsDropped counts and logs one emission whose cursor dropped cut lists to fit the byte budget
// (CHAOS-14315). It fires once per degraded emission, never for the before cursor of the next one.
func recordGitHubTestsCutListsDropped(cursor githubTestsChunkCursor, bytesBefore int, client *providerfoundation.HTTPClient, claim Claim, dropped []string) {
	encoded, _ := json.Marshal(cursor)
	for _, list := range dropped {
		if client != nil {
			client.Metrics.RecordCursorCutListDropped(list)
		}
	}
	slog.Warn(
		"github tests chunk cursor over the byte budget; cut lists dropped, the revisit of the cut second reads its runs again",
		"provider", claim.Provider, "dataset", claim.Dataset, "repository", logging.ProviderText(cursor.Repo),
		"dropped", strings.Join(dropped, ","), "bytes_after", len(encoded), "bytes_before", bytesBefore,
	)
}

// githubTestsAddExcludedSample is the one bounded insertion of an exclusion sample (all four reasons). A sample already
// present is not added again: a run read again (a resume from a degraded cursor) must not grow the cursor (CHAOS-14315).
func githubTestsAddExcludedSample(samples []string, sample string) []string {
	if len(samples) >= githubTestsMaxExcludedArtifactSampleRecords || slices.Contains(samples, sample) {
		return samples
	}
	return append(samples, sample)
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
	incomplete = append(incomplete, githubTestsSaturatedObservations(cursor)...)
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
		cursor.ArtifactsExcludedNotFound,
	)
	return CompleteRouteBatch{
		Effects: effects,
		Result: map[string]any{
			"pipeline_runs_synced": cursor.Pipelines, "job_runs_synced": cursor.Jobs,
			"acceptance_checks_synced": cursor.Acceptance, "test_suites_synced": cursor.Suites,
			"test_cases_synced": cursor.Cases, "coverage_snapshots_synced": cursor.Coverage,
			"repo": cursor.Repo, GitHubTestsResultKeyReportsComplete: len(incomplete) == 0,
			"reports_skipped": githubTestsIncompleteCount(incomplete),
			// The open phase's evidence: runs re-read by id, and whether its cap
			// (not the end of the open rows) stopped it. Never blocks the watermark.
			GitHubTestsResultKeyOpenRunsReread: cursor.OpenDone, GitHubTestsResultKeyOpenCapReached: cursor.OpenCapReached,
			GitHubTestsResultKeyOpenRunsSkipped: cursor.OpenSkipped,
			GitHubTestsResultKeyIncomplete:      incomplete,
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
	cursor = githubTestsRestorePagesEntered(cursor)
	cursor.OpenSkipEvent = nil
	// invocationBaseRequests is the running total every PRIOR chunk
	// invocation already persisted onto the cursor. requests below counts
	// only THIS invocation's physical wire attempts (via the counting Doer
	// wrapping client.Doer, below) -- cursor.Requests is stamped as their
	// sum at every point this invocation reports it, so a retried or
	// ultimately-failed attempt is never dropped from the total.
	invocationBaseRequests := cursor.Requests
	requests := 0
	counted := *client
	var rateObserved githubTestsRateObserver
	counted.Doer = githubTestsChunkCountingDoer{delegate: client.Doer, attempts: &requests, observed: &rateObserved}
	client = &counted
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
		if cursor.OpenFloorHit {
			batch.Result[GitHubTestsResultKeyOpenPhaseRateFloor] = cursor.OpenFloorRemaining
		}
		if cursor.OpenSkipLimitHit {
			batch.Result[GitHubTestsResultKeyOpenSkipLimit] = true
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
	maxArtifactBytes := handler.MaxArtifactBytes
	if maxArtifactBytes <= 0 {
		maxArtifactBytes = githubTestsMaxDownloadSize
	}
	if maxRuns < 1 || maxRuns > githubTestsMaxRuns || maxArtifacts < 1 || maxArtifacts > githubTestsMaxArtifacts || jobPages < 1 || jobPages > nativeMaxPages {
		return ErrInvalidConfiguration
	}
	maxWalkRecords := handler.MaxWalkRecords
	if maxWalkRecords == 0 {
		maxWalkRecords = githubTestsMaxWalkRecords
	}
	if maxWalkRecords < 1 || maxWalkRecords > githubTestsMaxWalkRecords {
		return ErrInvalidConfiguration
	}
	lateMaxPages := handler.lateRerunMaxPages()
	if lateMaxPages < 1 || lateMaxPages > githubTestsLateRerunMaxPages {
		return ErrInvalidConfiguration
	}
	if err := validatePerRunPageBudget(
		"MaxJobPages", jobPages, githubTestsPerRunPerPage, githubTestsMaxJobsPerRun,
	); err != nil {
		return err
	}
	root := providerRelativePath(client, "repos", owner, repository)
	var repo gitHubRepositoryPayload
	if err := fetchObjectRoute(ctx, client, githubRepoRoute, root, &repo); err != nil {
		return err
	}
	cursor.Repo = repo.FullName
	repoID, err := resolveGitHubRepoID(ctx, handler.RepoIdentity, claim, repo)
	if err != nil {
		return err
	}
	cursor.Requests = invocationBaseRequests + requests
	policyCache := map[string]githubTestsPolicy{}
	// Resume state of this invocation (CHAOS-14316). firstPage: only the invocation's first page resumes from the cursor.
	// The re-seek's free pages and its record credit ride the cursor (ReseekPages, RecordCredit; CHAOS-14385).
	firstPage := true
	walkBoundHit := func() bool { return githubTestsWalkRecords(cursor)-cursor.RecordCredit >= maxWalkRecords }
	windowBoundHit := func() bool { return githubTestsWindowRecords(cursor)-cursor.RecordCredit >= maxWalkRecords }
	reseek := githubTestsReseek
	// creditRecords: the records of a run created in the boundary second of a re-seek range, read by the listing that
	// re-sought, are not charged to the record bounds. Another listing earns no credit (its report rows of that second are
	// first reads; a range end clears ReseekPages); credit already earned stays.
	creditRecords := func(second int64, records int, deep bool) {
		boundary, ok := githubTestsCutSecond(cursor.RangeHi)
		if cursor.ReseekPages == 0 || !ok || second != boundary || records <= 0 {
			return
		}
		cursor.RecordCredit += records
		if deep {
			cursor.LateRecords -= records
		}
	}
	// creditDeletion: the records a run deleted before the anchor was charged stay charged, while the uninterrupted walk of
	// the survivors never charges them, so a proven deletion (githubTestsResumeDeleted) earns the average records of a read
	// run per deleted run, rounded up (runs phase: pipeline, job and acceptance rows per pipeline; artifacts phase: report
	// rows per pipeline, the artifacts listing lists the runs the runs phase read). The deletion credit stops at a twentieth
	// of the bound, checked against all the credit (the re-seek re-read credit above is not capped). A deep-range credit
	// also comes off LateRecords, as creditRecords does (CHAOS-14434).
	deletionCeiling := maxWalkRecords / 20
	creditDeletion := func(deleted int, phase string) {
		if deleted <= 0 {
			return
		}
		cursor.DeletionProven = 1
		if cursor.Pipelines <= 0 {
			return
		}
		records := cursor.Pipelines + cursor.Jobs + cursor.Acceptance
		if phase == githubTestsArtifactsPhase {
			records = cursor.Suites + cursor.Cases + cursor.Coverage
		}
		perRun := (records + cursor.Pipelines - 1) / cursor.Pipelines
		credit := min(deleted*perRun, max(0, deletionCeiling-cursor.RecordCredit))
		if credit <= 0 {
			return
		}
		cursor.RecordCredit += credit
		if phase == githubTestsRunsPhase && cursor.LateRuns {
			cursor.LateRecords = max(0, cursor.LateRecords-credit)
		}
	}
	// resumePageEnd re-reads the page before the one a page-end cursor names, for its last run (githubTestsResumeStart).
	resumePageEnd := func(page providerfoundation.PageVisit) (githubTestsPageEnd, error) {
		if !firstPage || cursor.NextURL != page.CursorBefore || !githubTestsNeedsPageEnd(cursor) {
			return githubTestsPageEnd{}, nil
		}
		previous := githubTestsPreviousPageURL(cursor.NextURL)
		if previous == "" {
			return githubTestsPageEnd{}, nil
		}
		collection, err := providerfoundation.CollectGitHubLinkPages(ctx, client, providerfoundation.GitHubPageOptions{
			Path: root + "/actions/runs", Route: githubRunsRoute, DataKey: "workflow_runs", MaxPages: 1, InitialURL: previous,
		})
		cursor.Requests = invocationBaseRequests + requests
		if err != nil {
			return githubTestsPageEnd{}, err
		}
		cursor.Pages += collection.Pages
		end := githubTestsPageEnd{known: true, items: collection.Items}
		if len(collection.Items) > 0 {
			end.lastRun = githubTestsListedRunID(collection.Items[len(collection.Items)-1])
		}
		return end, nil
	}
	// movedAtSpentBudget checks the anchor of a page-end resume whose page allowance is already spent, before the budget
	// stop is recorded: the next page would not be fetched, so its visitor never checks the page before it. A moved
	// listing is sought again (its first pages are free); an unmoved one stops at the budget as before. Only the
	// invocation's first listing call can carry a resume cursor.
	movedAtSpentBudget := func(phase string) (bool, error) {
		if !firstPage || !githubTestsNeedsPageEnd(cursor) {
			return false, nil
		}
		pageEnd, err := resumePageEnd(providerfoundation.PageVisit{CursorBefore: cursor.NextURL})
		if err != nil {
			return false, err
		}
		visit := providerfoundation.PageVisit{CursorBefore: cursor.NextURL}
		_, action := githubTestsResumeStart(cursor, visit, true, pageEnd, client, claim, phase)
		firstPage = false
		if action == githubTestsResumeReseek {
			creditDeletion(githubTestsResumeDeleted(cursor, visit, pageEnd), phase)
		}
		return action == githubTestsResumeReseek, nil
	}
	// creditAtBound checks the anchor of a resume that starts at the record bound, before the bound stop: the stop would
	// otherwise keep the charge of a run deleted before the anchor. A proven deletion that earns credit is credited and the
	// listing is sought again (true); a proven deletion without credit (no population, or the ceiling spent) only marks the
	// walk (DeletionProven) and stops at the bound; anything else stops at the bound as before (false). Only the
	// invocation's first listing call resumes.
	creditAtBound := func(phase string) (bool, error) {
		if !firstPage || cursor.NextURL == "" || cursor.AnchorID == "" {
			return false, nil
		}
		defer func() { firstPage = false }()
		page := providerfoundation.PageVisit{CursorBefore: cursor.NextURL}
		var pageEnd githubTestsPageEnd
		if cursor.Index > 0 {
			collection, err := providerfoundation.CollectGitHubLinkPages(ctx, client, providerfoundation.GitHubPageOptions{
				Path: root + "/actions/runs", Route: githubRunsRoute, DataKey: "workflow_runs", MaxPages: 1, InitialURL: cursor.NextURL,
			})
			cursor.Requests = invocationBaseRequests + requests
			if err != nil {
				return false, err
			}
			cursor.Pages += collection.Pages
			page.Items = collection.Items
		} else {
			end, err := resumePageEnd(page)
			if err != nil {
				return false, err
			}
			pageEnd = end
		}
		credit := cursor.RecordCredit
		creditDeletion(githubTestsResumeDeleted(cursor, page, pageEnd), phase)
		if cursor.RecordCredit == credit {
			return false, nil
		}
		githubTestsResumeStart(cursor, page, true, pageEnd, client, claim, phase)
		return true, nil
	}
	// coalesceHeld: the last emission was coalescable, so the executor may still hold it.
	coalesceHeld := false
	emitCursorAs := func(before, after githubTestsChunkCursor, batch CompleteRouteBatch, final, coalescable bool) error {
		before, _ = githubTestsFitCursor(before)
		beforeRaw, beforeErr := encodeGitHubTestsChunkCursor(before)
		if beforeErr != nil {
			return beforeErr
		}
		bytesBefore, _ := json.Marshal(after)
		after, dropped := githubTestsFitCursor(after)
		if len(dropped) > 0 {
			recordGitHubTestsCutListsDropped(after, len(bytesBefore), client, claim, dropped)
		}
		afterRaw, afterErr := encodeGitHubTestsChunkCursor(after)
		if afterErr != nil {
			return afterErr
		}
		if err := emit(ChunkRouteEmission{Batch: batch, CursorBefore: beforeRaw, CursorAfter: afterRaw, Final: final, Coalescable: coalescable}); err != nil {
			return err
		}
		coalesceHeld = coalescable
		return nil
	}
	emitCursor := func(before, after githubTestsChunkCursor, batch CompleteRouteBatch, final bool) error {
		return emitCursorAs(before, after, batch, final, false)
	}
	lookback := handler.refreshLookback()
	lateLookback := handler.lateRerunLookback()
	// nextRunsListing follows a finished listing range of the runs phase: the
	// runs range of the claim that holds a UTC midnight goes on to the deep
	// range, every other one to the artifacts phase.
	nextRunsListing := func(c githubTestsChunkCursor) githubTestsChunkCursor {
		c.RangeHi, c.RangeItems, c.RangeOldest, c.RangeOldestItems, c.AnchorID, c.ReseekPages, c.PagesEntered = "", 0, "", 0, "", 0, 0
		c = githubTestsClearCutKeys(c)
		if !c.LateRuns && githubTestsHoldsMidnight(claim) {
			c.LateRuns = true
			return c
		}
		c.LateRuns = false
		c.Phase = githubTestsArtifactsPhase
		return c
	}
	// endRunsRange is the end of one runs-phase listing range. A listing that
	// consumed GitHub's cap of results may hide older runs: the next range ends
	// at the oldest created time seen, in the same phase. A range that makes no
	// progress is recorded and the phase ends.
	endRunsRange := func(c githubTestsChunkCursor) githubTestsChunkCursor {
		nextHi, saturated, stuck := githubTestsRunsInterval(claim, c.LateRuns, lookback, lateLookback).peel(c.RangeItems, c.RangeOldest, c.RangeHi)
		items, hi := c.RangeItems, c.RangeHi
		if saturated && !stuck && c.RangeOldestItems >= githubTestsSearchResultCap {
			stuck, hi = true, nextHi
		}
		c.RangeItems, c.RangeOldest, c.RangeOldestItems, c.AnchorID, c.ReseekPages, c.PagesEntered = 0, "", 0, "", 0, 0
		switch {
		case !saturated:
			c = nextRunsListing(c)
		case stuck && c.LateRuns:
			c = recordGitHubTestsLateRerunRunsStop(c, client, claim, githubTestsListingSaturatedCause, c.LateRunPages)
			c = nextRunsListing(c)
		case stuck:
			c = recordGitHubTestsSaturated(c, client, claim, githubTestsRunInventorySaturatedComponent, githubTestsRunsPhase, hi, items)
			c = nextRunsListing(c)
		default:
			c.RangeHi = nextHi
			c = githubTestsFreezeCutKeys(c)
		}
		return c
	}
	endArtifactsRange := func(c githubTestsChunkCursor) githubTestsChunkCursor {
		nextHi, saturated, stuck := githubTestsArtifactInterval(claim, c.Relist == githubTestsRelistWalking, lateLookback).peel(c.RangeItems, c.RangeOldest, c.RangeHi)
		items, hi := c.RangeItems, c.RangeHi
		c.RangeItems, c.RangeOldest, c.AnchorID, c.ReseekPages, c.PagesEntered = 0, "", "", 0, 0
		switch {
		case !saturated:
			c.RangeHi = ""
			c = githubTestsClearCutKeys(c)
			c = githubTestsEndArtifactsListing(c, claim, handler)
		case stuck:
			if c.Relist == githubTestsRelistWalking {
				c = recordGitHubTestsRelistStop(c, client, claim, githubTestsListingSaturatedCause, hi, items)
			} else {
				c = recordGitHubTestsSaturated(c, client, claim, githubTestsArtifactInventorySaturatedComponent, githubTestsArtifactsPhase, hi, items)
			}
			c.RangeHi = ""
			c.Phase = githubTestsPhaseAfterArtifacts(handler)
			c = githubTestsClearCutKeys(c)
		default:
			c.RangeHi = nextHi
			c = githubTestsFreezeCutKeys(c)
		}
		return c
	}
	// collectRunDetail reads one run's jobs (and the branch policy its acceptance
	// rows need) and projects its job and acceptance rows. The runs phase and the
	// open phase share it, so a re-read run is written exactly like a listed one.
	// complete says the jobs read is whole: the walk proved its end (or stopped at
	// the deliberate item cap), got at least the declared total_count, and kept
	// every job it got.
	collectRunDetail := func(run gitHubWorkflowRunPayload, pipeline githubTestsPipelineRow) ([]githubTestsJobRow, []githubTestsAcceptanceRow, bool, error) {
		jobs := make([]githubTestsJobRow, 0)
		jobPage, pageErr := providerfoundation.CollectGitHubLinkPages(ctx, client, providerfoundation.GitHubPageOptions{
			Path:     root + "/actions/runs/" + url.PathEscape(pipeline.RunID) + "/jobs",
			Route:    githubRunJobsRoute,
			Query:    url.Values{"per_page": {strconv.Itoa(githubTestsPerRunPerPage)}},
			DataKey:  "jobs",
			MaxPages: jobPages,
			MaxItems: githubTestsMaxJobsPerRun + 1,
			TotalKey: "total_count",
		})
		if pageErr != nil {
			return nil, nil, false, pageErr
		}
		cursor.Requests = invocationBaseRequests + requests
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
				return nil, nil, false, providerfoundation.ErrNormalizationInvalid
			}
			row, ok := normalizeGitHubTestsJob(claim, repoID, pipeline.RunID, pipeline.RetryCount, job, normalizedAt)
			if ok {
				jobs = append(jobs, row)
			}
		}
		// A walk stopped by its page budget had a next page, so EndProven is false there. A walk that ended on its own with
		// every job total_count declares is whole too, even when its last page was full and carried no Link (CHAOS-14321).
		gotDeclared := jobPage.DeclaredTotalSeen && len(jobPage.Items) >= jobPage.DeclaredTotal
		complete := jobPage.ItemCapReached ||
			jobPage.EndProven && (!jobPage.DeclaredTotalSeen || gotDeclared) ||
			!jobPage.PageBudgetExhausted && gotDeclared
		complete = complete && len(jobs) == len(jobItems)
		targetBranch, prNumber := gitHubTestsTarget(run)
		policy := githubTestsPolicy{provenance: "github.branch_protection.target_branch_unavailable"}
		if targetBranch != nil {
			cached, ok := policyCache[*targetBranch]
			if !ok {
				cached, err = fetchGitHubTestsPolicy(ctx, client, root, *targetBranch)
				cursor.Requests = invocationBaseRequests + requests
				if err != nil {
					return nil, nil, false, err
				}
				policyCache[*targetBranch] = cached
			}
			policy = cached
		}
		acceptance := projectGitHubTestsChecks(claim, repoID, pipeline, jobs, policy, targetBranch, prNumber, testsOptionalString(stringValue(run.HTMLURL)), normalizedAt)
		return jobs, acceptance, complete, nil
	}
	emitRunPage := func(page providerfoundation.PageVisit) error {
		cursor.Pages++
		// Every physical page fetch, including one with zero items or every
		// item filtered before it can trigger a per-run request, must land in
		// cursor.Requests immediately: a scattered per-item-only refresh
		// missed this listing page's own wire attempt when no item on it ever
		// reached the job-fetch branch below.
		cursor.Requests = invocationBaseRequests + requests
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
		// A moved page is sought again as a new search (githubTestsReseek), which also drops the count of runs at the
		// oldest second (CHAOS-14321: the runs are counted again). The page left for the re-seek is not charged.
		pageEnd, pageEndErr := resumePageEnd(page)
		if pageEndErr != nil {
			return pageEndErr
		}
		start, action := githubTestsResumeStart(cursor, page, firstPage, pageEnd, client, claim, githubTestsRunsPhase)
		firstPage = false
		if action != githubTestsResumeAt {
			creditDeletion(githubTestsResumeDeleted(cursor, page, pageEnd), githubTestsRunsPhase)
			return errGitHubTestsReseek
		}
		if !(cursor.NextURL == page.CursorBefore && cursor.Index > 0) {
			cursor = githubTestsEnterPage(cursor, func(c *githubTestsChunkCursor) {
				if c.LateRuns {
					c.LateRunPages++
				} else {
					c.RunPages++
				}
			})
		}
		for index := start; index < len(page.Items); index++ {
			if walkBoundHit() {
				return errGitHubTestsWalkBound
			}
			before := cursor
			run, err := decodeGitHubWorkflowRun(page.Items[index])
			if err != nil {
				return providerfoundation.RouteFailure(githubRunsRoute, providerfoundation.ReasonBodyDecode, err)
			}
			pipeline, include := normalizeGitHubTestsPipeline(claim, repoID, run, normalizedAt)
			cursor.RangeItems++
			created := parseGitHubWorkflowTime(run.CreatedAt)
			if oldest := githubTestsOlderCreated(cursor.RangeOldest, created); oldest != cursor.RangeOldest {
				cursor.RangeOldest, cursor.ActiveCutKeys, cursor.RangeOldestItems = oldest, "", 0
			}
			if created != nil && created.UTC().Format(time.RFC3339) == cursor.RangeOldest {
				cursor.RangeOldestItems++
			}
			cutSecond, cutKey := githubTestsCutRunKey(run)
			wanted := githubTestsRunWanted(run, pipeline, claim)
			if cursor.LateRuns {
				wanted = githubTestsLateRerunWanted(run, claim, lookback)
			}
			read := include && githubTestsRunsInterval(claim, cursor.LateRuns, lookback, lateLookback).admitsUnder(pipeline.QueuedAt, cursor.RangeHi) &&
				wanted && !githubTestsSkipCutRun(cursor, cutSecond, cutKey)
			jobs := make([]githubTestsJobRow, 0)
			acceptance := make([]githubTestsAcceptanceRow, 0)
			var pipelines []githubTestsPipelineRow
			if read {
				var jobsComplete bool
				var detailErr error
				jobs, acceptance, jobsComplete, detailErr = collectRunDetail(run, pipeline)
				if detailErr != nil {
					return detailErr
				}
				cursor.Pipelines++
				cursor.Jobs += len(jobs)
				cursor.Acceptance += len(acceptance)
				if cursor.LateRuns {
					cursor.LateRecords += 1 + len(jobs) + len(acceptance)
				}
				pipelines = []githubTestsPipelineRow{pipeline}
				// Only a run GitHub calls completed whose jobs read is whole is recorded, in the same emission as its rows (a run
				// cut at the per-run jobs cap too: the cap is deterministic). Any other read is dropped and read again by the revisit.
				if githubTestsRunReadable(run) && jobsComplete {
					cursor = githubTestsRecordCutRun(cursor, cutSecond, cutKey)
				} else {
					cursor = githubTestsDropCutRun(cursor, cutKey)
				}
			}
			effects, effectErr := testOpsEffects(pipelines, jobs, acceptance, nil, nil, nil)
			if effectErr != nil {
				return effectErr
			}
			creditRecords(cutSecond, githubTestsWalkRecords(cursor)-githubTestsWalkRecords(before), cursor.LateRuns)
			after := cursor
			after.Index = index + 1
			after.NextURL = page.CursorBefore
			after.AnchorID = githubTestsAnchorID(run)
			// A finished page names the next page at index 0 (the short next URL); the anchor rides in AnchorID and the
			// resume checks it on this page (githubTestsResumeStart).
			if after.Index >= len(page.Items) {
				after.Index = 0
				after.NextURL = page.CursorAfter
				if after.NextURL == "" {
					after = endRunsRange(after)
				}
			}
			// A listed run is one small emission the resume re-reads from its before cursor: the executor may commit
			// several contiguous runs in one chunk group (CHAOS-14408).
			if err := emitCursorAs(before, after, CompleteRouteBatch{Effects: effects}, false, handler.CoalesceRuns); err != nil {
				return err
			}
			cursor = after
		}
		// An empty page, or a resume at the end of the last page (its run checked, nothing left on it), ends here.
		if len(page.Items) == 0 || start == len(page.Items) && page.CursorAfter == "" {
			cursor.NextURL = page.CursorAfter
			cursor.Index = 0
			if cursor.NextURL == "" {
				cursor = endRunsRange(cursor)
			}
		}
		// The visitor follows the next link as soon as this returns: a page that ends at the bound stops here. A listing that
		// ended at the bound has left the phase, so its inventory is complete.
		if cursor.Phase == githubTestsRunsPhase && walkBoundHit() {
			return errGitHubTestsWalkBound
		}
		return nil
	}

	for cursor.Phase == githubTestsRunsPhase {
		if walkBoundHit() {
			credited, creditErr := creditAtBound(githubTestsRunsPhase)
			if creditErr != nil {
				return creditErr
			}
			if credited {
				cursor = reseek(cursor)
				continue
			}
		}
		if walkBoundHit() && cursor.LateRuns {
			logGitHubTestsWalkBound(cursor, claim, githubTestsLateRerunRunsComponent, maxWalkRecords)
			cursor = githubTestsHoldDeepStopAfterDeletion(cursor, client, claim, githubTestsRunInventoryComponent)
			cursor = recordGitHubTestsLateRerunRunsStop(cursor, client, claim, githubTestsPageBudgetCause, cursor.LateRunPages)
			cursor = nextRunsListing(cursor)
			cursor.NextURL, cursor.Index = "", 0
			break
		}
		if walkBoundHit() {
			cursor = recordGitHubTestsWalkBound(cursor, client, claim, githubTestsRunInventoryComponent, maxWalkRecords)
			break
		}
		if cursor.LateRuns {
			// The deep range: its own budget; a stop records the advancing
			// late_rerun_runs pair and the phase goes on.
			allowance, budgetErr := githubTestsListingAllowance(lateMaxPages, cursor.LateRunPages, githubTestsFreePages(cursor))
			exhausted := errors.Is(budgetErr, ErrPaginationCapExceeded)
			if budgetErr != nil && !exhausted {
				return budgetErr
			}
			if exhausted {
				moved, movedErr := movedAtSpentBudget(githubTestsRunsPhase)
				if movedErr != nil {
					return movedErr
				}
				if moved {
					cursor = reseek(cursor)
					continue
				}
			}
			if !exhausted {
				pageOptions := providerfoundation.GitHubPageOptions{Path: root + "/actions/runs", Route: githubRunsRoute,
					Query: githubTestsLateRerunRunsQuery(claim, lookback, lateLookback, cursor.RangeHi), DataKey: "workflow_runs",
					MaxPages: allowance, InitialURL: cursor.NextURL}
				collection, visitErr := providerfoundation.VisitGitHubLinkPages(ctx, client, pageOptions, emitRunPage)
				if errors.Is(visitErr, errGitHubTestsReseek) {
					cursor = reseek(cursor)
					continue
				}
				if errors.Is(visitErr, errGitHubTestsWalkBound) {
					logGitHubTestsWalkBound(cursor, claim, githubTestsLateRerunRunsComponent, maxWalkRecords)
					cursor = githubTestsHoldDeepStopAfterDeletion(cursor, client, claim, githubTestsRunInventoryComponent)
					visitErr, exhausted = nil, true
				}
				if visitErr != nil {
					return visitErr
				}
				exhausted = exhausted || collection.PageBudgetExhausted
			}
			if exhausted {
				cursor = recordGitHubTestsLateRerunRunsStop(cursor, client, claim, githubTestsPageBudgetCause, cursor.LateRunPages)
				cursor = nextRunsListing(cursor)
				cursor.NextURL, cursor.Index = "", 0
			}
			continue
		}
		query := githubTestsRunsQuery(claim, lookback, cursor.RangeHi)
		allowance, budgetErr := githubTestsListingAllowance(
			(maxRuns+nativePerPage-1)/nativePerPage, cursor.RunPages, githubTestsFreePages(cursor))
		if errors.Is(budgetErr, ErrPaginationCapExceeded) {
			moved, movedErr := movedAtSpentBudget(githubTestsRunsPhase)
			if movedErr != nil {
				return movedErr
			}
			if moved {
				cursor = reseek(cursor)
				continue
			}
			cursor = recordGitHubTestsInventoryTruncation(
				cursor, client, claim, githubTestsRunInventoryComponent, cursor.RunPages)
			break
		}
		if budgetErr != nil {
			return budgetErr
		}
		pageOptions := providerfoundation.GitHubPageOptions{Path: root + "/actions/runs", Route: githubRunsRoute, Query: query, DataKey: "workflow_runs", MaxPages: allowance, InitialURL: cursor.NextURL}
		collection, visitErr := providerfoundation.VisitGitHubLinkPages(ctx, client, pageOptions, emitRunPage)
		if errors.Is(visitErr, errGitHubTestsReseek) {
			cursor = reseek(cursor)
			continue
		}
		if errors.Is(visitErr, errGitHubTestsWalkBound) && cursor.LateRuns {
			// The runs range ended at the bound and went on to the deep range: the loop ends the deep range.
			continue
		}
		if errors.Is(visitErr, errGitHubTestsWalkBound) {
			cursor = recordGitHubTestsWalkBound(cursor, client, claim, githubTestsRunInventoryComponent, maxWalkRecords)
			break
		}
		if visitErr != nil {
			return visitErr
		}
		if collection.PageBudgetExhausted {
			cursor = recordGitHubTestsInventoryTruncation(
				cursor, client, claim, githubTestsRunInventoryComponent, cursor.RunPages)
			break
		}
	}

	if cursor.Phase == githubTestsRunsPhase {
		cursor.Phase = githubTestsArtifactsPhase
		cursor.NextURL, cursor.Index, cursor.LateRuns, cursor.AnchorID = "", 0, false, ""
		// A runs phase stopped inside a peeled or re-sought range must not hand its range end to the artifacts listing.
		cursor.RangeHi, cursor.RangeItems, cursor.RangeOldest, cursor.RangeOldestItems, cursor.ReseekPages, cursor.PagesEntered = "", 0, "", 0, 0, 0
		cursor = githubTestsClearCutKeys(cursor)
	}
	handlerOpenRan := false
	// collectOpenRuns is the open phase: stored pipeline rows still open and
	// older than the runs listing reaches are re-read by id, oldest first, up to
	// the cap fixed at the phase start. A run GitHub still has is written like a
	// listed run (pipeline row, jobs, acceptance); a run it no longer has keeps
	// its row with the status testops.StatusDeleted.
	// skipOpenRun ends the re-read of one open run whose by-id read or jobs
	// pages failed: an error of that run alone moves the keyset past it in an
	// emission with no rows (a skip writes nothing; the next unit reads it
	// again), any other error, or the breaker's last transient, is returned.
	skipOpenRun := func(before githubTestsChunkCursor, row githubTestsPipelineRow, cause error, jobs bool) error {
		reason, status, route, transient, skip := githubTestsOpenRunSkip(ctx, cause, jobs)
		if !skip || transient && cursor.OpenTransientRun+1 >= githubTestsOpenTransientBreaker {
			return cause
		}
		effects, effectErr := testOpsEffects(nil, nil, nil, nil, nil, nil)
		if effectErr != nil {
			return effectErr
		}
		after := cursor
		after.OpenSkipped++
		if transient {
			after.OpenTransientRun++
		}
		after.OpenAfterStarted = row.StartedAt.UTC().Format(time.RFC3339Nano)
		after.OpenAfterRunID = row.RunID
		after.OpenSkipEvent = &githubTestsOpenSkipEvent{RunID: row.RunID, Reason: reason, Status: status, Route: route}
		// The executor records the skip when it confirms this emission's chunk committed (ObserveCommittedChunk).
		if err := emitCursor(before, after, CompleteRouteBatch{Effects: effects}, false); err != nil {
			return err
		}
		cursor = after
		cursor.OpenSkipEvent = nil
		return nil
	}
	collectOpenRuns := func() error {
		// The open phase reads stored pipeline rows: a runs-phase group the executor still holds must be durable first.
		if coalesceHeld {
			if err := emit(ChunkRouteEmission{Barrier: true}); err != nil {
				return err
			}
			coalesceHeld = false
		}
		if claim.SinceAt == nil {
			cursor.Phase = "done"
			return nil
		}
		// The open phase re-reads runs older than the window; the next unit re-reads what a bound stop leaves, so the stop adds
		// no inventory observation. It is published as a cap stop (open_runs_cap_reached): the open rows were not all re-read.
		stopAtWalkBound := func() bool {
			if !windowBoundHit() {
				return false
			}
			cursor.OpenCapReached = true
			logGitHubTestsWalkBound(cursor, claim, githubTestsOpenRunsWalkBoundComponent, maxWalkRecords)
			return true
		}
		if stopAtWalkBound() {
			return nil
		}
		startedBefore := claim.SinceAt.UTC().Add(-lookback)
		if cursor.OpenCap == 0 {
			backlog, err := handler.OpenRuns.OpenRunBacklog(ctx, claim, repoID, startedBefore, githubTestsOpenRunBacklogThreshold+1)
			if err != nil {
				return err
			}
			cursor.OpenCap = githubTestsOpenRunCap(backlog)
		}
		for cursor.OpenDone < cursor.OpenCap && cursor.OpenSkipped < cursor.OpenCap {
			var afterStarted time.Time
			if cursor.OpenAfterStarted != "" {
				parsed, err := time.Parse(time.RFC3339Nano, cursor.OpenAfterStarted)
				if err != nil {
					return ErrChunkCheckpointConflict
				}
				afterStarted = parsed
			}
			rows, err := handler.OpenRuns.OpenRuns(ctx, claim, repoID, startedBefore, afterStarted,
				cursor.OpenAfterRunID, min(cursor.OpenCap-cursor.OpenDone, 50))
			if err != nil {
				return err
			}
			cursor.Requests = invocationBaseRequests + requests
			if len(rows) == 0 {
				cursor.Phase = "done"
				return nil
			}
			for _, row := range rows {
				if cursor.OpenSkipped >= cursor.OpenCap {
					break
				}
				if stopAtWalkBound() {
					return nil
				}
				if hit, remaining := rateObserved.belowFloor(); hit {
					cursor.OpenFloorHit, cursor.OpenFloorRemaining = true, remaining
					slog.Warn(
						"open phase stopped: the installation's rate-limit headroom is under the floor, or its headers are missing",
						"provider", claim.Provider, "dataset", claim.Dataset, "unit", claim.ID, "repository", logging.ProviderText(cursor.Repo),
						"remaining", remaining, "limit", rateObserved.limit, "floor", rateObserved.floor(), "reread", cursor.OpenDone,
					)
					cursor.Phase = "done"
					return nil
				}
				before := cursor
				run, found, fetchErr := fetchGitHubTestsRun(ctx, client, root, row.RunID)
				cursor.Requests = invocationBaseRequests + requests
				if fetchErr != nil {
					if err := skipOpenRun(before, row, fetchErr, false); err != nil {
						return err
					}
					continue
				}
				var pipelines []githubTestsPipelineRow
				jobs := make([]githubTestsJobRow, 0)
				acceptance := make([]githubTestsAcceptanceRow, 0)
				if !found {
					pipelines = []githubTestsPipelineRow{githubTestsDeletedRow(row, normalizedAt)}
					slog.Info(
						"open run no longer exists at the provider; its row is marked deleted",
						"provider", claim.Provider, "dataset", claim.Dataset, "unit", claim.ID,
						"repository", logging.ProviderText(cursor.Repo), logging.ProviderIDAttr("run", row.RunID),
					)
				} else if pipeline, include := normalizeGitHubTestsPipeline(claim, repoID, run, normalizedAt); include {
					var detailErr error
					jobs, acceptance, _, detailErr = collectRunDetail(run, pipeline)
					if detailErr != nil {
						if err := skipOpenRun(before, row, detailErr, true); err != nil {
							return err
						}
						continue
					}
					pipelines = []githubTestsPipelineRow{pipeline}
					cursor.Pipelines++
					cursor.Jobs += len(jobs)
					cursor.Acceptance += len(acceptance)
				}
				effects, effectErr := testOpsEffects(pipelines, jobs, acceptance, nil, nil, nil)
				if effectErr != nil {
					return effectErr
				}
				after := cursor
				after.OpenDone++
				after.OpenTransientRun = 0
				after.OpenAfterStarted = row.StartedAt.UTC().Format(time.RFC3339Nano)
				after.OpenAfterRunID = row.RunID
				if err := emitCursor(before, after, CompleteRouteBatch{Effects: effects}, false); err != nil {
					return err
				}
				cursor = after
			}
		}
		cursor.OpenCapReached = cursor.OpenDone >= cursor.OpenCap
		cursor.OpenSkipLimitHit = !cursor.OpenCapReached
		cursor.Phase = "done"
		return nil
	}
	logOpenPhase := func() {
		if !handlerOpenRan {
			return
		}
		slog.Info(
			"open phase finished",
			"provider", claim.Provider, "dataset", claim.Dataset, "unit", claim.ID, "repository", logging.ProviderText(cursor.Repo),
			"reread", cursor.OpenDone, "cap", cursor.OpenCap, "cap_reached", cursor.OpenCapReached,
			"floor_hit", cursor.OpenFloorHit, "skipped", cursor.OpenSkipped, "skip_limit_hit", cursor.OpenSkipLimitHit,
			"rate_limit_remaining", rateObserved.remaining,
			"rate_limit_limit", rateObserved.limit, "rate_limit_floor", rateObserved.floor(),
		)
	}
	for cursor.Phase == githubTestsArtifactsPhase {
		if windowBoundHit() {
			credited, creditErr := creditAtBound(githubTestsArtifactsPhase)
			if creditErr != nil {
				return creditErr
			}
			if credited {
				cursor.ArchivesSeen, cursor.ArchivesUnreadable = nil, nil
				cursor = reseek(cursor)
				continue
			}
		}
		if windowBoundHit() {
			cursor = recordGitHubTestsArtifactsWalkBound(cursor, client, claim, maxWalkRecords)
			break
		}
		artifactQuery := githubTestsArtifactRunQueryAt(claim, cursor.RangeHi)
		if cursor.Relist == githubTestsRelistWalking {
			artifactQuery = githubTestsRelistQueryAt(claim, lateLookback, cursor.RangeHi)
		}
		artifactPage := func(page providerfoundation.PageVisit) error {
			cursor.Pages++
			// See emitRunPage's identical unconditional refresh above: this
			// listing page's own wire attempt must land in cursor.Requests even
			// when no item on it reaches a per-artifact download below.
			cursor.Requests = invocationBaseRequests + requests
			// First-entry-only counting, exactly as in emitRunPage above. This
			// twin has never fired in production only because no unit has ever
			// survived the runs phase to reach it (CHAOS-4130).
			pageEnd, pageEndErr := resumePageEnd(page)
			if pageEndErr != nil {
				return pageEndErr
			}
			start, action := githubTestsResumeStart(cursor, page, firstPage, pageEnd, client, claim, githubTestsArtifactsPhase)
			firstPage = false
			// The page left for a re-seek is not charged; a positional re-walk walks it, so it is (as before).
			if !(cursor.NextURL == page.CursorBefore && cursor.Index > 0) && action != githubTestsResumeReseek {
				cursor = githubTestsEnterPage(cursor, func(c *githubTestsChunkCursor) { c.ArtifactPages++ })
			}
			if action != githubTestsResumeAt {
				// A re-seek (githubTestsReseek) reads again the runs of the anchor's created second, and a positional
				// re-walk the whole page, which re-downloads and re-counts their artifacts -- including ones an earlier attempt already reflected in these counters.
				// That can cross the totality floor on a truly small real sample (e.g. a genuine 1-artifact repo whose
				// page moved under a resume): the SAME artifact would be counted twice, satisfying seen>=2 with only one
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
				if action == githubTestsResumeReseek {
					creditDeletion(githubTestsResumeDeleted(cursor, page, pageEnd), githubTestsArtifactsPhase)
					return errGitHubTestsReseek
				}
			}
			for index := start; index < len(page.Items); index++ {
				if windowBoundHit() {
					return errGitHubTestsWalkBound
				}
				before := cursor
				run, err := decodeGitHubWorkflowRun(page.Items[index])
				if err != nil {
					return providerfoundation.RouteFailure(githubRunsRoute, providerfoundation.ReasonBodyDecode, err)
				}
				pipeline, include := normalizeGitHubTestsPipeline(claim, repoID, run, normalizedAt)
				cursor.RangeItems++
				if oldest := githubTestsOlderCreated(cursor.RangeOldest, parseGitHubWorkflowTime(run.CreatedAt)); oldest != cursor.RangeOldest {
					cursor.RangeOldest, cursor.ActiveCutKeys = oldest, ""
				}
				cutSecond, cutKey := githubTestsCutRunKey(run)
				suites := []testSuiteResultRow(nil)
				cases := []testCaseResultRow(nil)
				coverage := []coverageSnapshotRow(nil)
				var completeness []testOpsRunReportCompletenessRow
				if include && githubTestsRunReadable(run) && !githubTestsArtifactRunOutside(pipeline, claim, cursor.Relist == githubTestsRelistWalking) &&
					githubTestsArtifactInterval(claim, cursor.Relist == githubTestsRelistWalking, lateLookback).admitsUnder(pipeline.QueuedAt, cursor.RangeHi) &&
					!githubTestsSkipCutRun(cursor, cutSecond, cutKey) {
					// The run's losses are the deltas of the cursor's exact loss counters across the run (CHAOS-13826).
					lossBefore := githubTestsTakeRunLossCounts(cursor)
					artPage, pageErr := providerfoundation.CollectGitHubLinkPages(ctx, client, providerfoundation.GitHubPageOptions{
						Path:  root + "/actions/runs/" + url.PathEscape(pipeline.RunID) + "/artifacts",
						Route: githubRunArtifactsRoute,
						Query: url.Values{"per_page": {strconv.Itoa(githubTestsArtifactsPerPage)}}, DataKey: "artifacts", MaxPages: 1,
					})
					if pageErr != nil {
						return pageErr
					}
					cursor.Requests = invocationBaseRequests + requests
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
						artifact, err := decodeGitHubTestsArtifact(artifactRaw)
						if err != nil {
							return providerfoundation.RouteFailure(githubRunArtifactsRoute, providerfoundation.ReasonBodyDecode, err)
						}
						if artifact.ID == "" {
							return providerfoundation.ErrNormalizationInvalid
						}
						if selected, reason := githubTestsArtifactSelectionSeam(artifact.Name); !selected {
							switch reason {
							case githubTestsExclusionReasonSuffix:
								cursor.ExcludedNonReportSuffix++
							case githubTestsExclusionReasonPrefix:
								cursor.ExcludedNonReportPrefix++
							}
							// Bounded the same way as GitHubTestsSkippedArtifact.Name
							// (CHAOS-4588/CHAOS-4591, codex round 1, P1): a
							// provider name is unbounded, and this sample
							// shares the same cursor byte budget.
							cursor.ExcludedArtifactSample = githubTestsAddExcludedSample(
								cursor.ExcludedArtifactSample, githubTestsTruncateArtifactName(artifact.Name)+" ("+reason+")",
							)
							continue
						}
						artifactItems = append(artifactItems, artifact)
					}
					// One artifact per name, the newest, before the cap (CHAOS-13873): an older attempt's copy of a re-run job's
					// report is not read again next to the new one.
					artifactItems, superseded := githubTestsKeepNewestArtifactPerName(artifactItems)
					supersededIDs := githubTestsSupersededIDs(superseded)
					for _, artifact := range superseded {
						cursor.ExcludedArtifactSample = githubTestsAddExcludedSample(
							cursor.ExcludedArtifactSample, githubTestsTruncateArtifactName(artifact.Name)+" ("+githubTestsExclusionReasonSuperseded+")",
						)
					}
					// CHAOS-13980: GitHub says an expired artifact's bytes are deleted, so a report-shaped one is lost for good. It is
					// decided here, after the newest-per-name rule (an older attempt's expired copy is superseded, not a loss) and BEFORE
					// the cap (an expired artifact never takes a download slot from a readable shard). Recorded once per (run, artifact):
					// observation, metric, durable marker. Not counted in ArchivesSeen/ArchivesUnreadable: no download happened, so it says
					// nothing about the channel (same rule as oversized below). Every expired artifact that passed the selection seam above
					// counts: its contents are unknown, so the route fails closed (the seam already dropped the known non-report names).
					artifactItems, expiredItems := githubTestsSplitExpiredArtifacts(artifactItems)
					reportsExpected := len(artifactItems) + len(expiredItems)
					nonReportArchives := 0
					for _, artifact := range expiredItems {
						if githubTestsSkippedMarkerRecorded(cursor.SkippedArtifacts, pipeline.RunID, string(artifact.ID), githubTestsArtifactExpiredCause) {
							continue
						}
						cursor.Incomplete = recordGitHubTestsSkippedArtifact(
							cursor.Incomplete, client, claim, cursor.Repo, pipeline.RunID,
							githubTestsArtifactExpiredCause,
						)
						cursor.SkippedArtifacts, cursor.SkippedArtifactsOverflow, cursor.SkippedArtifactCauseOverflow, cursor.SkippedArtifactCauseCount = appendGitHubTestsSkippedArtifact(
							cursor.SkippedArtifacts, cursor.SkippedArtifactsOverflow, cursor.SkippedArtifactCauseOverflow, cursor.SkippedArtifactCauseCount,
							GitHubTestsSkippedArtifact{
								RunID: pipeline.RunID, ArtifactID: string(artifact.ID), Name: artifact.Name,
								Cause: githubTestsArtifactExpiredCause,
							},
						)
					}
					// CHAOS-14000: the cap counts only artifacts whose zip directory lists a report member; one that lists none is
					// excluded, never a loss.
					// The checks' own wire count replaces the Do calls they made, so a transport resend is counted too.
					requestsBeforeAdmission := requests
					artifactItems, noReportItems, artifactsCapped, admitRequests, admitErr := githubTestsAdmitReportArtifacts(ctx, client, root, artifactItems, maxArtifacts)
					requests = requestsBeforeAdmission + admitRequests
					cursor.Requests = invocationBaseRequests + requests
					if admitErr != nil {
						return admitErr
					}
					// An artifact whose directory lists no report member is not a report source of the run (CHAOS-13826).
					reportsExpected -= len(noReportItems)
					for _, artifact := range noReportItems {
						cursor.ExcludedArtifactSample = githubTestsAddExcludedSample(cursor.ExcludedArtifactSample, githubTestsNoReportSampleRecord(artifact))
					}
					switch {
					case artifactsCapped:
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
					githubTestsSortArtifactsForAdmission(artifactItems)
					// The run's report rows are decided once every artifact is parsed (CHAOS-13794): duplicates are dropped by content,
					// then the survivors are admitted whole, smallest declared size first, under the run bound. An artifact that does
					// not fit is skipped whole with a marker and the walk goes on, so the order GitHub lists the artifacts in never
					// decides which one survives. Each cause is recorded once per run, a marker once per skipped artifact.
					var parsedReports []githubTestsParsedReport
					// Skips of a replayed artifact are not counted again in the unit, but they are still this run's losses.
					var replayedRunLosses []string
					// A deterministic whole-artifact skip whose marker is in the sample is a replay: not counted again, but still a
					// loss of this run (CHAOS-14223). artifact_unavailable is not deduplicated: a later look can read the artifact.
					replayedSkip := func(artifact githubTestsArtifactPayload, cause string) bool {
						if !githubTestsSkippedMarkerRecorded(cursor.SkippedArtifacts, pipeline.RunID, string(artifact.ID), cause) {
							return false
						}
						replayedRunLosses = append(replayedRunLosses, cause)
						return true
					}
					heldRows := 0
					reportRowsTruncated := false
					skipReportOverBound := func(artifact githubTestsArtifactPayload) {
						if !reportRowsTruncated {
							reportRowsTruncated = true
							cursor = recordGitHubTestsPerRunTruncation(
								cursor, client, claim, githubTestsRunReportsComponent,
								githubTestsPerRunCapCause, pipeline.RunID, heldRows,
							)
						}
						cursor.SkippedArtifacts, cursor.SkippedArtifactsOverflow, cursor.SkippedArtifactCauseOverflow, cursor.SkippedArtifactCauseCount = appendGitHubTestsSkippedArtifact(
							cursor.SkippedArtifacts, cursor.SkippedArtifactsOverflow, cursor.SkippedArtifactCauseOverflow, cursor.SkippedArtifactCauseCount,
							GitHubTestsSkippedArtifact{
								RunID: pipeline.RunID, ArtifactID: string(artifact.ID), Name: artifact.Name,
								Cause: githubTestsPerRunCapCause,
							},
						)
					}
					for _, artifact := range artifactItems {
						if artifact.Expired {
							continue // unreachable: expired artifacts are split out before the cap (CHAOS-13980)
						}
						archive, _, notFound, downloadErr := downloadGitHubTestsListedArtifact(ctx, client, root, string(artifact.ID), maxArtifactBytes, artifact.listedSize())
						cursor.Requests = invocationBaseRequests + requests
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
							// An artifact whose body exceeded the configured
							// per-artifact cap (githubTestsMaxDownloadSize by
							// default) is provider data
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
							//
							// Deliberately NOT counted toward
							// ArchivesSeen/ArchivesUnreadable (CHAOS-4315/
							// CHAOS-4185 interaction fix): a repository whose
							// only recent artifacts are legitimately oversized
							// (e.g. a 100+MB nightly bundle) downloaded fine --
							// we read every byte up to the cap -- so this is not
							// evidence our channel to read artifacts is broken,
							// the exact reasoning the routine-404/410 branch
							// below already applies. Bumping both counters here
							// (as this branch used to) fed a deliberate CHAOS-4315
							// policy skip into the CHAOS-4185 totality gate's own
							// denominator AND numerator: with >=2 oversized
							// artifacts and nothing else, seen==unreadable>=floor
							// tripped ErrGitHubTestsAllArtifactsUnreadable and
							// terminalized a unit CHAOS-4315 says must not fail,
							// discarding its committed rows every hour and
							// pinning since_at forever (CHAOS-4315 removed the
							// unit-level failure through the front door, this
							// reinstated it through the back door). Excluding
							// from BOTH counters -- not just Unreadable -- keeps
							// a genuinely broken channel mixed with oversized
							// skips still visible to the gate (3 corrupt + 2
							// oversized -> 3/3, not 3/5).
							if errors.Is(downloadErr, ErrGitHubTestsArtifactOversized) {
								if replayedSkip(artifact, githubTestsArtifactOversizedCause) {
									continue
								}
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
								// (githubTestsSkippedArtifactLogSample). No
								// ArchivesSeen/ArchivesUnreadable bump here --
								// see the doc comment on the branch above.
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
							// round 3). Counted on ArtifactsExcludedNotFound
							// purely for telemetry (the skip summary's
							// totality_excluded_not_found) -- never fed to the
							// gate itself.
							cursor.ArtifactsExcludedNotFound++
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
							if replayedSkip(artifact, githubTestsUnreadableArchiveCause) {
								cursor.ArchivesUnreadable = bumpGitHubTestsArchiveCounter(cursor.ArchivesUnreadable)
								continue
							}
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
						rows, parseErr := parseGitHubTestsReportArtifact(archive, string(artifact.ID), githubTestsReportSlotFor(run, artifact), supersededIDs[githubTestsArtifactSlotName(artifact)], repoID, pipeline.RunID, claim.OrgID, pipeline.StartedAtPtr(), pipeline.FinishedAt, normalizedAt)
						if parseErr != nil {
							// An archive that will not open is provider data, not
							// a fault of this unit. Skip it exactly as an expired
							// or empty artifact is skipped a few lines above, and
							// keep walking: one bad artifact must not cost the
							// healthy ones or the whole unit (CHAOS-4177).
							if errors.Is(parseErr, ErrGitHubTestsArchiveUnreadable) {
								if replayedSkip(artifact, githubTestsUnreadableArchiveCause) {
									cursor.ArchivesUnreadable = bumpGitHubTestsArchiveCounter(cursor.ArchivesUnreadable)
									continue
								}
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
						// A resumed walk can re-anchor and read this artifact again. The member skips are deterministic, so a (run,
						// artifact, cause) already in the marker sample is a replay: its contribution is dropped, not counted twice
						// (CHAOS-14030, CHAOS-14223).
						var replayedMember []string
						rows, replayedMember = rows.withoutReplayedMemberSkips(cursor.SkippedArtifacts, pipeline.RunID, string(artifact.ID))
						replayedRunLosses = append(replayedRunLosses, replayedMember...)
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
						recordGitHubTestsNonReportMembers(client, claim, rows)
						client.Metrics.RecordDuplicateTestCase(claim.Provider, claim.Dataset, rows.DuplicateCases)
						if rows.DuplicateCases > 0 {
							slog.Info(
								"within-suite duplicate test-case names disambiguated with an ordinal suffix",
								"provider", claim.Provider, "dataset", claim.Dataset, "unit", claim.ID,
								"repository", logging.ProviderText(cursor.Repo), logging.ProviderIDAttr("run", pipeline.RunID), "count", rows.DuplicateCases,
							)
						}
						client.Metrics.RecordNonFiniteTestTime(claim.Provider, claim.Dataset, rows.NonFiniteTimes)
						if rows.NonFiniteTimes > 0 {
							slog.Info(
								"non-finite test time stored as NULL",
								"provider", claim.Provider, "dataset", claim.Dataset, "unit", claim.ID,
								"repository", logging.ProviderText(cursor.Repo), logging.ProviderIDAttr("run", pipeline.RunID), "count", rows.NonFiniteTimes,
							)
						}
						client.Metrics.RecordDuplicateTestSuite(claim.Provider, claim.Dataset, rows.DuplicateSuites)
						if rows.DuplicateSuites > 0 {
							slog.Info(
								"sibling suite collision resolved: same-named suite objects disambiguated with an ordinal suffix",
								"provider", claim.Provider, "dataset", claim.Dataset, "unit", claim.ID,
								"repository", logging.ProviderText(cursor.Repo), logging.ProviderIDAttr("run", pipeline.RunID), "count", rows.DuplicateSuites,
							)
						}
						// Hold the parsed rows until the run is decided; an artifact that would take the held rows past
						// githubTestsMaxHeldReportRowsPerRun is skipped whole before it is held.
						held := githubTestsParsedReport{ID: artifact.ID, Name: artifact.Name, Suites: rows.Suites, Cases: rows.Cases, Coverage: rows.Coverage, Reports: rows.ReportsRead}
						if held.Reports == 0 && len(rows.SkippedArtifacts) == 0 && len(reportIncomplete) == 0 && len(replayedMember) == 0 {
							nonReportArchives++ // read whole and held no report: not a report source of the run (CHAOS-13826)
						}
						if heldRows+held.rows() > githubTestsMaxHeldReportRowsPerRun {
							skipReportOverBound(artifact)
							continue
						}
						heldRows += held.rows()
						parsedReports = append(parsedReports, held)
					}
					plan, planErr := githubTestsPlanRunReports(parsedReports)
					if planErr != nil {
						return fmt.Errorf("%w: sizing report rows: %v", ErrGitHubTestsIncomplete, planErr)
					}
					reportGitHubTestsPlannerKeeps(ctx, claim.Provider, claim.Dataset, claim.ID, cursor.Repo, pipeline.RunID, parsedReports, plan)
					recordedCauses := map[string]bool{}
					markReport := func(index int, cause string) {
						report := parsedReports[index]
						cursor.SkippedArtifacts, cursor.SkippedArtifactsOverflow, cursor.SkippedArtifactCauseOverflow, cursor.SkippedArtifactCauseCount = appendGitHubTestsSkippedArtifact(
							cursor.SkippedArtifacts, cursor.SkippedArtifactsOverflow, cursor.SkippedArtifactCauseOverflow, cursor.SkippedArtifactCauseCount,
							GitHubTestsSkippedArtifact{RunID: pipeline.RunID, ArtifactID: string(report.ID), Name: report.Name, Cause: cause},
						)
						if cause == githubTestsPerRunCapCause {
							if !reportRowsTruncated {
								reportRowsTruncated = true
								cursor = recordGitHubTestsPerRunTruncation(
									cursor, client, claim, githubTestsRunReportsComponent, cause, pipeline.RunID, heldRows,
								)
							}
							return
						}
						if !recordedCauses[cause] {
							recordedCauses[cause] = true
							cursor = recordGitHubTestsRunReportsDuplicate(cursor, client, claim, cause, pipeline.RunID, report.rows())
						}
					}
					for _, drop := range plan.Dropped {
						markReport(drop.Index, drop.Cause)
					}
					var mismatchIDs []string
					for _, mismatch := range plan.Mismatch {
						markReport(mismatch.Index, githubTestsAggregateMismatchCause)
						mismatchIDs = append(mismatchIDs, string(parsedReports[mismatch.Index].ID))
						for _, part := range mismatch.Parts {
							mismatchIDs = append(mismatchIDs, string(parsedReports[part].ID))
						}
					}
					if len(mismatchIDs) > 0 {
						slog.Warn(
							"provider run report artifacts match by case identity but disagree on outcome; all are kept and the unit is not advanced",
							"provider", claim.Provider, "dataset", claim.Dataset, "unit", claim.ID,
							"repository", logging.ProviderText(cursor.Repo), logging.ProviderIDAttr("run", pipeline.RunID),
							"artifact_ids", logging.ProviderText(strings.Join(mismatchIDs, ",")),
						)
					}
					for _, index := range plan.OverBound {
						markReport(index, githubTestsPerRunCapCause)
					}
					for _, index := range plan.Admitted {
						suites = append(suites, parsedReports[index].Suites...)
						cases = append(cases, parsedReports[index].Cases...)
						coverage = append(coverage, parsedReports[index].Coverage...)
					}
					// One verdict row per run, after its last emission's rows are decided (CHAOS-13826).
					lossAfter := githubTestsTakeRunLossCounts(cursor)
					for _, cause := range replayedRunLosses {
						if lossAfter.skipped[cause] <= lossBefore.skipped[cause] {
							lossAfter.skipped[cause] = lossBefore.skipped[cause] + 1
						}
					}
					completeness = githubTestsRunReportCompleteness(
						pipeline, run, lossBefore, lossAfter,
						reportsExpected-nonReportArchives, len(expiredItems), githubTestsAdmittedReports(parsedReports, plan.Admitted), normalizedAt,
					)
					// Only a run read whole (one complete verdict) is recorded, in the same emission as its rows; a read with no report
					// source or any loss removes the run's key, so a run read earlier in this range is read again by the revisit.
					if len(completeness) == 1 && completeness[0].Complete {
						cursor = githubTestsRecordCutRun(cursor, cutSecond, cutKey)
					} else {
						cursor = githubTestsDropCutRun(cursor, cutKey)
					}
				}
				cursor.Suites += len(suites)
				cursor.Cases += len(cases)
				cursor.Coverage += len(coverage)
				effects, effectErr := testOpsEffects(nil, nil, nil, suites, cases, coverage, completeness...)
				if effectErr != nil {
					return effectErr
				}
				creditRecords(cutSecond, githubTestsWalkRecords(cursor)-githubTestsWalkRecords(before), false)
				after := cursor
				after.Index = index + 1
				after.NextURL = page.CursorBefore
				after.AnchorID = githubTestsAnchorID(run)
				// A finished page names the next page at index 0, as in the runs phase.
				if after.Index >= len(page.Items) {
					after.Index = 0
					after.NextURL = page.CursorAfter
					// Publish the next phase (open, else the terminal one) the
					// moment the last artifact page is consumed, exactly as the
					// runs phase publishes "artifacts". Without it a cursor at
					// {phase:artifacts, next_url:"", index:0} is
					// indistinguishable from a phase that has not started, so a
					// continuation landing on the last item of the last page
					// re-walked the WHOLE artifacts phase -- spending two fresh
					// pages of budget per lap until the cap stopped it
					// (CHAOS-4130).
					if after.NextURL == "" {
						after = endArtifactsRange(after)
					}
				}
				if err := emitCursor(before, after, CompleteRouteBatch{Effects: effects}, false); err != nil {
					return err
				}
				cursor = after
			}
			if len(page.Items) == 0 || start == len(page.Items) && page.CursorAfter == "" {
				cursor.NextURL = page.CursorAfter
				cursor.Index = 0
				if cursor.NextURL == "" {
					cursor = endArtifactsRange(cursor)
				}
			}
			if cursor.Phase == githubTestsArtifactsPhase && windowBoundHit() {
				return errGitHubTestsWalkBound
			}
			return nil
		}
		// A resume inside a page (a finished page included: its cursor keeps the page to check the anchor) re-reads a
		// charged page, so that read is free, as in the runs phase.
		allowance, budgetErr := githubTestsListingAllowance((maxRuns+nativePerPage-1)/nativePerPage, cursor.ArtifactPages, githubTestsFreePages(cursor))
		if cursor.Relist == githubTestsRelistWalking {
			// The deep range: its own budget, under the shared listing allowance (charged on entering a page).
			allowance, budgetErr = githubTestsListingAllowance(lateMaxPages, cursor.ArtifactPages, githubTestsFreePages(cursor))
		}
		if errors.Is(budgetErr, ErrPaginationCapExceeded) {
			moved, movedErr := movedAtSpentBudget(githubTestsArtifactsPhase)
			if movedErr != nil {
				return movedErr
			}
			if moved {
				// A re-seek reads runs again: the totality counters become unknown, as in the visitor.
				cursor.ArchivesSeen, cursor.ArchivesUnreadable = nil, nil
				cursor = reseek(cursor)
				continue
			}
		}
		if errors.Is(budgetErr, ErrPaginationCapExceeded) && cursor.Relist == githubTestsRelistWalking {
			cursor = recordGitHubTestsRelistStop(cursor, client, claim, githubTestsPageBudgetCause, cursor.RangeHi, cursor.RangeItems)
			break
		}
		if errors.Is(budgetErr, ErrPaginationCapExceeded) {
			cursor = recordGitHubTestsInventoryTruncation(
				cursor, client, claim, githubTestsArtifactInventoryComponent, cursor.ArtifactPages)
			break
		}
		if budgetErr != nil {
			return budgetErr
		}
		collection, visitErr := providerfoundation.VisitGitHubLinkPages(ctx, client, providerfoundation.GitHubPageOptions{Path: root + "/actions/runs", Route: githubRunsRoute, Query: artifactQuery, DataKey: "workflow_runs", MaxPages: allowance, InitialURL: cursor.NextURL}, artifactPage)
		if errors.Is(visitErr, errGitHubTestsReseek) {
			cursor = reseek(cursor)
			continue
		}
		if errors.Is(visitErr, errGitHubTestsWalkBound) {
			cursor = recordGitHubTestsArtifactsWalkBound(cursor, client, claim, maxWalkRecords)
			break
		}
		if visitErr != nil {
			return visitErr
		}
		if collection.PageBudgetExhausted && cursor.Relist == githubTestsRelistWalking {
			cursor = recordGitHubTestsRelistStop(cursor, client, claim, githubTestsPageBudgetCause, cursor.RangeHi, cursor.RangeItems)
			break
		}
		if collection.PageBudgetExhausted {
			cursor = recordGitHubTestsInventoryTruncation(
				cursor, client, claim, githubTestsArtifactInventoryComponent, cursor.ArtifactPages)
			break
		}
	}

	// A listing that ran out of page budget leaves the artifacts phase here
	// with its phase unchanged; the open phase does not depend on it.
	if cursor.Phase == githubTestsArtifactsPhase {
		cursor.Phase = githubTestsPhaseAfterArtifacts(handler)
	}
	cursor = githubTestsClearCutKeys(cursor)
	cursor.AnchorID, cursor.ReseekPages, cursor.PagesEntered = "", 0, 0
	if cursor.Phase == githubTestsOpenPhase {
		handlerOpenRan = true
		if err := collectOpenRuns(); err != nil {
			return err
		}
		logOpenPhase()
	}

	// The inventory scan is complete. Publishing the terminal phase in the
	// SAME emission that carries the completion metadata means a crash between
	// this commit and MarkInventoryComplete resumes into emitFinalMetadata
	// rather than back into pagination.
	return emitFinalMetadata(cursor)
}

// ObserveCommittedChunk logs the WARN line and steps the counter of the open
// run skip a committed chunk carries. The repository attribute is the claim's
// own, never the provider's full_name.
func (handler GitHubTestsRouteHandler) ObserveCommittedChunk(claim Claim, chunk PreparedProviderChunk, metrics *providerfoundation.Metrics) {
	cursor, event := githubTestsChunkOpenSkip(chunk)
	if event == nil {
		return
	}
	metrics.RecordOpenRunSkipped(claim.Provider, claim.Dataset, event.Reason)
	slog.Warn(
		"open run re-read failed with an error of that run; it is skipped this unit and read again on a later unit",
		"provider", claim.Provider, "dataset", claim.Dataset, "unit", claim.ID, "repository", logging.ProviderText(claim.SourceExternalID),
		logging.ProviderIDAttr("run", event.RunID), "reason", event.Reason, "status", event.Status, "route", event.Route,
		"skipped", cursor.OpenSkipped,
	)
}

// ObserveUnconfirmedChunk names the open run skip of a chunk whose commit is
// unknown: the mark failed and the checkpoint read back failed too. If the
// commit landed, no recovery drains the chunk and the skip has no WARN line or
// counter step, so this distinct line keeps it from being silent.
func (handler GitHubTestsRouteHandler) ObserveUnconfirmedChunk(claim Claim, chunk PreparedProviderChunk) {
	_, event := githubTestsChunkOpenSkip(chunk)
	if event == nil {
		return
	}
	slog.Warn(
		"open run skip unconfirmed: the commit of its chunk failed and the checkpoint read back failed; if the commit landed, the skip is not recorded",
		"provider", claim.Provider, "dataset", claim.Dataset, "unit", claim.ID, "repository", logging.ProviderText(claim.SourceExternalID),
		logging.ProviderIDAttr("run", event.RunID), "chunk", chunk.Ordinal, "skip_reason", event.Reason, "outcome", "unknown",
	)
}

func githubTestsChunkOpenSkip(chunk PreparedProviderChunk) (githubTestsChunkCursor, *githubTestsOpenSkipEvent) {
	if !strings.Contains(chunk.CursorAfter, `"ose"`) {
		return githubTestsChunkCursor{}, nil
	}
	cursor, err := decodeGitHubTestsChunkCursor(chunk.CursorAfter)
	if err != nil {
		return githubTestsChunkCursor{}, nil
	}
	return cursor, cursor.OpenSkipEvent
}

var (
	_ ChunkedCompleteRouteHandler = GitHubTestsRouteHandler{}
	_ ChunkCommitObserver         = GitHubTestsRouteHandler{}
)
