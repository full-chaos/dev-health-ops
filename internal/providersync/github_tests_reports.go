package providersync

import (
	"archive/zip"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"time"
)

const (
	githubTestsMaxReportsPerRun    = 200
	githubTestsMaxArchiveEntries   = 2_000
	githubTestsMaxReportBytes      = 64 << 20
	githubTestsMaxArchiveBytes     = 256 << 20
	githubTestsMaxCompressionRatio = 200
)

var ErrGitHubTestsReportInvalid = errors.New("github tests report invalid")

// ErrGitHubTestsArchiveUnreadable narrows ErrGitHubTestsReportInvalid to the
// one case that is a property of the DOWNLOADED BYTES rather than of the
// caller: the archive container itself could not be opened.
//
// It exists so the routes can tell an unreadable artifact apart from a
// programming error (an empty repo/org id, a zero normalizedAt) that also
// reports ErrGitHubTestsReportInvalid. The first is provider data and must be
// skipped like an expired or empty artifact; the second is a bug and must stay
// fatal. Errors returned for it satisfy errors.Is for BOTH sentinels, so
// callers that only knew the wider one are unaffected (CHAOS-4177).
var ErrGitHubTestsArchiveUnreadable = errors.New("github tests archive unreadable")

type testSuiteResultRow struct {
	RepoID           string     `json:"repo_id"`
	RunID            string     `json:"run_id"`
	SuiteID          string     `json:"suite_id"`
	SuiteName        string     `json:"suite_name"`
	Framework        *string    `json:"framework"`
	Environment      *string    `json:"environment"`
	TotalCount       int64      `json:"total_count"`
	PassedCount      int64      `json:"passed_count"`
	FailedCount      int64      `json:"failed_count"`
	SkippedCount     int64      `json:"skipped_count"`
	ErrorCount       int64      `json:"error_count"`
	QuarantinedCount int64      `json:"quarantined_count"`
	RetriedCount     int64      `json:"retried_count"`
	DurationSeconds  *float64   `json:"duration_seconds"`
	StartedAt        *time.Time `json:"started_at"`
	FinishedAt       *time.Time `json:"finished_at"`
	TeamID           *string    `json:"team_id"`
	ServiceID        *string    `json:"service_id"`
	OrgID            string     `json:"org_id"`
	LastSynced       time.Time  `json:"last_synced"`
}

type testCaseResultRow struct {
	RepoID          string    `json:"repo_id"`
	RunID           string    `json:"run_id"`
	SuiteID         string    `json:"suite_id"`
	CaseID          string    `json:"case_id"`
	CaseName        string    `json:"case_name"`
	ClassName       *string   `json:"class_name"`
	Status          string    `json:"status"`
	DurationSeconds *float64  `json:"duration_seconds"`
	RetryAttempt    int64     `json:"retry_attempt"`
	FailureMessage  *string   `json:"failure_message"`
	FailureType     *string   `json:"failure_type"`
	StackTrace      *string   `json:"stack_trace"`
	IsQuarantined   bool      `json:"is_quarantined"`
	OrgID           string    `json:"org_id"`
	LastSynced      time.Time `json:"last_synced"`
}

type coverageSnapshotRow struct {
	RepoID            string    `json:"repo_id"`
	RunID             string    `json:"run_id"`
	SnapshotID        string    `json:"snapshot_id"`
	ReportFormat      *string   `json:"report_format"`
	LinesTotal        *int64    `json:"lines_total"`
	LinesCovered      *int64    `json:"lines_covered"`
	LineCoveragePct   *float64  `json:"line_coverage_pct"`
	BranchesTotal     *int64    `json:"branches_total"`
	BranchesCovered   *int64    `json:"branches_covered"`
	BranchCoveragePct *float64  `json:"branch_coverage_pct"`
	FunctionsTotal    *int64    `json:"functions_total"`
	FunctionsCovered  *int64    `json:"functions_covered"`
	CommitHash        *string   `json:"commit_hash"`
	Branch            *string   `json:"branch"`
	PRNumber          *int64    `json:"pr_number"`
	TeamID            *string   `json:"team_id"`
	ServiceID         *string   `json:"service_id"`
	OrgID             string    `json:"org_id"`
	LastSynced        time.Time `json:"last_synced"`
}

type githubTestsReportRows struct {
	Suites   []testSuiteResultRow
	Cases    []testCaseResultRow
	Coverage []coverageSnapshotRow
	Skipped  int
	issues   []githubTestsReportIssue
}

// GitHubTestsIncomplete is the bounded, provider-specific evidence retained
// when one report member cannot be parsed but other members remain valid. It
// deliberately records a stable local cause and count, never the untrusted
// archive member name or parser text.
type GitHubTestsIncomplete struct {
	Component string `json:"component"`
	Cause     string `json:"cause"`
	Count     int    `json:"count"`
}

// GitHubTestsSkippedArtifact is a durable per-artifact marker for ONE
// report_member artifact skip: enough for an operator to find the exact
// GitHub artifact behind a GitHubTestsIncomplete count, which -- unlike
// GitHubTestsIncomplete's closed Component/Cause vocabulary -- deliberately
// carries no provider-supplied identifiers at all. RunID and ArtifactID ARE
// provider-supplied and unbounded, which is exactly why this type is
// separate and why the number of records is capped
// (githubTestsMaxSkippedArtifactRecords) rather than growing with every
// skip: the chunk cursor that carries it between resumes has its own hard
// byte budget (maxChunkCursorBytes), and an unbounded per-artifact list
// would risk making the cursor itself unencodable on a source with heavy,
// sustained skip volume.
//
// Originally scoped to artifact_oversized skips only (CHAOS-4315). CHAOS-4394
// extended it to every report_member cause, once the watermark started
// advancing past all three: an operator retrying artifact_unavailable or
// unreadable_archive needs the same run id/artifact id an oversized retry
// needed. SizeBytes/CapBytes stay zero for the two causes that never had a
// size bound to report; Cause disambiguates the record without an operator
// having to cross-reference the count on GitHubTestsIncomplete.
type GitHubTestsSkippedArtifact struct {
	RunID      string `json:"run_id"`
	ArtifactID string `json:"artifact_id"`
	Cause      string `json:"cause"`
	SizeBytes  int64  `json:"size_bytes,omitempty"`
	CapBytes   int64  `json:"cap_bytes,omitempty"`
}

// githubTestsMaxSkippedArtifactRecords bounds how many GitHubTestsSkippedArtifact
// records one unit's cursor carries. Chosen conservatively against
// maxChunkCursorBytes (4KiB, chunked_persistence.go): a record serializes to
// roughly 90-120 bytes depending on id lengths, and the cursor already
// carries NextURL, Repo, and the rest of githubTestsChunkCursor's fields
// before this list even starts, so a cap sized for those bytes alone (e.g.
// 50) could push an otherwise-healthy cursor over budget and fail
// ErrChunkCheckpointConflict on encode -- turning a documented, harmless
// truncation (records beyond the cap collapse into
// SkippedArtifactsOverflow) into an undocumented, harmful one (the WHOLE
// cursor becomes unencodable, and unrelated committed progress is lost).
// Occurrences beyond this cap are still fully reflected in the closed
// GitHubTestsIncomplete Count and the RecordArtifactSkipped metric; only the
// per-artifact SAMPLE used to locate a specific artifact is bounded.
const githubTestsMaxSkippedArtifactRecords = 20

const (
	// githubTestsReportMemberComponent is the original vocabulary: one member
	// of a test-report archive that could not be parsed.
	githubTestsReportMemberComponent = "report_member"
	// githubTestsRunInventoryComponent and githubTestsArtifactInventoryComponent
	// record that an inventory phase stopped at its cumulative page budget
	// before the provider ran out of pages (CHAOS-4130). They are units of
	// COVERAGE incompleteness rather than parse failures, which is why they
	// share the slice.
	githubTestsRunInventoryComponent      = "run_inventory"
	githubTestsArtifactInventoryComponent = "artifact_inventory"
	githubTestsPageBudgetCause            = "page_budget_exhausted"
	// The per-run components record that ONE workflow run held more items than
	// its cap allows, so that run was committed with only the first cap-worth
	// (CHAOS-4142). Count is the number of RUNS truncated in that category,
	// never the number of items dropped, which keeps the evidence bounded on a
	// repository of any size. The truncated run's id goes to the structured
	// warning log, not here: GitHubTestsIncomplete deliberately carries no
	// provider-supplied subject string.
	githubTestsRunJobsComponent      = "run_jobs"
	githubTestsRunArtifactsComponent = "run_artifacts"
	githubTestsRunReportsComponent   = "run_reports"
	// githubTestsPerRunCapCause is the POSITIVELY OBSERVED item cap: the
	// provider handed back more items than the cap allows, so the boundary was
	// seen and the remainder is known to be beyond it.
	githubTestsPerRunCapCause = "per_run_cap"
	// githubTestsPerRunPageBudgetCause is the nested paginator running out of
	// page allowance INSIDE one run. It and the item cap above mean opposite
	// things about what was seen: a page-budget stop leaves an UNKNOWN
	// remainder, so it must not advance the watermark; the item cap leaves a
	// known one, so it may.
	//
	// They now arrive on SEPARATE paginator fields. They used to arrive on one
	// CapReached boolean -- which, contrary to how this comment first described
	// it, never carried both meanings: every write site was the page budget,
	// and the MaxItems stop set nothing at all. That silence is what let a
	// consumer read a page-budget stop as an item cap.
	githubTestsPerRunPageBudgetCause = "per_run_page_budget"
	// githubTestsUnreadableArchiveCause records that ONE artifact's archive
	// could not be opened, so none of its reports could be read and the
	// artifact was skipped. It joins the report_member vocabulary alongside
	// unreadable, malformed, archive_bounds and report_cap (CHAOS-4177). It
	// ADVANCES the watermark as of CHAOS-4394 -- see
	// githubTestsWatermarkAdvancingPairs for why.
	githubTestsUnreadableArchiveCause = "unreadable_archive"
	// githubTestsArtifactUnavailableCause records that ONE artifact could
	// never be fetched at all: the provider answered the artifact-download
	// redirect with no Location header, so there was nothing to follow. It
	// joins the report_member vocabulary alongside unreadable_archive but
	// names a DIFFERENT fact -- unreadable_archive means the bytes were
	// obtained and the container just would not open, this means the bytes
	// were never obtained -- so the two must not be conflated in the durable
	// evidence or an operator reading the cause could not tell which failure
	// actually happened (CHAOS-4191). It ADVANCES the watermark as of
	// CHAOS-4394 -- see githubTestsWatermarkAdvancingPairs for why.
	githubTestsArtifactUnavailableCause = "artifact_unavailable"
	// githubTestsArtifactOversizedCause records that ONE artifact's download
	// exceeded githubTestsMaxDownloadSize, so its bytes were never fully read
	// and the artifact was skipped. It joins the report_member vocabulary
	// alongside unreadable_archive and artifact_unavailable but names a
	// DIFFERENT fact -- the other two mean the bytes could not be obtained or
	// opened, this means a known safety bound was hit -- so the three must
	// not be conflated in the durable evidence or an operator reading the
	// cause could not tell which failure actually happened (CHAOS-4315,
	// reversing the prior UNIT-level-failure disposition for this bound).
	githubTestsArtifactOversizedCause = "artifact_oversized"
)

// githubTestsWatermarkAdvancingPairs is the CLOSED set of (component, cause)
// pairs that may advance the watermark. It is an allowlist rather than a
// blocklist on purpose: anything not named here withholds, so a future
// observation added to the vocabulary and forgotten here fails SAFE -- it
// stalls loudly instead of silently advancing over data nobody looked at.
//
// The single rule, which CHAOS-4142 established and codex's review forced us
// to apply one level down: **a page budget stop withholds the watermark; a
// positively observed item cap advances it.**
//
// A page budget stop -- whether it is the runs LISTING hitting its cumulative
// budget (CHAOS-4130) or a nested per-run paginator running out of pages --
// leaves an UNKNOWN remainder. GitHub serves newest-first, so advancing over
// an unobserved remainder is the permanent lower-bound hole CHAOS-2587
// describes, and it is the same hole one level down when the unobserved part
// is the tail of one run's jobs.
//
// A positively observed item cap is different in kind: the provider handed
// back more items than the cap allows, the walk over the window completed, and
// re-fetching returns exactly the same items. Withholding there recovers
// nothing and pins since_at forever, which is the CHAOS-4142 outage.
//
// report_member's three whole-artifact skip causes (artifact_oversized,
// artifact_unavailable, unreadable_archive) all advance. malformed/unreadable
// (report-parse-time issues inside an otherwise-read artifact, CHAOS-4153)
// stay withholding, unchanged.
//
// CHAOS-4315 first carved out ONLY githubTestsArtifactOversizedCause, on the
// theory that the other two causes are properties of ONE download ATTEMPT --
// a redirect with no Location, a connection that dropped -- so a later
// re-walk might genuinely observe that same artifact differently, and
// withholding buys a real chance at full coverage. That theory does not
// survive its own prod evidence: CHAOS-4394 (2026-08-28, 9 days after
// CHAOS-4315 shipped) found sync_watermarks for ops/acr/web STILL pinned at
// 2026-08-19, with the SAME artifacts recurring as artifact_unavailable and
// unreadable_archive on every hourly attempt -- identically, not
// intermittently. Whatever theoretical transience the redirect/connection
// mechanism has, these particular failures behave exactly like the
// oversized case in practice: the provider answers the same way every time,
// so withholding recovers nothing and pins since_at forever. All three
// report_member causes are therefore treated alike: skip the one artifact,
// keep walking, record a durable per-artifact marker (GitHubTestsSkippedArtifact,
// extended in CHAOS-4394 to cover all three) for a targeted backfill, and
// advance. reports_complete stays false regardless, so coverage honesty is
// unaffected -- only the permanent-stall consequence of withholding is
// removed. The totality gate (githubTestsCheckAllArtifactsUnreadable) is the
// real backstop against a systematically broken repository: it fails the
// unit outright, before any of this, when EVERY observed artifact was
// unreadable.
var githubTestsWatermarkAdvancingPairs = map[string]map[string]struct{}{
	githubTestsRunJobsComponent:      {githubTestsPerRunCapCause: {}},
	githubTestsRunArtifactsComponent: {githubTestsPerRunCapCause: {}},
	githubTestsRunReportsComponent:   {githubTestsPerRunCapCause: {}},
	githubTestsReportMemberComponent: {
		githubTestsArtifactOversizedCause:   {},
		githubTestsArtifactUnavailableCause: {},
		githubTestsUnreadableArchiveCause:   {},
	},
}

// githubTestsBlocksWatermark reports whether these observations leave any part
// of the requested window, or of a run inside it, UNOBSERVED. Coverage honesty
// is a SEPARATE claim: reports_complete stays false for any observation,
// withholding or not.
//
// skippedArtifacts and overflow feed the upgrade-boundary guard folded in
// here (codex review round 3, P1, CHAOS-4394): this function is the SINGLE
// source of truth for the blocking decision, called identically from
// githubTestsFinalMetadataBatch (which computes Watermark) and
// validateGitHubTestsCompletion (which validates it). Round 1 first wrote
// the guard as a SEPARATE check the Watermark closure applied on top of this
// function's result -- but validateGitHubTestsCompletion still called only
// the narrower, unaware function, so a legacy cursor that correctly withheld
// (nil Watermark) failed comparator validation instead of completing safely:
// the comparator's bidirectional invariant (githubTestsBlocksWatermark
// false => Watermark MUST equal BeforeAt, see
// TestGitHubTestsCompletionWatermarkInvariantIsBidirectional) rejected the
// batch outright, turning an intended safe fallback into a hard failure.
// Folding the guard in here, so both callers see the identical verdict, is
// what makes the invariant hold again.
func githubTestsBlocksWatermark(
	incomplete []GitHubTestsIncomplete, skippedArtifacts []GitHubTestsSkippedArtifact, overflow int,
) bool {
	for _, observation := range incomplete {
		causes, advancing := githubTestsWatermarkAdvancingPairs[observation.Component]
		if !advancing {
			return true
		}
		if _, ok := causes[observation.Cause]; !ok {
			return true
		}
	}
	return githubTestsReportMemberSkippedWithoutDurableMarker(incomplete, skippedArtifacts, overflow)
}

// githubTestsSkippedArtifactCause returns a marker's cause, recognizing a
// PRE-CHAOS-4394 marker as artifact_oversized even though it decodes with
// Cause == "" (codex review round 3, P2): that field did not exist before
// this change, and oversized was the ONLY cause CHAOS-4315 ever wrote a
// marker for, so an old marker's absence-of-Cause has exactly one possible
// meaning. SizeBytes disambiguates it from a genuinely malformed/empty
// marker: only the oversized branch (github_tests_chunked_route.go) ever
// sets it, so its presence is unambiguous corroborating evidence, not a
// guess. Treating a legacy oversized marker as unmarked would needlessly
// withhold a unit whose oversized skip was ALWAYS durably identified, just
// under a schema one field narrower.
func githubTestsSkippedArtifactCause(marker GitHubTestsSkippedArtifact) string {
	if marker.Cause != "" {
		return marker.Cause
	}
	if marker.SizeBytes > 0 {
		return githubTestsArtifactOversizedCause
	}
	return ""
}

// githubTestsReportMemberSkippedWithoutDurableMarker guards a narrow
// upgrade-boundary gap (codex review round 1, P1, CHAOS-4394): a chunk
// cursor checkpointed by the PRE-CHAOS-4394 binary can already carry
// artifact_unavailable/unreadable_archive counts on Incomplete with NO
// GitHubTestsSkippedArtifact marker, because the old binary only ever wrote
// markers for artifact_oversized. If such a cursor resumes under THIS binary
// and reaches "done", the plain per-(component,cause) allowlist alone would
// let it advance on report_member evidence that has no run/artifact identity
// behind it -- exactly the backfill-targeting promise this change makes,
// broken for the one unit that straddled the deploy.
//
// The check is PER CAUSE, not aggregate presence (codex review round 2, P1
// -- round 1's first version checked only "does SkippedArtifacts have ANY
// entry", which a legacy cursor mixing a marked artifact_oversized skip with
// an unmarked artifact_unavailable one would satisfy vacuously, advancing on
// the unmarked cause anyway). SkippedArtifactsOverflow is the one exception,
// checked in aggregate: it only exists on a THIS-binary cursor (the old
// binary never wrote it either), so overflow>0 anywhere proves this walk's
// marker-writing path executed for its full duration, and the sample cap
// legitimately leaving some individual occurrences unmarked is the SAME
// sanctioned degradation appendGitHubTestsSkippedArtifact already accepts
// for a heavy-skip-volume unit -- not evidence of a legacy cursor.
//
// This can fire at most once per unit -- the checkpoint this guards against
// is deleted the moment the unit terminalizes (repository_postgres.go's
// deletePreparedChunkStateTx, inside the same transaction as Complete), so
// the unit simply retries next window: today's status quo, no data lost, no
// permanent stall, not a new outage class.
func githubTestsReportMemberSkippedWithoutDurableMarker(
	incomplete []GitHubTestsIncomplete, skippedArtifacts []GitHubTestsSkippedArtifact, overflow int,
) bool {
	if overflow > 0 {
		return false
	}
	markedCauses := make(map[string]struct{}, len(skippedArtifacts))
	for _, marker := range skippedArtifacts {
		if cause := githubTestsSkippedArtifactCause(marker); cause != "" {
			markedCauses[cause] = struct{}{}
		}
	}
	for _, observation := range incomplete {
		if observation.Component != githubTestsReportMemberComponent || observation.Count == 0 {
			continue
		}
		if _, marked := markedCauses[observation.Cause]; !marked {
			return true
		}
	}
	return false
}

// GitHubTestsCicdPartialSuccessReason collapses a unit's incomplete
// observations into the bounded reason label RecordCicdPartialSuccess takes.
// Callers only reach this once githubTestsBlocksWatermark has already
// returned false for the same slice, so every cause present is, by
// construction, one that advances the watermark. A single distinct cause
// reports itself; more than one collapses to "mixed" rather than growing an
// unbounded combination label -- MetricCicdPartialSuccessReasonLabel bounds
// it again defensively on the write side. Exported: the telemetry call site
// lives in internal/jobs/providerunit, at the durable-completion boundary
// (codex review round 1, P2), not in this route.
func GitHubTestsCicdPartialSuccessReason(incomplete []GitHubTestsIncomplete) string {
	reason := ""
	for _, observation := range incomplete {
		if reason == "" {
			reason = observation.Cause
			continue
		}
		if reason != observation.Cause {
			return "mixed"
		}
	}
	return reason
}

// DecodeGitHubTestsIncomplete accepts both the live typed value a chunked
// route emits AND the generic []any/map[string]any shape produced when a
// prepared chunk is reloaded from its JSONB sidecar (codex review round 2,
// P1) -- the exact same dual-shape problem
// applyGitHubWorkItemsIncompletePolicy and decodeCompletionValue already
// solve for their own callers, applied here because
// internal/jobs/providerunit's completion path (loadChunkedFinalResult ->
// PostgresRepository.LoadPreparedChunk -> json.Unmarshal into
// map[string]any) means EVERY real chunked completion reaches its caller
// with "incomplete" as []any, never the typed slice -- a plain type
// assertion there always misses, silently disabling the reader. The
// marshal/unmarshal round-trip is a no-op on an already-typed value and a
// real decode on the generic one, so one function correctly serves both.
func DecodeGitHubTestsIncomplete(value any) ([]GitHubTestsIncomplete, bool) {
	if value == nil {
		return nil, false
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, false
	}
	var incomplete []GitHubTestsIncomplete
	if json.Unmarshal(encoded, &incomplete) != nil {
		return nil, false
	}
	return incomplete, true
}

// validatePerRunPageBudget refuses a configuration in which a per-run PAGE
// BUDGET would bind before the per-run ITEM CAP. It serves both the github and
// gitlab tests routes, because both had the same exposure.
//
// Why this is a configuration error and not a runtime disposition
// (CHAOS-4142, codex round 2, challenge 3):
//
// The watermark doctrine above says a page budget stop withholds the watermark
// because its remainder was never observed. That is the right rule when the
// budget is a property of the WINDOW -- a bigger window walk really can reach
// the remainder next time. It is the WRONG outcome when the budget is a fixed
// config value applied INSIDE one run: the same run exceeds the same budget on
// every future window, the watermark is withheld every time, and since_at is
// pinned forever. That is the identical four-day outage this route was changed
// to fix, re-entered through the other branch.
//
// Rather than teach the per-run page budget to advance -- which would make one
// rule mean two different things at two levels, the exact confusion that caused
// the original defect -- the branch is made UNREACHABLE by construction. The
// item cap binds first whenever the walk can hold strictly more items than the
// cap.
//
// The inequality is read off the OPERATORS, not off prose. Both providers need
// strictly more than cap items in hand before their item cap fires: github
// passes MaxItems = cap+1 against pagination.go's `len(Items) >= MaxItems`, and
// gitlab has no MaxItems and tests `len(items) > cap` in the route. The walk can
// hold at most budget*perPage items, since pagination.go checks
// `Pages >= MaxPages` at the top of its loop. So the budget is sufficient
// exactly when budget*perPage EXCEEDS cap -- equality is NOT enough, and
// budget*perPage == cap is the precise boundary that must be rejected.
//
// HONEST LIMIT: this closes the CONFIG-reachable path, which is the one that
// bites. It does not make the branch impossible against a provider that
// advertises a further page while returning SHORT pages, since then the walk
// holds fewer than budget*perPage items. That residual is exactly why the
// per-run page-budget branches STAY and keep withholding.
func validatePerRunPageBudget(setting string, budget, perPage, itemCap int) error {
	if budget*perPage > itemCap {
		return nil
	}
	return fmt.Errorf(
		"%w: %s=%d x per_page=%d = %d does not exceed the %d-item per-run cap, so a "+
			"per-run page budget stop would withhold the watermark on every future "+
			"window and stall the source permanently; set %s to at least %d",
		ErrInvalidConfiguration, setting, budget, perPage, budget*perPage, itemCap,
		setting, itemCap/perPage+1,
	)
}

// githubTestsIncompleteVocabulary is the CLOSED set of (component, cause)
// pairs a github tests/cicd unit may publish. The completion comparator fails
// closed against it, so a route cannot invent an observation that downstream
// coverage readers have no meaning for.
var githubTestsIncompleteVocabulary = map[string]map[string]struct{}{
	githubTestsReportMemberComponent: {
		"malformed": {}, "unreadable": {}, githubTestsUnreadableArchiveCause: {},
		githubTestsArtifactUnavailableCause: {}, githubTestsArtifactOversizedCause: {},
	},
	githubTestsRunInventoryComponent:      {githubTestsPageBudgetCause: {}},
	githubTestsArtifactInventoryComponent: {githubTestsPageBudgetCause: {}},
	githubTestsRunJobsComponent: {
		githubTestsPerRunCapCause: {}, githubTestsPerRunPageBudgetCause: {},
	},
	githubTestsRunArtifactsComponent: {
		githubTestsPerRunCapCause: {}, githubTestsPerRunPageBudgetCause: {},
	},
	// run_reports aggregates rows already parsed out of downloaded archives.
	// Nothing paginates there, so it has no page-budget cause.
	githubTestsRunReportsComponent: {githubTestsPerRunCapCause: {}},
}

func githubTestsIncompleteInVocabulary(observation GitHubTestsIncomplete) bool {
	causes, known := githubTestsIncompleteVocabulary[observation.Component]
	if !known {
		return false
	}
	_, ok := causes[observation.Cause]
	return ok
}

type githubTestsReportIssue struct {
	evidence GitHubTestsIncomplete
	blocking bool
}

func (rows *githubTestsReportRows) recordSkipped(cause string, blocking bool) {
	rows.Skipped++
	for index := range rows.issues {
		issue := &rows.issues[index]
		if issue.evidence.Cause == cause && issue.blocking == blocking {
			issue.evidence.Count++
			return
		}
	}
	rows.issues = append(rows.issues, githubTestsReportIssue{
		evidence: GitHubTestsIncomplete{
			Component: githubTestsReportMemberComponent, Cause: cause, Count: 1,
		},
		blocking: blocking,
	})
}

func (rows githubTestsReportRows) optionalIncomplete() ([]GitHubTestsIncomplete, bool) {
	result := make([]GitHubTestsIncomplete, 0, len(rows.issues))
	for _, issue := range rows.issues {
		if issue.blocking {
			return nil, false
		}
		result = append(result, issue.evidence)
	}
	return result, true
}

type lcovFileMetrics struct {
	path               string
	linesTotal         *int64
	linesCovered       *int64
	branchesTotal      *int64
	branchesCovered    *int64
	functionsTotal     *int64
	functionsCovered   *int64
	lineNumbers        map[int64]struct{}
	coveredLineNumbers map[int64]struct{}
}

type junitDocument struct {
	XMLName xml.Name
	Suites  []junitSuite `xml:"testsuite"`
	Cases   []junitCase  `xml:"testcase"`
}

type junitSuite struct {
	Name      string       `xml:"name,attr"`
	Framework string       `xml:"framework,attr"`
	Runner    string       `xml:"runner,attr"`
	Hostname  string       `xml:"hostname,attr"`
	File      string       `xml:"file,attr"`
	Timestamp string       `xml:"timestamp,attr"`
	Time      string       `xml:"time,attr"`
	Suites    []junitSuite `xml:"testsuite"`
	Cases     []junitCase  `xml:"testcase"`
}

type junitCase struct {
	Name      string       `xml:"name,attr"`
	ClassName string       `xml:"classname,attr"`
	File      string       `xml:"file,attr"`
	Time      string       `xml:"time,attr"`
	Failure   *junitDetail `xml:"failure"`
	Error     *junitDetail `xml:"error"`
	Skipped   *junitDetail `xml:"skipped"`
	SystemOut string       `xml:"system-out"`
	SystemErr string       `xml:"system-err"`
}

type junitDetail struct {
	Message string `xml:"message,attr"`
	Type    string `xml:"type,attr"`
	Text    string `xml:",chardata"`
}

// artifactID scopes every SuiteID/CaseID/SnapshotID this parse produces to
// the ONE artifact (GitHub) or job (GitLab) the archive was downloaded from.
// Without it, two distinct artifacts of the same run that carry a suite/case
// with the same name -- the ordinary shape of a matrix build, where every
// leg uploads an identically-named JUnit report -- hash to the SAME natural
// key. Both legs then land in the same run's EffectBatch, and
// TestOpsClickHouseEffects.WriteEffect's recordGitHubTestsKey rejects the
// second leg as a duplicate, returning the bare ErrInvalidConfiguration the
// unit terminalizes on after 5 attempts (CHAOS-4190). Callers must pass a
// value unique per download within the run: GitHub's artifact ID or GitLab's
// job ID, both already validated non-empty before the download that
// produced `archive`.
func parseGitHubTestsArtifact(
	archive []byte,
	artifactID, repoID, runID, orgID string,
	startedAt, finishedAt *time.Time,
	normalizedAt time.Time,
) (githubTestsReportRows, error) {
	if len(archive) == 0 || len(archive) > githubTestsMaxArchiveBytes || repoID == "" || runID == "" || orgID == "" || normalizedAt.IsZero() {
		return githubTestsReportRows{}, ErrGitHubTestsReportInvalid
	}
	normalizedAt = normalizedAt.UTC().Truncate(time.Millisecond)
	reader, err := zip.NewReader(bytes.NewReader(archive), int64(len(archive)))
	if err != nil {
		return githubTestsReportRows{}, fmt.Errorf(
			"%w: %w: zip: %v", ErrGitHubTestsReportInvalid, ErrGitHubTestsArchiveUnreadable, err,
		)
	}
	result := githubTestsReportRows{}
	processed := 0
	var expanded uint64
	members := reader.File
	if len(members) > githubTestsMaxArchiveEntries {
		members = members[:githubTestsMaxArchiveEntries]
	}
	for _, member := range members {
		if !isSafeGitHubTestsArchiveMemberName(member.Name) {
			continue
		}
		name := member.Name
		lower := strings.ToLower(name)
		if member.FileInfo().IsDir() || (!strings.HasSuffix(lower, ".xml") && !strings.HasSuffix(lower, ".info")) {
			continue
		}
		if processed >= githubTestsMaxReportsPerRun {
			result.recordSkipped("report_cap", true)
			continue
		}
		if member.UncompressedSize64 > githubTestsMaxReportBytes || expanded+member.UncompressedSize64 > githubTestsMaxArchiveBytes {
			result.recordSkipped("archive_bounds", true)
			continue
		}
		compressed := member.CompressedSize64
		if compressed == 0 {
			compressed = 1
		}
		if member.UncompressedSize64 > compressed*githubTestsMaxCompressionRatio {
			continue
		}
		body, err := readZipReport(member)
		if err != nil {
			result.recordSkipped("unreadable", false)
			continue
		}
		expanded += uint64(len(body))
		kind := classifyGitHubTestReport(lower, body)
		if kind == "" {
			continue
		}
		processed++
		switch kind {
		case "junit":
			suites, cases, err := parseJUnitRows(body, artifactID, repoID, runID, orgID, startedAt, finishedAt, normalizedAt)
			if err != nil {
				result.recordSkipped("malformed", false)
				continue
			}
			result.Suites = append(result.Suites, suites...)
			result.Cases = append(result.Cases, cases...)
		case "coverage":
			coverage, err := parseGitHubCoverageRow(body, name, artifactID, repoID, runID, orgID, normalizedAt)
			if err != nil {
				result.recordSkipped("malformed", false)
				continue
			}
			result.Coverage = append(result.Coverage, coverage)
		}
	}
	return result, nil
}

func isSafeGitHubTestsArchiveMemberName(name string) bool {
	if name == "" || strings.HasSuffix(name, "/") {
		return false
	}
	if strings.HasPrefix(name, "/") || strings.HasPrefix(name, `\`) {
		return false
	}
	for _, part := range strings.Split(strings.ReplaceAll(name, `\`, "/"), "/") {
		if part == ".." {
			return false
		}
	}
	if len(name) >= 2 && name[1] == ':' {
		return false
	}
	return true
}

type coberturaDocument struct {
	LinesValid      string           `xml:"lines-valid,attr"`
	LinesCovered    string           `xml:"lines-covered,attr"`
	BranchesValid   string           `xml:"branches-valid,attr"`
	BranchesCovered string           `xml:"branches-covered,attr"`
	Classes         []coberturaClass `xml:"packages>package>classes>class"`
}
type coberturaClass struct {
	Filename string          `xml:"filename,attr"`
	Lines    []coberturaLine `xml:"lines>line"`
}
type coberturaLine struct {
	Hits              string `xml:"hits,attr"`
	ConditionCoverage string `xml:"condition-coverage,attr"`
}

func parseGitHubCoverageRow(body []byte, reportPath, artifactID, repoID, runID, orgID string, normalizedAt time.Time) (coverageSnapshotRow, error) {
	trimmed := strings.TrimSpace(string(body))
	if strings.HasPrefix(strings.ToLower(trimmed), "<coverage") {
		return parseCoberturaRow(body, reportPath, artifactID, repoID, runID, orgID, normalizedAt)
	}
	return parseLCOVRow(body, reportPath, artifactID, repoID, runID, orgID, normalizedAt)
}

func parseCoberturaRow(body []byte, reportPath, artifactID, repoID, runID, orgID string, normalizedAt time.Time) (coverageSnapshotRow, error) {
	upper := bytes.ToUpper(body)
	if len(body) > githubTestsMaxReportBytes || bytes.Contains(upper, []byte("<!DOCTYPE")) || bytes.Contains(upper, []byte("<!ENTITY")) {
		return coverageSnapshotRow{}, ErrGitHubTestsReportInvalid
	}
	var document coberturaDocument
	if err := xml.Unmarshal(body, &document); err != nil {
		return coverageSnapshotRow{}, err
	}
	type metrics struct {
		path                                                     string
		linesTotal, linesCovered, branchesTotal, branchesCovered int64
	}
	files := make([]metrics, 0, len(document.Classes))
	positions := map[string]int{}
	for _, class := range document.Classes {
		if class.Filename == "" {
			continue
		}
		current := metrics{path: class.Filename}
		current.linesTotal = int64(len(class.Lines))
		for _, line := range class.Lines {
			if hits := parseOptionalInt64(line.Hits); hits != nil && *hits > 0 {
				current.linesCovered++
			}
			total, covered := coberturaConditionCounts(line.ConditionCoverage)
			current.branchesTotal += total
			current.branchesCovered += covered
		}
		if index, ok := positions[current.path]; ok {
			files[index] = current
		} else {
			positions[current.path] = len(files)
			files = append(files, current)
		}
	}
	linesTotal := parseOptionalInt64(document.LinesValid)
	linesCovered := parseOptionalInt64(document.LinesCovered)
	branchesTotal := parseOptionalInt64(document.BranchesValid)
	branchesCovered := parseOptionalInt64(document.BranchesCovered)
	if linesTotal == nil {
		var value int64
		for _, file := range files {
			value += file.linesTotal
		}
		linesTotal = nilIfZero(value)
	}
	if linesCovered == nil {
		var value int64
		for _, file := range files {
			value += file.linesCovered
		}
		linesCovered = nilIfZero(value)
	}
	if branchesTotal == nil {
		var value int64
		for _, file := range files {
			value += file.branchesTotal
		}
		branchesTotal = nilIfZero(value)
	}
	if branchesCovered == nil {
		var value int64
		for _, file := range files {
			value += file.branchesCovered
		}
		branchesCovered = nilIfZero(value)
	}
	if linesCovered != nil && linesTotal != nil && *linesCovered > *linesTotal {
		return coverageSnapshotRow{}, ErrGitHubTestsReportInvalid
	}
	if branchesCovered != nil && branchesTotal != nil && *branchesCovered > *branchesTotal {
		return coverageSnapshotRow{}, ErrGitHubTestsReportInvalid
	}
	serviceCounts := map[string]int{}
	serviceOrder := []string{}
	for _, file := range files {
		if service := testServiceID(file.path); service != nil {
			if _, ok := serviceCounts[*service]; !ok {
				serviceOrder = append(serviceOrder, *service)
			}
			serviceCounts[*service]++
		}
	}
	var serviceID *string
	best := 0
	for _, service := range serviceOrder {
		if serviceCounts[service] > best {
			value := service
			serviceID = &value
			best = serviceCounts[service]
		}
	}
	format := "cobertura"
	row := coverageSnapshotRow{RepoID: repoID, RunID: runID, SnapshotID: hashTestIdentifier(runID, artifactID, format, reportPath), ReportFormat: &format, LinesTotal: linesTotal, LinesCovered: linesCovered, BranchesTotal: branchesTotal, BranchesCovered: branchesCovered, ServiceID: serviceID, OrgID: orgID, LastSynced: normalizedAt}
	row.LineCoveragePct = coveragePercent(row.LinesCovered, row.LinesTotal)
	row.BranchCoveragePct = coveragePercent(row.BranchesCovered, row.BranchesTotal)
	return row, nil
}

func coberturaConditionCounts(value string) (int64, int64) {
	start := strings.Index(value, "(")
	slash := strings.Index(value, "/")
	if start < 0 || slash < start {
		return 0, 0
	}
	end := strings.Index(value[slash:], ")")
	if end < 0 {
		return 0, 0
	}
	covered := parseOptionalInt64(value[start+1 : slash])
	total := parseOptionalInt64(value[slash+1 : slash+end])
	if covered == nil || total == nil {
		return 0, 0
	}
	return *total, *covered
}

func readZipReport(member *zip.File) ([]byte, error) {
	stream, err := member.Open()
	if err != nil {
		return nil, err
	}
	defer stream.Close()
	body, err := io.ReadAll(io.LimitReader(stream, githubTestsMaxReportBytes+1))
	if err != nil || len(body) > githubTestsMaxReportBytes {
		return nil, ErrGitHubTestsReportInvalid
	}
	return body, nil
}

func classifyGitHubTestReport(name string, body []byte) string {
	head := strings.ToLower(string(body))
	if len(head) > 2048 {
		head = head[:2048]
	}
	if strings.Contains(head, "<testsuite") || strings.Contains(head, "<testsuites") {
		return "junit"
	}
	if strings.HasSuffix(name, ".info") || strings.HasPrefix(strings.TrimSpace(head), "tn:") || strings.HasPrefix(strings.TrimSpace(head), "sf:") || strings.Contains(head, "<coverage") {
		return "coverage"
	}
	return ""
}

func parseJUnitRows(
	body []byte, artifactID, repoID, runID, orgID string,
	fallbackStarted, fallbackFinished *time.Time,
	normalizedAt time.Time,
) ([]testSuiteResultRow, []testCaseResultRow, error) {
	if len(body) > githubTestsMaxReportBytes || bytes.Contains(bytes.ToUpper(body), []byte("<!DOCTYPE")) || bytes.Contains(bytes.ToUpper(body), []byte("<!ENTITY")) {
		return nil, nil, ErrGitHubTestsReportInvalid
	}
	var document junitDocument
	if err := xml.Unmarshal(body, &document); err != nil {
		return nil, nil, err
	}
	suites := document.Suites
	if document.XMLName.Local == "testsuite" {
		suites = []junitSuite{{Cases: document.Cases}}
		var root junitSuite
		if err := xml.Unmarshal(body, &root); err != nil {
			return nil, nil, err
		}
		suites = []junitSuite{root}
	}
	flat := make([]junitSuite, 0, len(suites))
	var visit func(junitSuite)
	visit = func(suite junitSuite) {
		if len(suite.Cases) > 0 {
			flat = append(flat, suite)
		}
		for _, child := range suite.Suites {
			visit(child)
		}
	}
	for _, suite := range suites {
		visit(suite)
	}
	resultSuites := make([]testSuiteResultRow, 0, len(flat))
	resultCases := make([]testCaseResultRow, 0)
	for _, suite := range flat {
		name := suite.Name
		if name == "" {
			name = "unnamed"
		}
		framework := inferJUnitFramework(suite)
		suiteID := hashTestIdentifier(runID, artifactID, name, "")
		started := parseGitHubTestsTime(suite.Timestamp)
		hasSuiteTimestamp := started != nil
		if started == nil {
			started = cloneTime(fallbackStarted)
		}
		duration := parseOptionalFloat(suite.Time)
		finished := cloneTime(fallbackFinished)
		if hasSuiteTimestamp && started != nil && duration != nil {
			value := started.Add(time.Duration(*duration * float64(time.Second))).UTC().Truncate(time.Millisecond)
			finished = &value
		}
		service := junitServiceID(suite)
		row := testSuiteResultRow{
			RepoID: repoID, RunID: runID, SuiteID: suiteID, SuiteName: name,
			Framework: &framework, TotalCount: int64(len(suite.Cases)), DurationSeconds: duration,
			StartedAt: started, FinishedAt: finished, ServiceID: service,
			OrgID: orgID, LastSynced: normalizedAt,
		}
		for _, testCase := range suite.Cases {
			caseRow := newJUnitCaseRow(testCase, row, normalizedAt)
			switch caseRow.Status {
			case "passed":
				row.PassedCount++
			case "failed":
				row.FailedCount++
			case "skipped":
				row.SkippedCount++
			case "error":
				row.ErrorCount++
			case "quarantined":
				row.QuarantinedCount++
			}
			resultCases = append(resultCases, caseRow)
		}
		resultSuites = append(resultSuites, row)
	}
	return resultSuites, resultCases, nil
}

func newJUnitCaseRow(item junitCase, suite testSuiteResultRow, normalizedAt time.Time) testCaseResultRow {
	name := item.Name
	if name == "" {
		name = "unnamed"
	}
	status, detail := "passed", (*junitDetail)(nil)
	switch {
	case item.Skipped != nil:
		status, detail = "skipped", item.Skipped
	case item.Failure != nil:
		status, detail = "failed", item.Failure
	case item.Error != nil:
		status, detail = "error", item.Error
	}
	if detail != nil && looksQuarantined(detail.Message, detail.Type, detail.Text) {
		status = "quarantined"
	}
	var message, failureType, trace *string
	if detail != nil {
		message = testsOptionalString(detail.Message)
		failureType = testsOptionalString(detail.Type)
		parts := make([]string, 0, 3)
		for _, value := range []string{strings.TrimSpace(detail.Text), strings.TrimSpace(item.SystemErr), strings.TrimSpace(item.SystemOut)} {
			if value != "" {
				parts = append(parts, value)
			}
		}
		if len(parts) > 0 {
			value := strings.Join(parts, "\n")
			if len(value) > 4096 {
				value = value[:4096]
			}
			trace = &value
		}
	}
	return testCaseResultRow{
		RepoID: suite.RepoID, RunID: suite.RunID, SuiteID: suite.SuiteID,
		CaseID: hashTestIdentifier(suite.SuiteID, name), CaseName: name,
		ClassName: testsOptionalString(item.ClassName), Status: status,
		DurationSeconds: parseOptionalFloat(item.Time), FailureMessage: message,
		FailureType: failureType, StackTrace: trace, IsQuarantined: status == "quarantined",
		OrgID: suite.OrgID, LastSynced: normalizedAt,
	}
}

func parseLCOVRow(body []byte, reportPath, artifactID, repoID, runID, orgID string, normalizedAt time.Time) (coverageSnapshotRow, error) {
	var currentPath *string
	var linesTotal, linesCovered, branchesTotal, branchesCovered, functionsTotal, functionsCovered *int64
	lineNumbers := map[int64]struct{}{}
	coveredLineNumbers := map[int64]struct{}{}
	files := make([]lcovFileMetrics, 0)
	flush := func() {
		if currentPath == nil {
			return
		}
		files = append(files, lcovFileMetrics{
			path: *currentPath, linesTotal: linesTotal, linesCovered: linesCovered,
			branchesTotal: branchesTotal, branchesCovered: branchesCovered,
			functionsTotal: functionsTotal, functionsCovered: functionsCovered,
			lineNumbers: lineNumbers, coveredLineNumbers: coveredLineNumbers,
		})
		currentPath = nil
		linesTotal, linesCovered = nil, nil
		branchesTotal, branchesCovered = nil, nil
		functionsTotal, functionsCovered = nil, nil
		lineNumbers = map[int64]struct{}{}
		coveredLineNumbers = map[int64]struct{}{}
	}
	for _, raw := range strings.Split(string(body), "\n") {
		line := strings.TrimSpace(raw)
		if line == "end_of_record" {
			flush()
			continue
		}
		key, value, found := strings.Cut(line, ":")
		if !found {
			continue
		}
		switch key {
		case "SF":
			flush()
			path := strings.TrimSpace(value)
			currentPath = &path
		case "DA":
			parts := strings.Split(value, ",")
			lineNumber, err := strconv.ParseInt(strings.TrimSpace(parts[0]), 10, 64)
			if err != nil {
				continue
			}
			lineNumbers[lineNumber] = struct{}{}
			if len(parts) > 1 {
				hits, hitErr := strconv.ParseInt(strings.TrimSpace(parts[1]), 10, 64)
				if hitErr == nil && hits > 0 {
					coveredLineNumbers[lineNumber] = struct{}{}
				}
			}
		case "LF":
			linesTotal = parseOptionalInt64(value)
		case "LH":
			linesCovered = parseOptionalInt64(value)
		case "BRF":
			branchesTotal = parseOptionalInt64(value)
		case "BRH":
			branchesCovered = parseOptionalInt64(value)
		case "FNF":
			functionsTotal = parseOptionalInt64(value)
		case "FNH":
			functionsCovered = parseOptionalInt64(value)
		}
	}
	flush()
	if len(files) == 0 {
		return coverageSnapshotRow{}, ErrGitHubTestsReportInvalid
	}
	var aggregateLinesTotal, aggregateLinesCovered int64
	var aggregateBranchesTotal, aggregateBranchesCovered int64
	var aggregateFunctionsTotal, aggregateFunctionsCovered int64
	serviceCounts := map[string]int{}
	serviceOrder := make([]string, 0)
	for _, file := range files {
		aggregateLinesTotal += int64(len(file.lineNumbers))
		if file.linesTotal != nil {
			aggregateLinesTotal += *file.linesTotal - int64(len(file.lineNumbers))
		}
		aggregateLinesCovered += int64(len(file.coveredLineNumbers))
		if file.linesCovered != nil {
			aggregateLinesCovered += *file.linesCovered - int64(len(file.coveredLineNumbers))
		}
		aggregateBranchesTotal += optionalInt64Value(file.branchesTotal)
		aggregateBranchesCovered += optionalInt64Value(file.branchesCovered)
		aggregateFunctionsTotal += optionalInt64Value(file.functionsTotal)
		aggregateFunctionsCovered += optionalInt64Value(file.functionsCovered)
		if service := testServiceID(file.path); service != nil {
			if _, seen := serviceCounts[*service]; !seen {
				serviceOrder = append(serviceOrder, *service)
			}
			serviceCounts[*service]++
		}
	}
	if aggregateLinesCovered > aggregateLinesTotal || aggregateBranchesCovered > aggregateBranchesTotal {
		return coverageSnapshotRow{}, ErrGitHubTestsReportInvalid
	}
	var serviceID *string
	bestCount := 0
	for _, service := range serviceOrder {
		if serviceCounts[service] > bestCount {
			value := service
			serviceID = &value
			bestCount = serviceCounts[service]
		}
	}
	format := "lcov"
	row := coverageSnapshotRow{
		RepoID: repoID, RunID: runID, SnapshotID: hashTestIdentifier(runID, artifactID, format, reportPath),
		ReportFormat: &format, LinesTotal: nilIfZero(aggregateLinesTotal), LinesCovered: nilIfZero(aggregateLinesCovered),
		BranchesTotal: nilIfZero(aggregateBranchesTotal), BranchesCovered: nilIfZero(aggregateBranchesCovered),
		FunctionsTotal: nilIfZero(aggregateFunctionsTotal), FunctionsCovered: nilIfZero(aggregateFunctionsCovered),
		ServiceID: serviceID, OrgID: orgID, LastSynced: normalizedAt,
	}
	row.LineCoveragePct = coveragePercent(row.LinesCovered, row.LinesTotal)
	row.BranchCoveragePct = coveragePercent(row.BranchesCovered, row.BranchesTotal)
	return row, nil
}

func parseOptionalInt64(value string) *int64 {
	parsed, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
	if err != nil {
		return nil
	}
	return &parsed
}

func optionalInt64Value(value *int64) int64 {
	if value == nil {
		return 0
	}
	return *value
}

func inferJUnitFramework(suite junitSuite) string {
	for _, item := range suite.Cases {
		path := strings.ToLower(item.File)
		if strings.HasSuffix(path, ".spec.js") || strings.HasSuffix(path, ".spec.ts") || strings.HasSuffix(path, ".test.js") || strings.HasSuffix(path, ".test.ts") {
			return "jest"
		}
		if strings.Contains(item.ClassName, "::") || strings.HasSuffix(path, ".py") {
			return "pytest"
		}
	}
	for _, candidate := range []string{suite.Framework, suite.Runner, suite.Hostname} {
		lower := strings.ToLower(candidate)
		if strings.Contains(lower, "jest") {
			return "jest"
		}
		if strings.Contains(lower, "pytest") {
			return "pytest"
		}
	}
	return "junit"
}

func junitServiceID(suite junitSuite) *string {
	path := suite.File
	if path == "" {
		for _, item := range suite.Cases {
			if item.File != "" {
				path = item.File
				break
			}
		}
	}
	return testServiceID(path)
}

func testServiceID(path string) *string {
	path = strings.TrimPrefix(strings.ReplaceAll(path, "\\", "/"), "./")
	parts := strings.FieldsFunc(path, func(r rune) bool { return r == '/' })
	for index, part := range parts {
		if (part == "services" || part == "apps" || part == "packages") && index+1 < len(parts) {
			return testsOptionalString(parts[index+1])
		}
	}
	if len(parts) > 0 {
		return testsOptionalString(parts[0])
	}
	return nil
}

func hashTestIdentifier(parts ...string) string {
	digest := sha256.Sum256([]byte(strings.Join(parts, "::")))
	return hex.EncodeToString(digest[:])
}

func parseOptionalFloat(value string) *float64 {
	if value == "" {
		return nil
	}
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil || math.IsNaN(parsed) || math.IsInf(parsed, 0) {
		return nil
	}
	return &parsed
}

func parseGitHubTestsTime(value string) *time.Time {
	if value == "" {
		return nil
	}
	parsed, err := time.Parse(time.RFC3339Nano, strings.Replace(value, "Z", "+00:00", 1))
	if err != nil {
		return nil
	}
	parsed = parsed.UTC().Truncate(time.Millisecond)
	return &parsed
}

func cloneTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	copy := value.UTC().Truncate(time.Millisecond)
	return &copy
}

func testsOptionalString(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

func looksQuarantined(values ...string) bool {
	haystack := strings.ToLower(strings.Join(values, " "))
	return strings.Contains(haystack, "quarantine") || strings.Contains(haystack, "quarantined") || strings.Contains(haystack, "xfail")
}

func nilIfZero(value int64) *int64 {
	if value == 0 {
		return nil
	}
	return &value
}

func coveragePercent(covered, total *int64) *float64 {
	if covered == nil || total == nil || *total == 0 {
		return nil
	}
	value := float64(*covered) / float64(*total) * 100
	return &value
}
