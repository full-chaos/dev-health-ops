package providersync

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

const (
	// GitHub Actions returns at most nativePerPage workflow runs per page. The
	// configured 2.5-month backfill window can exceed two pages, so the route
	// permits the package-wide nativeMaxPages ceiling (10,000 runs) while still
	// failing closed when GitHub advertises a page beyond that bound.
	githubTestsMaxRuns      = nativeMaxPages * nativePerPage
	githubTestsMaxArtifacts = 25
	// githubTestsMaxDownloadSize is the DEFAULT per-artifact download cap,
	// used whenever GitHubTestsRouteHandler.MaxArtifactBytes is unset
	// (WORKER_GITHUB_TESTS_MAX_ARTIFACT_BYTES, config.Config
	// WorkerGithubTestsMaxArtifactBytes) -- see downloadGitHubTestsArtifact's
	// maxBytes parameter. A legitimate repo shape (e.g. a large nightly test
	// bundle) can exceed 100 MiB without the read itself being broken; before
	// this became configurable the only remedy was a rebuild.
	githubTestsMaxDownloadSize = 100 << 20
	githubTestsRuleVersion     = "ci-acceptance.v1"
)

var ErrGitHubTestsIncomplete = errors.New("github tests inventory incomplete")

// ErrGitHubTestsArtifactUnavailable narrows ErrGitHubTestsIncomplete to the
// one download-time case that is a property of THIS ONE ARTIFACT rather than
// of the run or the unit: GitHub answered the artifact-download redirect with
// no Location header, so there was nothing to follow and its bytes could
// never be obtained. It is the download-time sibling of
// ErrGitHubTestsArchiveUnreadable (github_tests_reports.go), which narrows
// the same way for a container that WAS obtained but would not open -- both
// mean "skip this one artifact and keep walking, this is not a fault of the
// caller" (CHAOS-4191, extending CHAOS-4177's disposition one raise site
// earlier).
var ErrGitHubTestsArtifactUnavailable = fmt.Errorf("%w: artifact unavailable", ErrGitHubTestsIncomplete)

// ErrGitHubTestsArtifactOversized narrows ErrGitHubTestsIncomplete to the
// download-time bound violation: the artifact body exceeded the configured
// per-artifact cap (githubTestsMaxDownloadSize by default, overridable via
// GitHubTestsRouteHandler.MaxArtifactBytes / WORKER_GITHUB_TESTS_MAX_ARTIFACT_BYTES).
//
// CHAOS-4315 (reversing this sentinel's original disposition): in the
// CHUNKED route -- the one production dispatch always executes for cicd/tests
// (execution_registry.go marks github {cicd,tests} Chunked unconditionally)
// -- this is now a per-artifact SKIP, handled like
// ErrGitHubTestsArtifactUnavailable above (skipped with its own durable
// cause githubTestsArtifactOversizedCause and its own counter reason, walk
// continues) but with ONE deliberate divergence: it is also a
// WATERMARK-ADVANCING cause (githubTestsWatermarkAdvancingPairs), while
// artifact_unavailable stays withholding. The two causes differ in kind, not
// just in name: an unavailable/unreadable artifact is a TRANSIENT fact about
// this one attempt -- the redirect, the connection, the bytes obtained THIS
// time -- so a later re-walk of the same window might genuinely observe it
// differently, and withholding buys a real chance at recovering full
// coverage. An oversized artifact is DETERMINISTIC: the same artifact is the
// same number of bytes on every future re-walk of the same window, so
// withholding buys nothing -- it only pins sync_watermarks on that window
// forever, re-walking an unrecoverable loss hourly until GitHub's retention
// expires it (CHAOS-4142's "positively observed, deterministic bound
// advances; unknown remainder withholds" rule, reapplied one level down from
// its original per-run-cap application). This is the prior disposition's
// exact defect at the artifact granularity: prod hit `since_at` pinned at
// 2026-08-19 by exactly this mechanism. It matches the Python precedent
// (connectors/github.py's download_artifact_zip returns b"" past the same
// 100MB cap; processors/github.py's `if not data: continue` skips it) --
// prod Go was the one route out of step.
//
// The observed size and the cap are captured on
// *githubTestsArtifactOversizedError (below) rather than only in the error
// text, so the chunked route can persist a durable per-artifact marker
// (GitHubTestsSkippedArtifact) instead of leaving the run id/artifact
// id/size/cap in the log line only.
//
// The non-chunked oracle Collect below (github_tests_route.go, production-
// dead for cicd/tests) deliberately KEEPS the old terminal disposition,
// consistent with its documented divergence from the chunked route
// elsewhere in this file (see the comment at its per-run cap check): it
// exists only as the comparison implementation, so
// providerunit.deterministicTerminalCategory's GitHubTestsArtifactOversizedCategory
// mapping stays correct for that path without extra plumbing. It exists as
// its OWN sentinel, distinct from a plain read error, because it is
// deterministic: the same artifact is the same size on every attempt, so
// (for a caller that still propagates it) the generic bounded-retry path
// would otherwise burn every River attempt re-downloading it before
// collapsing into the generic exhausted category -- the same repeated-refusal
// waste CHAOS-3871 fixed for a pagination cap refusal. A genuine read error
// stays retryable: unlike a fixed artifact size, a dropped connection can
// succeed on retry (CHAOS-4191).
var ErrGitHubTestsArtifactOversized = fmt.Errorf("%w: artifact oversized", ErrGitHubTestsIncomplete)

// githubTestsArtifactOversizedError carries the size that crossed the cap (the
// declared size when the listing or the response states one, else the capped
// read, cap+1) and the configured cap alongside ErrGitHubTestsArtifactOversized, so a caller
// building a durable per-artifact marker (CHAOS-4315's GitHubTestsSkippedArtifact)
// does not have to parse them back out of the error string. ErrGitHubTestsArtifactOversized
// stays the stable sentinel every errors.Is check matches against; Unwrap
// keeps that working unchanged.
type githubTestsArtifactOversizedError struct {
	SizeBytes int64
	CapBytes  int64
}

func (e *githubTestsArtifactOversizedError) Error() string {
	return fmt.Sprintf(
		"%s: artifact size %d bytes exceeds cap %d bytes",
		ErrGitHubTestsArtifactOversized, e.SizeBytes, e.CapBytes,
	)
}

func (e *githubTestsArtifactOversizedError) Unwrap() error { return ErrGitHubTestsArtifactOversized }

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
	ID          json.Number     `json:"id"`
	Expired     bool            `json:"expired"`
	Name        string          `json:"name"`
	SizeInBytes json.RawMessage `json:"size_in_bytes"`
	CreatedAt   *string         `json:"created_at"`
}

// listedSize is the artifact's listed size_in_bytes, or -1 when it is
// missing, zero, negative or not an integer. A size above the cap no longer
// skips the artifact before its download: the download reads its report
// members by range, or refuses it without reading the body.
func (artifact githubTestsArtifactPayload) listedSize() int64 {
	size, err := strconv.ParseInt(string(artifact.SizeInBytes), 10, 64)
	if err != nil || size <= 0 {
		return -1
	}
	return size
}

// githubTestsNonReportArtifactSuffixes/Prefixes name GitHub/CI-generated
// artifact kinds that are never report_member candidates, matched
// case-insensitively. ".dockerbuild" bundles are created automatically by
// GitHub's Docker Build Summary integration (docker/build-push-action with
// attestations) for every matrix target in docker-images.yml -- outside any
// actions/upload-artifact step -- and, unlike an ordinary uploaded artifact,
// the classic archive_download_url for one is NOT zip-wrapped: it is the raw
// gzip payload (CHAOS-4588, verified byte-for-byte against a real
// full-chaos/dev-health-ops run: magic bytes 1f 8b, `file` reports "gzip
// compressed data"). zip.NewReader correctly refuses it -- the reader is not
// the bug.
//
// Feeding it to the report parser wastes a download+parse on every
// dev-health-ops CI run AND, worse, lets it consume githubTestsMaxArtifacts
// ahead of any real report artifact in the same run list: a docker-images.yml
// run regularly carries 15+ dockerbuild artifacts, over the 25-item cap
// combined with an equal count of harmless "digests-*" artifacts (see below),
// so every encounter recorded a window-blocking run_artifacts per_run_cap
// (CHAOS-4142) and pinned this repo's tests watermark since 2026-08-08.
// Treated exactly like a routine 404/410 artifact: excluded before any
// request, not counted toward seen, unreadable, or the per-run cap, and not
// part of the closed report_member Incomplete vocabulary -- it is not
// evidence about whether the read channel is broken, just a provider
// artifact type this route was never meant to read. Recorded on a SEPARATE
// bounded set of durable cursor fields (ExcludedNonReportSuffix/Prefix,
// ExcludedArtifactSample) purely for operator visibility and CHAOS-4591
// (per-sync-config selectable artifact ingestion).
//
// githubTestsNonReportArtifactPrefixes holds only go-images-, go-test-timings- and go-junit-part- by default
// (codex review round 1, P1): "digests-*" artifacts (this repo's own
// docker-images.yml uploads one empty touch-file per build target) ARE real,
// valid, tiny zips that parse cleanly and contribute zero report rows --
// harmless to read, unlike ".dockerbuild", so excluding them is a bandwidth
// optimization, not a correctness fix. Unlike ".dockerbuild" (a bizarre,
// non-standard extension no CI tool would choose for a real report archive),
// "digests-" is a plausible prefix a real report artifact could organically
// collide with on some other repository -- silently dropping it would mean
// `reports_complete=true` and the watermark advancing over genuinely lost
// data. The mechanism stays (this is a prefix list, not a special case) so
// CHAOS-4591 can populate it per org/repo as an explicit, informed opt-in
// rather than a global default nobody chose.
// ".spdx.json" are SBOM files: never reports, and 6 of them sit in every go.yml run.
var githubTestsNonReportArtifactSuffixes = []string{".dockerbuild", ".spdx.json"}

// "go-images-" are the docker-images tarballs ops CI uploads (>100 MiB); never
// reports (CHAOS-13193). go-test-* artifacts carry the JUnit XML and stay selected.
// "go-test-timings-" are per-shard .tsv.gz timing tables and "go-junit-part-" are per-shard JUnit parts that
// a fan-in step republishes merged; neither is a report candidate, and 17 timings per go.yml run pushed it past
// githubTestsMaxArtifacts (31 selected of 25).
var githubTestsNonReportArtifactPrefixes = []string{"go-images-", "go-test-timings-", "go-junit-part-"}

const (
	githubTestsExclusionReasonSuffix = "non_report_artifact_suffix"
	githubTestsExclusionReasonPrefix = "non_report_artifact_prefix"
)

// githubTestsArtifactSelectionSeam is the ONE place that decides whether a
// candidate artifact is worth downloading for report_member parsing at all.
// Both routes call it BEFORE any download and BEFORE the per-run artifact
// cap is applied (CHAOS-4588) -- selection must happen ahead of the cap, or
// non-report noise crowds out real report artifacts exactly the way it did
// before this fix.
//
// This is a fixed closed-list implementation today. CHAOS-4591 (per-sync-
// config selectable artifact ingestion) is expected to replace the body of
// this one function with a config-driven predicate (an org/repo-scoped
// include/exclude list) without moving either call site or changing the
// (selected, reason) contract -- `reason` is already the closed vocabulary
// an admin view would show next to each excluded artifact.
func githubTestsArtifactSelectionSeam(name string) (selected bool, reason string) {
	lower := strings.ToLower(name)
	for _, suffix := range githubTestsNonReportArtifactSuffixes {
		if strings.HasSuffix(lower, suffix) {
			return false, githubTestsExclusionReasonSuffix
		}
	}
	for _, prefix := range githubTestsNonReportArtifactPrefixes {
		if strings.HasPrefix(lower, prefix) {
			return false, githubTestsExclusionReasonPrefix
		}
	}
	return true, ""
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
	RepoIdentity       GitLabRepoIdentityReader
	MaxRuns            int
	MaxArtifactsPerRun int
	MaxJobPages        int
	// MaxArtifactBytes overrides githubTestsMaxDownloadSize when positive
	// (WORKER_GITHUB_TESTS_MAX_ARTIFACT_BYTES, config.Config). Zero/negative
	// keeps the package default -- see the zero-means-default pattern the
	// three fields above already use.
	MaxArtifactBytes int64
	// OpenRuns reads the stored open pipeline rows the open phase re-reads by
	// id. Nil leaves the open phase out (the oracle and unit fixtures without a
	// store).
	OpenRuns GitHubOpenRunsReader
	// CoalesceRuns marks each runs-phase run emission coalescable, so the
	// executor may commit several runs in one chunk group, and fences the open
	// phase with a barrier. False (the default, and the production wiring in
	// git_family_route.go) keeps one chunk group per run: coalescing ships dark.
	CoalesceRuns bool
	// RefreshLookback is how far before the window the runs phase lists; zero
	// keeps githubTestsRefreshLookbackDefault.
	RefreshLookback time.Duration
	// LateRerunLookback and LateRerunMaxPages bound the deep range of the
	// claim that holds a UTC midnight; zero or less keeps
	// githubTestsLateRerunLookbackDefault and githubTestsLateRerunMaxPages.
	LateRerunLookback time.Duration
	LateRerunMaxPages int
	// MaxWalkRecords lowers githubTestsMaxWalkRecords when positive; zero keeps it.
	MaxWalkRecords int
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
	if err := fetchObjectRoute(ctx, client, githubRepoRoute, root, &repo); err != nil {
		return CompleteRouteBatch{}, err
	}
	repoID, err := resolveGitHubRepoID(ctx, handler.RepoIdentity, claim, repo)
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
	maxArtifactBytes := handler.MaxArtifactBytes
	if maxArtifactBytes <= 0 {
		maxArtifactBytes = githubTestsMaxDownloadSize
	}
	if maxRuns < 1 || maxRuns > githubTestsMaxRuns || maxArtifacts < 1 || maxArtifacts > githubTestsMaxArtifacts || jobPages < 1 || jobPages > nativeMaxPages {
		return CompleteRouteBatch{}, ErrInvalidConfiguration
	}
	runPages := (maxRuns + nativePerPage - 1) / nativePerPage
	runsInterval := githubTestsRunsInterval(claim, false, handler.refreshLookback(), handler.lateRerunLookback())
	lateInterval := githubTestsRunsInterval(claim, true, handler.refreshLookback(), handler.lateRerunLookback())
	// A claim with a deep range admits each listed run under the ceiling of the response that supplied it, as the chunked
	// route does; a claim without one keeps main's admission.
	var runsAdmit func(*time.Time, string) bool
	if githubTestsHoldsMidnight(claim) {
		runsAdmit = runsInterval.admitsUnder
	}
	runItems, runListPages, runsOver, err := collectGitHubTestsRunsPeeledBy(ctx, client, root, runPages, func(hi string) url.Values {
		return githubTestsRunsQuery(claim, handler.refreshLookback(), hi)
	}, runsInterval.peel, runsAdmit, true)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	if runsOver || len(runItems) > maxRuns {
		return CompleteRouteBatch{}, ErrPaginationCapExceeded
	}
	lateMaxPages := handler.lateRerunMaxPages()
	if lateMaxPages < 1 || lateMaxPages > githubTestsLateRerunMaxPages {
		return CompleteRouteBatch{}, ErrInvalidConfiguration
	}
	// The deep range of the claim that holds a UTC midnight, as in the chunked
	// route. A stop (budget or a saturated second; this collector does not tell
	// them apart) is recorded as the late_rerun_runs page-budget observation.
	var lateItems []json.RawMessage
	lateListPages, lateOver := 0, false
	if githubTestsHoldsMidnight(claim) {
		lateItems, lateListPages, lateOver, err = collectGitHubTestsRunsPeeledBy(ctx, client, root, lateMaxPages, func(hi string) url.Values {
			return githubTestsLateRerunRunsQuery(claim, handler.refreshLookback(), handler.lateRerunLookback(), hi)
		}, lateInterval.peel, lateInterval.admitsUnder, true)
		if err != nil {
			return CompleteRouteBatch{}, err
		}
	}
	regularInterval, deepInterval := githubTestsArtifactInterval(claim, false, 0), githubTestsCreatedInterval{deep: true}
	artifactRunItems, artifactListPages, artifactsOver, err := collectGitHubTestsRunsPeeledBy(ctx, client, root, runPages, func(hi string) url.Values {
		return githubTestsArtifactRunQueryAt(claim, hi)
	}, regularInterval.peel, nil, false)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	if artifactsOver || len(artifactRunItems) > maxRuns {
		return CompleteRouteBatch{}, ErrPaginationCapExceeded
	}
	artifactRuns := make([]githubTestsListedRun, 0, len(artifactRunItems))
	for _, raw := range artifactRunItems {
		artifactRuns = append(artifactRuns, githubTestsListedRun{raw: raw})
	}
	var relistPages int
	relistStopped := false
	if githubTestsHoldsMidnight(claim) {
		deepInterval = githubTestsArtifactInterval(claim, true, handler.lateRerunLookback())
		relistItems, pages, over, err := collectGitHubTestsRunsPeeledBy(ctx, client, root, lateMaxPages, func(hi string) url.Values {
			return githubTestsRelistQueryAt(claim, handler.lateRerunLookback(), hi)
		}, deepInterval.peel, nil, false)
		if err != nil {
			return CompleteRouteBatch{}, err
		}
		relistPages, relistStopped = pages, over
		for _, raw := range relistItems {
			artifactRuns = append(artifactRuns, githubTestsListedRun{raw: raw, relist: true})
		}
	}
	pipelines := make([]githubTestsPipelineRow, 0, len(runItems))
	jobs := make([]githubTestsJobRow, 0)
	acceptance := make([]githubTestsAcceptanceRow, 0)
	suites := make([]testSuiteResultRow, 0)
	cases := make([]testCaseResultRow, 0)
	coverage := make([]coverageSnapshotRow, 0)
	incomplete := make([]GitHubTestsIncomplete, 0)
	if lateOver {
		incomplete = mergeGitHubTestsIncomplete(incomplete, GitHubTestsIncomplete{
			Component: githubTestsLateRerunRunsComponent, Cause: githubTestsPageBudgetCause, Count: 1,
		})
	}
	// Durable markers for expired report artifacts (CHAOS-13980). This route carries them in the result, like the chunked route,
	// so the shared watermark policy sees the same evidence for the same loss.
	var (
		skippedArtifacts     []GitHubTestsSkippedArtifact
		skippedOverflow      int
		skippedCauseOverflow map[string]bool
		skippedCauseCount    map[string]int
	)
	if relistStopped {
		incomplete = append(incomplete, GitHubTestsIncomplete{Component: githubTestsLateRerunReportsComponent, Cause: githubTestsPageBudgetCause, Count: 1})
	}
	requests, pages := 1+runListPages+lateListPages+artifactListPages+relistPages, runListPages+lateListPages+artifactListPages+relistPages
	policyCache := map[string]githubTestsPolicy{}
	for index, raw := range append(append([]json.RawMessage{}, runItems...), lateItems...) {
		run, err := decodeGitHubWorkflowRun(raw)
		if err != nil {
			return CompleteRouteBatch{}, providerfoundation.RouteFailure(githubRunsRoute, providerfoundation.ReasonBodyDecode, err)
		}
		pipeline, include := normalizeGitHubTestsPipeline(claim, repoID, run, normalizedAt)
		keep := include && runsInterval.admits(pipeline.QueuedAt) && githubTestsRunWanted(run, pipeline, claim)
		if index >= len(runItems) {
			keep = include && lateInterval.admits(pipeline.QueuedAt) && githubTestsLateRerunWanted(run, claim, handler.refreshLookback())
		}
		if !keep {
			continue
		}
		pipelines = append(pipelines, pipeline)
		jobPage, pageErr := providerfoundation.CollectGitHubLinkPages(ctx, client, providerfoundation.GitHubPageOptions{
			Path: root + "/actions/runs/" + url.PathEscape(pipeline.RunID) + "/jobs", Route: githubRunJobsRoute, Query: url.Values{"per_page": {"100"}}, DataKey: "jobs", MaxPages: jobPages,
		})
		if pageErr != nil {
			return CompleteRouteBatch{}, pageErr
		}
		requests += jobPage.Pages
		pages += jobPage.Pages
		// DELIBERATE DIVERGENCE from the chunked route's per-run disposition
		// (CHAOS-4142). This non-chunked Collect is production-dead for
		// cicd/tests: execution_registry.go:476 marks {github,gitlab} x
		// {cicd,tests} Chunked unconditionally, and chunked_executor.go routes
		// any ChunkedCompleteRouteHandler to CollectChunks, so nothing reaches
		// this cap. It survives as the oracle/comparison implementation. If a
		// fixture ever crosses this cap, mirror recordGitHubTestsPerRunTruncation
		// here rather than treating the refusal as intended behavior.
		if jobPage.PageBudgetExhausted {
			return CompleteRouteBatch{}, ErrPaginationCapExceeded
		}
		runJobs := make([]githubTestsJobRow, 0, len(jobPage.Items))
		for _, jobRaw := range jobPage.Items {
			job, err := decodeGitHubTestsJob(jobRaw)
			if err != nil {
				return CompleteRouteBatch{}, providerfoundation.RouteFailure(githubRunJobsRoute, providerfoundation.ReasonBodyDecode, err)
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
	for _, listed := range artifactRuns {
		run, err := decodeGitHubWorkflowRun(listed.raw)
		if err != nil {
			return CompleteRouteBatch{}, providerfoundation.RouteFailure(githubRunsRoute, providerfoundation.ReasonBodyDecode, err)
		}
		pipeline, include := normalizeGitHubTestsPipeline(claim, repoID, run, normalizedAt)
		interval := regularInterval
		if listed.relist {
			interval = deepInterval
		}
		if !include || !githubTestsRunReadable(run) || githubTestsArtifactRunOutside(pipeline, claim, listed.relist) || !interval.admits(pipeline.QueuedAt) {
			continue
		}
		artPage, pageErr := providerfoundation.CollectGitHubLinkPages(ctx, client, providerfoundation.GitHubPageOptions{
			Path: root + "/actions/runs/" + url.PathEscape(pipeline.RunID) + "/artifacts", Route: githubRunArtifactsRoute, Query: url.Values{"per_page": {strconv.Itoa(githubTestsArtifactsPerPage)}}, DataKey: "artifacts", MaxPages: 1,
		})
		if pageErr != nil {
			return CompleteRouteBatch{}, pageErr
		}
		requests += artPage.Pages
		pages += artPage.Pages
		// DELIBERATE DIVERGENCE from the chunked route's per-run disposition
		// (CHAOS-4142). This non-chunked Collect is production-dead for
		// cicd/tests: execution_registry.go:476 marks {github,gitlab} x
		// {cicd,tests} Chunked unconditionally, and chunked_executor.go routes
		// any ChunkedCompleteRouteHandler to CollectChunks, so nothing reaches
		// this cap. It survives as the oracle/comparison implementation. If a
		// fixture ever crosses this cap, mirror recordGitHubTestsPerRunTruncation
		// here rather than treating the refusal as intended behavior.
		artifacts := make([]githubTestsArtifactPayload, 0, len(artPage.Items))
		for _, artifactRaw := range artPage.Items {
			artifact, err := decodeGitHubTestsArtifact(artifactRaw)
			if err != nil {
				return CompleteRouteBatch{}, providerfoundation.RouteFailure(githubRunArtifactsRoute, providerfoundation.ReasonBodyDecode, err)
			}
			if artifact.ID == "" {
				return CompleteRouteBatch{}, providerfoundation.ErrNormalizationInvalid
			}
			if selected, _ := githubTestsArtifactSelectionSeam(artifact.Name); !selected {
				continue
			}
			artifacts = append(artifacts, artifact)
		}
		artifacts, superseded := githubTestsKeepNewestArtifactPerName(artifacts)
		supersededIDs := githubTestsSupersededIDs(superseded)
		// CHAOS-13980: same disposition as the chunked route -- expired artifacts are split out after newest-per-name and before the
		// cap, every expired artifact that passed the selection seam is recorded once per (run, artifact) with the same durable marker
		// (contents unknown: fail closed).
		artifacts, expiredItems := githubTestsSplitExpiredArtifacts(artifacts)
		for _, artifact := range expiredItems {
			if githubTestsSkippedMarkerRecorded(skippedArtifacts, pipeline.RunID, string(artifact.ID), githubTestsArtifactExpiredCause) {
				continue
			}
			incomplete = recordGitHubTestsSkippedArtifact(
				incomplete, client, claim, claim.SourceName, pipeline.RunID,
				githubTestsArtifactExpiredCause,
			)
			skippedArtifacts, skippedOverflow, skippedCauseOverflow, skippedCauseCount = appendGitHubTestsSkippedArtifact(
				skippedArtifacts, skippedOverflow, skippedCauseOverflow, skippedCauseCount,
				GitHubTestsSkippedArtifact{
					RunID: pipeline.RunID, ArtifactID: string(artifact.ID), Name: artifact.Name,
					Cause: githubTestsArtifactExpiredCause,
				},
			)
		}
		if artPage.PageBudgetExhausted {
			return CompleteRouteBatch{}, ErrPaginationCapExceeded
		}
		artifacts, _, artifactsCapped, admitRequests, admitErr := githubTestsAdmitReportArtifacts(ctx, client, root, artifacts, maxArtifacts)
		requests += admitRequests
		if admitErr != nil {
			return CompleteRouteBatch{}, admitErr
		}
		if artifactsCapped {
			return CompleteRouteBatch{}, ErrPaginationCapExceeded
		}
		for _, artifact := range artifacts {
			// notFound is not distinguished here: this route has no totality
			// accounting, and its own len(archive)==0 branch below already
			// treats a routine 404/410 and a genuinely empty 2xx body
			// identically (CHAOS-4185 codex round 3 only changed the
			// CHUNKED route's accounting, not this oracle's disposition).
			archive, used, _, downloadErr := downloadGitHubTestsListedArtifact(ctx, client, root, string(artifact.ID), maxArtifactBytes, artifact.listedSize())
			requests += used
			if downloadErr != nil {
				// Same disposition as an unreadable container below: an
				// artifact whose bytes could never be fetched is skipped and
				// the walk continues (CHAOS-4191, extending CHAOS-4177).
				if errors.Is(downloadErr, ErrGitHubTestsArtifactUnavailable) {
					incomplete = recordGitHubTestsSkippedArtifact(
						incomplete, client, claim, claim.SourceName, pipeline.RunID,
						githubTestsArtifactUnavailableCause,
					)
					continue
				}
				return CompleteRouteBatch{}, downloadErr
			}
			if len(archive) == 0 {
				continue
			}
			rows, parseErr := parseGitHubTestsReportArtifact(archive, string(artifact.ID), githubTestsReportSlotFor(run, artifact), supersededIDs[githubTestsArtifactSlotName(artifact)], repoID, pipeline.RunID, claim.OrgID, pipeline.StartedAtPtr(), pipeline.FinishedAt, normalizedAt)
			if parseErr != nil {
				// Same disposition as the chunked route: an archive that will
				// not open is skipped and the walk continues (CHAOS-4177).
				if errors.Is(parseErr, ErrGitHubTestsArchiveUnreadable) {
					incomplete = recordGitHubTestsSkippedArtifact(
						incomplete, client, claim, claim.SourceName, pipeline.RunID,
						githubTestsUnreadableArchiveCause,
					)
					continue
				}
				return CompleteRouteBatch{}, fmt.Errorf("%w: artifact parse failed: %v", ErrGitHubTestsIncomplete, parseErr)
			}
			reportIncomplete, optional := rows.optionalIncomplete()
			if !optional {
				return CompleteRouteBatch{}, fmt.Errorf("%w: reports skipped=%d: unsafe archive bounds", ErrGitHubTestsIncomplete, rows.Skipped)
			}
			for _, observation := range reportIncomplete {
				incomplete = mergeGitHubTestsIncomplete(incomplete, observation)
			}
			// The two size-guard skips carry their durable markers here as on the chunked route, so the watermark advances for
			// them (CHAOS-14030). malformed/unreadable markers stay dropped: the divergence noted below predates this.
			for _, marker := range rows.SkippedArtifacts {
				if githubTestsIsMemberGuardCause(marker.Cause) {
					skippedArtifacts, skippedOverflow, skippedCauseOverflow, skippedCauseCount = appendGitHubTestsSkippedArtifact(
						skippedArtifacts, skippedOverflow, skippedCauseOverflow, skippedCauseCount, marker,
					)
				}
			}
			recordGitHubTestsNonReportMembers(client, claim, rows)
			client.Metrics.RecordDuplicateTestCase(claim.Provider, claim.Dataset, rows.DuplicateCases)
			if rows.DuplicateCases > 0 {
				slog.Info(
					"within-suite duplicate test-case names disambiguated with an ordinal suffix",
					"provider", claim.Provider, "dataset", claim.Dataset, "unit", claim.ID,
					"repository", repo.FullName, logging.ProviderIDAttr("run", pipeline.RunID), "count", rows.DuplicateCases,
				)
			}
			client.Metrics.RecordNonFiniteTestTime(claim.Provider, claim.Dataset, rows.NonFiniteTimes)
			if rows.NonFiniteTimes > 0 {
				slog.Info(
					"non-finite test time stored as NULL",
					"provider", claim.Provider, "dataset", claim.Dataset, "unit", claim.ID,
					"repository", repo.FullName, logging.ProviderIDAttr("run", pipeline.RunID), "count", rows.NonFiniteTimes,
				)
			}
			client.Metrics.RecordDuplicateTestSuite(claim.Provider, claim.Dataset, rows.DuplicateSuites)
			if rows.DuplicateSuites > 0 {
				slog.Info(
					"sibling suite collision resolved: same-named suite objects disambiguated with an ordinal suffix",
					"provider", claim.Provider, "dataset", claim.Dataset, "unit", claim.ID,
					"repository", repo.FullName, logging.ProviderIDAttr("run", pipeline.RunID), "count", rows.DuplicateSuites,
				)
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
	// This route never builds a GitHubTestsSkippedArtifact marker for any
	// cause -- it has no persisted, resumable cursor to hang one off, and
	// (per CHAOS-4315) it stays fully terminal on an oversized artifact
	// rather than skip-and-continue, so it never reaches this point with an
	// oversized observation in the first place. Passing nil/0 therefore
	// means this oracle is conservative for artifact_unavailable/
	// unreadable_archive relative to the chunked production route once that
	// route actually has markers to show -- an intentional, precedented
	// divergence (CHAOS-4315 already diverges the two routes on the
	// oversized disposition itself; production dispatch always executes
	// CollectChunks, never this route, per execution_registry.go).
	if githubTestsBlocksWatermark(incomplete, skippedArtifacts, skippedOverflow, skippedCauseOverflow, skippedCauseCount) {
		watermark = nil
	}
	// One summary line per unit, same as the chunked production route
	// (githubTestsLogArtifactSkipSummary, CHAOS-4588) -- kept symmetric so the
	// oracle/comparator path never drifts from what production actually logs.
	// This route never builds SkippedArtifacts or the excluded-artifact
	// counters -- it has no persisted, resumable cursor to hang them off
	// (same divergence as skippedArtifacts above; see the oversized-artifact
	// doc comment).
	githubTestsLogArtifactSkipSummary(claim, repo.FullName, incomplete, skippedArtifacts, skippedCauseOverflow, 0, 0, nil, 0)
	result := map[string]any{
		"pipeline_runs_synced": len(pipelines), "job_runs_synced": len(jobs), "acceptance_checks_synced": len(acceptance),
		"test_suites_synced": len(suites), "test_cases_synced": len(cases), "coverage_snapshots_synced": len(coverage), "repo": repo.FullName,
		GitHubTestsResultKeyReportsComplete: len(incomplete) == 0,
		"reports_skipped":                   githubTestsIncompleteCount(incomplete),
		"incomplete":                        incomplete,
	}
	// The four skip-marker fields are added only when this route recorded skip evidence, so a clean completion serializes exactly
	// as it did before CHAOS-13980.
	addGitHubTestsNonChunkedSkipFields(result, skippedArtifacts, skippedOverflow, skippedCauseOverflow, skippedCauseCount)
	return CompleteRouteBatch{Effects: effects, Watermark: watermark, Result: result, Evidence: FetchEvidence{Provider: claim.Provider, Dataset: claim.Dataset, Requests: requests, Pages: pages, Records: len(pipelines) + len(jobs) + len(acceptance) + len(suites) + len(cases) + len(coverage)}}, nil
}

// addGitHubTestsNonChunkedSkipFields adds the four skip-marker fields to the non-chunked result when ANY skip evidence exists (a
// marker, an overflow count, a per-cause overflow flag or a per-cause count), all four together; with none, the result is left
// exactly as it was before CHAOS-13980.
func addGitHubTestsNonChunkedSkipFields(
	result map[string]any, skipped []GitHubTestsSkippedArtifact, overflow int, causeOverflow map[string]bool, causeCount map[string]int,
) {
	if len(skipped) == 0 && overflow == 0 && len(causeOverflow) == 0 && len(causeCount) == 0 {
		return
	}
	result["skipped_artifacts"] = skipped
	result["skipped_artifacts_overflow"] = overflow
	result["skipped_artifact_cause_overflow"] = causeOverflow
	result["skipped_artifact_cause_count"] = causeCount
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

// githubTestsListedRun is one run of the artifacts listings, with the range it was listed in.
type githubTestsListedRun struct {
	raw    json.RawMessage
	relist bool
}

// githubTestsArtifactRunQuery selects the runs whose report artifacts are
// fetched: runs of ANY branch (default branch and pull-request branches alike;
// the run conclusion does not matter either). The listing runs up to BeforeAt,
// the same upper bound the pipeline/job pass has. Without it the listing walked
// on to the present, so a catch-up window counted every later run against the
// 10,000-run listing cap. Its start is githubTestsArtifactRunsCreatedFrom: a run
// created before the window that is still changing inside it must be listed.
func githubTestsArtifactRunQuery(claim Claim) url.Values {
	query := url.Values{"per_page": {"100"}}
	switch {
	case claim.SinceAt != nil && claim.BeforeAt != nil:
		query.Set("created", githubTestsArtifactRunsCreatedFrom(claim)+".."+claim.BeforeAt.UTC().Format(time.RFC3339))
	case claim.SinceAt != nil:
		query.Set("created", ">="+githubTestsArtifactRunsCreatedFrom(claim))
	case claim.BeforeAt != nil:
		query.Set("created", "*.."+claim.BeforeAt.UTC().Format(time.RFC3339))
	}
	return query
}

// githubTestsReportRunOutsideWindow decides whether a run's report artifacts
// (test suites/cases, coverage) belong to this claim's window. It is shared by
// the chunked route and this non-chunked comparison implementation so the two
// cannot grade each other apart.
//
// The upper bound: a run that started after the claim's upper bound is out of
// scope, except in a backfill chunk that knows the planner's tiling
// (BackfillNext). The run_started_at of a run is its LATEST attempt, so a run
// queued across before or re-run days later starts after before, while the
// listing keeps it by created_at. In such a chunk the ownership rule below
// decides alone: the run is read by the one chunk that owns it.
//
// The lower bound is on the run's own last-updated time, not its start time --
// a re-run, a retried job, or a suite that lands late all bump that value, so
// a run which is still changing stays in scope for as long as it keeps
// changing. Only a run that has not changed since well before this window is
// skipped.
//
// "Well before" is one full window width earlier than the window's own start,
// so a run is offered this window AND the one before it -- two consecutive
// looks -- before its report artifacts stop being re-fetched. A claim reads
// only the runs its listing reaches: runs created from its floor
// (githubTestsArtifactRunsCreatedFrom). A run re-run after it left every floor
// is read by the daily late re-run re-list instead
// (github_tests_late_rerun_reports.go), whose rule floors the width at one day,
// so such a run can take one more look. The provider
// documents no bound on how long a run's artifacts can take to become
// listable after the run itself stops changing, so the margin is sized off
// the claim's own cadence (the only figure this route actually knows) rather
// than a fixed duration picked from a small sample.
//
// A backfill unit takes one look. Its chunks run in unit-id order, not time
// order, so the next chunk's look is no later look; it only read the previous
// chunk's reports again. It reads a run whose last-updated time is at or after
// since and either before its before, or later but created before the next
// chunk's listing floor (the next chunk cannot list that run), or later and
// the chunk is the backfill's last. The next chunk is the real one the claim
// carries (BackfillNext, CHAOS-14222); a claim without it guesses a next chunk
// of its own width. So each run is read by one chunk: the one its last-updated
// time falls in, or the last chunk that lists it. The last chunk's set stays
// bounded by its listing: runs created from its floor to its before.
//
// The claim edges fail open: a claim missing either edge (an open-start or a
// still-open claim) filters nothing on that edge, and a run with no
// last-updated time is never excluded -- a run we cannot date is a run we
// must not silently drop.
func githubTestsReportRunOutsideWindow(pipeline githubTestsPipelineRow, claim Claim) bool {
	startedAfter := claim.BeforeAt != nil && pipeline.StartedAt.After(claim.BeforeAt.UTC())
	if claim.SinceAt == nil || claim.BeforeAt == nil || pipeline.FinishedAt == nil {
		return startedAfter
	}
	since := claim.SinceAt.UTC()
	width := claim.BeforeAt.UTC().Sub(since)
	if width <= 0 {
		return startedAfter
	}
	finished := pipeline.FinishedAt.UTC()
	if claim.Mode == "backfill" {
		if startedAfter && claim.BackfillNext == nil {
			return true
		}
		return finished.Before(since) || !githubTestsBackfillReadsLaterRun(pipeline, claim, finished)
	}
	return startedAfter || finished.Before(since.Add(-width))
}

func githubTestsBackfillReadsLaterRun(pipeline githubTestsPipelineRow, claim Claim, finished time.Time) bool {
	before := claim.BeforeAt.UTC()
	if finished.Before(before) || pipeline.QueuedAt == nil {
		return true
	}
	next := claim
	nextBefore := before.Add(before.Sub(claim.SinceAt.UTC()))
	if claim.BackfillNext != nil {
		if claim.BackfillNext.BeforeAt == nil {
			return true
		}
		nextBefore = claim.BackfillNext.BeforeAt.UTC()
	}
	next.SinceAt, next.BeforeAt = &before, &nextBefore
	nextFloor, err := time.Parse(time.DateOnly, githubTestsArtifactRunsCreatedFrom(next))
	if err != nil {
		return true
	}
	return pipeline.QueuedAt.UTC().Before(nextFloor)
}

// githubTestsRunReadable: a run's report artifacts are read only once the run
// is completed. A run read mid-flight lists a partial artifact set whose ids
// differ from the finished run's, so the same cases land under two keys.
func githubTestsRunReadable(run gitHubWorkflowRunPayload) bool {
	return stringValue(run.Status) == "completed"
}

// githubTestsArtifactRunsCreatedFrom is the date a run listing starts from:
// the since date minus the claim's width, at least one day. It is never
// stricter than githubTestsReportRunOutsideWindow, which keeps a run that
// finished at or after since minus width, so a run created before the window
// and completed inside it is still listed. An incremental claim that holds a
// UTC midnight also lists the deep range below this floor
// (githubTestsRelistQueryAt).
func githubTestsArtifactRunsCreatedFrom(claim Claim) string {
	width := 24 * time.Hour
	if claim.BeforeAt != nil {
		width = max(width, claim.BeforeAt.UTC().Sub(claim.SinceAt.UTC()))
	}
	return claim.SinceAt.UTC().Add(-width).Format(time.DateOnly)
}

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
	response, err := client.DoRoute(ctx, http.MethodGet, githubRepoRoute+"/branches/{branch}/protection/required_status_checks", path, nil)
	if err != nil {
		var providerErr *providerfoundation.ProviderError
		if errors.As(err, &providerErr) && providerErr.StatusCode > 0 {
			return githubTestsPolicy{provenance: fmt.Sprintf("github.branch_protection.http_%d", providerErr.StatusCode)}, nil
		}
		return githubTestsPolicy{}, err
	}
	defer response.Body.Close()
	var payload githubTestsRequiredPayload
	raw, readErr := providerfoundation.ReadResponseBody(response, nativeMaxObjectBytes)
	if readErr != nil || len(raw) > nativeMaxObjectBytes || providerfoundation.DecodeResponseJSON(response, raw, &payload, 0) != nil {
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

// downloadGitHubTestsArtifact's notFound return distinguishes a ROUTINE
// provider-side disappearance (the artifact expired or was deleted between
// listing and download -- GitHub answers 404/410 for this, a documented,
// ordinary outcome) from every other nil-body case. The two used to be
// indistinguishable to callers: both returned (nil, N, nil). That
// conflation is exactly what let a genuinely benign vanished artifact be
// misclassified as an unreadable one once a caller started treating an
// empty body as evidence of totality (CHAOS-4185 codex round 3) -- two
// artifacts that simply expired between listing and download would
// otherwise satisfy the totality floor and terminalize a healthy unit.
func downloadGitHubTestsArtifact(
	ctx context.Context, client *providerfoundation.HTTPClient, root, artifactID string,
	maxBytes int64,
) (archive []byte, used int, notFound bool, err error) {
	return downloadGitHubTestsListedArtifact(ctx, client, root, artifactID, maxBytes, -1)
}

// downloadGitHubTestsListedArtifact is downloadGitHubTestsArtifact with the
// artifact's listed size (-1 when unknown): a listed size above the cap
// refuses a response without a Content-Length before any body byte is read.
func downloadGitHubTestsListedArtifact(
	ctx context.Context, client *providerfoundation.HTTPClient, root, artifactID string,
	maxBytes, listedSize int64,
) (archive []byte, used int, notFound bool, err error) {
	ctx, wire := providerfoundation.WithWireCount(ctx)
	defer func() { used = wire.Requests() }()
	path := root + "/actions/artifacts/" + url.PathEscape(artifactID) + "/zip"
	var fetch githubTestsArtifactFetch = func(ctx context.Context) (*http.Response, error) {
		return client.DoRoute(ctx, http.MethodGet, githubArtifactZipRoute, path, nil)
	}
	var refresh func(context.Context) (githubTestsArtifactFetch, error)
	response, err := fetch(ctx)
	if err != nil {
		if githubTestsGoneError(err) {
			return nil, 0, true, nil
		}
		return nil, 0, false, err
	}
	if response.StatusCode >= 300 && response.StatusCode < 400 {
		location := response.Header.Get("Location")
		response.Body.Close()
		if location == "" {
			return nil, 0, false, providerfoundation.ResponseFailure(response, "redirect", fmt.Errorf(
				"%w: redirect status=%d carried no Location header",
				ErrGitHubTestsArtifactUnavailable, response.StatusCode,
			))
		}
		api := fetch
		fetch = githubTestsArtifactBlobFetch(client, location)
		refresh = func(ctx context.Context) (githubTestsArtifactFetch, error) {
			return githubTestsArtifactRelocate(ctx, client, api)
		}
		response, err = fetch(ctx)
		if err != nil {
			return nil, 0, false, err
		}
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusNotFound || response.StatusCode == http.StatusGone {
		return nil, 0, true, nil
	}
	if response.StatusCode >= 400 {
		return nil, 0, false, githubTestsArtifactStatusFailure(response)
	}
	var body []byte
	var over int64
	var readErr error
	if response.ContentLength < 0 && listedSize > maxBytes {
		over = listedSize
	} else {
		body, over, readErr = providerfoundation.ReadResponseBodyWithin(response, maxBytes, logging.TransportFailure)
	}
	if readErr != nil {
		return nil, 0, false, providerfoundation.ResponseFailure(response, providerfoundation.ReasonBodyRead, fmt.Errorf(
			"%w: artifact download read failed: %w", ErrGitHubTestsIncomplete, errors.Unwrap(readErr),
		))
	}
	if over > 0 {
		// over is the final response's Content-Length when that is above the
		// cap (nothing was read), else cap+1: the read never passes cap+1, so
		// the true size beyond it is not known. Carried on the typed error (not just
		// the message) so the chunked route's durable marker
		// (GitHubTestsSkippedArtifact, CHAOS-4315) gets real ints, not a
		// re-parse of formatted text.
		oversized := providerfoundation.ResponseFailure(response, providerfoundation.ReasonBodyTooLarge, &githubTestsArtifactOversizedError{
			SizeBytes: over, CapBytes: maxBytes,
		})
		if !strings.EqualFold(strings.TrimSpace(response.Header.Get("Accept-Ranges")), "bytes") {
			logGitHubTestsRangeOutcome(artifactID, over, maxBytes, "range_not_advertised", wire)
			return nil, 0, false, oversized
		}
		response.Body.Close()
		size := int64(-1)
		if response.ContentLength > maxBytes {
			size = response.ContentLength
		}
		archive, gone, rangeErr := readGitHubTestsArtifactReports(ctx, fetch, refresh, size, maxBytes)
		var unusable *githubTestsRangeUnusableError
		if errors.As(rangeErr, &unusable) {
			logGitHubTestsRangeOutcome(artifactID, over, maxBytes, unusable.reason, wire)
			return nil, 0, false, oversized
		}
		if rangeErr == nil && !gone {
			logGitHubTestsRangeOutcome(artifactID, over, maxBytes, "range_read", wire)
		}
		return archive, 0, gone, rangeErr
	}
	return body, 0, false, nil
}

// logGitHubTestsRangeOutcome records what became of one over-cap artifact:
// read by range, or kept as artifact_oversized and why.
func logGitHubTestsRangeOutcome(artifactID string, size, capBytes int64, outcome string, wire *providerfoundation.WireCount) {
	slog.Info(
		"github tests over-cap artifact",
		"artifact", artifactID, "outcome", outcome, "size_bytes", size, "cap_bytes", capBytes, "requests", wire.Requests(),
	)
}

var _ CompleteRouteHandler = GitHubTestsRouteHandler{}

func decodeGitHubWorkflowRun(raw json.RawMessage) (gitHubWorkflowRunPayload, error) {
	var run gitHubWorkflowRunPayload
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if decoder.Decode(&run) != nil {
		return run, providerfoundation.ErrNormalizationInvalid
	}
	return run, nil
}

func decodeGitHubTestsJob(raw json.RawMessage) (githubTestsJobPayload, error) {
	var job githubTestsJobPayload
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if decoder.Decode(&job) != nil {
		return job, providerfoundation.ErrNormalizationInvalid
	}
	return job, nil
}

func decodeGitHubTestsArtifact(raw json.RawMessage) (githubTestsArtifactPayload, error) {
	var artifact githubTestsArtifactPayload
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if decoder.Decode(&artifact) != nil {
		return artifact, providerfoundation.ErrNormalizationInvalid
	}
	return artifact, nil
}
