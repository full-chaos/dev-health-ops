package providersync

import (
	"encoding/json"
	"sort"
	"time"
)

// testOpsRunReportCompletenessDestination is the per-run report-completeness table (migration 153): one row per walked attempt of
// a run with report sources, complete or partial with the reasons. Its name sorts after test_suite_results, so the effect
// committer (sortEffectBatches) writes it after the run's report rows of the same emission.
const testOpsRunReportCompletenessDestination = "testops_run_report_completeness"

// testOpsRunReportCompletenessRow is one walk's verdict on one attempt of a run. ReportsExpected counts the report sources the
// walk saw (for GitHub: the selected artifacts, newest per name, expired ones included); ReportsAdmitted those whose rows it wrote.
type testOpsRunReportCompletenessRow struct {
	OrgID           string    `json:"org_id"`
	RepoID          string    `json:"repo_id"`
	RunID           string    `json:"run_id"`
	RunAttempt      int64     `json:"run_attempt"`
	Provider        string    `json:"provider"`
	Complete        bool      `json:"complete"`
	Reasons         []string  `json:"reasons"`
	ReportsExpected int64     `json:"reports_expected"`
	ReportsAdmitted int64     `json:"reports_admitted"`
	LastSynced      time.Time `json:"last_synced"`
	VerdictVersion  uint64    `json:"verdict_version"`
}

// testOpsRunReportVerdictShift puts the verdict above every millisecond timestamp (2^42 ms is after the year 2100).
const testOpsRunReportVerdictShift = 42

// testOpsRunReportVerdictVersion is the table's ReplacingMergeTree version: within one (run, attempt), a complete row outranks
// every partial row, and between rows of one verdict the newer walk wins. An attempt's report set is fixed once the run is
// completed, so a later walk that loses a report it read before is a fetch failure, not news: it must not turn the run partial.
func testOpsRunReportVerdictVersion(complete bool, lastSynced time.Time) uint64 {
	version := uint64(lastSynced.UnixMilli())
	if complete {
		version |= 1 << testOpsRunReportVerdictShift
	}
	return version
}

// githubTestsRunReportLossCauses are the causes that name a report of the run as not read. Every cause a run's walk can record
// (githubTestsIncompleteVocabulary for run_artifacts, run_reports and report_member, and the skipped-artifact marker causes) is in
// exactly one of this set and githubTestsRunReportNotALossCauses; a cell holds the two sets to the vocabularies. A cause in
// neither still counts as a loss: missing is not complete.
//
// aggregate_of_parts is a loss until the producer drops only lossless aggregates (acp ruling on CHAOS-13826 C7): today's drop
// compares case identity and outcome only, so a dropped artifact may hold fields its parts do not.
var githubTestsRunReportLossCauses = map[string]struct{}{
	githubTestsPerRunCapCause:           {},
	githubTestsPerRunPageBudgetCause:    {},
	githubTestsArtifactUnavailableCause: {},
	githubTestsArtifactOversizedCause:   {},
	githubTestsUnreadableArchiveCause:   {},
	githubTestsMalformedCause:           {},
	githubTestsUnreadableCause:          {},
	githubTestsArtifactExpiredCause:     {},
	githubTestsAggregateMismatchCause:   {},
	githubTestsAggregateOfPartsCause:    {},
}

// githubTestsRunReportNotALossCauses are recorded causes that lose no report row.
var githubTestsRunReportNotALossCauses = map[string]struct{}{}

// githubTestsRunReportComponents are the incomplete components whose observations are about one run's reports.
var githubTestsRunReportComponents = map[string]struct{}{
	githubTestsRunArtifactsComponent: {},
	githubTestsRunReportsComponent:   {},
	githubTestsReportMemberComponent: {},
}

// githubTestsRunLossCounts is a copy of the cursor's per-cause loss counters, taken before a run is walked. The deltas after the
// run are its losses. Both counters are exact: a skipped-artifact marker counts its cause before the bounded sample drops it
// (appendGitHubTestsSkippedArtifact), and an incomplete observation adds to its (component, cause) count. The bounded marker
// sample is never read.
type githubTestsRunLossCounts struct {
	skipped    map[string]int
	incomplete map[string]int
}

func githubTestsTakeRunLossCounts(cursor githubTestsChunkCursor) githubTestsRunLossCounts {
	counts := githubTestsRunLossCounts{skipped: map[string]int{}, incomplete: map[string]int{}}
	for cause, count := range cursor.SkippedArtifactCauseCount {
		counts.skipped[cause] = count
	}
	for _, observation := range cursor.Incomplete {
		if _, ok := githubTestsRunReportComponents[observation.Component]; ok {
			counts.incomplete[observation.Cause] += observation.Count
		}
	}
	return counts
}

// githubTestsRunLossReasons are the sorted, distinct loss causes recorded between before and after. expired is the run's expired
// report artifacts: a revisit of the run does not record them again (the marker dedup), so they are counted from the walk itself.
func githubTestsRunLossReasons(before, after githubTestsRunLossCounts, expired int) []string {
	seen := map[string]struct{}{}
	if expired > 0 {
		seen[githubTestsArtifactExpiredCause] = struct{}{}
	}
	for cause, count := range after.skipped {
		if count > before.skipped[cause] {
			seen[cause] = struct{}{}
		}
	}
	for cause, count := range after.incomplete {
		if count > before.incomplete[cause] {
			seen[cause] = struct{}{}
		}
	}
	reasons := make([]string, 0, len(seen))
	for cause := range seen {
		if _, notALoss := githubTestsRunReportNotALossCauses[cause]; notALoss {
			continue
		}
		reasons = append(reasons, cause)
	}
	sort.Strings(reasons)
	return reasons
}

// githubTestsRunAttempt is the 1-based attempt of the run payload (1 when the payload carries none).
func githubTestsRunAttempt(run gitHubWorkflowRunPayload) int64 {
	attempt, err := json.Number(stringValue(run.RunAttempt)).Int64()
	if err != nil || attempt < 1 {
		return 1
	}
	return attempt
}

// githubTestsRunReportCompleteness is the walk's verdict row on one run, or nil when the run had no report source and no
// per-run artifact truncation: such a run has no tests to name complete or partial.
func githubTestsRunReportCompleteness(
	pipeline githubTestsPipelineRow, run gitHubWorkflowRunPayload,
	before, after githubTestsRunLossCounts, expected, expired, admitted int,
	normalizedAt time.Time,
) []testOpsRunReportCompletenessRow {
	reasons := githubTestsRunLossReasons(before, after, expired)
	if expected == 0 && len(reasons) == 0 {
		return nil
	}
	complete := len(reasons) == 0
	return []testOpsRunReportCompletenessRow{{
		OrgID: pipeline.OrgID, RepoID: pipeline.RepoID, RunID: pipeline.RunID, RunAttempt: githubTestsRunAttempt(run),
		Provider: pipeline.Provider, Complete: complete, Reasons: reasons,
		ReportsExpected: int64(expected), ReportsAdmitted: int64(admitted),
		LastSynced: normalizedAt, VerdictVersion: testOpsRunReportVerdictVersion(complete, normalizedAt),
	}}
}
