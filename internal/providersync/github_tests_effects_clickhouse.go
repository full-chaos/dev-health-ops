package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// TestOpsClickHouseEffects owns the six destinations emitted by complete
// provider TestOps routes. Every readback is a FINAL point lookup over the
// complete tenant-prefixed natural key and compares the full persisted row.
type TestOpsClickHouseEffects struct {
	Conn  driver.Conn
	Lease providerfoundation.LeaseGuard
}

// GitHubTestsClickHouseEffects remains as the source-compatible name used by
// the already-landed GitHub route. GitLab deliberately reuses the same sink so
// both providers get identical atomic-write and exact-readback semantics.
type GitHubTestsClickHouseEffects = TestOpsClickHouseEffects

func (sink TestOpsClickHouseEffects) valid(ctx context.Context, claim Claim, destination string) error {
	providerOK := claim.Provider == "github" || claim.Provider == "gitlab"
	if ctx == nil || sink.Lease == nil || claim.Validate() != nil || !providerOK || (claim.Dataset != "tests" && claim.Dataset != "cicd") || !githubTestsDestination(destination) {
		return ErrInvalidConfiguration
	}
	return sink.Lease.Assert(ctx)
}

func githubTestsDestination(destination string) bool {
	switch destination {
	case "ci_pipeline_runs", "ci_job_runs", "ci_acceptance_checks", "test_suite_results", "test_case_results", "coverage_snapshots":
		return true
	default:
		return false
	}
}

// CHAOS-5045. The GitHub TestOps report phase deliberately RE-COLLECTS runs it
// has already seen: GitHub's per-run artifact listing has no documented
// availability bound, so a report can become listable minutes after the run's
// own updated_at has settled. An earlier fix bounded the collection window on
// updated_at instead, and codex round 1 proved that silently and PERMANENTLY
// drops such a late report while the watermark advances over the gap -- the
// run is skipped, the unit reports complete, and after midnight the
// date-granular server-side filter stops returning the run at all.
//
// So the fetch is never skipped. The DUPLICATION the ticket is actually about
// -- 7,072,971 raw test_case_results rows for 3,795,833 distinct keys on one
// repo, invisible to every reader that goes through FINAL -- is suppressed one
// level lower instead: if the store already holds exactly the rows this batch
// would write, the INSERT is skipped. A re-collection that found nothing new
// costs a read, not a rewrite, and a ReplacingMergeTree row is not rewritten
// with a fresh last_synced for a run nothing changed.
//
// last_synced is EXCLUDED from the comparison by construction: it is the very
// field that differs on every pass and the one whose churn this fixes. Every
// other column participates, so a genuinely changed report -- a retried job, a
// late-arriving suite, a status flip -- still writes.
//
// The comparison is done in Go against rows read back through FINAL, NOT by
// hashing in ClickHouse and again in Go: a cross-language digest would have to
// agree on NULL handling and float formatting between the two runtimes, which
// is exactly the class of parity trap this repo has been bitten by before.

// testOpsRunKey identifies the natural-key prefix a report batch is scoped to.
type testOpsRunKey struct{ orgID, repoID, runID string }

func sameOptionalString(a, b *string) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

func sameOptionalFloat(a, b *float64) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

// storedTestCaseRows reads back every persisted case row for one run through
// FINAL, keyed by the part of the natural key the run prefix does not fix.
func storedTestCaseRows(ctx context.Context, conn driver.Conn, key testOpsRunKey) (map[[2]string]testCaseResultRow, error) {
	rows, err := conn.Query(ctx, `SELECT suite_id,case_id,case_name,class_name,status,duration_seconds,toInt64(retry_attempt),failure_message,failure_type,stack_trace,is_quarantined != 0 FROM test_case_results FINAL WHERE org_id=? AND repo_id=? AND run_id=?`, key.orgID, key.repoID, key.runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	stored := map[[2]string]testCaseResultRow{}
	for rows.Next() {
		row := testCaseResultRow{OrgID: key.orgID, RepoID: key.repoID, RunID: key.runID}
		if err := rows.Scan(&row.SuiteID, &row.CaseID, &row.CaseName, &row.ClassName, &row.Status, &row.DurationSeconds, &row.RetryAttempt, &row.FailureMessage, &row.FailureType, &row.StackTrace, &row.IsQuarantined); err != nil {
			return nil, err
		}
		stored[[2]string{row.SuiteID, row.CaseID}] = row
	}
	return stored, rows.Err()
}

func sameTestCaseRow(want, got testCaseResultRow) bool {
	return want.CaseName == got.CaseName &&
		sameOptionalString(want.ClassName, got.ClassName) &&
		want.Status == got.Status &&
		sameOptionalFloat(want.DurationSeconds, got.DurationSeconds) &&
		want.RetryAttempt == got.RetryAttempt &&
		sameOptionalString(want.FailureMessage, got.FailureMessage) &&
		sameOptionalString(want.FailureType, got.FailureType) &&
		sameOptionalString(want.StackTrace, got.StackTrace) &&
		want.IsQuarantined == got.IsQuarantined
}

// testCaseBatchAlreadyStored reports whether every row in the batch is already
// persisted with identical content. A batch spanning several runs is only
// skipped when EVERY run matches -- a partial match still writes the whole
// batch, because the INSERT is the unit of atomicity here and splitting it
// would trade a cheap redundant write for a much worse partial-write seam.
func testCaseBatchAlreadyStored(ctx context.Context, conn driver.Conn, rows []testCaseResultRow) (bool, error) {
	byRun := map[testOpsRunKey][]testCaseResultRow{}
	for _, row := range rows {
		key := testOpsRunKey{row.OrgID, row.RepoID, row.RunID}
		byRun[key] = append(byRun[key], row)
	}
	for key, want := range byRun {
		stored, err := storedTestCaseRows(ctx, conn, key)
		if err != nil {
			return false, err
		}
		// A differing row COUNT is decisive on its own: a run that gained or
		// lost cases is a changed run, whatever the surviving rows say.
		if len(stored) != len(want) {
			return false, nil
		}
		for _, row := range want {
			got, ok := stored[[2]string{row.SuiteID, row.CaseID}]
			if !ok || !sameTestCaseRow(row, got) {
				return false, nil
			}
		}
	}
	return true, nil
}

// storedTestSuiteRows is the suite-level twin of storedTestCaseRows.
func storedTestSuiteRows(ctx context.Context, conn driver.Conn, key testOpsRunKey) (map[string]testSuiteResultRow, error) {
	rows, err := conn.Query(ctx, `SELECT suite_id,suite_name,framework,environment,toInt64(total_count),toInt64(passed_count),toInt64(failed_count),toInt64(skipped_count),toInt64(error_count),toInt64(quarantined_count),toInt64(retried_count),duration_seconds,started_at,finished_at,team_id,service_id FROM test_suite_results FINAL WHERE org_id=? AND repo_id=? AND run_id=?`, key.orgID, key.repoID, key.runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	stored := map[string]testSuiteResultRow{}
	for rows.Next() {
		row := testSuiteResultRow{OrgID: key.orgID, RepoID: key.repoID, RunID: key.runID}
		if err := rows.Scan(&row.SuiteID, &row.SuiteName, &row.Framework, &row.Environment, &row.TotalCount, &row.PassedCount, &row.FailedCount, &row.SkippedCount, &row.ErrorCount, &row.QuarantinedCount, &row.RetriedCount, &row.DurationSeconds, &row.StartedAt, &row.FinishedAt, &row.TeamID, &row.ServiceID); err != nil {
			return nil, err
		}
		stored[row.SuiteID] = row
	}
	return stored, rows.Err()
}

func sameTestSuiteRow(want, got testSuiteResultRow) bool {
	return want.SuiteName == got.SuiteName &&
		sameOptionalString(want.Framework, got.Framework) &&
		sameOptionalString(want.Environment, got.Environment) &&
		want.TotalCount == got.TotalCount &&
		want.PassedCount == got.PassedCount &&
		want.FailedCount == got.FailedCount &&
		want.SkippedCount == got.SkippedCount &&
		want.ErrorCount == got.ErrorCount &&
		want.QuarantinedCount == got.QuarantinedCount &&
		want.RetriedCount == got.RetriedCount &&
		sameOptionalFloat(want.DurationSeconds, got.DurationSeconds) &&
		sameOptionalTime(want.StartedAt, got.StartedAt) &&
		sameOptionalTime(want.FinishedAt, got.FinishedAt) &&
		sameOptionalString(want.TeamID, got.TeamID) &&
		sameOptionalString(want.ServiceID, got.ServiceID)
}

func testSuiteBatchAlreadyStored(ctx context.Context, conn driver.Conn, rows []testSuiteResultRow) (bool, error) {
	byRun := map[testOpsRunKey][]testSuiteResultRow{}
	for _, row := range rows {
		key := testOpsRunKey{row.OrgID, row.RepoID, row.RunID}
		byRun[key] = append(byRun[key], row)
	}
	for key, want := range byRun {
		stored, err := storedTestSuiteRows(ctx, conn, key)
		if err != nil {
			return false, err
		}
		if len(stored) != len(want) {
			return false, nil
		}
		for _, row := range want {
			got, ok := stored[row.SuiteID]
			if !ok || !sameTestSuiteRow(row, got) {
				return false, nil
			}
		}
	}
	return true, nil
}

func (sink TestOpsClickHouseEffects) WriteEffect(ctx context.Context, claim Claim, effect EffectBatch) error {
	if err := sink.valid(ctx, claim, effect.Destination); err != nil {
		return err
	}
	if sink.Conn == nil {
		return ErrInvalidConfiguration
	}
	switch effect.Destination {
	case "ci_pipeline_runs":
		rows, err := decodeEffectRows[githubTestsPipelineRow](effect)
		if err != nil {
			return err
		}
		if len(rows) == 0 {
			return sink.Lease.Assert(ctx)
		}
		batch, err := sink.Conn.PrepareBatch(ctx, `INSERT INTO ci_pipeline_runs (org_id,repo_id,run_id,pipeline_name,provider,status,queued_at,started_at,finished_at,duration_seconds,queue_seconds,retry_count,cancel_reason,trigger_source,commit_hash,branch,pr_number,team_id,service_id,last_synced)`)
		if err != nil {
			return err
		}
		defer batch.Abort()
		seen := make(map[string]struct{}, len(rows))
		for _, row := range rows {
			if err := validateGitHubTestsRow(claim, row.OrgID, row.RepoID, row.RunID, row.LastSynced); err != nil {
				return err
			}
			if err := validateTestOpsProvider(claim, row.Provider); err != nil {
				return err
			}
			if !recordGitHubTestsKey(seen, row.OrgID, row.RepoID, row.RunID) {
				return duplicateNaturalKeyError(effect.Destination, "org_id", row.OrgID, "repo_id", row.RepoID, "run_id", row.RunID)
			}
			if err := batch.Append(row.OrgID, row.RepoID, row.RunID, row.PipelineName, row.Provider, row.Status, row.QueuedAt, row.StartedAt, row.FinishedAt, row.DurationSeconds, row.QueueSeconds, row.RetryCount, row.CancelReason, row.TriggerSource, row.CommitHash, row.Branch, row.PRNumber, row.TeamID, row.ServiceID, row.LastSynced); err != nil {
				return err
			}
		}
		return sendGitHubTestsBatch(ctx, sink.Lease, batch)
	case "ci_job_runs":
		rows, err := decodeEffectRows[githubTestsJobRow](effect)
		if err != nil {
			return err
		}
		if len(rows) == 0 {
			return sink.Lease.Assert(ctx)
		}
		batch, err := sink.Conn.PrepareBatch(ctx, `INSERT INTO ci_job_runs (org_id,repo_id,run_id,job_id,job_name,stage,status,started_at,finished_at,duration_seconds,runner_type,retry_attempt,last_synced)`)
		if err != nil {
			return err
		}
		defer batch.Abort()
		seen := make(map[string]struct{}, len(rows))
		for _, row := range rows {
			if err := validateGitHubTestsRow(claim, row.OrgID, row.RepoID, row.RunID, row.LastSynced); err != nil || row.JobID == "" {
				if err != nil {
					return err
				}
				return ErrInvalidConfiguration
			}
			if !recordGitHubTestsKey(seen, row.OrgID, row.RepoID, row.RunID, row.JobID) {
				return duplicateNaturalKeyError(effect.Destination, "org_id", row.OrgID, "repo_id", row.RepoID, "run_id", row.RunID, "job_id", row.JobID)
			}
			if err := batch.Append(row.OrgID, row.RepoID, row.RunID, row.JobID, row.JobName, row.Stage, row.Status, row.StartedAt, row.FinishedAt, row.DurationSeconds, row.RunnerType, row.RetryAttempt, row.LastSynced); err != nil {
				return err
			}
		}
		return sendGitHubTestsBatch(ctx, sink.Lease, batch)
	case "ci_acceptance_checks":
		rows, err := decodeEffectRows[githubTestsAcceptanceRow](effect)
		if err != nil {
			return err
		}
		if len(rows) == 0 {
			return sink.Lease.Assert(ctx)
		}
		batch, err := sink.Conn.PrepareBatch(ctx, `INSERT INTO ci_acceptance_checks (org_id,repo_id,run_id,check_key,check_name,provider,requirement,result,rule_version,provenance,target_branch,pr_number,source_url,observed_at,last_synced)`)
		if err != nil {
			return err
		}
		defer batch.Abort()
		seen := make(map[string]struct{}, len(rows))
		for _, row := range rows {
			if err := validateGitHubTestsRow(claim, row.OrgID, row.RepoID, row.RunID, row.LastSynced); err != nil || row.CheckKey == "" {
				if err != nil {
					return err
				}
				return ErrInvalidConfiguration
			}
			if err := validateTestOpsProvider(claim, row.Provider); err != nil {
				return err
			}
			if !recordGitHubTestsKey(seen, row.OrgID, row.RepoID, row.RunID, row.CheckKey) {
				return duplicateNaturalKeyError(effect.Destination, "org_id", row.OrgID, "repo_id", row.RepoID, "run_id", row.RunID, "check_key", row.CheckKey)
			}
			if err := batch.Append(row.OrgID, row.RepoID, row.RunID, row.CheckKey, row.CheckName, row.Provider, row.Requirement, row.Result, row.RuleVersion, row.Provenance, row.TargetBranch, row.PRNumber, row.SourceURL, row.ObservedAt, row.LastSynced); err != nil {
				return err
			}
		}
		return sendGitHubTestsBatch(ctx, sink.Lease, batch)
	case "test_suite_results":
		rows, err := decodeEffectRows[testSuiteResultRow](effect)
		if err != nil {
			return err
		}
		if len(rows) == 0 {
			return sink.Lease.Assert(ctx)
		}
		// CHAOS-5045, suite-level twin of the case-level skip below.
		unchanged, err := testSuiteBatchAlreadyStored(ctx, sink.Conn, rows)
		if err != nil {
			return err
		}
		if unchanged {
			return sink.Lease.Assert(ctx)
		}
		batch, err := sink.Conn.PrepareBatch(ctx, `INSERT INTO test_suite_results (org_id,repo_id,run_id,suite_id,suite_name,framework,environment,total_count,passed_count,failed_count,skipped_count,error_count,quarantined_count,retried_count,duration_seconds,started_at,finished_at,team_id,service_id,last_synced)`)
		if err != nil {
			return err
		}
		defer batch.Abort()
		seen := make(map[string]struct{}, len(rows))
		for _, row := range rows {
			if err := validateGitHubTestsRow(claim, row.OrgID, row.RepoID, row.RunID, row.LastSynced); err != nil || row.SuiteID == "" {
				if err != nil {
					return err
				}
				return ErrInvalidConfiguration
			}
			if !recordGitHubTestsKey(seen, row.OrgID, row.RepoID, row.RunID, row.SuiteID) {
				return duplicateNaturalKeyError(effect.Destination, "org_id", row.OrgID, "repo_id", row.RepoID, "run_id", row.RunID, "suite_id", row.SuiteID)
			}
			counts, err := gitHubTestsCounts(row.TotalCount, row.PassedCount, row.FailedCount, row.SkippedCount, row.ErrorCount, row.QuarantinedCount, row.RetriedCount)
			if err != nil {
				return err
			}
			if err := batch.Append(row.OrgID, row.RepoID, row.RunID, row.SuiteID, row.SuiteName, row.Framework, row.Environment, counts[0], counts[1], counts[2], counts[3], counts[4], counts[5], counts[6], row.DurationSeconds, row.StartedAt, row.FinishedAt, row.TeamID, row.ServiceID, row.LastSynced); err != nil {
				return err
			}
		}
		return sendGitHubTestsBatch(ctx, sink.Lease, batch)
	case "test_case_results":
		rows, err := decodeEffectRows[testCaseResultRow](effect)
		if err != nil {
			return err
		}
		if len(rows) == 0 {
			return sink.Lease.Assert(ctx)
		}
		// CHAOS-5045: skip the INSERT when the store already holds these exact
		// rows. See the comment above testOpsRunKey for why the fetch is never
		// skipped and only the write is.
		unchanged, err := testCaseBatchAlreadyStored(ctx, sink.Conn, rows)
		if err != nil {
			return err
		}
		if unchanged {
			return sink.Lease.Assert(ctx)
		}
		batch, err := sink.Conn.PrepareBatch(ctx, `INSERT INTO test_case_results (org_id,repo_id,run_id,suite_id,case_id,case_name,class_name,status,duration_seconds,retry_attempt,failure_message,failure_type,stack_trace,is_quarantined,last_synced)`)
		if err != nil {
			return err
		}
		defer batch.Abort()
		seen := make(map[string]struct{}, len(rows))
		for _, row := range rows {
			if err := validateGitHubTestsRow(claim, row.OrgID, row.RepoID, row.RunID, row.LastSynced); err != nil || row.SuiteID == "" || row.CaseID == "" {
				if err != nil {
					return err
				}
				return ErrInvalidConfiguration
			}
			if !recordGitHubTestsKey(seen, row.OrgID, row.RepoID, row.RunID, row.SuiteID, row.CaseID) {
				return duplicateNaturalKeyError(effect.Destination, "org_id", row.OrgID, "repo_id", row.RepoID, "run_id", row.RunID, "suite_id", row.SuiteID, "case_id", row.CaseID)
			}
			retry, err := gitHubTestsUint32(row.RetryAttempt)
			if err != nil {
				return err
			}
			if err := batch.Append(row.OrgID, row.RepoID, row.RunID, row.SuiteID, row.CaseID, row.CaseName, row.ClassName, row.Status, row.DurationSeconds, retry, row.FailureMessage, row.FailureType, row.StackTrace, row.IsQuarantined, row.LastSynced); err != nil {
				return err
			}
		}
		return sendGitHubTestsBatch(ctx, sink.Lease, batch)
	case "coverage_snapshots":
		rows, err := decodeEffectRows[coverageSnapshotRow](effect)
		if err != nil {
			return err
		}
		if len(rows) == 0 {
			return sink.Lease.Assert(ctx)
		}
		batch, err := sink.Conn.PrepareBatch(ctx, `INSERT INTO coverage_snapshots (org_id,repo_id,run_id,snapshot_id,report_format,lines_total,lines_covered,line_coverage_pct,branches_total,branches_covered,branch_coverage_pct,functions_total,functions_covered,commit_hash,branch,pr_number,team_id,service_id,last_synced)`)
		if err != nil {
			return err
		}
		defer batch.Abort()
		seen := make(map[string]struct{}, len(rows))
		for _, row := range rows {
			if err := validateGitHubTestsRow(claim, row.OrgID, row.RepoID, row.RunID, row.LastSynced); err != nil || row.SnapshotID == "" {
				if err != nil {
					return err
				}
				return ErrInvalidConfiguration
			}
			if !recordGitHubTestsKey(seen, row.OrgID, row.RepoID, row.RunID, row.SnapshotID) {
				return duplicateNaturalKeyError(effect.Destination, "org_id", row.OrgID, "repo_id", row.RepoID, "run_id", row.RunID, "snapshot_id", row.SnapshotID)
			}
			linesTotal, err := gitHubTestsNullableUint32(row.LinesTotal)
			if err != nil {
				return err
			}
			linesCovered, err := gitHubTestsNullableUint32(row.LinesCovered)
			if err != nil {
				return err
			}
			branchesTotal, err := gitHubTestsNullableUint32(row.BranchesTotal)
			if err != nil {
				return err
			}
			branchesCovered, err := gitHubTestsNullableUint32(row.BranchesCovered)
			if err != nil {
				return err
			}
			functionsTotal, err := gitHubTestsNullableUint32(row.FunctionsTotal)
			if err != nil {
				return err
			}
			functionsCovered, err := gitHubTestsNullableUint32(row.FunctionsCovered)
			if err != nil {
				return err
			}
			prNumber, err := gitHubTestsNullableUint32(row.PRNumber)
			if err != nil {
				return err
			}
			if err := batch.Append(row.OrgID, row.RepoID, row.RunID, row.SnapshotID, row.ReportFormat, linesTotal, linesCovered, row.LineCoveragePct, branchesTotal, branchesCovered, row.BranchCoveragePct, functionsTotal, functionsCovered, row.CommitHash, row.Branch, prNumber, row.TeamID, row.ServiceID, row.LastSynced); err != nil {
				return err
			}
		}
		return sendGitHubTestsBatch(ctx, sink.Lease, batch)
	}
	return ErrInvalidConfiguration
}

func recordGitHubTestsKey(seen map[string]struct{}, parts ...string) bool {
	key := strings.Join(parts, "\x00")
	if _, exists := seen[key]; exists {
		return false
	}
	seen[key] = struct{}{}
	return true
}

// ErrDuplicateNaturalKey narrows ErrInvalidConfiguration to specifically a
// recordGitHubTestsKey rejection (CHAOS-4392). It exists so
// providerunit.deterministicTerminalCategory can terminalize a batch that
// still hits this rejection on its FIRST attempt instead of burning all 5
// River attempts re-sending the identical colliding batch: this class is
// deterministic by construction (the natural key collides on every retry,
// same as the artifact-oversized/pagination-cap categories beside it), so
// retrying it wastes exactly the same 5x as those did before they got their
// own category. This is defense-in-depth, not the primary fix -- the primary
// fix disambiguates known-deterministic collisions (within-suite duplicate
// test-case names) before they ever reach WriteEffect at all; this sentinel
// only covers a collision this file's callers did not anticipate.
var ErrDuplicateNaturalKey = fmt.Errorf("%w: duplicate natural key", ErrInvalidConfiguration)

// duplicateNaturalKeyError names the destination and the colliding natural
// key on a recordGitHubTestsKey rejection. Before CHAOS-4190 this site
// returned the bare, context-free ErrInvalidConfiguration -- a duplicate
// batch always failed the SAME way whether it was two rows the caller
// re-sent verbatim, an upstream bug producing a genuine collision, or (until
// CHAOS-4190's fix) two distinct artifacts of one run that never should have
// collided at all -- and nothing in the error said which. fields must be
// passed as alternating name/value pairs in the same order recordGitHubTestsKey
// was called with, so the message and the actual dedup key never drift apart.
//
// The returned error also carries a structured DuplicateNaturalKeyDetail
// (CHAOS-4557): before this, the destination/fields only ever reached a
// stdout log line via lifecycleErrorDetail's err.Error() -- lost the moment
// the worker container restarted, with sync_run_units left holding nothing
// but the bare "duplicate_natural_key" category. DuplicateNaturalKeyDetailFrom
// lets a caller persist the same fields durably instead of re-deriving them
// from prose.
func duplicateNaturalKeyError(destination string, fields ...string) error {
	pairs := make([]string, 0, len(fields)/2)
	detailFields := make([]DuplicateNaturalKeyField, 0, len(fields)/2)
	for index := 0; index+1 < len(fields); index += 2 {
		name, value := fields[index], fields[index+1]
		pairs = append(pairs, name+"="+value)
		detailFields = append(detailFields, DuplicateNaturalKeyField{Name: name, Value: value})
	}
	wrapped := fmt.Errorf("%w in %s batch (%s)", ErrDuplicateNaturalKey, destination, strings.Join(pairs, " "))
	return &duplicateNaturalKeyErr{
		error:  wrapped,
		detail: DuplicateNaturalKeyDetail{Table: destination, Fields: detailFields},
	}
}

// DuplicateNaturalKeyField is one ordered name/value pair from a colliding
// natural key (e.g. run_id, suite_id, case_id), in the exact order
// recordGitHubTestsKey was called with.
type DuplicateNaturalKeyField struct {
	Name  string
	Value string
}

// DuplicateNaturalKeyDetail is the structured form of a recordGitHubTestsKey
// rejection: which ClickHouse destination table refused the batch, and the
// exact fields of the natural key that collided.
type DuplicateNaturalKeyDetail struct {
	Table  string
	Fields []DuplicateNaturalKeyField
}

// duplicateNaturalKeyErr pairs the existing formatted error (unchanged
// Error()/errors.Is behavior -- it still unwraps to ErrDuplicateNaturalKey and,
// through that, ErrInvalidConfiguration) with the structured detail a caller
// can extract via DuplicateNaturalKeyDetailFrom without parsing prose.
type duplicateNaturalKeyErr struct {
	error
	detail DuplicateNaturalKeyDetail
}

func (e *duplicateNaturalKeyErr) Unwrap() error { return e.error }

// duplicateNaturalKeyDetailMaxFields and duplicateNaturalKeyDetailMaxFieldLen
// bound what DuplicateNaturalKeyDetailFrom ever returns. Every natural key
// this file's recordGitHubTestsKey call sites build has at most 5 fields, each
// a short id, hash, or UUID -- the bound is defensive against a future call
// site growing the key, so persisting this detail can never carry an
// unbounded payload into Postgres.
const (
	duplicateNaturalKeyDetailMaxFields   = 8
	duplicateNaturalKeyDetailMaxFieldLen = 128
)

// DuplicateNaturalKeyDetailFrom extracts the destination table and colliding
// natural-key fields from an error wrapping ErrDuplicateNaturalKey via
// duplicateNaturalKeyError, bounded so the result is always safe to persist.
// ok is false when err carries no such structured detail (a plain
// ErrDuplicateNaturalKey built elsewhere, or an unrelated error).
func DuplicateNaturalKeyDetailFrom(err error) (table string, fields []DuplicateNaturalKeyField, ok bool) {
	var typed *duplicateNaturalKeyErr
	if !errors.As(err, &typed) {
		return "", nil, false
	}
	table = typed.detail.Table
	if len(table) > duplicateNaturalKeyDetailMaxFieldLen {
		table = table[:duplicateNaturalKeyDetailMaxFieldLen]
	}
	limit := len(typed.detail.Fields)
	if limit > duplicateNaturalKeyDetailMaxFields {
		limit = duplicateNaturalKeyDetailMaxFields
	}
	fields = make([]DuplicateNaturalKeyField, limit)
	for index := 0; index < limit; index++ {
		field := typed.detail.Fields[index]
		if len(field.Value) > duplicateNaturalKeyDetailMaxFieldLen {
			field.Value = field.Value[:duplicateNaturalKeyDetailMaxFieldLen]
		}
		fields[index] = field
	}
	return table, fields, true
}

func gitHubTestsUint32(value int64) (uint32, error) {
	if value < 0 || value > math.MaxUint32 {
		return 0, ErrInvalidConfiguration
	}
	return uint32(value), nil
}

func gitHubTestsNullableUint32(value *int64) (*uint32, error) {
	if value == nil {
		return nil, nil
	}
	converted, err := gitHubTestsUint32(*value)
	if err != nil {
		return nil, err
	}
	return &converted, nil
}

func gitHubTestsCounts(values ...int64) ([7]uint32, error) {
	var result [7]uint32
	if len(values) != len(result) {
		return result, ErrInvalidConfiguration
	}
	for index, value := range values {
		converted, err := gitHubTestsUint32(value)
		if err != nil {
			return result, err
		}
		result[index] = converted
	}
	return result, nil
}

func sendGitHubTestsBatch(ctx context.Context, lease providerfoundation.LeaseGuard, batch driver.Batch) error {
	if err := lease.Assert(ctx); err != nil {
		return err
	}
	return batch.Send()
}

func validateGitHubTestsRow(claim Claim, orgID, repoID, runID string, lastSynced time.Time) error {
	if orgID == "" || orgID != claim.OrgID || repoID == "" || runID == "" || lastSynced.IsZero() {
		return providerfoundation.ErrInvalidScope
	}
	return nil
}

func validateTestOpsProvider(claim Claim, persisted string) error {
	want := ""
	switch claim.Provider {
	case "github":
		want = "github_actions"
	case "gitlab":
		want = "gitlab_ci"
	}
	if want == "" || persisted != want {
		return providerfoundation.ErrInvalidScope
	}
	return nil
}

func (sink TestOpsClickHouseEffects) InspectEffect(ctx context.Context, claim Claim, effect EffectBatch) (EffectInspection, error) {
	if err := sink.valid(ctx, claim, effect.Destination); err != nil {
		return EffectConflict, err
	}
	if sink.Conn == nil {
		return EffectConflict, ErrInvalidConfiguration
	}
	switch effect.Destination {
	case "ci_pipeline_runs":
		rows, err := decodeEffectRows[githubTestsPipelineRow](effect)
		if err != nil {
			return EffectConflict, err
		}
		return inspectGitHubTestsRows(rows, func(expected githubTestsPipelineRow) (githubTestsPipelineRow, bool, error) {
			actual := githubTestsPipelineRow{}
			if err := validateTestOpsProvider(claim, expected.Provider); err != nil {
				return actual, false, err
			}
			found, err := queryGitHubTestsRow(ctx, sink.Conn, `SELECT org_id,repo_id,run_id,pipeline_name,provider,status,queued_at,started_at,finished_at,duration_seconds,queue_seconds,retry_count,cancel_reason,trigger_source,commit_hash,branch,pr_number,team_id,service_id,last_synced FROM ci_pipeline_runs FINAL WHERE org_id=? AND repo_id=? AND run_id=?`, []any{expected.OrgID, expected.RepoID, expected.RunID}, &actual.OrgID, &actual.RepoID, &actual.RunID, &actual.PipelineName, &actual.Provider, &actual.Status, &actual.QueuedAt, &actual.StartedAt, &actual.FinishedAt, &actual.DurationSeconds, &actual.QueueSeconds, &actual.RetryCount, &actual.CancelReason, &actual.TriggerSource, &actual.CommitHash, &actual.Branch, &actual.PRNumber, &actual.TeamID, &actual.ServiceID, &actual.LastSynced)
			return actual, found, err
		})
	case "ci_job_runs":
		rows, err := decodeEffectRows[githubTestsJobRow](effect)
		if err != nil {
			return EffectConflict, err
		}
		return inspectGitHubTestsRows(rows, func(expected githubTestsJobRow) (githubTestsJobRow, bool, error) {
			actual := githubTestsJobRow{}
			found, err := queryGitHubTestsRow(ctx, sink.Conn, `SELECT org_id,repo_id,run_id,job_id,job_name,stage,status,started_at,finished_at,duration_seconds,runner_type,retry_attempt,last_synced FROM ci_job_runs FINAL WHERE org_id=? AND repo_id=? AND run_id=? AND job_id=?`, []any{expected.OrgID, expected.RepoID, expected.RunID, expected.JobID}, &actual.OrgID, &actual.RepoID, &actual.RunID, &actual.JobID, &actual.JobName, &actual.Stage, &actual.Status, &actual.StartedAt, &actual.FinishedAt, &actual.DurationSeconds, &actual.RunnerType, &actual.RetryAttempt, &actual.LastSynced)
			return actual, found, err
		})
	case "ci_acceptance_checks":
		rows, err := decodeEffectRows[githubTestsAcceptanceRow](effect)
		if err != nil {
			return EffectConflict, err
		}
		return inspectGitHubTestsRows(rows, func(expected githubTestsAcceptanceRow) (githubTestsAcceptanceRow, bool, error) {
			actual := githubTestsAcceptanceRow{}
			if err := validateTestOpsProvider(claim, expected.Provider); err != nil {
				return actual, false, err
			}
			found, err := queryGitHubTestsRow(ctx, sink.Conn, `SELECT org_id,repo_id,run_id,check_key,check_name,provider,requirement,result,rule_version,provenance,target_branch,pr_number,source_url,observed_at,last_synced FROM ci_acceptance_checks FINAL WHERE org_id=? AND repo_id=? AND run_id=? AND check_key=?`, []any{expected.OrgID, expected.RepoID, expected.RunID, expected.CheckKey}, &actual.OrgID, &actual.RepoID, &actual.RunID, &actual.CheckKey, &actual.CheckName, &actual.Provider, &actual.Requirement, &actual.Result, &actual.RuleVersion, &actual.Provenance, &actual.TargetBranch, &actual.PRNumber, &actual.SourceURL, &actual.ObservedAt, &actual.LastSynced)
			return actual, found, err
		})
	case "test_suite_results":
		rows, err := decodeEffectRows[testSuiteResultRow](effect)
		if err != nil {
			return EffectConflict, err
		}
		return inspectGitHubTestsRows(rows, func(expected testSuiteResultRow) (testSuiteResultRow, bool, error) {
			actual := testSuiteResultRow{}
			found, err := queryGitHubTestsRow(ctx, sink.Conn, `SELECT org_id,repo_id,run_id,suite_id,suite_name,framework,environment,toInt64(total_count),toInt64(passed_count),toInt64(failed_count),toInt64(skipped_count),toInt64(error_count),toInt64(quarantined_count),toInt64(retried_count),duration_seconds,started_at,finished_at,team_id,service_id,last_synced FROM test_suite_results FINAL WHERE org_id=? AND repo_id=? AND run_id=? AND suite_id=?`, []any{expected.OrgID, expected.RepoID, expected.RunID, expected.SuiteID}, &actual.OrgID, &actual.RepoID, &actual.RunID, &actual.SuiteID, &actual.SuiteName, &actual.Framework, &actual.Environment, &actual.TotalCount, &actual.PassedCount, &actual.FailedCount, &actual.SkippedCount, &actual.ErrorCount, &actual.QuarantinedCount, &actual.RetriedCount, &actual.DurationSeconds, &actual.StartedAt, &actual.FinishedAt, &actual.TeamID, &actual.ServiceID, &actual.LastSynced)
			return actual, found, err
		})
	case "test_case_results":
		rows, err := decodeEffectRows[testCaseResultRow](effect)
		if err != nil {
			return EffectConflict, err
		}
		return inspectGitHubTestsRows(rows, func(expected testCaseResultRow) (testCaseResultRow, bool, error) {
			actual := testCaseResultRow{}
			found, err := queryGitHubTestsRow(ctx, sink.Conn, `SELECT org_id,repo_id,run_id,suite_id,case_id,case_name,class_name,status,duration_seconds,toInt64(retry_attempt),failure_message,failure_type,stack_trace,is_quarantined != 0,last_synced FROM test_case_results FINAL WHERE org_id=? AND repo_id=? AND run_id=? AND suite_id=? AND case_id=?`, []any{expected.OrgID, expected.RepoID, expected.RunID, expected.SuiteID, expected.CaseID}, &actual.OrgID, &actual.RepoID, &actual.RunID, &actual.SuiteID, &actual.CaseID, &actual.CaseName, &actual.ClassName, &actual.Status, &actual.DurationSeconds, &actual.RetryAttempt, &actual.FailureMessage, &actual.FailureType, &actual.StackTrace, &actual.IsQuarantined, &actual.LastSynced)
			return actual, found, err
		})
	case "coverage_snapshots":
		rows, err := decodeEffectRows[coverageSnapshotRow](effect)
		if err != nil {
			return EffectConflict, err
		}
		return inspectGitHubTestsRows(rows, func(expected coverageSnapshotRow) (coverageSnapshotRow, bool, error) {
			actual := coverageSnapshotRow{}
			found, err := queryGitHubTestsRow(ctx, sink.Conn, `SELECT org_id,repo_id,run_id,snapshot_id,report_format,CAST(lines_total,'Nullable(Int64)'),CAST(lines_covered,'Nullable(Int64)'),line_coverage_pct,CAST(branches_total,'Nullable(Int64)'),CAST(branches_covered,'Nullable(Int64)'),branch_coverage_pct,CAST(functions_total,'Nullable(Int64)'),CAST(functions_covered,'Nullable(Int64)'),commit_hash,branch,CAST(pr_number,'Nullable(Int64)'),team_id,service_id,last_synced FROM coverage_snapshots FINAL WHERE org_id=? AND repo_id=? AND run_id=? AND snapshot_id=?`, []any{expected.OrgID, expected.RepoID, expected.RunID, expected.SnapshotID}, &actual.OrgID, &actual.RepoID, &actual.RunID, &actual.SnapshotID, &actual.ReportFormat, &actual.LinesTotal, &actual.LinesCovered, &actual.LineCoveragePct, &actual.BranchesTotal, &actual.BranchesCovered, &actual.BranchCoveragePct, &actual.FunctionsTotal, &actual.FunctionsCovered, &actual.CommitHash, &actual.Branch, &actual.PRNumber, &actual.TeamID, &actual.ServiceID, &actual.LastSynced)
			return actual, found, err
		})
	default:
		return EffectConflict, ErrInvalidConfiguration
	}
}

func queryGitHubTestsRow(ctx context.Context, conn driver.Conn, query string, args []any, dest ...any) (bool, error) {
	rows, err := conn.Query(ctx, query, args...)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	if !rows.Next() {
		return false, rows.Err()
	}
	if err := rows.Scan(dest...); err != nil {
		return false, err
	}
	if rows.Next() {
		return false, ErrInvalidConfiguration
	}
	return true, rows.Err()
}
func inspectGitHubTestsRows[T any](expected []T, load func(T) (T, bool, error)) (EffectInspection, error) {
	if len(expected) == 0 {
		return EffectAbsent, nil
	}
	exact, absent := 0, 0
	for _, want := range expected {
		actual, found, err := load(want)
		if err != nil {
			return EffectConflict, err
		}
		if !found {
			absent++
			continue
		}
		wantJSON, _ := json.Marshal(want)
		actualJSON, _ := json.Marshal(actual)
		if string(wantJSON) != string(actualJSON) {
			return EffectConflict, nil
		}
		exact++
	}
	if exact == len(expected) {
		return EffectExact, nil
	}
	if absent == len(expected) {
		return EffectAbsent, nil
	}
	return EffectConflict, nil
}

var _ EffectSink = TestOpsClickHouseEffects{}
var _ EffectReadback = TestOpsClickHouseEffects{}
