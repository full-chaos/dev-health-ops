package providersync

import (
	"context"
	"encoding/json"
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
				return ErrInvalidConfiguration
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
				return ErrInvalidConfiguration
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
				return ErrInvalidConfiguration
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
				return ErrInvalidConfiguration
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
				return ErrInvalidConfiguration
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
				return ErrInvalidConfiguration
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
