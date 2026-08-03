//go:build integration

package providersync

import (
	"errors"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

func TestGitLabTestOpsAliasesShareCompleteReplacingRowOwnerInBothWriteOrders(t *testing.T) {
	ctx, sink := newGitHubTestsIntegrationSink(t)
	base := time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC)
	for index, order := range [][2]string{{"cicd", "tests"}, {"tests", "cicd"}} {
		olderClaim := nativeTestClaim("gitlab", order[0])
		newerClaim := nativeTestClaim("gitlab", order[1])
		runID := "alias-order-" + order[0] + "-then-" + order[1]
		older := completeGitLabPipelineRow(olderClaim, runID, base.Add(time.Duration(index)*time.Minute), false)
		newer := completeGitLabPipelineRow(newerClaim, runID, base.Add(time.Duration(index)*time.Minute+time.Second), true)
		olderEffect, err := effectBatchFromValues("ci_pipeline_runs", EffectReadbackRequired, []githubTestsPipelineRow{older})
		if err != nil {
			t.Fatal(err)
		}
		newerEffect, err := effectBatchFromValues("ci_pipeline_runs", EffectReadbackRequired, []githubTestsPipelineRow{newer})
		if err != nil {
			t.Fatal(err)
		}
		if err := sink.WriteEffect(ctx, olderClaim, olderEffect); err != nil {
			t.Fatalf("write %s older: %v", runID, err)
		}
		if err := sink.WriteEffect(ctx, newerClaim, newerEffect); err != nil {
			t.Fatalf("write %s newer: %v", runID, err)
		}
		inspection, err := sink.InspectEffect(ctx, newerClaim, newerEffect)
		if err != nil || inspection != EffectExact {
			t.Fatalf("%s newer full-row inspection=%s err=%v", runID, inspection, err)
		}
		inspection, err = sink.InspectEffect(ctx, olderClaim, olderEffect)
		if err != nil || inspection != EffectConflict {
			t.Fatalf("%s older row inspection=%s err=%v", runID, inspection, err)
		}
		var rows, nullNames uint64
		if err := sink.Conn.QueryRow(ctx, `SELECT count(),countIf(isNull(pipeline_name)) FROM ci_pipeline_runs FINAL WHERE org_id=? AND repo_id=? AND run_id=?`, newer.OrgID, newer.RepoID, newer.RunID).Scan(&rows, &nullNames); err != nil {
			t.Fatal(err)
		}
		if rows != 1 || nullNames != 1 {
			t.Fatalf("%s FINAL rows=%d NULL pipeline names=%d", runID, rows, nullNames)
		}
	}
}

func TestGitLabTestOpsSixDestinationsHaveExactRetryReadback(t *testing.T) {
	ctx, sink := newGitHubTestsIntegrationSink(t)
	claim := nativeTestClaim("gitlab", "tests")
	effects := gitLabTestsIntegrationEffects(t, claim, time.Date(2026, 8, 3, 13, 0, 0, 0, time.UTC))
	if len(effects) != 6 {
		t.Fatalf("effects=%d", len(effects))
	}
	for _, effect := range effects {
		for attempt := 1; attempt <= 2; attempt++ {
			if err := sink.WriteEffect(ctx, claim, effect); err != nil {
				t.Fatalf("%s attempt %d: %v", effect.Destination, attempt, err)
			}
			inspection, err := sink.InspectEffect(ctx, claim, effect)
			if err != nil || inspection != EffectExact {
				t.Fatalf("%s attempt %d inspection=%s err=%v", effect.Destination, attempt, inspection, err)
			}
		}
	}
}

func TestTestOpsSinkRejectsCrossProviderPipelineAndAcceptanceRows(t *testing.T) {
	ctx, sink := newGitHubTestsIntegrationSink(t)
	now := time.Date(2026, 8, 3, 14, 0, 0, 0, time.UTC)
	for _, test := range []struct {
		name     string
		claim    Claim
		provider string
		checkKey string
	}{
		{name: "gitlab claim github row", claim: nativeTestClaim("gitlab", "tests"), provider: "github_actions", checkKey: "github_actions:key"},
		{name: "github claim gitlab row", claim: nativeTestClaim("github", "tests"), provider: "gitlab_ci", checkKey: "gitlab_ci:key"},
	} {
		t.Run(test.name, func(t *testing.T) {
			pipeline := completeGitLabPipelineRow(test.claim, "cross-provider", now, false)
			pipeline.Provider = test.provider
			pipelineEffect, err := effectBatchFromValues("ci_pipeline_runs", EffectReadbackRequired, []githubTestsPipelineRow{pipeline})
			if err != nil {
				t.Fatal(err)
			}
			acceptanceEffect, err := effectBatchFromValues("ci_acceptance_checks", EffectReadbackRequired, []githubTestsAcceptanceRow{{
				OrgID: test.claim.OrgID, RepoID: pipeline.RepoID, RunID: pipeline.RunID,
				CheckKey: test.checkKey, CheckName: "unit", Provider: test.provider,
				Requirement: "required", Result: "passed", RuleVersion: githubTestsRuleVersion,
				Provenance: "fixture", ObservedAt: now, LastSynced: now,
			}})
			if err != nil {
				t.Fatal(err)
			}
			for _, effect := range []EffectBatch{pipelineEffect, acceptanceEffect} {
				if err := sink.WriteEffect(ctx, test.claim, effect); !errors.Is(err, providerfoundation.ErrInvalidScope) {
					t.Fatalf("%s write error=%v", effect.Destination, err)
				}
				if inspection, err := sink.InspectEffect(ctx, test.claim, effect); !errors.Is(err, providerfoundation.ErrInvalidScope) || inspection != EffectConflict {
					t.Fatalf("%s inspection=%s error=%v", effect.Destination, inspection, err)
				}
			}
		})
	}
}

func completeGitLabPipelineRow(claim Claim, runID string, lastSynced time.Time, nullPipelineName bool) githubTestsPipelineRow {
	pipelineName := testsOptionalString("GitLab CI")
	if nullPipelineName {
		pipelineName = nil
	}
	status, cancelReason := testsOptionalString("failure"), testsOptionalString("superseded")
	trigger, commit, branch := testsOptionalString("push"), testsOptionalString("abc123"), testsOptionalString("main")
	team, service := testsOptionalString("team-a"), testsOptionalString("service-a")
	queued := lastSynced.Add(-3 * time.Minute)
	started, finished := lastSynced.Add(-2*time.Minute), lastSynced.Add(-time.Minute)
	duration, queue := 60.0, 60.0
	pr := uint32(42)
	return githubTestsPipelineRow{
		OrgID: claim.OrgID, RepoID: "c7198fbc-1945-3717-05d8-eb78866b4e79", RunID: runID,
		PipelineName: pipelineName, Provider: "gitlab_ci", Status: status,
		QueuedAt: &queued, StartedAt: started, FinishedAt: &finished,
		DurationSeconds: &duration, QueueSeconds: &queue, RetryCount: 2,
		CancelReason: cancelReason, TriggerSource: trigger, CommitHash: commit,
		Branch: branch, PRNumber: &pr, TeamID: team, ServiceID: service, LastSynced: lastSynced,
	}
}

func gitLabTestsIntegrationEffects(t *testing.T, claim Claim, now time.Time) []EffectBatch {
	t.Helper()
	pipeline := completeGitLabPipelineRow(claim, "9001", now, false)
	status, stage, runner := testsOptionalString("passed"), testsOptionalString("test"), testsOptionalString("hosted")
	framework, environment := testsOptionalString("gitlab_ci"), testsOptionalString("ci")
	className, failure, failureType, stack := testsOptionalString("tests.TestAPI"), testsOptionalString("boom"), testsOptionalString("assertion"), testsOptionalString("trace")
	format, commit, branch := testsOptionalString("lcov"), testsOptionalString("abc123"), testsOptionalString("main")
	team, service := testsOptionalString("team-a"), testsOptionalString("service-a")
	total, covered, branchTotal, branchCovered, functionTotal, functionCovered := int64(2), int64(1), int64(4), int64(3), int64(6), int64(5)
	linePct, branchPct := 50.0, 75.0
	pr := uint32(42)
	coveragePR := int64(pr)
	values := []struct {
		name string
		rows any
	}{
		{"ci_pipeline_runs", []githubTestsPipelineRow{pipeline}},
		{"ci_job_runs", []githubTestsJobRow{{OrgID: claim.OrgID, RepoID: pipeline.RepoID, RunID: pipeline.RunID, JobID: "11", JobName: "unit", Stage: stage, Status: status, StartedAt: &pipeline.StartedAt, FinishedAt: pipeline.FinishedAt, DurationSeconds: pipeline.DurationSeconds, RunnerType: runner, RetryAttempt: 1, LastSynced: now}}},
		{"ci_acceptance_checks", []githubTestsAcceptanceRow{{OrgID: claim.OrgID, RepoID: pipeline.RepoID, RunID: pipeline.RunID, CheckKey: "gitlab_ci:key", CheckName: "unit", Provider: "gitlab_ci", Requirement: "required", Result: "passed", RuleVersion: githubTestsRuleVersion, Provenance: "fixture", TargetBranch: branch, PRNumber: &pr, SourceURL: testsOptionalString("https://gitlab.test/pipelines/9001"), ObservedAt: now, LastSynced: now}}},
		{"test_suite_results", []testSuiteResultRow{{OrgID: claim.OrgID, RepoID: pipeline.RepoID, RunID: pipeline.RunID, SuiteID: "suite", SuiteName: "suite", Framework: framework, Environment: environment, TotalCount: 1, PassedCount: 1, DurationSeconds: pipeline.DurationSeconds, StartedAt: &pipeline.StartedAt, FinishedAt: pipeline.FinishedAt, TeamID: team, ServiceID: service, LastSynced: now}}},
		{"test_case_results", []testCaseResultRow{{OrgID: claim.OrgID, RepoID: pipeline.RepoID, RunID: pipeline.RunID, SuiteID: "suite", CaseID: "case", CaseName: "case", ClassName: className, Status: "failed", DurationSeconds: pipeline.DurationSeconds, RetryAttempt: 1, FailureMessage: failure, FailureType: failureType, StackTrace: stack, LastSynced: now}}},
		{"coverage_snapshots", []coverageSnapshotRow{{OrgID: claim.OrgID, RepoID: pipeline.RepoID, RunID: pipeline.RunID, SnapshotID: "snapshot", ReportFormat: format, LinesTotal: &total, LinesCovered: &covered, LineCoveragePct: &linePct, BranchesTotal: &branchTotal, BranchesCovered: &branchCovered, BranchCoveragePct: &branchPct, FunctionsTotal: &functionTotal, FunctionsCovered: &functionCovered, CommitHash: commit, Branch: branch, PRNumber: &coveragePR, TeamID: team, ServiceID: service, LastSynced: now}}},
	}
	effects := make([]EffectBatch, 0, len(values))
	for _, value := range values {
		var effect EffectBatch
		var err error
		switch rows := value.rows.(type) {
		case []githubTestsPipelineRow:
			effect, err = effectBatchFromValues(value.name, EffectReadbackRequired, rows)
		case []githubTestsJobRow:
			effect, err = effectBatchFromValues(value.name, EffectReadbackRequired, rows)
		case []githubTestsAcceptanceRow:
			effect, err = effectBatchFromValues(value.name, EffectReadbackRequired, rows)
		case []testSuiteResultRow:
			effect, err = effectBatchFromValues(value.name, EffectReadbackRequired, rows)
		case []testCaseResultRow:
			effect, err = effectBatchFromValues(value.name, EffectReadbackRequired, rows)
		case []coverageSnapshotRow:
			effect, err = effectBatchFromValues(value.name, EffectReadbackRequired, rows)
		}
		if err != nil {
			t.Fatal(err)
		}
		effects = append(effects, effect)
	}
	return effects
}
