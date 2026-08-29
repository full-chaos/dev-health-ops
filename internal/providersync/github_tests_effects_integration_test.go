//go:build integration

package providersync

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/google/uuid"
)

func TestGitHubTestsIncompleteBatchCommitsOnceAndRemainsTenantScoped(t *testing.T) {
	ctx, sink := newGitHubTestsIntegrationSink(t)
	now := time.Date(2026, 8, 12, 12, 30, 0, 0, time.UTC)
	claimA := nativeTestClaim("github", "tests")
	claimB := claimA
	claimB.OrgID = "other-org"
	claimB.ID = uuid.NewString()
	claimB.SyncRunID = uuid.NewString()

	archive := githubTestsZip(t, map[string]string{
		"reports/good.xml":  githubTestsJUnitFixture,
		"reports/good.info": githubTestsLCOVFixture,
		"reports/bad.xml":   `<!DOCTYPE x [<!ENTITY x "boom">]><testsuite>&x;</testsuite>`,
	})
	collect := func(claim Claim) CompleteRouteBatch {
		t.Helper()
		doer := &githubTestsRouteDoer{t: t, archive: archive}
		batch, err := (GitHubTestsRouteHandler{}).Collect(
			ctx, claim, providerfoundation.Credential{}, githubTestsClient(t, doer), now,
		)
		if err != nil {
			t.Fatal(err)
		}
		comparison, err := (ProductionContractComparator{}).CompareCompleteRoute(ctx, claim, batch)
		if err != nil || !comparison.Match || comparison.NativeRecords != 7 {
			t.Fatalf("comparison=%+v error=%v", comparison, err)
		}
		if batch.Watermark != nil || batch.Result["reports_complete"] != false ||
			batch.Result["reports_skipped"] != 1 {
			t.Fatalf("incomplete batch was reported complete: %+v", batch)
		}
		return batch
	}

	batchA, batchB := collect(claimA), collect(claimB)
	if err := sink.WriteEffect(ctx, claimB, batchA.Effects[0]); !errors.Is(err, providerfoundation.ErrInvalidScope) {
		t.Fatalf("foreign tenant row error=%v", err)
	}
	for _, item := range []struct {
		claim Claim
		batch CompleteRouteBatch
	}{
		{claim: claimA, batch: batchA},
		{claim: claimB, batch: batchB},
	} {
		ledger := &memoryEffectLedger{}
		committer := EffectCommitter{
			Ledger: ledger, Sink: sink, Readback: sink,
			Now: func() time.Time { return now },
		}
		first, err := committer.Commit(ctx, item.claim, item.batch.Effects, now)
		if err != nil || first.Written != 6 {
			t.Fatalf("first commit=%+v error=%v", first, err)
		}
		second, err := committer.Commit(ctx, item.claim, item.batch.Effects, now)
		if err != nil || second.Written != 0 || second.Skipped != 6 {
			t.Fatalf("retry commit=%+v error=%v", second, err)
		}
		for _, effect := range item.batch.Effects {
			inspection, inspectErr := sink.InspectEffect(ctx, item.claim, effect)
			if inspectErr != nil || inspection != EffectExact {
				t.Fatalf("%s inspection=%s error=%v", effect.Destination, inspection, inspectErr)
			}
			var count uint64
			if err := sink.Conn.QueryRow(
				ctx, "SELECT count() FROM "+effect.Destination+" WHERE org_id = ?", item.claim.OrgID,
			).Scan(&count); err != nil {
				t.Fatal(err)
			}
			if count != uint64(len(effect.Rows)) {
				t.Fatalf("%s raw rows=%d want=%d", effect.Destination, count, len(effect.Rows))
			}
		}
	}
}

func TestGitHubTestsMultiRowEffectsAreAtomicAndRejectDuplicateNaturalKeys(t *testing.T) {
	ctx, sink := newGitHubTestsIntegrationSink(t)
	claim := nativeTestClaim("github", "tests")
	effects := githubTestsIntegrationEffects(t, claim, "passed", time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC))
	for _, effect := range effects {
		duplicate, err := BuildEffectBatch(effect.Destination, effect.Recovery, []json.RawMessage{effect.Rows[0], effect.Rows[0]})
		if err != nil {
			t.Fatal(err)
		}
		if err := sink.WriteEffect(ctx, claim, duplicate); !errors.Is(err, ErrInvalidConfiguration) {
			t.Fatalf("%s duplicate error=%v", effect.Destination, err)
		}
		if err := sink.Conn.Exec(ctx, "ALTER TABLE "+effect.Destination+" ADD CONSTRAINT reject_bad CHECK run_id != 'reject'"); err != nil {
			t.Fatal(err)
		}
		badRow := bytes.ReplaceAll(effect.Rows[0], []byte(`"run_id":"9001"`), []byte(`"run_id":"reject"`))
		bad, err := BuildEffectBatch(effect.Destination, effect.Recovery, []json.RawMessage{badRow})
		if err != nil {
			t.Fatal(err)
		}
		combined, err := BuildEffectBatch(effect.Destination, effect.Recovery, []json.RawMessage{effect.Rows[0], badRow})
		if err != nil {
			t.Fatal(err)
		}
		if err := sink.WriteEffect(ctx, claim, combined); err == nil {
			t.Fatalf("%s accepted server-rejected second row", effect.Destination)
		}
		for _, candidate := range []EffectBatch{effect, bad} {
			inspection, err := sink.InspectEffect(ctx, claim, candidate)
			if err != nil || inspection != EffectAbsent {
				t.Fatalf("%s partial batch inspection=%s err=%v", effect.Destination, inspection, err)
			}
		}
	}
}

func TestGitHubTestsJUnitMillisecondTimestampsReadBackExact(t *testing.T) {
	ctx, sink := newGitHubTestsIntegrationSink(t)
	claim := nativeTestClaim("github", "tests")
	normalizedAt := time.Date(2026, 8, 3, 12, 0, 0, 123000000, time.UTC)
	body := []byte(`<testsuite name="precision" timestamp="2026-08-03T11:00:00.123456789Z" time="0.0009"><testcase name="case"/></testsuite>`)
	suites, _, err := parseJUnitRows(body, "artifact-1", "c7198fbc-1945-3717-05d8-eb78866b4e79", "precision", claim.OrgID, nil, nil, normalizedAt, map[string]int{})
	if err != nil {
		t.Fatal(err)
	}
	if suites[0].StartedAt.Nanosecond()%int(time.Millisecond) != 0 || suites[0].FinishedAt.Nanosecond()%int(time.Millisecond) != 0 {
		t.Fatalf("timestamps not normalized before hashing: %+v", suites[0])
	}
	effect, err := effectBatchFromValues("test_suite_results", EffectReadbackRequired, suites)
	if err != nil {
		t.Fatal(err)
	}
	if err := sink.WriteEffect(ctx, claim, effect); err != nil {
		t.Fatal(err)
	}
	inspection, err := sink.InspectEffect(ctx, claim, effect)
	if err != nil || inspection != EffectExact {
		t.Fatalf("inspection=%s err=%v", inspection, err)
	}
}

func TestGitHubTestsReadbackSeparatesSameNaturalKeysAcrossTenantsForAllEffects(t *testing.T) {
	ctx, sink := newGitHubTestsIntegrationSink(t)
	now := time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC)
	claimA := nativeTestClaim("github", "tests")
	claimB := claimA
	claimB.OrgID = "other-org"
	effectsA := githubTestsIntegrationEffects(t, claimA, "passed", now)
	effectsB := githubTestsIntegrationEffects(t, claimB, "failed", now)
	for index, effectB := range effectsB {
		if err := sink.WriteEffect(ctx, claimB, effectB); err != nil {
			t.Fatalf("write foreign %s: %v", effectB.Destination, err)
		}
		foreign, err := sink.InspectEffect(ctx, claimB, effectB)
		if err != nil || foreign != EffectExact {
			t.Fatalf("foreign %s inspection=%s err=%v", effectB.Destination, foreign, err)
		}
		absent, err := sink.InspectEffect(ctx, claimA, effectsA[index])
		if err != nil || absent != EffectAbsent {
			t.Fatalf("own-before-write %s inspection=%s err=%v", effectB.Destination, absent, err)
		}
	}
	for index, effectA := range effectsA {
		if err := sink.WriteEffect(ctx, claimA, effectA); err != nil {
			t.Fatalf("write own %s: %v", effectA.Destination, err)
		}
		own, err := sink.InspectEffect(ctx, claimA, effectA)
		if err != nil || own != EffectExact {
			t.Fatalf("own %s inspection=%s err=%v", effectA.Destination, own, err)
		}
		foreign, err := sink.InspectEffect(ctx, claimB, effectsB[index])
		if err != nil || foreign != EffectExact {
			t.Fatalf("foreign-after-own %s inspection=%s err=%v", effectA.Destination, foreign, err)
		}
	}
}

func githubTestsIntegrationEffects(t *testing.T, claim Claim, result string, now time.Time) []EffectBatch {
	t.Helper()
	repo := "c7198fbc-1945-3717-05d8-eb78866b4e79"
	status := result
	format := "lcov"
	total, covered := int64(2), int64(1)
	pct := 50.0
	values := []struct {
		name string
		rows any
	}{
		{"ci_pipeline_runs", []githubTestsPipelineRow{{OrgID: claim.OrgID, RepoID: repo, RunID: "9001", Provider: "github_actions", Status: &status, StartedAt: now.Add(-time.Minute), LastSynced: now}}},
		{"ci_job_runs", []githubTestsJobRow{{OrgID: claim.OrgID, RepoID: repo, RunID: "9001", JobID: "11", JobName: "unit", Status: &status, LastSynced: now}}},
		{"ci_acceptance_checks", []githubTestsAcceptanceRow{{OrgID: claim.OrgID, RepoID: repo, RunID: "9001", CheckKey: "github_actions:key", CheckName: "unit", Provider: "github_actions", Requirement: "required", Result: result, RuleVersion: githubTestsRuleVersion, Provenance: "fixture", ObservedAt: now, LastSynced: now}}},
		{"test_suite_results", []testSuiteResultRow{{OrgID: claim.OrgID, RepoID: repo, RunID: "9001", SuiteID: "suite", SuiteName: "suite", TotalCount: 1, PassedCount: 1, LastSynced: now}}},
		{"test_case_results", []testCaseResultRow{{OrgID: claim.OrgID, RepoID: repo, RunID: "9001", SuiteID: "suite", CaseID: "case", CaseName: "case", Status: result, LastSynced: now}}},
		{"coverage_snapshots", []coverageSnapshotRow{{OrgID: claim.OrgID, RepoID: repo, RunID: "9001", SnapshotID: "snapshot", ReportFormat: &format, LinesTotal: &total, LinesCovered: &covered, LineCoveragePct: &pct, LastSynced: now}}},
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

func newGitHubTestsIntegrationSink(t *testing.T) (context.Context, GitHubTestsClickHouseEffects) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate ClickHouse: %v", err)
		}
	})
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	ddls := []string{
		`CREATE TABLE ci_pipeline_runs (org_id String,repo_id UUID,run_id String,pipeline_name Nullable(String),provider String,status Nullable(String),queued_at Nullable(DateTime64(3,'UTC')),started_at DateTime64(3,'UTC'),finished_at Nullable(DateTime64(3,'UTC')),duration_seconds Nullable(Float64),queue_seconds Nullable(Float64),retry_count UInt32,cancel_reason Nullable(String),trigger_source Nullable(String),commit_hash Nullable(String),branch Nullable(String),pr_number Nullable(UInt32),team_id Nullable(String),service_id Nullable(String),last_synced DateTime64(3,'UTC')) ENGINE=ReplacingMergeTree(last_synced) ORDER BY (org_id,repo_id,run_id)`,
		`CREATE TABLE ci_job_runs (org_id String,repo_id UUID,run_id String,job_id String,job_name String,stage Nullable(String),status Nullable(String),started_at Nullable(DateTime64(3,'UTC')),finished_at Nullable(DateTime64(3,'UTC')),duration_seconds Nullable(Float64),runner_type Nullable(String),retry_attempt UInt32,last_synced DateTime64(3,'UTC')) ENGINE=ReplacingMergeTree(last_synced) ORDER BY (org_id,repo_id,run_id,job_id)`,
		`CREATE TABLE ci_acceptance_checks (org_id String,repo_id UUID,run_id String,check_key String,check_name String,provider String,requirement String,result String,rule_version String,provenance String,target_branch Nullable(String),pr_number Nullable(UInt32),source_url Nullable(String),observed_at DateTime64(3,'UTC'),last_synced DateTime64(3,'UTC')) ENGINE=ReplacingMergeTree(last_synced) ORDER BY (org_id,repo_id,run_id,check_key)`,
		`CREATE TABLE test_suite_results (org_id String,repo_id UUID,run_id String,suite_id String,suite_name String,framework Nullable(String),environment Nullable(String),total_count UInt32,passed_count UInt32,failed_count UInt32,skipped_count UInt32,error_count UInt32,quarantined_count UInt32,retried_count UInt32,duration_seconds Nullable(Float64),started_at Nullable(DateTime64(3,'UTC')),finished_at Nullable(DateTime64(3,'UTC')),team_id Nullable(String),service_id Nullable(String),last_synced DateTime64(3,'UTC')) ENGINE=ReplacingMergeTree(last_synced) ORDER BY (org_id,repo_id,run_id,suite_id)`,
		`CREATE TABLE test_case_results (org_id String,repo_id UUID,run_id String,suite_id String,case_id String,case_name String,class_name Nullable(String),status String,duration_seconds Nullable(Float64),retry_attempt UInt32,failure_message Nullable(String),failure_type Nullable(String),stack_trace Nullable(String),is_quarantined UInt8,last_synced DateTime64(3,'UTC')) ENGINE=ReplacingMergeTree(last_synced) ORDER BY (org_id,repo_id,run_id,suite_id,case_id)`,
		`CREATE TABLE coverage_snapshots (org_id String,repo_id UUID,run_id String,snapshot_id String,report_format Nullable(String),lines_total Nullable(UInt32),lines_covered Nullable(UInt32),line_coverage_pct Nullable(Float64),branches_total Nullable(UInt32),branches_covered Nullable(UInt32),branch_coverage_pct Nullable(Float64),functions_total Nullable(UInt32),functions_covered Nullable(UInt32),commit_hash Nullable(String),branch Nullable(String),pr_number Nullable(UInt32),team_id Nullable(String),service_id Nullable(String),last_synced DateTime64(3,'UTC')) ENGINE=ReplacingMergeTree(last_synced) ORDER BY (org_id,repo_id,run_id,snapshot_id)`,
	}
	for _, ddl := range ddls {
		if err := conn.Exec(ctx, ddl); err != nil {
			t.Fatal(err)
		}
	}
	return ctx, GitHubTestsClickHouseEffects{Conn: conn, Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })}
}
