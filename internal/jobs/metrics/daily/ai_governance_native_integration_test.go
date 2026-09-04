//go:build integration

package daily

import (
	"context"
	"testing"
	"time"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// TestAIGovernanceComputeFamilyAgainstRealClickHouse is the ai_governance
// port's live-driver proof (CHAOS-4285). It runs the real production entry
// point, AIGovernanceExecutor.ComputeFamily, against a real ClickHouse with
// the production schema -- never a fake scanner.
//
// It proves four things a unit test structurally cannot:
//
//  1. DRIVER TYPE COMPATIBILITY. confidence is Float32 on the wire, repo_id is
//     Nullable(UUID), team_id/tool_name are Nullable(String), org_id on
//     ai_attribution is a UUID (not a String). A fake scanner accepts whatever
//     Go type the reader asks for; only a real driver rejects a wrong one.
//     This is the exact class of defect the CHAOS-4977 live-CH differential
//     found (a Map(String,Float64) column scanned as *string, invisible to
//     every existing unit test).
//
//  2. FINDING A -- the git_pull_requests fan-out does NOT happen here. The
//     fixture seeds TWO un-merged rows for the same (repo_id, number) with
//     different last_synced and DIFFERENT reviews_count. Python's query, which
//     joins that table with neither FINAL nor any dedup, would emit the
//     artifact twice and count it twice. This loader's argMax dedup must emit
//     it once, with the LATER last_synced's reviews_count winning.
//
//  3. Q1 -- IDEMPOTENCY. ComputeFamily is run TWICE, exactly as it is in
//     production when an org has more than one repo partition (the family is
//     org-scoped and ignores the partition's repo scope). With the
//     deterministic event_id, the second run re-derives the SAME ids, so the
//     ReplacingMergeTree collapses them and the DISTINCT event_id count stays
//     flat. Under Python's uuid4() it would double. This is the assertion that
//     makes the whole Q1 decision testable rather than merely argued.
//
//  4. The argMax replacements for Python's FINAL subqueries agree with the
//     answer FINAL would give, including the org_id-after-dedup ordering.
func TestAIGovernanceComputeFamilyAgainstRealClickHouse(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	clickhouseInstance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer clickhouseInstance.Close(context.Background())
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(clickhouseInstance.URI))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	for _, statement := range []string{
		// Production shape: migration 035 + 044's repo_id-in-ORDER-BY rebuild.
		`CREATE TABLE ai_attribution (
    record_id UUID, org_id UUID, provider LowCardinality(String),
    subject_type LowCardinality(String), subject_id String, repo_id Nullable(UUID),
    kind LowCardinality(String), source LowCardinality(String), confidence Float32,
    actor Nullable(String), evidence String,
    observed_at DateTime64(3, 'UTC'), ingested_at DateTime64(3, 'UTC'),
    superseded_by Nullable(UUID), computed_at DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (org_id, provider, subject_type, repo_id, subject_id, source)
SETTINGS allow_nullable_key = 1`,
		// Migration 043's live view definition, verbatim.
		`CREATE VIEW ai_attribution_resolved AS
SELECT record_id, org_id, provider, subject_type, subject_id, repo_id, kind,
       source, confidence, actor, evidence, observed_at, ingested_at,
       superseded_by, computed_at
FROM (
    SELECT *, multiIf(
        source = 'manual', 1, source = 'pr_label', 2, source = 'bot_author', 3,
        source = 'commit_trailer', 4, source = 'ci_annotation', 5,
        source = 'branch_name', 6, source = 'pr_body', 7, 8) AS _source_priority
    FROM ai_attribution FINAL WHERE superseded_by IS NULL
)
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY org_id, subject_type, repo_id, subject_id
    ORDER BY _source_priority ASC, confidence DESC) = 1`,
		`CREATE TABLE git_pull_requests (
    repo_id UUID, number UInt32, reviews_count UInt32 DEFAULT 0,
    last_synced DateTime64(3, 'UTC'), org_id String
) ENGINE = ReplacingMergeTree(last_synced) ORDER BY (repo_id, number)`,
		`CREATE TABLE ci_pipeline_runs (
    repo_id UUID, run_id String, status Nullable(String),
    started_at DateTime64(3, 'UTC'), last_synced DateTime64(3, 'UTC'), org_id String
) ENGINE = ReplacingMergeTree(last_synced) ORDER BY (repo_id, run_id)`,
		`CREATE TABLE security_alerts (
    repo_id UUID, alert_id String, source String,
    created_at DateTime64(3, 'UTC'), last_synced DateTime64(3, 'UTC'), org_id String
) ENGINE = ReplacingMergeTree(last_synced) ORDER BY (repo_id, alert_id)`,
		`CREATE TABLE ai_tool_allowlist (
    org_id String, tool_name String, model_name Nullable(String),
    status LowCardinality(String), reason Nullable(String),
    updated_at DateTime64(3, 'UTC'), computed_at DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(computed_at) ORDER BY (org_id, tool_name, ifNull(model_name, ''))`,
		// Migration 038, verbatim.
		`CREATE TABLE ai_policy_events (
    event_id UUID, org_id String, team_id Nullable(String), repo_id Nullable(UUID),
    rule_id LowCardinality(String), severity LowCardinality(String),
    subject_type LowCardinality(String), subject_id String,
    observed_at DateTime64(3, 'UTC'), evidence String,
    computed_at DateTime64(3, 'UTC') DEFAULT now64()
) ENGINE = ReplacingMergeTree(computed_at) PARTITION BY toYYYYMM(observed_at)
ORDER BY (org_id, ifNull(team_id, ''), ifNull(repo_id, toUUID('00000000-0000-0000-0000-000000000000')), rule_id, subject_type, subject_id, observed_at, event_id)`,
		`CREATE TABLE ai_governance_coverage_daily (
    org_id String, team_id Nullable(String), repo_id Nullable(UUID), day Date,
    ai_artifacts UInt64, declared_artifacts UInt64, human_reviewed_prs UInt64,
    security_scanned_prs UInt64, in_policy_artifacts UInt64,
    computed_at DateTime64(3, 'UTC') DEFAULT now64()
) ENGINE = ReplacingMergeTree(computed_at) PARTITION BY toYYYYMM(day)
ORDER BY (org_id, ifNull(team_id, ''), ifNull(repo_id, toUUID('00000000-0000-0000-0000-000000000000')), day)`,
	} {
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatalf("schema: %v\nstatement: %s", err, statement)
		}
	}

	const (
		orgID  = "00000000-0000-4000-8000-0000000000a0"
		repoID = "00000000-0000-4000-8000-0000000000a1"
	)

	// Three artifacts on the target day:
	//   PR 1  -- declared via pr_label, reviewed, scanned, allowlisted -> clean.
	//   PR 2  -- declared, but the repo has a dependabot finding      -> 1 violation.
	//   PR 3  -- source pr_body (NOT a declaring source)              -> 1 violation.
	if err := conn.Exec(ctx, `
INSERT INTO ai_attribution (record_id, org_id, provider, subject_type, subject_id, repo_id,
    kind, source, confidence, actor, evidence, observed_at, ingested_at, superseded_by, computed_at) VALUES
(generateUUIDv4(), toUUID('`+orgID+`'), 'github', 'pull_request', '1', toUUID('`+repoID+`'),
 'ai_assisted', 'pr_label', 0.95, NULL, '{"tool_name":"copilot","model_name":"gpt-4o"}',
 toDateTime64('2026-09-03 12:00:00', 3, 'UTC'), now64(3), NULL, now64(3)),
(generateUUIDv4(), toUUID('`+orgID+`'), 'github', 'pull_request', '2', toUUID('`+repoID+`'),
 'ai_assisted', 'pr_label', 0.95, NULL, '{"tool_name":"copilot","model_name":"gpt-4o"}',
 toDateTime64('2026-09-03 13:00:00', 3, 'UTC'), now64(3), NULL, now64(3)),
(generateUUIDv4(), toUUID('`+orgID+`'), 'github', 'pull_request', '3', toUUID('`+repoID+`'),
 'ai_assisted', 'pr_body', 0.25, NULL, '{"tool_name":"copilot","model_name":"gpt-4o"}',
 toDateTime64('2026-09-03 14:00:00', 3, 'UTC'), now64(3), NULL, now64(3))`); err != nil {
		t.Fatal(err)
	}

	// FINDING A FIXTURE: PR 1 has TWO un-merged rows with different
	// last_synced AND different reviews_count. Python's undeduped join would
	// emit the artifact twice; this loader must emit it once, taking the
	// later last_synced's reviews_count (2, so human_reviewed is true).
	// PRs 2 and 3 each get a single row.
	if err := conn.Exec(ctx, `
INSERT INTO git_pull_requests (repo_id, number, reviews_count, last_synced, org_id) VALUES
(toUUID('`+repoID+`'), 1, 0, toDateTime64('2026-09-03 10:00:00', 3, 'UTC'), '`+orgID+`'),
(toUUID('`+repoID+`'), 1, 2, toDateTime64('2026-09-03 11:00:00', 3, 'UTC'), '`+orgID+`'),
(toUUID('`+repoID+`'), 2, 3, toDateTime64('2026-09-03 11:00:00', 3, 'UTC'), '`+orgID+`'),
(toUUID('`+repoID+`'), 3, 1, toDateTime64('2026-09-03 11:00:00', 3, 'UTC'), '`+orgID+`')`); err != nil {
		t.Fatal(err)
	}
	// A successful pipeline run makes security_scanned true for the repo.
	// Duplicated on the dedup key so the argMax replacement for Python's FINAL
	// is exercised rather than assumed.
	if err := conn.Exec(ctx, `
INSERT INTO ci_pipeline_runs (repo_id, run_id, status, started_at, last_synced, org_id) VALUES
(toUUID('`+repoID+`'), 'run-1', 'pending', toDateTime64('2026-09-03 09:00:00', 3, 'UTC'),
 toDateTime64('2026-09-03 09:00:00', 3, 'UTC'), '`+orgID+`'),
(toUUID('`+repoID+`'), 'run-1', 'success', toDateTime64('2026-09-03 09:00:00', 3, 'UTC'),
 toDateTime64('2026-09-03 09:30:00', 3, 'UTC'), '`+orgID+`')`); err != nil {
		t.Fatal(err)
	}
	if err := conn.Exec(ctx, `
INSERT INTO security_alerts (repo_id, alert_id, source, created_at, last_synced, org_id) VALUES
(toUUID('`+repoID+`'), 'alert-1', 'dependabot', toDateTime64('2026-09-01 00:00:00', 3, 'UTC'),
 toDateTime64('2026-09-01 00:00:00', 3, 'UTC'), '`+orgID+`')`); err != nil {
		t.Fatal(err)
	}
	// Exact (tool+model) row beats the wildcard row -- CHAOS-2209 precedence.
	if err := conn.Exec(ctx, `
INSERT INTO ai_tool_allowlist (org_id, tool_name, model_name, status, reason, updated_at, computed_at) VALUES
('`+orgID+`', 'copilot', 'gpt-4o', 'allowed', NULL, now64(3), now64(3)),
('`+orgID+`', 'copilot', NULL, 'disallowed', NULL, now64(3), now64(3))`); err != nil {
		t.Fatal(err)
	}

	executor, err := NewAIGovernanceExecutor(conn)
	if err != nil {
		t.Fatal(err)
	}
	run := Run{OrganizationID: orgID, TargetDay: time.Date(2026, 9, 3, 0, 0, 0, 0, time.UTC)}
	partition := Partition{ID: "p1", RepoIDs: []RepositoryID{RepositoryID(repoID)}}

	firstWritten, err := executor.ComputeFamily(ctx, run, partition)
	if err != nil {
		t.Fatalf("first ComputeFamily: %v", err)
	}
	if firstWritten == 0 {
		t.Fatal("first run wrote zero rows -- the family computed nothing at all")
	}

	// (2) Finding A: exactly ONE coverage row, counting THREE artifacts. A
	// fan-out on PR 1's duplicate would make ai_artifacts 4, not 3.
	var aiArtifacts, declared, humanReviewed, securityScanned, inPolicy uint64
	if err := conn.QueryRow(ctx, `
SELECT ai_artifacts, declared_artifacts, human_reviewed_prs, security_scanned_prs, in_policy_artifacts
FROM ai_governance_coverage_daily FINAL WHERE org_id = ?`, orgID).
		Scan(&aiArtifacts, &declared, &humanReviewed, &securityScanned, &inPolicy); err != nil {
		t.Fatalf("read coverage: %v", err)
	}
	if aiArtifacts != 3 {
		t.Fatalf("ai_artifacts = %d, want 3 -- 4 means PR 1's duplicate git_pull_requests row fanned out "+
			"(the argMax dedup regressed to Python's undeduped join)", aiArtifacts)
	}
	if declared != 2 {
		t.Fatalf("declared_artifacts = %d, want 2 (pr_label declares, pr_body does not)", declared)
	}
	// PR 1's LATER row has reviews_count=2, so human_reviewed is true for it;
	// PRs 2 and 3 also have non-zero reviews. If argMax picked the EARLIER row
	// (reviews_count=0) this would be 2, not 3.
	if humanReviewed != 3 {
		t.Fatalf("human_reviewed_prs = %d, want 3 -- 2 means argMax took the EARLIER last_synced row "+
			"for PR 1 (reviews_count=0) instead of the later one", humanReviewed)
	}
	if securityScanned != 3 {
		t.Fatalf("security_scanned_prs = %d, want 3 -- the argMax replacement for Python's "+
			"`ci_pipeline_runs FINAL` must see run-1's later 'success' status", securityScanned)
	}
	// Every artifact carries the dependabot finding, so none is in policy.
	if inPolicy != 0 {
		t.Fatalf("in_policy_artifacts = %d, want 0 (the repo has a dependabot finding)", inPolicy)
	}

	var distinctEvents, totalEvents uint64
	if err := conn.QueryRow(ctx,
		`SELECT uniqExact(event_id), count() FROM ai_policy_events FINAL WHERE org_id = ?`, orgID).
		Scan(&distinctEvents, &totalEvents); err != nil {
		t.Fatalf("read policy events: %v", err)
	}
	if distinctEvents == 0 {
		t.Fatal("no policy events written")
	}

	// (3) Q1 idempotency: a SECOND ComputeFamily for the same org/day, which
	// is what production does for every additional repo partition. The
	// deterministic event_id must re-derive identically so the
	// ReplacingMergeTree collapses the rewrite.
	if _, err := executor.ComputeFamily(ctx, run, partition); err != nil {
		t.Fatalf("second ComputeFamily: %v", err)
	}
	var distinctAfter, totalAfter uint64
	if err := conn.QueryRow(ctx,
		`SELECT uniqExact(event_id), count() FROM ai_policy_events FINAL WHERE org_id = ?`, orgID).
		Scan(&distinctAfter, &totalAfter); err != nil {
		t.Fatalf("re-read policy events: %v", err)
	}
	if distinctAfter != distinctEvents {
		t.Fatalf("distinct event_id went %d -> %d across two runs of the SAME org/day. "+
			"The event_id derivation is not stable, so ai_policy_events can never dedup "+
			"-- this is exactly the uuid4() defect the port exists to fix.",
			distinctEvents, distinctAfter)
	}
	if totalAfter != totalEvents {
		t.Fatalf("ai_policy_events row count went %d -> %d after re-running the same partition; "+
			"FINAL should collapse the rewrite onto the identical ORDER BY key", totalEvents, totalAfter)
	}

	var coverageRows uint64
	if err := conn.QueryRow(ctx,
		`SELECT count() FROM ai_governance_coverage_daily FINAL WHERE org_id = ?`, orgID).
		Scan(&coverageRows); err != nil {
		t.Fatalf("count coverage rows: %v", err)
	}
	if coverageRows != 1 {
		t.Fatalf("coverage rows = %d after two runs, want 1 -- the rewrite must collapse", coverageRows)
	}
}
