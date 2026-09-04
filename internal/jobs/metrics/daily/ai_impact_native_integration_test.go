//go:build integration

package daily

import (
	"context"
	"testing"
	"time"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// TestAIImpactComputeFamilyAgainstRealClickHouse is the ai_impact port's
// live-driver proof (CHAOS-4280), running the real production entry point
// against a real ClickHouse with the production schema.
//
// SIX new readers land here at once, and the thing they most need proving is
// DRIVER TYPE COMPATIBILITY -- the class that a fake scanner is structurally
// blind to, because a fake accepts whatever Go type the reader asks for while
// only a real driver checks it against the declared column. That is not
// hypothetical for this PR: the sibling ai_governance loader shipped a *bool
// target against a Nullable(UInt8) column and CI's storage shard rejected it
// on the first run. The readers here scan Nullable(DateTime64), Nullable(UInt32),
// Array(String), UInt8 and a tuple unwrap, so each is the same kind of bet.
//
// It also proves, in one round trip:
//
//   - the tuple-argMax dedup picks ONE WHOLE ROW on a last_synced tie, rather
//     than mixing columns from different physical rows;
//   - the team resolver runs end-to-end -- repos.repo -> pattern -> team_id --
//     which is the sole source of this family's team dimension, since the
//     attribution loader normalises its own team_id to NULL;
//   - rows actually land in ai_impact_metrics_daily with the right key.
func TestAIImpactComputeFamilyAgainstRealClickHouse(t *testing.T) {
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
		// Production shapes. reviews_count / changes_requested_count are
		// non-nullable UInt32 with DEFAULT 0; additions/deletions/changed_files
		// are Nullable(UInt32); merged_at is Nullable(DateTime64) -- three
		// different nullability shapes in one reader, which is the point.
		`CREATE TABLE git_pull_requests (
    repo_id UUID, number UInt32, title Nullable(String), body Nullable(String),
    created_at DateTime64(3, 'UTC'), merged_at Nullable(DateTime64(3, 'UTC')),
    additions Nullable(UInt32), deletions Nullable(UInt32), changed_files Nullable(UInt32),
    changes_requested_count UInt32 DEFAULT 0, reviews_count UInt32 DEFAULT 0,
    last_synced DateTime64(3, 'UTC'), org_id String
) ENGINE = ReplacingMergeTree(last_synced) ORDER BY (repo_id, number)`,
		`CREATE TABLE git_pull_request_reviews (
    repo_id UUID, number UInt32, review_id String, reviewer String, state String,
    submitted_at DateTime64(3, 'UTC'), last_synced DateTime64(3, 'UTC'), org_id String
) ENGINE = ReplacingMergeTree(last_synced) ORDER BY (repo_id, number, review_id)`,
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
		`CREATE TABLE work_graph_issue_pr (
    repo_id UUID, pr_number UInt32, work_item_id String,
    last_synced DateTime64(3, 'UTC'), org_id String
) ENGINE = ReplacingMergeTree(last_synced) ORDER BY (repo_id, pr_number, work_item_id)`,
		`CREATE TABLE work_items (
    repo_id UUID, work_item_id String, type Nullable(String),
    last_synced DateTime64(3, 'UTC'), org_id String
) ENGINE = ReplacingMergeTree(last_synced) ORDER BY (repo_id, work_item_id)`,
		`CREATE TABLE work_graph_pr_commit (
    repo_id UUID, pr_number UInt32, commit_hash String, evidence String,
    last_synced DateTime64(3, 'UTC'), org_id String
) ENGINE = ReplacingMergeTree(last_synced) ORDER BY (repo_id, pr_number, commit_hash)`,
		`CREATE TABLE git_commits (
    repo_id UUID, hash String, committer_when DateTime64(3, 'UTC'),
    last_synced DateTime64(3, 'UTC'), org_id String
) ENGINE = ReplacingMergeTree(last_synced) ORDER BY (repo_id, hash)`,
		`CREATE TABLE git_commit_stats (
    repo_id UUID, commit_hash String, file_path String,
    last_synced DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(last_synced) ORDER BY (repo_id, commit_hash, file_path)`,
		// repo_patterns is Array(String) -- another distinct scan type.
		`CREATE TABLE teams (
    id String, name String, repo_patterns Array(String), is_active UInt8 DEFAULT 1,
    updated_at DateTime64(6), org_id String
) ENGINE = ReplacingMergeTree(updated_at) ORDER BY (id)`,
		`CREATE TABLE repos (
    id UUID, repo String, last_synced DateTime64(3, 'UTC'), org_id String
) ENGINE = ReplacingMergeTree(last_synced) ORDER BY (id)`,
		`CREATE TABLE ai_impact_metrics_daily (
    org_id String, team_id String, repo_id UUID, work_type LowCardinality(String),
    day Date, attribution_bucket LowCardinality(String),
    prs_total UInt32, prs_merged UInt32, ai_assisted_prs UInt32, agent_created_prs UInt32,
    human_prs UInt32, unknown_prs UInt32, ai_assisted_pr_ratio Nullable(Float64),
    agent_created_pr_count UInt32,
    cycle_time_avg_hours Nullable(Float64), baseline_cycle_time_avg_hours Nullable(Float64),
    ai_cycle_time_delta_hours Nullable(Float64),
    reviews_per_pr Nullable(Float64), baseline_reviews_per_pr Nullable(Float64),
    ai_review_amplification Nullable(Float64), changes_requested_per_pr Nullable(Float64),
    rework_prs UInt32, rework_drag_rate Nullable(Float64), followup_commits_count UInt32,
    revert_prs UInt32, revert_rate Nullable(Float64),
    incidents_count UInt32, incident_drag_rate Nullable(Float64),
    test_gap_prs UInt32, test_gap_rate Nullable(Float64),
    leverage_prs_component Float64, leverage_cycle_time_component Nullable(Float64),
    leverage_review_component Nullable(Float64), leverage_rework_component Nullable(Float64),
    leverage_test_component Nullable(Float64), leverage_incident_component Nullable(Float64),
    computed_at DateTime64(3, 'UTC') DEFAULT now64()
) ENGINE = ReplacingMergeTree(computed_at) PARTITION BY toYYYYMM(day)
ORDER BY (org_id, team_id, repo_id, work_type, day, attribution_bucket)`,
		// The incident tables LoadIncidentsStarted reads (reused, not re-ported).
		`CREATE TABLE operational_incidents (
    id String, org_id String, service_id String, normalized_status String,
    started_at DateTime64(3, 'UTC'), resolved_at Nullable(DateTime64(3, 'UTC')),
    is_deleted UInt8 DEFAULT 0, last_synced DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(last_synced) ORDER BY (org_id, id)`,
		`CREATE TABLE operational_service_repository_mappings (
    org_id String, service_id String, repo_id Nullable(UUID), is_active UInt8 DEFAULT 1,
    valid_from Nullable(DateTime64(3, 'UTC')), valid_to Nullable(DateTime64(3, 'UTC')),
    last_synced DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(last_synced) ORDER BY (org_id, service_id)
SETTINGS allow_nullable_key = 1`,
	} {
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatalf("schema: %v\nstatement: %s", err, statement)
		}
	}

	const (
		orgID  = "00000000-0000-4000-8000-0000000000a0"
		repoID = "00000000-0000-4000-8000-0000000000a1"
	)

	// PR 1 has TWO rows sharing an IDENTICAL last_synced that disagree on
	// reviews_count. The tuple-argMax must take one WHOLE row, so
	// reviews_per_pr lands on one of the two seeded values -- never a blend.
	// PR 2 is a plain single row with NULL additions/deletions/changed_files,
	// which is the Nullable(UInt32) scan path.
	if err := conn.Exec(ctx, `
INSERT INTO git_pull_requests (repo_id, number, created_at, merged_at, additions, deletions,
    changed_files, changes_requested_count, reviews_count, last_synced, org_id) VALUES
(toUUID('`+repoID+`'), 1, toDateTime64('2026-09-03 01:00:00', 3, 'UTC'),
 toDateTime64('2026-09-03 05:00:00', 3, 'UTC'), 10, 5, 2, 0, 4,
 toDateTime64('2026-09-03 09:00:00.000', 3, 'UTC'), '`+orgID+`'),
(toUUID('`+repoID+`'), 1, toDateTime64('2026-09-03 01:00:00', 3, 'UTC'),
 toDateTime64('2026-09-03 05:00:00', 3, 'UTC'), 10, 5, 2, 0, 6,
 toDateTime64('2026-09-03 09:00:00.000', 3, 'UTC'), '`+orgID+`'),
(toUUID('`+repoID+`'), 2, toDateTime64('2026-09-03 02:00:00', 3, 'UTC'),
 NULL, NULL, NULL, NULL, 0, 0,
 toDateTime64('2026-09-03 09:00:00', 3, 'UTC'), '`+orgID+`')`); err != nil {
		t.Fatal(err)
	}
	if err := conn.Exec(ctx, `
INSERT INTO git_pull_request_reviews (repo_id, number, review_id, reviewer, state, submitted_at, last_synced, org_id) VALUES
(toUUID('`+repoID+`'), 2, 'r1', 'alice', 'CHANGES_REQUESTED',
 toDateTime64('2026-09-03 03:00:00', 3, 'UTC'), toDateTime64('2026-09-03 09:00:00', 3, 'UTC'), '`+orgID+`')`); err != nil {
		t.Fatal(err)
	}
	// PR 1 -> ai_assisted (direct subject-id path). PR 2 left unattributed so
	// it lands in the unknown bucket.
	if err := conn.Exec(ctx, `
INSERT INTO ai_attribution (record_id, org_id, provider, subject_type, subject_id, repo_id,
    kind, source, confidence, actor, evidence, observed_at, ingested_at, superseded_by, computed_at) VALUES
(generateUUIDv4(), toUUID('`+orgID+`'), 'github', 'pull_request', '1', toUUID('`+repoID+`'),
 'ai_assisted', 'pr_label', 0.95, NULL, '{}',
 toDateTime64('2026-09-03 05:00:00', 3, 'UTC'), now64(3), NULL, now64(3))`); err != nil {
		t.Fatal(err)
	}
	// Linkage: PR 1 touches a test file -> has_test_change true.
	if err := conn.Exec(ctx, `
INSERT INTO work_graph_pr_commit (repo_id, pr_number, commit_hash, evidence, last_synced, org_id) VALUES
(toUUID('`+repoID+`'), 1, 'aaa', 'native', toDateTime64('2026-09-03 09:00:00', 3, 'UTC'), '`+orgID+`')`); err != nil {
		t.Fatal(err)
	}
	if err := conn.Exec(ctx, `
INSERT INTO git_commits (repo_id, hash, committer_when, last_synced, org_id) VALUES
(toUUID('`+repoID+`'), 'aaa', toDateTime64('2026-09-03 04:00:00', 3, 'UTC'),
 toDateTime64('2026-09-03 09:00:00', 3, 'UTC'), '`+orgID+`')`); err != nil {
		t.Fatal(err)
	}
	if err := conn.Exec(ctx, `
INSERT INTO git_commit_stats (repo_id, commit_hash, file_path, last_synced) VALUES
(toUUID('`+repoID+`'), 'aaa', 'src/thing.spec.ts', toDateTime64('2026-09-03 09:00:00', 3, 'UTC'))`); err != nil {
		t.Fatal(err)
	}
	// Team resolution: repos.repo "acme/alpha" must match the "acme/*" pattern
	// and produce team_id "team-acme" on every emitted row.
	if err := conn.Exec(ctx, `
INSERT INTO teams (id, name, repo_patterns, is_active, updated_at, org_id) VALUES
('team-acme', 'Acme', ['acme/*'], 1, toDateTime64('2026-09-01 00:00:00', 6), '`+orgID+`')`); err != nil {
		t.Fatal(err)
	}
	if err := conn.Exec(ctx, `
INSERT INTO repos (id, repo, last_synced, org_id) VALUES
(toUUID('`+repoID+`'), 'acme/alpha', toDateTime64('2026-09-01 00:00:00', 3, 'UTC'), '`+orgID+`')`); err != nil {
		t.Fatal(err)
	}

	executor, err := NewAIImpactExecutor(conn)
	if err != nil {
		t.Fatal(err)
	}
	run := Run{OrganizationID: orgID, TargetDay: time.Date(2026, 9, 3, 0, 0, 0, 0, time.UTC)}
	partition := Partition{ID: "p1", RepoIDs: []RepositoryID{RepositoryID(repoID)}}

	written, err := executor.ComputeFamily(ctx, run, partition)
	if err != nil {
		// Every one of the six readers runs before the first write, so a
		// driver type mismatch in ANY of them surfaces here.
		t.Fatalf("ComputeFamily against a real ClickHouse: %v", err)
	}
	if written == 0 {
		t.Fatal("wrote zero rows; the family computed nothing from a seeded fixture")
	}

	// The team dimension came from the resolver, not from the attribution row
	// (whose team_id the loader normalises to NULL). An empty team_id here
	// means the repos -> pattern -> team chain silently did not run.
	var teamRows uint64
	if err := conn.QueryRow(ctx,
		`SELECT count() FROM ai_impact_metrics_daily FINAL WHERE org_id = ? AND team_id = 'team-acme'`,
		orgID).Scan(&teamRows); err != nil {
		t.Fatalf("read team-scoped rows: %v", err)
	}
	if teamRows == 0 {
		t.Fatal("no row carries team_id 'team-acme' -- the repo-pattern resolver did not resolve " +
			"acme/alpha, so this family's whole team dimension is empty")
	}

	// Whole-row dedup: PR 1's two rows disagree on reviews_count (4 vs 6) at
	// an identical last_synced. reviews_per_pr for the ai_assisted bucket
	// (exactly one PR) must equal one of the seeded values, never a blend.
	var reviewsPerPR *float64
	if err := conn.QueryRow(ctx, `
SELECT reviews_per_pr FROM ai_impact_metrics_daily FINAL
WHERE org_id = ? AND attribution_bucket = 'ai_assisted'`, orgID).Scan(&reviewsPerPR); err != nil {
		t.Fatalf("read ai_assisted bucket: %v", err)
	}
	if reviewsPerPR == nil {
		t.Fatal("reviews_per_pr is NULL for a bucket with one PR")
	}
	if *reviewsPerPR != 4 && *reviewsPerPR != 6 {
		t.Fatalf("reviews_per_pr = %v; PR 1's two tied rows seeded reviews_count 4 and 6, so the "+
			"deduped value must be one of them -- anything else means the tuple-argMax blended rows",
			*reviewsPerPR)
	}

	// The unknown bucket is always emitted, even though PR 2 carries no
	// attribution -- ai_impact.py:419's `bucket != UNKNOWN_BUCKET` guard.
	var unknownRows uint64
	if err := conn.QueryRow(ctx,
		`SELECT count() FROM ai_impact_metrics_daily FINAL WHERE org_id = ? AND attribution_bucket = 'unknown'`,
		orgID).Scan(&unknownRows); err != nil {
		t.Fatalf("read unknown bucket: %v", err)
	}
	if unknownRows == 0 {
		t.Fatal("no unknown-bucket row; it must be emitted even when empty")
	}

	// Idempotency: the RMT key covers the producer's grouping, so re-running
	// the same partition must not grow the table.
	before := written
	if _, err := executor.ComputeFamily(ctx, run, partition); err != nil {
		t.Fatalf("second ComputeFamily: %v", err)
	}
	var totalRows uint64
	if err := conn.QueryRow(ctx,
		`SELECT count() FROM ai_impact_metrics_daily FINAL WHERE org_id = ?`, orgID).Scan(&totalRows); err != nil {
		t.Fatalf("count rows after re-run: %v", err)
	}
	if totalRows != uint64(before) {
		t.Fatalf("ai_impact_metrics_daily has %d rows after re-running the same partition, want %d; "+
			"FINAL should collapse the rewrite onto the identical ORDER BY key", totalRows, before)
	}
}
