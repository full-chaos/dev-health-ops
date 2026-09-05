//go:build integration

package daily

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/aiimpact"
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
) ENGINE = ReplacingMergeTree(last_synced) ORDER BY (org_id, repo_id, number)`,
		`CREATE TABLE git_pull_request_reviews (
    repo_id UUID, number UInt32, review_id String, reviewer String, state String,
    submitted_at DateTime64(3, 'UTC'), last_synced DateTime64(3, 'UTC'), org_id String
) ENGINE = ReplacingMergeTree(last_synced) ORDER BY (org_id, repo_id, number, review_id)`,
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
		// codex round chaos-4280-r2, finding 5: this table used to lack org_id,
		// so LoadAIImpactPRCommitLinkage's `s.org_id = p.org_id` join condition
		// failed schema validation and the executor's swallow-to-unavailable
		// path silently absorbed it -- this test asserted rows were written
		// and never noticed the linkage path never actually succeeded.
		`CREATE TABLE git_commit_stats (
    repo_id UUID, commit_hash String, file_path String,
    last_synced DateTime64(3, 'UTC'), org_id String
) ENGINE = ReplacingMergeTree(last_synced) ORDER BY (org_id, repo_id, commit_hash, file_path)`,
		// repo_patterns is Array(String) -- another distinct scan type.
		`CREATE TABLE teams (
    id String, name String, repo_patterns Array(String), is_active UInt8 DEFAULT 1,
    updated_at DateTime64(6), org_id String
) ENGINE = ReplacingMergeTree(updated_at) ORDER BY (org_id, id)`,
		`CREATE TABLE repos (
    id UUID, repo String, last_synced DateTime64(3, 'UTC'), org_id String
) ENGINE = ReplacingMergeTree(last_synced) ORDER BY (org_id, id)`,
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
INSERT INTO git_commit_stats (repo_id, commit_hash, file_path, last_synced, org_id) VALUES
(toUUID('`+repoID+`'), 'aaa', 'src/thing.spec.ts', toDateTime64('2026-09-03 09:00:00', 3, 'UTC'), '`+orgID+`')`); err != nil {
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

	// codex round chaos-4280-r2, finding 5: this test asserted rows were
	// written, team resolution, dedup, the unknown bucket, and rerun
	// idempotency, but never asserted the commit-linkage query itself
	// SUCCEEDED -- it could pass identically whether PR 1's has_test_change
	// resolved to true (the fixture's actual intent, see the comment above
	// the work_graph_pr_commit/git_commits/git_commit_stats inserts) or
	// silently degraded to unavailable via the swallow path
	// (ai_impact_native_executor.go, aiimpact.RecordLinkageUnavailable).
	// Capturing the counter here, before the fix, is what made that gap
	// visible: the fixture's git_commit_stats table lacked org_id, so the
	// linkage join failed schema validation on every run of this test.
	var linkageBefore bytes.Buffer
	if err := aiimpact.LinkageMetricsSource().WritePrometheus(&linkageBefore); err != nil {
		t.Fatalf("read linkage metric before: %v", err)
	}

	written, err := executor.ComputeFamily(ctx, run, partition)
	if err != nil {
		// Every one of the six readers runs before the first write, so a
		// driver type mismatch in ANY of them surfaces here.
		t.Fatalf("ComputeFamily against a real ClickHouse: %v", err)
	}
	if written == 0 {
		t.Fatal("wrote zero rows; the family computed nothing from a seeded fixture")
	}

	var linkageAfter bytes.Buffer
	if err := aiimpact.LinkageMetricsSource().WritePrometheus(&linkageAfter); err != nil {
		t.Fatalf("read linkage metric after: %v", err)
	}
	if before, after := parseLinkageUnavailableCounter(t, linkageBefore.String()),
		parseLinkageUnavailableCounter(t, linkageAfter.String()); after != before {
		t.Fatalf("dev_health_ai_impact_linkage_unavailable_total went %d -> %d, want unchanged -- "+
			"this fixture's commit linkage must SUCCEED (PR 1 has a real, well-formed "+
			"work_graph_pr_commit/git_commits/git_commit_stats chain), so a fresh increment "+
			"here means the linkage query silently failed and PR 1's has_test_change degraded "+
			"to unavailable instead of resolving true", before, after)
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

// TestAIImpactDedupIsTenantScoped is this family's half of the codex round 1
// P1 on #2229: a dedup GROUP BY that omits org_id picks the newest row ACROSS
// TENANTS. ai_impact had the same defect in three readers.
//
// Two orgs legitimately share one repo_id and PR number. Org B's row is NEWER
// with reviews_count=0; org A's is older with 4. Grouping without org_id lets
// B's row win globally, A's filter then misses, and A's reviews_per_pr reads
// as zero -- a metric that looks plausible and is another tenant's.
//
// Asserted in BOTH directions so the fix cannot degenerate into pinning one
// tenant, and against the reviews table too, since that reader had the same
// omission with a different key shape.
func TestAIImpactDedupIsTenantScoped(t *testing.T) {
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

	// CURRENT keys, from the rekey migrations -- not the stale CREATEs.
	for _, statement := range []string{
		`CREATE TABLE git_pull_requests (
    repo_id UUID, number UInt32, created_at DateTime64(3, 'UTC'),
    merged_at Nullable(DateTime64(3, 'UTC')),
    additions Nullable(UInt32), deletions Nullable(UInt32), changed_files Nullable(UInt32),
    changes_requested_count UInt32 DEFAULT 0, reviews_count UInt32 DEFAULT 0,
    last_synced DateTime64(3, 'UTC'), org_id String
) ENGINE = ReplacingMergeTree(last_synced) ORDER BY (org_id, repo_id, number)`,
		`CREATE TABLE git_pull_request_reviews (
    repo_id UUID, number UInt32, review_id String, reviewer String, state String,
    submitted_at DateTime64(3, 'UTC'), last_synced DateTime64(3, 'UTC'), org_id String
) ENGINE = ReplacingMergeTree(last_synced) ORDER BY (org_id, repo_id, number, review_id)`,
	} {
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatalf("schema: %v", err)
		}
	}

	const (
		sharedRepo = "00000000-0000-4000-8000-00000000ab01"
		orgA       = "00000000-0000-4000-8000-00000000ab00"
		orgB       = "00000000-0000-4000-8000-00000000cd00"
	)
	if err := conn.Exec(ctx, `
INSERT INTO git_pull_requests (repo_id, number, created_at, merged_at, additions, deletions,
    changed_files, changes_requested_count, reviews_count, last_synced, org_id) VALUES
(toUUID('`+sharedRepo+`'), 3, toDateTime64('2026-09-03 01:00:00', 3, 'UTC'),
 toDateTime64('2026-09-03 05:00:00', 3, 'UTC'), 1, 1, 1, 0, 4,
 toDateTime64('2026-09-03 09:00:00', 3, 'UTC'), '`+orgA+`'),
(toUUID('`+sharedRepo+`'), 3, toDateTime64('2026-09-03 01:00:00', 3, 'UTC'),
 toDateTime64('2026-09-03 05:00:00', 3, 'UTC'), 1, 1, 1, 0, 0,
 toDateTime64('2026-09-03 10:00:00', 3, 'UTC'), '`+orgB+`')`); err != nil {
		t.Fatal(err)
	}
	// Same shape on the reviews table: B's row is newer and differs in state.
	if err := conn.Exec(ctx, `
INSERT INTO git_pull_request_reviews (repo_id, number, review_id, reviewer, state, submitted_at, last_synced, org_id) VALUES
(toUUID('`+sharedRepo+`'), 3, 'rv1', 'alice', 'CHANGES_REQUESTED',
 toDateTime64('2026-09-03 03:00:00', 3, 'UTC'), toDateTime64('2026-09-03 09:00:00', 3, 'UTC'), '`+orgA+`'),
(toUUID('`+sharedRepo+`'), 3, 'rv1', 'bob', 'APPROVED',
 toDateTime64('2026-09-03 03:00:00', 3, 'UTC'), toDateTime64('2026-09-03 10:00:00', 3, 'UTC'), '`+orgB+`')`); err != nil {
		t.Fatal(err)
	}

	repoIDs := []uuid.UUID{uuid.MustParse(sharedRepo)}
	start := time.Date(2026, 9, 3, 0, 0, 0, 0, time.UTC)
	end := start.AddDate(0, 0, 1)

	for _, tenant := range []struct {
		org          string
		wantReviews  uint32
		wantRevState string
	}{
		{orgA, 4, "CHANGES_REQUESTED"},
		{orgB, 0, "APPROVED"},
	} {
		prs, err := LoadAIImpactPullRequests(ctx, conn, tenant.org, repoIDs, start, end)
		if err != nil {
			t.Fatalf("%s: load PRs: %v", tenant.org, err)
		}
		if len(prs) != 1 {
			t.Fatalf("%s: got %d PRs, want 1", tenant.org, len(prs))
		}
		if prs[0].ReviewsCount == nil || *prs[0].ReviewsCount != tenant.wantReviews {
			t.Fatalf("%s: reviews_count=%v, want %d -- the other tenant's newer row won a dedup "+
				"group that omitted org_id", tenant.org, prs[0].ReviewsCount, tenant.wantReviews)
		}

		reviews, err := LoadAIImpactReviews(ctx, conn, tenant.org, repoIDs, start, end)
		if err != nil {
			t.Fatalf("%s: load reviews: %v", tenant.org, err)
		}
		if len(reviews) != 1 {
			t.Fatalf("%s: got %d reviews, want 1", tenant.org, len(reviews))
		}
		if reviews[0].State == nil || *reviews[0].State != tenant.wantRevState {
			t.Fatalf("%s: review state=%v, want %q -- same cross-tenant dedup defect on the "+
				"reviews reader, whose key is (org_id, repo_id, number, review_id)",
				tenant.org, reviews[0].State, tenant.wantRevState)
		}
	}
}

// TestAIImpactAttributionTenantIsolation is codex round chaos-4280-r1's
// finding 1: LoadAIImpactAttributions joined work_graph_issue_pr and
// work_items on (repo_id, ...) alone, with no org_id in either ON clause.
//
// Two orgs share a repo_id and PR number and, by coincidence (work_item_id is
// an opaque provider string, not globally unique), the SAME work_item_id
// value -- org A links it to a "task", org B to a "bug". Org A's PR is the
// only row in git_pull_requests, so the outer WHERE org filter alone cannot
// catch the leak; only the join condition can.
//
// Before the fix this is deterministic, not merely possible: the query's own
// ORDER BY ends in `work_type` ascending, and "bug" < "task" lexically, so
// org B's row always sorts first and always wins the `seen`-map dedup --
// org A's answer is org B's classification on every run, not a coin flip.
func TestAIImpactAttributionTenantIsolation(t *testing.T) {
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
		`CREATE TABLE git_pull_requests (
    repo_id UUID, number UInt32, created_at DateTime64(3, 'UTC'),
    merged_at Nullable(DateTime64(3, 'UTC')),
    additions Nullable(UInt32), deletions Nullable(UInt32), changed_files Nullable(UInt32),
    changes_requested_count UInt32 DEFAULT 0, reviews_count UInt32 DEFAULT 0,
    last_synced DateTime64(3, 'UTC'), org_id String
) ENGINE = ReplacingMergeTree(last_synced) ORDER BY (org_id, repo_id, number)`,
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
) ENGINE = ReplacingMergeTree(last_synced) ORDER BY (org_id, repo_id, pr_number, work_item_id)`,
		`CREATE TABLE work_items (
    repo_id UUID, work_item_id String, type Nullable(String),
    last_synced DateTime64(3, 'UTC'), org_id String
) ENGINE = ReplacingMergeTree(last_synced) ORDER BY (org_id, repo_id, work_item_id)`,
	} {
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatalf("schema: %v\nstatement: %s", err, statement)
		}
	}

	const (
		orgA        = "00000000-0000-4000-8000-0000000000e0"
		orgB        = "00000000-0000-4000-8000-0000000000e1"
		sharedRepo  = "00000000-0000-4000-8000-0000000000e2"
		sharedWIID  = "shared-wi"
		wantWorkTyp = "task"
	)

	if err := conn.Exec(ctx, `
INSERT INTO git_pull_requests (repo_id, number, created_at, merged_at, additions, deletions,
    changed_files, changes_requested_count, reviews_count, last_synced, org_id) VALUES
(toUUID('`+sharedRepo+`'), 7, toDateTime64('2026-09-03 01:00:00', 3, 'UTC'),
 toDateTime64('2026-09-03 05:00:00', 3, 'UTC'), 1, 1, 1, 0, 0,
 toDateTime64('2026-09-03 09:00:00', 3, 'UTC'), '`+orgA+`')`); err != nil {
		t.Fatal(err)
	}
	// Both orgs' link rows point at the SAME work_item_id string -- opaque
	// provider ids collide across tenants by construction, not by mistake.
	if err := conn.Exec(ctx, `
INSERT INTO work_graph_issue_pr (repo_id, pr_number, work_item_id, last_synced, org_id) VALUES
(toUUID('`+sharedRepo+`'), 7, '`+sharedWIID+`', toDateTime64('2026-09-03 09:00:00', 3, 'UTC'), '`+orgA+`'),
(toUUID('`+sharedRepo+`'), 7, '`+sharedWIID+`', toDateTime64('2026-09-03 09:00:00', 3, 'UTC'), '`+orgB+`')`); err != nil {
		t.Fatal(err)
	}
	if err := conn.Exec(ctx, `
INSERT INTO work_items (repo_id, work_item_id, type, last_synced, org_id) VALUES
(toUUID('`+sharedRepo+`'), '`+sharedWIID+`', 'task', toDateTime64('2026-09-03 09:00:00', 3, 'UTC'), '`+orgA+`'),
(toUUID('`+sharedRepo+`'), '`+sharedWIID+`', 'bug', toDateTime64('2026-09-03 09:00:00', 3, 'UTC'), '`+orgB+`')`); err != nil {
		t.Fatal(err)
	}
	// One attribution row, owned by org A, referencing the shared work-item id.
	if err := conn.Exec(ctx, `
INSERT INTO ai_attribution (record_id, org_id, provider, subject_type, subject_id, repo_id,
    kind, source, confidence, actor, evidence, observed_at, ingested_at, superseded_by, computed_at) VALUES
(generateUUIDv4(), toUUID('`+orgA+`'), 'github', 'pull_request', '`+sharedWIID+`', toUUID('`+sharedRepo+`'),
 'ai_assisted', 'pr_label', 0.95, NULL, '{}',
 toDateTime64('2026-09-03 05:00:00', 3, 'UTC'), now64(3), NULL, now64(3))`); err != nil {
		t.Fatal(err)
	}

	repoIDs := []uuid.UUID{uuid.MustParse(sharedRepo)}
	start := time.Date(2026, 9, 3, 0, 0, 0, 0, time.UTC)
	end := start.AddDate(0, 0, 1)

	rows, err := LoadAIImpactAttributions(ctx, conn, orgA, repoIDs, start, end)
	if err != nil {
		t.Fatalf("load attributions: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("got %d attribution rows for org A, want exactly 1 -- org B's link/work_items rows "+
			"leaked into org A's join", len(rows))
	}
	if rows[0].WorkType == nil || *rows[0].WorkType != wantWorkTyp {
		t.Fatalf("org A's work_type = %v, want %q -- org B's work_items row (type=\"bug\") won the "+
			"join because it lacked org_id; before the fix this is deterministic (\"bug\" < \"task\" "+
			"in the query's own ORDER BY tiebreak), not a flaky race",
			rows[0].WorkType, wantWorkTyp)
	}
}

// TestAIImpactPRCommitLinkageTenantIsolation is codex round chaos-4280-r1's
// finding 2: LoadAIImpactPRCommitLinkage joined git_commit_stats on
// (repo_id, commit_hash) alone. git_commit_stats gained org_id in its current
// sorting key (027:61); the join predated that migration and was never
// updated, so two orgs sharing a repo_id and commit hash leak each other's
// file paths into the SAME PR's linkage slice.
func TestAIImpactPRCommitLinkageTenantIsolation(t *testing.T) {
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
		`CREATE TABLE work_graph_pr_commit (
    repo_id UUID, pr_number UInt32, commit_hash String, evidence String,
    last_synced DateTime64(3, 'UTC'), org_id String
) ENGINE = ReplacingMergeTree(last_synced) ORDER BY (org_id, repo_id, pr_number, commit_hash)`,
		`CREATE TABLE git_commits (
    repo_id UUID, hash String, committer_when DateTime64(3, 'UTC'),
    last_synced DateTime64(3, 'UTC'), org_id String
) ENGINE = ReplacingMergeTree(last_synced) ORDER BY (org_id, repo_id, hash)`,
		`CREATE TABLE git_commit_stats (
    repo_id UUID, commit_hash String, file_path String,
    last_synced DateTime64(3, 'UTC'), org_id String
) ENGINE = ReplacingMergeTree(last_synced) ORDER BY (org_id, repo_id, commit_hash, file_path)`,
	} {
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatalf("schema: %v\nstatement: %s", err, statement)
		}
	}

	const (
		orgA         = "00000000-0000-4000-8000-0000000000f0"
		orgB         = "00000000-0000-4000-8000-0000000000f1"
		sharedRepo   = "00000000-0000-4000-8000-0000000000f2"
		sharedHash   = "cccccccccccccccccccccccccccccccccccccccc"
		wantFilePath = "src/thing.go"
		leakFilePath = "tests/other_org_secret.spec.ts"
	)

	if err := conn.Exec(ctx, `
INSERT INTO work_graph_pr_commit (repo_id, pr_number, commit_hash, evidence, last_synced, org_id) VALUES
(toUUID('`+sharedRepo+`'), 9, '`+sharedHash+`', 'native', toDateTime64('2026-09-03 09:00:00', 3, 'UTC'), '`+orgA+`')`); err != nil {
		t.Fatal(err)
	}
	if err := conn.Exec(ctx, `
INSERT INTO git_commits (repo_id, hash, committer_when, last_synced, org_id) VALUES
(toUUID('`+sharedRepo+`'), '`+sharedHash+`', toDateTime64('2026-09-03 04:00:00', 3, 'UTC'),
 toDateTime64('2026-09-03 09:00:00', 3, 'UTC'), '`+orgA+`')`); err != nil {
		t.Fatal(err)
	}
	// Same repo_id + commit_hash, two DIFFERENT tenants -- a coincidence the
	// current schema's org_id column exists precisely to disambiguate.
	if err := conn.Exec(ctx, `
INSERT INTO git_commit_stats (repo_id, commit_hash, file_path, last_synced, org_id) VALUES
(toUUID('`+sharedRepo+`'), '`+sharedHash+`', '`+wantFilePath+`', toDateTime64('2026-09-03 09:00:00', 3, 'UTC'), '`+orgA+`'),
(toUUID('`+sharedRepo+`'), '`+sharedHash+`', '`+leakFilePath+`', toDateTime64('2026-09-03 09:00:00', 3, 'UTC'), '`+orgB+`')`); err != nil {
		t.Fatal(err)
	}

	linkage, err := LoadAIImpactPRCommitLinkage(
		ctx, conn, orgA, []uuid.UUID{uuid.MustParse(sharedRepo)}, []uint32{9})
	if err != nil {
		t.Fatalf("load pr commit linkage: %v", err)
	}
	stats := linkage[aiimpact.PRKey{RepoID: uuid.MustParse(sharedRepo), Number: 9}]
	if len(stats) != 1 {
		var paths []string
		for _, s := range stats {
			if s.FilePath != nil {
				paths = append(paths, *s.FilePath)
			}
		}
		t.Fatalf("org A's linkage has %d file(s), want exactly 1 (%v) -- org B's git_commit_stats "+
			"row leaked in because the join lacked org_id", len(stats), paths)
	}
	if stats[0].FilePath == nil || *stats[0].FilePath != wantFilePath {
		t.Fatalf("org A's linked file = %v, want %q -- got org B's file instead",
			stats[0].FilePath, wantFilePath)
	}
}

// TestAIImpactLinkageQueryFailureIsObservable is codex round chaos-4280-r1's
// finding 5. The swallow-to-unavailable behavior itself is correct,
// deliberate Python parity (CHAOS-2183, see ai_impact_native_executor.go) and
// this test does NOT change that: ComputeFamily must still succeed and still
// write rows. What it proves is the fix -- that the condition is no longer
// SILENT: aiimpact.RecordLinkageUnavailable fires exactly once.
//
// The failure is a REAL ClickHouse error, not an injected fake: this fixture
// omits work_graph_pr_commit's `evidence` column, so
// LoadAIImpactPRCommitLinkage's own SELECT list fails schema validation.
func TestAIImpactLinkageQueryFailureIsObservable(t *testing.T) {
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
		`CREATE TABLE git_pull_requests (
    repo_id UUID, number UInt32, title Nullable(String), body Nullable(String),
    created_at DateTime64(3, 'UTC'), merged_at Nullable(DateTime64(3, 'UTC')),
    additions Nullable(UInt32), deletions Nullable(UInt32), changed_files Nullable(UInt32),
    changes_requested_count UInt32 DEFAULT 0, reviews_count UInt32 DEFAULT 0,
    last_synced DateTime64(3, 'UTC'), org_id String
) ENGINE = ReplacingMergeTree(last_synced) ORDER BY (org_id, repo_id, number)`,
		`CREATE TABLE git_pull_request_reviews (
    repo_id UUID, number UInt32, review_id String, reviewer String, state String,
    submitted_at DateTime64(3, 'UTC'), last_synced DateTime64(3, 'UTC'), org_id String
) ENGINE = ReplacingMergeTree(last_synced) ORDER BY (org_id, repo_id, number, review_id)`,
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
) ENGINE = ReplacingMergeTree(last_synced) ORDER BY (org_id, repo_id, pr_number, work_item_id)`,
		`CREATE TABLE work_items (
    repo_id UUID, work_item_id String, type Nullable(String),
    last_synced DateTime64(3, 'UTC'), org_id String
) ENGINE = ReplacingMergeTree(last_synced) ORDER BY (org_id, repo_id, work_item_id)`,
		// `evidence` DELIBERATELY OMITTED -- LoadAIImpactPRCommitLinkage selects
		// it, so this table shape makes that query fail real schema validation.
		`CREATE TABLE work_graph_pr_commit (
    repo_id UUID, pr_number UInt32, commit_hash String,
    last_synced DateTime64(3, 'UTC'), org_id String
) ENGINE = ReplacingMergeTree(last_synced) ORDER BY (org_id, repo_id, pr_number, commit_hash)`,
		`CREATE TABLE git_commits (
    repo_id UUID, hash String, committer_when DateTime64(3, 'UTC'),
    last_synced DateTime64(3, 'UTC'), org_id String
) ENGINE = ReplacingMergeTree(last_synced) ORDER BY (org_id, repo_id, hash)`,
		`CREATE TABLE git_commit_stats (
    repo_id UUID, commit_hash String, file_path String,
    last_synced DateTime64(3, 'UTC'), org_id String
) ENGINE = ReplacingMergeTree(last_synced) ORDER BY (org_id, repo_id, commit_hash, file_path)`,
		`CREATE TABLE teams (
    id String, name String, repo_patterns Array(String), is_active UInt8 DEFAULT 1,
    updated_at DateTime64(6), org_id String
) ENGINE = ReplacingMergeTree(updated_at) ORDER BY (org_id, id)`,
		`CREATE TABLE repos (
    id UUID, repo String, last_synced DateTime64(3, 'UTC'), org_id String
) ENGINE = ReplacingMergeTree(last_synced) ORDER BY (org_id, id)`,
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
		orgID  = "00000000-0000-4000-8000-0000000000f9"
		repoID = "00000000-0000-4000-8000-0000000000fa"
	)
	if err := conn.Exec(ctx, `
INSERT INTO git_pull_requests (repo_id, number, created_at, merged_at, additions, deletions,
    changed_files, changes_requested_count, reviews_count, last_synced, org_id) VALUES
(toUUID('`+repoID+`'), 1, toDateTime64('2026-09-03 01:00:00', 3, 'UTC'), NULL,
 NULL, NULL, NULL, 0, 0, toDateTime64('2026-09-03 09:00:00', 3, 'UTC'), '`+orgID+`')`); err != nil {
		t.Fatal(err)
	}
	if err := conn.Exec(ctx, `
INSERT INTO repos (id, repo, last_synced, org_id) VALUES
(toUUID('`+repoID+`'), 'acme/alpha', toDateTime64('2026-09-01 00:00:00', 3, 'UTC'), '`+orgID+`')`); err != nil {
		t.Fatal(err)
	}

	var before bytes.Buffer
	if err := aiimpact.LinkageMetricsSource().WritePrometheus(&before); err != nil {
		t.Fatalf("read linkage metric before: %v", err)
	}

	executor, err := NewAIImpactExecutor(conn)
	if err != nil {
		t.Fatal(err)
	}
	run := Run{OrganizationID: orgID, TargetDay: time.Date(2026, 9, 3, 0, 0, 0, 0, time.UTC)}
	partition := Partition{ID: "p1", RepoIDs: []RepositoryID{RepositoryID(repoID)}}

	written, err := executor.ComputeFamily(ctx, run, partition)
	if err != nil {
		t.Fatalf("ComputeFamily must still succeed on a linkage failure (Python parity, CHAOS-2183): %v", err)
	}
	if written == 0 {
		t.Fatal("wrote zero rows; a linkage failure must degrade test_gap_rate to null, not abort the family")
	}

	var after bytes.Buffer
	if err := aiimpact.LinkageMetricsSource().WritePrometheus(&after); err != nil {
		t.Fatalf("read linkage metric after: %v", err)
	}
	beforeCount := parseLinkageUnavailableCounter(t, before.String())
	afterCount := parseLinkageUnavailableCounter(t, after.String())
	if afterCount != beforeCount+1 {
		t.Fatalf("dev_health_ai_impact_linkage_unavailable_total went %d -> %d, want +1 -- "+
			"the linkage failure was NOT recorded, so it is silent again", beforeCount, afterCount)
	}
}

func parseLinkageUnavailableCounter(t *testing.T, prometheusText string) uint64 {
	t.Helper()
	const prefix = "dev_health_ai_impact_linkage_unavailable_total "
	for _, line := range strings.Split(prometheusText, "\n") {
		if value, ok := strings.CutPrefix(line, prefix); ok {
			var n uint64
			if _, err := fmt.Sscanf(strings.TrimSpace(value), "%d", &n); err != nil {
				t.Fatalf("parse counter value %q: %v", value, err)
			}
			return n
		}
	}
	t.Fatalf("no %q line in prometheus output: %s", prefix, prometheusText)
	return 0
}
