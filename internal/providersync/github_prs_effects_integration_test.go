//go:build integration

package providersync

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// gitPullRequestsDDL mirrors the production table after migration 027
// (org_id column + org_id-first sort key). The engine and version column are
// the point of this suite, the same as reposDDL's: the readback must resolve
// the winning ReplacingMergeTree version rather than every physical row.
const gitPullRequestsDDL = `
CREATE TABLE git_pull_requests (
  repo_id UUID,
  number UInt32,
  title Nullable(String),
  body Nullable(String),
  state Nullable(String),
  author_name Nullable(String),
  author_email Nullable(String),
  created_at DateTime64(3, 'UTC'),
  merged_at Nullable(DateTime64(3, 'UTC')),
  closed_at Nullable(DateTime64(3, 'UTC')),
  head_branch Nullable(String),
  base_branch Nullable(String),
  additions Nullable(UInt32),
  deletions Nullable(UInt32),
  changed_files Nullable(UInt32),
  first_review_at Nullable(DateTime64(3, 'UTC')),
  first_comment_at Nullable(DateTime64(3, 'UTC')),
  changes_requested_count UInt32 DEFAULT 0,
  reviews_count UInt32 DEFAULT 0,
  comments_count UInt32 DEFAULT 0,
  last_synced DateTime64(3, 'UTC'),
  source_id Nullable(UUID) DEFAULT NULL,
  org_id String DEFAULT 'default'
) ENGINE = ReplacingMergeTree(last_synced)
ORDER BY (org_id, repo_id, number)`

type pullRequestReadbackHarness struct {
	conn       driver.Conn
	repository *PostgresRepository
	claim      Claim
	sink       GitHubPullRequestClickHouseEffects
	now        time.Time
}

func startPullRequestReadbackHarness(
	t *testing.T,
	ctx context.Context,
) *pullRequestReadbackHarness {
	t.Helper()
	postgres, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeContext, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := postgres.Close(closeContext); err != nil {
			t.Errorf("terminate PostgreSQL: %v", err)
		}
	})
	clickhouseInstance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeContext, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := clickhouseInstance.Close(closeContext); err != nil {
			t.Errorf("terminate ClickHouse: %v", err)
		}
	})
	pool, err := pgxpool.New(ctx, postgres.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	createProviderSyncFixture(t, ctx, pool)
	seedProviderSyncFixture(t, ctx, pool)
	// since_at/before_at are widened to match gitHubPullRequestListFixture's
	// window (2026-07-01..2026-07-31T23:59:59Z, the same window
	// nativeTestClaim("github", "prs") uses): the fixture seeded by
	// seedProviderSyncFixture defaults to a one-day window
	// (2026-07-22T12:00..2026-07-23T12:00) sized for repo-metadata's
	// WatermarkNone dataset, which would filter every PR in the list fixture
	// out of the claim window.
	if _, err := pool.Exec(ctx, `
UPDATE public.sync_run_units
SET provider = 'github', dataset_key = 'prs',
    cost_class = 'medium', processor_flags = '{"sync_prs": true}',
    since_at = '2026-07-01T00:00:00Z', before_at = '2026-07-31T23:59:59Z'
WHERE id = $1`, firstUnitID); err != nil {
		t.Fatal(err)
	}
	conn, err := clickhousestore.Open(
		ctx, clickhousestore.DefaultConfig(clickhouseInstance.URI),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if err := conn.Exec(ctx, gitPullRequestsDDL); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	repository, err := NewPostgresRepository(pool)
	if err != nil {
		t.Fatal(err)
	}
	claim, err := repository.Claim(ctx, ClaimRequest{
		UnitID: firstUnitID, OrgID: "org-acme", Owner: uuid.NewString(), Now: now,
		LeaseDuration: time.Minute, AllowExpiredRecovery: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	return &pullRequestReadbackHarness{
		conn:       conn,
		repository: repository,
		claim:      claim,
		now:        now,
		sink: GitHubPullRequestClickHouseEffects{
			Conn: conn, Lease: leaseGuardAt(repository, claim, now),
		},
	}
}

func pullRequestEffect(t *testing.T, row pullRequestRow) EffectBatch {
	t.Helper()
	effect, err := effectBatchFromValues(
		"git_pull_requests", EffectReadbackRequired, []pullRequestRow{row},
	)
	if err != nil {
		t.Fatal(err)
	}
	return effect
}

// TestGitHubPullRequestReadbackResolvesWinningReplacingMergeTreeVersion
// mirrors TestGitHubRepositoryReadbackResolvesWinningReplacingMergeTreeVersion
// for git_pull_requests: only a live engine proves the `FINAL` point-lookup
// query shape actually resolves the winning version and that NULL
// merged_at/closed_at on the WINNING row read back as nil, not some other
// version's value.
func TestGitHubPullRequestReadbackResolvesWinningReplacingMergeTreeVersion(
	t *testing.T,
) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	harness := startPullRequestReadbackHarness(t, ctx)
	claim, sink, now := harness.claim, harness.sink, harness.now
	current := pullRequestReadbackFixture(now)
	current.OrgID = claim.OrgID
	effect := pullRequestEffect(t, current)

	// 1. Nothing written yet.
	inspection, err := sink.InspectEffect(ctx, claim, effect)
	if err != nil || inspection != EffectAbsent {
		t.Fatalf("empty table inspection=%s error=%v", inspection, err)
	}

	// 2. Only an earlier occurrence's version exists (an open PR, so
	//    merged_at/closed_at are NULL). Pre-merge history must not be
	//    mistaken for this effect.
	previous := current
	previous.LastSynced = now.Add(-24 * time.Hour)
	previous.State = "open"
	previous.MergedAt, previous.ClosedAt = nil, nil
	if err := sink.WriteEffect(ctx, claim, pullRequestEffect(t, previous)); err != nil {
		t.Fatal(err)
	}
	inspection, err = sink.InspectEffect(ctx, claim, effect)
	if err != nil || inspection != EffectAbsent {
		t.Fatalf("stale-only inspection=%s error=%v", inspection, err)
	}

	// 3. This occurrence lands on top of that unmerged history.
	if err := sink.WriteEffect(ctx, claim, effect); err != nil {
		t.Fatal(err)
	}
	assertPullRequestVersionCount(t, ctx, harness, current.RepoID, current.Number, 2)
	inspection, err = sink.InspectEffect(ctx, claim, effect)
	if err != nil || inspection != EffectExact {
		t.Fatalf("pre-merge history inspection=%s error=%v", inspection, err)
	}

	// 4. A duplicate reinsert of the identical row is still exactly this
	//    effect; ReplacingMergeTree collapses the copies on merge.
	if err := sink.WriteEffect(ctx, claim, effect); err != nil {
		t.Fatal(err)
	}
	assertPullRequestVersionCount(t, ctx, harness, current.RepoID, current.Number, 3)
	inspection, err = sink.InspectEffect(ctx, claim, effect)
	if err != nil || inspection != EffectExact {
		t.Fatalf("duplicate inspection=%s error=%v", inspection, err)
	}
}

// TestGitHubPullRequestReadbackDoesNotReconstructRowFromMixedVersions is
// codex H2 (CHAOS-3122), the single most dangerous defect in this pair's
// review: independently computing `argMax(column, last_synced)` per column
// is NOT equivalent to reading the row with the maximum last_synced.
// ClickHouse's argMax skips a row whose ARGUMENT is NULL when picking the
// max, so if the WINNING (most recent) row has a NULL in some column while
// an OLDER, non-winning row has a non-NULL value there, the old per-column
// query would silently backfill that column from the wrong version --
// verified empirically against a real unmerged multi-part ReplacingMergeTree
// table before this fix landed. The existing
// TestGitHubPullRequestReadbackResolvesWinningReplacingMergeTreeVersion test
// puts NULLs on the OLDER version only, which cannot observe this: this
// test puts them on the WINNING version specifically, with an OLDER version
// that has non-NULL values in the exact same columns, which is the only
// shape that can distinguish "read the winning row" from "assemble a row
// from whichever version has a non-NULL value per column".
func TestGitHubPullRequestReadbackDoesNotReconstructRowFromMixedVersions(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	harness := startPullRequestReadbackHarness(t, ctx)
	claim, sink, now := harness.claim, harness.sink, harness.now

	// Older version (merged, so merged_at/closed_at/body are all non-NULL).
	older := pullRequestReadbackFixture(now.Add(-time.Hour))
	older.OrgID = claim.OrgID
	if err := sink.WriteEffect(ctx, claim, pullRequestEffect(t, older)); err != nil {
		t.Fatal(err)
	}

	// Winning version: the PR was reopened (state back to "open"), so
	// merged_at, closed_at, AND first_review_at are all NULL on the row
	// that must win -- while the older version above has non-NULL values in
	// every one of those columns. A per-column argMax reconstruction would
	// backfill some or all of them from the older row instead of correctly
	// reading NULL.
	winning := pullRequestReadbackFixture(now)
	winning.OrgID = claim.OrgID
	winning.State = "open"
	winning.MergedAt, winning.ClosedAt = nil, nil
	winning.FirstReviewAt = nil
	winning.ReviewsCount, winning.ChangesRequestedCount = 0, 0
	effect := pullRequestEffect(t, winning)
	if err := sink.WriteEffect(ctx, claim, effect); err != nil {
		t.Fatal(err)
	}
	assertPullRequestVersionCount(t, ctx, harness, winning.RepoID, winning.Number, 2)

	inspection, err := sink.InspectEffect(ctx, claim, effect)
	if err != nil {
		t.Fatal(err)
	}
	if inspection != EffectExact {
		t.Fatalf("inspection=%s want %s: a row-consistent read of the winning "+
			"version (all fields NULL) must match exactly, not a Frankenstein "+
			"row backfilled from the older, non-winning version", inspection, EffectExact)
	}

	// The other direction: asserting the OLDER row's (non-NULL) shape as
	// "expected" must now report a conflict, not an accidental exact match
	// -- if the bug were present, an attacker (or a future refactor) could
	// have this comparison spuriously pass by reconstructing exactly the
	// old row's values.
	staleEffect := pullRequestEffect(t, older)
	inspection, err = sink.InspectEffect(ctx, claim, staleEffect)
	if err != nil {
		t.Fatal(err)
	}
	if inspection != EffectConflict {
		t.Fatalf("inspection=%s want %s: the older, superseded version must "+
			"never read back as the current winner", inspection, EffectConflict)
	}
}

// TestGitHubPullRequestReadbackReportsConflictingWinningVersions mirrors
// TestGitHubRepositoryReadbackReportsConflictingWinningVersions.
func TestGitHubPullRequestReadbackReportsConflictingWinningVersions(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	t.Run("same version, different content", func(t *testing.T) {
		harness := startPullRequestReadbackHarness(t, ctx)
		claim, sink, now := harness.claim, harness.sink, harness.now
		expected := pullRequestReadbackFixture(now)
		expected.OrgID = claim.OrgID
		other := expected
		otherTitle := "Some other title"
		other.Title = &otherTitle
		if err := sink.WriteEffect(ctx, claim, pullRequestEffect(t, other)); err != nil {
			t.Fatal(err)
		}
		inspection, err := sink.InspectEffect(ctx, claim, pullRequestEffect(t, expected))
		if err != nil || inspection != EffectConflict {
			t.Fatalf("inspection=%s error=%v", inspection, err)
		}
	})

	t.Run("newer occurrence superseded the key", func(t *testing.T) {
		harness := startPullRequestReadbackHarness(t, ctx)
		claim, sink, now := harness.claim, harness.sink, harness.now
		expected := pullRequestReadbackFixture(now)
		expected.OrgID = claim.OrgID
		newer := expected
		newer.LastSynced = now.Add(time.Hour)
		if err := sink.WriteEffect(ctx, claim, pullRequestEffect(t, newer)); err != nil {
			t.Fatal(err)
		}
		inspection, err := sink.InspectEffect(ctx, claim, pullRequestEffect(t, expected))
		if err != nil || inspection != EffectConflict {
			t.Fatalf("inspection=%s error=%v", inspection, err)
		}
	})
}

// TestGitHubPullRequestCrashWindowRecoversWithoutDuplicateVersion is the
// end-to-end fence, mirroring
// TestGitHubRepositoryCrashWindowRecoversWithoutDuplicateVersion: ClickHouse
// accepted the insert, the process died before CommitEffect, and recovery
// must mark the effect committed from the readback instead of writing a
// second physical version.
func TestGitHubPullRequestCrashWindowRecoversWithoutDuplicateVersion(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	harness := startPullRequestReadbackHarness(t, ctx)
	claim, sink, now := harness.claim, harness.sink, harness.now
	collectedAt := now
	fixtures := defaultGitHubPullRequestFixtures()
	firstBatch, err := (GitHubPullRequestRouteHandler{}).Collect(
		ctx, claim, providerfoundation.Credential{},
		gitHubPullRequestClient(t, &gitHubPullRequestDoer{t: t, bodies: fixtures}, "https://api.github.com"),
		collectedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	effect := firstBatch.Effects[0]
	var row pullRequestRow
	if err := json.Unmarshal(effect.Rows[0], &row); err != nil {
		t.Fatal(err)
	}

	state, err := NewEffectLedgerState(claim, []EffectBatch{effect}, collectedAt)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := harness.repository.PrepareEffects(ctx, claim, state, collectedAt); err != nil {
		t.Fatal(err)
	}
	if err := harness.repository.BeginEffect(
		ctx, claim, 0, effect.ContentDigest, now.Add(2*time.Second),
	); err != nil {
		t.Fatal(err)
	}
	if err := sink.WriteEffect(ctx, claim, effect); err != nil {
		t.Fatal(err)
	}
	recoveryNow := now.Add(61 * time.Second)
	freshRepository, err := NewPostgresRepository(harness.repository.Pool)
	if err != nil {
		t.Fatal(err)
	}
	recovered, err := freshRepository.Claim(ctx, ClaimRequest{
		UnitID: firstUnitID, OrgID: "org-acme", Owner: uuid.NewString(),
		Now: recoveryNow, LeaseDuration: time.Minute, AllowExpiredRecovery: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	persisted, err := freshRepository.LoadEffects(ctx, recovered, recoveryNow)
	if err != nil {
		t.Fatal(err)
	}
	if !persisted.CreatedAt.UTC().Equal(collectedAt.UTC()) {
		t.Fatalf(
			"persisted ledger CreatedAt=%s want=%s", persisted.CreatedAt, collectedAt,
		)
	}
	recoveredBatch, err := (GitHubPullRequestRouteHandler{}).Collect(
		ctx, recovered, providerfoundation.Credential{},
		gitHubPullRequestClient(t, &gitHubPullRequestDoer{t: t, bodies: fixtures}, "https://api.github.com"),
		persisted.CreatedAt.UTC(),
	)
	if err != nil {
		t.Fatal(err)
	}
	if recoveredBatch.Effects[0].ContentDigest != effect.ContentDigest {
		t.Fatalf(
			"regenerated digest=%s want=%s",
			recoveredBatch.Effects[0].ContentDigest, effect.ContentDigest,
		)
	}
	freshSink := GitHubPullRequestClickHouseEffects{
		Conn: harness.conn, Lease: leaseGuardAt(freshRepository, recovered, recoveryNow),
	}
	result, err := (EffectCommitter{
		Ledger: freshRepository, Sink: freshSink, Readback: freshSink,
		Now: func() time.Time { return recoveryNow },
	}).Commit(ctx, recovered, recoveredBatch.Effects, persisted.CreatedAt.UTC())
	if err != nil || result.MarkedCommitted != 1 || result.Written != 0 {
		t.Fatalf("result=%+v error=%v", result, err)
	}
	assertPullRequestVersionCount(t, ctx, harness, row.RepoID, row.Number, 1)
}

func assertPullRequestVersionCount(
	t *testing.T,
	ctx context.Context,
	harness *pullRequestReadbackHarness,
	repoID string,
	number int,
	want uint64,
) {
	t.Helper()
	var rows uint64
	if err := harness.conn.QueryRow(ctx, `
SELECT count() FROM git_pull_requests
WHERE org_id = 'org-acme' AND repo_id = ? AND number = ?`,
		repoID, number,
	).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != want {
		t.Fatalf("physical git_pull_requests versions=%d want %d", rows, want)
	}
}
