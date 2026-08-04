//go:build integration

package providersync

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

const gitPullRequestReviewsDDL = `CREATE TABLE git_pull_request_reviews (org_id String, repo_id UUID, number UInt32, review_id String, reviewer String, state String, submitted_at DateTime64(3, 'UTC'), last_synced DateTime64(3, 'UTC'), source_id Nullable(UUID)) ENGINE = ReplacingMergeTree(last_synced) ORDER BY (org_id, repo_id, number, review_id)`

func TestGitHubPullRequestReviewEffectsAreNonEmptyTenantFencedAndLeaseGuarded(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(ctx)
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.Exec(ctx, gitPullRequestReviewsDDL); err != nil {
		t.Fatal(err)
	}
	claim := nativeTestClaim("github", "pr-reviews")
	now := time.Date(2026, 7, 26, 12, 0, 0, 0, time.UTC)
	row := pullRequestReviewRow{
		OrgID: claim.OrgID, RepoID: "c7198fbc-1945-3717-05d8-eb78866b4e79",
		Number: 42, ReviewID: "9007199254740993", Reviewer: "octocat",
		State: "APPROVED", SubmittedAt: now.Add(-time.Hour), LastSynced: now,
	}
	effect, err := effectBatchFromValues(
		"git_pull_request_reviews", EffectReadbackRequired, []pullRequestReviewRow{row},
	)
	if err != nil {
		t.Fatal(err)
	}
	safe := GitHubPullRequestReviewClickHouseEffects{
		Conn: conn, Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
	}
	if inspection, err := safe.InspectEffect(ctx, claim, effect); err != nil || inspection != EffectAbsent {
		t.Fatalf("empty inspection=%s err=%v", inspection, err)
	}

	leaseLost := errors.New("lease lost before send")
	assertions := 0
	guarded := GitHubPullRequestReviewClickHouseEffects{
		Conn: conn, Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error {
			assertions++
			if assertions == 2 {
				return leaseLost
			}
			return nil
		}),
	}
	if err := guarded.WriteEffect(ctx, claim, effect); !errors.Is(err, providerfoundation.ErrLeaseLost) {
		t.Fatalf("write after lost lease err=%v assertions=%d", err, assertions)
	}
	if inspection, err := safe.InspectEffect(ctx, claim, effect); err != nil || inspection != EffectAbsent {
		t.Fatalf("lost-lease write became visible: inspection=%s err=%v", inspection, err)
	}
	if err := safe.WriteEffect(ctx, claim, effect); err != nil {
		t.Fatal(err)
	}
	if inspection, err := safe.InspectEffect(ctx, claim, effect); err != nil || inspection != EffectExact {
		t.Fatalf("persisted inspection=%s err=%v", inspection, err)
	}

	foreign := row
	foreign.OrgID = "other-org"
	foreign.LastSynced = now.Add(time.Minute)
	foreignClaim := claim
	foreignClaim.OrgID = foreign.OrgID
	foreignEffect, err := effectBatchFromValues(
		"git_pull_request_reviews", EffectReadbackRequired, []pullRequestReviewRow{foreign},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := safe.WriteEffect(ctx, foreignClaim, foreignEffect); err != nil {
		t.Fatal(err)
	}
	if inspection, err := safe.InspectEffect(ctx, claim, effect); err != nil || inspection != EffectExact {
		t.Fatalf("foreign newer row crossed tenant fence: inspection=%s err=%v", inspection, err)
	}
}

// TestGitHubPullRequestReviewCompositeCrashRecoveryIsExact proves the D16
// boundary against the real engines. A crash after the enriched complete PR
// write must recover by FINAL readback, never blind replay into a second
// ReplacingMergeTree version.
func TestGitHubPullRequestReviewCompositeCrashRecoveryIsExact(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	harness := startPullRequestReviewCompositeHarness(t, ctx)
	claim, now := harness.claim, harness.now
	normalizedAt := now.Add(123456 * time.Microsecond)
	graphql := `{"data":{"repository":{"pr0":{"number":42,"reviews":{"nodes":[{"id":"review-1","state":"CHANGES_REQUESTED","submittedAt":"2026-07-11T10:30:00Z","author":{"login":"octocat"}}],"pageInfo":{"hasNextPage":false,"endCursor":""}}}}}}`
	collect := func(current Claim, stamp time.Time) CompleteRouteBatch {
		doer := &gitHubPullRequestReviewRouteDoer{
			t: t, restBodies: defaultGitHubPullRequestFixtures(), graphQLReply: graphql,
		}
		batch, err := (GitHubPullRequestReviewRouteHandler{}).Collect(
			ctx, current, providerfoundation.Credential{},
			gitHubPullRequestClient(t, doer, "https://api.github.com"), stamp,
		)
		if err != nil {
			t.Fatal(err)
		}
		return batch
	}
	first := collect(claim, normalizedAt)
	sink := GitHubPullRequestReviewClickHouseEffects{
		Conn: harness.conn, Lease: leaseGuardAt(harness.repository, claim, now),
	}
	crash := errors.New("simulated crash after enriched pull-request write")
	_, err := (EffectCommitter{
		Ledger: harness.repository,
		Sink: crashAfterGitHubPullRequestReviewWrite{
			sink: sink, destination: "git_pull_requests", failure: crash,
		},
		Now: func() time.Time { return now.Add(10 * time.Second) },
	}).Commit(ctx, claim, first.Effects, normalizedAt)
	if !errors.Is(err, crash) {
		t.Fatalf("first commit error=%v", err)
	}
	persisted, err := harness.repository.LoadEffects(ctx, claim, now.Add(11*time.Second))
	if err != nil || len(persisted.Effects) != 2 {
		t.Fatalf("persisted=%+v error=%v", persisted, err)
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
	persisted, err = freshRepository.LoadEffects(ctx, recovered, recoveryNow)
	if err != nil || !persisted.CreatedAt.UTC().Equal(normalizedAt.UTC()) {
		t.Fatalf("reloaded ledger=%+v error=%v", persisted, err)
	}
	rebuilt := collect(recovered, persisted.CreatedAt)
	for index := range first.Effects {
		if rebuilt.Effects[index].Destination != first.Effects[index].Destination ||
			rebuilt.Effects[index].ContentDigest != first.Effects[index].ContentDigest {
			t.Fatalf("rebuilt effect[%d]=%+v want=%+v", index, rebuilt.Effects[index], first.Effects[index])
		}
	}
	freshSink := GitHubPullRequestReviewClickHouseEffects{
		Conn: harness.conn, Lease: leaseGuardAt(freshRepository, recovered, recoveryNow),
	}
	result, err := (EffectCommitter{
		Ledger: freshRepository, Sink: freshSink, Readback: freshSink,
		Now: func() time.Time { return recoveryNow },
	}).Commit(ctx, recovered, rebuilt.Effects, persisted.CreatedAt)
	if err != nil || result != (EffectCommitResult{Skipped: 1, MarkedCommitted: 1}) {
		t.Fatalf("recovery result=%+v error=%v", result, err)
	}
	assertPullRequestVersionCount(t, ctx, harness, "c7198fbc-1945-3717-05d8-eb78866b4e79", 42, 1)
	var reviewRows uint64
	if err := harness.conn.QueryRow(ctx, `SELECT count() FROM git_pull_request_reviews WHERE org_id = ?`, claim.OrgID).Scan(&reviewRows); err != nil {
		t.Fatal(err)
	}
	if reviewRows != 1 {
		t.Fatalf("physical git_pull_request_reviews versions=%d want 1", reviewRows)
	}
}

type crashAfterGitHubPullRequestReviewWrite struct {
	sink        GitHubPullRequestReviewClickHouseEffects
	destination string
	failure     error
}

func (writer crashAfterGitHubPullRequestReviewWrite) WriteEffect(
	ctx context.Context, claim Claim, effect EffectBatch,
) error {
	if err := writer.sink.WriteEffect(ctx, claim, effect); err != nil {
		return err
	}
	if effect.Destination == writer.destination {
		return writer.failure
	}
	return nil
}

func startPullRequestReviewCompositeHarness(
	t *testing.T,
	ctx context.Context,
) *pullRequestReadbackHarness {
	t.Helper()
	postgres, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := postgres.Close(closeCtx); err != nil {
			t.Errorf("terminate PostgreSQL: %v", err)
		}
	})
	pool, err := pgxpool.New(ctx, postgres.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	createProviderSyncFixture(t, ctx, pool)
	seedProviderSyncFixture(t, ctx, pool)
	if _, err := pool.Exec(ctx, `
UPDATE public.sync_run_units
SET provider = 'github', dataset_key = 'pr-reviews',
    cost_class = 'medium', processor_flags = '{"sync_prs": true}',
    since_at = '2026-07-01T00:00:00Z', before_at = '2026-07-31T23:59:59Z'
WHERE id = $1`, firstUnitID); err != nil {
		t.Fatal(err)
	}
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate ClickHouse: %v", err)
		}
	})
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if err := conn.Exec(ctx, gitPullRequestsDDL); err != nil {
		t.Fatal(err)
	}
	if err := conn.Exec(ctx, gitPullRequestReviewsDDL); err != nil {
		t.Fatal(err)
	}
	repository, err := NewPostgresRepository(pool)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	claim, err := repository.Claim(ctx, ClaimRequest{
		UnitID: firstUnitID, OrgID: "org-acme", Owner: uuid.NewString(), Now: now,
		LeaseDuration: time.Minute, AllowExpiredRecovery: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	return &pullRequestReadbackHarness{
		conn: conn, repository: repository, claim: claim, now: now,
		sink: GitHubPullRequestClickHouseEffects{
			Conn: conn, Lease: leaseGuardAt(repository, claim, now),
		},
	}
}
