//go:build integration

package providersync

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// TestGitHubDeploymentsCarryPullRequestForwardFromRouteToClickHouse runs the
// deployments unit the way a sync does: collect from the provider, write the
// effect, re-sync while the per-SHA pull request lookup fails, then recover
// by collecting the same failing pass again and inspecting it.
func TestGitHubDeploymentsCarryPullRequestForwardFromRouteToClickHouse(t *testing.T) {
	ctx, conn := newDeploymentsIntegrationConn(t)
	lease := providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })
	sink := GitHubDeploymentsClickHouseEffects{Conn: conn, Lease: lease}
	claim := nativeTestClaim("github", "deployments")
	doer := &githubDeploymentPullLookupDoer{pullStatus: http.StatusOK, pullBody: `[{"number":5,"merged_at":"2026-07-22T09:00:00Z","merge_commit_sha":"abc123"}]`}
	collect := func(at time.Time) EffectBatch {
		t.Helper()
		batch, err := (GitHubDeploymentsRouteHandler{}).Collect(ctx, claim, providerfoundation.Credential{}, gitHubRepositoryClient(t, doer, "https://api.github.com"), at)
		if err != nil {
			t.Fatal(err)
		}
		if len(batch.Effects) != 1 || len(batch.Effects[0].Rows) != 1 {
			t.Fatalf("effects=%+v", batch.Effects)
		}
		return batch.Effects[0]
	}
	first := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	if err := sink.WriteEffect(ctx, claim, collect(first)); err != nil {
		t.Fatal(err)
	}

	doer.pullStatus, doer.pullBody = http.StatusNotFound, `{"message":"Not Found"}`
	second := first.Add(time.Hour)
	if err := sink.WriteEffect(ctx, claim, collect(second)); err != nil {
		t.Fatal(err)
	}
	stored := deploymentRow{OrgID: claim.OrgID, RepoID: "c7198fbc-1945-3717-05d8-eb78866b4e79", DeploymentID: "821"}
	mergedAt, number, _ := readDeploymentPullRequestPair(t, ctx, conn, stored)
	if mergedAt == nil || !mergedAt.Equal(time.Date(2026, 7, 22, 9, 0, 0, 0, time.UTC)) || number == nil || *number != 5 {
		t.Fatalf("merged_at=%v pull_request_number=%v want the first sync's pull request 5 carried forward", mergedAt, number)
	}
	assertDeploymentInspection(t, ctx, sink, claim, collect(second), EffectExact)
}

// TestGitHubPullRequestSocialCarriesReviewsForwardFromRouteToClickHouse runs
// the PR-social unit the same way: a reviewed sync, a re-sync whose review
// enrichment fails, then recovery of that failing pass.
func TestGitHubPullRequestSocialCarriesReviewsForwardFromRouteToClickHouse(t *testing.T) {
	ctx, conn := newDeploymentsIntegrationConn(t)
	lease := providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })
	sink := GitHubPullRequestSocialClickHouseEffects{Conn: conn, Lease: lease}
	claim := nativeTestClaim("github", "pr-reviews")
	doer := &gitHubPullRequestReviewRouteDoer{
		t: t, restBodies: defaultGitHubPullRequestFixtures(),
		graphQLReply: `{"data":{"repository":{"pr0":{"number":42,"reviews":{"nodes":[` +
			`{"databaseId":7001,"state":"APPROVED","submittedAt":"2026-07-11T10:30:00Z","author":{"login":"octocat"}},` +
			`{"id":"R_changes","state":"CHANGES_REQUESTED","submittedAt":"2026-07-10T11:00:00Z","author":{"login":"hubot"}}` +
			`],"pageInfo":{"hasNextPage":false,"endCursor":""}}}}}}`,
	}
	collect := func(at time.Time) []EffectBatch {
		t.Helper()
		batch, err := (GitHubPullRequestReviewRouteHandler{}).Collect(ctx, claim, providerfoundation.Credential{}, gitHubPullRequestClient(t, doer, "https://api.github.com"), at)
		if err != nil {
			t.Fatal(err)
		}
		if len(batch.Effects) != 2 || batch.Effects[0].Destination != "git_pull_requests" {
			t.Fatalf("effects=%+v", batch.Effects)
		}
		return batch.Effects
	}
	write := func(effects []EffectBatch) {
		t.Helper()
		for _, effect := range effects {
			if err := sink.WriteEffect(ctx, claim, effect); err != nil {
				t.Fatal(err)
			}
		}
	}
	first := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	write(collect(first))

	doer.graphQLStatus, doer.graphQLReply = http.StatusServiceUnavailable, `{"message":"temporarily unavailable"}`
	second := first.Add(time.Hour)
	write(collect(second))

	pulls := GitHubPullRequestClickHouseEffects{Conn: conn, Lease: lease}
	winner, err := pulls.scanWinningPullRequestVersion(ctx, claim.OrgID, "c7198fbc-1945-3717-05d8-eb78866b4e79", 42)
	if err != nil {
		t.Fatal(err)
	}
	wantFirst := time.Date(2026, 7, 10, 11, 0, 0, 0, time.UTC)
	if !winner.Found || winner.Row.FirstReviewAt == nil || !winner.Row.FirstReviewAt.Equal(wantFirst) ||
		winner.Row.ReviewsCount != 2 || winner.Row.ChangesRequestedCount != 1 || !winner.LastSynced.Equal(second) {
		t.Fatalf("winner=%+v last_synced=%s want the first sync's review columns on the second sync's row", winner.Row, winner.LastSynced)
	}
	recovered := collect(second)
	inspection, err := sink.InspectEffect(ctx, claim, recovered[0])
	if err != nil || inspection != EffectExact {
		t.Fatalf("recovery inspection=%s err=%v want=%s", inspection, err, EffectExact)
	}
}

// carryForwardLedgerHarness is a real Postgres effect ledger plus a migrated
// ClickHouse, with the fixture unit set to the dataset under test.
type carryForwardLedgerHarness struct {
	conn       driver.Conn
	repository *PostgresRepository
	claim      Claim
	now        time.Time
}

func startCarryForwardLedgerHarness(t *testing.T, ctx context.Context, dataset, flags string) *carryForwardLedgerHarness {
	t.Helper()
	return startProviderLedgerHarness(t, ctx, "github", dataset, flags)
}

// startProviderLedgerHarness points the fixture unit at provider/dataset; a
// gitlab unit's source carries the numeric project id the GitLab routes read.
func startProviderLedgerHarness(t *testing.T, ctx context.Context, provider, dataset, flags string) *carryForwardLedgerHarness {
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
	chschema.Apply(ctx, t, clickhouseInstance)
	pool, err := pgxpool.New(ctx, postgres.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	createProviderSyncFixture(t, ctx, pool)
	seedProviderSyncFixture(t, ctx, pool)
	if _, err := pool.Exec(ctx, `
UPDATE public.sync_run_units
SET provider = $4, dataset_key = $2, cost_class = 'medium', processor_flags = $3::jsonb,
    since_at = '2026-07-01T00:00:00Z', before_at = '2026-07-31T23:59:59Z'
WHERE id = $1`, firstUnitID, dataset, flags, provider); err != nil {
		t.Fatal(err)
	}
	if provider == "gitlab" {
		if _, err := pool.Exec(ctx, `UPDATE public.integration_sources SET external_id = '123' WHERE id = $1`, firstSourceID); err != nil {
			t.Fatal(err)
		}
	}
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(clickhouseInstance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
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
	return &carryForwardLedgerHarness{conn: conn, repository: repository, claim: claim, now: now}
}

// crashAfterWriteThenRecover prepares the ledger for `effect`, marks it
// writing, lets the sink write it, abandons the lease before CommitEffect,
// then re-claims the unit and commits the same effect bytes the way a
// retried unit does.
func (harness *carryForwardLedgerHarness) crashAfterWriteThenRecover(
	t *testing.T, ctx context.Context, effect EffectBatch,
	sinkFor func(*PostgresRepository, Claim, time.Time) EffectSink,
	readbackFor func(*PostgresRepository, Claim, time.Time) EffectReadback,
) (EffectCommitResult, error) {
	t.Helper()
	claim, now := harness.claim, harness.now
	state, err := NewEffectLedgerState(claim, []EffectBatch{effect}, now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := harness.repository.PrepareEffects(ctx, claim, state, now); err != nil {
		t.Fatal(err)
	}
	if err := harness.repository.BeginEffect(ctx, claim, 0, effect.ContentDigest, now.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := sinkFor(harness.repository, claim, now).WriteEffect(ctx, claim, effect); err != nil {
		t.Fatal(err)
	}
	recoveryNow := now.Add(61 * time.Second)
	fresh, err := NewPostgresRepository(harness.repository.Pool)
	if err != nil {
		t.Fatal(err)
	}
	recovered, err := fresh.Claim(ctx, ClaimRequest{
		UnitID: firstUnitID, OrgID: "org-acme", Owner: uuid.NewString(),
		Now: recoveryNow, LeaseDuration: time.Minute, AllowExpiredRecovery: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	persisted, err := fresh.LoadEffects(ctx, recovered, recoveryNow)
	if err != nil {
		t.Fatal(err)
	}
	return (EffectCommitter{
		Ledger: fresh, Sink: sinkFor(fresh, recovered, recoveryNow), Readback: readbackFor(fresh, recovered, recoveryNow),
		Now: func() time.Time { return recoveryNow },
	}).Commit(ctx, recovered, []EffectBatch{effect}, persisted.CreatedAt.UTC())
}

// TestCarryForwardCrashWindowRecoversThroughTheEffectLedger drives each
// guarded write through the real effect ledger: the sink carries stored
// values into a row whose lookup failed, the process dies before
// CommitEffect, and the retried unit must mark the effect committed from
// the readback -- not report it ambiguous and not write a second version.
func TestCarryForwardCrashWindowRecoversThroughTheEffectLedger(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	// Registered before the merge-freeze cleanups so it runs after them.
	t.Cleanup(cancel)

	pullRequestSink := func(repository *PostgresRepository, claim Claim, at time.Time) GitHubPullRequestClickHouseEffects {
		return GitHubPullRequestClickHouseEffects{Lease: leaseGuardAt(repository, claim, at)}
	}
	for _, variant := range []string{"merged_at", "reviews"} {
		harness := startCarryForwardLedgerHarness(t, ctx, "prs", `{"sync_prs": true}`)
		// Two physical versions are counted below; a background merge must
		// not collapse them first.
		if err := harness.conn.Exec(ctx, `SYSTEM STOP MERGES git_pull_requests`); err != nil {
			t.Fatal(err)
		}
		prior := pullRequestReadbackFixture(harness.now.Add(-time.Hour))
		prior.OrgID = harness.claim.OrgID
		seed := pullRequestSink(harness.repository, harness.claim, harness.now)
		seed.Conn = harness.conn
		if err := seed.WriteEffect(ctx, harness.claim, pullRequestEffect(t, prior)); err != nil {
			t.Fatal(err)
		}
		failed := prior
		failed.LastSynced = harness.now
		if variant == "merged_at" {
			failed.MergedAt = nil
		} else {
			failed.FirstReviewAt, failed.ReviewsCount, failed.ChangesRequestedCount = nil, 0, 0
			failed.ReviewsLookupFailed = true
		}
		result, err := harness.crashAfterWriteThenRecover(t, ctx, pullRequestEffect(t, failed),
			func(repository *PostgresRepository, claim Claim, at time.Time) EffectSink {
				sink := pullRequestSink(repository, claim, at)
				sink.Conn = harness.conn
				return sink
			},
			func(repository *PostgresRepository, claim Claim, at time.Time) EffectReadback {
				sink := pullRequestSink(repository, claim, at)
				sink.Conn = harness.conn
				return sink
			},
		)
		if err != nil || result.MarkedCommitted != 1 || result.Written != 0 {
			t.Fatalf("%s: recovery result=%+v err=%v want MarkedCommitted=1 Written=0", variant, result, err)
		}
		var versions uint64
		if err := harness.conn.QueryRow(ctx, `SELECT count() FROM git_pull_requests WHERE org_id = ? AND repo_id = ? AND number = ?`,
			prior.OrgID, prior.RepoID, prior.Number).Scan(&versions); err != nil {
			t.Fatal(err)
		}
		if versions != 2 {
			t.Fatalf("%s: physical versions=%d want 2 (the prior sync and the one guarded write)", variant, versions)
		}
	}

	harness := startCarryForwardLedgerHarness(t, ctx, "deployments", `{"sync_deployments": true}`)
	freezeDeploymentMerges(t, ctx, harness.conn)
	deploymentSink := func(repository *PostgresRepository, claim Claim, at time.Time) GitHubDeploymentsClickHouseEffects {
		return GitHubDeploymentsClickHouseEffects{Conn: harness.conn, Lease: leaseGuardAt(repository, claim, at)}
	}
	prior := deploymentIntegrationFullRow(harness.claim, "ledger-recovery", harness.now.Add(-time.Hour), 42)
	if err := deploymentSink(harness.repository, harness.claim, harness.now).WriteEffect(ctx, harness.claim, deploymentEffect(t, prior)); err != nil {
		t.Fatal(err)
	}
	failed := prior
	failed.LastSynced = harness.now
	failed.MergedAt, failed.PullRequestNumber = nil, nil
	failed.PullRequestLookupFailed = true
	result, err := harness.crashAfterWriteThenRecover(t, ctx, deploymentEffect(t, failed),
		func(repository *PostgresRepository, claim Claim, at time.Time) EffectSink {
			return deploymentSink(repository, claim, at)
		},
		func(repository *PostgresRepository, claim Claim, at time.Time) EffectReadback {
			return deploymentSink(repository, claim, at)
		},
	)
	if err != nil || result.MarkedCommitted != 1 || result.Written != 0 {
		t.Fatalf("deployments: recovery result=%+v err=%v want MarkedCommitted=1 Written=0", result, err)
	}
	assertDeploymentPhysicalCount(t, ctx, harness.conn, failed, 2)
}
