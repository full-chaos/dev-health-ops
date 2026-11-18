//go:build integration

package providersync

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// deploymentInterleaveConn runs hook once, after the deployments writer's held-version read and before its insert.
type deploymentInterleaveConn struct {
	driver.Conn
	hook  func()
	fired bool
}

func (c *deploymentInterleaveConn) PrepareBatch(ctx context.Context, query string, opts ...driver.PrepareBatchOption) (driver.Batch, error) {
	if !c.fired && strings.Contains(query, "INSERT INTO deployments") {
		c.fired = true
		c.hook()
	}
	return c.Conn.PrepareBatch(ctx, query, opts...)
}

// CHAOS-6014 C1a, deployments twin: a's per-SHA lookup failed (it carries the held, still empty pull request pair) at
// the newer version; b states the merged pull request at an older version, inside a's window or after a's insert. The
// repair of whichever write lands last keeps b's merged_at, pull_request_number and pull_request_tier. The recovery
// readback of a's effect reads its own repair row as Exact (C8).
func TestDeploymentInterleaveKeepsTheMergedPullRequest(t *testing.T) {
	ctx, conn := newDeploymentsIntegrationConn(t)
	lease := providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })
	now := time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC)
	claim := nativeTestClaim("github", "deployments")
	for _, order := range []string{"interleaved", "after"} {
		t.Run(order, func(t *testing.T) {
			held := deploymentIntegrationFullRow(claim, "interleave-"+order, now, 1)
			held.MergedAt, held.PullRequestNumber, held.PullRequestTier = nil, nil, ""
			sink := GitHubDeploymentsClickHouseEffects{Conn: conn, Lease: lease}
			if err := sink.WriteEffect(ctx, claim, deploymentEffect(t, held)); err != nil {
				t.Fatal(err)
			}
			a := held
			a.PullRequestLookupFailed = true
			a.LastSynced = now.Add(2 * time.Hour)
			b := deploymentIntegrationFullRow(claim, "interleave-"+order, now.Add(time.Hour), 42)
			b.PullRequestTier = pullRequestTierExactMergeSHA
			writeB := func() {
				if err := sink.WriteEffect(ctx, claim, deploymentEffect(t, b)); err != nil {
					t.Fatal(err)
				}
			}
			hooked := &deploymentInterleaveConn{Conn: conn, hook: writeB, fired: order != "interleaved"}
			sinkA := GitHubDeploymentsClickHouseEffects{Conn: hooked, Lease: lease}
			aEffect := deploymentEffect(t, a)
			if err := sinkA.WriteEffect(ctx, claim, aEffect); err != nil {
				t.Fatal(err)
			}
			if order == "after" {
				writeB()
			} else if !hooked.fired {
				t.Fatal("b did not write inside a's window")
			}
			mergedAt, number, _ := readDeploymentPullRequestPair(t, ctx, conn, b)
			if mergedAt == nil || !mergedAt.Equal(*b.MergedAt) || number == nil || *number != 42 {
				t.Fatalf("merged_at=%v pull_request_number=%v; want b's %v/42", mergedAt, number, b.MergedAt)
			}
			if tier := readDeploymentPullRequestTier(t, ctx, conn, b); tier != pullRequestTierExactMergeSHA {
				t.Fatalf("tier=%q; want b's %q", tier, pullRequestTierExactMergeSHA)
			}
			if order == "interleaved" {
				assertDeploymentInspection(t, ctx, sink, claim, aEffect, EffectExact)
			}
		})
	}
}

// CHAOS-6014 C8 and the named residual 1, on the pull request writer.
//   - C8: a's write carries a NULL first review over b's statement and repairs it; a recovery readback of a's effect
//     reads Exact (its own repair row), and after a foreign newer version it reads Conflict.
//   - Residual 1: the held review lead is non-null (FR, 1); b states (FR, 2) at an older version after a's read; a
//     carries (FR, 1) at the newer version. FINAL keeps a's (FR, 1): the repair never replaces a non-null lead.
func TestPullRequestKeptRepairReadbackAndResidual(t *testing.T) {
	t.Run("C8 readback of the writer's own repair", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		harness := startPullRequestReadbackHarness(t, ctx)
		seedOpenPullRequest(t, ctx, harness)
		cases := interleaveCases()
		a, b := cases[0].a(harness.now), cases[0].b(harness.now)
		a.OrgID, b.OrgID = harness.claim.OrgID, harness.claim.OrgID
		hooked := &interleaveConn{Conn: harness.conn, hook: func() {
			if err := harness.sink.WriteEffect(ctx, harness.claim, pullRequestEffect(t, b)); err != nil {
				t.Fatal(err)
			}
		}}
		sinkA := harness.sink
		sinkA.Conn = hooked
		aEffect := pullRequestEffect(t, a)
		if err := sinkA.WriteEffect(ctx, harness.claim, aEffect); err != nil {
			t.Fatal(err)
		}
		if inspection, err := harness.sink.InspectEffect(ctx, harness.claim, aEffect); err != nil || inspection != EffectExact {
			t.Fatalf("readback of a after its repair = %s, %v; want Exact", inspection, err)
		}
		foreign := b
		foreign.LastSynced = harness.now.Add(5 * time.Hour)
		if err := harness.sink.WriteEffect(ctx, harness.claim, pullRequestEffect(t, foreign)); err != nil {
			t.Fatal(err)
		}
		if inspection, err := harness.sink.InspectEffect(ctx, harness.claim, aEffect); err != nil || inspection != EffectConflict {
			t.Fatalf("readback of a after a foreign newer version = %s, %v; want Conflict", inspection, err)
		}
	})

	t.Run("residual 1: a carried non-null lead keeps the older group", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		harness := startPullRequestReadbackHarness(t, ctx)
		held := pullRequestReadbackFixture(harness.now)
		held.OrgID = harness.claim.OrgID
		held.State = "open"
		held.MergedAt, held.ClosedAt = nil, nil
		held.ReviewsCount, held.ChangesRequestedCount = 1, 0
		if err := harness.sink.WriteEffect(ctx, harness.claim, pullRequestEffect(t, held)); err != nil {
			t.Fatal(err)
		}
		a := held
		a.LastSynced = harness.now.Add(2 * time.Hour)
		a.FirstReviewAt, a.ReviewsCount, a.ChangesRequestedCount = nil, 0, 0
		a.ReviewsLookupFailed = true
		b := held
		b.LastSynced = harness.now.Add(time.Hour)
		b.ReviewsCount = 2
		hooked := &interleaveConn{Conn: harness.conn, hook: func() {
			if err := harness.sink.WriteEffect(ctx, harness.claim, pullRequestEffect(t, b)); err != nil {
				t.Fatal(err)
			}
		}}
		sinkA := harness.sink
		sinkA.Conn = hooked
		if err := sinkA.WriteEffect(ctx, harness.claim, pullRequestEffect(t, a)); err != nil {
			t.Fatal(err)
		}
		winner, err := harness.sink.scanWinningPullRequestVersion(ctx, harness.claim.OrgID, a.RepoID, a.Number)
		if err != nil {
			t.Fatal(err)
		}
		if winner.Row.ReviewsCount != 1 || !timePointersEqual(winner.Row.FirstReviewAt, held.FirstReviewAt) {
			t.Fatalf("winner reviews_count=%d first_review_at=%s; the named residual is a's carried (FR, 1)",
				winner.Row.ReviewsCount, fmtTime(winner.Row.FirstReviewAt))
		}
		t.Logf("CELL6014 RESIDUAL case=carry_over_non_null winner.reviews_count=%d (b stated 2)", winner.Row.ReviewsCount)
	})
}
