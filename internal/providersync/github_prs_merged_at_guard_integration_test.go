//go:build integration

package providersync

import (
	"context"
	"testing"
	"time"
)

// TestGitHubPullRequestWriteEffectRefusesMergedAtRegression is the
// table-level fence for the write-once guard: the unit tests in
// github_prs_merged_at_guard_test.go prove guardPullRequestMergedAtRegressions
// refuses the regression in isolation, but writePullRequestEffect's real
// path is a read-before-write followed by a ReplacingMergeTree batch insert
// against a live engine -- only a real ClickHouse proves the correction
// this guard makes actually survives that path and wins the FINAL
// point-lookup, not just that the in-memory function returns the right
// value.
func TestGitHubPullRequestWriteEffectRefusesMergedAtRegression(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	harness := startPullRequestReadbackHarness(t, ctx)
	claim, sink, now := harness.claim, harness.sink, harness.now

	// 1. The pull request merges: merged_at populated, this becomes the
	//    winning physical row.
	merged := pullRequestReadbackFixture(now)
	merged.OrgID = claim.OrgID
	if err := sink.WriteEffect(ctx, claim, pullRequestEffect(t, merged)); err != nil {
		t.Fatal(err)
	}

	// 2. A LATER sync occurrence reports the same pull request with
	//    merged_at NULL and a changed title -- the exact regression this
	//    guard exists to catch, arriving through the real batch insert
	//    path rather than a hand-built map.
	regressed := merged
	regressed.LastSynced = now.Add(time.Hour)
	regressed.MergedAt = nil
	freshTitle := "Retitled after merge, before the regressed sync"
	regressed.Title = &freshTitle
	if err := sink.WriteEffect(ctx, claim, pullRequestEffect(t, regressed)); err != nil {
		t.Fatal(err)
	}

	// 3. The winning physical row must still carry the ORIGINAL merged_at
	//    -- not null -- while every other column (title) reflects the
	//    fresh regressed sync. Read directly rather than through
	//    InspectEffect/comparePullRequestVersion, which would report a
	//    conflict against the (uncorrected) `regressed` expectation and
	//    prove nothing about what actually landed.
	winner, err := sink.scanWinningPullRequestVersion(ctx, claim.OrgID, merged.RepoID, merged.Number)
	if err != nil {
		t.Fatal(err)
	}
	if !winner.Found {
		t.Fatal("no winning row found after the regressed write")
	}
	if winner.Row.MergedAt == nil {
		t.Fatal("winning row merged_at is NULL: the regression landed through the real write path")
	}
	if !winner.Row.MergedAt.UTC().Equal(merged.MergedAt.UTC()) {
		t.Fatalf(
			"winning row merged_at=%s want the ORIGINAL merged_at=%s",
			winner.Row.MergedAt, merged.MergedAt,
		)
	}
	if winner.Row.Title == nil || *winner.Row.Title != freshTitle {
		t.Fatalf(
			"winning row title=%v want=%q: every other column must still land fresh",
			winner.Row.Title, freshTitle,
		)
	}
	assertPullRequestVersionCount(t, ctx, harness, merged.RepoID, merged.Number, 2)
}

// TestGitHubPullRequestWriteEffectAllowsGenuineUnmergedHistory is the
// end-to-end negative control: an open pull request synced before any merge
// ever occurred must write through unchanged (merged_at stays NULL), proving
// the guard does not fire on ordinary pre-merge history -- only on a null
// arriving where a populated value already won.
func TestGitHubPullRequestWriteEffectAllowsGenuineUnmergedHistory(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	harness := startPullRequestReadbackHarness(t, ctx)
	claim, sink, now := harness.claim, harness.sink, harness.now

	open := pullRequestReadbackFixture(now)
	open.OrgID = claim.OrgID
	open.State = "open"
	open.MergedAt, open.ClosedAt = nil, nil
	if err := sink.WriteEffect(ctx, claim, pullRequestEffect(t, open)); err != nil {
		t.Fatal(err)
	}

	stillOpen := open
	stillOpen.LastSynced = now.Add(time.Hour)
	if err := sink.WriteEffect(ctx, claim, pullRequestEffect(t, stillOpen)); err != nil {
		t.Fatal(err)
	}

	winner, err := sink.scanWinningPullRequestVersion(ctx, claim.OrgID, open.RepoID, open.Number)
	if err != nil {
		t.Fatal(err)
	}
	if !winner.Found {
		t.Fatal("no winning row found")
	}
	if winner.Row.MergedAt != nil {
		t.Fatalf("winning row merged_at=%s want nil: still-open history is not a regression", winner.Row.MergedAt)
	}
}

// TestGitHubPullRequestInspectEffectMatchesGuardedMergedAtOnRecovery is the
// crash-recovery fence for the merged_at guard: the regressed effect's own
// bytes still carry the pre-guard NULL, while the row the guard wrote
// carries the stored merged_at. InspectEffect must apply the same guard to
// its expected rows and call that write exact; a genuinely different row
// must still read as a conflict.
func TestGitHubPullRequestInspectEffectMatchesGuardedMergedAtOnRecovery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	harness := startPullRequestReadbackHarness(t, ctx)
	claim, sink, now := harness.claim, harness.sink, harness.now

	merged := pullRequestReadbackFixture(now)
	merged.OrgID = claim.OrgID
	if err := sink.WriteEffect(ctx, claim, pullRequestEffect(t, merged)); err != nil {
		t.Fatal(err)
	}
	regressed := merged
	regressed.LastSynced = now.Add(time.Hour)
	regressed.MergedAt = nil
	regressedEffect := pullRequestEffect(t, regressed)
	if err := sink.WriteEffect(ctx, claim, regressedEffect); err != nil {
		t.Fatal(err)
	}

	inspection, err := sink.InspectEffect(ctx, claim, regressedEffect)
	if err != nil || inspection != EffectExact {
		t.Fatalf("recovery inspection=%s err=%v want=%s", inspection, err, EffectExact)
	}

	divergent := regressed
	otherTitle := "A title this effect never wrote"
	divergent.Title = &otherTitle
	inspection, err = sink.InspectEffect(ctx, claim, pullRequestEffect(t, divergent))
	if err != nil || inspection != EffectConflict {
		t.Fatalf("divergent inspection=%s err=%v want=%s", inspection, err, EffectConflict)
	}
}

// TestGitHubPullRequestWriteEffectCarriesReviewsForwardOnFailedEnrichment
// proves a failed review enrichment never replaces a stored review history
// with the base collector's zero values, that every other column still lands
// fresh, and that crash recovery of that same effect reads back exact.
func TestGitHubPullRequestWriteEffectCarriesReviewsForwardOnFailedEnrichment(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	harness := startPullRequestReadbackHarness(t, ctx)
	claim, sink, now := harness.claim, harness.sink, harness.now

	reviewed := pullRequestReadbackFixture(now)
	reviewed.OrgID = claim.OrgID
	if err := sink.WriteEffect(ctx, claim, pullRequestEffect(t, reviewed)); err != nil {
		t.Fatal(err)
	}
	failed := reviewed
	failed.LastSynced = now.Add(time.Hour)
	failed.FirstReviewAt, failed.ReviewsCount, failed.ChangesRequestedCount = nil, 0, 0
	failed.ReviewsLookupFailed = true
	freshTitle := "Retitled while reviews were unavailable"
	failed.Title = &freshTitle
	failedEffect := pullRequestEffect(t, failed)
	if err := sink.WriteEffect(ctx, claim, failedEffect); err != nil {
		t.Fatal(err)
	}

	winner, err := sink.scanWinningPullRequestVersion(ctx, claim.OrgID, reviewed.RepoID, reviewed.Number)
	if err != nil {
		t.Fatal(err)
	}
	if !winner.Found || !timePointersEqual(winner.Row.FirstReviewAt, reviewed.FirstReviewAt) ||
		winner.Row.ReviewsCount != reviewed.ReviewsCount ||
		winner.Row.ChangesRequestedCount != reviewed.ChangesRequestedCount {
		t.Fatalf("winner=%+v want the stored review columns first=%v reviews=%d changes=%d",
			winner.Row, reviewed.FirstReviewAt, reviewed.ReviewsCount, reviewed.ChangesRequestedCount)
	}
	if winner.Row.Title == nil || *winner.Row.Title != freshTitle || !winner.LastSynced.Equal(failed.LastSynced) {
		t.Fatalf("winner=%+v: every other column must land fresh", winner)
	}
	assertPullRequestVersionCount(t, ctx, harness, reviewed.RepoID, reviewed.Number, 2)

	inspection, err := sink.InspectEffect(ctx, claim, failedEffect)
	if err != nil || inspection != EffectExact {
		t.Fatalf("recovery inspection=%s err=%v want=%s", inspection, err, EffectExact)
	}
}

// TestGitHubPullRequestWriteEffectWritesSuccessfulEmptyReviews is the
// negative control: an enrichment that succeeded writes what it found, even
// over a stored review history.
func TestGitHubPullRequestWriteEffectWritesSuccessfulEmptyReviews(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	harness := startPullRequestReadbackHarness(t, ctx)
	claim, sink, now := harness.claim, harness.sink, harness.now

	reviewed := pullRequestReadbackFixture(now)
	reviewed.OrgID = claim.OrgID
	if err := sink.WriteEffect(ctx, claim, pullRequestEffect(t, reviewed)); err != nil {
		t.Fatal(err)
	}
	empty := reviewed
	empty.LastSynced = now.Add(time.Hour)
	empty.FirstReviewAt, empty.ReviewsCount, empty.ChangesRequestedCount = nil, 0, 0
	emptyEffect := pullRequestEffect(t, empty)
	if err := sink.WriteEffect(ctx, claim, emptyEffect); err != nil {
		t.Fatal(err)
	}

	winner, err := sink.scanWinningPullRequestVersion(ctx, claim.OrgID, reviewed.RepoID, reviewed.Number)
	if err != nil {
		t.Fatal(err)
	}
	if !winner.Found || winner.Row.FirstReviewAt != nil || winner.Row.ReviewsCount != 0 || winner.Row.ChangesRequestedCount != 0 {
		t.Fatalf("winner=%+v want the successful enrichment's empty review columns", winner.Row)
	}
	inspection, err := sink.InspectEffect(ctx, claim, emptyEffect)
	if err != nil || inspection != EffectExact {
		t.Fatalf("inspection=%s err=%v want=%s", inspection, err, EffectExact)
	}
}
