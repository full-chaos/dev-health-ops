package providersync

import (
	"testing"
	"time"
)

// TestGuardPullRequestMergedAtRegressionsRefusesNullOverPopulated constructs
// the exact regression this guard exists to prevent: a batch row arrives
// with merged_at NULL for a key whose currently winning physical row already
// has a populated merged_at. The guard must not let that row carry a null
// forward -- it must carry the stored value into the row instead, and must
// report exactly one guarded event naming that row's own key and the value
// it preserved.
func TestGuardPullRequestMergedAtRegressionsRefusesNullOverPopulated(t *testing.T) {
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	priorMergedAt := now.Add(-2 * time.Hour)

	row := pullRequestReadbackFixture(now)
	row.MergedAt = nil // the incoming regression
	rows := []pullRequestRow{row}

	stored := map[pullRequestMergedAtGuardKey]*time.Time{
		{RepoID: row.RepoID, Number: row.Number}: &priorMergedAt,
	}

	guarded := guardPullRequestMergedAtRegressions(rows, stored)

	if rows[0].MergedAt == nil {
		t.Fatal("guarded row still has a nil merged_at; the regression landed")
	}
	if !rows[0].MergedAt.UTC().Equal(priorMergedAt.UTC()) {
		t.Fatalf("guarded row merged_at=%s want=%s", rows[0].MergedAt, priorMergedAt)
	}
	if len(guarded) != 1 {
		t.Fatalf("guarded events=%d want 1", len(guarded))
	}
	event := guarded[0]
	if event.RepoID != row.RepoID || event.Number != row.Number {
		t.Fatalf("guarded event key=(%s,%d) want=(%s,%d)",
			event.RepoID, event.Number, row.RepoID, row.Number)
	}
	if !event.StoredMergedAt.UTC().Equal(priorMergedAt.UTC()) {
		t.Fatalf("guarded event StoredMergedAt=%s want=%s", event.StoredMergedAt, priorMergedAt)
	}
}

// TestGuardPullRequestMergedAtRegressionsLeavesEveryOtherColumnUntouched
// proves the guard corrects ONLY merged_at: a row with a title change AND a
// regressing merged_at must still carry the fresh title through, because
// ReplacingMergeTree writes a whole row and this guard's whole point is to
// let every other column land fresh while refusing just the one field.
func TestGuardPullRequestMergedAtRegressionsLeavesEveryOtherColumnUntouched(t *testing.T) {
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	priorMergedAt := now.Add(-2 * time.Hour)

	row := pullRequestReadbackFixture(now)
	row.MergedAt = nil
	freshTitle := "Retitled after merge"
	row.Title = &freshTitle
	rows := []pullRequestRow{row}

	stored := map[pullRequestMergedAtGuardKey]*time.Time{
		{RepoID: row.RepoID, Number: row.Number}: &priorMergedAt,
	}
	guardPullRequestMergedAtRegressions(rows, stored)

	if rows[0].Title == nil || *rows[0].Title != freshTitle {
		t.Fatalf("Title=%v want=%q: the guard must not touch other columns", rows[0].Title, freshTitle)
	}
}

// TestGuardPullRequestMergedAtRegressionsAllowsNewPullRequest is the
// no-prior-row case: a key absent from `stored` (a pull request never
// synced before) has nothing to protect, so a null merged_at is simply the
// normal not-yet-merged state and must pass through untouched with no event.
func TestGuardPullRequestMergedAtRegressionsAllowsNewPullRequest(t *testing.T) {
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	row := pullRequestReadbackFixture(now)
	row.MergedAt = nil
	rows := []pullRequestRow{row}

	guarded := guardPullRequestMergedAtRegressions(rows, map[pullRequestMergedAtGuardKey]*time.Time{})

	if rows[0].MergedAt != nil {
		t.Fatalf("MergedAt=%v want nil: a brand-new open PR must stay unmerged", rows[0].MergedAt)
	}
	if len(guarded) != 0 {
		t.Fatalf("guarded events=%d want 0", len(guarded))
	}
}

// TestGuardPullRequestMergedAtRegressionsAllowsStillUnmergedHistory covers a
// key that IS present in `stored` but whose prior winning value was itself
// nil (an open PR synced before, still open): a null-over-null is not a
// regression and must pass through untouched.
func TestGuardPullRequestMergedAtRegressionsAllowsStillUnmergedHistory(t *testing.T) {
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	row := pullRequestReadbackFixture(now)
	row.MergedAt = nil
	rows := []pullRequestRow{row}

	stored := map[pullRequestMergedAtGuardKey]*time.Time{
		{RepoID: row.RepoID, Number: row.Number}: nil,
	}
	guarded := guardPullRequestMergedAtRegressions(rows, stored)

	if rows[0].MergedAt != nil {
		t.Fatalf("MergedAt=%v want nil: still-open history is not a regression", rows[0].MergedAt)
	}
	if len(guarded) != 0 {
		t.Fatalf("guarded events=%d want 0", len(guarded))
	}
}

// TestGuardPullRequestMergedAtRegressionsAllowsPopulatedIncoming is the
// non-null-incoming case: this guard's scope is ONLY a null arriving over a
// populated value. A populated incoming merged_at (even one that differs
// from the stored value) is a different concern entirely and must never be
// touched here.
func TestGuardPullRequestMergedAtRegressionsAllowsPopulatedIncoming(t *testing.T) {
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	priorMergedAt := now.Add(-2 * time.Hour)
	row := pullRequestReadbackFixture(now) // MergedAt populated by the fixture
	incomingMergedAt := *row.MergedAt
	rows := []pullRequestRow{row}

	stored := map[pullRequestMergedAtGuardKey]*time.Time{
		{RepoID: row.RepoID, Number: row.Number}: &priorMergedAt,
	}
	guarded := guardPullRequestMergedAtRegressions(rows, stored)

	if rows[0].MergedAt == nil || !rows[0].MergedAt.UTC().Equal(incomingMergedAt.UTC()) {
		t.Fatalf("MergedAt=%v want=%s: a populated incoming value must pass through as-is",
			rows[0].MergedAt, incomingMergedAt)
	}
	if len(guarded) != 0 {
		t.Fatalf("guarded events=%d want 0", len(guarded))
	}
}

// TestGuardPullRequestMergedAtRegressionsIsScopedPerRepo proves the guard
// key is (repo_id, number), not number alone: two different repos can
// legitimately use the same PR number, and a stored value for one repo must
// never leak into the decision for the other repo's row with no prior
// history.
func TestGuardPullRequestMergedAtRegressionsIsScopedPerRepo(t *testing.T) {
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	priorMergedAt := now.Add(-2 * time.Hour)

	otherRepoRow := pullRequestReadbackFixture(now)
	otherRepoRow.RepoID = "f1c5b6e2-30ab-4a3b-9d7a-2d9a6a9c5b90"
	otherRepoRow.MergedAt = nil
	rows := []pullRequestRow{otherRepoRow}

	sameNumberDifferentRepo := pullRequestReadbackFixture(now)
	stored := map[pullRequestMergedAtGuardKey]*time.Time{
		{RepoID: sameNumberDifferentRepo.RepoID, Number: sameNumberDifferentRepo.Number}: &priorMergedAt,
	}
	guarded := guardPullRequestMergedAtRegressions(rows, stored)

	if rows[0].MergedAt != nil {
		t.Fatalf(
			"MergedAt=%v want nil: a different repo's stored value must not cross-apply",
			rows[0].MergedAt,
		)
	}
	if len(guarded) != 0 {
		t.Fatalf("guarded events=%d want 0", len(guarded))
	}
}

// TestGuardPullRequestMergedAtRegressionsHandlesMixedBatch is the batch
// shape this guard is actually optimized for: several rows in one call,
// only some of which regress. Every non-regressing row must be left exactly
// as it arrived.
func TestGuardPullRequestMergedAtRegressionsHandlesMixedBatch(t *testing.T) {
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	priorMergedAt := now.Add(-2 * time.Hour)

	regressing := pullRequestReadbackFixture(now)
	regressing.Number = 1
	regressing.MergedAt = nil

	stillOpen := pullRequestReadbackFixture(now)
	stillOpen.Number = 2
	stillOpen.State = "open"
	stillOpen.MergedAt = nil

	freshlyMerged := pullRequestReadbackFixture(now)
	freshlyMerged.Number = 3

	rows := []pullRequestRow{regressing, stillOpen, freshlyMerged}
	stored := map[pullRequestMergedAtGuardKey]*time.Time{
		{RepoID: regressing.RepoID, Number: 1}: &priorMergedAt,
		{RepoID: stillOpen.RepoID, Number: 2}:  nil,
		// number 3 absent: brand new merged PR, nothing stored yet.
	}
	guarded := guardPullRequestMergedAtRegressions(rows, stored)

	if rows[0].MergedAt == nil || !rows[0].MergedAt.UTC().Equal(priorMergedAt.UTC()) {
		t.Fatalf("row 1 MergedAt=%v want=%s", rows[0].MergedAt, priorMergedAt)
	}
	if rows[1].MergedAt != nil {
		t.Fatalf("row 2 MergedAt=%v want nil", rows[1].MergedAt)
	}
	if rows[2].MergedAt == nil {
		t.Fatal("row 3 MergedAt is nil, want the fixture's populated value")
	}
	if len(guarded) != 1 || guarded[0].Number != 1 {
		t.Fatalf("guarded=%+v want exactly one event for number=1", guarded)
	}
}
