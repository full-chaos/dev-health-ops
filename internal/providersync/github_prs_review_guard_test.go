package providersync

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

func pullRequestReviewGuardStored(row pullRequestRow, prior storedPullRequestReviews) map[pullRequestMergedAtGuardKey]storedPullRequestReviews {
	return map[pullRequestMergedAtGuardKey]storedPullRequestReviews{
		{RepoID: row.RepoID, Number: row.Number}: prior,
	}
}

func pullRequestReviewGuardFailedRow(now time.Time) pullRequestRow {
	row := pullRequestReadbackFixture(now)
	row.FirstReviewAt, row.ReviewsCount, row.ChangesRequestedCount = nil, 0, 0
	row.ReviewsLookupFailed = true
	return row
}

func TestGuardPullRequestReviewRegressionsCarriesColumnsForwardOnFailure(t *testing.T) {
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	priorFirst := now.Add(-3 * time.Hour)
	row := pullRequestReviewGuardFailedRow(now)
	freshTitle := "Retitled while reviews were unavailable"
	row.Title = &freshTitle
	rows := []pullRequestRow{row}

	carried := guardPullRequestReviewRegressions(rows, pullRequestReviewGuardStored(row, storedPullRequestReviews{
		FirstReviewAt: &priorFirst, ReviewsCount: 4, ChangesRequestedCount: 2,
	}))

	if rows[0].FirstReviewAt == nil || !rows[0].FirstReviewAt.Equal(priorFirst) ||
		rows[0].ReviewsCount != 4 || rows[0].ChangesRequestedCount != 2 {
		t.Fatalf("row=%+v want stored review columns (first=%v reviews=4 changes=2)", rows[0], priorFirst)
	}
	if rows[0].Title == nil || *rows[0].Title != freshTitle || rows[0].MergedAt == nil {
		t.Fatalf("row=%+v: the review guard must not touch other columns", rows[0])
	}
	if len(carried) != 1 || carried[0].RepoID != row.RepoID || carried[0].Number != row.Number {
		t.Fatalf("carried=%+v want exactly one event for (%s,%d)", carried, row.RepoID, row.Number)
	}
}

func TestGuardPullRequestReviewRegressionsCarriesEachNonEmptyColumnAlone(t *testing.T) {
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	priorFirst := now.Add(-3 * time.Hour)
	for name, prior := range map[string]storedPullRequestReviews{
		"first_review_at":         {FirstReviewAt: &priorFirst},
		"reviews_count":           {ReviewsCount: 3},
		"changes_requested_count": {ChangesRequestedCount: 1},
	} {
		row := pullRequestReviewGuardFailedRow(now)
		rows := []pullRequestRow{row}
		carried := guardPullRequestReviewRegressions(rows, pullRequestReviewGuardStored(row, prior))
		if len(carried) != 1 || !timePointersEqual(rows[0].FirstReviewAt, prior.FirstReviewAt) ||
			rows[0].ReviewsCount != prior.ReviewsCount || rows[0].ChangesRequestedCount != prior.ChangesRequestedCount {
			t.Fatalf("%s: row=%+v carried=%d want the stored triple carried", name, rows[0], len(carried))
		}
	}
}

func TestGuardPullRequestReviewRegressionsKeepsSuccessfulEnrichment(t *testing.T) {
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	priorFirst := now.Add(-3 * time.Hour)
	row := pullRequestReviewGuardFailedRow(now)
	row.ReviewsLookupFailed = false
	rows := []pullRequestRow{row}

	carried := guardPullRequestReviewRegressions(rows, pullRequestReviewGuardStored(row, storedPullRequestReviews{
		FirstReviewAt: &priorFirst, ReviewsCount: 4, ChangesRequestedCount: 2,
	}))

	if rows[0].FirstReviewAt != nil || rows[0].ReviewsCount != 0 || rows[0].ChangesRequestedCount != 0 || len(carried) != 0 {
		t.Fatalf("row=%+v carried=%d: a successful enrichment must write what it found", rows[0], len(carried))
	}
}

func TestGuardPullRequestReviewRegressionsSkipsBrandNewAndEmptyStored(t *testing.T) {
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	row := pullRequestReviewGuardFailedRow(now)

	rows := []pullRequestRow{row}
	if carried := guardPullRequestReviewRegressions(rows, map[pullRequestMergedAtGuardKey]storedPullRequestReviews{}); len(carried) != 0 || rows[0].ReviewsCount != 0 {
		t.Fatalf("brand-new key: row=%+v carried=%d", rows[0], len(carried))
	}
	rows = []pullRequestRow{row}
	if carried := guardPullRequestReviewRegressions(rows, pullRequestReviewGuardStored(row, storedPullRequestReviews{})); len(carried) != 0 {
		t.Fatalf("empty stored review columns: carried=%d want 0", len(carried))
	}
}

func TestGuardPullRequestReviewRegressionsIsScopedPerKey(t *testing.T) {
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	row := pullRequestReviewGuardFailedRow(now)
	other := row
	other.Number = row.Number + 1
	rows := []pullRequestRow{row, other}

	carried := guardPullRequestReviewRegressions(rows, pullRequestReviewGuardStored(row, storedPullRequestReviews{ReviewsCount: 5}))

	if rows[0].ReviewsCount != 5 || rows[1].ReviewsCount != 0 || len(carried) != 1 {
		t.Fatalf("rows=%+v carried=%d: stored values must stay on their own key", rows, len(carried))
	}
}

// pullRequestGuardReadFailsConn fails only the first Query (the guard read)
// and serves an empty result to every later one.
type pullRequestGuardReadFailsConn struct {
	driver.Conn
	queries int
}

func (conn *pullRequestGuardReadFailsConn) Query(context.Context, string, ...any) (driver.Rows, error) {
	conn.queries++
	if conn.queries == 1 {
		return nil, errPullRequestGuardReadFailed
	}
	return emptyDeploymentEffectsRows{}, nil
}

var errPullRequestGuardReadFailed = errors.New("guard read failed")

func TestGitHubPullRequestInspectEffectFailsWhenGuardReadFails(t *testing.T) {
	claim := nativeTestClaim("github", "prs")
	row := pullRequestReadbackFixture(time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC))
	row.OrgID = claim.OrgID
	effect, err := effectBatchFromValues("git_pull_requests", EffectReadbackRequired, []pullRequestRow{row})
	if err != nil {
		t.Fatal(err)
	}
	conn := &pullRequestGuardReadFailsConn{}
	sink := GitHubPullRequestClickHouseEffects{
		Conn: conn, Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
	}
	inspection, err := sink.InspectEffect(context.Background(), claim, effect)
	if !errors.Is(err, errPullRequestGuardReadFailed) || inspection != EffectConflict || conn.queries != 1 {
		t.Fatalf("inspection=%s err=%v queries=%d want the guard read error, %s, 1 query", inspection, err, conn.queries, EffectConflict)
	}
}
