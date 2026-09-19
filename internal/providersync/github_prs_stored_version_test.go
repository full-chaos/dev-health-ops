package providersync

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/storedversion"
)

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

// Every column the pull request contract keeps is written back into the
// typed row, and the typed row's values round-trip through the contract.
func TestPullRequestContractWritesEveryKeptColumnBack(t *testing.T) {
	row := pullRequestReadbackFixture(time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC))
	held := time.Date(2026, 6, 1, 9, 0, 0, 0, time.UTC)
	positions, err := storedversion.Positions(pullRequestInsert)
	if err != nil {
		t.Fatal(err)
	}
	for _, column := range pullRequestContract.Kept() {
		value := any(held)
		if column == "changes_requested_count" || column == "reviews_count" {
			value = uint32(7)
		}
		if err := setPullRequestKept(&row, column, value); err != nil {
			t.Fatalf("%s: %v", column, err)
		}
		got := pullRequestValues(row)[positions[column]]
		want := value
		if count, ok := value.(uint32); ok {
			want = int(count)
		}
		if got != want {
			t.Errorf("%s = %#v after write-back, want %#v", column, got, want)
		}
		if err := setPullRequestKept(&row, column, nil); column != "changes_requested_count" && column != "reviews_count" && err != nil {
			t.Errorf("%s: a held null: %v", column, err)
		}
	}
	if err := setPullRequestKept(&row, "title", "t"); err == nil {
		t.Error("a column the contract does not keep was accepted")
	}
}

// A failed review enrichment leaves the review-derived columns unstated;
// first_comment_at is never produced and is always kept.
func TestPullRequestCarryFollowsTheReviewLookup(t *testing.T) {
	failed := pullRequestCarry(pullRequestRow{ReviewsLookupFailed: true})
	succeeded := pullRequestCarry(pullRequestRow{})
	for _, column := range []string{"first_review_at", "changes_requested_count", "reviews_count"} {
		if !failed[column] || succeeded[column] {
			t.Errorf("%s: carried=%v on a failed lookup, %v on a successful one", column, failed[column], succeeded[column])
		}
	}
	if !failed["first_comment_at"] || !succeeded["first_comment_at"] {
		t.Error("first_comment_at is not always kept")
	}
	if failed["merged_at"] || succeeded["title"] {
		t.Error("a stated column is carried")
	}
}

// A failed read of the held versions fails WriteEffect before any insert.
func TestGitHubPullRequestWriteEffectFailsWhenTheHeldVersionReadFails(t *testing.T) {
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
	if err := sink.WriteEffect(context.Background(), claim, effect); !errors.Is(err, errPullRequestGuardReadFailed) || conn.queries != 1 {
		t.Fatalf("err=%v queries=%d want the read error and no insert", err, conn.queries)
	}
}

// The named guard events survive the contract: a refused null merged_at logs
// the merged_at event, an empty review list over a held first review logs the
// review refusal with the three held values, and review columns carried over
// a failed enrichment log the carry event.
func TestPullRequestOutcomesKeepTheNamedGuardEvents(t *testing.T) {
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previous) })
	claim := nativeTestClaim("github", "prs")
	held := time.Date(2026, 6, 1, 9, 0, 0, 0, time.UTC)
	refused := pullRequestRow{RepoID: "r", Number: 1, MergedAt: &held, FirstReviewAt: &held, ReviewsCount: 3, ChangesRequestedCount: 1}
	carried := pullRequestRow{RepoID: "r", Number: 2, FirstReviewAt: &held, ReviewsCount: 2, ReviewsLookupFailed: true}
	logPullRequestOutcomes(context.Background(), claim, []pullRequestRow{refused, carried}, []storedversion.Outcome{
		{Key: []any{"r", 1}, Refused: []string{"merged_at", "first_review_at", "changes_requested_count", "reviews_count"}},
		{Key: []any{"r", 2}, Carried: []string{"first_review_at", "reviews_count", "first_comment_at"}},
	})
	text := logs.String()
	for _, want := range []string{
		pullRequestMergedAtRegressionGuardedEvent,
		pullRequestReviewRegressionRefusedEvent + " org_id=",
		"stored_reviews_count=3", "stored_changes_requested_count=1",
		pullRequestReviewRegressionGuardedEvent,
		storedVersionCarriedEvent,
	} {
		if !strings.Contains(text, want) {
			t.Errorf("log lacks %q:\n%s", want, text)
		}
	}
	if strings.Contains(text, storedVersionRefusedEvent) {
		t.Errorf("a named refusal also logged the generic refusal event:\n%s", text)
	}
}

// A failed read of the held versions fails the work-item write before any
// insert: the connection's PrepareBatch is never reached.
func TestWorkItemsWriteFailsWhenTheHeldVersionReadFails(t *testing.T) {
	conn := &pullRequestGuardReadFailsConn{}
	row := workItemStoredRow{WorkItemID: "gh:acme/api#1", Provider: "github", Assignees: []string{}, Labels: []string{}}
	err := writeWorkItemsKeepingHeldColumns(context.Background(), conn, "org-1", gitHubWorkItemsInsert, []workItemStoredRow{row})
	if !errors.Is(err, errPullRequestGuardReadFailed) || conn.queries != 1 {
		t.Fatalf("err=%v queries=%d want the read error and no insert", err, conn.queries)
	}
}

// The held columns are appended to the Python-parity column list, in order,
// and the contract names every column of the extended insert.
func TestWorkItemsInsertAppendsTheHeldColumns(t *testing.T) {
	positions, err := storedversion.Positions(withHeldWorkItemColumns(gitHubWorkItemsInsert))
	if err != nil {
		t.Fatal(err)
	}
	base, err := storedversion.Positions(gitHubWorkItemsInsert)
	if err != nil {
		t.Fatal(err)
	}
	for i, column := range workItemsHeldColumns {
		if positions[column] != len(base)+i {
			t.Errorf("%s at %d, want %d", column, positions[column], len(base)+i)
		}
	}
	if len(positions) != len(workItemsContract.Columns) {
		t.Errorf("insert has %d columns, contract %d", len(positions), len(workItemsContract.Columns))
	}
}
