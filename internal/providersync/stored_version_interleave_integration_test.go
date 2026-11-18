//go:build integration

package providersync

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// interleaveConn runs hook once, after the writer's held-version read and
// before its INSERT batch: the window storedversion.Apply leaves open.
type interleaveConn struct {
	driver.Conn
	hook  func()
	fired bool
}

func (c *interleaveConn) PrepareBatch(ctx context.Context, query string, opts ...driver.PrepareBatchOption) (driver.Batch, error) {
	if !c.fired && strings.Contains(query, "INSERT INTO git_pull_requests") {
		c.fired = true
		c.hook()
	}
	return c.Conn.PrepareBatch(ctx, query, opts...)
}

type interleaveCase struct {
	name string
	// a is the writer whose read-then-insert window is opened; b writes
	// inside it (interleaved) or before a starts (serial).
	a, b  func(now time.Time) pullRequestRow
	check func(t *testing.T, winner pullRequestVersion, b pullRequestRow) string
}

func interleaveCases() []interleaveCase {
	return []interleaveCase{
		{
			// R2 carry: a's review enrichment failed, so a carries the held
			// review columns; b states the first review.
			name: "carried_first_review_at",
			a: func(now time.Time) pullRequestRow {
				row := pullRequestReadbackFixture(now)
				row.LastSynced = now.Add(2 * time.Hour)
				row.FirstReviewAt, row.ReviewsCount, row.ChangesRequestedCount = nil, 0, 0
				row.ReviewsLookupFailed = true
				return row
			},
			b: func(now time.Time) pullRequestRow {
				row := pullRequestReadbackFixture(now)
				row.LastSynced = now.Add(time.Hour)
				return row
			},
			check: func(t *testing.T, winner pullRequestVersion, b pullRequestRow) string {
				if timePointersEqual(winner.Row.FirstReviewAt, b.FirstReviewAt) && winner.Row.ReviewsCount == b.ReviewsCount {
					return ""
				}
				return "first_review_at/reviews_count stated by b lost"
			},
		},
		{
			// R3 terminal: a states a stale null merged_at; b states the merge.
			name: "terminal_merged_at",
			a: func(now time.Time) pullRequestRow {
				row := pullRequestReadbackFixture(now)
				row.LastSynced = now.Add(2 * time.Hour)
				row.State = "open"
				row.MergedAt, row.ClosedAt = nil, nil
				return row
			},
			b: func(now time.Time) pullRequestRow {
				row := pullRequestReadbackFixture(now)
				row.LastSynced = now.Add(time.Hour)
				return row
			},
			check: func(t *testing.T, winner pullRequestVersion, b pullRequestRow) string {
				if timePointersEqual(winner.Row.MergedAt, b.MergedAt) {
					return ""
				}
				return "merged_at stated by b lost (terminal guard defeated)"
			},
		},
	}
}

// seedOpenPullRequest writes the held version both writers start from: an
// open, unreviewed pull request.
func seedOpenPullRequest(t *testing.T, ctx context.Context, harness *pullRequestReadbackHarness) {
	t.Helper()
	row := pullRequestReadbackFixture(harness.now)
	row.OrgID = harness.claim.OrgID
	row.State = "open"
	row.MergedAt, row.ClosedAt, row.FirstReviewAt = nil, nil, nil
	row.ReviewsCount, row.ChangesRequestedCount = 0, 0
	if err := harness.sink.WriteEffect(ctx, harness.claim, pullRequestEffect(t, row)); err != nil {
		t.Fatal(err)
	}
}

// TestStoredVersionInterleaveKeepsValue is the CHAOS-6014 cell: two
// production writers of one git_pull_requests key, b (older version) inserting
// inside a's read-then-insert window. The serial order is the control: the
// same two writes, b first, keep b's value. Before the kept-group repair the
// interleaved order lost it, and so did the after order (b lands after a's
// insert): the loss does not need the window, only b's older version. The
// repair of whichever write lands last keeps b's value in every order.
func TestStoredVersionInterleaveKeepsValue(t *testing.T) {
	for _, tc := range interleaveCases() {
		for _, variant := range []struct {
			order  string
			bNewer bool
		}{{"serial", false}, {"interleaved", false}, {"after", false}, {"interleaved", true}, {"after", true}} {
			order := variant.order
			bVersion := "b_older"
			if variant.bNewer {
				bVersion = "b_newer"
			}
			t.Run(tc.name+"/"+order+"/"+bVersion, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
				defer cancel()
				harness := startPullRequestReadbackHarness(t, ctx)
				seedOpenPullRequest(t, ctx, harness)
				a, b := tc.a(harness.now), tc.b(harness.now)
				a.OrgID, b.OrgID = harness.claim.OrgID, harness.claim.OrgID
				if variant.bNewer {
					b.LastSynced = harness.now.Add(3 * time.Hour)
				}
				writeB := func() {
					if err := harness.sink.WriteEffect(ctx, harness.claim, pullRequestEffect(t, b)); err != nil {
						t.Fatal(err)
					}
				}
				hooked := &interleaveConn{Conn: harness.conn, hook: writeB}
				sinkA := harness.sink
				sinkA.Conn = hooked
				if order != "interleaved" {
					hooked.fired = true
				}
				if order == "serial" {
					writeB()
				}
				if err := sinkA.WriteEffect(ctx, harness.claim, pullRequestEffect(t, a)); err != nil {
					t.Fatal(err)
				}
				if order == "after" {
					writeB()
				}
				if order == "interleaved" && !hooked.fired {
					t.Fatal("hook never fired: b did not write inside a's window, nothing was tested")
				}
				winner, err := harness.sink.scanWinningPullRequestVersion(ctx, harness.claim.OrgID, a.RepoID, a.Number)
				if err != nil {
					t.Fatal(err)
				}
				if !winner.Found {
					t.Fatal("no winning row")
				}
				t.Logf("CELL6014 case=%s order=%s b=%s winner.last_synced=%s merged_at=%v first_review_at=%v reviews_count=%d",
					tc.name, order, bVersion, winner.LastSynced.UTC().Format(time.RFC3339), fmtTime(winner.Row.MergedAt),
					fmtTime(winner.Row.FirstReviewAt), winner.Row.ReviewsCount)
				if lost := tc.check(t, winner, b); lost != "" {
					t.Errorf("CELL6014 RED case=%s order=%s b=%s: %s", tc.name, order, bVersion, lost)
				} else {
					t.Logf("CELL6014 KEPT case=%s order=%s b=%s", tc.name, order, bVersion)
				}
			})
		}
	}
}

func fmtTime(value *time.Time) string {
	if value == nil {
		return "NULL"
	}
	return value.UTC().Format(time.RFC3339)
}
