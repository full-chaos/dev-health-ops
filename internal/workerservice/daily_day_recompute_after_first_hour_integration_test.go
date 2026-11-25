//go:build integration

package workerservice

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily"
	"github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
)

// The nightly fan-out of 01:00 UTC computes its own calendar day, so it sees
// only the first hour of that day. These cases trace which later run brings the
// rest of the day into its rows, and which full-organization run of the day is
// the one a marker sync would read (run_marker.go markerSync: the newest
// full_org run of the day by created_at).
func TestDailyDayDRecomputedAfterFirstHour(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	rig := newTouchedRig(t, ctx)
	day := time.Date(2026, 3, 10, 0, 0, 0, 0, time.UTC)
	next := day.AddDate(0, 0, 1)
	dayKey, nextKey := day.Format("2006-01-02"), next.Format("2006-01-02")

	t.Run("late commits of the day reach its rows", func(t *testing.T) {
		orgID := uuid.NewString()
		repo := uuid.New()
		insertTouchedRepo(t, ctx, rig.conn, orgID, repo, "acme/api", "github")

		insertDayCommit(t, ctx, rig.conn, orgID, repo, "c1", day.Add(30*time.Minute))
		first := rig.scheduledFanoutRun(t, ctx, orgID, day.Add(time.Hour))
		if got := rig.runTargetDay(t, ctx, first.ID); got != dayKey {
			t.Fatalf("the fan-out due %s 01:00 computes %s, want %s", dayKey, got, dayKey)
		}
		assertDayCommits(t, ctx, rig.conn, orgID, day, map[string]uint64{"dev-a@example.com": 1}, "after the fan-out of the day")
		if got := rig.newestFullOrgGeneration(t, ctx, orgID, day); got != first.Generation {
			t.Fatalf("newest full-org run of the day = %q, want the fan-out %q", got, first.Generation)
		}

		insertDayCommit(t, ctx, rig.conn, orgID, repo, "c2", day.Add(14*time.Hour))
		second := rig.scheduledFanoutRun(t, ctx, orgID, next.Add(time.Hour))
		if got := rig.runTargetDay(t, ctx, second.ID); got != nextKey {
			t.Fatalf("the fan-out due %s 01:00 computes %s, want %s", nextKey, got, nextKey)
		}
		assertDayCommits(t, ctx, rig.conn, orgID, day, map[string]uint64{"dev-a@example.com": 1},
			"after the next day's fan-out: it does not recompute the day before")

		// A sync whose unit window ends inside the day: its post-sync run of
		// every repository recomputes the day.
		inDay := rig.seedSyncWindow(t, ctx, orgID, "commits", day.Add(13*time.Hour), day.Add(15*time.Hour))
		runs := rig.fanoutAndRun(t, ctx, rig.service(t, rig.touched, nil), orgID, inDay)
		if got := touchedRunDays(runs); !reflect.DeepEqual(got, []string{dayKey}) || !runs[dayKey].fullOrg {
			t.Fatalf("post-sync runs of the in-day sync = %+v, want one full-org run of %s", runs, dayKey)
		}
		assertDayCommits(t, ctx, rig.conn, orgID, day, map[string]uint64{"dev-a@example.com": 2}, "after the in-day sync")
		if got := rig.newestFullOrgGeneration(t, ctx, orgID, day); got != "post-sync:"+inDay.SyncRunID() {
			t.Fatalf("newest full-org run of the day = %q, want the post-sync run of the in-day sync", got)
		}

		// A commit of 23:30 synced at 00:30 the next day by a unit with no
		// end: the post-sync window run computes the next day, and the record
		// of touched days starts a run of the listed repository for the day.
		insertDayCommit(t, ctx, rig.conn, orgID, repo, "c3", day.Add(23*time.Hour+30*time.Minute))
		overnight := rig.seedOpenEndedSync(t, ctx, orgID, "commits", next, next.Add(30*time.Minute))
		runs = rig.fanoutAndRun(t, ctx, rig.service(t, rig.touched, nil), orgID, overnight)
		if got := touchedRunDays(runs); !reflect.DeepEqual(got, []string{dayKey, nextKey}) {
			t.Fatalf("post-sync runs of the overnight sync = %v, want %s (touched) and %s (window)", got, dayKey, nextKey)
		}
		if runs[dayKey].fullOrg || runs[dayKey].repos != repo.String() || !runs[nextKey].fullOrg {
			t.Fatalf("overnight runs = %+v: the touched day is a run of its repository, the window day a full-org run", runs)
		}
		assertDayCommits(t, ctx, rig.conn, orgID, day, map[string]uint64{"dev-a@example.com": 3}, "after the overnight sync")
		if got := rig.newestFullOrgGeneration(t, ctx, orgID, day); got != "post-sync:"+inDay.SyncRunID() {
			t.Fatalf("newest full-org run of the day = %q, want still the in-day sync's run: a run of listed repositories is not full-org", got)
		}
	})

	t.Run("a review of the day on an older open pull request reaches its rows", func(t *testing.T) {
		orgID := uuid.NewString()
		repo := uuid.New()
		insertTouchedRepo(t, ctx, rig.conn, orgID, repo, "acme/web", "github")
		rig.scheduledFanoutRun(t, ctx, orgID, day.Add(time.Hour))
		assertDayReviews(t, ctx, rig.conn, orgID, day, map[string]uint64{}, "after the fan-out of the day")

		insertOpenPullRequest(t, ctx, rig.conn, orgID, repo, 7, day.AddDate(0, 0, -3).Add(10*time.Hour))
		insertReview(t, ctx, rig.conn, orgID, repo, 7, "r1", "dev-a@example.com", day.Add(16*time.Hour))
		overnight := rig.seedOpenEndedSync(t, ctx, orgID, "commits", next, next.Add(30*time.Minute))
		rig.fanoutAndRun(t, ctx, rig.service(t, rig.touched, nil), orgID, overnight)
		assertDayReviews(t, ctx, rig.conn, orgID, day, map[string]uint64{"dev-a@example.com": 1}, "after the overnight sync")
	})
}

func (rig *touchedRig) scheduledFanoutRun(t *testing.T, ctx context.Context, orgID string, due time.Time) daily.Run {
	t.Helper()
	run := rig.startRun(t, ctx, func(tx pgx.Tx) (daily.Run, error) {
		return rig.store.StartScheduledFanoutRunTx(ctx, tx, daily.ScheduledFanoutRequest{
			OrganizationID: orgID, TargetDay: due,
			Generation: daily.ScheduledFanoutGenerationPrefix + due.Format(time.RFC3339),
		}, nilPartitionPublisher{})
	})
	rig.dispatchAndRun(t, ctx, run)
	return run
}

// seedOpenEndedSync seeds a finished sync run with one successful unit that
// has no end (before_at NULL) and the given completion time.
func (rig *touchedRig) seedOpenEndedSync(
	t *testing.T, ctx context.Context, orgID, dataset string, since, completedAt time.Time,
) syncdispatchruntime.PostSyncArgs {
	t.Helper()
	runID, outboxID, integrationID := uuid.NewString(), uuid.NewString(), uuid.NewString()
	pgseed.EnsureSyncRun(ctx, t, rig.pool, pgseed.SyncRun{ID: runID, OrgID: orgID, IntegrationID: integrationID})
	pgseed.SyncDispatchOutbox(ctx, t, rig.pool, outboxID, runID, orgID, "post_sync", "dispatched", "river", touchedRigRouteGeneration)
	pgseed.InsertSyncRunUnit(ctx, t, rig.pool, pgseed.SyncRunUnit{
		ID: uuid.NewString(), RunID: runID, OrgID: orgID, IntegrationID: integrationID, SourceID: uuid.NewString(),
		DatasetKey: dataset, Status: "success", SinceAt: &since,
	})
	if _, err := rig.pool.Exec(ctx, `UPDATE sync_runs SET completed_at = $2 WHERE id = $1::uuid`, runID, completedAt); err != nil {
		t.Fatal(err)
	}
	return syncdispatchruntime.PostSyncArgs{TransportArgs: syncdispatchruntime.TransportArgs{
		Version: syncdispatchruntime.ContractVersionV1, OrgID: orgID, RunID: runID,
		DispatchOutbox: outboxID, RouteGeneration: touchedRigRouteGeneration,
	}}
}

func (rig *touchedRig) runTargetDay(t *testing.T, ctx context.Context, runID string) string {
	t.Helper()
	var day string
	if err := rig.pool.QueryRow(ctx, `SELECT target_day::text FROM daily_metrics_runs WHERE id = $1::uuid`, runID).Scan(&day); err != nil {
		t.Fatal(err)
	}
	return day
}

// newestFullOrgGeneration is the run markerSync reads for the day
// (run_marker.go, the SELECT of the latest full_org run by created_at).
func (rig *touchedRig) newestFullOrgGeneration(t *testing.T, ctx context.Context, orgID string, day time.Time) string {
	t.Helper()
	var generation string
	if err := rig.pool.QueryRow(ctx, `
SELECT run.generation
FROM public.daily_metrics_runs AS run
WHERE run.org_id = $1::uuid AND run.target_day = $2::date AND run.full_org
ORDER BY run.created_at DESC, run.id
LIMIT 1`, orgID, day.Format("2006-01-02")).Scan(&generation); err != nil {
		t.Fatal(err)
	}
	return generation
}

func insertDayCommit(t *testing.T, ctx context.Context, conn driver.Conn, orgID string, repo uuid.UUID, hash string, at time.Time) {
	t.Helper()
	if err := conn.Exec(ctx, `INSERT INTO git_commits
		(repo_id, hash, author_name, author_email, author_when, committer_when, parents, last_synced, org_id)
		VALUES (?, ?, 'Dev A', 'dev-a@example.com', ?, ?, 1, ?, ?)`,
		repo, hash, at, at, time.Now().UTC(), orgID); err != nil {
		t.Fatal(err)
	}
}

func insertOpenPullRequest(t *testing.T, ctx context.Context, conn driver.Conn, orgID string, repo uuid.UUID, number uint32, createdAt time.Time) {
	t.Helper()
	if err := conn.Exec(ctx, `INSERT INTO git_pull_requests
		(repo_id, number, state, author_name, author_email, created_at, last_synced, org_id)
		VALUES (?, ?, 'open', 'Dev B', 'dev-b@example.com', ?, ?, ?)`,
		repo, number, createdAt, time.Now().UTC(), orgID); err != nil {
		t.Fatal(err)
	}
}

func insertReview(t *testing.T, ctx context.Context, conn driver.Conn, orgID string, repo uuid.UUID, number uint32, reviewID, reviewer string, at time.Time) {
	t.Helper()
	if err := conn.Exec(ctx, `INSERT INTO git_pull_request_reviews
		(repo_id, number, review_id, reviewer, state, submitted_at, last_synced, org_id)
		VALUES (?, ?, ?, ?, 'APPROVED', ?, ?, ?)`,
		repo, number, reviewID, reviewer, at, time.Now().UTC(), orgID); err != nil {
		t.Fatal(err)
	}
}

func dayUserSums(t *testing.T, ctx context.Context, conn driver.Conn, orgID string, day time.Time, column string) map[string]uint64 {
	t.Helper()
	rows, err := conn.Query(ctx, `SELECT author_email, toUInt64(sum(`+column+`)) FROM user_metrics_daily FINAL
		WHERE org_id = ? AND day = ? GROUP BY author_email HAVING sum(`+column+`) > 0`, orgID, day)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	sums := map[string]uint64{}
	for rows.Next() {
		var email string
		var sum uint64
		if err := rows.Scan(&email, &sum); err != nil {
			t.Fatal(err)
		}
		sums[email] = sum
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return sums
}

func assertDayCommits(t *testing.T, ctx context.Context, conn driver.Conn, orgID string, day time.Time, want map[string]uint64, when string) {
	t.Helper()
	if got := dayUserSums(t, ctx, conn, orgID, day, "commits_count"); !reflect.DeepEqual(got, want) {
		t.Fatalf("commits of %s %s = %v, want %v", day.Format("2006-01-02"), when, got, want)
	}
}

func assertDayReviews(t *testing.T, ctx context.Context, conn driver.Conn, orgID string, day time.Time, want map[string]uint64, when string) {
	t.Helper()
	if got := dayUserSums(t, ctx, conn, orgID, day, "reviews_given"); !reflect.DeepEqual(got, want) {
		t.Fatalf("reviews given on %s %s = %v, want %v", day.Format("2006-01-02"), when, got, want)
	}
}
