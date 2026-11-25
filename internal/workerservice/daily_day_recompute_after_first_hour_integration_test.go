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
// rest of the day into its rows, and what the day's run marker says after each
// step. Each step ends its runs the way a finalize commit does and then runs
// markerSync (BackfillRunMarkers, the function CompleteFinalize calls), with the
// real ClickHouse marker store wired, so dispatch claims append 'reopened' too.
func TestDailyDayDRecomputedAfterFirstHour(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	rig := newTouchedRig(t, ctx)
	markers, err := daily.NewClickHouseRunMarkerStore(rig.conn)
	if err != nil {
		t.Fatal(err)
	}
	rig.store.SetRunMarkerWriter(markers)
	rig.store.SetRunMarkerReader(markers)
	day := time.Date(2026, 3, 10, 0, 0, 0, 0, time.UTC)
	next := day.AddDate(0, 0, 1)
	dayKey, nextKey := day.Format("2006-01-02"), next.Format("2006-01-02")

	t.Run("late commits of the day reach its rows", func(t *testing.T) {
		orgID := uuid.NewString()
		repo := uuid.New()
		insertTouchedRepo(t, ctx, rig.conn, orgID, repo, "acme/api", "github")
		dev := func(n uint64) map[string]uint64 { return map[string]uint64{"dev-a@example.com": n} }

		// Step a: a commit of 00:30, then the fan-out due 01:00 of the day.
		insertDayCommit(t, ctx, rig.conn, orgID, repo, "c1", day.Add(30*time.Minute))
		first := rig.scheduledFanoutRun(t, ctx, orgID, day.Add(time.Hour))
		if got := rig.runTargetDay(t, ctx, first.ID); got != dayKey {
			t.Fatalf("step a: the fan-out due %s 01:00 computes %s, want %s", dayKey, got, dayKey)
		}
		markerA := rig.endStep(t, ctx, orgID, day, "a")
		assertDayCommits(t, ctx, rig.conn, orgID, day, dev(1), "after step a (the fan-out of the day)")
		if markerA.state != "succeeded" || markerA.generation != first.Generation {
			t.Fatalf("step a: marker of the day = %+v, want succeeded by the fan-out %q", markerA, first.Generation)
		}

		// Step b: a commit of 14:00, then the fan-out due 01:00 of the next day.
		insertDayCommit(t, ctx, rig.conn, orgID, repo, "c2", day.Add(14*time.Hour))
		second := rig.scheduledFanoutRun(t, ctx, orgID, next.Add(time.Hour))
		if got := rig.runTargetDay(t, ctx, second.ID); got != nextKey {
			t.Fatalf("step b: the fan-out due %s 01:00 computes %s, want %s", nextKey, got, nextKey)
		}
		markerB := rig.endStep(t, ctx, orgID, day, "b")
		assertDayCommits(t, ctx, rig.conn, orgID, day, dev(1), "after step b (the next day's fan-out does not recompute the day)")
		if markerB != markerA {
			t.Fatalf("step b: marker of the day moved: %+v, before %+v", markerB, markerA)
		}

		// Step c: a sync whose unit window ends inside the day. Its post-sync
		// run of every repository recomputes the day.
		inDay := rig.seedSyncWindow(t, ctx, orgID, "commits", day.Add(13*time.Hour), day.Add(15*time.Hour))
		runs := rig.fanoutAndRun(t, ctx, rig.service(t, rig.touched, nil), orgID, inDay)
		if got := touchedRunDays(runs); !reflect.DeepEqual(got, []string{dayKey}) || !runs[dayKey].fullOrg {
			t.Fatalf("step c: post-sync runs = %+v, want one full-org run of %s", runs, dayKey)
		}
		markerC := rig.endStep(t, ctx, orgID, day, "c")
		assertDayCommits(t, ctx, rig.conn, orgID, day, dev(2), "after step c (the in-day sync)")
		if markerC.state != "succeeded" || markerC.generation != "post-sync:"+inDay.SyncRunID() || markerC.version <= markerA.version {
			t.Fatalf("step c: marker of the day = %+v, want succeeded by the in-day sync's run, newer than %+v", markerC, markerA)
		}

		// Step d: a commit of 23:30 synced at 00:30 the next day by a unit with
		// no end: the window run is the next day; the day gets a run of its
		// touched repository.
		insertDayCommit(t, ctx, rig.conn, orgID, repo, "c3", day.Add(23*time.Hour+30*time.Minute))
		overnight := rig.seedOpenEndedSync(t, ctx, orgID, "commits", next, next.Add(30*time.Minute))
		runs = rig.fanoutAndRun(t, ctx, rig.service(t, rig.touched, nil), orgID, overnight)
		if got := touchedRunDays(runs); !reflect.DeepEqual(got, []string{dayKey, nextKey}) {
			t.Fatalf("step d: post-sync runs = %v, want %s (touched) and %s (window)", got, dayKey, nextKey)
		}
		if runs[dayKey].fullOrg || runs[dayKey].repos != repo.String() || !runs[nextKey].fullOrg {
			t.Fatalf("step d: runs = %+v, want a run of the touched repository for the day and a full-org run of the next day", runs)
		}
		markerD := rig.endStep(t, ctx, orgID, day, "d")
		assertDayCommits(t, ctx, rig.conn, orgID, day, dev(3), "after step d (the overnight sync)")

		// Step e: a commit of 22:00 synced at 03:00 the next day, after the
		// next day's fan-out; no later full-org run of the day follows.
		insertDayCommit(t, ctx, rig.conn, orgID, repo, "c4", day.Add(22*time.Hour))
		late := rig.seedOpenEndedSync(t, ctx, orgID, "commits", next.Add(2*time.Hour), next.Add(3*time.Hour))
		runs = rig.fanoutAndRun(t, ctx, rig.service(t, rig.touched, nil), orgID, late)
		if got := touchedRunDays(runs); !reflect.DeepEqual(got, []string{dayKey, nextKey}) || runs[dayKey].fullOrg {
			t.Fatalf("step e: post-sync runs = %+v, want a run of the touched repository for %s and the window run of %s", runs, dayKey, nextKey)
		}
		markerE := rig.endStep(t, ctx, orgID, day, "e")
		assertDayCommits(t, ctx, rig.conn, orgID, day, dev(4), "after step e (the sync after the next day's fan-out)")

		// G1: the runs of listed repositories in steps d and e recomputed the
		// day; the marker is the one step c wrote, field for field.
		if markerD != markerC || markerE != markerC {
			t.Fatalf("G1: marker of the day after d %+v / e %+v, after c %+v", markerD, markerE, markerC)
		}
	})

	t.Run("a review of the day on an older open pull request reaches its rows", func(t *testing.T) {
		orgID := uuid.NewString()
		repo := uuid.New()
		insertTouchedRepo(t, ctx, rig.conn, orgID, repo, "acme/web", "github")
		rig.scheduledFanoutRun(t, ctx, orgID, day.Add(time.Hour))
		rig.endStep(t, ctx, orgID, day, "g2-a")
		assertDayReviews(t, ctx, rig.conn, orgID, day, map[string]uint64{}, "after the fan-out of the day")

		insertOpenPullRequest(t, ctx, rig.conn, orgID, repo, 7, day.AddDate(0, 0, -3).Add(10*time.Hour))
		insertReview(t, ctx, rig.conn, orgID, repo, 7, "r1", "dev-a@example.com", day.Add(16*time.Hour))
		overnight := rig.seedOpenEndedSync(t, ctx, orgID, "commits", next, next.Add(30*time.Minute))
		runs := rig.fanoutAndRun(t, ctx, rig.service(t, rig.touched, nil), orgID, overnight)
		t.Logf("g2: post-sync runs by day = %v", touchedRunDays(runs))
		rig.endStep(t, ctx, orgID, day, "g2-b")
		assertDayReviews(t, ctx, rig.conn, orgID, day, map[string]uint64{"dev-a@example.com": 1}, "after the overnight sync")
	})
}

// dayMarker is the winning daily_metrics_run_marker row of one org-day under
// the documented reader rule (argMax by (version, state = 'reopened')).
type dayMarker struct {
	state       string
	generation  string
	finalizedAt time.Time
	version     uint64
	rows        uint64
}

// endStep ends every open daily run of the organization as a finalize commit
// does, runs markerSync for the day and the next day, logs the day's marker and
// returns it.
func (rig *touchedRig) endStep(t *testing.T, ctx context.Context, orgID string, day time.Time, step string) dayMarker {
	t.Helper()
	if _, err := rig.pool.Exec(ctx, `
UPDATE public.daily_metrics_runs
SET status = 'succeeded', finalization_status = 'succeeded', finalized_at = clock_timestamp(), updated_at = clock_timestamp()
WHERE org_id = $1::uuid AND status IN ('pending', 'running')`, orgID); err != nil {
		t.Fatal(err)
	}
	if _, err := rig.store.BackfillRunMarkers(ctx, orgID, day, day.AddDate(0, 0, 1), false); err != nil {
		t.Fatalf("step %s: marker sync: %v", step, err)
	}
	var marker dayMarker
	if err := rig.conn.QueryRow(ctx, `
SELECT argMax(state, (version, state = 'reopened')), argMax(generation, (version, state = 'reopened')),
       argMax(finalized_at, (version, state = 'reopened')), max(version), count()
FROM daily_metrics_run_marker WHERE org_id = ? AND target_day = ?`, orgID, day).
		Scan(&marker.state, &marker.generation, &marker.finalizedAt, &marker.version, &marker.rows); err != nil {
		t.Fatal(err)
	}
	t.Logf("step %s: day %s marker state=%s generation=%s version=%d finalized_at=%s marker_rows=%d after_day_end=%t",
		step, day.Format("2006-01-02"), marker.state, marker.generation, marker.version,
		marker.finalizedAt.UTC().Format(time.RFC3339Nano), marker.rows, !marker.finalizedAt.Before(day.AddDate(0, 0, 1)))
	marker.rows = 0
	return marker
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
