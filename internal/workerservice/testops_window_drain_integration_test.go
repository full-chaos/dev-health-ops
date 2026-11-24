//go:build integration

package workerservice

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily"
	"github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime"
	"github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime/synclog"
)

// The testops window lane (CHAOS-14562) on the production stores: the record of RecordTouchedTests, the window fan-out and the
// window drain with their tw- runs.

// seedWindowKeys writes one retraction row of a run of repo on base, written now, and records it: the window lane then holds
// base+1 .. base+29 of repo. It returns those days, oldest first.
func seedWindowKeys(t *testing.T, ctx context.Context, rig *touchedRig, orgID string, repo uuid.UUID, base time.Time) []string {
	t.Helper()
	now := time.Now().UTC()
	started := base.Add(10 * time.Hour)
	if err := rig.conn.Exec(ctx, `INSERT INTO test_suite_results (repo_id, run_id, suite_id, suite_name, total_count,
		passed_count, failed_count, skipped_count, started_at, org_id, last_synced, report_slot, run_attempt, retracted)
		VALUES (?, ?, 'retired', 'pkg/a', 0, 0, 0, 0, ?, ?, ?, 'total', 2, 1)`,
		repo, "run-"+repo.String()[:8], started, orgID, now); err != nil {
		t.Fatal(err)
	}
	if _, err := rig.touched.RecordTouchedTests(ctx, orgID, now.Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	days := make([]string, 0, 29)
	for offset := 1; offset <= 29; offset++ {
		days = append(days, base.AddDate(0, 0, offset).Format("2006-01-02"))
	}
	return days
}

func windowRuns(rig *touchedRig) touchedDrainRuns {
	return touchedDrainRuns{store: rig.store, publisher: nilPartitionPublisher{}, lane: daily.TouchedLaneTestopsWindow}
}

func (rig *drainRig) windowDrain(t *testing.T, store syncdispatchruntime.TouchedDaysDrainStore, cap int) *syncdispatchruntime.TouchedDaysDrain {
	t.Helper()
	if store == nil {
		store = rig.touched.TestopsWindowLane()
	}
	drain, err := syncdispatchruntime.NewTestopsWindowDrain(
		rig.pool, store, windowRuns(rig.touchedRig), synclog.New(slog.New(slog.NewJSONHandler(rig.logs, nil))))
	if err != nil {
		t.Fatal(err)
	}
	drain.SetObserver(rig.observer)
	drain.SetRepoDayCap(cap)
	return drain
}

// windowKeysOfRuns are the (day|repository) keys the runs of a generation prefix list, with the number of runs that list each.
func windowKeysOfRuns(t *testing.T, ctx context.Context, rig *touchedRig, orgID, prefix string, open bool) map[string]int {
	t.Helper()
	status := ""
	if open {
		status = " AND run.status IN ('pending', 'running')"
	}
	rows, err := rig.pool.Query(ctx, `
SELECT run.target_day::text, repo
FROM public.daily_metrics_runs AS run
JOIN public.daily_metrics_partitions AS part ON part.run_id = run.id
CROSS JOIN LATERAL json_array_elements_text(part.repo_ids::json) AS repo
WHERE run.org_id = $1::uuid AND run.generation LIKE $2`+status, orgID, prefix+"%")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	keys := map[string]int{}
	for rows.Next() {
		var day, repo string
		if err := rows.Scan(&day, &repo); err != nil {
			t.Fatal(err)
		}
		keys[day+"|"+repo]++
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return keys
}

func endRunsOf(t *testing.T, ctx context.Context, rig *touchedRig, orgID, prefix, status string) {
	t.Helper()
	if _, err := rig.pool.Exec(ctx, `
UPDATE public.daily_metrics_runs
SET status = $3, finalization_status = $3, finalized_at = clock_timestamp(), updated_at = clock_timestamp()
WHERE org_id = $1::uuid AND generation LIKE $2 AND status IN ('pending', 'running')`, orgID, prefix+"%", status); err != nil {
		t.Fatal(err)
	}
}

func (rig *touchedRig) windowPendingCount(t *testing.T, ctx context.Context, orgID string) int {
	t.Helper()
	window := rig.touched.TestopsWindowLane()
	pending, err := window.PendingDays(ctx, orgID, 5000)
	if err != nil {
		t.Fatal(err)
	}
	repos, err := window.PendingRepositories(ctx, orgID, pending.Days, 1000)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, listed := range repos {
		count += len(listed)
	}
	return count
}

func TestTestopsWindowLane(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()
	rig := &drainRig{touchedRig: newTouchedRig(t, ctx), observer: &drainObserver{}, logs: newDrainLogs()}
	base := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)

	// C3: the fan-out starts at most its cap of window keys and leaves the rest pending, with one warning line and the cut
	// counter; the drain, five keys a pass, takes the rest. Every key gets exactly one run.
	t.Run("C3 a cut of the fan-out stays pending and the drain takes every key once", func(t *testing.T) {
		rig.reset()
		orgID := uuid.NewString()
		repo := uuid.New()
		days := seedWindowKeys(t, ctx, rig.touchedRig, orgID, repo, base)
		args := rig.seedSync(t, ctx, orgID, "tests", time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC))
		rig.setRunStart(t, ctx, args, time.Now().UTC().Add(-time.Hour))
		logs := &lockedBuffer{}
		service, err := syncdispatchruntime.NewNativePostSyncService(rig.pool, dailyPostSyncWriter{store: rig.store, publisher: nilPartitionPublisher{}},
			touchedStubRemaining{}, touchedStubWorkGraph{}, touchedStubPublish{}, touchedStubPublish{},
			synclog.New(slog.New(slog.NewJSONHandler(logs, nil))))
		if err != nil {
			t.Fatal(err)
		}
		if err := service.SetTouchedDays(rig.touched, dailyPostSyncWriter{store: rig.store, publisher: nilPartitionPublisher{}}); err != nil {
			t.Fatal(err)
		}
		if err := service.SetTestopsWindowDays(rig.touched.TestopsWindowLane(),
			dailyPostSyncWriter{store: rig.store, publisher: nilPartitionPublisher{}, lane: daily.TouchedLaneTestopsWindow}); err != nil {
			t.Fatal(err)
		}
		observer := &touchedCountObserver{}
		service.SetTouchedDaysObserver(observer)
		service.SetTestopsWindowRepoDayCap(5)
		if err := service.Fanout(ctx, args); err != nil {
			t.Fatal(err)
		}
		taken := windowKeysOfRuns(t, ctx, rig.touchedRig, orgID, daily.TestopsWindowSyncGenerationPrefix, false)
		if len(taken) != 5 {
			t.Fatalf("the fan-out started %d window keys %v, want its cap of 5", len(taken), taken)
		}
		for _, day := range days[24:] {
			if taken[day+"|"+repo.String()] != 1 {
				t.Errorf("the fan-out took %v, want the 5 newest days %v", taken, days[24:])
				break
			}
		}
		if left := rig.windowPendingCount(t, ctx, orgID); left != 24 {
			t.Fatalf("pending window keys after the fan-out = %d, want 24", left)
		}
		if got := observer.counts[jobruntime.PostSyncTouchedDaysTestopsWindowKeysDeferred]; got != 24 {
			t.Fatalf("testops_window_keys_deferred = %d, want the 24 keys the cap left", got)
		}
		// The retraction row was written in the sync run: its record writes the 29 keys again.
		if got := observer.counts[jobruntime.PostSyncTouchedDaysTestopsWindowKeysRecorded]; got != 29 {
			t.Fatalf("testops_window_keys_recorded = %d, want 29", got)
		}
		var warns []string
		for _, line := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
			if strings.Contains(line, `"msg":"post_sync_fanout.testops_window_cut"`) {
				warns = append(warns, line)
			}
		}
		if len(warns) != 1 || !strings.Contains(warns[0], `"level":"WARN"`) || !strings.Contains(warns[0], `"testops_window_repo_days_taken":5`) ||
			strings.Contains(warns[0], "error") {
			t.Fatalf("cut lines = %v, want one WARN with the counts and no error operand", warns)
		}

		drain := rig.windowDrain(t, nil, 5)
		for passes := 0; passes < 10 && rig.windowPendingCount(t, ctx, orgID) > 0; passes++ {
			endRunsOf(t, ctx, rig.touchedRig, orgID, "tw-", "succeeded")
			drain.DrainTouchedDays(ctx, orgID, pass("n"))
		}
		all := windowKeysOfRuns(t, ctx, rig.touchedRig, orgID, "tw-", false)
		if len(all) != 29 {
			t.Fatalf("window keys with a run = %d, want all 29", len(all))
		}
		for key, runs := range all {
			if runs != 1 {
				t.Errorf("key %s has %d runs, want 1", key, runs)
			}
		}
		if got := rig.observer.count(jobruntime.TouchedDaysDrainTestopsWindowDaysStarted); got == 0 {
			t.Errorf("testops_window_days_started = 0 after the drain")
		}
		if got := rig.observer.count(jobruntime.TouchedDaysDrainDaysStarted); got != 0 {
			t.Errorf("the window drain counted %d days_started of the touched-day lane", got)
		}
	})

	// C12 (R3): one trigger runs the touched-day pass and then the window pass; both lanes start their runs.
	t.Run("C12 one trigger starts the runs of both lanes", func(t *testing.T) {
		rig.reset()
		orgID := uuid.NewString()
		normal := seedPendingDays(t, ctx, rig.touchedRig, orgID, time.Date(2026, 6, 10, 0, 0, 0, 0, time.UTC), 3)
		window := seedWindowKeys(t, ctx, rig.touchedRig, orgID, uuid.New(), base)
		drains := touchedDaysDrains{days: rig.drain(t, nil, nil), testopsWindow: rig.windowDrain(t, nil, 0)}
		drains.DrainTouchedDays(ctx, orgID, pass("n"))
		// The touched-day lane holds the 3 seeded days and the day of the run of the retraction row.
		if got := rig.openDays(t, ctx, orgID); len(got) != len(normal)+1 {
			t.Fatalf("touched-day lane runs %v, want the %d pending days", got, len(normal)+1)
		}
		if got := windowKeysOfRuns(t, ctx, rig.touchedRig, orgID, daily.TestopsWindowDrainGenerationPrefix, true); len(got) != len(window) {
			t.Fatalf("window lane runs list %d keys, want %d", len(got), len(window))
		}
		if got := rig.pendingDays(t, ctx, orgID); len(got) != 0 {
			t.Fatalf("touched-day lane pending after the trigger: %v", got)
		}
		if got := rig.windowPendingCount(t, ctx, orgID); got != 0 {
			t.Fatalf("window lane pending after the trigger: %d", got)
		}
	})

	// C13 (R4): two window passes at the same time start the keys once: the lane's lock and the in-flight recount under it.
	t.Run("C13 two window passes at the same time start each key once", func(t *testing.T) {
		rig.reset()
		orgID := uuid.NewString()
		seedWindowKeys(t, ctx, rig.touchedRig, orgID, uuid.New(), base)
		drain := rig.windowDrain(t, nil, 0)
		var group sync.WaitGroup
		for index := 0; index < 2; index++ {
			group.Add(1)
			go func() {
				defer group.Done()
				drain.DrainTouchedDays(ctx, orgID, pass("e"))
			}()
		}
		group.Wait()
		keys := windowKeysOfRuns(t, ctx, rig.touchedRig, orgID, daily.TestopsWindowDrainGenerationPrefix, false)
		if len(keys) != 29 {
			t.Fatalf("two passes started %d keys, want 29", len(keys))
		}
		for key, runs := range keys {
			if runs != 1 {
				t.Fatalf("key %s has %d runs, want 1", key, runs)
			}
		}
	})

	// C14 (R5): a window mark that keeps failing stops the chain: the pass the end of the runs triggers starts nothing.
	t.Run("C14 a window mark that fails stops the window chain", func(t *testing.T) {
		rig.reset()
		orgID := uuid.NewString()
		seedWindowKeys(t, ctx, rig.touchedRig, orgID, uuid.New(), base)
		failing := rig.windowDrain(t, drainFaultStore{TouchedDaysDrainStore: rig.touched.TestopsWindowLane(), failMark: true}, 0)
		failing.DrainTouchedDays(ctx, orgID, pass("n"))
		first := windowKeysOfRuns(t, ctx, rig.touchedRig, orgID, daily.TestopsWindowDrainGenerationPrefix, false)
		if len(first) != 29 {
			t.Fatalf("first pass started %d keys, want 29", len(first))
		}
		var runID string
		if err := rig.pool.QueryRow(ctx, `SELECT id::text FROM public.daily_metrics_runs
WHERE org_id = $1::uuid AND generation LIKE 'tw-drain:%' ORDER BY id LIMIT 1`, orgID).Scan(&runID); err != nil {
			t.Fatal(err)
		}
		endRunsOf(t, ctx, rig.touchedRig, orgID, "tw-", "succeeded")
		failing.DrainTouchedDays(ctx, orgID, "e:"+runID)
		again := windowKeysOfRuns(t, ctx, rig.touchedRig, orgID, daily.TestopsWindowDrainGenerationPrefix, false)
		if len(again) != len(first) {
			t.Fatalf("after the end of a run whose mark is missing, %d keys have runs (was %d): the chain did not stop", len(again), len(first))
		}
		if got := rig.observer.count(jobruntime.TouchedDaysDrainTestopsWindowChainStopped); got != 1 {
			t.Fatalf("testops_window_chain_stopped_mark_missing = %d, want 1", got)
		}
	})

	// C6: a window run that ended without a result gives its keys back; the next pass starts them again.
	t.Run("C6 a failed window run returns its keys and they run again", func(t *testing.T) {
		rig.reset()
		orgID := uuid.NewString()
		seedWindowKeys(t, ctx, rig.touchedRig, orgID, uuid.New(), base)
		drain := rig.windowDrain(t, nil, 0)
		drain.DrainTouchedDays(ctx, orgID, pass("n"))
		endRunsOf(t, ctx, rig.touchedRig, orgID, "tw-", "failed")
		if got := rig.windowPendingCount(t, ctx, orgID); got != 0 {
			t.Fatalf("pending before the next pass = %d, want 0 (marked at the start)", got)
		}
		drain.DrainTouchedDays(ctx, orgID, pass("n"))
		keys := windowKeysOfRuns(t, ctx, rig.touchedRig, orgID, daily.TestopsWindowDrainGenerationPrefix, false)
		for key, runs := range keys {
			if runs != 2 {
				t.Fatalf("key %s has %d runs, want 2 (the failed one and the retry)", key, runs)
			}
		}
		if got := rig.observer.count(jobruntime.TouchedDaysDrainTestopsWindowDaysReturned); got == 0 {
			t.Fatalf("testops_window_days_returned_to_pending = 0")
		}
	})

	// C11 (R2): the skip after failed runs counts the runs of its own lane only.
	t.Run("C11 failed runs of one lane do not skip a day of the other", func(t *testing.T) {
		orgID := uuid.NewString()
		dayX, dayY := time.Date(2026, 7, 5, 0, 0, 0, 0, time.UTC), time.Date(2026, 7, 6, 0, 0, 0, 0, time.UTC)
		repo := uuid.NewString()
		tx, err := rig.pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for index := 0; index < 3; index++ {
			if _, err := windowRuns(rig.touchedRig).StartTx(ctx, tx, orgID, dayX, fmt.Sprintf("e:%d-%s", index, uuid.NewString()), []string{repo}); err != nil {
				t.Fatal(err)
			}
			if _, err := rig.productionRuns().StartTx(ctx, tx, orgID, dayY, fmt.Sprintf("e:%d-%s", index, uuid.NewString()), []string{repo}); err != nil {
				t.Fatal(err)
			}
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		endRunsOf(t, ctx, rig.touchedRig, orgID, "", "failed")
		days := []time.Time{dayX, dayY}
		flagged := func(runs touchedDrainRuns) []string {
			failed, err := runs.DaysWithOnlyFailedRuns(ctx, orgID, days, 3, 24*time.Hour, 24*time.Hour)
			if err != nil {
				t.Fatal(err)
			}
			var out []string
			for day := range failed {
				out = append(out, day)
			}
			sort.Strings(out)
			return out
		}
		if got := flagged(rig.productionRuns()); len(got) != 1 || got[0] != "2026-07-06" {
			t.Fatalf("touched-day lane skips %v, want 2026-07-06 only", got)
		}
		if got := flagged(windowRuns(rig.touchedRig)); len(got) != 1 || got[0] != "2026-07-05" {
			t.Fatalf("window lane skips %v, want 2026-07-05 only", got)
		}
	})
}
