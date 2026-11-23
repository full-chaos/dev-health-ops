//go:build integration

package workerservice

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily"
	"github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime"
	"github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime/synclog"
)

// The cells of the first review of the testops window lane (G1, G3, G4, G5). The counter events and log messages are named
// by their text, as an operator reads them.

const (
	windowKeysDeferredEvent  = "testops_window_keys_deferred"
	windowDrainDeferredMsg   = "touched_days_drain.testops_window_keys_deferred"
	windowFanoutDeferredMsg  = "post_sync_fanout.testops_window_cut"
	windowDrainSplitDayMsg   = "touched_days_drain.day_split"
	windowCellTargetDayShift = 5
)

// windowService is the fan-out with both lanes, its log and its counters. A writer limit above zero lowers the repository
// limit of the window writer.
func windowService(t *testing.T, rig *touchedRig, cap, writerLimit int) (*syncdispatchruntime.NativePostSyncService, *lockedBuffer, *touchedCountObserver) {
	t.Helper()
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
	var writer syncdispatchruntime.TouchedDayPostSyncWriter = dailyPostSyncWriter{
		store: rig.store, publisher: nilPartitionPublisher{}, lane: daily.TouchedLaneTestopsWindow}
	if writerLimit > 0 {
		writer = touchedLimitWriter{dailyPostSyncWriter: writer.(dailyPostSyncWriter), limit: writerLimit}
	}
	if err := service.SetTestopsWindowDays(rig.touched.TestopsWindowLane(), writer); err != nil {
		t.Fatal(err)
	}
	observer := &touchedCountObserver{}
	service.SetTouchedDaysObserver(observer)
	service.SetTestopsWindowRepoDayCap(cap)
	return service, logs, observer
}

func linesOf(buffer interface{ String() string }, message string) []string {
	var lines []string
	for _, line := range strings.Split(strings.TrimSpace(buffer.String()), "\n") {
		if strings.Contains(line, `"msg":"`+message+`"`) {
			lines = append(lines, line)
		}
	}
	return lines
}

// hookStore runs hook once, when the pass reads the repositories of its days: after the first read of the pass (its runs
// state) and before its start.
type hookStore struct {
	syncdispatchruntime.TouchedDaysDrainStore
	hook func()
}

func (store *hookStore) PendingRepositories(ctx context.Context, org string, days []time.Time, limit int) (map[string][]string, error) {
	if store.hook != nil {
		hook := store.hook
		store.hook = nil
		hook()
	}
	return store.TouchedDaysDrainStore.PendingRepositories(ctx, org, days, limit)
}

// startOneRun commits one run of the lane of runs for day.
func startOneRun(t *testing.T, ctx context.Context, rig *touchedRig, runs touchedDrainRuns, orgID string, day time.Time) {
	t.Helper()
	tx, err := rig.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runs.StartTx(ctx, tx, orgID, day, "e:"+uuid.NewString(), []string{uuid.NewString()}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
}

// failedRunsOn commits count failed runs of the window lane for day, created age ago.
func failedRunsOn(t *testing.T, ctx context.Context, rig *touchedRig, orgID string, day time.Time, count int, age time.Duration) {
	t.Helper()
	for index := 0; index < count; index++ {
		startOneRun(t, ctx, rig, windowRuns(rig), orgID, day)
	}
	if _, err := rig.pool.Exec(ctx, `
UPDATE public.daily_metrics_runs
SET status = 'failed', finalization_status = 'failed', finalized_at = clock_timestamp(), updated_at = clock_timestamp(),
    created_at = clock_timestamp() - make_interval(secs => $3)
WHERE org_id = $1::uuid AND generation LIKE 'tw-%' AND target_day = $2::date`, orgID, day, age.Seconds()); err != nil {
		t.Fatal(err)
	}
}

func TestTestopsWindowLaneReviewOne(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()
	rig := &drainRig{touchedRig: newTouchedRig(t, ctx), observer: &drainObserver{}, logs: newDrainLogs()}
	base := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)

	// G1: a window key on the target day of the sync belongs to a run of the window lane. The ordinary run of that day fails;
	// the window key is still computed by its own run, and a failure of that run is the window drain's to return.
	t.Run("G1 a window key on the fan-out target day is owned by a window run", func(t *testing.T) {
		rig.reset()
		orgID := uuid.NewString()
		repo := uuid.New()
		seedWindowKeys(t, ctx, rig.touchedRig, orgID, repo, base)
		target := base.AddDate(0, 0, windowCellTargetDayShift)
		key := target.Format("2006-01-02") + "|" + repo.String()
		args := rig.seedSync(t, ctx, orgID, "tests", target)
		rig.setRunStart(t, ctx, args, time.Now().UTC().Add(-time.Hour))
		service, _, _ := windowService(t, rig.touchedRig, 0, 0)
		if err := service.Fanout(ctx, args); err != nil {
			t.Fatal(err)
		}
		endRunsOf(t, ctx, rig.touchedRig, orgID, "post-sync:", "failed")
		if got := windowKeysOfRuns(t, ctx, rig.touchedRig, orgID, daily.TestopsWindowSyncGenerationPrefix, false)[key]; got != 1 {
			t.Fatalf("window key %s has %d tw-sync runs after the fan-out, want 1: its mark is not the ordinary run's", key, got)
		}
		// The window run fails too: the window drain returns the key and runs it again.
		endRunsOf(t, ctx, rig.touchedRig, orgID, "tw-", "failed")
		rig.windowDrain(t, nil, 0).DrainTouchedDays(ctx, orgID, pass("n"))
		if got := windowKeysOfRuns(t, ctx, rig.touchedRig, orgID, daily.TestopsWindowDrainGenerationPrefix, false)[key]; got != 1 {
			t.Fatalf("window key %s has %d tw-drain runs after its run failed, want 1 (returned and run again)", key, got)
		}
	})

	// G3: a run of one lane committed between the first read and the start of a pass of the other lane does not stop it.
	t.Run("G3 a window run committed during an ordinary pass does not stop it", func(t *testing.T) {
		rig.reset()
		orgID := uuid.NewString()
		days := seedPendingDays(t, ctx, rig.touchedRig, orgID, time.Date(2026, 6, 10, 0, 0, 0, 0, time.UTC), 3)
		store := &hookStore{TouchedDaysDrainStore: rig.touched, hook: func() {
			startOneRun(t, ctx, rig.touchedRig, windowRuns(rig.touchedRig), orgID, base)
		}}
		rig.drain(t, store, nil).DrainTouchedDays(ctx, orgID, pass("n"))
		if got := rig.openDays(t, ctx, orgID); len(got) != len(days) {
			t.Fatalf("the ordinary pass started %v, want the %d pending days", got, len(days))
		}
	})
	t.Run("G3 an ordinary run committed during a window pass does not stop it", func(t *testing.T) {
		rig.reset()
		orgID := uuid.NewString()
		days := seedWindowKeys(t, ctx, rig.touchedRig, orgID, uuid.New(), base)
		store := &hookStore{TouchedDaysDrainStore: rig.touched.TestopsWindowLane(), hook: func() {
			startOneRun(t, ctx, rig.touchedRig, rig.productionRuns(), orgID, base)
		}}
		rig.windowDrain(t, store, 0).DrainTouchedDays(ctx, orgID, pass("n"))
		if got := windowKeysOfRuns(t, ctx, rig.touchedRig, orgID, daily.TestopsWindowDrainGenerationPrefix, true); len(got) != len(days) {
			t.Fatalf("the window pass started %d keys, want %d", len(got), len(days))
		}
	})

	// G4: the cut of a window pass is the keys of its eligible days a cap deferred, reported once after the start, counts only.
	cutCell := func(t *testing.T, repos, cap, wantTaken, wantDeferred int) {
		rig.reset()
		orgID := uuid.NewString()
		for index := 0; index < repos; index++ {
			seedWindowKeys(t, ctx, rig.touchedRig, orgID, uuid.New(), base)
		}
		rig.windowDrain(t, nil, cap).DrainTouchedDays(ctx, orgID, pass("n"))
		if got := len(windowKeysOfRuns(t, ctx, rig.touchedRig, orgID, daily.TestopsWindowDrainGenerationPrefix, false)); got != wantTaken {
			t.Fatalf("the pass started %d keys, want %d", got, wantTaken)
		}
		warns := rig.logLines(windowDrainDeferredMsg)
		if len(warns) != 1 || !strings.Contains(warns[0], `"level":"WARN"`) ||
			!strings.Contains(warns[0], fmt.Sprintf(`"testops_window_keys_deferred":%d`, wantDeferred)) || strings.Contains(warns[0], "2026-") ||
			strings.Contains(warns[0], orgID) {
			t.Fatalf("deferred lines %v, want one WARN of %d keys, counts only", warns, wantDeferred)
		}
		if split := rig.logLines(windowDrainSplitDayMsg); len(split) != 0 {
			t.Fatalf("the window pass wrote per-day split lines with a date: %v", split)
		}
		if got := rig.observer.count(jobruntime.TouchedDaysDrainEvent(windowKeysDeferredEvent)); got != uint64(wantDeferred) {
			t.Fatalf("testops_window_keys_deferred = %d, want %d", got, wantDeferred)
		}
	}
	t.Run("G4 a cap that ends on a day boundary is reported", func(t *testing.T) { cutCell(t, 2, 6, 6, 52) })
	t.Run("G4 a cap inside a day is reported once, counts only", func(t *testing.T) { cutCell(t, 3, 7, 7, 80) })
	t.Run("G4 a skipped day is no cut", func(t *testing.T) {
		rig.reset()
		orgID := uuid.NewString()
		days := seedWindowKeys(t, ctx, rig.touchedRig, orgID, uuid.New(), base)
		newest, err := time.Parse("2006-01-02", days[len(days)-1])
		if err != nil {
			t.Fatal(err)
		}
		failedRunsOn(t, ctx, rig.touchedRig, orgID, newest, 3, time.Minute)
		rig.windowDrain(t, nil, 0).DrainTouchedDays(ctx, orgID, pass("n"))
		if got := len(windowKeysOfRuns(t, ctx, rig.touchedRig, orgID, daily.TestopsWindowDrainGenerationPrefix, true)); got != 28 {
			t.Fatalf("the pass started %d keys, want the 28 healthy days", got)
		}
		if warns := rig.logLines(windowDrainDeferredMsg); len(warns) != 0 {
			t.Fatalf("a skipped day was reported as a cut: %v", warns)
		}
		if got := rig.observer.count(jobruntime.TouchedDaysDrainEvent(windowKeysDeferredEvent)); got != 0 {
			t.Fatalf("testops_window_keys_deferred = %d for a skipped day, want 0", got)
		}
	})

	// G5: the window lane takes its keys newest day first: a day over the repository limit of one run is split, not skipped.
	t.Run("G5 the fan-out splits a newest day over the repository limit", func(t *testing.T) {
		rig.reset()
		orgID := uuid.NewString()
		for index := 0; index < 3; index++ {
			seedWindowKeys(t, ctx, rig.touchedRig, orgID, uuid.New(), base)
		}
		args := rig.seedSync(t, ctx, orgID, "tests", time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC))
		rig.setRunStart(t, ctx, args, time.Now().UTC().Add(-time.Hour))
		service, logs, observer := windowService(t, rig.touchedRig, 5, 2)
		if err := service.Fanout(ctx, args); err != nil {
			t.Fatal(err)
		}
		taken := windowKeysOfRuns(t, ctx, rig.touchedRig, orgID, daily.TestopsWindowSyncGenerationPrefix, false)
		byDay := map[string]int{}
		for key := range taken {
			byDay[key[:10]]++
		}
		newest := base.AddDate(0, 0, 29).Format("2006-01-02")
		if len(taken) != 5 || byDay[newest] != 2 || byDay[base.AddDate(0, 0, 28).Format("2006-01-02")] != 2 ||
			byDay[base.AddDate(0, 0, 27).Format("2006-01-02")] != 1 {
			t.Fatalf("the fan-out took %v, want 2+2+1 keys of the 3 newest days", byDay)
		}
		if got := observer.counts[jobruntime.PostSyncTouchedDaysEvent(windowKeysDeferredEvent)]; got != 87-5 {
			t.Fatalf("testops_window_keys_deferred = %d, want 82", got)
		}
		if warns := linesOf(logs, windowFanoutDeferredMsg); len(warns) != 1 || strings.Contains(warns[0], orgID) {
			t.Fatalf("fan-out deferred lines %v, want one, counts only", warns)
		}
	})
	t.Run("G5 a due retry of the newest day ranks by its day", func(t *testing.T) {
		rig.reset()
		orgID := uuid.NewString()
		days := seedWindowKeys(t, ctx, rig.touchedRig, orgID, uuid.New(), base)
		newest, err := time.Parse("2006-01-02", days[len(days)-1])
		if err != nil {
			t.Fatal(err)
		}
		failedRunsOn(t, ctx, rig.touchedRig, orgID, newest, 3, 25*time.Hour)
		rig.windowDrain(t, nil, 3).DrainTouchedDays(ctx, orgID, pass("n"))
		got := windowKeysOfRuns(t, ctx, rig.touchedRig, orgID, daily.TestopsWindowDrainGenerationPrefix, true)
		var started []string
		for key := range got {
			started = append(started, key[:10])
		}
		if len(got) != 3 || !strings.Contains(strings.Join(started, " "), days[len(days)-1]) ||
			!strings.Contains(strings.Join(started, " "), days[len(days)-3]) {
			t.Fatalf("the pass started days %v, want the 3 newest, the due retry included", started)
		}
	})
}
