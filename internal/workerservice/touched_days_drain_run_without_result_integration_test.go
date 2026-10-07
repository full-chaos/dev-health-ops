//go:build integration

package workerservice

import (
	"bytes"
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily"
)

// touchedEvent appends one event of the record of the touched days at a time
// the test chooses: the state a mark or a touch of that time left.
func touchedEvent(t *testing.T, ctx context.Context, rig *touchedRig, orgID string, day time.Time, repo, kind string, at time.Time) {
	t.Helper()
	if err := rig.conn.Exec(ctx, `
INSERT INTO daily_metrics_touched_days (org_id, day, repo_id, kind, at)
SELECT ?, toDate(?), ?, ?, fromUnixTimestamp64Milli(toInt64(?), 'UTC')`,
		orgID, day.Format("2006-01-02"), repo, kind, at.UTC().UnixMilli()); err != nil {
		t.Fatal(err)
	}
}

// touchedEventCount counts the events of one kind of one key.
func touchedEventCount(t *testing.T, ctx context.Context, rig *touchedRig, orgID string, day time.Time, repo, kind string) uint64 {
	t.Helper()
	var count uint64
	if err := rig.conn.QueryRow(ctx, `
SELECT count() FROM daily_metrics_touched_days FINAL
WHERE org_id = ? AND toString(day) = ? AND toString(repo_id) = ? AND kind = ?`,
		orgID, day.Format("2006-01-02"), repo, kind).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

// listedRun starts a run of one day for one repository under generation, then
// sets its creation time and its end. status "" leaves the run not ended.
func listedRun(
	t *testing.T, ctx context.Context, rig *touchedRig, orgID string, day time.Time, generation, repo string,
	created time.Time, status string, ended time.Time,
) {
	t.Helper()
	run := rig.startRun(t, ctx, func(tx pgx.Tx) (daily.Run, error) {
		return rig.store.StartRunTx(ctx, tx, daily.StartRunRequest{
			OrganizationID: orgID, TargetDay: day, Generation: generation,
			RepositoryIDs: []daily.RepositoryID{daily.RepositoryID(repo)},
		}, nilPartitionPublisher{})
	})
	if _, err := rig.pool.Exec(ctx, `
UPDATE public.daily_metrics_runs SET created_at = $2, updated_at = $2 WHERE id = $1::uuid`, run.ID, created); err != nil {
		t.Fatal(err)
	}
	switch status {
	case "":
	case "canceled":
		// A canceled run has no finalize: only its row says when it ended.
		if _, err := rig.pool.Exec(ctx, `
UPDATE public.daily_metrics_runs SET status = 'canceled', updated_at = $2 WHERE id = $1::uuid`, run.ID, ended); err != nil {
			t.Fatal(err)
		}
	default:
		if _, err := rig.pool.Exec(ctx, `
UPDATE public.daily_metrics_runs
SET status = $2, finalization_status = $2, finalized_at = $3, updated_at = $3 WHERE id = $1::uuid`, run.ID, status, ended); err != nil {
			t.Fatal(err)
		}
	}
}

// Every way a touched-day run can end without a result gives its keys back to
// the pending days: failed, canceled, and never ended. The keys are found by
// their own marks, not by the newest run of the day.
func TestTouchedDaysDrainReturnsTheKeysOfEveryRunWithoutAResult(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	rig := &drainRig{touchedRig: newTouchedRig(t, ctx), observer: &drainObserver{}, logs: &bytes.Buffer{}}
	day := time.Date(2026, 7, 30, 0, 0, 0, 0, time.UTC)
	dayKey := day.Format("2006-01-02")
	first, second := uuid.NewString(), uuid.NewString()

	// The drain started a run of the day for one repository and it failed.
	// A fan-out had started a run of the same day for another repository
	// before that, and its run is still open: it is the newest run of the day.
	t.Run("a failed run returns its keys when a newer run of the day is open, once", func(t *testing.T) {
		rig.reset()
		orgID := uuid.NewString()
		now := time.Now().UTC()
		touchedEvent(t, ctx, rig.touchedRig, orgID, day, first, "touched", now.Add(-12*time.Minute))
		touchedEvent(t, ctx, rig.touchedRig, orgID, day, first, "dispatched", now.Add(-10*time.Minute))
		listedRun(t, ctx, rig.touchedRig, orgID, day, daily.TouchedDrainGenerationPrefix+"e:"+uuid.NewString(), first,
			now.Add(-10*time.Minute), "failed", now.Add(-6*time.Minute))
		// The second key is marked AFTER the failure: its run is not the one
		// that failed, so it must stay as it is.
		touchedEvent(t, ctx, rig.touchedRig, orgID, day, second, "touched", now.Add(-5*time.Minute))
		touchedEvent(t, ctx, rig.touchedRig, orgID, day, second, "dispatched", now.Add(-4*time.Minute))
		listedRun(t, ctx, rig.touchedRig, orgID, day, "post-sync:"+uuid.NewString(), second,
			now.Add(-4*time.Minute), "", time.Time{})
		if got := rig.pendingDays(t, ctx, orgID); len(got) != 0 {
			t.Fatalf("pending before the pass = %v, want none", got)
		}
		drain := rig.drain(t, nil, nil)

		passID := pass("e")
		drain.DrainTouchedDays(ctx, orgID, passID)
		if got := rig.openDays(t, ctx, orgID); !reflect.DeepEqual(got, []string{dayKey}) {
			t.Fatalf("the pass after the failed run started %v, want a run of %s for the keys of the failed run", got, dayKey)
		}
		// The line of the pass names its trigger: the id of the run that
		// ended, readable by a search of the logs.
		wantPass := `"drain_pass":"` + strings.ReplaceAll(passID, "-", "_") + `"`
		if got := rig.logLines("touched_days_drain.pass"); len(got) != 1 || !strings.Contains(got[0], wantPass) {
			t.Fatalf("pass lines = %v, want one line with %s", got, wantPass)
		}
		if got := rig.observer.count(jobruntime.TouchedDaysDrainDaysReturned); got != 1 {
			t.Fatalf("days_returned_to_pending = %d, want 1", got)
		}
		if got := touchedEventCount(t, ctx, rig.touchedRig, orgID, day, second, "touched"); got != 1 {
			t.Fatalf("the key marked after the failure has %d touched events, want 1 (not returned)", got)
		}

		// The new run ends well. The failed run is still inside the lookback:
		// it must not return the day a second time, or the chain never ends.
		endDrainRuns(t, ctx, rig.touchedRig, orgID, "succeeded")
		before := rig.listedRunsByDay(t, ctx, orgID)
		drain.DrainTouchedDays(ctx, orgID, pass("e"))
		if after := rig.listedRunsByDay(t, ctx, orgID); !reflect.DeepEqual(before, after) {
			t.Fatalf("a second pass started a run for the same failure: %v -> %v", before, after)
		}
		if got := rig.observer.count(jobruntime.TouchedDaysDrainDaysReturned); got != 1 {
			t.Fatalf("days_returned_to_pending after the second pass = %d, want 1", got)
		}
		if got := rig.pendingDays(t, ctx, orgID); len(got) != 0 {
			t.Fatalf("pending after the second pass = %v, want none", got)
		}
	})

	t.Run("a canceled run returns its day", func(t *testing.T) {
		rig.reset()
		orgID := uuid.NewString()
		now := time.Now().UTC()
		touchedEvent(t, ctx, rig.touchedRig, orgID, day, first, "touched", now.Add(-12*time.Minute))
		touchedEvent(t, ctx, rig.touchedRig, orgID, day, first, "dispatched", now.Add(-10*time.Minute))
		listedRun(t, ctx, rig.touchedRig, orgID, day, "post-sync:"+uuid.NewString(), first,
			now.Add(-10*time.Minute), "canceled", now.Add(-6*time.Minute))
		rig.drain(t, nil, nil).DrainTouchedDays(ctx, orgID, pass("e"))
		if got := rig.openDays(t, ctx, orgID); !reflect.DeepEqual(got, []string{dayKey}) {
			t.Fatalf("the pass after a canceled run started %v, want a run of %s", got, dayKey)
		}
	})

	// A run that is not ended a day after its start will never end by itself
	// (a blocked run, a run whose jobs were lost). Its day must come back.
	t.Run("a run that never ends returns its day after a day, and not before", func(t *testing.T) {
		rig.reset()
		orgID := uuid.NewString()
		now := time.Now().UTC()
		mark := now.Add(-25 * time.Hour)
		touchedEvent(t, ctx, rig.touchedRig, orgID, day, first, "touched", mark.Add(-time.Minute))
		touchedEvent(t, ctx, rig.touchedRig, orgID, day, first, "dispatched", mark)
		generation := "post-sync:" + uuid.NewString()
		listedRun(t, ctx, rig.touchedRig, orgID, day, generation, first, now.Add(-23*time.Hour), "", time.Time{})
		drain := rig.drain(t, nil, nil)

		drain.DrainTouchedDays(ctx, orgID, pass("n"))
		if got := rig.openDays(t, ctx, orgID); len(got) != 0 {
			t.Fatalf("a pass started %v for a run that is open for 23 hours, want none", got)
		}
		// The row of the run is one minute older than the mark of its keys:
		// the two times come from two clocks. The end of the run counts as a
		// day after its creation, which is after the mark.
		if _, err := rig.pool.Exec(ctx, `
UPDATE public.daily_metrics_runs SET created_at = $3 WHERE org_id = $1::uuid AND generation = $2`,
			orgID, generation, mark.Add(-time.Minute)); err != nil {
			t.Fatal(err)
		}
		drain.DrainTouchedDays(ctx, orgID, pass("n"))
		if got := rig.openDays(t, ctx, orgID); !reflect.DeepEqual(got, []string{dayKey}) {
			t.Fatalf("the pass a day after a run that never ended started %v, want a run of %s", got, dayKey)
		}
	})

	// A day whose runs never end must not be started again each night for
	// ever: three such runs count as three failed runs.
	t.Run("a day whose three newest runs never ended is skipped", func(t *testing.T) {
		rig.reset()
		orgID := uuid.NewString()
		now := time.Now().UTC()
		mark := now.Add(-25 * time.Hour)
		touchedEvent(t, ctx, rig.touchedRig, orgID, day, first, "touched", mark.Add(-time.Minute))
		touchedEvent(t, ctx, rig.touchedRig, orgID, day, first, "dispatched", mark)
		for index := 0; index < 3; index++ {
			listedRun(t, ctx, rig.touchedRig, orgID, day, daily.TouchedDrainGenerationPrefix+"n:"+uuid.NewString(), first,
				mark.Add(-time.Duration(index)*24*time.Hour), "", time.Time{})
		}
		before := rig.listedRunsByDay(t, ctx, orgID)
		rig.drain(t, nil, nil).DrainTouchedDays(ctx, orgID, pass("n"))
		if after := rig.listedRunsByDay(t, ctx, orgID); !reflect.DeepEqual(before, after) {
			t.Fatalf("a pass started a run for a day whose runs never end: %v -> %v", before, after)
		}
		if got := rig.observer.count(jobruntime.TouchedDaysDrainDaysSkipped); got != 1 {
			t.Fatalf("days_skipped_after_failed_runs = %d, want 1", got)
		}
		if got := rig.pendingDays(t, ctx, orgID); !reflect.DeepEqual(got, []string{dayKey}) {
			t.Fatalf("pending = %v, want the day %s to stay visible", got, dayKey)
		}
	})
}

// A day over the repository limit is taken in parts. A work scope with items
// in several repositories is in more than one part. No part may write the row
// of the scope from its own repositories only: after each part, and in every
// stored version, the row holds the items of every repository of the scope,
// and after the last part the derived rows equal a run of every repository.
func TestTouchedDaysDrainSplitOfADayKeepsAWorkScopeOfSeveralRepositoriesWhole(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	rig := &drainRig{touchedRig: newTouchedRig(t, ctx), observer: &drainObserver{}, logs: &bytes.Buffer{}}
	orgID := uuid.NewString()
	day := time.Date(2026, 7, 30, 0, 0, 0, 0, time.UTC)
	now := time.Now().UTC()
	const scope = "scope-shared"
	for index := 0; index < 5; index++ {
		repo := uuid.New()
		insertTouchedRepo(t, ctx, rig.conn, orgID, repo, "acme/shared-"+string(rune('a'+index)), "github")
		insertPartitionKeyItems(t, ctx, rig.conn, orgID, day, partitionKeyItem{
			repo: repo, id: "gh:acme/shared-" + string(rune('a'+index)) + "#1", scope: scope, synced: now})
	}
	if _, err := rig.touched.RecordTouched(ctx, orgID, now.Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	drain := rig.drain(t, nil, drainFaultRuns{touchedDrainRuns: rig.productionRuns(), limit: 2})
	var sizes []int
	for part := 0; part < 5; part++ {
		drain.DrainTouchedDays(ctx, orgID, pass("e"))
		_, runID := openDrainRuns(t, ctx, rig.touchedRig, orgID)
		if runID == "" {
			break
		}
		run, err := rig.store.LoadRun(ctx, runID)
		if err != nil {
			t.Fatal(err)
		}
		listed := 0
		for _, partition := range rig.dispatchAndRun(t, ctx, run) {
			listed += len(partition.RepoIDs)
		}
		sizes = append(sizes, listed)
		endDrainRuns(t, ctx, rig.touchedRig, orgID, "succeeded")
		got := readWorkScopeRow(t, ctx, rig.conn, orgID, scope, day)
		if got.itemsCompleted != 5 || got.itemsCompletedInAnyVersion != 5 {
			t.Fatalf("after part %d (%d repositories) the row of the work scope = %+v, want the 5 items of its 5 repositories in every stored version",
				len(sizes), listed, got)
		}
	}
	if !reflect.DeepEqual(sizes, []int{2, 2, 1}) {
		t.Fatalf("parts of %v repositories, want 2, 2, 1", sizes)
	}
	split := derivedDaySnapshot(t, ctx, rig.conn, orgID, day)
	rig.fullRecompute(t, ctx, orgID, day)
	if whole := derivedDaySnapshot(t, ctx, rig.conn, orgID, day); !reflect.DeepEqual(split, whole) {
		t.Fatalf("derived rows after the split parts differ from a run of every repository:\nsplit: %v\nwhole: %v", split, whole)
	}
	if strings.HasPrefix(split["work_item_metrics_daily"], "0 rows") {
		t.Fatalf("the split parts wrote no work_item_metrics_daily row: %v", split)
	}
}
