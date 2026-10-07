//go:build integration

package workerservice

import (
	"context"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime"
)

// unitWindowDays is the length of the unit window of these tests: the 15 days
// of the runs of every repository, the PostSyncTouchedDaysPerFanout days the
// fan-out starts a run of its own for, and 14 days it leaves pending.
const unitWindowDays = 60

// unitWindowOrg is one organization after the fan-out of a sync whose
// work-items unit had a window of unitWindowDays days.
type unitWindowOrg struct {
	orgID  string
	repo   uuid.UUID
	target time.Time
	since  time.Time
	args   syncdispatchruntime.PostSyncArgs
	// left are the days the fan-out left pending with no run, oldest first.
	left []string
}

func (org unitWindowOrg) day(daysBeforeTarget int) time.Time {
	return org.target.AddDate(0, 0, -daysBeforeTarget)
}

// newUnitWindowOrg stores one work item of one repository, runs the fan-out of
// a sync with a window of unitWindowDays days and checks what the fan-out did
// with the days of the window: every (day, repository) key of the window is
// marked or pending, and the oldest 14 days have no run.
func newUnitWindowOrg(t *testing.T, ctx context.Context, rig *drainRig) unitWindowOrg {
	t.Helper()
	org := unitWindowOrg{
		orgID: uuid.NewString(), repo: uuid.New(),
		target: time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC),
	}
	org.since = org.day(unitWindowDays - 1)
	insertTouchedRepo(t, ctx, rig.conn, org.orgID, org.repo, "acme/api", "github")
	// The events of the item are on the target day, so every other key of the
	// record comes from the window and not from a raw row.
	insertTouchedItems(t, ctx, rig.conn, org.orgID, touchedItem{
		repo: org.repo, id: "gh:acme/api#1", provider: "github", day: org.target, completed: true, synced: time.Now().UTC(),
	})
	org.args = rig.fanoutOfUnitWindow(t, ctx, org)
	runs := rig.runsOf(t, ctx, org.orgID, org.args)
	everyRepository, listed := 0, 0
	for day, run := range runs {
		if run.fullOrg {
			everyRepository++
			continue
		}
		listed++
		if run.repos != org.repo.String() {
			t.Fatalf("day %s: repositories of the run = %q, want the repository of the work item", day, run.repos)
		}
	}
	if everyRepository != 15 || listed != syncdispatchruntime.PostSyncTouchedDaysPerFanout {
		t.Fatalf("runs of the fan-out: %d of every repository and %d of listed repositories, want 15 and %d",
			everyRepository, listed, syncdispatchruntime.PostSyncTouchedDaysPerFanout)
	}
	for offset := unitWindowDays - 1; offset >= 15+syncdispatchruntime.PostSyncTouchedDaysPerFanout; offset-- {
		org.left = append(org.left, org.day(offset).Format("2006-01-02"))
	}
	if got := rig.pendingDays(t, ctx, org.orgID); !reflect.DeepEqual(got, org.left) {
		t.Fatalf("pending after the fan-out = %v, want the %d oldest days of the window %v", got, len(org.left), org.left)
	}
	for _, day := range org.left {
		if _, hasRun := runs[day]; hasRun {
			t.Fatalf("day %s is pending and has a run of the fan-out", day)
		}
	}
	return org
}

// fanoutOfUnitWindow seeds a finished sync of the organization with the window
// of unitWindowDays days and runs its fan-out.
func (rig *drainRig) fanoutOfUnitWindow(t *testing.T, ctx context.Context, org unitWindowOrg) syncdispatchruntime.PostSyncArgs {
	t.Helper()
	args := rig.seedSyncWindow(t, ctx, org.orgID, "work-items", org.since, org.target.Add(12*time.Hour))
	if err := rig.service(t, rig.touched, nil).Fanout(ctx, args); err != nil {
		t.Fatal(err)
	}
	return args
}

// failRun ends one run as failed.
func (rig *drainRig) failRun(t *testing.T, ctx context.Context, runID string) {
	t.Helper()
	tag, err := rig.pool.Exec(ctx, `
UPDATE public.daily_metrics_runs
SET status = 'failed', finalization_status = 'failed', finalized_at = clock_timestamp(), updated_at = clock_timestamp()
WHERE id = $1::uuid`, runID)
	if err != nil {
		t.Fatal(err)
	}
	if tag.RowsAffected() != 1 {
		t.Fatalf("failed %d runs, want 1", tag.RowsAffected())
	}
}

// drainRunRepositories returns the repositories the drain runs of one day
// list, one entry for each run.
func (rig *drainRig) drainRunRepositories(t *testing.T, ctx context.Context, orgID, day string) []string {
	t.Helper()
	rows, err := rig.pool.Query(ctx, `
SELECT COALESCE((SELECT string_agg(repo, ',' ORDER BY repo)
                 FROM public.daily_metrics_partitions AS part, json_array_elements_text(part.repo_ids) AS repo
                 WHERE part.run_id = run.id), '')
FROM public.daily_metrics_runs AS run
WHERE run.org_id = $1::uuid AND run.target_day = $2::date AND run.generation LIKE 'touched-drain:%'`, orgID, day)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var lists []string
	for rows.Next() {
		var list string
		if err := rows.Scan(&list); err != nil {
			t.Fatal(err)
		}
		lists = append(lists, list)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return lists
}

// errorLines returns the Error lines of the passes since the last reset.
func (rig *drainRig) errorLines() []string {
	var lines []string
	for _, line := range strings.Split(strings.TrimSpace(rig.logs.String()), "\n") {
		if strings.Contains(line, `"level":"ERROR"`) {
			lines = append(lines, line)
		}
	}
	return lines
}

func sortedDays(counts map[string]int) []string {
	days := make([]string, 0, len(counts))
	for day := range counts {
		days = append(days, day)
	}
	sort.Strings(days)
	return days
}

// A sync marks every day of its unit window as touched, for each repository of
// its work items. The owner of such a key is the newest run of a fan-out or of
// the drain that lists it; a key that no run lists has no owner. Each case is
// one way a key of the window meets the return of the keys of a run without a
// result: no case loses a day, starts a run twice or writes an Error line that
// is not true.
func TestTouchedDaysOfAUnitWindowUnderTheOwnerRule(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	rig := newDrainRig(t, ctx)

	t.Run("a window day that the fan-out started a run for is returned when that run fails", func(t *testing.T) {
		org := newUnitWindowOrg(t, ctx, rig)
		rig.reset()
		failed, kept := org.day(20), org.day(21)
		failedKey := failed.Format("2006-01-02")
		runs := rig.runsOf(t, ctx, org.orgID, org.args)
		if runs[failedKey].fullOrg || runs[failedKey].id == "" {
			t.Fatalf("day %s has no run of listed repositories of the fan-out: %+v", failedKey, runs[failedKey])
		}
		rig.failRun(t, ctx, runs[failedKey].id)
		keptTouch := newestTouchedAt(t, ctx, rig.touchedRig, org.orgID, kept, org.repo.String())

		rig.drain(t, nil, nil).DrainTouchedDays(ctx, org.orgID, pass("n"))

		if got := rig.observer.count(jobruntime.TouchedDaysDrainDaysReturned); got != 1 {
			t.Fatalf("days_returned_to_pending = %d, want 1: the window day of the failed run of the fan-out stayed marked", got)
		}
		wantRuns := append([]string{}, org.left...)
		wantRuns = append(wantRuns, failedKey)
		sort.Strings(wantRuns)
		if got := sortedDays(drainRunsOf(t, ctx, rig, org.orgID)); !reflect.DeepEqual(got, wantRuns) {
			t.Fatalf("drain runs for %v, want the days the fan-out left and the day of its failed run %v", got, wantRuns)
		}
		if got := rig.drainRunRepositories(t, ctx, org.orgID, failedKey); !reflect.DeepEqual(got, []string{org.repo.String()}) {
			t.Fatalf("repositories of the drain run of %s = %v, want the one repository of the key", failedKey, got)
		}
		if got := newestTouchedAt(t, ctx, rig.touchedRig, org.orgID, kept, org.repo.String()); got != keptTouch {
			t.Fatalf("the key of a fan-out run that is not ended was returned: newest touch %d, want %d", got, keptTouch)
		}
		if got := rig.pendingDays(t, ctx, org.orgID); len(got) != 0 {
			t.Fatalf("pending after the pass = %v, want none", got)
		}
	})

	t.Run("a window day behind the fan-out has no owner and is only pending", func(t *testing.T) {
		org := newUnitWindowOrg(t, ctx, rig)
		rig.reset()
		oldest, err := time.Parse("2006-01-02", org.left[0])
		if err != nil {
			t.Fatal(err)
		}
		touchBefore := newestTouchedAt(t, ctx, rig.touchedRig, org.orgID, oldest, org.repo.String())

		rig.drain(t, nil, nil).DrainTouchedDays(ctx, org.orgID, pass("n"))

		if got := rig.observer.count(jobruntime.TouchedDaysDrainDaysReturned); got != 0 {
			t.Fatalf("days_returned_to_pending = %d, want 0: a pending key with no run is not a returned key", got)
		}
		if got := newestTouchedAt(t, ctx, rig.touchedRig, org.orgID, oldest, org.repo.String()); got != touchBefore {
			t.Fatalf("the pending key with no run got a new event: newest touch %d, want %d", got, touchBefore)
		}
		if got := rig.observer.count(jobruntime.TouchedDaysDrainDaysSkipped); got != 0 {
			t.Fatalf("days skipped = %d, want 0", got)
		}
		if got := rig.errorLines(); len(got) != 0 {
			t.Fatalf("Error lines of the pass = %v, want none", got)
		}
		counts := drainRunsOf(t, ctx, rig, org.orgID)
		if got := sortedDays(counts); !reflect.DeepEqual(got, org.left) || totalRuns(counts) != len(org.left) {
			t.Fatalf("drain runs by day = %v, want one for each day the fan-out left %v", counts, org.left)
		}
		if got := rig.pendingDays(t, ctx, org.orgID); len(got) != 0 {
			t.Fatalf("pending after the pass = %v, want none", got)
		}
	})

	t.Run("a pending window day that an older failed run lists gets no second event and one run", func(t *testing.T) {
		org := newUnitWindowOrg(t, ctx, rig)
		drain := rig.drain(t, nil, nil)
		// The drain takes the days the fan-out left; its run of one of them
		// fails and no pass runs before the next sync.
		drain.DrainTouchedDays(ctx, org.orgID, pass("n"))
		again := org.left[3]
		againDay, err := time.Parse("2006-01-02", again)
		if err != nil {
			t.Fatal(err)
		}
		failDrainRunOf(t, ctx, rig, org.orgID, again)
		endDrainRuns(t, ctx, rig.touchedRig, org.orgID, "succeeded")
		if got := rig.pendingDays(t, ctx, org.orgID); len(got) != 0 {
			t.Fatalf("pending before the second sync = %v, want none (the failed run marked its key)", got)
		}
		// The next sync has the same window: it marks the day again, and its
		// fan-out starts runs for the newest days only.
		insertTouchedItems(t, ctx, rig.conn, org.orgID, touchedItem{
			repo: org.repo, id: "gh:acme/api#2", provider: "github", day: org.target, completed: true, synced: time.Now().UTC(),
		})
		rig.fanoutOfUnitWindow(t, ctx, org)
		if got := rig.pendingDays(t, ctx, org.orgID); !reflect.DeepEqual(got, org.left) {
			t.Fatalf("pending after the second fan-out = %v, want %v", got, org.left)
		}
		touchOfSync := newestTouchedAt(t, ctx, rig.touchedRig, org.orgID, againDay, org.repo.String())
		touchedEvents := touchedEventCount(t, ctx, rig.touchedRig, org.orgID, againDay, org.repo.String(), "touched")
		before := drainRunsOf(t, ctx, rig, org.orgID)
		rig.reset()

		drain.DrainTouchedDays(ctx, org.orgID, pass("n"))

		if got := rig.observer.count(jobruntime.TouchedDaysDrainDaysReturned); got != 0 {
			t.Fatalf("days_returned_to_pending = %d, want 0: the key of the failed run was pending already", got)
		}
		if got := newestTouchedAt(t, ctx, rig.touchedRig, org.orgID, againDay, org.repo.String()); got != touchOfSync {
			t.Fatalf("the pending key got a second event: newest touch %d, want the one of the sync %d", got, touchOfSync)
		}
		if got := touchedEventCount(t, ctx, rig.touchedRig, org.orgID, againDay, org.repo.String(), "touched"); got != touchedEvents {
			t.Fatalf("'touched' events of the key = %d, want %d", got, touchedEvents)
		}
		after := drainRunsOf(t, ctx, rig, org.orgID)
		for _, day := range org.left {
			if after[day] != before[day]+1 {
				t.Fatalf("drain runs of %s = %d, want %d (one more run)", day, after[day], before[day]+1)
			}
		}
		if after[again] != 2 {
			t.Fatalf("drain runs of %s = %d, want 2 (the failed run and one run for the new mark)", again, after[again])
		}
		if got := rig.errorLines(); len(got) != 0 {
			t.Fatalf("Error lines of the pass = %v, want none", got)
		}
		if got := rig.pendingDays(t, ctx, org.orgID); len(got) != 0 {
			t.Fatalf("pending after the pass = %v, want none", got)
		}

		// The chain ends: the pass that the end of one of these runs triggers
		// starts nothing, returns nothing and does not report a missing mark.
		_, endedRun := openDrainRuns(t, ctx, rig.touchedRig, org.orgID)
		endDrainRuns(t, ctx, rig.touchedRig, org.orgID, "succeeded")
		rig.reset()

		drain.DrainTouchedDays(ctx, org.orgID, "e:"+endedRun)

		if got := drainRunsOf(t, ctx, rig, org.orgID); !reflect.DeepEqual(got, after) {
			t.Fatalf("drain runs after the end of the chain = %v, want no new run %v", got, after)
		}
		if got := rig.observer.count(jobruntime.TouchedDaysDrainDaysReturned); got != 0 {
			t.Fatalf("days_returned_to_pending of the last pass = %d, want 0: the failed run owns no key any more", got)
		}
		if got := rig.observer.count(drainEventChainStopped); got != 0 {
			t.Fatalf("chain_stopped_mark_missing = %d, want 0", got)
		}
		if got := rig.errorLines(); len(got) != 0 {
			t.Fatalf("Error lines of the last pass = %v, want none", got)
		}
	})

	t.Run("a window day of the run of every repository is returned when that run fails", func(t *testing.T) {
		org := newUnitWindowOrg(t, ctx, rig)
		rig.reset()
		failed, kept := org.day(3), org.day(4)
		failedKey := failed.Format("2006-01-02")
		runs := rig.runsOf(t, ctx, org.orgID, org.args)
		if !runs[failedKey].fullOrg || !runs[kept.Format("2006-01-02")].fullOrg {
			t.Fatalf("days %s and %s have no run of every repository of the fan-out", failedKey, kept.Format("2006-01-02"))
		}
		rig.failRun(t, ctx, runs[failedKey].id)
		keptTouch := newestTouchedAt(t, ctx, rig.touchedRig, org.orgID, kept, org.repo.String())

		rig.drain(t, nil, nil).DrainTouchedDays(ctx, org.orgID, pass("n"))

		if got := rig.observer.count(jobruntime.TouchedDaysDrainDaysReturned); got != 1 {
			t.Fatalf("days_returned_to_pending = %d, want 1: the window day of the failed run of every repository stayed marked", got)
		}
		if got := rig.drainRunRepositories(t, ctx, org.orgID, failedKey); !reflect.DeepEqual(got, []string{org.repo.String()}) {
			t.Fatalf("repositories of the drain run of %s = %v, want the one repository the window marked", failedKey, got)
		}
		if got := newestTouchedAt(t, ctx, rig.touchedRig, org.orgID, kept, org.repo.String()); got != keptTouch {
			t.Fatalf("the key of a run of every repository that is not ended was returned: newest touch %d, want %d", got, keptTouch)
		}
		counts := drainRunsOf(t, ctx, rig, org.orgID)
		if totalRuns(counts) != len(org.left)+1 {
			t.Fatalf("drain runs by day = %v, want the %d days the fan-out left and the day of the failed run", counts, len(org.left))
		}
		if got := rig.pendingDays(t, ctx, org.orgID); len(got) != 0 {
			t.Fatalf("pending after the pass = %v, want none", got)
		}

		// A second pass after the runs ended starts nothing more.
		endDrainRuns(t, ctx, rig.touchedRig, org.orgID, "succeeded")
		rig.reset()
		rig.drain(t, nil, nil).DrainTouchedDays(ctx, org.orgID, pass("n"))
		if got := drainRunsOf(t, ctx, rig, org.orgID); !reflect.DeepEqual(got, counts) {
			t.Fatalf("drain runs after the second pass = %v, want no new run %v", got, counts)
		}
		if got := rig.observer.count(jobruntime.TouchedDaysDrainDaysReturned); got != 0 {
			t.Fatalf("days_returned_to_pending of the second pass = %d, want 0: the drain run owns the key now", got)
		}
	})
}
