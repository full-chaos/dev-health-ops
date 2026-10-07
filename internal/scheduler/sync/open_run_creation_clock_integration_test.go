//go:build integration

package sync

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// These tests drive the production path end to end (HandoffDueResult, the
// occurrence reconciler, the native materializer), so every run row is written
// as production writes it: sync_runs.created_at is the occurrence's cron
// instant. Every observed-at instant is at or before the real clock, so the Go
// clock never runs ahead of the database now().

type openRunRig struct {
	t          *testing.T
	ctx        context.Context
	pool       *pgxpool.Pool
	fixture    materializerFixture
	repository *Repository
	reconciler *OccurrenceReconciler
}

func newOpenRunRig(t *testing.T, dueBase time.Time) openRunRig {
	t.Helper()
	fixture := startMaterializerPostgres(t)
	configureMaterializerFixtureAsDue(t, fixture, dueBase)
	repository, err := newRepositoryWithOwnership(fixture.pool, reviewedGoMutationOwnershipPolicy())
	if err != nil {
		t.Fatal(err)
	}
	materializer, err := NewNativeMaterializer(fixture.pool)
	if err != nil {
		t.Fatal(err)
	}
	reconciler, err := NewOccurrenceReconciler(fixture.pool, materializer)
	if err != nil {
		t.Fatal(err)
	}
	return openRunRig{t: t, ctx: context.Background(), pool: fixture.pool, fixture: fixture, repository: repository, reconciler: reconciler}
}

// window is one scheduler loop step: hand off, then plan.
func (rig openRunRig) window(at time.Time) HandoffResult {
	rig.t.Helper()
	result, err := rig.repository.HandoffDueResult(rig.ctx, at, 4, NewOccurrenceCoordinator())
	if err != nil {
		rig.t.Fatalf("window at %s: %v", at, err)
	}
	if _, err := rig.reconciler.Reconcile(rig.ctx, at, 10); err != nil {
		rig.t.Fatalf("reconcile at %s: %v", at, err)
	}
	return result
}

func (rig openRunRig) runOf(occurrenceID string) string {
	rig.t.Helper()
	var runID string
	if err := rig.pool.QueryRow(rig.ctx, `SELECT COALESCE(sync_run_id::text,'') FROM public.scheduled_sync_occurrences WHERE occurrence_id = $1`, occurrenceID).Scan(&runID); err != nil {
		rig.t.Fatal(err)
	}
	if runID == "" {
		rig.t.Fatalf("occurrence %s has no run", occurrenceID)
	}
	return runID
}

// heartbeat gives every unit of the run a heartbeat at the database now().
func (rig openRunRig) heartbeat(runID string) {
	rig.t.Helper()
	tag, err := rig.pool.Exec(rig.ctx, `UPDATE public.sync_run_units SET last_heartbeat_at = now() WHERE sync_run_id = $1::uuid`, runID)
	if err != nil {
		rig.t.Fatal(err)
	}
	if tag.RowsAffected() == 0 {
		rig.t.Fatalf("run %s has no unit", runID)
	}
}

func (rig openRunRig) dump(label string) {
	rig.t.Helper()
	rows, err := rig.pool.Query(rig.ctx, `
SELECT occurrence.scheduled_for, occurrence.created_at, COALESCE(run.status,''), run.created_at, run.completed_at,
       (SELECT min(before_at) FROM public.sync_run_units AS unit WHERE unit.sync_run_id = run.id),
       (SELECT count(*) FROM public.sync_run_units AS unit WHERE unit.sync_run_id = run.id),
       now()
FROM public.scheduled_sync_occurrences AS occurrence
LEFT JOIN public.sync_runs AS run ON run.id = occurrence.sync_run_id
ORDER BY occurrence.scheduled_for`)
	if err != nil {
		rig.t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var scheduledFor, occurrenceCreated, dbNow time.Time
		var status string
		var runCreated, completed, before *time.Time
		var units int
		if err := rows.Scan(&scheduledFor, &occurrenceCreated, &status, &runCreated, &completed, &before, &units, &dbNow); err != nil {
			rig.t.Fatal(err)
		}
		rig.t.Logf("%s: occurrence scheduled_for=%s occurrence.created_at=%s run.status=%q run.created_at=%s run.completed_at=%s units=%d unit.before_at=%s db_now=%s",
			label, stamp(&scheduledFor), stamp(&occurrenceCreated), status, stamp(runCreated), stamp(completed), units, stamp(before), stamp(&dbNow))
	}
}

func stamp(value *time.Time) string {
	if value == nil {
		return "NULL"
	}
	return value.UTC().Format(time.RFC3339)
}

func currentHour() time.Time {
	return time.Now().UTC().Add(-5 * time.Second).Truncate(time.Hour)
}

// A run older than the age cap lets one run start. That new run was made
// one simulated hour ago and has a heartbeat at the database now(): the next
// tick must skip.
func TestRunStartedPastTheAgeCapHoldsTheNextTick(t *testing.T) {
	hour := currentHour()
	first := hour.Add(-50 * time.Hour)
	rig := newOpenRunRig(t, first.Add(-30*time.Minute))

	one := rig.window(first.Add(time.Second))
	if one.Minted() != 1 {
		t.Fatalf("first window minted %d, want 1", one.Minted())
	}
	oldRun := rig.runOf(one.HandedOff[0].ID)
	rig.heartbeat(oldRun)

	two := rig.window(hour.Add(-time.Hour + time.Second))
	if two.Minted() != 1 || len(two.OpenRunsPastBound) != 1 || two.OpenRunsPastBound[0].Reason != OpenRunAgeCap {
		t.Fatalf("window beside the 50 h old run = %#v, want one start past the age cap", two)
	}
	newRun := rig.runOf(two.HandedOff[0].ID)
	rig.heartbeat(newRun)
	rig.dump("after the start past the age cap")

	three := rig.window(hour.Add(time.Second))
	rig.dump("after the next tick")
	if len(three.SkippedOpenRun) != 1 || three.Minted() != 0 {
		t.Fatalf("the run started one tick ago is open and has a heartbeat now, but the next tick: skipped=%d minted=%d past_bound=%#v handed_off=%#v; want one skip",
			len(three.SkippedOpenRun), three.Minted(), three.OpenRunsPastBound, three.HandedOff)
	}
}

// The scheduler was down for three hours. The first window after the stop
// starts one run. Its units wait for admission. Seventy simulated minutes
// later the next tick must skip: the run is inside the two hour interval that
// starts at its creation.
func TestRunStartedAfterASchedulerStopHoldsTheNextTick(t *testing.T) {
	hour := currentHour()
	missed := hour.Add(-3 * time.Hour)
	rig := newOpenRunRig(t, missed.Add(-30*time.Minute))

	one := rig.window(hour.Add(-70 * time.Minute))
	if one.Minted() != 1 {
		t.Fatalf("first window minted %d, want 1", one.Minted())
	}
	_ = rig.runOf(one.HandedOff[0].ID)
	rig.dump("after the first window")

	two := rig.window(hour.Add(time.Second))
	rig.dump("after the next tick")
	if len(two.SkippedOpenRun) != 1 || two.Minted() != 0 {
		t.Fatalf("the run was created 70 minutes ago and is open, but the next tick: skipped=%d minted=%d past_bound=%#v; want one skip",
			len(two.SkippedOpenRun), two.Minted(), two.OpenRunsPastBound)
	}
}

// A daily schedule: the run of yesterday's instant is open with a heartbeat
// now. Today's tick must skip: the age cap is longer than one day.
func TestDailyScheduleSkipsTheTickWhileYesterdaysRunIsOpen(t *testing.T) {
	hour := currentHour()
	yesterday := hour.Add(-24 * time.Hour)
	rig := newOpenRunRig(t, yesterday.Add(-30*time.Minute))
	cron := fmt.Sprintf("0 %d * * *", hour.Hour())
	if _, err := rig.pool.Exec(rig.ctx, `UPDATE public.sync_configurations SET sync_options = json_build_object('schedule_cron', $2::text) WHERE id = $1::uuid`, rig.fixture.occurrence.ConfigID, cron); err != nil {
		t.Fatal(err)
	}
	if _, err := rig.pool.Exec(rig.ctx, `UPDATE public.scheduled_jobs SET schedule_cron = $2 WHERE sync_config_id = $1::uuid`, rig.fixture.occurrence.ConfigID, cron); err != nil {
		t.Fatal(err)
	}
	one := rig.window(yesterday.Add(time.Second))
	if one.Minted() != 1 {
		t.Fatalf("first window minted %d, want 1", one.Minted())
	}
	rig.heartbeat(rig.runOf(one.HandedOff[0].ID))

	two := rig.window(hour.Add(time.Second))
	rig.dump("after today's tick")
	if len(two.SkippedOpenRun) != 1 || two.Minted() != 0 {
		t.Fatalf("daily schedule, yesterday's run open with a heartbeat now: skipped=%d minted=%d past_bound=%#v; want one skip",
			len(two.SkippedOpenRun), two.Minted(), two.OpenRunsPastBound)
	}
}

// Two scheduled runs are open (a pile from before the rule). The newer one
// ends first, before the next cron instant. The older one holds four ticks
// back, then ends. The run that starts next must stand for the newest due
// instant and its unit window must reach it.
func TestRunAfterAPileStandsForTheNewestDueInstant(t *testing.T) {
	hour := currentHour()
	first := hour.Add(-6 * time.Hour)
	rig := newOpenRunRig(t, first.Add(-30*time.Minute))

	one := rig.window(first.Add(time.Second))
	if one.Minted() != 1 {
		t.Fatalf("first window minted %d, want 1", one.Minted())
	}
	older := rig.runOf(one.HandedOff[0].ID)
	// The older run shows no progress for six hours on the database clock, so
	// the second run starts beside it: the pile.
	two := rig.window(first.Add(time.Hour + time.Second))
	if two.Minted() != 1 {
		t.Fatalf("second window = %#v, want the second run of the pile", two)
	}
	newer := rig.runOf(two.HandedOff[0].ID)
	endScheduledRun(t, rig.pool, newer, "success", first.Add(time.Hour+20*time.Minute))
	rig.heartbeat(older)

	for offset := 2; offset <= 5; offset++ {
		skip := rig.window(first.Add(time.Duration(offset)*time.Hour + time.Second))
		if len(skip.SkippedOpenRun) != 1 || skip.Minted() != 0 {
			t.Fatalf("tick %d with the older run open = %#v, want a skip", offset, skip)
		}
	}
	endScheduledRun(t, rig.pool, older, "success", first.Add(5*time.Hour+20*time.Minute))
	watermark := first.Add(-10 * time.Minute)
	if _, err := rig.pool.Exec(rig.ctx, `UPDATE public.sync_watermarks SET last_synced_at = $2 WHERE org_id = $1 AND dataset_key = 'commits'`, rig.fixture.occurrence.OrgID, watermark); err != nil {
		t.Fatal(err)
	}

	resumeAt := hour.Add(time.Second)
	resumed := rig.window(resumeAt)
	rig.dump("after the resume tick")
	if resumed.Minted() != 1 {
		t.Fatalf("resume tick = %#v, want one run", resumed)
	}
	got := resumed.HandedOff[0].ScheduledFor
	var before *time.Time
	if err := rig.pool.QueryRow(rig.ctx, `SELECT min(before_at) FROM public.sync_run_units WHERE sync_run_id = $1::uuid`, rig.runOf(resumed.HandedOff[0].ID)).Scan(&before); err != nil {
		t.Fatal(err)
	}
	if !got.Equal(hour) || before == nil || before.Before(hour) {
		t.Fatalf("after four skipped ticks the next run stands for %s and its unit window ends at %s; want the newest due instant %s",
			stamp(&got), stamp(before), stamp(&hour))
	}
}

// The same scheduler stop, on an every-minute schedule so that every simulated instant is
// within seconds of the database clock: the scheduler was down for three
// hours, the first window after the stop starts one run 61 seconds ago, its
// units wait, and the next minute's tick must skip.
func TestRunStartedAfterASchedulerStopHoldsTheNextTickMinuteCron(t *testing.T) {
	minute := time.Now().UTC().Add(-5 * time.Second).Truncate(time.Minute)
	rig := newOpenRunRig(t, minute.Add(-3*time.Hour-30*time.Second))
	for _, statement := range []string{
		`UPDATE public.sync_configurations SET sync_options = json_build_object('schedule_cron', '* * * * *') WHERE id = $1::uuid`,
		`UPDATE public.scheduled_jobs SET schedule_cron = '* * * * *' WHERE sync_config_id = $1::uuid`,
	} {
		if _, err := rig.pool.Exec(rig.ctx, statement, rig.fixture.occurrence.ConfigID); err != nil {
			t.Fatal(err)
		}
	}
	one := rig.window(minute.Add(-61 * time.Second))
	if one.Minted() != 1 {
		t.Fatalf("first window minted %d, want 1", one.Minted())
	}
	_ = rig.runOf(one.HandedOff[0].ID)
	rig.dump("after the first window")
	two := rig.window(minute.Add(time.Second))
	rig.dump("after the next tick")
	if len(two.SkippedOpenRun) != 1 || two.Minted() != 0 {
		t.Fatalf("the run was created 62 seconds ago and is open, but the next tick: skipped=%d minted=%d past_bound=%#v; want one skip",
			len(two.SkippedOpenRun), two.Minted(), two.OpenRunsPastBound)
	}
}
