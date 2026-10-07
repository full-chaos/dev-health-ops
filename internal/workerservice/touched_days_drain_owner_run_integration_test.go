//go:build integration

package workerservice

import (
	"bytes"
	"context"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily"
	"github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime"
)

// The counters these tests read, by their exported label.
const (
	drainEventDaysRetried  = jobruntime.TouchedDaysDrainEvent("days_retried_after_failed_runs")
	drainEventChainStopped = jobruntime.TouchedDaysDrainEvent("chain_stopped_mark_missing")
)

func newDrainRig(t *testing.T, ctx context.Context) *drainRig {
	t.Helper()
	return &drainRig{touchedRig: newTouchedRig(t, ctx), observer: &drainObserver{}, logs: &bytes.Buffer{}}
}

// drainRunsOf counts the drain runs of the organization, by day.
func drainRunsOf(t *testing.T, ctx context.Context, rig *drainRig, orgID string) map[string]int {
	t.Helper()
	rows, err := rig.pool.Query(ctx, `
SELECT target_day::text, count(*) FROM public.daily_metrics_runs
WHERE org_id = $1::uuid AND generation LIKE 'touched-drain:%' GROUP BY target_day`, orgID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	counts := map[string]int{}
	for rows.Next() {
		var (
			day   string
			count int
		)
		if err := rows.Scan(&day, &count); err != nil {
			t.Fatal(err)
		}
		counts[day] = count
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return counts
}

func totalRuns(counts map[string]int) int {
	total := 0
	for _, count := range counts {
		total += count
	}
	return total
}

// newestTouchedAt is the time of the newest 'touched' event of one key, in
// milliseconds.
func newestTouchedAt(t *testing.T, ctx context.Context, rig *touchedRig, orgID string, day time.Time, repo string) int64 {
	t.Helper()
	var millis int64
	if err := rig.conn.QueryRow(ctx, `
SELECT toUnixTimestamp64Milli(max(at)) FROM daily_metrics_touched_days
WHERE org_id = ? AND toString(day) = ? AND toString(repo_id) = ? AND kind = 'touched'`,
		orgID, day.Format("2006-01-02"), repo).Scan(&millis); err != nil {
		t.Fatal(err)
	}
	return millis
}

// The return of the days of a run without a result does not depend on the two
// clocks agreeing. The marks carry the ClickHouse clock and the end of a run
// the worker clock; each case sets them apart in one direction.
func TestTouchedDaysDrainReturnsTheKeysOfAFailedRunWhateverTheClockSkew(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	rig := newDrainRig(t, ctx)
	day := time.Date(2026, 7, 30, 0, 0, 0, 0, time.UTC)
	dayKey := day.Format("2006-01-02")

	cases := []struct {
		name string
		// markAfterNow is the time of the mark of the failed run, as an offset
		// from the worker clock.
		markAfterNow time.Duration
		// createdAfterNow and failedAfterNow are the creation and the end of
		// the run, as offsets from the worker clock.
		createdAfterNow, failedAfterNow time.Duration
	}{
		{name: "the mark is 30 seconds ahead of the end of the run",
			markAfterNow: 30 * time.Second, createdAfterNow: -time.Minute, failedAfterNow: -59 * time.Second},
		{name: "the mark is 30 seconds behind the creation of the run",
			markAfterNow: -90 * time.Second, createdAfterNow: -time.Minute, failedAfterNow: -30 * time.Second},
		{name: "the end of the run is one hour behind its mark",
			markAfterNow: -time.Minute, createdAfterNow: -time.Hour, failedAfterNow: -time.Hour + time.Second},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			rig.reset()
			orgID, repo := uuid.NewString(), uuid.NewString()
			now := time.Now().UTC().Truncate(time.Millisecond)
			touchedEvent(t, ctx, rig.touchedRig, orgID, day, repo, "touched", now.Add(test.markAfterNow-time.Minute))
			touchedEvent(t, ctx, rig.touchedRig, orgID, day, repo, "dispatched", now.Add(test.markAfterNow))
			listedRun(t, ctx, rig.touchedRig, orgID, day, "post-sync:"+uuid.NewString(), repo,
				now.Add(test.createdAfterNow), "failed", now.Add(test.failedAfterNow))
			if got := rig.pendingDays(t, ctx, orgID); len(got) != 0 {
				t.Fatalf("pending before the pass = %v, want none (the failed run marked its key)", got)
			}

			rig.drain(t, nil, nil).DrainTouchedDays(ctx, orgID, pass("n"))

			if got := rig.openDays(t, ctx, orgID); !reflect.DeepEqual(got, []string{dayKey}) {
				t.Fatalf("the pass after the failed run started runs for %v, want %s: the day of the failed run stayed marked and is lost", got, dayKey)
			}
			if got := rig.observer.count(jobruntime.TouchedDaysDrainDaysReturned); got != 1 {
				t.Fatalf("days_returned_to_pending = %d, want 1", got)
			}
		})
	}
}

// A run without a result gives its keys back however long ago it was created:
// no pass has to see it inside a number of hours.
func TestTouchedDaysDrainReturnsTheKeysOfARunWithoutAResultOfAnyAge(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	rig := newDrainRig(t, ctx)
	day := time.Date(2026, 7, 30, 0, 0, 0, 0, time.UTC)
	dayKey := day.Format("2006-01-02")

	for _, test := range []struct {
		name    string
		created time.Duration
		status  string
	}{
		{name: "failed, created 73 hours ago", created: 73 * time.Hour, status: "failed"},
		{name: "canceled, created 30 days ago", created: 30 * 24 * time.Hour, status: "canceled"},
		{name: "never ended, created 73 hours ago", created: 73 * time.Hour, status: ""},
		{name: "never ended, created 400 days ago", created: 400 * 24 * time.Hour, status: ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			rig.reset()
			orgID, repo := uuid.NewString(), uuid.NewString()
			created := time.Now().UTC().Add(-test.created)
			touchedEvent(t, ctx, rig.touchedRig, orgID, day, repo, "touched", created.Add(-2*time.Minute))
			touchedEvent(t, ctx, rig.touchedRig, orgID, day, repo, "dispatched", created.Add(-time.Minute))
			listedRun(t, ctx, rig.touchedRig, orgID, day, daily.TouchedDrainGenerationPrefix+"n:"+uuid.NewString(), repo,
				created, test.status, created.Add(time.Minute))

			rig.drain(t, nil, nil).DrainTouchedDays(ctx, orgID, pass("n"))

			if got := drainRunsOf(t, ctx, rig, orgID)[dayKey]; got != 2 {
				t.Fatalf("drain runs of %s = %d, want 2 (the old run and one new run): the key of the old run stayed marked with no run", dayKey, got)
			}
			if got := rig.observer.count(jobruntime.TouchedDaysDrainDaysReturned); got != 1 {
				t.Fatalf("days_returned_to_pending = %d, want 1", got)
			}
		})
	}
}

// ageAllRuns moves every run of the organization back in time.
func ageAllRuns(t *testing.T, ctx context.Context, rig *drainRig, orgID string, by time.Duration) {
	t.Helper()
	if _, err := rig.pool.Exec(ctx, `
UPDATE public.daily_metrics_runs
SET created_at = created_at - make_interval(secs => $2), updated_at = updated_at - make_interval(secs => $2),
    finalized_at = finalized_at - make_interval(secs => $2)
WHERE org_id = $1::uuid`, orgID, by.Seconds()); err != nil {
		t.Fatal(err)
	}
}

// A day whose runs keep failing is not left for an operator: the drain starts
// one more run for it 24 hours after its newest run. Between two such runs it
// starts none, so a set of days that always fail ends its chain after one run
// each. The days stay reported and keep the gauge of the oldest pending touch
// above zero.
func TestTouchedDaysDrainRetriesASkippedDayOnceInADayAndTheChainEnds(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	rig := newDrainRig(t, ctx)
	orgID := uuid.NewString()
	repo := uuid.NewString()
	days := []time.Time{
		time.Date(2026, 7, 28, 0, 0, 0, 0, time.UTC),
		time.Date(2026, 7, 29, 0, 0, 0, 0, time.UTC),
		time.Date(2026, 7, 30, 0, 0, 0, 0, time.UTC),
	}
	now := time.Now().UTC()
	for _, day := range days {
		touchedEvent(t, ctx, rig.touchedRig, orgID, day, repo, "touched", now.Add(-48*time.Hour))
		for run := 0; run < syncdispatchruntime.TouchedDrainFailedRunsBeforeSkip; run++ {
			created := now.Add(-time.Duration(30-run) * time.Hour)
			listedRun(t, ctx, rig.touchedRig, orgID, day, daily.TouchedDrainGenerationPrefix+"e:"+uuid.NewString(), repo,
				created, "failed", created.Add(time.Minute))
		}
	}
	const before = 3 * syncdispatchruntime.TouchedDrainFailedRunsBeforeSkip
	if got := totalRuns(drainRunsOf(t, ctx, rig, orgID)); got != before {
		t.Fatalf("seeded drain runs = %d, want %d", got, before)
	}
	drain := rig.drain(t, nil, nil)

	// The newest run of each day is 28 hours old: each day gets one run.
	drain.DrainTouchedDays(ctx, orgID, pass("e"))
	if got := totalRuns(drainRunsOf(t, ctx, rig, orgID)); got != before+3 {
		t.Fatalf("drain runs after the first pass = %d, want %d: one more run for each of the 3 days whose newest run is older than 24 hours", got, before+3)
	}
	if got := rig.observer.count(drainEventDaysRetried); got != 3 {
		t.Fatalf("days_retried_after_failed_runs = %d, want 3", got)
	}

	// Every one of them fails. The end of each triggers a pass.
	endedRuns, _ := openDrainRuns(t, ctx, rig.touchedRig, orgID)
	endDrainRuns(t, ctx, rig.touchedRig, orgID, "failed")
	for range endedRuns {
		rig.reset()
		drain = rig.drain(t, nil, nil)
		drain.DrainTouchedDays(ctx, orgID, pass("e"))
		if got := totalRuns(drainRunsOf(t, ctx, rig, orgID)); got != before+3 {
			t.Fatalf("drain runs after a pass of the same chain = %d, want %d: the chain must end after one retry of each day", got, before+3)
		}
		if got := rig.observer.count(jobruntime.TouchedDaysDrainDaysSkipped); got != 3 {
			t.Fatalf("days_skipped_after_failed_runs = %d, want 3", got)
		}
		if got := rig.logLines("touched_days_drain.days_skipped_after_failed_runs"); len(got) != 1 || !strings.Contains(got[0], `"level":"ERROR"`) {
			t.Fatalf("skipped lines = %v, want one Error line", got)
		}
		// The age counts from the return of the keys of the failed run, not
		// from the first touch: the table keeps the newest event of a key.
		if got := rig.observer.ages[orgID]; got <= 0 {
			t.Fatalf("age of the oldest pending touch = %s, want more than 0: a skipped day is still a pending day", got)
		}
	}
	if got := rig.pendingDays(t, ctx, orgID); len(got) != 3 {
		t.Fatalf("pending days = %v, want the 3 days", got)
	}

	// 25 hours later the next pass starts one run for each again.
	ageAllRuns(t, ctx, rig, orgID, 25*time.Hour)
	rig.reset()
	drain = rig.drain(t, nil, nil)
	drain.DrainTouchedDays(ctx, orgID, pass("n"))
	if got := totalRuns(drainRunsOf(t, ctx, rig, orgID)); got != before+6 {
		t.Fatalf("drain runs one day later = %d, want %d: one more run for each day", got, before+6)
	}

	// The cause is gone: the runs succeed and the days are done.
	endDrainRuns(t, ctx, rig.touchedRig, orgID, "succeeded")
	drain.DrainTouchedDays(ctx, orgID, pass("e"))
	if got := rig.pendingDays(t, ctx, orgID); len(got) != 0 {
		t.Fatalf("pending days after the runs succeeded = %v, want none", got)
	}
	if got := totalRuns(drainRunsOf(t, ctx, rig, orgID)); got != before+6 {
		t.Fatalf("drain runs after the success = %d, want %d", got, before+6)
	}
}

// While the mark of a pass does not reach ClickHouse the chain ends: the pass
// that the end of its runs triggers starts nothing, so the same newest days
// are not started again and again. When the mark works again the nightly pass
// starts them once more and the chain reaches the older days.
func TestTouchedDaysDrainEndsTheChainWhenTheMarkOfAPassIsMissing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	rig := newDrainRig(t, ctx)
	orgID := uuid.NewString()
	newest := time.Date(2026, 7, 31, 0, 0, 0, 0, time.UTC)
	days := seedPendingDays(t, ctx, rig.touchedRig, orgID, newest, 40)
	perPass := syncdispatchruntime.TouchedDaysPerDrainPass

	failing := rig.drain(t, drainFaultStore{TouchedDaysDrainStore: rig.touched, failMark: true}, nil)
	failing.DrainTouchedDays(ctx, orgID, pass("n"))
	if got := totalRuns(drainRunsOf(t, ctx, rig, orgID)); got != perPass {
		t.Fatalf("runs of the first pass = %d, want %d", got, perPass)
	}
	if got := rig.observer.count(jobruntime.TouchedDaysDrainMarkFailed); got != 1 {
		t.Fatalf("mark_failed = %d, want 1", got)
	}
	_, endedRun := openDrainRuns(t, ctx, rig.touchedRig, orgID)
	endDrainRuns(t, ctx, rig.touchedRig, orgID, "succeeded")

	for attempt := 0; attempt < 3; attempt++ {
		rig.reset()
		failing = rig.drain(t, drainFaultStore{TouchedDaysDrainStore: rig.touched, failMark: true}, nil)
		failing.DrainTouchedDays(ctx, orgID, "e:"+endedRun)
		if got := totalRuns(drainRunsOf(t, ctx, rig, orgID)); got != perPass {
			t.Fatalf("runs after the pass that the end of a run triggered = %d, want %d: the pass started the same days again because their mark is missing", got, perPass)
		}
		if got := rig.observer.count(drainEventChainStopped); got != 1 {
			t.Fatalf("chain_stopped_mark_missing = %d, want 1", got)
		}
		lines := rig.logLines("touched_days_drain.failed")
		if len(lines) != 1 || !strings.Contains(lines[0], `"level":"ERROR"`) || !strings.Contains(lines[0], "chain_stopped_mark_missing") {
			t.Fatalf("failure lines = %v, want one Error line of the stopped chain", lines)
		}
	}
	if got := rig.pendingDays(t, ctx, orgID); len(got) != len(days) {
		t.Fatalf("pending days = %d, want all %d", len(got), len(days))
	}

	// The mark works again. The nightly pass starts the newest days once
	// more, and the pass after their end reaches the 9 older days.
	rig.reset()
	working := rig.drain(t, nil, nil)
	working.DrainTouchedDays(ctx, orgID, pass("n"))
	_, endedRun = openDrainRuns(t, ctx, rig.touchedRig, orgID)
	endDrainRuns(t, ctx, rig.touchedRig, orgID, "succeeded")
	working.DrainTouchedDays(ctx, orgID, "e:"+endedRun)
	counts := drainRunsOf(t, ctx, rig, orgID)
	for index, key := range days {
		want := 2
		if index < len(days)-perPass {
			want = 1
		}
		if counts[key] != want {
			t.Fatalf("drain runs of %s = %d, want %d (runs by day: %v)", key, counts[key], want, counts)
		}
	}
	if got := rig.observer.count(drainEventChainStopped); got != 0 {
		t.Fatalf("chain_stopped_mark_missing with a working mark = %d, want 0", got)
	}
	endDrainRuns(t, ctx, rig.touchedRig, orgID, "succeeded")
	if got := rig.pendingDays(t, ctx, orgID); len(got) != 0 {
		t.Fatalf("pending days at the end = %v, want none", got)
	}
}

// afterTakeStore calls back once, after the pass read its pending days.
type afterTakeStore struct {
	syncdispatchruntime.TouchedDaysDrainStore
	once      *sync.Once
	afterTake func()
}

func (store afterTakeStore) PendingRepositories(ctx context.Context, org string, days []time.Time, limit int) (map[string][]string, error) {
	repositories, err := store.TouchedDaysDrainStore.PendingRepositories(ctx, org, days, limit)
	store.once.Do(store.afterTake)
	return repositories, err
}

// The mark of a pass carries the time its pending days were read, not the time
// of the mark: a key that is touched after the read and before the mark stays
// pending.
func TestTouchedDaysDrainMarkDoesNotEndAKeyTouchedAfterTheRead(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	rig := newDrainRig(t, ctx)
	orgID, repo := uuid.NewString(), uuid.NewString()
	day := time.Date(2026, 7, 30, 0, 0, 0, 0, time.UTC)
	touchedEvent(t, ctx, rig.touchedRig, orgID, day, repo, "touched", time.Now().UTC().Add(-time.Hour))
	store := afterTakeStore{TouchedDaysDrainStore: rig.touched, once: &sync.Once{}, afterTake: func() {
		time.Sleep(50 * time.Millisecond)
		var millis int64
		if err := rig.conn.QueryRow(ctx, `SELECT toUnixTimestamp64Milli(now64(3))`).Scan(&millis); err != nil {
			t.Error(err)
			return
		}
		touchedEvent(t, ctx, rig.touchedRig, orgID, day, repo, "touched", time.UnixMilli(millis))
		time.Sleep(50 * time.Millisecond)
	}}

	rig.drain(t, store, nil).DrainTouchedDays(ctx, orgID, pass("n"))

	if got := totalRuns(drainRunsOf(t, ctx, rig, orgID)); got != 1 {
		t.Fatalf("drain runs = %d, want 1", got)
	}
	if got := touchedEventCount(t, ctx, rig.touchedRig, orgID, day, repo, "dispatched"); got != 1 {
		t.Fatalf("dispatched events = %d, want 1 (the pass marked its key)", got)
	}
	if got := rig.pendingDays(t, ctx, orgID); !reflect.DeepEqual(got, []string{day.Format("2006-01-02")}) {
		t.Fatalf("pending days = %v, want the day: it was touched after the read, and the run may not have read that row", got)
	}
}

// The return writes only the keys of its own organization, and gives a key
// that is pending already no new event.
func TestTouchedDaysDrainReturnWritesOnlyTheMarkedKeysOfItsOrganization(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	rig := newDrainRig(t, ctx)
	day := time.Date(2026, 7, 30, 0, 0, 0, 0, time.UTC)
	repo, pendingRepo := uuid.NewString(), uuid.NewString()
	failedOrg, otherOrg := uuid.NewString(), uuid.NewString()
	now := time.Now().UTC().Truncate(time.Millisecond)

	// Both organizations have the same (day, repository) key, marked. Only the
	// run of the first one failed.
	for _, orgID := range []string{failedOrg, otherOrg} {
		touchedEvent(t, ctx, rig.touchedRig, orgID, day, repo, "touched", now.Add(-12*time.Minute))
		touchedEvent(t, ctx, rig.touchedRig, orgID, day, repo, "dispatched", now.Add(-10*time.Minute))
	}
	listedRun(t, ctx, rig.touchedRig, failedOrg, day, daily.TouchedDrainGenerationPrefix+"e:"+uuid.NewString(), repo,
		now.Add(-10*time.Minute), "failed", now.Add(-6*time.Minute))
	listedRun(t, ctx, rig.touchedRig, otherOrg, day, daily.TouchedDrainGenerationPrefix+"e:"+uuid.NewString(), repo,
		now.Add(-10*time.Minute), "succeeded", now.Add(-6*time.Minute))
	// A second failed run of the first organization lists a key that is
	// pending: its mark never reached the table.
	pendingSince := now.Add(-3 * time.Hour)
	otherDay := day.AddDate(0, 0, -1)
	touchedEvent(t, ctx, rig.touchedRig, failedOrg, otherDay, pendingRepo, "touched", pendingSince)
	for run := 0; run < syncdispatchruntime.TouchedDrainFailedRunsBeforeSkip; run++ {
		created := now.Add(-time.Duration(20-run) * time.Minute)
		listedRun(t, ctx, rig.touchedRig, failedOrg, otherDay, daily.TouchedDrainGenerationPrefix+"e:"+uuid.NewString(), pendingRepo,
			created, "failed", created.Add(time.Minute))
	}

	rig.drain(t, nil, nil).DrainTouchedDays(ctx, failedOrg, pass("n"))

	if got := touchedEventCount(t, ctx, rig.touchedRig, failedOrg, day, repo, "touched"); got != 1 {
		t.Fatalf("touched events of the returned key = %d, want 1", got)
	}
	if got := newestTouchedAt(t, ctx, rig.touchedRig, failedOrg, day, repo); got <= now.Add(-10*time.Minute).UnixMilli() {
		t.Fatalf("the key of the failed run was not returned: its newest touch is %d", got)
	}
	if got := rig.pendingDays(t, ctx, otherOrg); len(got) != 0 {
		t.Fatalf("pending days of the other organization = %v, want none: the return wrote a key of another organization", got)
	}
	if got := newestTouchedAt(t, ctx, rig.touchedRig, otherOrg, day, repo); got != now.Add(-12*time.Minute).UnixMilli() {
		t.Fatalf("newest touch of the other organization = %d, want %d (not written)", got, now.Add(-12*time.Minute).UnixMilli())
	}
	if got := newestTouchedAt(t, ctx, rig.touchedRig, failedOrg, otherDay, pendingRepo); got != pendingSince.UnixMilli() {
		t.Fatalf("newest touch of the key that was pending = %d, want %d: the time it has waited must not move", got, pendingSince.UnixMilli())
	}
	if got := rig.observer.count(jobruntime.TouchedDaysDrainDaysReturned); got != 1 {
		t.Fatalf("days_returned_to_pending = %d, want 1 (the day with the pending key is not counted)", got)
	}
}

// A run of every repository that failed returns every marked key of its day
// but the keys a newer run lists: that run is their owner.
func TestTouchedDaysDrainReturnsTheKeysOfAFailedRunOfEveryRepositoryButTheOnesANewerRunLists(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	rig := newDrainRig(t, ctx)
	orgID := uuid.NewString()
	day := time.Date(2026, 7, 30, 0, 0, 0, 0, time.UTC)
	dayKey := day.Format("2006-01-02")
	first, second := uuid.NewString(), uuid.NewString()
	now := time.Now().UTC().Truncate(time.Millisecond)
	for _, repo := range []string{first, second} {
		touchedEvent(t, ctx, rig.touchedRig, orgID, day, repo, "touched", now.Add(-12*time.Minute))
		touchedEvent(t, ctx, rig.touchedRig, orgID, day, repo, "dispatched", now.Add(-10*time.Minute))
	}
	// The run of every repository of a fan-out: no list.
	full := rig.startRun(t, ctx, func(tx pgx.Tx) (daily.Run, error) {
		return rig.store.StartRunTx(ctx, tx, daily.StartRunRequest{
			OrganizationID: orgID, TargetDay: day, Generation: "post-sync:" + uuid.NewString(),
		}, nilPartitionPublisher{})
	})
	if _, err := rig.pool.Exec(ctx, `
UPDATE public.daily_metrics_runs
SET status = 'failed', finalization_status = 'failed', finalized_at = $2, updated_at = $2, created_at = $3
WHERE id = $1::uuid AND full_org`, full.ID, now.Add(-8*time.Minute), now.Add(-10*time.Minute)); err != nil {
		t.Fatal(err)
	}
	// A newer run lists the second key and has a result.
	listedRun(t, ctx, rig.touchedRig, orgID, day, daily.TouchedDrainGenerationPrefix+"e:"+uuid.NewString(), second,
		now.Add(-5*time.Minute), "succeeded", now.Add(-4*time.Minute))

	rig.drain(t, nil, nil).DrainTouchedDays(ctx, orgID, pass("n"))

	if got := newestTouchedAt(t, ctx, rig.touchedRig, orgID, day, first); got <= now.Add(-10*time.Minute).UnixMilli() {
		t.Fatalf("the key that only the failed run of every repository lists was not returned: newest touch %d", got)
	}
	if got := newestTouchedAt(t, ctx, rig.touchedRig, orgID, day, second); got != now.Add(-12*time.Minute).UnixMilli() {
		t.Fatalf("the key that a newer run with a result lists was returned: newest touch %d, want %d", got, now.Add(-12*time.Minute).UnixMilli())
	}
	if got := drainRunsOf(t, ctx, rig, orgID)[dayKey]; got != 2 {
		t.Fatalf("drain runs of the day = %d, want 2 (the newer run and one run for the returned key)", got)
	}
}

// A failed run of a pass whose mark reached the table does not end the chain:
// only a run with a result whose keys are all pending says that the mark is
// missing.
func TestTouchedDaysDrainFailedRunOfAMarkedPassDoesNotEndTheChain(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	rig := newDrainRig(t, ctx)
	orgID := uuid.NewString()
	newest := time.Date(2026, 7, 31, 0, 0, 0, 0, time.UTC)
	days := seedPendingDays(t, ctx, rig.touchedRig, orgID, newest, 2)
	drain := rig.drain(t, nil, nil)
	drain.DrainTouchedDays(ctx, orgID, pass("n"))
	_, endedRun := openDrainRuns(t, ctx, rig.touchedRig, orgID)
	// The run of the newest day fails, the other one succeeds.
	if _, err := rig.pool.Exec(ctx, `
UPDATE public.daily_metrics_runs
SET status = 'failed', finalization_status = 'failed', finalized_at = clock_timestamp(), updated_at = clock_timestamp()
WHERE org_id = $1::uuid AND target_day = $2::date`, orgID, days[1]); err != nil {
		t.Fatal(err)
	}
	endDrainRuns(t, ctx, rig.touchedRig, orgID, "succeeded")

	drain.DrainTouchedDays(ctx, orgID, "e:"+endedRun)

	if got := rig.observer.count(drainEventChainStopped); got != 0 {
		t.Fatalf("chain_stopped_mark_missing = %d, want 0: the mark of the pass reached the table", got)
	}
	counts := drainRunsOf(t, ctx, rig, orgID)
	if counts[days[1]] != 2 || counts[days[0]] != 1 {
		t.Fatalf("drain runs by day = %v, want 2 for %s (the failed run and its restart) and 1 for %s", counts, days[1], days[0])
	}
}

// The read of the runs without a result is bounded, and a pass that hits the
// bound says so: no run behind the bound is left without a line.
func TestTouchedDaysDrainReportsAFullReadOfTheRunsWithoutAResult(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	rig := newDrainRig(t, ctx)
	orgID := uuid.NewString()
	// One failed drain run for each of 3661 days, each with one repository.
	if _, err := rig.pool.Exec(ctx, `
WITH runs AS (
    INSERT INTO public.daily_metrics_runs
        (id, org_id, target_day, generation, status, finalization_status, created_at, updated_at, full_org)
    SELECT gen_random_uuid(), $1::uuid, DATE '2026-07-31' - step, 'touched-drain:e:' || gen_random_uuid()::text,
           'failed', 'failed', clock_timestamp() - make_interval(hours => step), clock_timestamp(), false
    FROM generate_series(1, 3661) AS step
    RETURNING id
)
INSERT INTO public.daily_metrics_partitions (id, run_id, ordinal, repo_ids, status, attempt_count, created_at, updated_at)
SELECT gen_random_uuid(), id, 0, json_build_array(gen_random_uuid()::text), 'failed', 0, clock_timestamp(), clock_timestamp()
FROM runs`, orgID); err != nil {
		t.Fatal(err)
	}

	rig.drain(t, nil, nil).DrainTouchedDays(ctx, orgID, pass("n"))

	if got := rig.observer.count(jobruntime.TouchedDaysDrainEvent("return_read_truncated")); got != 1 {
		t.Fatalf("return_read_truncated = %d, want 1", got)
	}
	var lines []string
	for _, line := range rig.logLines("touched_days_drain.failed") {
		if strings.Contains(line, "return_read_truncated") && strings.Contains(line, `"level":"ERROR"`) {
			lines = append(lines, line)
		}
	}
	if len(lines) != 1 {
		t.Fatalf("Error lines of the full read = %v, want 1", lines)
	}
}
