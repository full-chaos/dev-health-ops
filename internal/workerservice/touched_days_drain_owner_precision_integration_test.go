//go:build integration

package workerservice

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily"
)

// Only a run of a post-sync fan-out or of the drain can be the owner of a
// key: only those runs mark keys. A newer run of another generation (here the
// nightly run of every repository, with a result) marks nothing and gives the
// key no result of the touch, so the key of the failed run before it goes back
// to pending. If the nightly run were the owner, the key would stay marked and
// its day would be lost.
func TestTouchedDaysDrainANewerRunThatMarksNothingIsNotTheOwnerOfAKey(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	rig := newDrainRig(t, ctx)
	orgID, repo := uuid.NewString(), uuid.NewString()
	day := time.Date(2026, 7, 30, 0, 0, 0, 0, time.UTC)
	dayKey := day.Format("2006-01-02")
	now := time.Now().UTC().Truncate(time.Millisecond)
	touchedEvent(t, ctx, rig.touchedRig, orgID, day, repo, "touched", now.Add(-12*time.Minute))
	touchedEvent(t, ctx, rig.touchedRig, orgID, day, repo, "dispatched", now.Add(-10*time.Minute))
	listedRun(t, ctx, rig.touchedRig, orgID, day, "post-sync:"+uuid.NewString(), repo,
		now.Add(-10*time.Minute), "failed", now.Add(-9*time.Minute))
	// The nightly run of the day: newer, of every repository, with a result.
	nightly := rig.startRun(t, ctx, func(tx pgx.Tx) (daily.Run, error) {
		return rig.store.StartRunTx(ctx, tx, daily.StartRunRequest{
			OrganizationID: orgID, TargetDay: day, Generation: daily.ScheduledFanoutGenerationPrefix + dayKey,
		}, nilPartitionPublisher{})
	})
	tag, err := rig.pool.Exec(ctx, `
UPDATE public.daily_metrics_runs
SET status = 'succeeded', finalization_status = 'succeeded', finalized_at = $2, updated_at = $2, created_at = $3
WHERE id = $1::uuid AND full_org`, nightly.ID, now.Add(-4*time.Minute), now.Add(-5*time.Minute))
	if err != nil || tag.RowsAffected() != 1 {
		t.Fatalf("end the nightly run of every repository: %v, %d rows", err, tag.RowsAffected())
	}

	rig.drain(t, nil, nil).DrainTouchedDays(ctx, orgID, pass("n"))

	if got := rig.observer.count(jobruntime.TouchedDaysDrainDaysReturned); got != 1 {
		t.Fatalf("days_returned_to_pending = %d, want 1: the nightly run owns the key of the failed run and the day is lost", got)
	}
	if got := rig.openDays(t, ctx, orgID); !reflect.DeepEqual(got, []string{dayKey}) {
		t.Fatalf("the pass started runs for %v, want %s", got, dayKey)
	}
}

// The return of a run of listed repositories names the keys the run lists and
// no other key of its day. A second key of the same day that an older run with
// a result lists stays marked: it gets no event and no run.
func TestTouchedDaysDrainReturnsOnlyTheKeysTheFailedRunLists(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	rig := newDrainRig(t, ctx)
	orgID := uuid.NewString()
	listed, other := uuid.NewString(), uuid.NewString()
	day := time.Date(2026, 7, 30, 0, 0, 0, 0, time.UTC)
	dayKey := day.Format("2006-01-02")
	now := time.Now().UTC().Truncate(time.Millisecond)
	for _, repo := range []string{listed, other} {
		touchedEvent(t, ctx, rig.touchedRig, orgID, day, repo, "touched", now.Add(-12*time.Minute))
		touchedEvent(t, ctx, rig.touchedRig, orgID, day, repo, "dispatched", now.Add(-10*time.Minute))
	}
	listedRun(t, ctx, rig.touchedRig, orgID, day, "post-sync:"+uuid.NewString(), other,
		now.Add(-11*time.Minute), "succeeded", now.Add(-10*time.Minute))
	listedRun(t, ctx, rig.touchedRig, orgID, day, "post-sync:"+uuid.NewString(), listed,
		now.Add(-10*time.Minute), "failed", now.Add(-9*time.Minute))

	rig.drain(t, nil, nil).DrainTouchedDays(ctx, orgID, pass("n"))

	if got := newestTouchedAt(t, ctx, rig.touchedRig, orgID, day, listed); got <= now.Add(-10*time.Minute).UnixMilli() {
		t.Fatalf("the key the failed run lists was not returned: newest touch %d", got)
	}
	if got := newestTouchedAt(t, ctx, rig.touchedRig, orgID, day, other); got != now.Add(-12*time.Minute).UnixMilli() {
		t.Fatalf("a key the failed run does not list was returned: newest touch %d, want %d", got, now.Add(-12*time.Minute).UnixMilli())
	}
	if got := rig.drainRunRepositories(t, ctx, orgID, dayKey); !reflect.DeepEqual(got, []string{listed}) {
		t.Fatalf("repositories of the drain runs of the day = %v, want one run of the key the failed run lists", got)
	}
}
