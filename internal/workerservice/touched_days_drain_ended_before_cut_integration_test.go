//go:build integration

package workerservice

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime"
)

// A fan-out run that ended after the second newest batch of the drain began, but
// whose mark failed, must stop the chain at its end however old its created_at
// is: a run is read when it ends, not when it was created.
func TestReviewEndedRunBeforeStopCheckCutIsNotSilentlySkipped(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	rig := newDrainRig(t, ctx)
	rig.reset()

	target := time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC)
	orgID := uuid.NewString()
	var items []touchedItem
	now := time.Now().UTC()
	for offset := 1; offset <= 40; offset++ {
		items = append(items, touchedItem{repo: uuid.Nil, id: fmt.Sprintf("linear:OPS-%d", offset), provider: "linear",
			day: target.AddDate(0, 0, -20-offset), synced: now})
	}
	insertTouchedItems(t, ctx, rig.conn, orgID, items...)
	args := rig.seedSync(t, ctx, orgID, "work-items", target)
	failing := touchedFaultStore{TouchedDaysStore: rig.touched, failMark: true}
	if err := rig.service(t, failing, nil).Fanout(ctx, args); err != nil {
		t.Fatal(err)
	}
	if _, err := rig.pool.Exec(ctx, `
UPDATE public.daily_metrics_runs
SET status = 'succeeded', finalization_status = 'succeeded', finalized_at = clock_timestamp(),
    created_at = $2, updated_at = $2
WHERE org_id = $1::uuid`, orgID, time.Now().UTC().Add(-6*time.Hour)); err != nil {
		t.Fatal(err)
	}
	_, newest := drainBatches(t, ctx, rig, orgID, 0)

	before := totalRuns(drainRunsOf(t, ctx, rig, orgID))
	rig.drain(t, nil, nil).DrainTouchedDays(ctx, orgID, "e:"+newest[0])
	if got := rig.observer.count(drainEventChainStopped); got != 1 {
		t.Fatalf("chain_stopped_mark_missing = %d, want 1: the ended run before the cut has a missing mark", got)
	}
	if got := totalRuns(drainRunsOf(t, ctx, rig, orgID)) - before; got != 0 {
		t.Fatalf("the pass started %d runs (per pass %d), want 0", got, syncdispatchruntime.TouchedDaysPerDrainPass)
	}
}
