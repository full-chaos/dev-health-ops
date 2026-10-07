//go:build integration

package workerservice

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily"
)

// The two handlers that trigger the drain are built with the drain set: the
// dispatcher (the nightly floor) and the finalize handler (the continuation).
// buildDailyWorker builds both through these constructors, so a worker cannot
// run a dispatcher or a finalize handler that triggers no pass.
func TestDailyHandlersOfTheWorkerTriggerTheTouchedDaysDrain(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	rig := newTouchedRig(t, ctx)
	drain, err := newTouchedDaysDrain(rig.pool, rig.conn, rig.store, nilPartitionPublisher{}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	discoverer, err := daily.NewClickHouseRepositoryDiscoverer(rig.conn)
	if err != nil {
		t.Fatal(err)
	}

	dispatcher, err := newDrainingDailyDispatcher(rig.store, nilPartitionPublisher{}, discoverer, drain)
	if err != nil {
		t.Fatal(err)
	}
	if got := dispatcher.TouchedDaysDrainer(); got != daily.TouchedDaysDrainer(drain) {
		t.Fatalf("drain of the dispatcher = %v, want the drain of the worker: the nightly run would trigger no pass", got)
	}
	finalize, err := newDrainingDailyFinalizeHandler(rig.store, drain)
	if err != nil {
		t.Fatal(err)
	}
	if got := finalize.TouchedDaysDrainer(); got != daily.TouchedDaysDrainer(drain) {
		t.Fatalf("drain of the finalize handler = %v, want the drain of the worker: the end of a run would trigger no pass", got)
	}

	if _, err := newDrainingDailyDispatcher(rig.store, nilPartitionPublisher{}, discoverer, nil); err == nil {
		t.Fatal("a dispatcher without a drain was built")
	}
	if _, err := newDrainingDailyFinalizeHandler(rig.store, nil); err == nil {
		t.Fatal("a finalize handler without a drain was built")
	}
}
