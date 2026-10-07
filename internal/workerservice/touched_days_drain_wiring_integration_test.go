//go:build integration

package workerservice

import (
	"context"
	"testing"
)

// wireTouchedDaysDrain builds the drain as the worker does (newTouchedDaysDrain)
// and sets it on the rig's dispatcher, which is the nightly trigger. It
// returns the continuation trigger: the call the finalize handler makes when a
// daily run ended.
func wireTouchedDaysDrain(t *testing.T, rig *touchedRig) func(ctx context.Context, orgID, endedRunID string) {
	t.Helper()
	drain, err := newTouchedDaysDrain(rig.pool, rig.conn, rig.store, nilPartitionPublisher{}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	rig.dispatch.SetTouchedDaysDrainer(drain)
	return func(ctx context.Context, orgID, endedRunID string) {
		drain.DrainTouchedDays(ctx, orgID, "e:"+endedRunID)
	}
}
