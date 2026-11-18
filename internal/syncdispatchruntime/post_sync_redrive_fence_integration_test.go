//go:build integration

package syncdispatchruntime

import (
	"context"
	"testing"
)

// TestPostSyncRedriveFence (CHAOS-14115 A8): the terminal delivery repair re-arms an exhausted post_sync row without
// moving its attempt, and the next claim moves it. A pass of the exhausted delivery's attempt that still arrives
// then writes nothing, and the new delivery's pass commits.
func TestPostSyncRedriveFence(t *testing.T) {
	kind := postSyncFenceKind(postSyncPassOptions{})
	withFenceRig(t, kind, 1, func(ctx context.Context, rig *fenceRig) {
		// The repair's re-arm (terminal_delivery_repair.go) followed by the Kernel's claim and River dispatch.
		if _, err := rig.other.Exec(ctx, `
UPDATE sync_dispatch_outbox
SET status = 'pending', available_at = now(), last_error = 'river_delivery_exhausted', dispatched_at = NULL,
    dispatched_transport = NULL, dispatched_route_generation = NULL, transport_job_id = NULL, claim_token = NULL,
    claim_expires_at = NULL, claim_transport = NULL, claim_route_generation = NULL, updated_at = now()
WHERE id = $1`, kind.outbox); err != nil {
			t.Fatal(err)
		}
		if status, attempts := rig.outboxState(t, ctx); status != "pending" || attempts != 1 {
			t.Fatalf("re-armed row status=%q attempts=%d, want pending/1", status, attempts)
		}
		if _, err := rig.other.Exec(ctx, `
UPDATE sync_dispatch_outbox
SET status = 'dispatched', attempts = attempts + 1, dispatched_at = now(), dispatched_transport = 'river',
    dispatched_route_generation = $2, transport_job_id = '424242', last_error = NULL, updated_at = now()
WHERE id = $1`, kind.outbox, kind.generation); err != nil {
			t.Fatal(err)
		}
		if err := kind.pass(t, ctx, rig.pool, 1); err != nil || kind.wrote(t, ctx, rig.other) {
			t.Fatalf("exhausted attempt 1 after the re-drive: err=%v wrote=%t, want nil/false", err, kind.wrote(t, ctx, rig.other))
		}
		if err := kind.pass(t, ctx, rig.pool, 2); err != nil || !kind.wrote(t, ctx, rig.other) {
			t.Fatalf("re-driven attempt 2: err=%v wrote=%t, want nil/true", err, kind.wrote(t, ctx, rig.other))
		}
	})
}
