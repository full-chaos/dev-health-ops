//go:build integration

package postgres

import (
	"context"
	"testing"
	"time"
)

// PLAN-14250-4 N2: the coordinator only reads sync_dispatch_bucket_signals (the materializer's recovery read); every
// signal write runs on the domain pool. Its posture grants SELECT only: INSERT and UPDATE are refused (42501).
func TestP4CoordinatorOnlyReadsBucketSignals(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	_, uri, roles := startGrantHarness(t, ctx)
	coordinator := connectAs(t, ctx, uri, roles.coordinator, grantCoordinatorPass)
	if err := execInRolledBackTransaction(t, ctx, coordinator, `SELECT org_id FROM public.sync_dispatch_bucket_signals LIMIT 1`); err != nil {
		t.Fatalf("coordinator SELECT on signals: %v", err)
	}
	for _, statement := range []string{
		`INSERT INTO public.sync_dispatch_bucket_signals (org_id, provider, cost_class, generation, acked_generation, updated_at) VALUES ('o', 'github', 'medium', 1, 0, now())`,
		`UPDATE public.sync_dispatch_bucket_signals SET acked_generation = generation`,
	} {
		err := execInRolledBackTransaction(t, ctx, coordinator, statement)
		t.Logf("coordinator %q: err = %v", statement, err)
		if !isInsufficientPrivilege(err) {
			t.Errorf("coordinator %q: err = %v, want 42501 (coordinator reads signals only)", statement, err)
		}
	}
}
