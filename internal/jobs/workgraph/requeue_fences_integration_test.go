//go:build integration

package workgraph

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

// TestRequeueIsFencedByTokenLeaseAndState pins every fence of
// PostgresStore.Requeue (CHAOS-8782, vet 1 finding 1): a write from a claimant
// that is not the current holder must change nothing and report ErrLeaseLost.
func TestRequeueIsFencedByTokenLeaseAndState(t *testing.T) {
	ctx := context.Background()
	pool, store, _ := startWorkGraphReleaseLostFixture(t, ctx)
	insertMaterializeRequest(t, ctx, pool)

	claimIt := func() *Claim {
		claim, err := store.Claim(ctx, testRequestID, KindMaterialize)
		if err != nil || claim == nil {
			t.Fatalf("claim = %v, %v", claim, err)
		}
		return claim
	}
	stateOf := func() string { request, _ := requestAndLedgerState(t, ctx, pool); return request }

	claim := claimIt()

	// 1. A stale or foreign claim token.
	foreign := *claim
	foreign.Token = uuid.NewString()
	if err := store.Requeue(ctx, foreign); !errors.Is(err, ErrLeaseLost) || stateOf() != "running" {
		t.Fatalf("foreign token: err=%v state=%s, want ErrLeaseLost and running", err, stateOf())
	}

	// 2. An expired lease (the clock moves past it): the claimant may no longer
	// stand the row down; the reclaim path owns it.
	realNow := store.now
	store.now = func() time.Time { return time.Now().Add(2 * defaultLease) }
	if err := store.Requeue(ctx, *claim); !errors.Is(err, ErrLeaseLost) || stateOf() != "running" {
		t.Fatalf("expired lease: err=%v state=%s, want ErrLeaseLost and running", err, stateOf())
	}
	store.now = realNow

	// 3. The holder, with a live lease: the request goes back to pending and
	// unleased.
	if err := store.Requeue(ctx, *claim); err != nil {
		t.Fatalf("holder requeue = %v", err)
	}
	var cleared int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM work_graph_execution_requests
WHERE id = $1 AND state = 'pending' AND claim_token IS NULL AND lease_expires_at IS NULL`, testRequestID).Scan(&cleared); err != nil || cleared != 1 {
		t.Fatalf("requeued row not pending/unleased: %d, %v", cleared, err)
	}

	// 4. A second requeue with the same (now released) claim: no row is
	// running, so nothing changes.
	if err := store.Requeue(ctx, *claim); !errors.Is(err, ErrLeaseLost) || stateOf() != "pending" {
		t.Fatalf("requeue of a pending row: err=%v state=%s, want ErrLeaseLost and pending", err, stateOf())
	}

	// 5. A terminal row is never reopened: claim, complete, then requeue.
	next := claimIt()
	if err := store.Complete(ctx, *next, []byte(`{"outcome":{"status":"success"}}`)); err != nil {
		t.Fatal(err)
	}
	if err := store.Requeue(ctx, *next); !errors.Is(err, ErrLeaseLost) || stateOf() != "succeeded" {
		t.Fatalf("requeue of a succeeded row: err=%v state=%s, want ErrLeaseLost and succeeded", err, stateOf())
	}
}

// TestFailedReleaseLandsEvenWhenTheJobContextIsCancelled (vet 2 F2): the
// detached release context is what lets the failed state be written during a
// drain, through the real store.
func TestFailedReleaseLandsEvenWhenTheJobContextIsCancelled(t *testing.T) {
	ctx := context.Background()
	pool, store, _ := startWorkGraphReleaseLostFixture(t, ctx)
	insertMaterializeRequest(t, ctx, pool)
	claim, err := store.Claim(ctx, testRequestID, KindMaterialize)
	if err != nil || claim == nil {
		t.Fatalf("claim = %v, %v", claim, err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if err := releaseFailed(store, cancelled, *claim, "deterministic failure: scope_invalid"); err != nil {
		t.Fatalf("releaseFailed with a cancelled job context = %v", err)
	}
	if request, ledger := requestAndLedgerState(t, ctx, pool); request != "failed" || ledger != "failed" {
		t.Fatalf("request=%s ledger=%s, want failed/failed", request, ledger)
	}
}
