//go:build integration

package ratelimitvalkey_test

import (
	"context"
	"sync"
	"testing"
	"time"

	valkeygo "github.com/valkey-io/valkey-go"

	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/ratelimitvalkey"
)

func counterTTL(t *testing.T, client valkeygo.Client, key string) int64 {
	t.Helper()
	ttl, err := client.Do(context.Background(), client.B().Pttl().Key(key).Build()).AsInt64()
	if err != nil {
		t.Fatal(err)
	}
	return ttl
}

// The refund gives one hit back atomically: a missing counter stays missing
// (no key without a TTL), a live one drops by one and keeps its TTL, and
// many parallel refunds never take it below 0.
func TestRefundIsAtomicAndNeverCreatesOrUnderflowsACounter(t *testing.T) {
	clientA, clientB := startValkey(t)
	a, b := newStore(t, clientA), newStore(t, clientB)
	ctx := context.Background()
	limit := httpapi.Limit{ID: "refund_atomic", Count: 100, Window: time.Hour}
	h := httpapi.Hit{Limit: limit, Key: "k", Path: "/p"}
	key := ratelimitvalkey.CounterKey(limit, "k", "/p")

	if err := a.Refund(ctx, h); err != nil {
		t.Fatal(err)
	}
	if n, err := clientA.Do(ctx, clientA.B().Exists().Key(key).Build()).AsInt64(); err != nil || n != 0 {
		t.Fatalf("a refund of a missing counter left a key (exists=%d, %v)", n, err)
	}
	for i := 0; i < 20; i++ {
		if _, err := a.Increment(ctx, h); err != nil {
			t.Fatal(err)
		}
	}
	ttl := counterTTL(t, clientA, key)
	if err := a.Refund(ctx, h); err != nil {
		t.Fatal(err)
	}
	if n, _ := a.Peek(ctx, h); n != 19 {
		t.Fatalf("count = %d after one refund of 20, want 19", n)
	}
	if after := counterTTL(t, clientA, key); after <= 0 || after > ttl {
		t.Fatalf("ttl %d after the refund (was %d): the refund touched the window", after, ttl)
	}
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(store *ratelimitvalkey.Store) {
			defer wg.Done()
			if err := store.Refund(ctx, h); err != nil {
				t.Error(err)
			}
		}([]*ratelimitvalkey.Store{a, b}[i%2])
	}
	wg.Wait()
	if n, _ := a.Peek(ctx, h); n != 0 {
		t.Fatalf("count = %d after 50 refunds of 19, want 0 (never negative)", n)
	}
	if n, err := a.Increment(ctx, h); err != nil || n != 1 {
		t.Fatalf("next hit counted %d (%v), want 1", n, err)
	}
}
