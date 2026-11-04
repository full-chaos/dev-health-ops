package httpapi

import (
	"context"
	"errors"
	"testing"
	"time"
)

// no Refund method reachable: the embedded store's Refund is hidden by a
// narrower interface value.
type plainStore struct{ inner CounterStore }

func (s plainStore) Increment(ctx context.Context, h Hit) (int64, error) {
	return s.inner.Increment(ctx, h)
}
func (s plainStore) Peek(ctx context.Context, h Hit) (int64, error) { return s.inner.Peek(ctx, h) }
func (s plainStore) Backend() string                                { return s.inner.Backend() }

func TestMemoryRefundLowersOneHitAndStopsAtZero(t *testing.T) {
	store := NewMemoryCounters(nil)
	limit := Limit{ID: "refund-mem", Count: 3, Window: time.Hour}
	limiter := NewKeyedLimiter(store, limit)
	ctx := context.Background()
	for i := 0; i < 2; i++ {
		if _, err := limiter.Allow(ctx, "k", "p"); err != nil {
			t.Fatal(err)
		}
	}
	if err := limiter.Refund(ctx, "k", "p"); err != nil {
		t.Fatal(err)
	}
	if n, _ := store.Peek(ctx, Hit{Limit: limit, Key: "k", Path: "p"}); n != 1 {
		t.Fatalf("count = %d after one refund of two hits, want 1", n)
	}
	for i := 0; i < 3; i++ {
		if err := limiter.Refund(ctx, "k", "p"); err != nil {
			t.Fatal(err)
		}
	}
	if n, _ := store.Peek(ctx, Hit{Limit: limit, Key: "k", Path: "p"}); n != 0 {
		t.Fatalf("count = %d after refunds past zero, want 0", n)
	}
	if n, err := store.Increment(ctx, Hit{Limit: limit, Key: "k", Path: "p"}); err != nil || n != 1 {
		t.Fatalf("hit after the refunds counted %d (%v), want 1: the counter went below 0", n, err)
	}
}

func TestMemoryRefundOfAnExpiredWindowOrUnknownPairChangesNothing(t *testing.T) {
	now := time.Unix(1000, 0)
	store := NewMemoryCounters(func() time.Time { return now })
	limit := Limit{ID: "refund-exp", Count: 3, Window: time.Minute}
	limiter := NewKeyedLimiter(store, limit)
	ctx := context.Background()
	if err := limiter.Refund(ctx, "never", "p"); err != nil {
		t.Fatal(err)
	}
	if _, err := limiter.Allow(ctx, "k", "p"); err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Minute)
	if err := limiter.Refund(ctx, "k", "p"); err != nil {
		t.Fatal(err)
	}
	if n, err := store.Increment(ctx, Hit{Limit: limit, Key: "k", Path: "p"}); err != nil || n != 1 {
		t.Fatalf("first hit of the new window counted %d (%v), want 1", n, err)
	}
	if n, _ := store.Peek(ctx, Hit{Limit: limit, Key: "never", Path: "p"}); n != 0 {
		t.Fatalf("a refund made a counter for an unknown pair: %d", n)
	}
}

func TestRefundOnAStoreWithoutTheSeamAnswersErrNoRefundAndKeepsTheHit(t *testing.T) {
	inner := NewMemoryCounters(nil)
	limit := Limit{ID: "refund-none", Count: 3, Window: time.Hour}
	limiter := NewKeyedLimiter(plainStore{inner}, limit)
	ctx := context.Background()
	if _, err := limiter.Allow(ctx, "k", "p"); err != nil {
		t.Fatal(err)
	}
	if err := limiter.Refund(ctx, "k", "p"); !errors.Is(err, ErrNoRefund) {
		t.Fatalf("err = %v, want ErrNoRefund", err)
	}
	if n, _ := inner.Peek(ctx, Hit{Limit: limit, Key: "k", Path: "p"}); n != 1 {
		t.Fatalf("count = %d, want 1", n)
	}
	var nilLimiter *KeyedLimiter
	if err := nilLimiter.Refund(ctx, "k", "p"); err != nil {
		t.Fatalf("nil limiter refund: %v", err)
	}
}
