//go:build integration

package authruntime

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/storage/valkey"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// The store the service runs on is the lazy one: it forwards the refund to the
// Valkey store, so a refused login gives its charge back in the live counter.
func TestTheLazyStoreForwardsTheRefundToValkey(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	instance, err := containers.StartValkey(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		_ = instance.Close(closeCtx)
	})
	store := &lazyStore{config: valkey.DefaultConfig(instance.URI)}
	limit := httpapi.Limit{ID: "auth_svc_login_ip_account", Count: 10, Window: time.Hour}
	limiter := httpapi.NewKeyedLimiter(store, limit)
	if _, err := limiter.Allow(ctx, "k", "s01"); err != nil {
		t.Fatal(err)
	}
	if err := limiter.Refund(ctx, "k", "s01"); err != nil {
		t.Fatalf("refund through the lazy store: %v", err)
	}
	if n, err := store.Peek(ctx, httpapi.Hit{Limit: limit, Key: "k", Path: "s01"}); err != nil || n != 0 {
		t.Fatalf("count = %d (%v) after the refund, want 0", n, err)
	}
}
