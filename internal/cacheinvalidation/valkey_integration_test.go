//go:build integration

package cacheinvalidation

import (
	"context"
	"testing"
	"time"

	valkeystore "github.com/full-chaos/dev-health-ops/internal/storage/valkey"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// TestValkeyOrgCacheInvalidatorIncrementsTheEpochWithATTL pins the write the
// Python read path depends on: INCR semantics (absent -> 1, then +1) on the
// contract key, with an expiry so an org that stops syncing does not leave
// the key behind forever (CHAOS-4226).
func TestValkeyOrgCacheInvalidatorIncrementsTheEpochWithATTL(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	instance, err := containers.StartValkey(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate Valkey: %v", err)
		}
	})
	client, err := valkeystore.Open(ctx, valkeystore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	invalidator, err := NewValkeyOrgCacheInvalidator(client)
	if err != nil {
		t.Fatal(err)
	}

	const orgID = "00000000-0000-4000-8000-0000000000f1"
	key := OrgCacheEpochKey(orgID)
	for want := int64(1); want <= 2; want++ {
		if err := invalidator.InvalidateOrg(ctx, orgID); err != nil {
			t.Fatalf("InvalidateOrg #%d: %v", want, err)
		}
		got, err := client.Do(ctx, client.B().Get().Key(key).Build()).AsInt64()
		if err != nil {
			t.Fatalf("GET %s: %v", key, err)
		}
		if got != want {
			t.Fatalf("epoch after bump #%d = %d want %d", want, got, want)
		}
		ttl, err := client.Do(ctx, client.B().Ttl().Key(key).Build()).AsInt64()
		if err != nil {
			t.Fatalf("TTL %s: %v", key, err)
		}
		if ttl <= 0 || ttl > int64(OrgCacheEpochTTL.Seconds()) {
			t.Fatalf("epoch key ttl=%d want (0, %d]", ttl, int64(OrgCacheEpochTTL.Seconds()))
		}
	}
	if err := invalidator.InvalidateOrg(ctx, ""); err == nil {
		t.Fatal("empty org accepted")
	}
}
