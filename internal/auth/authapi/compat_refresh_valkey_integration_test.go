//go:build integration

package authapi

import (
	"context"
	"net/http"
	"testing"
	"time"

	valkeygo "github.com/valkey-io/valkey-go"

	"github.com/full-chaos/dev-health-ops/internal/auth/ratelimitvalkey"
	valkeystore "github.com/full-chaos/dev-health-ops/internal/storage/valkey"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// Two CompatRoutes handlers, each over its own client of one real Valkey (two auth-service replicas), share one
// refresh budget: ten refreshes split across them are admitted, the 11th is go-api's 429 through either replica.
func TestCompatRefreshBudgetIsSharedByTwoReplicasInTheLiveStore(t *testing.T) {
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
	open := func() *ratelimitvalkey.Store {
		var client valkeygo.Client
		client, err = valkeystore.Open(ctx, valkeystore.DefaultConfig(instance.URI))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(client.Close)
		store, newErr := ratelimitvalkey.New(client)
		if newErr != nil {
			t.Fatal(newErr)
		}
		return store
	}
	first, second := newCompatFixture(t), newCompatFixture(t)
	first.counters, second.counters = open(), open()
	first.buildCompat()
	second.buildCompat()
	replicas := []http.Handler{trustedProxy(t, first.server), trustedProxy(t, second.server)}
	for i := 0; i < 10; i++ {
		if r := refreshAttempt(replicas[i%2], proxyPeer, "198.51.100.30"); r.Code != http.StatusUnauthorized {
			t.Fatalf("refresh %d = %d, want 401", i+1, r.Code)
		}
	}
	for i, replica := range replicas {
		assertRateLimited(t, "replica "+string(rune('1'+i)), refreshAttempt(replica, proxyPeer, "198.51.100.30"))
	}
}
