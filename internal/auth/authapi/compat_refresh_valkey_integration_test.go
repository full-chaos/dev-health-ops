//go:build integration

package authapi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	valkeygo "github.com/valkey-io/valkey-go"

	"github.com/full-chaos/dev-health-ops/internal/auth/ratelimitvalkey"
	valkeystore "github.com/full-chaos/dev-health-ops/internal/storage/valkey"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// Two CompatRoutes handlers, each over its own client of one real Valkey (two auth-service replicas), share both
// refresh budgets: ten refreshes of one credential, or 100 from one address, split across them are admitted, and
// the next is go-api's 429 with Retry-After through either replica.
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
	// One credential: its budget of 10 is shared by both replicas.
	token := nextCredential()
	sameCredential := func(h http.Handler) *httptest.ResponseRecorder {
		return send(h, proxyPeer, "198.51.100.30", http.MethodPost, PatternCompatRefresh, `{"refresh_token":"`+token+`"}`, nil)
	}
	for i := 0; i < 10; i++ {
		if r := sameCredential(replicas[i%2]); r.Code != http.StatusUnauthorized {
			t.Fatalf("refresh %d = %d, want 401", i+1, r.Code)
		}
	}
	for i, replica := range replicas {
		assertRefreshRateLimited(t, "replica "+string(rune('1'+i)), sameCredential(replica))
	}
	// One address: its ceiling of 100 is shared by both replicas, whatever the credential.
	for i := 0; i < addrCeiling; i++ {
		if r := refreshAttempt(replicas[i%2], proxyPeer, "198.51.100.31"); r.Code != http.StatusUnauthorized {
			t.Fatalf("address refresh %d = %d, want 401", i+1, r.Code)
		}
	}
	for i, replica := range replicas {
		assertRefreshRateLimited(t, "address ceiling, replica "+string(rune('1'+i)), refreshAttempt(replica, proxyPeer, "198.51.100.31"))
	}
}
