//go:build integration

package authapi

import (
	"context"
	"net/http"
	"sync"
	"testing"
	"time"

	valkeygo "github.com/valkey-io/valkey-go"

	"github.com/full-chaos/dev-health-ops/internal/auth/ratelimitvalkey"
	valkeystore "github.com/full-chaos/dev-health-ops/internal/storage/valkey"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// twoReplicasOverOneValkey returns two compat fixtures (two auth-service
// replicas) whose limiters count in one real Valkey.
func twoReplicasOverOneValkey(t *testing.T) (*compatFixture, *compatFixture) {
	t.Helper()
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
	return first, second
}

// The stall repro shape in the live store: ten logins answered 503 before the
// password check, across two replicas, then the database is back: no 429.
func TestLoginTenRefusedTriesThenALoginIsNot429InTheLiveStore(t *testing.T) {
	first, second := twoReplicasOverOneValkey(t)
	replicas := []*compatFixture{first, second}
	for _, f := range replicas {
		f.loginUsers.err = pgDown
	}
	for i := 0; i < loginAccountLimit.Count; i++ {
		if r := goodLogin(replicas[i%2]); r.Code != http.StatusServiceUnavailable {
			t.Fatalf("try %d = %d, want 503", i+1, r.Code)
		}
	}
	for _, f := range replicas {
		f.loginUsers.err = nil
	}
	if r := goodLogin(first); r.Code != http.StatusOK {
		t.Fatalf("login after the outage = %d, want 200: %s", r.Code, r.Body)
	}
}

// Acceptance (2) in the live store: thirty wrong-password logins arrive
// together on two replicas; exactly ten answer 401 and twenty 429, and the
// next one is a 429.
func TestLoginParallelBurstKeepsTheBoundInTheLiveStore(t *testing.T) {
	first, second := twoReplicasOverOneValkey(t)
	replicas := []*compatFixture{first, second}
	const total = 30
	codes := make(chan int, total)
	var wg sync.WaitGroup
	for i := 0; i < total; i++ {
		wg.Add(1)
		go func(f *compatFixture) {
			defer wg.Done()
			codes <- wrongLogin(f).Code
		}(replicas[i%2])
	}
	wg.Wait()
	close(codes)
	tally := map[int]int{}
	for code := range codes {
		tally[code]++
	}
	if tally[http.StatusUnauthorized] != loginAccountLimit.Count || tally[http.StatusTooManyRequests] != total-loginAccountLimit.Count {
		t.Fatalf("answers %v, want %d x 401 and %d x 429", tally, loginAccountLimit.Count, total-loginAccountLimit.Count)
	}
	assertRateLimited(t, "login after the burst", wrongLogin(first))
}

// A refund made on one replica is seen by the other: nine 401s, one refused try
// on replica B (given back), then the tenth 401 passes and the eleventh is a 429.
func TestLoginRefundOnOneReplicaIsSeenByTheOtherInTheLiveStore(t *testing.T) {
	first, second := twoReplicasOverOneValkey(t)
	for i := 0; i < loginAccountLimit.Count-1; i++ {
		if r := wrongLogin(first); r.Code != http.StatusUnauthorized {
			t.Fatalf("try %d = %d, want 401", i+1, r.Code)
		}
	}
	second.loginUsers.err = pgDown
	if r := goodLogin(second); r.Code != http.StatusServiceUnavailable {
		t.Fatalf("refused try = %d, want 503", r.Code)
	}
	if r := wrongLogin(first); r.Code != http.StatusUnauthorized {
		t.Fatalf("tenth 401 = %d: the refund was not seen", r.Code)
	}
	second.loginUsers.err = nil
	assertRateLimited(t, "eleventh", wrongLogin(second))
}
