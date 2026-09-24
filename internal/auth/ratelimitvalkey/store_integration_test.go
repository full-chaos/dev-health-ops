//go:build integration

package ratelimitvalkey_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	valkeygo "github.com/valkey-io/valkey-go"

	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/ratelimitvalkey"
	valkeystore "github.com/full-chaos/dev-health-ops/internal/storage/valkey"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// startValkey returns two independent clients on one real Valkey (two api
// replicas), plus the URI.
func startValkey(t *testing.T) (a, b valkeygo.Client) {
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
	open := func() valkeygo.Client {
		client, openErr := valkeystore.Open(ctx, valkeystore.DefaultConfig(instance.URI))
		if openErr != nil {
			t.Fatal(openErr)
		}
		t.Cleanup(client.Close)
		return client
	}
	return open(), open()
}

func newStore(t *testing.T, client valkeygo.Client) *ratelimitvalkey.Store {
	t.Helper()
	store, err := ratelimitvalkey.New(client)
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func hit(t *testing.T, store *ratelimitvalkey.Store, limit httpapi.Limit, key, path string) bool {
	t.Helper()
	allowed, err := httpapi.NewKeyedLimiter(store, limit).Allow(context.Background(), key, path)
	if err != nil {
		t.Fatalf("Allow(%s, %s, %s): %v", limit.ID, key, path, err)
	}
	return allowed
}

// The limit is one budget across replicas: N hits split over two stores on
// two clients sum to N, and the (N+1)th is refused whichever replica takes it.
func TestLimitHoldsAcrossReplicas(t *testing.T) {
	clientA, clientB := startValkey(t)
	a, b := newStore(t, clientA), newStore(t, clientB)
	limit := httpapi.Limit{ID: "shared", Count: 10, Window: time.Hour}
	for i := range 10 {
		store := a
		if i%2 == 1 {
			store = b
		}
		if !hit(t, store, limit, "admin-user:x", "/orgs/1/invites") {
			t.Fatalf("hit %d of 10 was refused", i+1)
		}
	}
	for name, store := range map[string]*ratelimitvalkey.Store{"a": a, "b": b} {
		if hit(t, store, limit, "admin-user:x", "/orgs/1/invites") {
			t.Fatalf("the 11th hit through replica %s was allowed", name)
		}
	}
	if a.Backend() != "redis" {
		t.Fatalf("Backend() = %q, want redis", a.Backend())
	}
}

// Fixed window as slowapi does it: the window starts at the first hit and
// lasts exactly the window; a refused hit neither extends nor restarts it.
func TestFixedWindowStartsAtTheFirstHitAndIsNotExtendedByRefusals(t *testing.T) {
	client, _ := startValkey(t)
	store := newStore(t, client)
	limit := httpapi.Limit{ID: "window", Count: 2, Window: 2 * time.Second}
	start := time.Now()
	if !hit(t, store, limit, "k", "/p") || !hit(t, store, limit, "k", "/p") {
		t.Fatal("the first two hits were refused")
	}
	time.Sleep(1200 * time.Millisecond)
	if hit(t, store, limit, "k", "/p") { // refused, and it must not push the expiry out
		t.Fatal("the third hit inside the window was allowed")
	}
	if wait := 2*time.Second + 250*time.Millisecond - time.Since(start); wait > 0 {
		time.Sleep(wait)
	}
	if !hit(t, store, limit, "k", "/p") {
		t.Fatal("the window did not expire on time: a refused hit extended it")
	}
}

// Different (key, path) pairs and different limit IDs never share a counter.
func TestPairsAndLimitsAreIndependent(t *testing.T) {
	client, _ := startValkey(t)
	store := newStore(t, client)
	one := httpapi.Limit{ID: "one", Count: 1, Window: time.Hour}
	two := httpapi.Limit{ID: "two", Count: 1, Window: time.Hour}
	if !hit(t, store, one, "a", "/x") {
		t.Fatal("first hit refused")
	}
	if hit(t, store, one, "a", "/x") {
		t.Fatal("second hit of the same pair allowed")
	}
	for _, other := range []struct {
		limit     httpapi.Limit
		key, path string
	}{{one, "b", "/x"}, {one, "a", "/y"}, {two, "a", "/x"}} {
		if !hit(t, store, other.limit, other.key, other.path) {
			t.Fatalf("(%s, %s, %s) shares another pair's counter", other.limit.ID, other.key, other.path)
		}
	}
}

// The per-key path bound: one caller minting distinct paths is refused for
// new paths past the bound and nobody else is affected; its tracked paths
// keep counting.
func TestOneCallerCannotMintUnboundedPathsOrStarveAnother(t *testing.T) {
	client, _ := startValkey(t)
	store := newStore(t, client)
	limit := httpapi.Limit{ID: "paths", Count: 5, Window: time.Hour}
	for i := range ratelimitvalkey.MaxPathsPerKey {
		if !hit(t, store, limit, "minter", fmt.Sprintf("/orgs/%d/invites", i)) {
			t.Fatalf("path %d within the bound was refused", i)
		}
	}
	if hit(t, store, limit, "minter", "/orgs/one-too-many/invites") {
		t.Fatal("a new path past the per-key bound was allowed")
	}
	if !hit(t, store, limit, "other", "/orgs/one-too-many/invites") {
		t.Fatal("another caller was refused after the minter filled its own bound")
	}
	if !hit(t, store, limit, "minter", "/orgs/0/invites") {
		t.Fatal("an already-tracked path of the minter was refused")
	}
	// A refused new path counts nothing: it never created a counter.
	if !hit(t, store, httpapi.Limit{ID: "paths", Count: 1, Window: time.Hour}, "another-minter", "/orgs/a/invites") {
		t.Fatal("unrelated first hit refused")
	}
}

// Every counter carries a TTL (INCR and PEXPIRE are one script): no counter
// can outlive its window, and a hit sets the window, not more.
func TestEveryCounterHasATTLNoLongerThanTheWindow(t *testing.T) {
	client, _ := startValkey(t)
	store := newStore(t, client)
	limit := httpapi.Limit{ID: "ttl", Count: 3, Window: 90 * time.Second}
	hit(t, store, limit, "k", "/p")
	hit(t, store, limit, "k", "/p")
	ctx := context.Background()
	for _, key := range []string{ratelimitvalkey.CounterKey(limit, "k", "/p"), ratelimitvalkey.PathsKey(limit, "k")} {
		ttl, err := client.Do(ctx, client.B().Pttl().Key(key).Build()).AsInt64()
		if err != nil {
			t.Fatal(err)
		}
		if ttl <= 0 || ttl > limit.Window.Milliseconds() {
			t.Fatalf("PTTL(%s) = %dms, want within (0, %dms]", key, ttl, limit.Window.Milliseconds())
		}
	}
}

// A store that cannot reach Valkey returns the error, never an allowance.
func TestStoreErrorIsReturnedNotSwallowed(t *testing.T) {
	client, _ := startValkey(t)
	store := newStore(t, client)
	client.Close()
	allowed, err := httpapi.NewKeyedLimiter(store, httpapi.Limit{ID: "down", Count: 5, Window: time.Hour}).Allow(context.Background(), "k", "/p")
	if err == nil || allowed {
		t.Fatalf("Allow on a closed client = (%v, %v), want (false, error)", allowed, err)
	}
}

func TestNewRejectsANilClientAndADegenerateLimitAllowsEverything(t *testing.T) {
	if _, err := ratelimitvalkey.New(nil); err == nil {
		t.Fatal("New(nil) succeeded")
	}
	client, _ := startValkey(t)
	store := newStore(t, client)
	for _, limit := range []httpapi.Limit{{ID: "", Count: 1, Window: time.Hour}, {ID: "x", Count: 0, Window: time.Hour}, {ID: "x", Count: 1}} {
		if !hit(t, store, limit, "k", "/p") {
			t.Fatalf("degenerate limit %+v refused a hit", limit)
		}
	}
}

// The keyspace is readable by anything that can SCAN: a caller key (an admin's
// user id) and a request path (an organization or user id) must not appear in
// it, and the keys must still be a function of (limit, caller, path).
func TestCallerKeyAndPathAreNotStoredInCleartext(t *testing.T) {
	client, _ := startValkey(t)
	store := newStore(t, client)
	limit := httpapi.Limit{ID: "leak", Count: 5, Window: time.Hour}
	const callerKey = "admin-user:11111111-2222-3333-4444-555555555555"
	const path = "/api/v1/admin/orgs/99999999-8888-7777-6666-555555555555/invites"
	hit(t, store, limit, callerKey, path)
	hit(t, store, limit, callerKey, path)
	ctx := context.Background()
	keys, err := client.Do(ctx, client.B().Keys().Pattern("dho:rl:*").Build()).AsStrSlice()
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 2 {
		t.Fatalf("keys = %q, want the counter and the path set", keys)
	}
	for _, key := range keys {
		for _, secret := range []string{"11111111", "admin-user", "99999999", "/api/v1", "invites"} {
			if strings.Contains(key, secret) {
				t.Fatalf("Valkey key %q carries %q in clear", key, secret)
			}
		}
	}
	// The path set's members are stored too: they must not carry the path.
	members, err := client.Do(ctx, client.B().Smembers().Key(ratelimitvalkey.PathsKey(limit, callerKey)).Build()).AsStrSlice()
	if err != nil || len(members) != 1 {
		t.Fatalf("path set members = %q (%v), want one", members, err)
	}
	for _, secret := range []string{"99999999", "/api/v1", "invites"} {
		if strings.Contains(members[0], secret) {
			t.Fatalf("path set member %q carries %q in clear", members[0], secret)
		}
	}
	// Still deterministic and per pair: the same pair is one counter, another
	// path is another.
	if got := ratelimitvalkey.CounterKey(limit, callerKey, path); got == ratelimitvalkey.CounterKey(limit, callerKey, path+"x") {
		t.Fatal("two paths share one counter key")
	}
	count, err := client.Do(ctx, client.B().Get().Key(ratelimitvalkey.CounterKey(limit, callerKey, path)).Build()).AsInt64()
	if err != nil || count != 2 {
		t.Fatalf("counter = %d (%v), want 2", count, err)
	}
}
