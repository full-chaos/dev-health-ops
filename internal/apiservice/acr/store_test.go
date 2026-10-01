package acr

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestPostgresEntitlementStoreLookupRejectsMalformedOrgIDBeforeTouchingThePool(t *testing.T) {
	// Pool is nil (unconfigured), and Lookup must still return
	// ErrOrgNotFound, not ErrUnavailable, for a malformed org_id -- the
	// format check runs before the pool is touched (store.go's own comment).
	store := PostgresEntitlementStore{Pool: nil}
	_, err := store.Lookup(context.Background(), "not-a-uuid")
	if !errors.Is(err, ErrOrgNotFound) {
		t.Fatalf("Lookup(malformed) error = %v, want ErrOrgNotFound", err)
	}
	if errors.Is(err, ErrUnavailable) {
		t.Fatalf("Lookup(malformed) error = %v, must not also be ErrUnavailable", err)
	}
}

func TestPostgresEntitlementStoreLookupWithNilPoolAndValidOrgID(t *testing.T) {
	store := PostgresEntitlementStore{Pool: nil}
	_, err := store.Lookup(context.Background(), uuid.NewString())
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Lookup(valid, nil pool) error = %v, want ErrUnavailable", err)
	}
}

func TestPostgresEntitlementStoreReadyWithNilPoolIsUnavailable(t *testing.T) {
	if err := (PostgresEntitlementStore{Pool: nil}).Ready(context.Background()); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Ready(nil pool) error = %v, want ErrUnavailable", err)
	}
}

// unreachablePool is a real pgx pool whose server does not answer: every query
// fails at connect, the way a Postgres outage does.
func unreachablePool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), "postgres://acr:acr@127.0.0.1:1/acr?connect_timeout=1")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// TestPostgresEntitlementStoreLookupMapsAnUnreachableStoreToUnavailable pins the
// organization-existence read's error mapping (store.go): a read that fails is
// ErrUnavailable (the route's 503), never the raw driver error (a 500) and never
// "not found" or an open decision.
func TestPostgresEntitlementStoreLookupMapsAnUnreachableStoreToUnavailable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	store := PostgresEntitlementStore{Pool: unreachablePool(t)}
	_, err := store.Lookup(ctx, uuid.NewString())
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Lookup(unreachable store) error = %v, want ErrUnavailable", err)
	}
	if errors.Is(err, ErrOrgNotFound) {
		t.Fatalf("Lookup(unreachable store) error = %v, must not read as a missing organization", err)
	}
}

// TestEntitlementRouteAnswers503WhenTheRealStoreIsUnreachable runs the route over
// the real store: the refusal an authorization check owes when its store cannot
// be read is the 503 body, not a 500 and not a decision.
func TestEntitlementRouteAnswers503WhenTheRealStoreIsUnreachable(t *testing.T) {
	server := newTestServer(PostgresEntitlementStore{Pool: unreachablePool(t)})
	defer server.Close()
	for _, path := range []string{"/api/v1/internal/acr/entitlements/" + uuid.NewString(), "/api/v1/internal/acr/health"} {
		response, err := http.Get(server.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		raw, err := io.ReadAll(response.Body)
		_ = response.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		if response.StatusCode != http.StatusServiceUnavailable || strings.TrimSpace(string(raw)) != `{"detail":"Service unavailable"}` {
			t.Fatalf("GET %s over an unreachable store = %d %q, want 503 {\"detail\":\"Service unavailable\"}", path, response.StatusCode, raw)
		}
	}
}
