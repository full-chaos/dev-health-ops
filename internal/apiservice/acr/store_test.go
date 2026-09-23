package acr

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
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
