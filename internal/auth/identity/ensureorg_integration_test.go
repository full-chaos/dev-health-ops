//go:build integration

package identity

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/org"
)

// EnsureOrganization projects an organization the auth schema lacks, and
// never changes one it has: a suspended (deprovisioned) organization stays
// suspended while go-api's row is still active.
func TestEnsureOrganizationProjectsOnlyAMissingRow(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	id := e.seedOrg(t, ctx)
	if err := e.store.EnsureOrganization(ctx, id); err != nil {
		t.Fatalf("EnsureOrganization of a missing row: %v", err)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.organizations WHERE id = $1 AND disabled_at IS NULL`, id); n != 1 {
		t.Fatalf("projected rows = %d, want 1 active", n)
	}
	if err := e.orgStore(t).Suspend(ctx, id, nil, org.Actor{}); err != nil {
		t.Fatal(err)
	}
	if err := e.store.EnsureOrganization(ctx, id); err != nil {
		t.Fatalf("EnsureOrganization of a suspended row: %v", err)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.organizations WHERE id = $1 AND disabled_at IS NOT NULL`, id); n != 1 {
		t.Fatal("EnsureOrganization reactivated a suspended organization")
	}
	if err := e.store.EnsureOrganization(ctx, uuid.New()); !errors.Is(err, ErrOrganizationNotFound) {
		t.Fatalf("EnsureOrganization of an unknown organization = %v, want ErrOrganizationNotFound", err)
	}
}
