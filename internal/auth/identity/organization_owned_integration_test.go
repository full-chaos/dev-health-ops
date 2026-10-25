//go:build integration

package identity

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/org"
)

func (e *env) ownedOrgsStore(t *testing.T) *Store {
	t.Helper()
	s, err := New(Config{Pool: e.runtime, Schema: e.store.schema, OrgsOwnedByAuth: true})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func (e *env) orgStore(t *testing.T) *org.Store {
	t.Helper()
	s, err := org.New(org.Config{Pool: e.runtime, Schema: e.store.schema})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// seedAuthOwnedOrg projects a public org once (switch off) and returns its id.
func (e *env) seedAuthOwnedOrg(t *testing.T, ctx context.Context) uuid.UUID {
	t.Helper()
	id := e.seedOrg(t, ctx)
	if err := e.store.ProjectOrganization(ctx, id); err != nil {
		t.Fatal(err)
	}
	return id
}

func TestProjectOrganizationOwnedByAuthDoesNotReactivateASuspendedOrganization(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	id := e.seedAuthOwnedOrg(t, ctx)
	if err := e.orgStore(t).Suspend(ctx, id, nil, org.Actor{}); err != nil {
		t.Fatal(err)
	}
	reactivated := e.outboxRows(t, ctx, "organization.reactivated", id.String())
	var rev int64
	if err := e.admin.QueryRow(ctx, `SELECT revision FROM auth.aggregate_revisions WHERE aggregate_type = 'organization' AND aggregate_id = $1`, "org_"+id.String()).Scan(&rev); err != nil {
		t.Fatal(err)
	}
	// public.organizations is still active, as go-api would leave it.
	if err := e.ownedOrgsStore(t).ProjectOrganization(ctx, id); err != nil {
		t.Fatalf("ProjectOrganization with the switch on: %v", err)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.organizations WHERE id = $1 AND disabled_at IS NOT NULL`, id); n != 1 {
		t.Fatal("the projection cleared the suspension")
	}
	if got := e.outboxRows(t, ctx, "organization.reactivated", id.String()); got != reactivated {
		t.Fatalf("reactivated events %d -> %d", reactivated, got)
	}
	var after int64
	if err := e.admin.QueryRow(ctx, `SELECT revision FROM auth.aggregate_revisions WHERE aggregate_type = 'organization' AND aggregate_id = $1`, "org_"+id.String()).Scan(&after); err != nil {
		t.Fatal(err)
	}
	if after != rev {
		t.Fatalf("revision %d -> %d", rev, after)
	}
	// Control: the same call with the switch off does reactivate (the defect this closes).
	if err := e.store.ProjectOrganization(ctx, id); err != nil {
		t.Fatal(err)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.organizations WHERE id = $1 AND disabled_at IS NULL`, id); n != 1 {
		t.Fatal("control failed: the switch-off projection no longer reactivates, so this test proves nothing")
	}
}

func TestProjectOrganizationOwnedByAuthDoesNotOverwriteARename(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	id := e.seedAuthOwnedOrg(t, ctx)
	if err := e.orgStore(t).Rename(ctx, id, "Renamed by auth", nil, org.Actor{}); err != nil {
		t.Fatal(err)
	}
	if err := e.ownedOrgsStore(t).ProjectOrganization(ctx, id); err != nil {
		t.Fatal(err)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.organizations WHERE id = $1 AND name = 'Renamed by auth'`, id); n != 1 {
		t.Fatal("the projection overwrote the name")
	}
}

func TestProjectOrganizationOwnedByAuthRefusesAMissingOrganization(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	id := e.seedOrg(t, ctx) // in public, never projected
	if err := e.ownedOrgsStore(t).ProjectOrganization(ctx, id); !errors.Is(err, ErrOrganizationNotFound) {
		t.Fatalf("missing auth row = %v, want ErrOrganizationNotFound", err)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.organizations WHERE id = $1`, id); n != 0 {
		t.Fatal("the owned projection wrote a row")
	}
}

func TestProjectOrganizationOwnedByAuthWritesNothingForAnExistingUnchangedOrganization(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	id := e.seedAuthOwnedOrg(t, ctx)
	before := len(e.orgAudit(t, ctx, id))
	if err := e.ownedOrgsStore(t).ProjectOrganization(ctx, id); err != nil {
		t.Fatal(err)
	}
	if got := len(e.orgAudit(t, ctx, id)); got != before {
		t.Fatalf("audit rows %d -> %d", before, got)
	}
}

func TestProjectOrganizationOffStillProjectsAPublicChange(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	id := e.seedAuthOwnedOrg(t, ctx)
	if _, err := e.admin.Exec(ctx, `UPDATE public.organizations SET name = 'legacy rename' WHERE id = $1`, id); err != nil {
		t.Fatal(err)
	}
	if err := e.store.ProjectOrganization(ctx, id); err != nil {
		t.Fatal(err)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.organizations WHERE id = $1 AND name = 'legacy rename'`, id); n != 1 {
		t.Fatal("the default projection stopped copying public.organizations")
	}
}
