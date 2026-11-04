//go:build integration

package identity

import (
	"context"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/org"
)

func (e *env) orgStore(t *testing.T) *org.Store {
	t.Helper()
	s, err := org.New(org.Config{Pool: e.runtime, Schema: e.store.schema})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// seedAuthOwnedOrg projects a public org once and returns its id.
func (e *env) seedAuthOwnedOrg(t *testing.T, ctx context.Context) uuid.UUID {
	t.Helper()
	id := e.seedOrg(t, ctx)
	if err := e.store.ProjectOrganization(ctx, id); err != nil {
		t.Fatal(err)
	}
	return id
}

// A suspended organization stays suspended while go-api's row is active: the projection takes go-api's name (it does
// write) but never clears disabled_at (keepSuspended).
func TestProjectOrganizationKeepsASuspendedOrganizationSuspended(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	id := e.seedAuthOwnedOrg(t, ctx)
	if err := e.orgStore(t).Suspend(ctx, id, nil, org.Actor{}); err != nil {
		t.Fatal(err)
	}
	reactivated := e.outboxRows(t, ctx, "organization.reactivated", id.String())
	if _, err := e.admin.Exec(ctx, `UPDATE public.organizations SET name = 'renamed-by-go-api' WHERE id = $1`, id); err != nil {
		t.Fatal(err)
	}
	if err := e.store.ProjectOrganization(ctx, id); err != nil {
		t.Fatal(err)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.organizations WHERE id = $1 AND name = 'renamed-by-go-api'`, id); n != 1 {
		t.Fatal("control failed: the projection wrote nothing, so this test proves nothing")
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.organizations WHERE id = $1 AND disabled_at IS NOT NULL`, id); n != 1 {
		t.Fatal("the projection cleared the suspension")
	}
	if got := e.outboxRows(t, ctx, "organization.reactivated", id.String()); got != reactivated {
		t.Fatalf("reactivated events %d -> %d", reactivated, got)
	}
}

func TestProjectOrganizationStillProjectsAPublicChange(t *testing.T) {
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
		t.Fatal("the projection stopped copying public.organizations")
	}
}
