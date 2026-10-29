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
	// An existing row is never changed: go-api's rename does not reach it
	// through EnsureOrganization (ProjectOrganization would take it).
	if _, err := e.admin.Exec(ctx, `UPDATE public.organizations SET name = 'renamed-after-ensure' WHERE id = $1`, id); err != nil {
		t.Fatal(err)
	}
	if err := e.store.EnsureOrganization(ctx, id); err != nil {
		t.Fatalf("EnsureOrganization of an existing row: %v", err)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.organizations WHERE id = $1 AND name = 'renamed-after-ensure'`, id); n != 0 {
		t.Fatal("EnsureOrganization changed an existing row")
	}
	if err := e.store.EnsureOrganization(ctx, uuid.New()); !errors.Is(err, ErrOrganizationNotFound) {
		t.Fatalf("EnsureOrganization of an unknown organization = %v, want ErrOrganizationNotFound", err)
	}
}

// No refresh reactivates an organization suspended in the auth schema: the
// sign-in projection and the organizations backfill still take go-api's name,
// and never clear disabled_at, while go-api's row is active.
func TestProjectionAndBackfillKeepASuspendedOrganizationSuspended(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	id := e.seedOrg(t, ctx)
	if err := e.store.ProjectOrganization(ctx, id); err != nil {
		t.Fatal(err)
	}
	if err := e.orgStore(t).Suspend(ctx, id, nil, org.Actor{}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.admin.Exec(ctx, `UPDATE public.organizations SET name = 'renamed-while-suspended' WHERE id = $1`, id); err != nil {
		t.Fatal(err)
	}
	if err := e.store.ProjectOrganization(ctx, id); err != nil {
		t.Fatalf("ProjectOrganization: %v", err)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.organizations WHERE id = $1 AND disabled_at IS NOT NULL AND name = 'renamed-while-suspended'`, id); n != 1 {
		t.Fatal("the projection reactivated a suspended organization or dropped go-api's name")
	}
	var slug string
	if err := e.admin.QueryRow(ctx, `SELECT slug FROM public.organizations WHERE id = $1`, id).Scan(&slug); err != nil {
		t.Fatal(err)
	}
	res, err := e.store.BackfillOrganization(ctx, OrgBackfillTarget{ID: id, Slug: slug, Name: "renamed-again", Active: true}, false)
	if err != nil || res.Reactivated {
		t.Fatalf("BackfillOrganization = %+v, %v; want no reactivation", res, err)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.organizations WHERE id = $1 AND disabled_at IS NOT NULL AND name = 'renamed-again'`, id); n != 1 {
		t.Fatal("the backfill reactivated a suspended organization or dropped go-api's name")
	}
	if n := e.outboxRows(t, ctx, "organization.reactivated", id.String()); n != 0 {
		t.Fatalf("%d organization.reactivated events, want 0", n)
	}
}

// A projection planned before a suspension commits never clears it: the UPDATE
// itself leaves disabled_at in place when go-api's row is active (the read the
// projection made is stale by then), and still takes go-api's name.
func TestUpdateOrganizationNeverClearsASuspensionReadStale(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	id := e.seedOrg(t, ctx)
	if err := e.store.ProjectOrganization(ctx, id); err != nil {
		t.Fatal(err)
	}
	var stale orgState
	if err := e.admin.QueryRow(ctx, `SELECT slug, name, disabled_at IS NULL FROM auth.organizations WHERE id = $1`, id).
		Scan(&stale.slug, &stale.name, &stale.active); err != nil || !stale.active {
		t.Fatalf("projected row: %+v %v", stale, err)
	}
	// The deprovision (here the organization verb) commits after the projection read.
	if err := e.orgStore(t).Suspend(ctx, id, nil, org.Actor{}); err != nil {
		t.Fatal(err)
	}
	src := orgState{slug: stale.slug, name: "renamed-after-a-stale-read", active: true}
	if err := e.store.updateOrganization(ctx, id, src, stale); err != nil {
		t.Fatalf("updateOrganization: %v", err)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.organizations WHERE id = $1 AND disabled_at IS NOT NULL AND name = 'renamed-after-a-stale-read'`, id); n != 1 {
		t.Fatal("the stale projection cleared the suspension or dropped go-api's name")
	}
	if n := e.outboxRows(t, ctx, "organization.reactivated", id.String()); n != 0 {
		t.Fatalf("%d organization.reactivated events, want 0", n)
	}
	// A suspension still flows: go-api's is_active false suspends an active organization.
	other := e.seedOrg(t, ctx)
	if err := e.store.ProjectOrganization(ctx, other); err != nil {
		t.Fatal(err)
	}
	if _, err := e.admin.Exec(ctx, `UPDATE public.organizations SET is_active = false WHERE id = $1`, other); err != nil {
		t.Fatal(err)
	}
	if err := e.store.ProjectOrganization(ctx, other); err != nil {
		t.Fatal(err)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.organizations WHERE id = $1 AND disabled_at IS NOT NULL`, other); n != 1 {
		t.Fatal("go-api's suspension did not reach the auth organization")
	}
}
