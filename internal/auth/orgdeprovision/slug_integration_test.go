//go:build integration

package orgdeprovision

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/auth/org"
)

// freedSlug is the form a deprovision leaves in auth.organizations.slug, written out here so a change of the
// production form is a red cell, never a silent agreement.
func freedSlug(id uuid.UUID) string { return "~deleted~" + id.String() }

type authOrgRow struct {
	slug, name        string
	disabled          bool
	disabledAt        time.Time
	createdAt         time.Time
	auditRows, outbox int
}

func (r authOrgRow) same(o authOrgRow) bool {
	return r.slug == o.slug && r.name == o.name && r.disabled == o.disabled && r.disabledAt.Equal(o.disabledAt) &&
		r.createdAt.Equal(o.createdAt) && r.auditRows == o.auditRows && r.outbox == o.outbox
}

func (e *env) authOrg(t *testing.T, ctx context.Context, id uuid.UUID) authOrgRow {
	t.Helper()
	var r authOrgRow
	var disabledAt *time.Time
	if err := e.admin.QueryRow(ctx, `SELECT slug, name, disabled_at IS NOT NULL, disabled_at, created_at FROM auth.organizations WHERE id = $1`, id).
		Scan(&r.slug, &r.name, &r.disabled, &disabledAt, &r.createdAt); err != nil {
		t.Fatalf("read auth.organizations %v: %v", id, err)
	}
	if disabledAt != nil {
		r.disabledAt = *disabledAt
	}
	r.auditRows = e.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE organization_id = $1`, id.String())
	r.outbox = e.orgEvents(t, ctx, "", id)
	return r
}

// orgEvents counts the organization's outbox events of one type ("" = every type).
func (e *env) orgEvents(t *testing.T, ctx context.Context, eventType string, id uuid.UUID) int {
	t.Helper()
	return e.count(t, ctx, `SELECT count(*) FROM auth.auth_outbox_events WHERE aggregate_type = 'organization'
AND payload->>'organization_id' = $1 AND ($2 = '' OR event_type = $2)`, "org_"+id.String(), eventType)
}

func (e *env) publicSlug(t *testing.T, ctx context.Context, id uuid.UUID) string {
	t.Helper()
	var slug string
	if err := e.admin.QueryRow(ctx, `SELECT slug FROM public.organizations WHERE id = $1`, id).Scan(&slug); err != nil {
		t.Fatalf("read public.organizations %v: %v", id, err)
	}
	return slug
}

// purge is go-api's delete after the deprovision: the public rows go.
func (e *env) purge(t *testing.T, ctx context.Context, id uuid.UUID) {
	t.Helper()
	for _, sql := range []string{`DELETE FROM public.memberships WHERE org_id = $1`, `DELETE FROM public.org_invites WHERE org_id = $1`,
		`DELETE FROM public.organizations WHERE id = $1`} {
		if _, err := e.admin.Exec(ctx, sql, id); err != nil {
			t.Fatalf("purge %q: %v", sql, err)
		}
	}
}

// seedOrgWithSlug is seedOrg with a chosen slug.
func (e *env) seedOrgWithSlug(t *testing.T, ctx context.Context, slug string) uuid.UUID {
	t.Helper()
	id := uuid.New()
	if _, err := e.admin.Exec(ctx, `INSERT INTO public.organizations (id, slug, name) VALUES ($1, $2, 'slug-reuse-test')`, id, slug); err != nil {
		t.Fatalf("seed public.organizations: %v", err)
	}
	if err := e.identity.ProjectOrganization(ctx, id); err != nil {
		t.Fatalf("ProjectOrganization: %v", err)
	}
	return id
}

// A deprovision frees the organization's slug in the same commit that suspends it: the row keeps its id, name, creation
// time and suspension, and holds the freed form. go-api's row (still there until the purge) keeps its slug. The commit
// carries the organization.suspended event and no organization.updated one.
func TestDeprovisionFreesTheSlug(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	orgID := e.seedOrg(t, ctx)
	slug := e.publicSlug(t, ctx, orgID)
	before := e.authOrg(t, ctx, orgID)
	if before.slug != slug || before.disabled {
		t.Fatalf("before: auth row %+v, want slug %q active", before, slug)
	}
	if _, err := e.service.Deprovision(ctx, Input{OrgID: orgID, Authorize: allowOrg}); err != nil {
		t.Fatalf("Deprovision: %v", err)
	}
	after := e.authOrg(t, ctx, orgID)
	if after.slug != freedSlug(orgID) {
		t.Fatalf("auth slug after the deprovision = %q, want %q", after.slug, freedSlug(orgID))
	}
	if !after.disabled || after.name != before.name || !after.createdAt.Equal(before.createdAt) {
		t.Fatalf("after: %+v; want suspended, name %q, created_at %v", after, before.name, before.createdAt)
	}
	if got := e.publicSlug(t, ctx, orgID); got != slug {
		t.Fatalf("public slug = %q, want %q kept", got, slug)
	}
	if n := e.orgEvents(t, ctx, "organization.suspended", orgID); n != 1 {
		t.Fatalf("organization.suspended events = %d, want 1", n)
	}
	if n := e.orgEvents(t, ctx, "organization.updated", orgID); n != 0 {
		t.Fatalf("organization.updated events = %d, want 0", n)
	}
	if n := e.auditRows(t, ctx, EventDeprovisioned, orgID); n != 1 {
		t.Fatalf("deprovision audit rows = %d, want 1", n)
	}
}

// An organization suspended before the deprovision kept its slug (and so does a row a deprovision before this fix
// left): a deprovision frees it with one audit row and one organization.updated event, and suspends nothing again.
// A repeat writes nothing.
func TestDeprovisionFreesTheSlugOfASuspendedOrganization(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	orgID := e.seedOrg(t, ctx)
	slug := e.publicSlug(t, ctx, orgID)
	if err := e.orgs.Suspend(ctx, orgID, nil, org.Actor{}); err != nil {
		t.Fatalf("Suspend: %v", err)
	}
	suspended := e.authOrg(t, ctx, orgID)
	if suspended.slug != slug || !suspended.disabled {
		t.Fatalf("suspended row %+v, want slug %q kept and suspended", suspended, slug)
	}
	res, err := e.service.Deprovision(ctx, Input{OrgID: orgID, Authorize: allowOrg})
	if err != nil {
		t.Fatalf("Deprovision: %v", err)
	}
	if *res != (Result{}) {
		t.Fatalf("result = %+v, want zero counts (the organization was already suspended)", res)
	}
	after := e.authOrg(t, ctx, orgID)
	if after.slug != freedSlug(orgID) || !after.disabled || !after.disabledAt.Equal(suspended.disabledAt) {
		t.Fatalf("after: %+v; want slug %q and the suspension of %v kept", after, freedSlug(orgID), suspended.disabledAt)
	}
	if n := e.orgEvents(t, ctx, "organization.updated", orgID); n != 1 {
		t.Fatalf("organization.updated events = %d, want 1", n)
	}
	if n := e.orgEvents(t, ctx, "organization.suspended", orgID); n != 1 {
		t.Fatalf("organization.suspended events = %d, want 1 (the suspend's only)", n)
	}
	if n := e.auditRows(t, ctx, EventDeprovisioned, orgID); n != 1 {
		t.Fatalf("deprovision audit rows = %d, want 1", n)
	}
	if _, err := e.service.Deprovision(ctx, Input{OrgID: orgID, Authorize: allowOrg}); err != nil {
		t.Fatalf("repeat Deprovision: %v", err)
	}
	if again := e.authOrg(t, ctx, orgID); !again.same(after) {
		t.Fatalf("a repeat with nothing left wrote: %+v, was %+v", again, after)
	}
}

// A plain suspension is not a delete: the slug stays.
func TestSuspendKeepsTheSlug(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	orgID := e.seedOrg(t, ctx)
	slug := e.publicSlug(t, ctx, orgID)
	if err := e.orgs.Suspend(ctx, orgID, nil, org.Actor{}); err != nil {
		t.Fatalf("Suspend: %v", err)
	}
	if got := e.authOrg(t, ctx, orgID); got.slug != slug || !got.disabled {
		t.Fatalf("after Suspend: %+v, want slug %q and suspended", got, slug)
	}
}

// Between the deprovision and go-api's purge (or after a purge that failed) go-api's row still holds the old slug: a
// sign-in projection or an organizations backfill of it never writes that slug back.
func TestAProjectionKeepsTheFreedSlug(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	orgID := e.seedOrg(t, ctx)
	slug := e.publicSlug(t, ctx, orgID)
	if _, err := e.service.Deprovision(ctx, Input{OrgID: orgID, Authorize: allowOrg}); err != nil {
		t.Fatalf("Deprovision: %v", err)
	}
	if err := e.identity.ProjectOrganization(ctx, orgID); err != nil {
		t.Fatalf("ProjectOrganization: %v", err)
	}
	if got := e.authOrg(t, ctx, orgID); got.slug != freedSlug(orgID) || !got.disabled {
		t.Fatalf("after a projection: %+v, want slug %q and suspended", got, freedSlug(orgID))
	}
	if _, err := e.identity.BackfillOrganization(ctx, identity.OrgBackfillTarget{ID: orgID, Slug: slug, Name: "deprovision-test", Active: true}, false); err != nil {
		t.Fatalf("BackfillOrganization: %v", err)
	}
	if got := e.authOrg(t, ctx, orgID); got.slug != freedSlug(orgID) || !got.disabled {
		t.Fatalf("after a backfill: %+v, want slug %q and suspended", got, freedSlug(orgID))
	}
}

// The product sequence at the store: X deprovisioned and purged, then a new organization Y with X's slug. Y's first
// projection inserts Y's own row (it errored before the fix); X's row keeps its id, name, suspension and audit
// history, X's memberships stay removed, and Y gets no membership. A second delete of the same name frees Y's slug
// in its own form.
func TestTheFreedSlugIsReusedByANewOrganization(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	slug := "reuse-" + uuid.NewString()[:8]
	x := e.seedOrgWithSlug(t, ctx, slug)
	owner, _ := e.seedUser(t, ctx)
	e.add(t, ctx, x, owner, "owner")
	if _, err := e.service.Deprovision(ctx, Input{OrgID: x, Authorize: allowOrg}); err != nil {
		t.Fatalf("Deprovision X: %v", err)
	}
	e.purge(t, ctx, x)
	xRow := e.authOrg(t, ctx, x)
	y := e.seedOrgWithSlug(t, ctx, slug)
	if y == x {
		t.Fatal("Y took X's id")
	}
	yRow := e.authOrg(t, ctx, y)
	if yRow.slug != slug || yRow.disabled {
		t.Fatalf("Y's auth row %+v, want slug %q active", yRow, slug)
	}
	if got := e.authOrg(t, ctx, x); !got.same(xRow) {
		t.Fatalf("X's auth row changed when Y was projected: %+v, was %+v", got, xRow)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.memberships WHERE organization_id = $1 AND status <> 'removed'`, x); n != 0 {
		t.Fatalf("X has %d live auth memberships, want 0", n)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.memberships WHERE organization_id = $1`, y); n != 0 {
		t.Fatalf("Y has %d auth memberships, want 0 (nothing of X attaches to Y)", n)
	}
	if _, err := e.service.Deprovision(ctx, Input{OrgID: y, Authorize: allowOrg}); err != nil {
		t.Fatalf("Deprovision Y: %v", err)
	}
	if got := e.authOrg(t, ctx, y); got.slug != freedSlug(y) {
		t.Fatalf("Y's slug after its deprovision = %q, want %q", got.slug, freedSlug(y))
	}
}

// A reactivation of a deprovisioned organization keeps the freed slug (the defined answer: the slug is not restored).
func TestReactivateKeepsTheFreedSlug(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	orgID := e.seedOrg(t, ctx)
	if _, err := e.service.Deprovision(ctx, Input{OrgID: orgID, Authorize: allowOrg}); err != nil {
		t.Fatalf("Deprovision: %v", err)
	}
	if err := e.orgs.Reactivate(ctx, orgID, nil, org.Actor{}); err != nil {
		t.Fatalf("Reactivate: %v", err)
	}
	if got := e.authOrg(t, ctx, orgID); got.slug != freedSlug(orgID) || got.disabled {
		t.Fatalf("after Reactivate: %+v, want slug %q and active", got, freedSlug(orgID))
	}
}
