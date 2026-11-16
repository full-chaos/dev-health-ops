//go:build integration

package identity

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts/provisioning"
)

// The executed write cell of the admin force delete (PLAN-13380 S5), as the NOSUPERUSER runtime role with users owned:
// the erase deletes the principal, auth.users and public.users; the impersonation and billing-audit rows that named the
// user stay with the reference set to NULL; the create's audit row stays with its subject set to NULL and the id in
// its attributes; the erase row names the admin and the id; principal.disabled is on the outbox; then GET finds no
// user, a repeat answers ErrUserNotFound, and the same email is created again.
func TestAdminForceDeleteErasesTheUserAndItsEmailRegistersAgain(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	none := func(context.Context, audit.TxOps) error { return nil }
	actor := e.seedUser(t, ctx, uniqueEmail("erase-actor"), true, true)
	e.project(t, ctx, actor)
	email := uniqueEmail("erase")
	created, err := e.owned.AdminCreateUser(ctx, AdminCreateRequest{Email: email, Actor: actor, Authorize: none})
	if err != nil {
		t.Fatal(err)
	}
	org := uuid.New()
	if _, err := e.admin.Exec(ctx, `INSERT INTO public.organizations (id, slug, name) VALUES ($1, $2, 'erase')`, org, "org-"+org.String()); err != nil {
		t.Fatal(err)
	}
	impersonation, billing := uuid.New(), uuid.New()
	if _, err := e.admin.Exec(ctx, `INSERT INTO public.impersonation_sessions (id, admin_user_id, target_user_id, target_org_id, target_role, expires_at)
VALUES ($1, $2, $3, $4, 'member', now() + interval '1 hour')`, impersonation, created.ID, actor, org); err != nil {
		t.Fatal(err)
	}
	if _, err := e.admin.Exec(ctx, `INSERT INTO public.billing_audit_log (id, org_id, actor_id, action, resource_type, resource_id, description)
VALUES ($1, $2, $3, 'x', 'x', $1, 'kept verbatim')`, billing, org, created.ID); err != nil {
		t.Fatal(err)
	}

	result, err := e.owned.AdminForceDeleteUser(ctx, AdminForceDeleteRequest{Principal: created.ID, Actor: actor, Authorize: none, RevokeRoles: none})
	if err != nil {
		t.Fatalf("erase: %v", err)
	}
	if !result.WasActive {
		t.Error("was_active = false, want true")
	}
	var principals, authUsers, publicUsers int
	if err := e.admin.QueryRow(ctx, `SELECT (SELECT count(*) FROM auth.principals WHERE id = $1), (SELECT count(*) FROM auth.users WHERE principal_id = $1),
(SELECT count(*) FROM public.users WHERE id = $1)`, created.ID).Scan(&principals, &authUsers, &publicUsers); err != nil {
		t.Fatal(err)
	}
	if principals+authUsers+publicUsers != 0 {
		t.Errorf("rows left: principals %d, auth.users %d, public.users %d; want 0", principals, authUsers, publicUsers)
	}
	var adminRef *uuid.UUID
	var targetRef uuid.UUID
	if err := e.admin.QueryRow(ctx, `SELECT admin_user_id, target_user_id FROM public.impersonation_sessions WHERE id = $1`, impersonation).Scan(&adminRef, &targetRef); err != nil {
		t.Fatalf("the impersonation row is gone: %v", err)
	}
	if adminRef != nil || targetRef != actor {
		t.Errorf("impersonation admin_user_id %v target_user_id %v, want NULL and the actor", adminRef, targetRef)
	}
	var actorRef *uuid.UUID
	var description string
	if err := e.admin.QueryRow(ctx, `SELECT actor_id, description FROM public.billing_audit_log WHERE id = $1`, billing).Scan(&actorRef, &description); err != nil {
		t.Fatalf("the billing audit row is gone: %v", err)
	}
	if actorRef != nil || description != "kept verbatim" {
		t.Errorf("billing audit actor_id %v description %q, want NULL and the text verbatim", actorRef, description)
	}
	var kept, erased int
	if err := e.admin.QueryRow(ctx, `SELECT
  (SELECT count(*) FROM auth.security_audit_events WHERE subject_principal_id IS NULL AND attributes->>'target_id' = $1 AND event_type <> 'auth.user.erased'),
  (SELECT count(*) FROM auth.security_audit_events WHERE event_type = 'auth.user.erased' AND subject_principal_id IS NULL
     AND actor_principal_id = $2 AND attributes->>'target_id' = $1 AND attributes->>'reason_code' = 'principal_erased')`,
		created.ID.String(), actor).Scan(&kept, &erased); err != nil {
		t.Fatal(err)
	}
	if kept < 1 || erased != 1 {
		t.Errorf("audit rows: %d earlier rows kept with a NULL subject (want >= 1), %d erase rows (want 1)", kept, erased)
	}
	var disabled int
	if err := e.admin.QueryRow(ctx, `SELECT count(*) FROM auth.auth_outbox_events WHERE aggregate_type = 'principal' AND aggregate_id = $1
AND event_type = 'principal.disabled'`, provisioning.WireID("principal", created.ID.String())).Scan(&disabled); err != nil {
		t.Fatal(err)
	}
	if disabled < 1 {
		t.Error("no principal.disabled event for the erased principal")
	}

	if _, err := e.owned.AdminForceDeleteUser(ctx, AdminForceDeleteRequest{Principal: created.ID, Actor: actor, Authorize: none, RevokeRoles: none}); !errors.Is(err, ErrUserNotFound) {
		t.Errorf("a repeat erase: %v, want ErrUserNotFound", err)
	}
	again, err := e.owned.AdminCreateUser(ctx, AdminCreateRequest{Email: email, Actor: actor, Authorize: none})
	if err != nil {
		t.Fatalf("the same email after the erase: %v", err)
	}
	if again.ID == created.ID {
		t.Error("the new user has the erased principal's id")
	}
}

// A user that a RESTRICT history row of the auth schema names (here: it granted a platform role, 0034:47) is refused
// with ErrReferenceBlocked, and nothing is written: the user, its public row and its audit rows stay as they were.
func TestAdminForceDeleteOfAReferencedUserWritesNothing(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	none := func(context.Context, audit.TxOps) error { return nil }
	actor := e.seedUser(t, ctx, uniqueEmail("erase-blocked-actor"), true, true)
	e.project(t, ctx, actor)
	created, err := e.owned.AdminCreateUser(ctx, AdminCreateRequest{Email: uniqueEmail("erase-blocked"), Actor: actor, Authorize: none})
	if err != nil {
		t.Fatal(err)
	}
	// The blocking row, seeded as the table owner with the triggers off for this one statement only.
	tx, err := e.admin.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `SET LOCAL session_replication_role = replica`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO auth.platform_role_assignments (principal_id, role_key, granted_by) VALUES ($1, 'platform_admin', $2)`,
		actor, created.ID); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	var before int
	if err := e.admin.QueryRow(ctx, `SELECT count(*) FROM auth.security_audit_events WHERE subject_principal_id = $1`, created.ID).Scan(&before); err != nil {
		t.Fatal(err)
	}
	_, err = e.owned.AdminForceDeleteUser(ctx, AdminForceDeleteRequest{Principal: created.ID, Actor: actor, Authorize: none, RevokeRoles: none})
	if !errors.Is(err, ErrReferenceBlocked) {
		t.Fatalf("erase of a user a RESTRICT row names: %v, want ErrReferenceBlocked", err)
	}
	var principals, publicUsers, after int
	if err := e.admin.QueryRow(ctx, `SELECT (SELECT count(*) FROM auth.principals WHERE id = $1), (SELECT count(*) FROM public.users WHERE id = $1),
(SELECT count(*) FROM auth.security_audit_events WHERE subject_principal_id = $1)`, created.ID).Scan(&principals, &publicUsers, &after); err != nil {
		t.Fatal(err)
	}
	if principals != 1 || publicUsers != 1 || after != before || before < 1 {
		t.Errorf("after the refused erase: principals %d, public.users %d, audit rows %d -> %d; want 1, 1, unchanged and >= 1", principals, publicUsers, before, after)
	}
}
