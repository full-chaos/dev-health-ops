//go:build integration

package authruntime

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/crypto/bcrypt"

	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/auth/usersbackfill"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
)

// TestUsersOwnedGuardWaivesOnlyKeyableBoundOIDCUsers runs the real users backfill and then the guard, both as the
// auth runtime role. An OIDC user bound to an OIDC provider row (users.sso_provider_id) with a subject the sign-in
// path can key is re-linked at its first owned sign-in, so the guard is ready with it and no identity. A bound SAML
// user is never waived: go-api stores only the NameID value, not its format, so its key cannot be verified; it holds
// the guard until it has an identity. Each other shape (unbound, no subject, a subject the key rule refuses, bound to
// a row of another protocol) and a social user with no subject is refused until the row is repaired.
func TestUsersOwnedGuardWaivesOnlyKeyableBoundOIDCUsers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate PostgreSQL: %v", err)
		}
	})
	admin, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	pgschema.Apply(ctx, t, admin)
	role, err := containers.RoleName("guard_sso", instance)
	if err != nil {
		t.Fatal(err)
	}
	password := testRolePassword(t)
	if _, err := admin.Exec(ctx, fmt.Sprintf(`CREATE ROLE %q LOGIN PASSWORD '%s'`, role, password)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { containers.DropRole(admin, role, t.Logf) })
	if _, err := authschema.Apply(ctx, admin, authschema.Options{Schema: "auth", RuntimeRole: role, UsersOwned: true}); err != nil {
		t.Fatalf("authschema.Apply: %v", err)
	}
	cfg := admin.Config().Copy()
	cfg.ConnConfig.User = role
	cfg.ConnConfig.Password = password
	runtime, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(runtime.Close)
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		t.Fatal(err)
	}
	store, err := identity.New(identity.Config{Pool: runtime, Schema: schema})
	if err != nil {
		t.Fatal(err)
	}

	org := uuid.New()
	if _, err := admin.Exec(ctx, `INSERT INTO public.organizations (id, slug, name) VALUES ($1, $2, 'guard-sso')`, org, "org-"+org.String()); err != nil {
		t.Fatal(err)
	}
	provider := func(protocol string) uuid.UUID {
		t.Helper()
		id := uuid.New()
		if _, err := admin.Exec(ctx, `INSERT INTO public.sso_providers (id, org_id, name, protocol, status) VALUES ($1, $2, $3, $4, 'active')`,
			id, org, "idp-"+id.String(), protocol); err != nil {
			t.Fatalf("sso provider %s: %v", protocol, err)
		}
		return id
	}
	oidcRow, samlRow := provider("oidc"), provider("saml")
	user := func(authProvider string, hash, subject *string, bound *uuid.UUID) uuid.UUID {
		t.Helper()
		id := uuid.New()
		if _, err := admin.Exec(ctx, `INSERT INTO public.users
	(id, email, password_hash, auth_provider, auth_provider_id, sso_provider_id, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, true, true, false, 0, now(), now())`, id, id.String()+"@example.com", hash, authProvider, subject, bound); err != nil {
			t.Fatalf("public user %s: %v", authProvider, err)
		}
		if _, err := admin.Exec(ctx, `INSERT INTO public.memberships (id, user_id, org_id, role) VALUES ($1, $2, $3, 'member')`, uuid.New(), id, org); err != nil {
			t.Fatalf("membership %s: %v", authProvider, err)
		}
		return id
	}
	backfill := func(wantSkipped int) usersbackfill.Report {
		t.Helper()
		rep, err := usersbackfill.Run(ctx, usersbackfill.PGSource{Pool: runtime}, store, usersbackfill.Options{})
		if err != nil {
			t.Fatalf("users backfill: %v", err)
		}
		if rep.SkippedTotal() != wantSkipped {
			t.Fatalf("users backfill skipped %d rows, want %d: %v", rep.SkippedTotal(), wantSkipped, rep.Lines())
		}
		return rep
	}
	check := func(step, want string) {
		t.Helper()
		err := checkUsersOwned(ctx, runtime, schema)
		if want == "" {
			if err != nil {
				t.Fatalf("%s: expected ready, got %v", step, err)
			}
			return
		}
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Fatalf("%s: expected a refusal naming %q, got %v", step, want, err)
		}
	}
	set := func(id uuid.UUID, column string, value any) {
		t.Helper()
		if _, err := admin.Exec(ctx, `UPDATE public.users SET `+column+` = $2 WHERE id = $1`, id, value); err != nil {
			t.Fatalf("update %s: %v", column, err)
		}
	}
	s := func(v string) *string { return &v }
	const refused = "provider users without an external identity=1"

	hash, err := bcrypt.GenerateFromPassword([]byte("guard-sso-local"), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	user("local", s(string(hash)), nil, nil)
	user("github", nil, s("920001"), nil)
	user("oidc", nil, s("oidc-sub-1"), &oidcRow)
	backfill(0)
	check("local + github + bound oidc", "")

	samlBound := user("saml", nil, s("nameid-1@example.com"), &samlRow)
	backfill(0)
	check("saml row bound to a saml provider row (NameID format not stored)", refused+" (sso_subject_unverifiable=1)")
	var links int
	if err := admin.QueryRow(ctx, `SELECT count(*) FROM auth.external_identities WHERE provider IN ($1, $2)`,
		oidcRow.String(), samlRow.String()).Scan(&links); err != nil || links != 0 {
		t.Fatalf("the backfill linked %d SSO identities (%v); a bound SSO user is never carried", links, err)
	}
	if _, err := store.LinkIdentity(ctx, identity.ExternalKey{Issuer: "https://idp.example.com/saml", Provider: identity.Provider(samlRow.String()),
		Subject: "saml1|urn:oasis:names:tc:SAML:2.0:nameid-format:persistent||nameid-1@example.com"}, samlBound, identity.Actor{}); err != nil {
		t.Fatalf("link the saml identity: %v", err)
	}
	check("the saml row with its identity", "")

	unbound := user("oidc", nil, s("oidc-sub-2"), nil)
	backfill(0)
	check("oidc row with no provider binding", refused)
	set(unbound, "sso_provider_id", oidcRow)
	backfill(0)
	check("the oidc row bound again", "")

	noSubject := user("oidc", nil, nil, &oidcRow)
	backfill(0)
	check("oidc row with no subject", refused)
	set(noSubject, "auth_provider_id", "oidc-sub-5")
	backfill(0)
	check("the oidc row with its subject", "")

	emptySubject := user("oidc", nil, s(""), &oidcRow)
	backfill(0)
	check("oidc row with an empty subject", refused)
	set(emptySubject, "auth_provider_id", "oidc-sub-3")
	backfill(0)
	check("the oidc row with its subject", "")

	edgeSpace := user("oidc", nil, s(" oidc-sub-6"), &oidcRow)
	backfill(0)
	check("oidc row with a subject the key rule refuses (leading space)", refused)
	set(edgeSpace, "auth_provider_id", "oidc-sub-6")
	backfill(0)
	check("the oidc row with a keyable subject", "")

	otherProtocol := user("oidc", nil, s("oidc-sub-4"), &samlRow)
	backfill(0)
	check("oidc row bound to a saml provider row", refused)
	set(otherProtocol, "sso_provider_id", oidcRow)
	backfill(0)
	check("the oidc row bound to an oidc provider row", "")

	socialEmpty := user("github", nil, s(""), nil)
	backfill(1)
	check("github row with an empty subject (skipped as invalid_identity)", "public users without an auth user=1")
	check("github row with an empty subject (skipped as invalid_identity)", refused)
	set(socialEmpty, "auth_provider_id", "920002")
	backfill(0)
	check("the github row with its subject", "")
}
