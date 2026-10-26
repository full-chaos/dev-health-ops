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
	"github.com/full-chaos/dev-health-ops/internal/edgetokenmint"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
)

// TestUsersOwnedGuardAndBackfillAgreeOnAServicePrincipalInLivePostgres runs the real users backfill and then the
// guard, both as the auth runtime role: local + github + the two proof placeholders is ready and the backfill report
// still lists them as not carried; a same-shaped service row under another id, a saml user without an identity, a
// proof principal with a password and a service row with a password hash are each refused.
func TestUsersOwnedGuardAndBackfillAgreeOnAServicePrincipalInLivePostgres(t *testing.T) {
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
	role, err := containers.RoleName("guard_backfill", instance)
	if err != nil {
		t.Fatal(err)
	}
	const password = "guard-backfill-test-only-not-a-credential"
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

	user := func(provider string, hash, subject *string) uuid.UUID {
		t.Helper()
		id := uuid.New()
		if _, err := admin.Exec(ctx, `INSERT INTO public.users
	(id, email, password_hash, auth_provider, auth_provider_id, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, true, true, false, 0, now(), now())`, id, id.String()+"@example.com", hash, provider, subject); err != nil {
			t.Fatalf("public user %s: %v", provider, err)
		}
		return id
	}
	backfill := func() usersbackfill.Report {
		t.Helper()
		rep, err := usersbackfill.Run(ctx, usersbackfill.PGSource{Pool: runtime}, store, usersbackfill.Options{})
		if err != nil {
			t.Fatalf("users backfill: %v", err)
		}
		if rep.SkippedTotal() != 0 {
			t.Fatalf("users backfill skipped rows: %v", rep.Lines())
		}
		return rep
	}
	check := func(want string) {
		t.Helper()
		err := checkUsersOwned(ctx, runtime, schema)
		if want == "" {
			if err != nil {
				t.Fatalf("expected ready, got %v", err)
			}
			return
		}
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Fatalf("expected a refusal naming %q, got %v", want, err)
		}
	}
	link := func(id uuid.UUID) {
		t.Helper()
		key := identity.ExternalKey{Issuer: "https://github.com", Provider: identity.ProviderGitHub, Subject: id.String()}
		if _, err := store.LinkIdentity(ctx, key, id, identity.Actor{}); err != nil {
			t.Fatalf("identity: %v", err)
		}
	}
	const refused = "provider users without an external identity=1"

	// The baseline holds the go-api-prove placeholder; add the admin proof placeholder too.
	prove, admin2 := edgetokenmint.ProvePrincipalID, edgetokenmint.AdminProofPrincipalID
	var baseline int
	if err := admin.QueryRow(ctx, `SELECT count(*) FROM public.users WHERE id = $1 AND auth_provider = 'service' AND password_hash IS NULL AND auth_provider_id IS NULL`, prove).Scan(&baseline); err != nil || baseline != 1 {
		t.Fatalf("the baseline go-api-prove placeholder row is missing (%d, %v)", baseline, err)
	}
	if _, err := admin.Exec(ctx, `INSERT INTO public.users
	(id, email, password_hash, auth_provider, auth_provider_id, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, 'admin-proof@service.dev-health.invalid', NULL, 'service', NULL, true, false, false, 0, now(), now())
ON CONFLICT (id) DO NOTHING`, admin2); err != nil {
		t.Fatalf("admin proof placeholder: %v", err)
	}
	hash, err := bcrypt.GenerateFromPassword([]byte("guard-backfill-local"), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	local, subject := string(hash), "910001"
	user("local", &local, nil)
	user("github", nil, &subject)

	var placeholders, uncarried int
	if err := admin.QueryRow(ctx, `SELECT
  count(*) FILTER (WHERE id IN ($1::uuid, $2::uuid) AND auth_provider = 'service' AND password_hash IS NULL AND auth_provider_id IS NULL),
  count(*) FILTER (WHERE auth_provider IS NOT NULL AND auth_provider NOT IN ('', 'local', 'github', 'gitlab', 'google'))
FROM public.users`, prove, admin2).Scan(&placeholders, &uncarried); err != nil {
		t.Fatal(err)
	}
	if placeholders != 2 || uncarried != placeholders {
		t.Fatalf("start state: %d proof placeholders, %d rows of a provider the backfill cannot carry; want the 2 placeholders only", placeholders, uncarried)
	}
	rep := backfill()
	if rep.IdentityNotCarry != placeholders || rep.IdentityLinked != 1 {
		t.Fatalf("the report must list both proof placeholders as not carried (%d) and link the github identity: %v", placeholders, rep.Lines())
	}
	check("")
	var placeholderLinks int
	if err := admin.QueryRow(ctx, `SELECT count(*) FROM auth.external_identities WHERE principal_id IN ($1::uuid, $2::uuid)`, prove, admin2).Scan(&placeholderLinks); err != nil || placeholderLinks != 0 {
		t.Fatalf("a proof placeholder holds %d identity link(s) after the backfill (%v); want 0", placeholderLinks, err)
	}

	// The same shape under any other id is an ordinary row of a provider without an identity: refused.
	sameShape := user("service", nil, nil)
	if rep := backfill(); rep.IdentityNotCarry != placeholders+1 {
		t.Fatalf("the report must list the same-shaped row as not carried too: %v", rep.Lines())
	}
	check(refused)
	link(sameShape)
	check("")

	saml := user("saml", nil, nil)
	backfill()
	check(refused)
	link(saml)
	check("")

	// A proof principal with a password is not a placeholder: refused until the password is gone again.
	if _, err := admin.Exec(ctx, `UPDATE public.users SET password_hash = $2 WHERE id = $1`, admin2, local); err != nil {
		t.Fatal(err)
	}
	backfill()
	check(refused)
	if _, err := admin.Exec(ctx, `UPDATE public.users SET password_hash = NULL WHERE id = $1`, admin2); err != nil {
		t.Fatal(err)
	}
	backfill()
	check("")

	user("service", &local, nil)
	backfill()
	check(refused)
}
