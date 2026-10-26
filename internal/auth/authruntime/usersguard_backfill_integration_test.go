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

// TestUsersOwnedGuardAndBackfillAgreeOnAServicePrincipalInLivePostgres runs the real users backfill and then the
// guard, both as the auth runtime role: local + github + the service placeholder is ready and the backfill report
// still lists the placeholder as not carried; a saml user without an identity is refused; a service row with a
// password hash is refused.
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

	user := func(provider string, hash, subject *string) {
		t.Helper()
		id := uuid.New()
		if _, err := admin.Exec(ctx, `INSERT INTO public.users
	(id, email, password_hash, auth_provider, auth_provider_id, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, true, true, false, 0, now(), now())`, id, id.String()+"@example.com", hash, provider, subject); err != nil {
			t.Fatalf("public user %s: %v", provider, err)
		}
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

	hash, err := bcrypt.GenerateFromPassword([]byte("guard-backfill-local"), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	local, subject := string(hash), "910001"
	user("local", &local, nil)
	user("github", nil, &subject)
	user("service", nil, nil)

	var placeholders, uncarried int
	if err := admin.QueryRow(ctx, `SELECT
  count(*) FILTER (WHERE auth_provider = 'service' AND password_hash IS NULL AND auth_provider_id IS NULL),
  count(*) FILTER (WHERE auth_provider IS NOT NULL AND auth_provider NOT IN ('', 'local', 'github', 'gitlab', 'google'))
FROM public.users`).Scan(&placeholders, &uncarried); err != nil {
		t.Fatal(err)
	}
	if placeholders < 1 || uncarried != placeholders {
		t.Fatalf("start state: %d service placeholders, %d rows of a provider the backfill cannot carry; want only placeholders", placeholders, uncarried)
	}
	rep := backfill()
	if rep.IdentityNotCarry != placeholders || rep.IdentityLinked != 1 {
		t.Fatalf("the report must list every service placeholder as not carried (%d) and link the github identity: %v", placeholders, rep.Lines())
	}
	check("")

	user("saml", nil, nil)
	backfill()
	check("provider users without an external identity=1")
	var samlID uuid.UUID
	if err := admin.QueryRow(ctx, `SELECT id FROM public.users WHERE auth_provider = 'saml'`).Scan(&samlID); err != nil {
		t.Fatal(err)
	}
	key := identity.ExternalKey{Issuer: "https://github.com", Provider: identity.ProviderGitHub, Subject: samlID.String()}
	if _, err := store.LinkIdentity(ctx, key, samlID, identity.Actor{}); err != nil {
		t.Fatalf("identity: %v", err)
	}
	check("")

	user("service", &local, nil)
	backfill()
	check("provider users without an external identity=1")
}
