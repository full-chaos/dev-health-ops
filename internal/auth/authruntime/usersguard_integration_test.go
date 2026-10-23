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

	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
)

// TestUsersOwnedGuardCountsEachGapInLivePostgres runs the guard's own SQL as
// the auth runtime role over the real ops schema and the real auth schema:
// it passes with no gap and names each of the three gaps, one at a time and
// together, with the exact count.
func TestUsersOwnedGuardCountsEachGapInLivePostgres(t *testing.T) {
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
	role, err := containers.RoleName("guard_runtime", instance)
	if err != nil {
		t.Fatal(err)
	}
	const password = "guard-test-only-not-a-credential"
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

	publicUser := func(id uuid.UUID, provider string) {
		t.Helper()
		if _, err := admin.Exec(ctx, `INSERT INTO public.users
	(id, email, password_hash, auth_provider, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, NULL, $3, true, true, false, 0, now(), now())`, id, id.String()+"@example.com", provider); err != nil {
			t.Fatalf("public user: %v", err)
		}
	}
	authUser := func(id uuid.UUID) {
		t.Helper()
		if _, err := admin.Exec(ctx, `INSERT INTO auth.principals (id, kind, display_name) VALUES ($1, 'user', '')`, id); err != nil {
			t.Fatalf("auth principal: %v", err)
		}
		if _, err := admin.Exec(ctx, `INSERT INTO auth.users (principal_id, email, email_lower) VALUES ($1, $2, $2)`, id, id.String()+"@example.com"); err != nil {
			t.Fatalf("auth user: %v", err)
		}
	}
	linker, err := identity.New(identity.Config{Pool: runtime, Schema: schema})
	if err != nil {
		t.Fatal(err)
	}
	identityOf := func(id uuid.UUID) {
		t.Helper()
		key := identity.ExternalKey{Issuer: "https://github.com", Provider: identity.ProviderGitHub, Subject: id.String()}
		if _, err := linker.LinkIdentity(ctx, key, id, identity.Actor{}); err != nil {
			t.Fatalf("identity: %v", err)
		}
	}
	check := func(want ...string) {
		t.Helper()
		err := checkUsersOwned(ctx, runtime, schema)
		if len(want) == 0 {
			if err != nil {
				t.Fatalf("expected no gap, got %v", err)
			}
			return
		}
		if err == nil {
			t.Fatalf("expected %v, got no error", want)
		}
		for _, fragment := range want {
			if !strings.Contains(err.Error(), fragment) {
				t.Fatalf("error %q lacks %q", err, fragment)
			}
		}
	}

	// The ops schema may carry users of its own: mirror each one into the auth
	// schema so the starting state has no gap.
	seeded, err := admin.Query(ctx, `SELECT id, email, coalesce(auth_provider, '') FROM public.users`)
	if err != nil {
		t.Fatal(err)
	}
	type seedRow struct {
		id              uuid.UUID
		email, provider string
	}
	var seeds []seedRow
	for seeded.Next() {
		var r seedRow
		if err := seeded.Scan(&r.id, &r.email, &r.provider); err != nil {
			t.Fatal(err)
		}
		seeds = append(seeds, r)
	}
	seeded.Close()
	for _, r := range seeds {
		if _, err := admin.Exec(ctx, `INSERT INTO auth.principals (id, kind, display_name) VALUES ($1, 'user', '')`, r.id); err != nil {
			t.Fatalf("mirror principal: %v", err)
		}
		if _, err := admin.Exec(ctx, `INSERT INTO auth.users (principal_id, email, email_lower) VALUES ($1, $2, lower($2))`, r.id, r.email); err != nil {
			t.Fatalf("mirror user: %v", err)
		}
		if r.provider != "" && r.provider != "local" {
			identityOf(r.id)
		}
	}
	check()

	both := uuid.New()
	publicUser(both, "local")
	authUser(both)
	check()

	publicOnly := uuid.New()
	publicUser(publicOnly, "local")
	check("public users without an auth user=1", "auth users without a public user=0", "external identity=0")
	authUser(publicOnly)
	check()

	authOnly := uuid.New()
	authUser(authOnly)
	check("public users without an auth user=0", "auth users without a public user=1", "external identity=0")
	publicUser(authOnly, "local")
	check()

	social := uuid.New()
	publicUser(social, "github")
	authUser(social)
	check("public users without an auth user=0", "auth users without a public user=0", "external identity=1")
	identityOf(social)
	check()

	// All three at once, each counted once.
	publicUser(uuid.New(), "local")
	authUser(uuid.New())
	noIdentity := uuid.New()
	publicUser(noIdentity, "saml")
	authUser(noIdentity)
	check("public users without an auth user=1", "auth users without a public user=1", "external identity=1")
}
