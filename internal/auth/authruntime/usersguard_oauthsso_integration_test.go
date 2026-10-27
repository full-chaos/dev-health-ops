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
	"github.com/full-chaos/dev-health-ops/internal/auth/usersbackfill"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
)

// TestUsersOwnedGuardCountsOAuthSSOAsItsOwnClass: an OAuth-SSO user with a carried social key is not ready until the
// row key is carried, and a self-hosted row's user holds the guard until the row is gone.
func TestUsersOwnedGuardCountsOAuthSSOAsItsOwnClass(t *testing.T) {
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
	role, err := containers.RoleName("guard_oauthsso", instance)
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
	if _, err := admin.Exec(ctx, `INSERT INTO public.organizations (id, slug, name) VALUES ($1, $2, 'guard-oauthsso')`, org, "org-"+org.String()); err != nil {
		t.Fatal(err)
	}
	parity := func() {
		t.Helper()
		c, err := identity.ReadOAuthSSOCount(ctx, runtime, authschema.Quote(schema))
		if err != nil {
			t.Fatal(err)
		}
		want := fmt.Sprintf("oauth_sso_missing_row_key=%d, oauth_sso_ambiguous_row=%d, oauth_sso_selfhosted_unbindable=%d", c.Missing, c.Ambiguous, c.SelfHostedUnbindable)
		if err := checkUsersOwned(ctx, runtime, schema); err == nil || !strings.Contains(err.Error(), want) {
			t.Fatalf("the guard and the count line disagree: want %q in %v (line %q)", want, err, c.Line())
		}
	}
	row := func(protocol, config string) uuid.UUID {
		t.Helper()
		id := uuid.New()
		if _, err := admin.Exec(ctx, `INSERT INTO public.sso_providers (id, org_id, name, protocol, status, config) VALUES ($1, $2, $3, $4, 'active', $5::json)`,
			id, org, "idp-"+id.String(), protocol, config); err != nil {
			t.Fatalf("provider %s: %v", protocol, err)
		}
		return id
	}
	memberOrg := org
	user := func(provider, subject string) {
		t.Helper()
		id := uuid.New()
		if _, err := admin.Exec(ctx, `INSERT INTO public.users
	(id, email, auth_provider, auth_provider_id, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, $3, $4, true, true, false, 0, now(), now())`, id, id.String()+"@example.com", provider, subject); err != nil {
			t.Fatalf("user: %v", err)
		}
		if _, err := admin.Exec(ctx, `INSERT INTO public.memberships (id, user_id, org_id, role) VALUES ($1, $2, $3, 'member')`, uuid.New(), id, memberOrg); err != nil {
			t.Fatalf("membership: %v", err)
		}
	}
	backfill := func() {
		t.Helper()
		if rep, err := usersbackfill.Run(ctx, usersbackfill.PGSource{Pool: runtime}, store, usersbackfill.Options{}); err != nil || rep.SkippedTotal() != 0 {
			t.Fatalf("users backfill: %v %v", err, rep.Lines())
		}
	}
	carry := func() {
		t.Helper()
		if rep, err := usersbackfill.RunOAuthSSO(ctx, usersbackfill.PGOAuthSSOSource{Pool: runtime}, store, usersbackfill.Options{}); err != nil || len(rep.Skipped) != 0 {
			t.Fatalf("oauth sso carry: %v %+v", err, rep)
		}
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

	plainOrg := uuid.New()
	if _, err := admin.Exec(ctx, `INSERT INTO public.organizations (id, slug, name) VALUES ($1, $2, 'guard-plain')`, plainOrg, "org-"+plainOrg.String()); err != nil {
		t.Fatal(err)
	}
	memberOrg = plainOrg
	user("github", "930000")
	memberOrg = org
	backfill()
	check("plain social user, no provider row", "")
	row("oauth_github", `{}`)
	check("plain social user, row only in another org", "")
	user("github", "930001")
	backfill()
	check("social key carried, so no identity gap", "provider users without an external identity=0 ")
	check("social key carried, row key not (class)", "oauth_sso_missing_row_key=1")
	carry()
	check("row key carried", "")
	second := row("oauth_github", `{}`)
	backfill()
	carry()
	check("a second active row makes the user ambiguous", "oauth_sso_ambiguous_row=1")
	parity()
	if _, err := admin.Exec(ctx, `UPDATE public.sso_providers SET status = 'disabled' WHERE id = $1`, second); err != nil {
		t.Fatal(err)
	}
	check("second row disabled", "")

	hosted := row("oauth_gitlab", `{"base_url":"https://git.corp.example"}`)
	user("gitlab", "930002")
	backfill()
	carry()
	check("self-hosted row's user is never carried", "oauth_sso_selfhosted_unbindable=1")
	if _, err := admin.Exec(ctx, `UPDATE public.sso_providers SET status = 'disabled' WHERE id = $1`, hosted); err != nil {
		t.Fatal(err)
	}
	check("self-hosted row disabled", "")
}
