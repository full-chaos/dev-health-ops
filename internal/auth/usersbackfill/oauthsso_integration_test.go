//go:build integration

package usersbackfill

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

func (e *reconEnv) oauthOrg(t *testing.T, ctx context.Context) uuid.UUID {
	t.Helper()
	id := uuid.New()
	if _, err := e.admin.Exec(ctx, `INSERT INTO public.organizations (id, slug, name, tier, managed_by, is_active, settings, created_at, updated_at)
VALUES ($1, $2, $2, 'enterprise', 'stripe', true, '{}', now(), now())`, id, "org-"+id.String()[:8]); err != nil {
		t.Fatal(err)
	}
	return id
}

func (e *reconEnv) oauthRow(t *testing.T, ctx context.Context, org uuid.UUID, protocol, config string) uuid.UUID {
	t.Helper()
	id := uuid.New()
	if _, err := e.admin.Exec(ctx, `INSERT INTO public.sso_providers (id, org_id, name, protocol, status, config, created_at, updated_at)
VALUES ($1, $2, $3, $4, 'active', $5::json, now(), now())`, id, org, "p-"+id.String()[:8], protocol, config); err != nil {
		t.Fatal(err)
	}
	return id
}

func (e *reconEnv) oauthUser(t *testing.T, ctx context.Context, org uuid.UUID, provider, subject string) uuid.UUID {
	t.Helper()
	id := uuid.New()
	if _, err := e.admin.Exec(ctx, `INSERT INTO public.users (id, email, auth_provider, auth_provider_id, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, $3, $4, true, true, false, 0, now(), now())`, id, id.String()+"@example.test", provider, subject); err != nil {
		t.Fatal(err)
	}
	if _, err := e.admin.Exec(ctx, `INSERT INTO public.memberships (id, user_id, org_id, role) VALUES ($1, $2, $3, 'member')`, uuid.New(), id, org); err != nil {
		t.Fatal(err)
	}
	return id
}

func (e *reconEnv) identityCount(t *testing.T, ctx context.Context, principal uuid.UUID, issuer, provider string) int {
	t.Helper()
	var n int
	if err := e.admin.QueryRow(ctx, `SELECT count(*) FROM auth.external_identities WHERE principal_id = $1 AND issuer = $2 AND provider = $3`,
		principal, issuer, provider).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestBackfillOAuthSSOThenOwnedLoginHitsKey(t *testing.T) {
	e := getReconEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	var role string
	if err := e.runtime.QueryRow(ctx, `SELECT current_user`).Scan(&role); err != nil {
		t.Fatal(err)
	}
	for _, g := range []string{
		`GRANT SELECT (user_id, org_id) ON public.memberships TO %q`,
		`GRANT SELECT (id, org_id, protocol, status, config) ON public.sso_providers TO %q`,
	} {
		if _, err := e.admin.Exec(ctx, fmt.Sprintf(g, role)); err != nil {
			t.Fatal(err)
		}
	}
	base := e.settleOAuthSSO(t, ctx)
	org := e.oauthOrg(t, ctx)
	rowA := e.oauthRow(t, ctx, org, "oauth_github", `{}`)
	rowB := e.oauthRow(t, ctx, org, "oauth_github", `{}`)
	gh := e.oauthUser(t, ctx, org, "github", "777")

	shOrg := e.oauthOrg(t, ctx)
	shRow := e.oauthRow(t, ctx, shOrg, "oauth_gitlab", `{"base_url":"https://git.corp.example"}`)
	sh := e.oauthUser(t, ctx, shOrg, "gitlab", "123")

	e.backfill(t, ctx)
	rep, err := RunOAuthSSO(ctx, PGOAuthSSOSource{Pool: e.runtime}, e.store, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Users != int(base.Total)+2 || rep.Linked != 2 || rep.SelfHostedUnbindable != 1+int(base.SelfHostedUnbindable) || len(rep.Skipped) != 0 {
		t.Fatalf("report %+v", rep)
	}

	owned, err := identity.New(identity.Config{Pool: e.runtime, Schema: e.schema, UsersOwnedByAuth: true})
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range []uuid.UUID{rowA, rowB} {
		p, found, err := owned.IdentityBySubject(ctx, "https://github.com", identity.Provider(row.String()), "777", nil)
		if err != nil || !found || p.ID != gh {
			t.Fatalf("owned login at row %s: found=%v id=%v err=%v", row, found, p.ID, err)
		}
	}
	if n := e.identityCount(t, ctx, gh, "https://github.com", "github"); n != 1 {
		t.Fatalf("social key kept = %d", n)
	}
	if n := e.identityCount(t, ctx, sh, "https://gitlab.com", "gitlab"); n != 1 {
		t.Fatalf("self-hosted user's social key kept = %d", n)
	}
	var selfHostedKeys int
	if err := e.admin.QueryRow(ctx, `SELECT count(*) FROM auth.external_identities WHERE provider = $1`, shRow.String()).Scan(&selfHostedKeys); err != nil || selfHostedKeys != 0 {
		t.Fatalf("self-hosted row keys = %d (%v)", selfHostedKeys, err)
	}

	count, err := identity.ReadOAuthSSOCount(ctx, e.runtime, authschemaQuote(e))
	if err != nil {
		t.Fatal(err)
	}
	if count != (identity.OAuthSSOCount{Total: base.Total + 2, Keyed: base.Keyed + 1, Missing: 0, SelfHostedUnbindable: base.SelfHostedUnbindable + 1}) {
		t.Fatalf("count line %q", count.Line())
	}
	again, err := RunOAuthSSO(ctx, PGOAuthSSOSource{Pool: e.runtime}, e.store, Options{})
	if err != nil || again.Linked != 0 {
		t.Fatalf("rerun linked %d (%v)", again.Linked, err)
	}
}

func authschemaQuote(e *reconEnv) string { return authschema.Quote(e.schema) }

func (e *reconEnv) grantOAuthSSOReads(t *testing.T, ctx context.Context) {
	t.Helper()
	var role string
	if err := e.runtime.QueryRow(ctx, `SELECT current_user`).Scan(&role); err != nil {
		t.Fatal(err)
	}
	for _, g := range []string{
		`GRANT SELECT (user_id, org_id) ON public.memberships TO %q`,
		`GRANT SELECT (id, org_id, protocol, status, config) ON public.sso_providers TO %q`,
	} {
		if _, err := e.admin.Exec(ctx, fmt.Sprintf(g, role)); err != nil {
			t.Fatal(err)
		}
	}
}

// settleOAuthSSO carries everything pending, so a test counts only its own users.
func (e *reconEnv) settleOAuthSSO(t *testing.T, ctx context.Context) identity.OAuthSSOCount {
	t.Helper()
	e.grantOAuthSSOReads(t, ctx)
	e.backfill(t, ctx)
	if _, err := RunOAuthSSO(ctx, PGOAuthSSOSource{Pool: e.runtime}, e.store, Options{}); err != nil {
		t.Fatal(err)
	}
	count, err := identity.ReadOAuthSSOCount(ctx, e.runtime, authschemaQuote(e))
	if err != nil {
		t.Fatal(err)
	}
	return count
}

func (e *reconEnv) verbPool(t *testing.T, ctx context.Context) {
	t.Helper()
	old := connectFn
	connectFn = func(context.Context, cli.Env, string) (*pgxpool.Pool, authschema.ValidatedIdentifier, int) {
		pool, err := pgxpool.NewWithConfig(ctx, e.runtime.Config().Copy())
		if err != nil {
			t.Fatal(err)
		}
		return pool, e.schema, cli.ExitOK
	}
	t.Cleanup(func() { connectFn = old })
}

func TestVerbUsersBackfillCarriesOAuthSSOAndCountVerbPrintsOneLine(t *testing.T) {
	e := getReconEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	base := e.settleOAuthSSO(t, ctx)
	e.verbPool(t, ctx)

	org := e.oauthOrg(t, ctx)
	e.oauthRow(t, ctx, org, "oauth_github", `{}`)
	e.oauthRow(t, ctx, org, "oauth_gitlab", `{"base_url":"https://gitlab.com/"}`)
	e.oauthUser(t, ctx, org, "github", "8101")
	e.oauthUser(t, ctx, org, "gitlab", "8102")
	for i, odd := range []string{"https://GitLab.com/", "https://gitlab.com:443"} {
		o := e.oauthOrg(t, ctx)
		e.oauthRow(t, ctx, o, "oauth_gitlab", fmt.Sprintf(`{"base_url":%q}`, odd))
		e.oauthUser(t, ctx, o, "gitlab", fmt.Sprintf("820%d", i))
	}

	countOut := func() string {
		var out bytes.Buffer
		code := oauthSSOCountVerb(t).Run(ctx, cli.Env{Lookup: func(string) (string, bool) { return "", false }, Stdout: &out, Stderr: &bytes.Buffer{}})
		if code != cli.ExitOK {
			t.Fatalf("count verb exit %d", code)
		}
		return out.String()
	}
	before := countOut()
	if want := (identity.OAuthSSOCount{Total: base.Total + 4, Keyed: base.Keyed, Missing: 2, SelfHostedUnbindable: base.SelfHostedUnbindable + 2}).Line() + "\n"; before != want {
		t.Fatalf("before the verb: %q, want %q", before, want)
	}

	var stdout bytes.Buffer
	code := Command().Children[0].Run(ctx, cli.Env{Lookup: func(string) (string, bool) { return "", false }, Stdout: &stdout, Stderr: &bytes.Buffer{}})
	if code != cli.ExitOK {
		t.Fatalf("users-backfill exit %d\n%s", code, stdout.String())
	}
	if !strings.Contains(stdout.String(), "oauth_sso_keys_linked=2\n") {
		t.Fatalf("stdout %q", stdout.String())
	}
	after := countOut()
	if want := (identity.OAuthSSOCount{Total: base.Total + 4, Keyed: base.Keyed + 2, Missing: 0, SelfHostedUnbindable: base.SelfHostedUnbindable + 2}).Line() + "\n"; after != want {
		t.Fatalf("after the verb: %q, want %q", after, want)
	}
	if strings.ContainsAny(after, "@") || strings.Count(after, "\n") != 1 {
		t.Fatalf("the count line is not one line of counts: %q", after)
	}
}
