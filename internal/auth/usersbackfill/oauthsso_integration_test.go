//go:build integration

package usersbackfill

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// The package shares one Postgres: every row these tests create is removed when the test ends, so the orgs,
// reconcile and purge tests see only their own rows.
var oauthSSOTracked struct {
	sync.Mutex
	users, orgs, rows []uuid.UUID
}

func (e *reconEnv) oauthSSOCleanup(t *testing.T) {
	t.Helper()
	t.Cleanup(func() {
		oauthSSOTracked.Lock()
		defer oauthSSOTracked.Unlock()
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		// The auth security tables refuse writes outside audit.Commit; replica mode (the test owner) lifts that
		// for this cleanup only.
		tx, err := e.admin.Begin(ctx)
		if err != nil {
			t.Errorf("cleanup begin: %v", err)
			return
		}
		defer func() { _ = tx.Rollback(ctx) }()
		if _, err := tx.Exec(ctx, `SET LOCAL session_replication_role = replica`); err != nil {
			t.Errorf("cleanup role: %v", err)
			return
		}
		for _, q := range []struct {
			sql string
			ids []uuid.UUID
		}{
			{`DELETE FROM auth.external_identities WHERE principal_id = ANY($1)`, oauthSSOTracked.users},
			{`DELETE FROM auth.users WHERE principal_id = ANY($1)`, oauthSSOTracked.users},
			{`DELETE FROM auth.principals WHERE id = ANY($1)`, oauthSSOTracked.users},
			{`DELETE FROM public.memberships WHERE user_id = ANY($1)`, oauthSSOTracked.users},
			{`DELETE FROM public.users WHERE id = ANY($1)`, oauthSSOTracked.users},
			{`DELETE FROM public.sso_providers WHERE id = ANY($1)`, oauthSSOTracked.rows},
			{`DELETE FROM public.memberships WHERE org_id = ANY($1)`, oauthSSOTracked.orgs},
			{`DELETE FROM public.organizations WHERE id = ANY($1)`, oauthSSOTracked.orgs},
		} {
			if _, err := tx.Exec(ctx, q.sql, q.ids); err != nil {
				t.Errorf("cleanup %q: %v", q.sql, err)
				return
			}
		}
		if err := tx.Commit(ctx); err != nil {
			t.Errorf("cleanup commit: %v", err)
		}
		oauthSSOTracked.users, oauthSSOTracked.orgs, oauthSSOTracked.rows = nil, nil, nil
	})
}

func trackOAuthSSO(list *[]uuid.UUID, id uuid.UUID) {
	oauthSSOTracked.Lock()
	defer oauthSSOTracked.Unlock()
	*list = append(*list, id)
}

func (e *reconEnv) oauthOrg(t *testing.T, ctx context.Context) uuid.UUID {
	t.Helper()
	id := uuid.New()
	if _, err := e.admin.Exec(ctx, `INSERT INTO public.organizations (id, slug, name, tier, managed_by, is_active, settings, created_at, updated_at)
VALUES ($1, $2, $2, 'enterprise', 'stripe', true, '{}', now(), now())`, id, "org-"+id.String()[:8]); err != nil {
		t.Fatal(err)
	}
	trackOAuthSSO(&oauthSSOTracked.orgs, id)
	return id
}

func (e *reconEnv) oauthRow(t *testing.T, ctx context.Context, org uuid.UUID, protocol, config string) uuid.UUID {
	t.Helper()
	id := uuid.New()
	if _, err := e.admin.Exec(ctx, `INSERT INTO public.sso_providers (id, org_id, name, protocol, status, config, created_at, updated_at)
VALUES ($1, $2, $3, $4, 'active', $5::json, now(), now())`, id, org, "p-"+id.String()[:8], protocol, config); err != nil {
		t.Fatal(err)
	}
	trackOAuthSSO(&oauthSSOTracked.rows, id)
	return id
}

func (e *reconEnv) oauthUser(t *testing.T, ctx context.Context, org uuid.UUID, provider, subject string) uuid.UUID {
	t.Helper()
	id := uuid.New()
	if _, err := e.admin.Exec(ctx, `INSERT INTO public.users (id, email, auth_provider, auth_provider_id, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, $3, $4, true, true, false, 0, now(), now())`, id, id.String()+"@example.test", provider, subject); err != nil {
		t.Fatal(err)
	}
	trackOAuthSSO(&oauthSSOTracked.users, id)
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
	e.oauthSSOCleanup(t)
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
	gh := e.oauthUser(t, ctx, org, "github", "777")

	shOrg := e.oauthOrg(t, ctx)
	shRow := e.oauthRow(t, ctx, shOrg, "oauth_gitlab", `{"base_url":"https://git.corp.example"}`)
	sh := e.oauthUser(t, ctx, shOrg, "gitlab", "123")

	e.backfill(t, ctx)
	rep, err := RunOAuthSSO(ctx, PGOAuthSSOSource{Pool: e.runtime}, e.store, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Users != int(base.Total)+2 || rep.Linked != 1 || rep.SelfHostedUnbindable != 1+int(base.SelfHostedUnbindable) || len(rep.Skipped) != 0 {
		t.Fatalf("report %+v", rep)
	}

	owned, err := identity.New(identity.Config{Pool: e.runtime, Schema: e.schema, UsersOwnedByAuth: true})
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range []uuid.UUID{rowA} {
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
	if count != (identity.OAuthSSOCount{Total: base.Total + 2, Keyed: base.Keyed + 1, Missing: 0, Ambiguous: base.Ambiguous, SelfHostedUnbindable: base.SelfHostedUnbindable + 1}) {
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
	e.oauthSSOCleanup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	base := e.settleOAuthSSO(t, ctx)
	e.verbPool(t, ctx)

	org := e.oauthOrg(t, ctx)
	e.oauthRow(t, ctx, org, "oauth_github", `{}`)
	e.oauthRow(t, ctx, org, "oauth_gitlab", `{"base_url":"https://gitlab.com/"}`)
	e.oauthUser(t, ctx, org, "github", "8101")
	e.oauthUser(t, ctx, org, "gitlab", "8102")
	for i, odd := range []string{"https://GitLab.com/", "https://gitlab.com:443", "https://gitlab.com//"} {
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
	if want := (identity.OAuthSSOCount{Total: base.Total + 5, Keyed: base.Keyed, Missing: 2, Ambiguous: base.Ambiguous, SelfHostedUnbindable: base.SelfHostedUnbindable + 3}).Line() + "\n"; before != want {
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
	if want := (identity.OAuthSSOCount{Total: base.Total + 5, Keyed: base.Keyed + 2, Missing: 0, Ambiguous: base.Ambiguous, SelfHostedUnbindable: base.SelfHostedUnbindable + 3}).Line() + "\n"; after != want {
		t.Fatalf("after the verb: %q, want %q", after, want)
	}
	if strings.ContainsAny(after, "@") || strings.Count(after, "\n") != 1 {
		t.Fatalf("the count line is not one line of counts: %q", after)
	}
}

func (e *reconEnv) principalCount(t *testing.T, ctx context.Context) int {
	t.Helper()
	var n int
	if err := e.admin.QueryRow(ctx, `SELECT count(*) FROM auth.principals`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func (e *reconEnv) keysAt(t *testing.T, ctx context.Context, principal uuid.UUID) int {
	t.Helper()
	var n int
	if err := e.admin.QueryRow(ctx, `SELECT count(*) FROM auth.external_identities WHERE principal_id = $1`, principal).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// Exactly one candidate row carries; zero rows carries nothing and is not counted (a plain social user, the
// social key is its identity); two rows in one org or one row in each of two orgs carries nothing and is counted
// as ambiguous_row. A social user in a row's org gets the row key and nothing else: no new principal.
func TestBackfillOAuthSSOCarriesOnlyExactlyOneRowAndCountsTheRest(t *testing.T) {
	e := getReconEnv(t)
	e.oauthSSOCleanup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	base := e.settleOAuthSSO(t, ctx)

	oneOrg := e.oauthOrg(t, ctx)
	e.oauthRow(t, ctx, oneOrg, "oauth_github", `{}`)
	one := e.oauthUser(t, ctx, oneOrg, "github", "9101")

	noRowOrg := e.oauthOrg(t, ctx)
	zero := e.oauthUser(t, ctx, noRowOrg, "github", "9102")

	twoOrg := e.oauthOrg(t, ctx)
	e.oauthRow(t, ctx, twoOrg, "oauth_github", `{}`)
	e.oauthRow(t, ctx, twoOrg, "oauth_github", `{}`)
	sameOrg := e.oauthUser(t, ctx, twoOrg, "github", "9103")

	orgA, orgB := e.oauthOrg(t, ctx), e.oauthOrg(t, ctx)
	e.oauthRow(t, ctx, orgA, "oauth_github", `{}`)
	e.oauthRow(t, ctx, orgB, "oauth_github", `{}`)
	across := e.oauthUser(t, ctx, orgA, "github", "9104")
	if _, err := e.admin.Exec(ctx, `INSERT INTO public.memberships (id, user_id, org_id, role) VALUES ($1, $2, $3, 'member')`, uuid.New(), across, orgB); err != nil {
		t.Fatal(err)
	}

	e.backfill(t, ctx)
	principals := e.principalCount(t, ctx)
	rep, err := RunOAuthSSO(ctx, PGOAuthSSOSource{Pool: e.runtime}, e.store, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Linked != 1 || len(rep.Skipped) != 0 || rep.Ambiguous != int(base.Ambiguous)+2 {
		t.Fatalf("report %+v", rep)
	}
	if got := e.principalCount(t, ctx); got != principals {
		t.Fatalf("the carry created principals: %d -> %d", principals, got)
	}
	for name, want := range map[string]struct {
		id   uuid.UUID
		keys int
	}{"one row": {one, 2}, "zero rows": {zero, 1}, "two rows in one org": {sameOrg, 1}, "one row in each of two orgs": {across, 1}} {
		if got := e.keysAt(t, ctx, want.id); got != want.keys {
			t.Fatalf("%s: %d identities, want %d", name, got, want.keys)
		}
	}
	count, err := identity.ReadOAuthSSOCount(ctx, e.runtime, authschemaQuote(e))
	if err != nil {
		t.Fatal(err)
	}
	if count != (identity.OAuthSSOCount{Total: base.Total + 3, Keyed: base.Keyed + 1, Missing: 0, Ambiguous: base.Ambiguous + 2, SelfHostedUnbindable: base.SelfHostedUnbindable}) {
		t.Fatalf("count line %q", count.Line())
	}
	again, err := RunOAuthSSO(ctx, PGOAuthSSOSource{Pool: e.runtime}, e.store, Options{})
	if err != nil || again.Linked != 0 || again.Ambiguous != rep.Ambiguous {
		t.Fatalf("rerun %+v (%v)", again, err)
	}
	if after, _ := identity.ReadOAuthSSOCount(ctx, e.runtime, authschemaQuote(e)); after != count {
		t.Fatalf("rerun changed the count: %q -> %q", count.Line(), after.Line())
	}
}

// A skipped carry (the row key is already linked to another principal) fails the verb with exit 1 and a skip class.
func TestVerbUsersBackfillOAuthSSOSkipFailsWithExitOne(t *testing.T) {
	e := getReconEnv(t)
	e.oauthSSOCleanup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	e.settleOAuthSSO(t, ctx)
	e.verbPool(t, ctx)

	org := e.oauthOrg(t, ctx)
	row := e.oauthRow(t, ctx, org, "oauth_github", `{}`)
	e.oauthUser(t, ctx, org, "github", "9201")
	other := e.seed(t, ctx, uniq("skip"), true, false)
	trackOAuthSSO(&oauthSSOTracked.users, other)
	e.backfill(t, ctx)
	key := identity.ExternalKey{Issuer: "https://github.com", Provider: identity.Provider(row.String()), Subject: "9201"}
	if _, err := e.store.CarryExtraIdentity(ctx, other, key, false); err != nil {
		t.Fatal(err)
	}

	var stdout bytes.Buffer
	code := Command().Children[0].Run(ctx, cli.Env{Lookup: func(string) (string, bool) { return "", false }, Stdout: &stdout, Stderr: &bytes.Buffer{}})
	if code != cli.ExitFailure || !strings.Contains(stdout.String(), "skipped.identity_conflict=1\n") {
		t.Fatalf("exit %d, stdout %q", code, stdout.String())
	}
}
