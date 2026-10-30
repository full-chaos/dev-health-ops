//go:build integration

package policy

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
)

// On the real public schema, the token's own org is served only while
// public.memberships holds the (user, org) row, and an admin level only while
// that row's role is owner or admin: a demotion and a removal take effect on
// the next request with the same access token, and a re-added member is
// served again. The read is one per request behind ReadOnce, and it is served
// by an index on user_id.
func TestTokenOrgMembershipIsTheLivePublicRow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	instance := pgschema.Start(ctx, t)
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)

	user, org := uuid.New(), uuid.New()
	pgseed.User(ctx, t, pool, user.String())
	pgseed.Org(ctx, t, pool, org.String(), "free")
	membership := uuid.NewString()
	pgseed.Membership(ctx, t, pool, membership, user.String(), org.String(), "admin")

	auth := authenticator(t, PGStore{Pool: pool})
	scope := NewScope(auth, quiet())
	// go-api's scope declares rows; every route here is an org-scoped one.
	scope.DeclaredRows(func(string) (BearerRow, bool) { return BearerRow{AccessToken: true}, true })
	chain := func(level Authz) http.Handler {
		return auth.ReadOnce(scope.Principal(scope.OrgScope(scope.Impersonation(NewGuard(auth, quiet()).Wrap(level, echo)))))
	}
	token := "Bearer " + sign(t, claims(func(c jwt.MapClaims) {
		c["sub"], c["org_id"], c["role"], c["tv"] = user.String(), org.String(), "admin", 0
	}))
	call := func(level Authz) response {
		return serve(chain(level), http.MethodGet, "/x", map[string]string{"Authorization": token})
	}
	callWithHeader := func(level Authz) response {
		return serve(chain(level), http.MethodGet, "/x", map[string]string{"Authorization": token, "X-Org-Id": org.String()})
	}
	denied := `{"detail": "X-Org-Id not permitted for this user"}`

	if got := call(AdminOrg); got.status != 200 || !strings.HasPrefix(got.body, "org="+org.String()+" ") {
		t.Fatalf("admin member: %d %q, want 200 in the token's org", got.status, got.body)
	}
	if _, err := pool.Exec(ctx, `UPDATE memberships SET role = 'member' WHERE id = $1::uuid`, membership); err != nil {
		t.Fatal(err)
	}
	if got := call(AdminOrg); got.status != http.StatusForbidden || got.body != `{"detail":"Admin access required"}` {
		t.Fatalf("demoted to member, admin route: %d %q, want 403 Admin access required", got.status, got.body)
	}
	if got := call(Authenticated); got.status != 200 {
		t.Fatalf("demoted to member, member route: %d %q, want 200", got.status, got.body)
	}
	if _, err := pool.Exec(ctx, `DELETE FROM memberships WHERE id = $1::uuid`, membership); err != nil {
		t.Fatal(err)
	}
	for _, level := range []Authz{Authenticated, Admin, AdminOrg} {
		if got := call(level); got.status != http.StatusForbidden || got.body != denied {
			t.Fatalf("removed, level %v: %d %q, want the X-Org-Id refusal", level, got.status, got.body)
		}
		if got := callWithHeader(level); got.status != http.StatusForbidden || got.body != denied {
			t.Fatalf("removed, X-Org-Id = the token's org, level %v: %d %q, want the X-Org-Id refusal", level, got.status, got.body)
		}
	}
	pgseed.Membership(ctx, t, pool, uuid.NewString(), user.String(), org.String(), "owner")
	if got := call(AdminOrg); got.status != 200 {
		t.Fatalf("re-added as owner: %d %q, want 200", got.status, got.body)
	}

	// At a realistic shape (each user in 1 to 3 of 20 orgs, and one org of
	// 50000 members) the read is an index read on user_id, bounded by the
	// user's own memberships: no plan filters user_id out of an org-wide scan.
	if _, err := pool.Exec(ctx, `
WITH o AS (INSERT INTO organizations (id, slug, name, tier, is_active, created_at, updated_at)
           SELECT gen_random_uuid(), 'o-' || g, 'o ' || g, 'free', true, now(), now() FROM generate_series(1, 20) g RETURNING id),
     oa AS (SELECT array_agg(id ORDER BY id) AS a FROM o),
     u AS (INSERT INTO users (id, email, created_at, updated_at)
           SELECT gen_random_uuid(), gen_random_uuid()::text || '@example.test', now(), now() FROM generate_series(1, 2000) RETURNING id),
     un AS (SELECT id, row_number() OVER (ORDER BY id) AS n FROM u)
INSERT INTO memberships (id, user_id, org_id, role, created_at)
SELECT gen_random_uuid(), un.id, oa.a[((un.n + k) % 20)::int + 1], 'member', now()
FROM un, oa, unnest(ARRAY[0, 7, 13]) AS k
WHERE k = 0 OR (k = 7 AND un.n % 3 >= 1) OR (k = 13 AND un.n % 3 = 2)`); err != nil {
		t.Fatal(err)
	}
	big := uuid.New()
	pgseed.Org(ctx, t, pool, big.String(), "free")
	if _, err := pool.Exec(ctx, `
WITH u AS (INSERT INTO users (id, email, created_at, updated_at)
           SELECT gen_random_uuid(), gen_random_uuid()::text || '@example.test', now(), now() FROM generate_series(1, 50000) RETURNING id)
INSERT INTO memberships (id, user_id, org_id, role, created_at)
SELECT gen_random_uuid(), u.id, $1::uuid, 'member', now() FROM u`, big); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `ANALYZE memberships`); err != nil {
		t.Fatal(err)
	}
	var bigMember uuid.UUID
	if err := pool.QueryRow(ctx, `SELECT user_id FROM memberships WHERE org_id = $1 LIMIT 1`, big).Scan(&bigMember); err != nil {
		t.Fatal(err)
	}
	for _, read := range []struct {
		name      string
		user, org uuid.UUID
	}{{"the token's member", user, org}, {"a member of the 50000-member org", bigMember, big}} {
		rows, err := pool.Query(ctx, "EXPLAIN "+membershipSQL, read.user, read.org)
		if err != nil {
			t.Fatal(err)
		}
		var plan []string
		for rows.Next() {
			var line string
			if err := rows.Scan(&line); err != nil {
				t.Fatal(err)
			}
			plan = append(plan, line)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		text := strings.Join(plan, "\n")
		t.Logf("membership read plan, %s:\n%s", read.name, text)
		userIndexed := false
		for _, line := range plan {
			line = strings.TrimSpace(line)
			if strings.HasPrefix(line, "Index Cond:") && strings.Contains(line, "user_id") {
				userIndexed = true
			}
			if strings.HasPrefix(line, "Filter:") && strings.Contains(line, "user_id") {
				t.Fatalf("%s: the membership read filters user_id out of a wider scan:\n%s", read.name, text)
			}
		}
		if !userIndexed {
			t.Fatalf("%s: the membership read has no index condition on user_id:\n%s", read.name, text)
		}
	}
}
