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
// by the unique (user_id, org_id) key.
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
	chain := func(level Authz) http.Handler {
		return auth.ReadOnce(scope.Principal(scope.OrgScope(scope.Impersonation(NewGuard(auth, quiet()).Wrap(level, echo)))))
	}
	token := "Bearer " + sign(t, claims(func(c jwt.MapClaims) {
		c["sub"], c["org_id"], c["role"], c["tv"] = user.String(), org.String(), "admin", 0
	}))
	call := func(level Authz) response {
		return serve(chain(level), http.MethodGet, "/x", map[string]string{"Authorization": token})
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
	}
	pgseed.Membership(ctx, t, pool, uuid.NewString(), user.String(), org.String(), "owner")
	if got := call(AdminOrg); got.status != 200 {
		t.Fatalf("re-added as owner: %d %q, want 200", got.status, got.body)
	}

	// The read is served by the unique (user_id, org_id) key at a realistic
	// size: one indexed row lookup per request.
	if _, err := pool.Exec(ctx, `
WITH u AS (INSERT INTO users (id, email, created_at, updated_at)
           SELECT gen_random_uuid(), gen_random_uuid()::text || '@example.test', now(), now() FROM generate_series(1, 2000) RETURNING id),
     o AS (INSERT INTO organizations (id, slug, name, tier, is_active, created_at, updated_at)
           SELECT gen_random_uuid(), 'o-' || g, 'o ' || g, 'free', true, now(), now() FROM generate_series(1, 20) g RETURNING id)
INSERT INTO memberships (id, user_id, org_id, role, created_at)
SELECT gen_random_uuid(), u.id, o.id, 'member', now() FROM u CROSS JOIN o`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `ANALYZE memberships`); err != nil {
		t.Fatal(err)
	}
	var plan strings.Builder
	rows, err := pool.Query(ctx, `EXPLAIN SELECT role FROM memberships WHERE user_id = $1 AND org_id = $2 LIMIT 1`, user, org)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatal(err)
		}
		plan.WriteString(line + "\n")
	}
	rows.Close()
	t.Logf("membership read plan:\n%s", plan.String())
	if !strings.Contains(plan.String(), "uq_membership_user_org") {
		t.Fatalf("the membership read is not served by uq_membership_user_org:\n%s", plan.String())
	}
}
