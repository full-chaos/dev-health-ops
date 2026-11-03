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

// A caller whose token org is active asks for another organization with
// X-Org-Id. While that organization is suspended its members' public rows
// remain, and the plain membership read must not admit them.
func TestLegacyGuardRefusesAnXOrgIdOfASuspendedOrganization(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	instance := pgschema.Start(ctx, t)
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)

	user, home, other := uuid.New(), uuid.New(), uuid.New()
	pgseed.User(ctx, t, pool, user.String())
	pgseed.Org(ctx, t, pool, home.String(), "free")
	pgseed.Org(ctx, t, pool, other.String(), "free")
	pgseed.Membership(ctx, t, pool, uuid.NewString(), user.String(), home.String(), "admin")
	pgseed.Membership(ctx, t, pool, uuid.NewString(), user.String(), other.String(), "admin")

	auth := authenticator(t, PGStore{Pool: pool})
	scope := NewScope(auth, quiet())
	scope.DeclaredRows(func(string) (BearerRow, bool) { return BearerRow{AccessToken: true}, true })
	chain := func(level Authz) http.Handler {
		return auth.ReadOnce(scope.Principal(scope.OrgScope(scope.Impersonation(wrapLevel(NewGuard(auth, quiet()), level, echo)))))
	}
	token := "Bearer " + sign(t, claims(func(c jwt.MapClaims) {
		c["sub"], c["org_id"], c["role"], c["tv"] = user.String(), home.String(), "admin", 0
	}))
	call := func(level Authz) response {
		return serve(chain(level), http.MethodGet, "/x", map[string]string{"Authorization": token, "X-Org-Id": other.String()})
	}
	denied := `{"detail": "X-Org-Id not permitted for this user"}`
	setActive := func(active bool) {
		t.Helper()
		if _, err := pool.Exec(ctx, `UPDATE organizations SET is_active = $2 WHERE id = $1::uuid`, other, active); err != nil {
			t.Fatal(err)
		}
	}

	for _, level := range []Authz{Authenticated, Admin, AdminOrg} {
		if got := call(level); got.status != 200 || !strings.HasPrefix(got.body, "org="+other.String()+" ") {
			t.Fatalf("active X-Org-Id org, level %v: %d %q, want 200 in that org", level, got.status, got.body)
		}
	}
	setActive(false)
	for _, level := range []Authz{Authenticated, Admin, AdminOrg} {
		if got := call(level); got.status != http.StatusForbidden || got.body != denied {
			t.Fatalf("suspended X-Org-Id org, level %v: %d %q, want the X-Org-Id refusal", level, got.status, got.body)
		}
	}
	role, member, err := (PGStore{Pool: pool}).Membership(ctx, user, other)
	if err != nil || role != "" || member {
		t.Fatalf("Membership of a suspended org = %q %v %v, want no role and not a member", role, member, err)
	}
	setActive(true)
	if got := call(AdminOrg); got.status != 200 {
		t.Fatalf("reactivated X-Org-Id org: %d %q, want 200", got.status, got.body)
	}
}
