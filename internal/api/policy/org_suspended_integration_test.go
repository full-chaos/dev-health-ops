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

// A member of an organization whose public.organizations row is not active is
// refused by the legacy guard, as the PDP refuses it: the organization's
// suspend keeps the members' public rows, so the row alone must not admit a
// caller whose token (a legacy token has no auth session row) still verifies.
func TestLegacyGuardRefusesAMemberOfASuspendedOrganization(t *testing.T) {
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
	pgseed.Membership(ctx, t, pool, uuid.NewString(), user.String(), org.String(), "admin")

	// The auth session read is faked (pgAuthenticator): the subject is the organization row, not session liveness.
	auth := pgAuthenticator(t, PGStore{Pool: pool})
	scope := NewScope(auth, quiet())
	scope.DeclaredRows(func(string) (BearerRow, bool) { return BearerRow{AccessToken: true}, true })
	chain := func(level Authz) http.Handler {
		return auth.ReadOnce(scope.Principal(scope.OrgScope(scope.Impersonation(wrapLevel(NewGuard(auth, quiet()), level, echo)))))
	}
	token := "Bearer " + sign(t, claims(func(c jwt.MapClaims) {
		c["sub"], c["org_id"], c["role"], c["tv"] = user.String(), org.String(), "admin", 0
	}))
	call := func(level Authz) response {
		return serve(chain(level), http.MethodGet, "/x", map[string]string{"Authorization": token})
	}
	denied := `{"detail": "X-Org-Id not permitted for this user"}`
	setActive := func(active bool) {
		t.Helper()
		if _, err := pool.Exec(ctx, `UPDATE organizations SET is_active = $2 WHERE id = $1::uuid`, org, active); err != nil {
			t.Fatal(err)
		}
	}

	for _, level := range []Authz{Authenticated, Admin, AdminOrg} {
		if got := call(level); got.status != 200 || !strings.HasPrefix(got.body, "org="+org.String()+" ") {
			t.Fatalf("active org, level %v: %d %q, want 200 in the token's org", level, got.status, got.body)
		}
	}
	setActive(false)
	t.Run("suspended org: the legacy answer", func(t *testing.T) {
		for _, level := range []Authz{Authenticated, Admin, AdminOrg} {
			if got := call(level); got.status != http.StatusForbidden || got.body != denied {
				t.Fatalf("suspended org, level %v: %d %q, want the X-Org-Id refusal", level, got.status, got.body)
			}
		}
	})
	role, member, orgFound, err := (PGStore{Pool: pool}).OrgMembership(ctx, user, org)
	if err != nil || role != "" || member || !orgFound {
		t.Fatalf("OrgMembership of a suspended org = %q %v %v %v, want no role, not a member, the org found", role, member, orgFound, err)
	}
	setActive(true)
	if got := call(AdminOrg); got.status != 200 {
		t.Fatalf("reactivated org: %d %q, want 200", got.status, got.body)
	}
}
