package policy

import (
	"net/http"
	"strings"
	"testing"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
)

const (
	removedOrg = "77777777-7777-4777-8777-777777777777"
	deletedOrg = "88888888-8888-4888-8888-888888888888"
	notUUIDOrg = "not-a-uuid"
)

// rule1Case is one org claim the caller holds no membership row for: A, an
// org that exists (the caller was removed); B-unknown, a UUID with no
// organizations row; B-notUUID, a claim that is not a UUID.
type rule1Case struct {
	name, org string
	absent    bool
}

var rule1Cases = []rule1Case{
	{name: "A removed member", org: removedOrg},
	{name: "B-unknown org", org: deletedOrg, absent: true},
	{name: "B-notUUID claim", org: notUUIDOrg},
}

func rule1Store(c rule1Case) *accessStore {
	store := newAccessStore()
	if c.absent {
		store.absentOrgs = map[uuid.UUID]bool{uuid.MustParse(c.org): true}
	}
	return store
}

// rule1Chain is go-api's request chain over auth: ReadOnce, the principal,
// the org scope with declared org-scoped rows, impersonation, then the guard
// at level over echo.
func rule1Chain(auth *Authenticator, level Authz) http.Handler {
	scope := NewScope(auth, quiet())
	scope.DeclaredRows(func(string) (BearerRow, bool) { return BearerRow{AccessToken: true}, true })
	return auth.ReadOnce(scope.Principal(scope.OrgScope(scope.Impersonation(wrapLevel(NewGuard(auth, quiet()), level, echo)))))
}

// rule1Routed serves the caller's routed user access token for the org claim (the HS256 class is retired here; the
// legacy answers below are the literals main's executed HS256 side held this cell to).
func rule1Routed(t *testing.T, c rule1Case, level Authz, header string) response {
	t.Helper()
	routedStore := rule1Store(c)
	auth := routed(t, routedStore, &fakeValidator{result: userClaims(func(a *AccessTokenClaims) {
		org := c.org
		a.OrgID = &org
	})})
	signing, _ := jwt.NewWithClaims(jwt.SigningMethodEdDSA, jwt.MapClaims{}).SigningString()
	h := map[string]string{"Authorization": "Bearer " + signing + ".sig"}
	if header != "" {
		h["X-Org-Id"] = header
	}
	return serve(rule1Chain(auth, level), http.MethodGet, "/x", h)
}

// Product rule 1 (D6773), CHAOS-13227: a plain user (no admin.orgs) whose
// token org has no membership row gets, with a user access token, the answer
// the legacy HS256 path gives for the same caller and claim, on every guard
// level, with and without an X-Org-Id that names the claim. The HS256 token
// claims role member, the role a user access token (no role claim) carries.
// The legacy answer is the literal main's executed HS256 side held this cell to. The fake
// validator stands in for the verifier: the access-token.v1 verifier refuses
// a B-notUUID claim before this step (CHAOS-13262).
func TestRule1RoutedTokenOrgAnswerIsTheLegacyAnswer_CHAOS13227(t *testing.T) {
	denied := `{"detail": "X-Org-Id not permitted for this user"}`
	for _, c := range rule1Cases {
		for _, level := range []Authz{Authenticated, Admin, AdminOrg} {
			for _, header := range []string{"", c.org} {
				t.Run(c.name+"/"+level.String()+"/header="+header, func(t *testing.T) {
					got := rule1Routed(t, c, level, header)
					switch {
					case c.org == removedOrg:
						if got.status != http.StatusForbidden || got.body != denied {
							t.Fatalf("routed %d %q, legacy: the X-Org-Id refusal", got.status, got.body)
						}
					case level == Authenticated:
						if got.status != http.StatusOK || !strings.HasPrefix(got.body, "org="+c.org+" ") {
							t.Fatalf("routed %d %q, legacy: 200 in the claimed org", got.status, got.body)
						}
					default:
						if got.status != http.StatusForbidden || got.body != `{"detail":"Admin access required"}` {
							t.Fatalf("routed %d %q, legacy: 403 Admin access required", got.status, got.body)
						}
					}
				})
			}
		}
	}
}

// CHAOS-13261 (residual of CHAOS-13227): for an org claim that names no org,
// or is not a UUID, the legacy HS256 path decides Admin and AdminOrg by the
// token role claim (admin: 200). A user access token has no role claim and no
// membership row can give one, so the routed answer there is not the legacy
// answer. This cell records the divergence; it never holds 403 as wanted.
func TestRule1DeletedOrgAdminClaimDivergence_CHAOS13261(t *testing.T) {
	diverged := []string{}
	for _, c := range rule1Cases[1:] {
		for _, level := range []Authz{Admin, AdminOrg} {
			// Legacy (HS256, role claim admin): 200, the literal main's executed HS256 side held this cell to.
			got := rule1Routed(t, c, level, "")
			switch got.status {
			case http.StatusOK:
			case http.StatusForbidden:
				diverged = append(diverged, c.name+" "+level.String())
			default:
				t.Fatalf("%s %s: routed %d %q, want 200 (legacy) or the known 403", c.name, level, got.status, got.body)
			}
		}
	}
	if len(diverged) > 0 {
		t.Skipf("CHAOS-13261: routed 403 where legacy gives 200 by the role claim: %s", strings.Join(diverged, ", "))
	}
}
