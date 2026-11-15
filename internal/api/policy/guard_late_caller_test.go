package policy

import (
	"net/http"
	"testing"
)

// lateCallerHandler serves the billing row behind WrapLate with a late check that records the caller it sees and lets the
// request go on.
func lateCallerHandler(t *testing.T, enf *Enforcer, seen *[]*User) http.Handler {
	t.Helper()
	guard := NewGuard(routed(t, personaStore(personaAdmin), &fakeValidator{result: userClaims(nil)}), quiet())
	if enf != nil {
		guard.SetEnforcer(enf)
	}
	late := func(_ http.ResponseWriter, r *http.Request) bool {
		*seen = append(*seen, UserFrom(r.Context()))
		return true
	}
	mux := http.NewServeMux()
	mux.Handle("GET /api/v1/billing/invoices", guard.WrapLate(Authenticated, late, echo))
	return mux
}

// A route's late check runs before an enforced refusal, so it must see the authenticated caller there as on the allowed and
// legacy paths: a check that reads the caller's organization would otherwise judge an empty caller.
func TestTheLateCheckSeesTheCallerOnEveryPath(t *testing.T) {
	for name, enf := range map[string]func(t *testing.T) *Enforcer{
		"pdp deny": func(t *testing.T) *Enforcer {
			return newEnforcerFor(t, newTestPDP(t, denyWith("role_lacks_action")).client(t), onFor("billing"))
		},
		"pdp allow": func(t *testing.T) *Enforcer {
			return newEnforcerFor(t, newTestPDP(t, allowAll).client(t), onFor("billing"))
		},
		"no cell": func(*testing.T) *Enforcer { return nil },
	} {
		t.Run(name, func(t *testing.T) {
			var seen []*User
			got := enforceRequest(lateCallerHandler(t, enf(t), &seen), "GET", "/api/v1/billing/invoices")
			want := http.StatusOK
			if name == "pdp deny" {
				want = http.StatusForbidden
			}
			if got.status != want {
				t.Fatalf("%d %q, want %d", got.status, got.body, want)
			}
			if len(seen) != 1 || seen[0] == nil || seen[0].OrgID != ownOrg {
				t.Fatalf("late check saw %v, want the caller of org %s once", seen, ownOrg)
			}
		})
	}
}
