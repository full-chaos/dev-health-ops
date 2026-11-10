package server

import (
	"errors"
	"net/http"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole/rule"
)

// go-api's Scope.Impersonation applies a live impersonation row to a holder of admin.impersonate only
// (internal/api/policy/scope.go). /query's live scope must decide the same way: by the action, never by the
// superuser flag. A caller without the action is served as itself, its row unread; a holder is served as the
// target, superuser or not.

// impersonationQueryRoutes are the mounts of the /query pipeline (newDocumentDispatchHandler), on each listener
// that serves them.
var impersonationQueryRoutes = []struct{ listener, path string }{
	{"query", "/query"},
	{"internal", "/query"},
	{"query", "/query/proof"},
	{"internal", "/query/proof-write"},
	{"internal", "/query/run-operation"},
}

func impersonationRow() *policy.Impersonation {
	return &policy.Impersonation{AdminUserID: ecUser, TargetUserID: ecTarget, TargetOrgID: irTargetOrg, TargetRole: "viewer"}
}

// platformWithout is every platform action but the one named.
func platformWithout(action string) *rule.Platform {
	var held []string
	for _, known := range rule.PlatformActions {
		if known != action {
			held = append(held, known)
		}
	}
	platform := rule.PlatformOf(held...)
	return &platform
}

// impersonatorStore: ecUser (a member of ecOrg) is in state, with row as its live go-api row (nil: none) and
// rowErr as the row read's failure.
func impersonatorStore(state policy.UserState, row *policy.Impersonation, rowErr error) *accessStore {
	store := newAccessStore()
	store.states[ecUser] = state
	store.sessions = map[uuid.UUID]*policy.Impersonation{}
	if row != nil {
		store.sessions[ecUser] = row
	}
	store.errSession = rowErr
	return store
}

// revokedState is a live superuser whose platform set no longer holds admin.impersonate.
func revokedState() policy.UserState {
	return policy.UserState{IsActive: true, IsSuperuser: true, Platform: platformWithout(rule.ActionAdminImpersonate), TokenVersion: 5}
}

// nonSuperuserHolderState is a live non-superuser whose platform set holds admin.impersonate.
func nonSuperuserHolderState() policy.UserState {
	platform := rule.PlatformOf(rule.ActionAdminImpersonate)
	return policy.UserState{IsActive: true, IsSuperuser: false, Platform: &platform, TokenVersion: 5}
}

// flagSuperuserState is a live superuser that carries only the flag (Platform nil): the users read of every
// production store.
func flagSuperuserState() policy.UserState {
	return policy.UserState{IsActive: true, IsSuperuser: true, TokenVersion: 5}
}

func impersonationStack(t *testing.T, minter *accessMinter, store *accessStore) *behaviourStack {
	t.Helper()
	return newAccessStack(t, store, testAccess(minter.keys, nil, accessTestNow))
}

func postQuery(s *behaviourStack, listener, path, bearer string) liveAnswer {
	return s.live(liveReq{listener: listener, method: http.MethodPost, path: path, bearer: bearer, body: documentBody(iaDocument)})
}

// TestQueryServesTheRevokedImpersonatorAsItself: on each /query mount a superuser without admin.impersonate,
// beside a live row, is served in its own org with no impersonation; its row is not read (a failed read changes
// nothing).
func TestQueryServesTheRevokedImpersonatorAsItself(t *testing.T) {
	quietStdLog(t)
	minter := newAccessMinter(t)
	token := minter.mint(ecUser, nil, nil)
	stores := map[string]func() *accessStore{
		"live row": func() *accessStore { return impersonatorStore(revokedState(), impersonationRow(), nil) },
		"row read fails": func() *accessStore {
			return impersonatorStore(revokedState(), nil, errors.New("session store timeout"))
		},
	}
	for storeName, store := range stores {
		for _, route := range impersonationQueryRoutes {
			t.Run(storeName+" "+route.listener+" "+route.path, func(t *testing.T) {
				s := impersonationStack(t, minter, store())
				got := postQuery(s, route.listener, route.path, token)
				if got.status != http.StatusOK || len(*s.seen) != 1 {
					t.Fatalf("got %d %q, resolver ran %d; want 200 and one run", got.status, got.body, len(*s.seen))
				}
				if claims := (*s.seen)[0]; claims.ImpersonationActive || claims.OrgID != ecOrg.String() {
					t.Fatalf("claims = %+v, want the caller itself (its own org %s, no impersonation)", claims, ecOrg)
				}
			})
		}
	}
}

// TestQueryImpersonatesANonSuperuserHolderAsGoAPIDoes: a platform set that is not the flag's set (no production
// users read carries one today) pins the rule: a non-superuser holding admin.impersonate, beside a live row, is
// served on each /query mount as the row's target, as go-api serves it.
func TestQueryImpersonatesANonSuperuserHolderAsGoAPIDoes(t *testing.T) {
	quietStdLog(t)
	minter := newAccessMinter(t)
	token := minter.mint(ecUser, nil, nil)
	for _, route := range impersonationQueryRoutes {
		t.Run(route.listener+" "+route.path, func(t *testing.T) {
			s := impersonationStack(t, minter, impersonatorStore(nonSuperuserHolderState(), impersonationRow(), nil))
			got := postQuery(s, route.listener, route.path, token)
			if got.status != http.StatusOK || len(*s.seen) != 1 {
				t.Fatalf("got %d %q, resolver ran %d; want 200 and one run", got.status, got.body, len(*s.seen))
			}
			if claims := (*s.seen)[0]; !claims.ImpersonationActive || claims.OrgID != irTargetOrg.String() || claims.Role != "viewer" {
				t.Fatalf("claims = %+v, want the target (org %s, role viewer, impersonation active)", claims, irTargetOrg)
			}
		})
	}
}

// TestQueryImpersonatesAFlagSuperuserAsBefore: a superuser that carries only the flag (every production users
// read) holds admin.impersonate, so a live row still makes it the target on each /query mount.
func TestQueryImpersonatesAFlagSuperuserAsBefore(t *testing.T) {
	quietStdLog(t)
	minter := newAccessMinter(t)
	token := minter.mint(ecUser, nil, nil)
	for _, route := range impersonationQueryRoutes {
		t.Run(route.listener+" "+route.path, func(t *testing.T) {
			s := impersonationStack(t, minter, impersonatorStore(flagSuperuserState(), impersonationRow(), nil))
			got := postQuery(s, route.listener, route.path, token)
			if got.status != http.StatusOK || len(*s.seen) != 1 {
				t.Fatalf("got %d %q, resolver ran %d; want 200 and one run", got.status, got.body, len(*s.seen))
			}
			if claims := (*s.seen)[0]; !claims.ImpersonationActive || claims.OrgID != irTargetOrg.String() {
				t.Fatalf("claims = %+v, want the target (org %s, impersonation active)", claims, irTargetOrg)
			}
		})
	}
}
