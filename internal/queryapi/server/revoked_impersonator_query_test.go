package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole/rule"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/digest"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/routeswitch"
)

// go-api's Scope.Impersonation applies a live impersonation row to a holder of admin.impersonate only
// (internal/api/policy/scope.go). /query's live scope must decide the same way: by the action, never by the
// superuser flag. A caller without the action is served as itself, and the write door refuses it while its row
// is live; a holder is served as the target, superuser or not.

const impersonationRefusalBody = `{"detail":"Not allowed in an impersonation session"}`

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

// TestQueryImpersonatesANonSuperuserHolderAsGoAPIDoes: the answer change. A non-superuser holding
// admin.impersonate, beside a live row, is served on each /query mount as the row's target, as go-api serves it.
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

// impersonationWriteDoor is the proof-write door (digest.KindMutation, every org allowed) over store.
func impersonationWriteDoor(t *testing.T, minter *accessMinter, store *accessStore) (http.HandlerFunc, *[]authctx.Claims) {
	t.Helper()
	users := newQueryAPIUserStoreFrom(store)
	auth := ecEdgeAuth(t, users)
	if err := routeAuthenticator(auth, testAccess(minter.keys, nil, accessTestNow).opts); err != nil {
		t.Fatal(err)
	}
	seen := &[]authctx.Claims{}
	mux := routeswitch.NewMux(routeswitch.StaticSwitch{"probe": true})
	mux.Register("probe", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		claims, _ := authctx.FromContext(r.Context())
		*seen = append(*seen, claims)
		w.WriteHeader(http.StatusOK)
	}))
	getenv := getenvFunc(func(string) string { return "" })
	allow := func(context.Context, string) bool { return true }
	routes := http.NewServeMux()
	routes.Handle("/query/proof-write", newDocumentDispatchHandler(getenv, mux, map[string]string{digestHex(proofWriteMutation): "probe"}, nil, auth, users, digest.KindMutation, allow))
	return routes.ServeHTTP, seen
}

func postProofWrite(handler http.HandlerFunc, token string) *httptest.ResponseRecorder {
	body, _ := json.Marshal(map[string]string{"query": proofWriteMutation})
	request := httptest.NewRequest(http.MethodPost, "/query/proof-write", strings.NewReader(string(body)))
	request.Header.Set("Authorization", "Bearer "+token)
	recorder := httptest.NewRecorder()
	handler(recorder, request)
	return recorder
}

// TestProofWriteDoorRefusesTheRevokedImpersonator: the write door refuses a superuser without admin.impersonate
// while its row is live, with the impersonation refusal and no resolver run; with no live row it is served as
// itself; a failed row read is the door's answer for a holder's failed read (the request is not served).
func TestProofWriteDoorRefusesTheRevokedImpersonator(t *testing.T) {
	quietStdLog(t)
	minter := newAccessMinter(t)
	token := minter.mint(ecUser, nil, nil)
	live, seenLive := impersonationWriteDoor(t, minter, impersonatorStore(revokedState(), impersonationRow(), nil))
	if got := postProofWrite(live, token); got.Code != http.StatusForbidden || strings.TrimSpace(got.Body.String()) != impersonationRefusalBody || len(*seenLive) != 0 {
		t.Fatalf("live row: %d %s, ran %d; want 403 %s and no resolver", got.Code, got.Body.String(), len(*seenLive), impersonationRefusalBody)
	}
	none, seenNone := impersonationWriteDoor(t, minter, impersonatorStore(revokedState(), nil, nil))
	if got := postProofWrite(none, token); got.Code != http.StatusOK || len(*seenNone) != 1 || (*seenNone)[0].ImpersonationActive || (*seenNone)[0].OrgID != ecOrg.String() {
		t.Fatalf("no row: %d %s claims=%+v, want 200 served as itself", got.Code, got.Body.String(), *seenNone)
	}
	fails, seenFails := impersonationWriteDoor(t, minter, impersonatorStore(revokedState(), nil, errors.New("session store timeout")))
	holder, _ := impersonationWriteDoor(t, minter, impersonatorStore(flagSuperuserState(), nil, errors.New("session store timeout")))
	got, want := postProofWrite(fails, token), postProofWrite(holder, token)
	if got.Code == http.StatusOK || len(*seenFails) != 0 || got.Code != want.Code || got.Body.String() != want.Body.String() {
		t.Fatalf("row read fails: %d %s, ran %d; want the holder's failed-read answer %d %s", got.Code, got.Body.String(), len(*seenFails), want.Code, want.Body.String())
	}
}

// TestProofWriteDoorFailsClosedForAPlainMemberWhoseRowReadFails: the write door reads the row of every caller
// without admin.impersonate, so a plain member's write now depends on that read; a failed read is the door's failed
// identity read (the bare 401) with no resolver run.
func TestProofWriteDoorFailsClosedForAPlainMemberWhoseRowReadFails(t *testing.T) {
	quietStdLog(t)
	minter := newAccessMinter(t)
	token := minter.mint(ecUser, nil, nil)
	store := newAccessStore()
	store.errSession = errors.New("session store timeout")
	door, seen := impersonationWriteDoor(t, minter, store)
	if got := postProofWrite(door, token); got.Code != http.StatusUnauthorized || got.Body.String() != queryRefusal || len(*seen) != 0 {
		t.Fatalf("got %d %q, ran %d; want 401 %q and no resolver", got.Code, got.Body.String(), len(*seen), queryRefusal)
	}
	ok, seenOK := impersonationWriteDoor(t, minter, newAccessStore())
	if got := postProofWrite(ok, token); got.Code != http.StatusOK || len(*seenOK) != 1 {
		t.Fatalf("read succeeds: got %d %q, ran %d; want 200 and one run", got.Code, got.Body.String(), len(*seenOK))
	}
}
