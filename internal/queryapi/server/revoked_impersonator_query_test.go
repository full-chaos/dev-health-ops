package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole/rule"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/digest"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/routeswitch"
)

// A caller that no longer holds admin.impersonate (a superuser whose platform set lost the action) beside a live
// go-api impersonation row. go-api's scope applies a row to a holder only; /query's live scope must do the same: the
// caller is decided as itself, never as the target, and the write door refuses it while the row is live.

var revokedSwitches = map[string]bool{"switch unset": false, "switch on": true}

// TestQueryServesTheRevokedImpersonatorAsItself: on each /query mount the revoked caller is served in its own org,
// with no impersonation, the switch unset or on; its row is not read on a read (a failed read changes nothing).
func TestQueryServesTheRevokedImpersonatorAsItself(t *testing.T) {
	quietStdLog(t)
	minter := newAccessMinter(t)
	stores := map[string]func() *accessStore{
		"live row":       func() *accessStore { return revokedHolderStore(legacySession(), nil) },
		"row read fails": func() *accessStore { return revokedHolderStore(nil, errors.New("session store timeout")) },
	}
	for storeName, store := range stores {
		for switchName, on := range revokedSwitches {
			for signing, token := range legacyTokens(t, minter) {
				for _, route := range legacyQueryRoutes {
					t.Run(storeName+" "+switchName+" "+signing+" "+route.listener+" "+route.path, func(t *testing.T) {
						s := legacyStack(t, minter, store(), on)
						got, _ := legacyQuery(s, route.listener, route.path, token)
						if got.Status != http.StatusOK || len(*s.seen) != 1 {
							t.Fatalf("got %d %s, resolver ran %d; want 200 and one run", got.Status, got.Body, len(*s.seen))
						}
						if claims := (*s.seen)[0]; claims.ImpersonationActive || claims.OrgID != ecOrg.String() {
							t.Fatalf("claims = %+v, want the caller itself (its own org %s, no impersonation)", claims, ecOrg)
						}
					})
				}
			}
		}
	}
}

// revokedWriteDoor is the proof-write door (digest.KindMutation, the org allowlist) over store.
func revokedWriteDoor(t *testing.T, minter *accessMinter, store *accessStore, on bool) (http.HandlerFunc, *[]authctx.Claims) {
	t.Helper()
	users := newQueryAPIUserStoreFrom(store)
	auth := ecEdgeAuth(t, users)
	if err := routeAuthenticator(auth, testAccess(minter.keys, nil, accessTestNow).opts, true); err != nil {
		t.Fatal(err)
	}
	seen := &[]authctx.Claims{}
	mux := routeswitch.NewMux(routeswitch.StaticSwitch{"probe": true})
	mux.Register("probe", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		claims, _ := authctx.FromContext(r.Context())
		*seen = append(*seen, claims)
		w.WriteHeader(http.StatusOK)
	}))
	getenv := getenvFunc(func(name string) string {
		if name == impersonationProxyEnv && on {
			return "true"
		}
		return ""
	})
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

// TestProofWriteDoorRefusesTheRevokedImpersonator: the write door refuses the revoked caller while its row is live,
// with the impersonation refusal, the switch unset or on; with no live row it is served as itself; a failed row read
// is the door's answer for a failed identity read (the request is not served).
func TestProofWriteDoorRefusesTheRevokedImpersonator(t *testing.T) {
	quietStdLog(t)
	minter := newAccessMinter(t)
	for switchName, on := range revokedSwitches {
		for signing, token := range legacyTokens(t, minter) {
			t.Run(switchName+" "+signing, func(t *testing.T) {
				live, seenLive := revokedWriteDoor(t, minter, revokedHolderStore(legacySession(), nil), on)
				if got := postProofWrite(live, token); got.Code != http.StatusForbidden || strings.TrimSpace(got.Body.String()) != legacyRefusalBody || len(*seenLive) != 0 {
					t.Fatalf("live row: %d %s, ran %d; want 403 %s and no resolver", got.Code, got.Body.String(), len(*seenLive), legacyRefusalBody)
				}
				none, seenNone := revokedWriteDoor(t, minter, revokedHolderStore(nil, nil), on)
				if got := postProofWrite(none, token); got.Code != http.StatusOK || len(*seenNone) != 1 || (*seenNone)[0].ImpersonationActive || (*seenNone)[0].OrgID != ecOrg.String() {
					t.Fatalf("no row: %d %s claims=%+v, want 200 served as itself", got.Code, got.Body.String(), *seenNone)
				}
				fails, seenFails := revokedWriteDoor(t, minter, revokedHolderStore(nil, errors.New("session store timeout")), on)
				holder, _ := revokedWriteDoor(t, minter, legacyStore(nil, errors.New("session store timeout")), on)
				got, want := postProofWrite(fails, token), postProofWrite(holder, token)
				if got.Code == http.StatusOK || len(*seenFails) != 0 || got.Code != want.Code || got.Body.String() != want.Body.String() {
					t.Fatalf("row read fails: %d %s, ran %d; want the holder's failed-read answer %d %s", got.Code, got.Body.String(), len(*seenFails), want.Code, want.Body.String())
				}
			})
		}
	}
}

// TestProofWriteDoorFailsClosedForAPlainMemberWhoseRowReadFails: the write door reads the row of every caller that
// does not hold admin.impersonate, so a plain member's write now depends on that read; a failed read is the door's
// failed identity read (the bare 401), with no resolver run, the switch unset or on.
func TestProofWriteDoorFailsClosedForAPlainMemberWhoseRowReadFails(t *testing.T) {
	quietStdLog(t)
	minter := newAccessMinter(t)
	member := map[string]string{
		"HS256": ecMintEdgeToken(t, ecEdgeClaims{sub: ecUser.String(), orgID: ecOrg.String(), role: "admin", tokenVersion: 5}),
		"EdDSA": minter.mint(ecUser, nil, nil),
	}
	for switchName, on := range revokedSwitches {
		for signing, token := range member {
			t.Run(switchName+" "+signing, func(t *testing.T) {
				store := newAccessStore()
				store.errSession = errors.New("session store timeout")
				door, seen := revokedWriteDoor(t, minter, store, on)
				if got := postProofWrite(door, token); got.Code != http.StatusUnauthorized || got.Body.String() != queryRefusal || len(*seen) != 0 {
					t.Fatalf("got %d %q, ran %d; want 401 %q and no resolver", got.Code, got.Body.String(), len(*seen), queryRefusal)
				}
				ok, seenOK := revokedWriteDoor(t, minter, newAccessStore(), on)
				if got := postProofWrite(ok, token); got.Code != http.StatusOK || len(*seenOK) != 1 {
					t.Fatalf("read succeeds: got %d %q, ran %d; want 200 and one run", got.Code, got.Body.String(), len(*seenOK))
				}
			})
		}
	}
}

// nonSuperuserHolderStore is a live non-superuser whose platform set holds admin.impersonate, beside a live go-api
// row. /query serves it as itself (main's answer): a session is applied only to a live superuser that holds the action.
func nonSuperuserHolderStore() *accessStore {
	store := legacyStore(legacySession(), nil)
	platform := rule.PlatformOf(rule.ActionAdminImpersonate)
	store.states[ecUser] = policy.UserState{IsActive: true, IsSuperuser: false, Platform: &platform, TokenVersion: 5}
	return store
}

// TestQueryServesANonSuperuserHolderAsItself: on each /query mount and on the write door, the non-superuser holder
// beside a live row is served as itself in its own org, with no impersonation, the switch unset or on.
func TestQueryServesANonSuperuserHolderAsItself(t *testing.T) {
	quietStdLog(t)
	minter := newAccessMinter(t)
	tokens := map[string]string{
		"HS256": ecMintEdgeToken(t, ecEdgeClaims{sub: ecUser.String(), orgID: ecOrg.String(), role: "admin", tokenVersion: 5}),
		"EdDSA": minter.mint(ecUser, nil, nil),
	}
	asItself := func(t *testing.T, code int, body string, seen []authctx.Claims) {
		t.Helper()
		if code != http.StatusOK || len(seen) != 1 || seen[0].ImpersonationActive || seen[0].OrgID != ecOrg.String() {
			t.Fatalf("got %d %s claims=%+v, want 200 served as itself (its own org %s, no impersonation)", code, body, seen, ecOrg)
		}
	}
	for switchName, on := range revokedSwitches {
		for signing, token := range tokens {
			for _, route := range legacyQueryRoutes {
				t.Run(switchName+" "+signing+" "+route.listener+" "+route.path, func(t *testing.T) {
					s := legacyStack(t, minter, nonSuperuserHolderStore(), on)
					got, _ := legacyQuery(s, route.listener, route.path, token)
					asItself(t, got.Status, got.Body, *s.seen)
				})
			}
			t.Run(switchName+" "+signing+" write door", func(t *testing.T) {
				door, seen := revokedWriteDoor(t, minter, nonSuperuserHolderStore(), on)
				got := postProofWrite(door, token)
				asItself(t, got.Code, got.Body.String(), *seen)
			})
		}
	}
}
