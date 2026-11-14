package server

import (
	"bytes"
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

// go-api's Scope.Impersonation reads the row of a caller without admin.impersonate on a request that does not only
// read, and only while the legacy refusal is on (internal/api/policy/scope.go). /query tells a read by the document
// kind, as /graphql does: with the switch on, such a caller beside a live row has its non-query documents refused on
// every /query mount, and its queries are served as itself; with the switch off its row is never read.

// nonHolderUnparsableDocument is a registered text that does not parse: not a read.
const nonHolderUnparsableDocument = "mutation NonHolderUnparsable { probe"

// nonHolderStores are the callers without admin.impersonate: a plain member, and a superuser whose platform set lost
// the action.
func nonHolderStores(session *policy.Impersonation, sessionErr error) map[string]*accessStore {
	member := newAccessStore()
	member.sessions = map[uuid.UUID]*policy.Impersonation{}
	if session != nil {
		member.sessions[ecUser] = session
	}
	member.errSession = sessionErr
	revoked := legacyStore(session, sessionErr)
	var held []string
	for _, action := range rule.PlatformActions {
		if action != rule.ActionAdminImpersonate {
			held = append(held, action)
		}
	}
	platform := rule.PlatformOf(held...)
	revoked.states[ecUser] = policy.UserState{IsActive: true, IsSuperuser: true, Platform: &platform, TokenVersion: 5}
	return map[string]*accessStore{"plain member": member, "revoked superuser": revoked}
}

var nonHolderNonQueryDocuments = map[string]string{
	"mutation":     legacyMutationDocument,
	"subscription": legacyHolderSubscriptionDocument,
	"unparsable":   nonHolderUnparsableDocument,
}

// TestQueryRefusesANonHoldersNonQueryDocumentBesideALiveRowUnderTheSwitch: switch on, a live row: each non-query
// document is refused with the impersonation refusal on each /query mount, no resolver; a query is served as itself.
func TestQueryRefusesANonHoldersNonQueryDocumentBesideALiveRowUnderTheSwitch(t *testing.T) {
	quietStdLog(t)
	minter := newAccessMinter(t)
	token := minter.mint(ecUser, nil, nil)
	for caller := range nonHolderStores(nil, nil) {
		for _, route := range legacyQueryRoutes {
			t.Run(caller+" "+route.listener+" "+route.path, func(t *testing.T) {
				for kind, document := range nonHolderNonQueryDocuments {
					s := legacyStack(t, minter, nonHolderStores(legacySession(), nil)[caller], true)
					if got := postDocument(s, route.listener, route.path, token, document); got.Status != http.StatusForbidden || got.Body != legacyRefusalBody || len(*s.seen) != 0 {
						t.Fatalf("%s: got %d %s, ran %d; want 403 %s and no resolver", kind, got.Status, got.Body, len(*s.seen), legacyRefusalBody)
					}
				}
				s := legacyStack(t, minter, nonHolderStores(legacySession(), nil)[caller], true)
				if got := postDocument(s, route.listener, route.path, token, iaDocument); got.Status != http.StatusOK || len(*s.seen) != 1 || (*s.seen)[0].ImpersonationActive || (*s.seen)[0].OrgID != ecOrg.String() {
					t.Fatalf("query: %+v claims %+v; want 200 as itself", got, *s.seen)
				}
			})
		}
	}
}

// TestQueryFailsClosedForANonHoldersWriteWhoseRowReadFailsUnderTheSwitch: switch on, the row read fails: a non-query
// document gets go-api's 503 for a live read that failed (as go-api answers there), no resolver;
// a query is served as itself.
func TestQueryFailsClosedForANonHoldersWriteWhoseRowReadFailsUnderTheSwitch(t *testing.T) {
	quietStdLog(t)
	minter := newAccessMinter(t)
	token := minter.mint(ecUser, nil, nil)
	fails := func(caller string) *accessStore {
		return nonHolderStores(nil, errors.New("session store timeout"))[caller]
	}
	for caller := range nonHolderStores(nil, nil) {
		for _, route := range legacyQueryRoutes {
			t.Run(caller+" "+route.listener+" "+route.path, func(t *testing.T) {
				for kind, document := range nonHolderNonQueryDocuments {
					s := legacyStack(t, minter, fails(caller), true)
					if got := postDocument(s, route.listener, route.path, token, document); got.Status != http.StatusServiceUnavailable || got.Body != restUnavailable || len(*s.seen) != 0 {
						t.Fatalf("%s: got %d %q, ran %d; want 503 %s and no resolver", kind, got.Status, got.Body, len(*s.seen), restUnavailable)
					}
				}
				s := legacyStack(t, minter, fails(caller), true)
				if got := postDocument(s, route.listener, route.path, token, iaDocument); got.Status != http.StatusOK || len(*s.seen) != 1 || (*s.seen)[0].ImpersonationActive {
					t.Fatalf("query: %+v claims %+v; want 200 as itself", got, *s.seen)
				}
			})
		}
	}
}

// TestQueryLeavesANonHolderAsItselfWithTheSwitchOff: switch off, its row is never read: beside a live row, or with a
// row read that would fail, every document is served as itself.
func TestQueryLeavesANonHolderAsItselfWithTheSwitchOff(t *testing.T) {
	quietStdLog(t)
	minter := newAccessMinter(t)
	token := minter.mint(ecUser, nil, nil)
	shapes := map[string]func(caller string) *accessStore{
		"live row": func(caller string) *accessStore { return nonHolderStores(legacySession(), nil)[caller] },
		"row read fails": func(caller string) *accessStore {
			return nonHolderStores(nil, errors.New("session store timeout"))[caller]
		},
	}
	for shape, store := range shapes {
		for caller := range nonHolderStores(nil, nil) {
			for _, route := range legacyQueryRoutes {
				t.Run(shape+" "+caller+" "+route.listener+" "+route.path, func(t *testing.T) {
					s := legacyStack(t, minter, store(caller), false)
					if got := postDocument(s, route.listener, route.path, token, legacyMutationDocument); got.Status != http.StatusOK || len(*s.seen) != 1 || (*s.seen)[0].ImpersonationActive || (*s.seen)[0].OrgID != ecOrg.String() {
						t.Fatalf("mutation: %+v claims %+v; want 200 as itself", got, *s.seen)
					}
				})
			}
		}
	}
}

// TestWriteDoorRefusesANonHoldersWriteUnderTheSwitch: the proof-write door (digest.KindMutation): switch on, a live
// row = 403, a failed row read = 503, no resolver; switch off = 200 as itself.
func TestWriteDoorRefusesANonHoldersWriteUnderTheSwitch(t *testing.T) {
	quietStdLog(t)
	minter := newAccessMinter(t)
	token := minter.mint(ecUser, nil, nil)
	door := func(t *testing.T, store *accessStore, on bool) (http.HandlerFunc, *[]authctx.Claims) {
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
		getenv := getenvFunc(func(name string) string {
			if name == impersonationProxyEnv && on {
				return "true"
			}
			return ""
		})
		routes := http.NewServeMux()
		routes.Handle("/query/proof-write", newDocumentDispatchHandler(getenv, mux, map[string]string{digestHex(proofWriteMutation): "probe"}, nil, auth, users, digest.KindMutation, func(context.Context, string) (bool, error) { return true, nil }))
		return routes.ServeHTTP, seen
	}
	post := func(handler http.HandlerFunc) *httptest.ResponseRecorder {
		body, _ := json.Marshal(map[string]string{"query": proofWriteMutation})
		request := httptest.NewRequest(http.MethodPost, "/query/proof-write", bytes.NewReader(body))
		request.Header.Set("Authorization", "Bearer "+token)
		recorder := httptest.NewRecorder()
		handler(recorder, request)
		return recorder
	}
	for caller := range nonHolderStores(nil, nil) {
		t.Run(caller, func(t *testing.T) {
			live, seenLive := door(t, nonHolderStores(legacySession(), nil)[caller], true)
			if got := post(live); got.Code != http.StatusForbidden || strings.TrimSpace(got.Body.String()) != legacyRefusalBody || len(*seenLive) != 0 {
				t.Fatalf("switch on, live row: %d %q, ran %d; want 403 %s and no resolver", got.Code, got.Body.String(), len(*seenLive), legacyRefusalBody)
			}
			fails, seenFails := door(t, nonHolderStores(nil, errors.New("session store timeout"))[caller], true)
			if got := post(fails); got.Code != http.StatusServiceUnavailable || strings.TrimSpace(got.Body.String()) != restUnavailable || len(*seenFails) != 0 {
				t.Fatalf("switch on, row read fails: %d %q, ran %d; want 503 %s and no resolver", got.Code, got.Body.String(), len(*seenFails), restUnavailable)
			}
			off, seenOff := door(t, nonHolderStores(legacySession(), nil)[caller], false)
			if got := post(off); got.Code != http.StatusOK || len(*seenOff) != 1 || (*seenOff)[0].ImpersonationActive || (*seenOff)[0].OrgID != ecOrg.String() {
				t.Fatalf("switch off: %d %q, claims %+v; want 200 as itself", got.Code, got.Body.String(), *seenOff)
			}
		})
	}
}

// sessionReadCounter is a store that counts its impersonation row reads.
type sessionReadCounter struct {
	*accessStore
	reads *int
}

func (s sessionReadCounter) ActiveImpersonation(ctx context.Context, id uuid.UUID) (*policy.Impersonation, error) {
	*s.reads++
	return s.accessStore.ActiveImpersonation(ctx, id)
}

// TestQueryReadsNoNonHolderRowForAQueryUnderTheSwitch: as go-api reads a non-holder's row on a write only, /query
// reads it only for a non-query document. Switch on, the row store failing: a query is served as itself with no row
// read; a mutation reads the row once and gets the failed-read 503.
func TestQueryReadsNoNonHolderRowForAQueryUnderTheSwitch(t *testing.T) {
	quietStdLog(t)
	minter := newAccessMinter(t)
	token := minter.mint(ecUser, nil, nil)
	for caller := range nonHolderStores(nil, nil) {
		for _, route := range legacyQueryRoutes {
			t.Run(caller+" "+route.listener+" "+route.path, func(t *testing.T) {
				run := func(document string) (legacyAnswer, *behaviourStack, int) {
					reads := 0
					store := sessionReadCounter{accessStore: nonHolderStores(nil, errors.New("session store timeout"))[caller], reads: &reads}
					s := newAccessStackRefusing(t, queryAPIUserStore{queryAPIStoreBase: store}, testAccess(minter.keys, nil, accessTestNow), true)
					got := postDocument(s, route.listener, route.path, token, document)
					return got, s, reads
				}
				got, s, reads := run(iaDocument)
				if got.Status != http.StatusOK || len(*s.seen) != 1 || (*s.seen)[0].ImpersonationActive || reads != 0 {
					t.Fatalf("query: %+v, ran %d, row reads %d; want 200 as itself and no row read", got, len(*s.seen), reads)
				}
				got, s, reads = run(legacyMutationDocument)
				if got.Status != http.StatusServiceUnavailable || got.Body != restUnavailable || len(*s.seen) != 0 || reads != 1 {
					t.Fatalf("mutation: %+v, ran %d, row reads %d; want 503, no resolver and one row read", got, len(*s.seen), reads)
				}
			})
		}
	}
}
