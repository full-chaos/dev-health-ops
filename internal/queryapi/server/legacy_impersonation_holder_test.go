package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
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

// legacyHolderSubscriptionDocument is a registered subscription on the access stack: not a query, so refused like a
// mutation.
const legacyHolderSubscriptionDocument = "subscription LegacyHolderSub { probe }"

// actionOnlyHolderStore is a live admin that is not a superuser and holds admin.impersonate by its platform set (a
// member of ecOrg), with session as its go-api row and sessionErr as the row read's failure. /query serves it as
// itself; with the switch on, its live row refuses its non-query documents.
func actionOnlyHolderStore(session *policy.Impersonation, sessionErr error) *accessStore {
	store := legacyStore(session, sessionErr)
	platform := rule.PlatformOf(rule.ActionAdminImpersonate)
	store.states[ecUser] = policy.UserState{IsActive: true, IsSuperuser: false, Platform: &platform, TokenVersion: 5}
	return store
}

// actionOnlyHolderTokens is the caller's legacy HS256 token: with its user access token go-api's scope lets it
// impersonate, so it is the legacy caller (TestQueryAppliesASessionAsGoAPIsScopeDoes).
func actionOnlyHolderTokens(t *testing.T, minter *accessMinter) map[string]string {
	t.Helper()
	return map[string]string{
		"HS256": ecMintEdgeToken(t, ecEdgeClaims{sub: ecUser.String(), orgID: ecOrg.String(), role: "admin", tokenVersion: 5}),
	}
}

// postDocument sends document as the POST body to path on listener.
func postDocument(s *behaviourStack, listener, path, bearer, document string) legacyAnswer {
	body, _ := json.Marshal(map[string]string{"query": document})
	request := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+bearer)
	recorder := httptest.NewRecorder()
	s.handlers[listener].ServeHTTP(recorder, request)
	header := recorder.Header().Clone()
	header.Del("X-Request-ID")
	return legacyAnswer{recorder.Code, strings.TrimSpace(recorder.Body.String()), header}
}

// TestQueryRefusesAnActionOnlyHoldersWriteBesideALiveRow: with the switch on, an admin.impersonate holder that is
// not a superuser beside a live (or unreadable) go-api row has its mutation refused on each /query mount (403, no
// resolver); its query keeps today's answer, byte-equal to the switch off (served as itself). The switch off changes
// nothing.
func TestQueryRefusesAnActionOnlyHoldersWriteBesideALiveRow(t *testing.T) {
	quietStdLog(t)
	minter := newAccessMinter(t)
	stores := map[string]func() *accessStore{
		"live row":       func() *accessStore { return actionOnlyHolderStore(legacySession(), nil) },
		"row read fails": func() *accessStore { return actionOnlyHolderStore(nil, errors.New("session store timeout")) },
	}
	for storeName, store := range stores {
		for signing, token := range actionOnlyHolderTokens(t, minter) {
			for _, route := range legacyQueryRoutes {
				t.Run(storeName+" "+signing+" "+route.listener+" "+route.path, func(t *testing.T) {
					for kind, document := range map[string]string{"mutation": legacyMutationDocument, "subscription": legacyHolderSubscriptionDocument} {
						on := legacyStack(t, minter, store(), true)
						if got := postDocument(on, route.listener, route.path, token, document); got.Status != http.StatusForbidden || got.Body != legacyRefusalBody || len(*on.seen) != 0 {
							t.Fatalf("switch on, %s: got %d %s, ran %d; want 403 %s and no resolver", kind, got.Status, got.Body, len(*on.seen), legacyRefusalBody)
						}
					}
					read := legacyStack(t, minter, store(), true)
					got := postDocument(read, route.listener, route.path, token, iaDocument)
					off := legacyStack(t, minter, store(), false)
					if was := postDocument(off, route.listener, route.path, token, iaDocument); got.Status != http.StatusOK || !got.equal(was) || len(*read.seen) != 1 || (*read.seen)[0].ImpersonationActive || (*read.seen)[0].OrgID != ecOrg.String() {
						t.Fatalf("switch on, query: %+v (claims %+v), off %+v; want 200 as itself, byte-equal", got, *read.seen, was)
					}
					offWrite := legacyStack(t, minter, store(), false)
					if got := postDocument(offWrite, route.listener, route.path, token, legacyMutationDocument); got.Status != http.StatusOK || len(*offWrite.seen) != 1 {
						t.Fatalf("switch off, mutation: got %d %s, ran %d; want today's 200", got.Status, got.Body, len(*offWrite.seen))
					}
				})
			}
		}
	}
}

// TestWriteDoorRefusesAnActionOnlyHoldersWriteBesideALiveRow: the proof-write door (digest.KindMutation) refuses the
// same caller's mutation with the switch on, and serves it with the switch off (today's answer).
func TestWriteDoorRefusesAnActionOnlyHoldersWriteBesideALiveRow(t *testing.T) {
	quietStdLog(t)
	minter := newAccessMinter(t)
	door := func(t *testing.T, on bool) (http.HandlerFunc, *[]authctx.Claims) {
		t.Helper()
		store := newQueryAPIUserStoreFrom(actionOnlyHolderStore(legacySession(), nil))
		auth := ecEdgeAuth(t, store)
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
		routes := http.NewServeMux()
		routes.Handle("/query/proof-write", newDocumentDispatchHandler(getenv, mux, map[string]string{digestHex(proofWriteMutation): "probe"}, nil, auth, store, digest.KindMutation, func(context.Context, string) bool { return true }))
		return routes.ServeHTTP, seen
	}
	post := func(handler http.HandlerFunc, token string) *httptest.ResponseRecorder {
		body, _ := json.Marshal(map[string]string{"query": proofWriteMutation})
		request := httptest.NewRequest(http.MethodPost, "/query/proof-write", bytes.NewReader(body))
		request.Header.Set("Authorization", "Bearer "+token)
		recorder := httptest.NewRecorder()
		handler(recorder, request)
		return recorder
	}
	for signing, token := range actionOnlyHolderTokens(t, minter) {
		t.Run(signing, func(t *testing.T) {
			on, seenOn := door(t, true)
			if got := post(on, token); got.Code != http.StatusForbidden || strings.TrimSpace(got.Body.String()) != legacyRefusalBody || len(*seenOn) != 0 {
				t.Fatalf("switch on: %d %q, ran %d; want 403 %s and no resolver", got.Code, got.Body.String(), len(*seenOn), legacyRefusalBody)
			}
			off, seenOff := door(t, false)
			if got := post(off, token); got.Code != http.StatusOK || len(*seenOff) != 1 || (*seenOff)[0].ImpersonationActive {
				t.Fatalf("switch off: %d %q, claims %+v; want today's 200 as itself", got.Code, got.Body.String(), *seenOff)
			}
		})
	}
}

// TestInvalidImpersonationSwitchStartsWithOneWarnAndRefuses: an AUTH_PROXY_IMPERSONATION that does not parse never
// stops the process (product rule 3: no start refusal for config hygiene), with or without the query routes
// configured: the plane builds, one WARN names the variable and the accepted values, and the refusal is on (the
// /graphql deps and every handler read it as on). A value that parses writes no WARN.
func TestInvalidImpersonationSwitchStartsWithOneWarnAndRefuses(t *testing.T) {
	for raw, wantWarn := range map[string]bool{"yes": true, "on": true, "": false, "true": false, "false": false} {
		t.Run(raw, func(t *testing.T) {
			var buffer bytes.Buffer
			previous := slog.Default()
			slog.SetDefault(slog.New(slog.NewJSONHandler(&buffer, nil)))
			defer slog.SetDefault(previous)
			plane, err := BuildWithLookup(func(name string) (string, bool) {
				if name == impersonationProxyEnv {
					return raw, raw != ""
				}
				return "", false
			})
			if err != nil {
				t.Fatalf("%s=%q: Build = %v, want the process to start", impersonationProxyEnv, raw, err)
			}
			if plane.Close != nil {
				plane.Close()
			}
			warns := 0
			for _, line := range strings.Split(strings.TrimSpace(buffer.String()), "\n") {
				var record map[string]any
				if json.Unmarshal([]byte(line), &record) != nil || record["level"] != "WARN" || record["variable"] != impersonationProxyEnv {
					continue
				}
				if accepted, _ := record["accepted"].(string); !strings.Contains(accepted, "true") || !strings.Contains(accepted, "false") {
					t.Fatalf("WARN %v does not name the accepted values", record)
				}
				warns++
			}
			if want := map[bool]int{true: 1, false: 0}[wantWarn]; warns != want {
				t.Fatalf("%s=%q: %d WARN lines, want %d: %s", impersonationProxyEnv, raw, warns, want, buffer.String())
			}
			getenv := getenvFunc(func(name string) string {
				if name == impersonationProxyEnv {
					return raw
				}
				return ""
			})
			if refuse := newGraphQLEdgeDeps(getenv, func(string) (string, bool) { return "", false }, nil, nil).refuseLegacyImpersonation; refuse != (wantWarn || raw == "true") {
				t.Fatalf("%s=%q: /graphql refuse=%v, want %v (fail closed)", impersonationProxyEnv, raw, refuse, wantWarn || raw == "true")
			}
		})
	}
}

// TestQueryLeavesANonHoldersWriteBesideALiveRow: the refusal follows admin.impersonate: a plain member (no platform
// action) beside a live row, or whose row read fails, keeps today's answer on a mutation, the switch on.
func TestQueryLeavesANonHoldersWriteBesideALiveRow(t *testing.T) {
	quietStdLog(t)
	minter := newAccessMinter(t)
	member := func(session *policy.Impersonation, sessionErr error) *accessStore {
		store := newAccessStore()
		store.sessions = map[uuid.UUID]*policy.Impersonation{}
		if session != nil {
			store.sessions[ecUser] = session
		}
		store.errSession = sessionErr
		return store
	}
	for name, store := range map[string]func() *accessStore{
		"live row":       func() *accessStore { return member(legacySession(), nil) },
		"row read fails": func() *accessStore { return member(nil, errors.New("session store timeout")) },
	} {
		for signing, token := range actionOnlyHolderTokens(t, minter) {
			for _, route := range legacyQueryRoutes {
				t.Run(name+" "+signing+" "+route.listener+" "+route.path, func(t *testing.T) {
					s := legacyStack(t, minter, store(), true)
					if got := postDocument(s, route.listener, route.path, token, legacyMutationDocument); got.Status != http.StatusOK || len(*s.seen) != 1 {
						t.Fatalf("got %d %s, ran %d; want today's 200 and one run", got.Status, got.Body, len(*s.seen))
					}
				})
			}
		}
	}
}
