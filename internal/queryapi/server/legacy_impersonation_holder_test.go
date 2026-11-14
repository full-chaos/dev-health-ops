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
// member of ecOrg), with session as its go-api row and sessionErr as the row read's failure. /query applies its live
// row to it as to any holder (liveScope); with the switch on, the live row refuses the request.
func actionOnlyHolderStore(session *policy.Impersonation, sessionErr error) *accessStore {
	store := legacyStore(session, sessionErr)
	platform := rule.PlatformOf(rule.ActionAdminImpersonate)
	store.states[ecUser] = policy.UserState{IsActive: true, IsSuperuser: false, Platform: &platform, TokenVersion: 5}
	return store
}

func actionOnlyHolderTokens(t *testing.T, minter *accessMinter) map[string]string {
	t.Helper()
	return map[string]string{
		"EdDSA": minter.mint(ecUser, nil, nil),
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

// TestQueryRefusesAnActionOnlyHoldersWriteBesideALiveRow: an admin.impersonate holder that is not a superuser is a
// holder like any other. With the switch on, beside a live go-api row, each of its documents (query, mutation,
// subscription) is refused on each /query mount (403, no resolver); a failed row read is go-api's 503 (fail closed).
// With the switch off, the live row makes it the target, as for a superuser holder.
func TestQueryRefusesAnActionOnlyHoldersWriteBesideALiveRow(t *testing.T) {
	quietStdLog(t)
	minter := newAccessMinter(t)
	documents := map[string]string{"query": iaDocument, "mutation": legacyMutationDocument, "subscription": legacyHolderSubscriptionDocument}
	for signing, token := range actionOnlyHolderTokens(t, minter) {
		for _, route := range legacyQueryRoutes {
			t.Run(signing+" "+route.listener+" "+route.path, func(t *testing.T) {
				for kind, document := range documents {
					on := legacyStack(t, minter, actionOnlyHolderStore(legacySession(), nil), true)
					if got := postDocument(on, route.listener, route.path, token, document); got.Status != http.StatusForbidden || got.Body != legacyRefusalBody || len(*on.seen) != 0 {
						t.Fatalf("switch on, live row, %s: got %d %s, ran %d; want 403 %s and no resolver", kind, got.Status, got.Body, len(*on.seen), legacyRefusalBody)
					}
					fails := legacyStack(t, minter, actionOnlyHolderStore(nil, errors.New("session store timeout")), true)
					if got := postDocument(fails, route.listener, route.path, token, document); got.Status != http.StatusServiceUnavailable || got.Body != restUnavailable || len(*fails.seen) != 0 {
						t.Fatalf("switch on, row read fails, %s: got %d %s, ran %d; want 503 and no resolver", kind, got.Status, got.Body, len(*fails.seen))
					}
				}
				off := legacyStack(t, minter, actionOnlyHolderStore(legacySession(), nil), false)
				if got := postDocument(off, route.listener, route.path, token, iaDocument); got.Status != http.StatusOK || len(*off.seen) != 1 || !(*off.seen)[0].ImpersonationActive || (*off.seen)[0].OrgID != irTargetOrg.String() {
					t.Fatalf("switch off, query: %+v (claims %+v); want 200 as the target", got, *off.seen)
				}
			})
		}
	}
}

// TestWriteDoorRefusesAnActionOnlyHoldersWriteBesideALiveRow: the proof-write door (digest.KindMutation) refuses the
// same caller's mutation with the switch on, and with the switch off serves it as the row's target.
func TestWriteDoorRefusesAnActionOnlyHoldersWriteBesideALiveRow(t *testing.T) {
	quietStdLog(t)
	minter := newAccessMinter(t)
	door := func(t *testing.T, on bool) (http.HandlerFunc, *[]authctx.Claims) {
		t.Helper()
		store := newQueryAPIUserStoreFrom(actionOnlyHolderStore(legacySession(), nil))
		auth := ecEdgeAuth(t, store)
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
			if got := post(off, token); got.Code != http.StatusOK || len(*seenOff) != 1 || !(*seenOff)[0].ImpersonationActive || (*seenOff)[0].OrgID != irTargetOrg.String() {
				t.Fatalf("switch off: %d %q, claims %+v; want 200 as the target", got.Code, got.Body.String(), *seenOff)
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
