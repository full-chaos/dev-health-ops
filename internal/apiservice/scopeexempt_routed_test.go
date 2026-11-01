package apiservice

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

var routedExemptRows = []struct {
	target string
	want   bool
}{
	{"POST /api/v1/auth/change-password", true},
	{"POST /api/v1/auth/change%2Dpassword", true},
	{"POST /api/v1/auth/change%2dpassword", true},
	{"POST /api/v1/%61uth/change-password", true},
	{"POST /api/v1/auth%2Fchange-password", true},
	{"POST /api/v1/auth%2fchange-password", true},
	{"GET /api/v1/auth/change-password", false},
	{"POST /api/v1/auth/change-password/", false},
	{"POST /api/v1/auth/change%2Fpassword", false},
	{"POST /api/v1/auth/change%252Dpassword", false},
	{"POST /api/v1/auth/me", false},
	{"POST /api/v1/webhooks/git%68ub", false},
}

func scopeExemptMarked(t *testing.T, exempt map[string]bool, method, path string) bool {
	t.Helper()
	marked := false
	probe := markScopeExempt(exempt)(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		marked, _ = r.Context().Value(signatureOnlyKey{}).(bool)
	}))
	probe.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(method, path, nil))
	return marked
}

func (f *ownerFixture) send(t *testing.T, method, path, bearer string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(method, path, strings.NewReader(`{"current_password": "old", "new_password": "long-enough-1"}`))
	request.RemoteAddr = "192.0.2.10:4711"
	request.Header.Set("Origin", proxyOrigin)
	request.Header.Set("Content-Type", "application/json")
	if bearer != "" {
		request.Header.Set("Authorization", "Bearer "+bearer)
	}
	recorder := httptest.NewRecorder()
	f.handler.ServeHTTP(recorder, request)
	return recorder
}

// C1: the exempt set is the set of paths the router routes (r.URL.Path), exact method and path.
func TestScopeExemptMatchesTheRoutedPath(t *testing.T) {
	exempt := mustProxyConfig(t, allOn("http://127.0.0.1:1")).ScopeExemptRoutes()
	for _, row := range routedExemptRows {
		method, path, _ := strings.Cut(row.target, " ")
		if got := scopeExemptMarked(t, exempt, method, path); got != row.want {
			t.Errorf("%s: exempt %v, want %v", row.target, got, row.want)
		}
	}
}

// C1 differential: on the real router, a request is exempt exactly when the router sends it to the forward.
func TestScopeExemptEqualsRoutedToTheForward(t *testing.T) {
	stub := newStubUpstream(t, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusTeapot) })
	cfg := mustProxyConfig(t, allOn(stub.URL))
	f := newOwnerFixture(t, cfg)
	exempt := cfg.ScopeExemptRoutes()
	for _, row := range routedExemptRows {
		method, path, _ := strings.Cut(row.target, " ")
		before := len(stub.requests())
		f.send(t, method, path, sessionRouteBearer)
		forwarded := len(stub.requests()) > before
		marked := scopeExemptMarked(t, exempt, method, path)
		if marked != forwarded {
			t.Errorf("%s: exempt %v but routed to the forward %v", row.target, marked, forwarded)
		}
	}
}

// C2: the encoded forms are forwarded unread, as the exact form is.
func TestEncodedChangePasswordIsForwardedUnreadUnderTheCredentialsFlag(t *testing.T) {
	stub := newStubUpstream(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusTeapot)
		_, _ = w.Write([]byte(ownerUpstreamBody))
	})
	f := newOwnerFixture(t, mustProxyConfig(t, allOn(stub.URL)))
	for _, path := range []string{"/api/v1/auth/change%2Dpassword", "/api/v1/auth%2Fchange-password"} {
		t.Run(path, func(t *testing.T) {
			before := len(stub.requests())
			recorder := f.send(t, http.MethodPost, path, sessionRouteBearer)
			if recorder.Code != http.StatusTeapot || recorder.Body.String() != ownerUpstreamBody {
				t.Fatalf("answer %d %s, want auth-service's 418", recorder.Code, recorder.Body.String())
			}
			if len(stub.requests()) != before+1 {
				t.Fatal("the request did not reach auth-service")
			}
			if recorder.Header().Get("X-Impersonating") != "" {
				t.Fatal("go-api's impersonation layer ran on a route it forwards unread")
			}
			if got := f.reads.Load(); got != 0 {
				t.Fatalf("go-api made %d store reads for a route auth-service authenticates", got)
			}
		})
	}
}

// C3: flag off, the encoded form still runs the scope layers.
func TestEncodedChangePasswordKeepsGoAPIsChainWithTheCredentialsFlagOff(t *testing.T) {
	stub := newStubUpstream(t, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusTeapot) })
	env := allOn(stub.URL)
	env[AuthProxyFlagEnv(AuthCredentials)] = "false"
	f := newOwnerFixture(t, mustProxyConfig(t, env))
	for _, path := range []string{"/api/v1/auth/change%2Dpassword", "/api/v1/auth%2Fchange-password"} {
		recorder := f.send(t, http.MethodPost, path, ownerSuperuserBearer(t, f.store.admin))
		if recorder.Header().Get("X-Impersonating") != "true" {
			t.Errorf("%s: flag off: the impersonation layer did not run (%d %v)", path, recorder.Code, recorder.Header())
		}
	}
	if seen := stub.requests(); len(seen) != 0 {
		t.Fatalf("auth-service saw %d requests with the flag off", len(seen))
	}
}
