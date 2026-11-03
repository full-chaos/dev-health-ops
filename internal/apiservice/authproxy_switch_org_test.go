package apiservice

import (
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

const switchOrgBody = `{"org_id": "3a6f0c52-9b0e-4d7a-8f3b-2c1d5e6f7a8b"}`

func (f *ownerFixture) switchOrg(t *testing.T, bearer, body string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(http.MethodPost, "/api/v1/auth/switch-org", strings.NewReader(body))
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

// With the sessions aggregate on, switch-org is auth-service's route: go-api forwards it unread whatever the bearer
// (none, go-api's own, one addressed to auth-service, a broken one) and whatever the body, answers with auth-service's
// answer, and reads nothing itself, so an eddsa session_access_token (audience auth-service) is never judged by go-api.
func TestSwitchOrgIsForwardedUnreadUnderTheSessionsFlag(t *testing.T) {
	stub := newStubUpstream(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusTeapot)
		_, _ = w.Write([]byte(ownerUpstreamBody))
	})
	f := newOwnerFixture(t, mustProxyConfig(t, allOn(stub.URL)))
	for name, c := range map[string]struct{ bearer, body string }{
		"no bearer":            {"", switchOrgBody},
		"go-api access token":  {proxyBearer(t), switchOrgBody},
		"session-route bearer": {sessionRouteBearer, switchOrgBody},
		"broken bearer":        {"not-a-token", switchOrgBody},
		"body not json":        {sessionRouteBearer, `{"org_id": `},
		"superuser token":      {ownerSuperuserBearer(t, f.store.admin), switchOrgBody},
	} {
		t.Run(name, func(t *testing.T) {
			before := len(stub.requests())
			recorder := f.switchOrg(t, c.bearer, c.body)
			if recorder.Code != http.StatusTeapot || recorder.Body.String() != ownerUpstreamBody {
				t.Fatalf("answer %d %s, want auth-service's 418", recorder.Code, recorder.Body.String())
			}
			if len(stub.requests()) != before+1 {
				t.Fatal("the request did not reach auth-service")
			}
			seen := stub.last(t)
			if seen.body != c.body || seen.path != "/api/v1/auth/switch-org" {
				t.Fatalf("forwarded %s %q, want the client's bytes", seen.path, seen.body)
			}
			want := ""
			if c.bearer != "" {
				want = "Bearer " + c.bearer
			}
			if got := seen.header.Get("Authorization"); got != want {
				t.Fatalf("forwarded Authorization %q, want %q", got, want)
			}
			if recorder.Header().Get("X-Impersonating") != "" {
				t.Fatal("go-api's impersonation layer ran on a route it forwards unread")
			}
		})
	}
	if got := f.reads.Load(); got != 0 {
		t.Fatalf("go-api made %d store reads for a route auth-service authenticates", got)
	}
}

// Only the sessions flag moves the route: with it off, go-api's guard answers a missing or foreign bearer itself,
// the scope layers run, and nothing is forwarded, (the credentials flag cannot be on without it).
func TestSwitchOrgKeepsGoAPIsChainWithTheSessionsFlagOff(t *testing.T) {
	stub := newStubUpstream(t, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusTeapot) })
	f := newOwnerFixture(t, mustProxyConfig(t, map[string]string{EnvAuthProxyURL: stub.URL}))
	for name, bearer := range map[string]string{"no bearer": "", "session-route bearer": sessionRouteBearer} {
		if recorder := f.switchOrg(t, bearer, switchOrgBody); recorder.Code != http.StatusUnauthorized {
			t.Errorf("%s: %d %s, want go-api's own 401", name, recorder.Code, recorder.Body.String())
		}
	}
	if seen := stub.requests(); len(seen) != 0 {
		t.Fatalf("auth-service saw %d requests with the sessions flag off", len(seen))
	}
	if recorder := f.switchOrg(t, ownerSuperuserBearer(t, f.store.admin), `{"org_id": "not-a-uuid"}`); recorder.Header().Get("X-Impersonating") != "true" {
		t.Fatalf("sessions flag off: the impersonation layer did not run (%d %v)", recorder.Code, recorder.Header())
	}
}

// The acp corpus leg counts switch-org in the sessions aggregate.
func TestAuthCorpusAcpRoutesListSwitchOrgUnderSessions(t *testing.T) {
	raw, err := os.ReadFile("../../ci/auth_corpus_acp_routes.tsv")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains("\n"+string(raw), "\nsessions\tPOST\t/api/v1/auth/switch-org\n") {
		t.Error("ci/auth_corpus_acp_routes.tsv has no sessions POST /api/v1/auth/switch-org line")
	}
}
