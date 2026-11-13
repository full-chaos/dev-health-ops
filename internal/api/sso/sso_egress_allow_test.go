package sso

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"golang.org/x/oauth2"
)

const ssoEgressAllowEnv = "SSO_EGRESS_ALLOW_CIDRS"

// startListedIdP is an IdP on 127.0.0.1 over plain HTTP that counts the requests it receives; its bodies are bounded.
func startListedIdP(t *testing.T) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var hits atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/token" {
			_, _ = w.Write([]byte(`{"access_token":"at-1","token_type":"Bearer","expires_in":300}`))
			return
		}
		_, _ = w.Write([]byte(`{"sub":"sub-1"}`))
	}))
	t.Cleanup(server.Close)
	return server, &hits
}

// With SSO_EGRESS_ALLOW_CIDRS naming the IdP's address, the default OIDC client reaches it, by address and by the
// name localhost.
func TestSSOEgressAllowLetsTheDefaultOIDCClientReachAListedIdP(t *testing.T) {
	t.Setenv(ssoEgressAllowEnv, "127.0.0.1/32")
	server, hits := startListedIdP(t)
	for _, target := range []string{server.URL, strings.Replace(server.URL, "127.0.0.1", "localhost", 1)} {
		response, err := defaultOIDCClient().Get(target + "/userinfo")
		if err != nil {
			t.Fatalf("GET %s with the IdP listed: %v", target, err)
		}
		response.Body.Close()
		if response.StatusCode != http.StatusOK {
			t.Fatalf("GET %s: status %d", target, response.StatusCode)
		}
	}
	if hits.Load() != 2 {
		t.Fatalf("the IdP saw %d request(s), want 2", hits.Load())
	}
}

// The OIDC calls of the handlers production builds (no Deps.HTTPClient) reach a listed IdP: the token exchange and
// userinfo.
func TestSSOEgressAllowLetsTheOIDCHandlersReachAListedIdP(t *testing.T) {
	t.Setenv(ssoEgressAllowEnv, "127.0.0.1/32")
	server, hits := startListedIdP(t)
	h := newHandlers(Deps{})
	if _, err := h.fetchUserinfo(context.Background(), server.URL+"/userinfo", "at-1"); err != nil {
		t.Fatalf("userinfo with the IdP listed: %v", err)
	}
	config := oauth2.Config{ClientID: "id", ClientSecret: "client-secret-cell",
		Endpoint: oauth2.Endpoint{TokenURL: server.URL + "/token", AuthStyle: oauth2.AuthStyleInParams}}
	if _, err := h.exchangeOIDCCode(context.Background(), config, "code", "verifier"); err != nil {
		t.Fatalf("token exchange with the IdP listed: %v", err)
	}
	if hits.Load() != 2 {
		t.Fatalf("the IdP saw %d request(s), want 2", hits.Load())
	}
}

// The social OAuth exchange of the same handlers is not widened by the SSO list: a token endpoint on 127.0.0.1 is
// still refused before any request reaches it.
func TestSSOEgressAllowNeverReachesTheSocialOAuthExchange(t *testing.T) {
	t.Setenv(ssoEgressAllowEnv, "127.0.0.1/32")
	server, hits := startListedIdP(t)
	h := newHandlers(Deps{})
	if _, err := h.exchangeOAuthCode(context.Background(), server.URL+"/token", "id", "client-secret-cell", "code", "https://app.test/cb"); err == nil {
		t.Fatal("the social OAuth exchange reached 127.0.0.1 with the SSO list set")
	}
	if hits.Load() != 0 {
		t.Fatalf("the social token endpoint on 127.0.0.1 was reached %d time(s)", hits.Load())
	}
}
