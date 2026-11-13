package sso

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/oauth2"

	"github.com/full-chaos/dev-health-ops/internal/api/externalurl"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/proxycapture"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/redirectprobe"
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

// C-a: the OIDC client newHandlers installs keeps today's posture (30 s timeout, no redirect, the GuardedTransport TLS
// and connection settings, no proxy); only its dial check differs, and it is not the social client.
func TestTheOIDCClientKeepsTodaysTimeoutTLSAndRedirectPolicy(t *testing.T) {
	h := newHandlers(Deps{})
	client, social := h.oidcClient, h.HTTPClient
	if client == nil || client == social {
		t.Fatal("the OIDC calls do not have a client of their own")
	}
	if client.Timeout != 30*time.Second || social.Timeout != 30*time.Second {
		t.Fatalf("timeouts %v / %v, want 30s", client.Timeout, social.Timeout)
	}
	for _, c := range []*http.Client{client, social} {
		if c.CheckRedirect == nil || c.CheckRedirect(nil, nil) != http.ErrUseLastResponse {
			t.Fatal("a default client follows redirects")
		}
	}
	got, ok := client.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("transport %T", client.Transport)
	}
	want := externalurl.GuardedTransport()
	if got.Proxy != nil || !reflect.DeepEqual(got.TLSClientConfig, want.TLSClientConfig) ||
		got.TLSHandshakeTimeout != want.TLSHandshakeTimeout || got.ForceAttemptHTTP2 != want.ForceAttemptHTTP2 ||
		got.MaxIdleConns != want.MaxIdleConns || got.IdleConnTimeout != want.IdleConnTimeout ||
		got.ExpectContinueTimeout != want.ExpectContinueTimeout || got.MaxResponseHeaderBytes != want.MaxResponseHeaderBytes ||
		got.DisableKeepAlives != want.DisableKeepAlives || got.DialContext == nil {
		t.Fatal("the OIDC transport differs from GuardedTransport beyond its dial check")
	}
	if supplied := (&http.Client{}); newHandlers(Deps{HTTPClient: supplied}).oidcClient != supplied {
		t.Fatal("a supplied client does not make the OIDC calls")
	}
}

// C-a: with the IdP listed, the OIDC client reaches it and still follows no redirect to another origin.
func TestSSOEgressAllowKeepsTheOIDCClientsNoRedirectPolicy(t *testing.T) {
	t.Setenv(ssoEgressAllowEnv, "127.0.0.1/32")
	probe := redirectprobe.New(t)
	h := newHandlers(Deps{})
	if response, err := h.oidcClient.Get(probe.Base.URL + "/jwks"); err == nil {
		response.Body.Close()
	}
	probe.Assert(t)
}

func TestTheDefaultSocialClientRefusesRedirectsOnItsOwn(t *testing.T) {
	if defaultSocialClient().CheckRedirect == nil || defaultSocialClient().CheckRedirect(nil, nil) != http.ErrUseLastResponse {
		t.Fatal("the default social client follows redirects")
	}
}

func TestTheDefaultSocialClientNeverDialsTheEnvironmentProxy(t *testing.T) {
	proxycapture.Check(t, runCaptureChild, func(ctx context.Context) {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, proxycapture.Target, nil)
		if err != nil {
			panic(err)
		}
		request.Header.Set("Authorization", "Bearer "+proxycapture.Canary)
		if response, err := defaultSocialClient().Do(request); err == nil {
			_ = response.Body.Close()
		}
	}).AssertNothingReached(t)
}

// Every OIDC call of the handlers goes through the OIDC client and no social OAuth call does: oidc.go names no
// h.HTTPClient, oauth.go names no oidcClient.
func TestOIDCAndSocialCallsUseTheirOwnClients(t *testing.T) {
	read := func(name string) string {
		raw, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		return string(raw)
	}
	oidcSource, oauthSource := read("oidc.go"), read("oauth.go")
	if strings.Contains(oidcSource, "h.HTTPClient") || strings.Count(oidcSource, "h.oidcClient") != 4 {
		t.Errorf("oidc.go: h.HTTPClient %d, h.oidcClient %d; want 0 and the 4 OIDC calls",
			strings.Count(oidcSource, "h.HTTPClient"), strings.Count(oidcSource, "h.oidcClient"))
	}
	if strings.Contains(oauthSource, "oidcClient") || strings.Count(oauthSource, "h.HTTPClient") != 2 {
		t.Errorf("oauth.go: oidcClient %d, h.HTTPClient %d; want 0 and the 2 social calls",
			strings.Count(oauthSource, "oidcClient"), strings.Count(oauthSource, "h.HTTPClient"))
	}
}
