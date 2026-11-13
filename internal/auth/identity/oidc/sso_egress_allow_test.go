package oidc

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/externalurl"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity/social"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/proxycapture"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/redirectprobe"
)

const ssoEgressAllowEnv = "SSO_EGRESS_ALLOW_CIDRS"

type plainIdPHits struct{ token, jwks, userinfo atomic.Int32 }

// startPlainIdP is an OIDC provider over plain HTTP on 127.0.0.1 (the job-local IdP form), addressed by host, that
// counts the calls of each endpoint. Every body it writes is bounded.
func startPlainIdP(t *testing.T, host string, now time.Time) (string, *plainIdPHits) {
	t.Helper()
	signer := &unitIdP{key: unitKey(t)}
	hits := &plainIdPHits{}
	var idToken atomic.Value
	mux := http.NewServeMux()
	mux.HandleFunc("/jwks", func(w http.ResponseWriter, _ *http.Request) {
		hits.jwks.Add(1)
		_ = json.NewEncoder(w).Encode(map[string]any{"keys": []map[string]any{{
			"kty": "RSA", "kid": "k1", "use": "sig", "alg": "RS256",
			"n": base64.RawURLEncoding.EncodeToString(signer.key.N.Bytes()),
			"e": base64.RawURLEncoding.EncodeToString(big.NewInt(int64(signer.key.E)).Bytes()),
		}}})
	})
	mux.HandleFunc("/token", func(w http.ResponseWriter, _ *http.Request) {
		hits.token.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "at-1", "token_type": "Bearer", "expires_in": 300,
			"id_token": idToken.Load()})
	})
	mux.HandleFunc("/userinfo", func(w http.ResponseWriter, _ *http.Request) {
		hits.userinfo.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"sub":"sub-1"}`))
	})
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	base := strings.Replace(server.URL, "127.0.0.1", host, 1)
	idToken.Store(signer.sign(t, map[string]any{"alg": "RS256", "kid": "k1", "typ": "JWT"}, map[string]any{
		"iss": base, "aud": unitClient, "sub": "sub-1", "nonce": unitNonce, "iat": now.Unix(),
		"exp": now.Add(5 * time.Minute).Unix(), "email": "u@example.test", "email_verified": true}))
	return base, hits
}

// newDefaultClientService is a Service as production builds it: no HTTPClient, so New installs the default clients.
func newDefaultClientService(t *testing.T, now time.Time, endpoints *OAuthEndpoints) *Service {
	t.Helper()
	var secret contracts.SecretString
	raw, _ := json.Marshal(uuid.NewString())
	if err := json.Unmarshal(raw, &secret); err != nil {
		t.Fatal(err)
	}
	svc, err := New(Config{
		Providers: &fakeProviders{}, Flows: &scriptedFlows{}, Identities: &orderIdentities{linked: true},
		Memberships: resolveMembers{found: true, role: "member"}, Entitlements: &countingEntitlements{allowed: true},
		Sessions: &resolveSessions{}, SignInRecord: noSignInRecord{}, StateSecret: secret, AppBaseURL: "https://app.example",
		OAuthEndpoints: endpoints, Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	return svc
}

func plainIdPRow(base, jwksURI string) Provider {
	return Provider{ID: uuid.New(), OrgID: uuid.New(), Protocol: "oidc", AutoProvision: true, Config: ProviderConfig{
		ClientID: unitClient, Issuer: base, TokenEndpoint: base + "/token", JWKSURI: jwksURI, UserinfoEndpoint: base + "/userinfo"}}
}

// With SSO_EGRESS_ALLOW_CIDRS naming the job-local IdP's address, the OIDC calls of the default client (token
// exchange, JWKS, userinfo) reach it, by address and by the name localhost (the job's form: localhost also resolves
// to ::1, which stays refused, and the dial goes on to 127.0.0.1).
func TestSSOEgressAllowLetsTheOIDCCallsReachAListedIdP(t *testing.T) {
	t.Setenv(ssoEgressAllowEnv, "127.0.0.1/32")
	for _, host := range []string{"127.0.0.1", "localhost"} {
		t.Run(host, func(t *testing.T) {
			now := time.Unix(1_800_000_000, 0)
			base, hits := startPlainIdP(t, host, now)
			svc := newDefaultClientService(t, now, nil)
			_, failure := svc.exchangeAndValidate(context.Background(), plainIdPRow(base, base+"/jwks"),
				flowState{Nonce: unitNonce, RedirectURI: "https://app.example/cb"}, "code-1")
			if failure != nil {
				t.Fatalf("the OIDC calls to a listed IdP were refused: reason %s status %d", failure.Reason, failure.Status)
			}
			if hits.token.Load() != 1 || hits.jwks.Load() < 1 || hits.userinfo.Load() != 1 {
				t.Fatalf("IdP hits token=%d jwks=%d userinfo=%d, want each reached", hits.token.Load(), hits.jwks.Load(), hits.userinfo.Load())
			}
		})
	}
}

// The OIDC client that reaches a listed IdP keeps today's redirect policy: the JWKS read (go-oidc uses the client as
// it is, with no NoRedirects wrapper) does not follow a redirect to another origin.
func TestSSOEgressAllowKeepsTheOIDCClientsNoRedirectPolicy(t *testing.T) {
	t.Setenv(ssoEgressAllowEnv, "127.0.0.1/32")
	now := time.Unix(1_800_000_000, 0)
	base, _ := startPlainIdP(t, "127.0.0.1", now)
	probe := redirectprobe.New(t)
	svc := newDefaultClientService(t, now, nil)
	_, failure := svc.exchangeAndValidate(context.Background(), plainIdPRow(base, probe.Base.URL+"/jwks"),
		flowState{Nonce: unitNonce, RedirectURI: "https://app.example/cb"}, "code-1")
	if failure == nil {
		t.Fatal("the ID token verified over a redirected JWKS read")
	}
	probe.Assert(t)
}

// The social OAuth calls of the same Service are not widened by the SSO list: with it set to 127.0.0.1/32, a GitLab
// token endpoint on 127.0.0.1 is still refused before connect (dependency_unavailable, no request reaches it).
func TestSSOEgressAllowNeverReachesTheSocialOAuthCalls(t *testing.T) {
	t.Setenv(ssoEgressAllowEnv, "127.0.0.1/32")
	var hits atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"access_token":"at-1","token_type":"bearer"}`))
	}))
	t.Cleanup(server.Close)
	now := time.Unix(1_800_000_000, 0)
	svc := newDefaultClientService(t, now, &OAuthEndpoints{Social: social.Endpoints{GitLabBase: server.URL}})
	row := StoredProvider{ID: uuid.New(), OrgID: uuid.New(), Protocol: "oauth_gitlab", Status: "active", AutoProvision: true,
		Config: `{"client_id":"client-1"}`}
	secrets := `{"client_secret":"gitlab-test-only"}`
	row.EncryptedSecrets = &secrets
	token, state, err := sealOAuthState(svc.cfg.StateSecret.Reveal(), oauthFlowState{ProviderID: row.ID.String(),
		OrgID: row.OrgID.String(), RedirectURI: "https://app.example/cb", NonceHash: hashLoginNonce("cookie-1")}, now)
	if err != nil {
		t.Fatal(err)
	}
	cookieName := oauthLoginNonceCookieName(state.ID)
	_, _, failure := svc.callbackOAuth(context.Background(), row, CallbackInput{Code: "c", State: token,
		Cookie: func(name string) (string, bool) { return "cookie-1", name == cookieName }})
	if failure == nil || failure.Reason != ReasonDependency {
		t.Fatalf("social OAuth to 127.0.0.1 with the SSO list set: failure %+v, want dependency_unavailable", failure)
	}
	if hits.Load() != 0 {
		t.Fatalf("the social token endpoint on 127.0.0.1 was reached %d time(s)", hits.Load())
	}
}

// C-a: the OIDC client New installs keeps today's posture (30 s timeout, no redirect, the GuardedTransport TLS and
// connection settings, no proxy); only its dial check differs, and it is not the social client.
func TestTheOIDCClientKeepsTodaysTimeoutTLSAndRedirectPolicy(t *testing.T) {
	svc := newDefaultClientService(t, time.Unix(1_800_000_000, 0), nil)
	assertIdPClientPosture(t, svc.oidcClient, svc.cfg.HTTPClient)
}

func assertIdPClientPosture(t *testing.T, client, social *http.Client) {
	t.Helper()
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
		t.Fatalf("the OIDC transport differs from GuardedTransport beyond its dial check")
	}
}

func TestTheDefaultOIDCCallClientRefusesRedirectsOnItsOwn(t *testing.T) {
	client := defaultIdPClient()
	if client.CheckRedirect == nil || client.CheckRedirect(nil, nil) != http.ErrUseLastResponse {
		t.Fatal("the default OIDC call client follows redirects")
	}
}

func TestTheDefaultOIDCCallClientNeverDialsTheEnvironmentProxy(t *testing.T) {
	proxycapture.Check(t, runCaptureChild, func(ctx context.Context) {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, proxycapture.Target, nil)
		if err != nil {
			panic(err)
		}
		request.Header.Set("Authorization", "Bearer "+proxycapture.Canary)
		if response, err := defaultIdPClient().Do(request); err == nil {
			_ = response.Body.Close()
		}
	}).AssertNothingReached(t)
}

// Every OIDC call of the Service goes through the OIDC client and no social OAuth call does: service.go names no
// s.cfg.HTTPClient, oauth_flow.go names no oidcClient.
func TestOIDCAndSocialCallsUseTheirOwnClients(t *testing.T) {
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatal(err)
	}
	read := func(name string) string {
		raw, err := os.ReadFile(filepath.Join(root, "internal", "auth", "identity", "oidc", name))
		if err != nil {
			t.Fatal(err)
		}
		return string(raw)
	}
	service, oauthFlow := read("service.go"), read("oauth_flow.go")
	if strings.Contains(service, "s.cfg.HTTPClient") || strings.Count(service, "s.oidcClient") != 4 {
		t.Errorf("service.go: s.cfg.HTTPClient %d, s.oidcClient %d; want 0 and the 4 OIDC calls",
			strings.Count(service, "s.cfg.HTTPClient"), strings.Count(service, "s.oidcClient"))
	}
	if strings.Contains(oauthFlow, "oidcClient") || strings.Count(oauthFlow, "s.cfg.HTTPClient") != 2 {
		t.Errorf("oauth_flow.go: oidcClient %d, s.cfg.HTTPClient %d; want 0 and the 2 social calls",
			strings.Count(oauthFlow, "oidcClient"), strings.Count(oauthFlow, "s.cfg.HTTPClient"))
	}
}
