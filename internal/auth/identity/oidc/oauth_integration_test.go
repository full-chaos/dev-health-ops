//go:build integration

package oidc

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
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

func TestOAuthPKCEIsSentAndCheckedForEveryProvider(t *testing.T) {
	for _, protocol := range []string{"oauth_github", "oauth_gitlab", "oauth_google"} {
		t.Run(protocol, func(t *testing.T) {
			h := newHarness(t, harnessOpts{signup: true})
			provider, client := h.seedOAuthProvider(t, h.orgID, protocol, nil)
			s := h.startOAuth(t, provider)
			if s.challenge == "" || s.query.Get("code_challenge_method") != "S256" {
				t.Fatalf("authorize carried no S256 challenge: %v", s.query)
			}
			if _, failure := h.finishOAuth(t, provider, client, s, fakeAccountWith(uniqueEmail("pkce"), true), true); failure != nil {
				t.Fatalf("callback: %v", failure)
			}
			// A verifier that does not match the challenge is refused at the token endpoint.
			before := h.snapshot(t)
			bad := h.startOAuth(t, provider)
			bad.challenge = pkceChallenge("another-verifier")
			_, failure := h.finishOAuth(t, provider, client, bad, fakeAccountWith(uniqueEmail("pkce"), true), true)
			wantDenied(t, failure, http.StatusBadRequest, ReasonDependency)
			wantUnchanged(t, h, before)
		})
	}
}

func TestOAuthInsecureBaseURLIsRefused(t *testing.T) {
	h := newHarness(t, harnessOpts{signup: true})
	provider, _ := h.seedOAuthProvider(t, h.orgID, "oauth_gitlab", map[string]any{"base_url": "http://gitlab.example"})
	before, hits := h.snapshot(t), h.idp.oauth.hits
	_, err := h.svc.AuthorizeOAuth(context.Background(), provider)
	if !errors.Is(err, ErrProviderConfig) {
		t.Fatalf("authorize with an http base_url = %v, want ErrProviderConfig", err)
	}
	_, _, err = h.svc.CallbackOAuth(context.Background(), provider, CallbackInput{Code: "c", State: "s"})
	if !errors.Is(err, ErrProviderConfig) {
		t.Fatalf("callback with an http base_url = %v, want ErrProviderConfig", err)
	}
	wantUnchanged(t, h, before)
	if h.idp.oauth.hits != hits {
		t.Fatal("a row with an http base_url reached the provider")
	}
	loopback, client := h.seedOAuthProvider(t, h.orgID, "oauth_gitlab", map[string]any{"base_url": "http://localhost:1"})
	if _, err := h.svc.AuthorizeOAuth(context.Background(), loopback); err != nil {
		t.Fatalf("authorize with an http loopback base_url: %v (client %s)", err, client)
	}
}

func TestOAuthUnverifiedEmailNeverLinksOrProvisions(t *testing.T) {
	t.Run("new account", func(t *testing.T) {
		h := newHarness(t, harnessOpts{signup: true})
		provider, client := h.seedOAuthProvider(t, h.orgID, "oauth_gitlab", nil)
		before := h.snapshot(t)
		_, failure := h.finishOAuth(t, provider, client, h.startOAuth(t, provider), fakeAccountWith(uniqueEmail("unverified"), false), true)
		wantDenied(t, failure, http.StatusForbidden, ReasonEmailUnverified)
		wantUnchanged(t, h, before)
	})
	t.Run("existing go-api account of that subject", func(t *testing.T) {
		h := newHarness(t, harnessOpts{signup: false})
		provider, client := h.seedOAuthProvider(t, h.orgID, "oauth_gitlab", nil)
		account := fakeAccountWith(uniqueEmail("legacy"), false)
		user := h.seedPublicUser(t, account.Email, "gitlab", account.ID)
		h.seedMembership(t, user, h.orgID, "member")
		before := h.snapshot(t)
		_, failure := h.finishOAuth(t, provider, client, h.startOAuth(t, provider), account, true)
		wantDenied(t, failure, http.StatusForbidden, ReasonEmailUnverified)
		wantUnchanged(t, h, before)
	})
	t.Run("github without a verified address", func(t *testing.T) {
		h := newHarness(t, harnessOpts{signup: true})
		provider, client := h.seedOAuthProvider(t, h.orgID, "oauth_github", nil)
		before := h.snapshot(t)
		_, failure := h.finishOAuth(t, provider, client, h.startOAuth(t, provider), fakeAccountWith(uniqueEmail("gh"), false), true)
		wantDenied(t, failure, http.StatusForbidden, ReasonEmailUnverified)
		wantUnchanged(t, h, before)
	})
}

func TestOIDCStateIsRefusedByTheOAuthCallback(t *testing.T) {
	h := newHarness(t, harnessOpts{signup: true})
	provider, _ := h.seedOAuthProvider(t, h.orgID, "oauth_gitlab", nil)
	oidcState, sealed, err := sealState(h.svc.cfg.StateSecret.Reveal(), flowState{ProviderID: provider.String(), OrgID: h.orgID.String(),
		Nonce: "n", CodeVerifier: "v", RedirectURI: "https://app.example/cb", NonceHash: hashLoginNonce("cookie")}, h.now)
	if err != nil {
		t.Fatal(err)
	}
	before := h.snapshot(t)
	_, _, err = h.svc.CallbackOAuth(context.Background(), provider, CallbackInput{Code: "c", State: oidcState,
		Cookie: func(name string) (string, bool) { return "cookie", name == oauthLoginNonceCookieName(sealed.ID) }})
	var failure *Error
	if !errors.As(err, &failure) || failure.Status != http.StatusBadRequest || failure.Reason != ReasonMalformed {
		t.Fatalf("an OIDC state at the OAuth callback = %v, want 400 malformed", err)
	}
	wantUnchanged(t, h, before)
}

func TestOAuthCallbackRefusesReplayedState(t *testing.T) {
	h := newHarness(t, harnessOpts{signup: true})
	provider, client := h.seedOAuthProvider(t, h.orgID, "oauth_gitlab", nil)
	s := h.startOAuth(t, provider)
	account := fakeAccountWith(uniqueEmail("replay"), true)
	if _, failure := h.finishOAuth(t, provider, client, s, account, true); failure != nil {
		t.Fatalf("first callback: %v", failure)
	}
	before, hits := h.snapshot(t), h.idp.oauth.hits
	_, failure := h.finishOAuth(t, provider, client, s, account, true)
	wantDenied(t, failure, http.StatusBadRequest, ReasonReplayed)
	wantUnchanged(t, h, before)
	if h.idp.oauth.hits != hits {
		t.Fatal("a replayed state reached the provider")
	}
}

func TestOAuthCallbackRefusesStateForAnotherProvider(t *testing.T) {
	h := newHarness(t, harnessOpts{signup: true})
	provider, client := h.seedOAuthProvider(t, h.orgID, "oauth_gitlab", nil)
	other, _ := h.seedOAuthProvider(t, h.orgID, "oauth_gitlab", nil)
	before := h.snapshot(t)
	_, failure := h.finishOAuth(t, other, client, h.startOAuth(t, provider), fakeAccountWith(uniqueEmail("x"), true), true)
	wantDenied(t, failure, http.StatusBadRequest, ReasonMalformed)
	wantUnchanged(t, h, before)
}

func TestOAuthCallbackRefusesMissingLoginNonceCookie(t *testing.T) {
	h := newHarness(t, harnessOpts{signup: true})
	provider, client := h.seedOAuthProvider(t, h.orgID, "oauth_gitlab", nil)
	s := h.startOAuth(t, provider)
	before := h.snapshot(t)
	_, failure := h.finishOAuth(t, provider, client, s, fakeAccountWith(uniqueEmail("x"), true), false)
	wantDenied(t, failure, http.StatusBadRequest, ReasonMalformed)
	wantUnchanged(t, h, before)
	if _, failure := h.finishOAuth(t, provider, client, s, fakeAccountWith(uniqueEmail("x"), true), true); failure != nil {
		t.Fatalf("flow owner after a cookieless attempt: %v", failure)
	}
}

func TestOAuthTokenOfAnotherClientIsRefused(t *testing.T) {
	for _, protocol := range []string{"oauth_github", "oauth_gitlab", "oauth_google"} {
		t.Run(protocol, func(t *testing.T) {
			h := newHarness(t, harnessOpts{signup: true})
			provider, client := h.seedOAuthProvider(t, h.orgID, protocol, nil)
			h.idp.oauth.tokenClient = "client-of-another-app"
			before := h.snapshot(t)
			_, failure := h.finishOAuth(t, provider, client, h.startOAuth(t, provider), fakeAccountWith(uniqueEmail("aud"), true), true)
			wantDenied(t, failure, http.StatusBadRequest, ReasonAudience)
			wantUnchanged(t, h, before)
		})
	}
}

func TestOAuthEmailCollisionIsAConflict(t *testing.T) {
	h := newHarness(t, harnessOpts{signup: false})
	provider, client := h.seedOAuthProvider(t, h.orgID, "oauth_google", nil)
	email := uniqueEmail("holder")
	h.seedPublicUser(t, email, "local", "")
	before := h.snapshot(t)
	_, failure := h.finishOAuth(t, provider, client, h.startOAuth(t, provider), fakeAccountWith(email, true), true)
	wantDenied(t, failure, http.StatusConflict, ReasonEmailCollision)
	wantUnchanged(t, h, before)
}

func TestOAuthSubjectIsScopedByProviderRow(t *testing.T) {
	h := newHarness(t, harnessOpts{signup: true})
	first, client := h.seedOAuthProvider(t, h.orgID, "oauth_gitlab", nil)
	account := fakeAccountWith(uniqueEmail("owner"), true)
	owner, failure := h.finishOAuth(t, first, client, h.startOAuth(t, first), account, true)
	if failure != nil {
		t.Fatalf("first row: %v", failure)
	}
	second, client2 := h.seedOAuthProvider(t, h.seedOrg(t), "oauth_gitlab", nil)
	h.ids.signup = false
	before := h.snapshot(t)
	account.Email = uniqueEmail("asserted")
	login, failure := h.finishOAuth(t, second, client2, h.startOAuth(t, second), account, true)
	if failure == nil && login.PrincipalID == owner.PrincipalID {
		t.Fatal("the same provider subject through another row signed in as the first row's account")
	}
	wantDenied(t, failure, http.StatusForbidden, ReasonSignupUnavailable)
	wantUnchanged(t, h, before)
}

func TestOAuthSelfHostedGitLabIssuerIsTheBaseURL(t *testing.T) {
	h := newHarness(t, harnessOpts{signup: true})
	base := h.idp.server.URL + "/gitlab/selfhosted"
	provider, client := h.seedOAuthProvider(t, h.orgID, "oauth_gitlab", map[string]any{"base_url": base})
	s := h.startOAuth(t, provider)
	account := fakeAccountWith(uniqueEmail("self"), true)
	login, failure := h.finishOAuth(t, provider, client, s, account, true)
	if failure != nil {
		t.Fatalf("callback: %v", failure)
	}
	if n := h.count(t, `SELECT count(*) FROM auth.external_identities WHERE principal_id = $1 AND issuer = $2 AND provider = $3 AND subject = $4`,
		login.PrincipalID, base, provider.String(), account.ID); n != 1 {
		t.Fatalf("identity rows with the self-hosted issuer = %d", n)
	}
}

func TestOAuthExistingSSOAccountSignsInBySubject(t *testing.T) {
	h := newHarness(t, harnessOpts{signup: false})
	provider, client := h.seedOAuthProvider(t, h.orgID, "oauth_gitlab", nil)
	account := fakeAccountWith(uniqueEmail("legacy"), true)
	user := h.seedPublicUser(t, account.Email, "gitlab", account.ID)
	h.seedMembership(t, user, h.orgID, "admin")
	login, failure := h.finishOAuth(t, provider, client, h.startOAuth(t, provider), account, true)
	if failure != nil {
		t.Fatalf("callback: %v", failure)
	}
	if login.PrincipalID != user || login.Role != "admin" {
		t.Fatalf("login %+v, want user %s admin", login, user)
	}
	if n := h.count(t, `SELECT count(*) FROM auth.external_identities WHERE principal_id = $1 AND issuer = 'https://gitlab.com' AND provider = $2 AND subject = $3`,
		user, provider.String(), account.ID); n != 1 {
		t.Fatalf("projected identity rows = %d", n)
	}
}

func TestOAuthExistingAccountNeedsProviderOrgMembership(t *testing.T) {
	h := newHarness(t, harnessOpts{signup: false})
	provider, client := h.seedOAuthProvider(t, h.orgID, "oauth_gitlab", nil)
	account := fakeAccountWith(uniqueEmail("elsewhere"), true)
	user := h.seedPublicUser(t, uniqueEmail("stored"), "gitlab", account.ID)
	h.seedMembership(t, user, h.seedOrg(t), "owner")
	before := h.snapshot(t)
	_, failure := h.finishOAuth(t, provider, client, h.startOAuth(t, provider), account, true)
	wantDenied(t, failure, http.StatusForbidden, ReasonMembershipRequired)
	wantUnchanged(t, h, before)
}

func TestOAuthSubjectInTwoOrganizationsIsAConflict(t *testing.T) {
	h := newHarness(t, harnessOpts{signup: false})
	provider, client := h.seedOAuthProvider(t, h.orgID, "oauth_gitlab", nil)
	account := fakeAccountWith(uniqueEmail("two-orgs"), true)
	inOrg := h.seedPublicUser(t, account.Email, "gitlab", account.ID)
	h.seedMembership(t, inOrg, h.orgID, "admin")
	elsewhere := h.seedPublicUser(t, uniqueEmail("other-org"), "gitlab", account.ID)
	h.seedMembership(t, elsewhere, h.seedOrg(t), "member")
	before := h.snapshot(t)
	_, failure := h.finishOAuth(t, provider, client, h.startOAuth(t, provider), account, true)
	wantDenied(t, failure, http.StatusConflict, ReasonIdentityConflict)
	wantUnchanged(t, h, before)
}

func TestOAuthNewAccountFollowsTheSignupSwitch(t *testing.T) {
	t.Run("off", func(t *testing.T) {
		h := newHarness(t, harnessOpts{signup: false})
		provider, client := h.seedOAuthProvider(t, h.orgID, "oauth_google", nil)
		before := h.snapshot(t)
		_, failure := h.finishOAuth(t, provider, client, h.startOAuth(t, provider), fakeAccountWith(uniqueEmail("off"), true), true)
		wantDenied(t, failure, http.StatusForbidden, ReasonSignupUnavailable)
		wantUnchanged(t, h, before)
	})
	t.Run("on", func(t *testing.T) {
		h := newHarness(t, harnessOpts{signup: true})
		provider, client := h.seedOAuthProvider(t, h.orgID, "oauth_google", nil)
		account := fakeAccountWith(uniqueEmail("on"), true)
		login, failure := h.finishOAuth(t, provider, client, h.startOAuth(t, provider), account, true)
		if failure != nil {
			t.Fatalf("callback: %v", failure)
		}
		if n := h.count(t, `SELECT count(*) FROM auth.external_identities WHERE principal_id = $1 AND issuer = 'https://accounts.google.com' AND provider = $2 AND subject = $3`,
			login.PrincipalID, provider.String(), account.ID); n != 1 {
			t.Fatalf("provisioned identity rows = %d", n)
		}
		req := h.sessions.requests[len(h.sessions.requests)-1]
		if len(req.AMR) != 1 || req.AMR[0] != "fed" {
			t.Fatalf("session request %+v", req)
		}
	})
}

func TestOAuthLinkedIdentityAnswersWithTheStoredEmail(t *testing.T) {
	h := newHarness(t, harnessOpts{signup: false})
	provider, client := h.seedOAuthProvider(t, h.orgID, "oauth_gitlab", nil)
	account := fakeAccountWith(uniqueEmail("asserted"), true)
	stored := uniqueEmail("stored")
	owner := h.link(t, ExternalKey{Issuer: "https://gitlab.com", Provider: provider.String(), Subject: account.ID}, stored)
	login, failure := h.finishOAuth(t, provider, client, h.startOAuth(t, provider), account, true)
	if failure != nil {
		t.Fatalf("callback: %v", failure)
	}
	if login.PrincipalID != owner || login.Email != stored {
		t.Fatalf("login %s %q, want %s with the stored email %q", login.PrincipalID, login.Email, owner, stored)
	}
}

func TestOAuthEntitlementIsCheckedBeforeAnyDecode(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, harnessOpts{signup: true})
	unlicensedOrg := h.seedOrgTier(t, "community")
	malformed := h.seedRawProviderAs(t, unlicensedOrg, `[]`, "oauth_gitlab", "active")
	var failure *Error
	if _, err := h.svc.AuthorizeOAuth(ctx, malformed); !errors.As(err, &failure) || failure.Status != http.StatusPaymentRequired {
		t.Fatalf("authorize, non-entitled org with a malformed config = %v, want 402", err)
	}
	if _, _, err := h.svc.CallbackOAuth(ctx, malformed, CallbackInput{Code: "c", State: "s"}); !errors.As(err, &failure) || failure.Status != http.StatusPaymentRequired {
		t.Fatalf("callback, non-entitled org with a malformed config = %v, want 402", err)
	}
	if _, err := h.svc.AuthorizeOAuthByType(ctx, "gitlab", h.seedOrgTier(t, "community")); !errors.As(err, &failure) || failure.Status != http.StatusPaymentRequired {
		t.Fatalf("by type, non-entitled org without any row = %v, want 402 before the lookup", err)
	}
	licensed := h.seedRawProviderAs(t, h.orgID, `[]`, "oauth_gitlab", "active")
	if _, err := h.svc.AuthorizeOAuth(ctx, licensed); !errors.Is(err, ErrProviderConfig) {
		t.Fatalf("authorize, entitled org with a malformed config = %v, want ErrProviderConfig", err)
	}
}

func TestOAuthProviderChecksPrecedeConfigDecode(t *testing.T) {
	h := newHarness(t, harnessOpts{signup: true})
	for name, row := range map[string][2]string{"an OIDC row": {"oidc", "active"}, "inactive": {"oauth_gitlab", "inactive"}} {
		t.Run(name, func(t *testing.T) {
			provider := h.seedRawProviderAs(t, h.orgID, `[]`, row[0], row[1])
			var failure *Error
			if _, err := h.svc.AuthorizeOAuth(context.Background(), provider); !errors.As(err, &failure) || failure.Status != http.StatusBadRequest {
				t.Fatalf("authorize = %v, want 400 before the config is decoded", err)
			}
			if _, _, err := h.svc.CallbackOAuth(context.Background(), provider, CallbackInput{Code: "c", State: "s"}); !errors.As(err, &failure) || failure.Status != http.StatusBadRequest {
				t.Fatalf("callback = %v, want 400 before the config is decoded", err)
			}
		})
	}
}

func TestOAuthByTypePicksTheDefaultActiveProvider(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, harnessOpts{signup: true})
	org := h.seedOrg(t)
	if _, err := h.svc.AuthorizeOAuthByType(ctx, "gitlab", org); err == nil {
		t.Fatal("by type with no row answered")
	} else if failure := err.(*Error); failure.Status != http.StatusNotFound {
		t.Fatalf("by type with no row = %v, want 404", err)
	}
	_, plain := h.seedOAuthProvider(t, org, "oauth_gitlab", nil)
	preferred, preferredClient := h.seedOAuthProvider(t, org, "oauth_gitlab", nil)
	if _, err := h.env.admin.Exec(ctx, `UPDATE public.sso_providers SET is_default = true WHERE id = $1`, preferred); err != nil {
		t.Fatal(err)
	}
	result, err := h.svc.AuthorizeOAuthByType(ctx, "gitlab", org)
	if err != nil {
		t.Fatalf("by type: %v", err)
	}
	if !strings.Contains(result.AuthorizationURL, "client_id="+preferredClient) || strings.Contains(result.AuthorizationURL, plain) {
		t.Fatalf("by type picked another row: %s", result.AuthorizationURL)
	}
	if _, err := h.svc.AuthorizeOAuthByType(ctx, "bitbucket", org); err == nil || err.(*Error).Status != http.StatusBadRequest {
		t.Fatalf("an unknown type = %v, want 400", err)
	}
}

func TestOAuthOutcomesRecordedOnce(t *testing.T) {
	h := newHarness(t, harnessOpts{signup: true})
	reader := sdkmetric.NewManualReader()
	var logged bytes.Buffer
	cfg := h.svc.cfg
	cfg.Meter = sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test")
	cfg.Logger = slog.New(slog.NewTextHandler(&logged, nil))
	svc, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	h.svc = svc
	unlicensed, _ := h.seedOAuthProvider(t, h.seedOrgTier(t, "community"), "oauth_gitlab", nil)
	mux := http.NewServeMux()
	mux.Handle("POST /api/v1/auth/oauth/{provider_id}/authorize", svc.OAuthAuthorizeHandler())
	mux.Handle("POST /api/v1/auth/oauth/{provider_id}/callback", svc.OAuthCallbackHandler())
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	for _, route := range []string{"authorize", "callback"} {
		resp, err := http.Post(server.URL+"/api/v1/auth/oauth/"+unlicensed.String()+"/"+route, "application/json",
			strings.NewReader(`{"code":"c","state":"s"}`))
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusPaymentRequired {
			t.Fatalf("%s for a non-entitled organization = %d, want 402", route, resp.StatusCode)
		}
	}
	provider, client := h.seedOAuthProvider(t, h.orgID, "oauth_gitlab", nil)
	if _, failure := h.finishOAuth(t, provider, client, h.startOAuth(t, provider), fakeAccountWith(uniqueEmail("counted"), true), true); failure != nil {
		t.Fatalf("callback: %v", failure)
	}
	if _, err := svc.AuthorizeOAuthByType(context.Background(), "gitlab", h.orgID); err != nil {
		t.Fatalf("by type: %v", err)
	}
	counts := outcomeCounts(t, reader)
	for key, want := range map[string]int64{
		"oauth_authorize/denied/not_entitled": 1, "oauth_callback/denied/not_entitled": 1,
		"oauth_authorize/success/accepted": 1, "oauth_authorize_by_type/success/accepted": 1,
		"oauth_callback/success/accepted": 1,
	} {
		if counts[key] != want {
			t.Errorf("auth.oidc.outcomes %s = %d, want %d (all: %v)", key, counts[key], want, counts)
		}
	}
	if n := strings.Count(logged.String(), "reason=not_entitled"); n != 2 {
		t.Errorf("refusal log lines with reason=not_entitled = %d, want 2", n)
	}
	wantOneLogLinePerOutcome(t, logged.String(), counts)
}

func TestOAuthHandlersRoundTrip(t *testing.T) {
	h := newHarness(t, harnessOpts{signup: true})
	org := h.seedOrg(t)
	provider, client := h.seedOAuthProvider(t, org, "oauth_google", nil)
	mux := http.NewServeMux()
	mux.Handle("GET /api/v1/auth/oauth/{provider_type}/authorize", h.svc.OAuthAuthorizeByTypeHandler())
	mux.Handle("POST /api/v1/auth/oauth/{provider_id}/callback", h.svc.OAuthCallbackHandler())
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	resp, err := http.Get(server.URL + "/api/v1/auth/oauth/google/authorize?org_id=" + org.String())
	if err != nil {
		t.Fatal(err)
	}
	var authz map[string]string
	_ = json.NewDecoder(resp.Body).Decode(&authz)
	resp.Body.Close()
	cookies := resp.Cookies()
	if resp.StatusCode != http.StatusOK || len(cookies) != 1 || !cookies[0].Secure || !cookies[0].HttpOnly {
		t.Fatalf("authorize: %d %v %v", resp.StatusCode, authz, cookies)
	}
	parsed, _ := http.NewRequest(http.MethodGet, authz["authorization_url"], nil)
	q := parsed.URL.Query()
	if q.Get("access_type") != "offline" || q.Get("prompt") != "consent" || q.Get("code_challenge_method") != "S256" {
		t.Fatalf("Google authorize URL %s", authz["authorization_url"])
	}
	code := "code-" + uuid.NewString()
	h.idp.oauth.grant(code, fakeGrant{client: client, challenge: q.Get("code_challenge"), account: fakeAccountWith(uniqueEmail("http"), true)})
	body, _ := json.Marshal(map[string]string{"code": code, "state": authz["state"]})
	req, _ := http.NewRequest(http.MethodPost, server.URL+"/api/v1/auth/oauth/"+provider.String()+"/callback", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.AddCookie(&http.Cookie{Name: cookies[0].Name, Value: cookies[0].Value})
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var login map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&login)
	if resp.StatusCode != http.StatusOK || login["token_type"] != "bearer" || login["user_id"] == "" {
		t.Fatalf("callback: %d %v", resp.StatusCode, login)
	}
	for _, query := range []string{"", "?org_id=not-a-uuid"} {
		resp, err := http.Get(server.URL + "/api/v1/auth/oauth/google/authorize" + query)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusUnprocessableEntity {
			t.Fatalf("by type with org_id %q = %d, want 422", query, resp.StatusCode)
		}
	}
}

func TestOAuthCodeOfAnotherFlowIsRefused(t *testing.T) {
	for _, protocol := range []string{"oauth_github", "oauth_gitlab", "oauth_google"} {
		t.Run(protocol, func(t *testing.T) {
			h := newHarness(t, harnessOpts{signup: true})
			provider, client := h.seedOAuthProvider(t, h.orgID, protocol, nil)
			flowA := h.startOAuth(t, provider)
			flowB := h.startOAuth(t, provider)
			// Flow A's code (bound at the provider to flow A's authorize request)
			// presented with flow B's state and cookie.
			crossed := flowB
			crossed.challenge = flowA.challenge
			before := h.snapshot(t)
			_, failure := h.finishOAuth(t, provider, client, crossed, fakeAccountWith(uniqueEmail("crossed"), true), true)
			if failure == nil {
				t.Fatal("a code issued for another flow completed this flow")
			}
			wantDenied(t, failure, http.StatusBadRequest, ReasonDependency)
			wantUnchanged(t, h, before)
		})
	}
}

func TestOAuthGitHubBaseIsGitHubComOnly(t *testing.T) {
	h := newHarness(t, harnessOpts{signup: true})
	for base, ok := range map[string]bool{"https://ghe.example.com": false, "https://api.github.com": false,
		"http://localhost:8080": false, "https://github.com": true, "https://github.com/": true} {
		provider, _ := h.seedOAuthProvider(t, h.orgID, "oauth_github", map[string]any{"base_url": base})
		_, err := h.svc.AuthorizeOAuth(context.Background(), provider)
		if ok && err != nil {
			t.Errorf("base_url %q: %v, want accepted", base, err)
		}
		if !ok && !errors.Is(err, ErrProviderConfig) {
			t.Errorf("base_url %q: %v, want ErrProviderConfig", base, err)
		}
	}
}

func TestOAuthProviderRejectingPKCEIsARefusal(t *testing.T) {
	for _, protocol := range []string{"oauth_github", "oauth_gitlab", "oauth_google"} {
		t.Run(protocol, func(t *testing.T) {
			h := newHarness(t, harnessOpts{signup: true})
			var logged bytes.Buffer
			cfg := h.svc.cfg
			cfg.Logger = slog.New(slog.NewTextHandler(&logged, nil))
			svc, err := New(cfg)
			if err != nil {
				t.Fatal(err)
			}
			h.svc = svc
			provider, client := h.seedOAuthProvider(t, h.orgID, protocol, nil)
			s := h.startOAuth(t, provider)
			h.idp.oauth.mu.Lock()
			h.idp.oauth.rejectVerifier = true
			calls := h.idp.oauth.tokenCalls
			h.idp.oauth.mu.Unlock()
			before := h.snapshot(t)
			_, failure := h.finishOAuth(t, provider, client, s, fakeAccountWith(uniqueEmail("nopkce"), true), true)
			wantDenied(t, failure, http.StatusBadRequest, ReasonDependency)
			wantUnchanged(t, h, before)
			h.idp.oauth.mu.Lock()
			exchanges := h.idp.oauth.tokenCalls - calls
			h.idp.oauth.mu.Unlock()
			if exchanges != 1 {
				t.Fatalf("token endpoint calls = %d, want 1: no retry without PKCE", exchanges)
			}
			if !strings.Contains(logged.String(), "route=oauth_callback outcome=denied reason=dependency_unavailable") {
				t.Fatalf("no refusal log line with its reason:\n%s", logged.String())
			}
		})
	}
}
