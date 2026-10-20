package oidc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/google/uuid"
	"golang.org/x/oauth2"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity/social"
)

const (
	protocolOAuth       = "oauth"
	routeOAuthAuthorize = "oauth_authorize"
	routeOAuthCallback  = "oauth_callback"
)

// OAuthEndpoints are the public URLs of the OAuth providers. GitLab's
// authorize and token URLs hang off its base (Social.GitLabBase, or a
// self-hosted row's base_url).
type OAuthEndpoints struct {
	GitHubAuthorize, GitHubToken string
	GoogleAuthorize, GoogleToken string
	Social                       social.Endpoints
}

// DefaultOAuthEndpoints are go-api's defaults (internal/api/sso/oauth.go).
var DefaultOAuthEndpoints = OAuthEndpoints{
	GitHubAuthorize: "https://github.com/login/oauth/authorize",
	GitHubToken:     "https://github.com/login/oauth/access_token",
	GoogleAuthorize: "https://accounts.google.com/o/oauth2/v2/auth",
	GoogleToken:     "https://oauth2.googleapis.com/token",
	Social:          social.DefaultEndpoints,
}

// oauthProviderOf maps a stored protocol to its OAuth provider.
func oauthProviderOf(protocol string) (social.Provider, bool) {
	switch protocol {
	case "oauth_github":
		return social.GitHub, true
	case "oauth_gitlab":
		return social.GitLab, true
	case "oauth_google":
		return social.Google, true
	}
	return "", false
}

// oauthProtocolForType maps a URL provider type to its stored protocol.
func oauthProtocolForType(providerType string) (string, bool) {
	switch providerType {
	case "github", "gitlab", "google":
		return "oauth_" + providerType, true
	}
	return "", false
}

var oauthSpec = protocolSpec{
	accepts:  func(p string) bool { _, ok := oauthProviderOf(p); return ok },
	notFound: "OAuth provider not found", wrongProtocol: "Provider is not OAuth", inactive: "OAuth provider is not active",
}

// OAuthConfig is the OAuth part of sso_providers.config.
type OAuthConfig struct {
	ClientID, RedirectURI, BaseURL string
	Scopes                         []string
}

// decodeOAuthConfig is go-api decodeOAuthConfig: base_url and redirect_uri,
// when set, must be https or an http loopback address.
func decodeOAuthConfig(raw string) (OAuthConfig, error) {
	var object map[string]any
	if err := decodeJSONObject(raw, &object); err != nil {
		return OAuthConfig{}, err
	}
	cfg := OAuthConfig{ClientID: objString(object, "client_id"), RedirectURI: objString(object, "redirect_uri"),
		BaseURL: objString(object, "base_url"), Scopes: []string{}}
	if value, ok := object["scopes"]; ok && value != nil {
		items, ok := value.([]any)
		if !ok {
			return OAuthConfig{}, fmt.Errorf("%w: scopes", errProviderShape)
		}
		for _, item := range items {
			text, ok := item.(string)
			if !ok {
				return OAuthConfig{}, fmt.Errorf("%w: scopes item", errProviderShape)
			}
			cfg.Scopes = append(cfg.Scopes, text)
		}
	}
	for _, candidate := range []string{cfg.BaseURL, cfg.RedirectURI} {
		if candidate == "" {
			continue
		}
		if err := validateHTTPSOrLoopback(candidate); err != nil {
			return OAuthConfig{}, fmt.Errorf("base_url/redirect_uri %w", err)
		}
	}
	return cfg, nil
}

// oauthTarget is everything one OAuth row needs to reach its provider.
type oauthTarget struct {
	provider          social.Provider
	authorize, token  string
	issuer            string
	endpoints         social.Endpoints
	pkce              bool
	googleOfflineMode bool
}

func (s *Service) oauthTarget(row Provider) (oauthTarget, *Error) {
	p, ok := oauthProviderOf(row.Protocol)
	if !ok {
		return oauthTarget{}, deny(http.StatusBadRequest, ReasonMalformed, oauthSpec.wrongProtocol)
	}
	eps := DefaultOAuthEndpoints
	if s.cfg.OAuthEndpoints != nil {
		eps = *s.cfg.OAuthEndpoints
	}
	t := oauthTarget{provider: p, endpoints: eps.Social, issuer: social.SocialIssuer(p)}
	switch p {
	case social.GitHub:
		t.authorize, t.token = eps.GitHubAuthorize, eps.GitHubToken
	case social.Google:
		t.authorize, t.token, t.pkce, t.googleOfflineMode = eps.GoogleAuthorize, eps.GoogleToken, true, true
	case social.GitLab:
		base := strings.TrimRight(eps.Social.GitLabBase, "/")
		if row.OAuth.BaseURL != "" {
			base = strings.TrimRight(row.OAuth.BaseURL, "/")
			// A self-hosted instance is its own issuer.
			t.issuer = base
		}
		t.endpoints.GitLabBase = base
		t.authorize, t.token, t.pkce = base+"/oauth/authorize", base+"/oauth/token", true
	}
	return t, nil
}

func oauthCallbackPath(providerID uuid.UUID) string {
	return "/api/v1/auth/oauth/" + providerID.String() + "/callback"
}

func oauthLoginNonceCookieName(flowID string) string { return "dho_oauth_login_nonce_" + flowID }

// AuthorizeOAuth starts an OAuth flow for a provider row.
func (s *Service) AuthorizeOAuth(ctx context.Context, providerID uuid.UUID) (AuthorizeResult, error) {
	result, failure := s.authorizeOAuth(ctx, providerID)
	if failure != nil {
		s.refused(ctx, routeOAuthAuthorize, providerID, failure)
		return AuthorizeResult{}, failure
	}
	s.succeeded(ctx, routeOAuthAuthorize, providerID, nil)
	return result, nil
}

// AuthorizeOAuthByType starts an OAuth flow for an organization's active row
// of one provider type: the entitlement gate runs on the organization before
// any row is read.
func (s *Service) AuthorizeOAuthByType(ctx context.Context, providerType string, orgID uuid.UUID) (AuthorizeResult, error) {
	result, providerID, failure := s.authorizeOAuthByType(ctx, providerType, orgID)
	if failure != nil {
		s.refused(ctx, routeOAuthAuthorize, providerID, failure)
		return AuthorizeResult{}, failure
	}
	s.succeeded(ctx, routeOAuthAuthorize, providerID, nil)
	return result, nil
}

func (s *Service) authorizeOAuthByType(ctx context.Context, providerType string, orgID uuid.UUID) (AuthorizeResult, uuid.UUID, *Error) {
	protocol, ok := oauthProtocolForType(providerType)
	if !ok {
		return AuthorizeResult{}, uuid.Nil, deny(http.StatusBadRequest, ReasonMalformed, "Invalid OAuth provider type")
	}
	if failure := s.entitled(ctx, orgID); failure != nil {
		return AuthorizeResult{}, uuid.Nil, failure
	}
	head, err := s.cfg.Providers.DefaultProviderHead(ctx, orgID, protocol)
	if errors.Is(err, ErrProviderNotFound) {
		return AuthorizeResult{}, uuid.Nil, deny(http.StatusNotFound, ReasonMalformed,
			"No active "+providerType+" OAuth provider found for this organization")
	}
	if err != nil {
		return AuthorizeResult{}, uuid.Nil, internal(ReasonStoreUnavailable, err)
	}
	result, failure := s.authorizeOAuth(ctx, head.ID)
	return result, head.ID, failure
}

func (s *Service) authorizeOAuth(ctx context.Context, providerID uuid.UUID) (AuthorizeResult, *Error) {
	row, failure := s.loadProvider(ctx, providerID, oauthSpec)
	if failure != nil {
		return AuthorizeResult{}, failure
	}
	target, failure := s.oauthTarget(row)
	if failure != nil {
		return AuthorizeResult{}, failure
	}
	redirect := row.OAuth.RedirectURI
	if redirect == "" {
		redirect = strings.TrimRight(s.cfg.AppBaseURL, "/") + oauthCallbackPath(row.ID)
	}
	var verifier string
	var err error
	if target.pkce {
		if verifier, err = randomURLSafe(64); err != nil {
			return AuthorizeResult{}, internal(ReasonDependency, err)
		}
	}
	loginNonce, err := randomURLSafe(32)
	if err != nil {
		return AuthorizeResult{}, internal(ReasonDependency, err)
	}
	token, state, err := sealOAuthState(s.cfg.StateSecret.Reveal(), oauthFlowState{
		ProviderID: row.ID.String(), OrgID: row.OrgID.String(), CodeVerifier: verifier, RedirectURI: redirect,
		NonceHash: hashLoginNonce(loginNonce),
	}, s.cfg.Now())
	if err != nil {
		return AuthorizeResult{}, internal(ReasonDependency, err)
	}
	if err := s.cfg.Flows.Begin(ctx, Flow{Hash: flowHash(state.ID), Protocol: protocolOAuth, ProviderID: row.ID,
		ExpiresAt: time.Unix(state.ExpiresAt, 0)}); err != nil {
		return AuthorizeResult{}, internal(ReasonStoreUnavailable, err)
	}
	params := url.Values{
		"client_id": {row.OAuth.ClientID}, "redirect_uri": {redirect}, "scope": {strings.Join(row.OAuth.Scopes, " ")},
		"state": {token}, "response_type": {"code"},
	}
	if target.googleOfflineMode {
		params.Set("access_type", "offline")
		params.Set("prompt", "consent")
	}
	if verifier != "" {
		params.Set("code_challenge", pkceChallenge(verifier))
		params.Set("code_challenge_method", "S256")
	}
	cookie := &http.Cookie{
		Name: oauthLoginNonceCookieName(state.ID), Value: loginNonce, Path: oauthCallbackPath(row.ID),
		MaxAge: int(stateTTL / time.Second), HttpOnly: true, Secure: true, SameSite: http.SameSiteLaxMode,
	}
	return AuthorizeResult{AuthorizationURL: target.authorize + "?" + params.Encode(), State: token, Cookie: cookie}, nil
}

// CallbackOAuth completes an OAuth flow. The returned cookie, when non-nil,
// clears the flow's login-nonce cookie on success and failure alike.
func (s *Service) CallbackOAuth(ctx context.Context, providerID uuid.UUID, in CallbackInput) (Login, *http.Cookie, error) {
	login, clear, failure := s.callbackOAuth(ctx, providerID, in)
	if failure != nil {
		s.refused(ctx, routeOAuthCallback, providerID, failure)
		return Login{}, clear, failure
	}
	s.succeeded(ctx, routeOAuthCallback, providerID, &login.PrincipalID)
	return login, clear, nil
}

func oauthFailure(reason Reason, code string, cause error) *Error {
	return &Error{Status: http.StatusBadRequest, Reason: reason, err: cause,
		Detail: map[string]any{"message": "OAuth authentication failed", "reason": code}}
}

func (s *Service) callbackOAuth(ctx context.Context, providerID uuid.UUID, in CallbackInput) (Login, *http.Cookie, *Error) {
	row, failure := s.loadProvider(ctx, providerID, oauthSpec)
	if failure != nil {
		return Login{}, nil, failure
	}
	target, failure := s.oauthTarget(row)
	if failure != nil {
		return Login{}, nil, failure
	}
	if in.Code == "" {
		return Login{}, nil, oauthFailure(ReasonMalformed, "oauth_missing_code", errors.New("empty code"))
	}
	now := s.cfg.Now()
	state, err := openOAuthState(s.cfg.StateSecret.Reveal(), in.State, row.ID.String(), now)
	if errors.Is(err, errStateExpired) {
		return Login{}, nil, oauthFailure(ReasonExpired, "oauth_state_expired", err)
	}
	if err != nil || state.OrgID != row.OrgID.String() {
		return Login{}, nil, oauthFailure(ReasonMalformed, "oauth_state_invalid", errStateInvalid)
	}
	clear := &http.Cookie{Name: oauthLoginNonceCookieName(state.ID), Value: "", Path: oauthCallbackPath(row.ID), MaxAge: -1,
		HttpOnly: true, Secure: true, SameSite: http.SameSiteLaxMode}
	cookie := ""
	if in.Cookie != nil {
		cookie, _ = in.Cookie(oauthLoginNonceCookieName(state.ID))
	}
	if cookie == "" || !constantTimeEqual(hashLoginNonce(cookie), state.NonceHash) {
		return Login{}, clear, oauthFailure(ReasonMalformed, "oauth_state_invalid", errors.New("login nonce cookie missing or wrong"))
	}
	consumed, err := s.cfg.Flows.Consume(ctx, flowHash(state.ID), protocolOAuth, row.ID, now)
	if err != nil {
		return Login{}, clear, internal(ReasonStoreUnavailable, err)
	}
	if !consumed {
		return Login{}, clear, oauthFailure(ReasonReplayed, "oauth_state_replayed", errors.New("flow already used or unknown"))
	}

	secret := clientSecret(s.cfg.Secrets, row.EncryptedSecrets)
	token, err := exchangeCode(ctx, s.cfg.HTTPClient, oauth2.Config{
		ClientID: row.OAuth.ClientID, ClientSecret: secret, RedirectURL: state.RedirectURI,
		Endpoint: oauth2.Endpoint{AuthURL: target.authorize, TokenURL: target.token, AuthStyle: oauth2.AuthStyleInParams},
	}, in.Code, state.CodeVerifier)
	if err != nil || token.AccessToken == "" {
		return Login{}, clear, oauthFailure(ReasonDependency, "oauth_token_exchange_failed", err)
	}
	client := &social.ProviderClient{HTTP: s.cfg.HTTPClient, Endpoints: target.endpoints,
		Clients: map[social.Provider]social.ClientCredentials{target.provider: social.NewClientCredentials(row.OAuth.ClientID, secret)}}
	prof, err := client.Fetch(ctx, target.provider, secretString(token.AccessToken))
	if err != nil {
		reason := ReasonDependency
		var perr *social.ProviderError
		if errors.As(err, &perr) {
			reason = contractReason(string(perr.Reason))
		}
		return Login{}, clear, oauthFailure(reason, "oauth_userinfo_fetch_failed", err)
	}
	// An unverified provider email never links or provisions, so it is refused
	// before any lookup (go-api D2745).
	if !prof.EmailVerified {
		return Login{}, clear, deny(http.StatusForbidden, ReasonEmailUnverified,
			"No verified email address is available from this OAuth provider")
	}
	if !validSubject(prof.Subject) {
		return Login{}, clear, oauthFailure(ReasonMissingSub, "oauth_userinfo_fetch_failed", errors.New("provider subject invalid"))
	}
	login, failure := s.resolve(ctx, row, validated{
		key:                ExternalKey{Issuer: target.issuer, Provider: row.ID.String(), Subject: prof.Subject},
		profile:            profile{Email: prof.Email, EmailVerified: prof.EmailVerified, FullName: prof.FullName},
		legacyAuthProvider: string(target.provider),
	}, in.RequestID)
	if failure != nil {
		return Login{}, clear, failure
	}
	return login, clear, nil
}

// contractReason keeps a provider reason that is an outcome code of this
// package and maps anything else to dependency_unavailable.
func contractReason(code string) Reason {
	for _, r := range allReasons {
		if string(r) == code {
			return r
		}
	}
	return ReasonDependency
}

// secretString wraps a value in contracts.SecretString through its JSON
// decoder, the type's only constructor.
func secretString(value string) contracts.SecretString {
	var s contracts.SecretString
	raw, _ := json.Marshal(value)
	_ = s.UnmarshalJSON(raw)
	return s
}
