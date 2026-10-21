package oidc

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	gooidc "github.com/coreos/go-oidc/v3/oidc"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"golang.org/x/oauth2"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/requestid"
	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
)

// ProviderKind is the go-api users.auth_provider value of an OIDC account.
const ProviderKind = "oidc"

// AMR is the authentication method of a federated sign-in.
var AMR = []string{"fed"}

// ExternalKey is the identity an IdP asserts. For per-organization SSO the
// provider is the sso_providers row id, not a kind: two organizations may
// configure the same issuer with their own clients, and a subject consented
// through one row never resolves through another.
type ExternalKey struct {
	Issuer   string
	Provider string
	Subject  string
}

// Principal is the account an identity resolves to. Email is the address
// stored for the account, never an IdP claim.
type Principal struct {
	ID     uuid.UUID
	Email  string
	Active bool
}

// LegacyMatch scopes the fallback to accounts go-api signed in before the
// identity store existed: exactly one public.users row, across all
// organizations, with this auth_provider and the subject as auth_provider_id,
// and that account a member of MembershipOrg. No row is a miss.
type LegacyMatch struct {
	AuthProvider  string
	MembershipOrg uuid.UUID
}

// FederatedAccount is a new account created from an IdP sign-in. RequestID is
// the server-minted audit request id, nil when there is none.
type FederatedAccount struct {
	Key       ExternalKey
	Email     string
	FullName  string
	RequestID *string
}

// IdentityStore is the identity store as this flow uses it.
//
// IdentityBySubject looks the key up, then the legacy fallback; on a fallback
// hit the store links the key to that account under audit. A miss is
// ErrIdentityNotFound; more than one legacy match is ErrIdentityAmbiguous; one
// outside the provider organization is ErrNotOrgMember. EmailInUse normalizes the address and reports whether
// any account holds it; it is used only to refuse, never to select. A bad
// address is ErrInvalidEmail. ProvisionFederated creates the principal, its
// user and the identity under audit; while new accounts are off it returns
// ErrSignupNotAvailable, and a race with another account is
// ErrIdentityConflict.
type IdentityStore interface {
	IdentityBySubject(ctx context.Context, key ExternalKey, legacy LegacyMatch) (Principal, error)
	EmailInUse(ctx context.Context, email string) (bool, error)
	ProvisionFederated(ctx context.Context, account FederatedAccount) (Principal, error)
}

// MembershipReader reads a principal's role in an organization.
type MembershipReader interface {
	Role(ctx context.Context, principalID, orgID uuid.UUID) (role string, found bool, err error)
}

// MembershipJIT adds a new principal to the provider's organization. go-api
// is the membership writer until Wave 4, so the auth-service runtime leaves it
// nil and a new account signs in with no organization, as in go-api.
type MembershipJIT interface {
	Join(ctx context.Context, principalID, orgID uuid.UUID) (role string, err error)
}

// Entitlements decides whether an organization may use SSO.
type Entitlements interface {
	SSOAllowed(ctx context.Context, orgID uuid.UUID) (bool, error)
	// DeniedDetail is the 402 body detail.
	DeniedDetail() any
}

// SessionRequest asks for a session after a successful sign-in.
type SessionRequest struct {
	PrincipalID uuid.UUID
	OrgID       *uuid.UUID
	Role        string
	AMR         []string
}

// Tokens is what the caller receives.
type Tokens struct {
	AccessToken  string
	RefreshToken string
	ExpiresIn    int64
}

// SessionIssuer creates the session and mints its tokens.
type SessionIssuer interface {
	Issue(ctx context.Context, req SessionRequest) (Tokens, error)
}

// Config builds a Service.
type Config struct {
	Providers     ProviderSource
	Flows         FlowStore
	Identities    IdentityStore
	Memberships   MembershipReader
	MembershipJIT MembershipJIT
	Entitlements  Entitlements
	Sessions      SessionIssuer
	Secrets       SecretDecrypter
	// StateSecret derives the state key (HKDF); it must be non-empty.
	StateSecret contracts.SecretString
	// AppBaseURL builds the default redirect URI: <AppBaseURL>/oidc/<id>/callback.
	AppBaseURL string
	// HTTPClient reaches the IdP; nil means an SSRF-guarded client.
	HTTPClient *http.Client
	// OAuthEndpoints are the OAuth providers' public URLs; nil means
	// DefaultOAuthEndpoints. A self-hosted GitLab row's base_url overrides
	// the GitLab base.
	OAuthEndpoints *OAuthEndpoints
	Now            func() time.Time
	Logger         *slog.Logger
	Meter          metric.Meter
}

// Service runs the OIDC flow.
type Service struct {
	cfg      Config
	outcomes metric.Int64Counter
}

// New validates the configuration.
func New(cfg Config) (*Service, error) {
	switch {
	case cfg.Providers == nil, cfg.Flows == nil, cfg.Identities == nil, cfg.Memberships == nil,
		cfg.Entitlements == nil, cfg.Sessions == nil:
		return nil, errors.New("oidc: a required dependency is missing")
	case cfg.StateSecret.Reveal() == "":
		return nil, errors.New("oidc: no state secret")
	}
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = defaultHTTPClient()
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	meter := cfg.Meter
	if meter == nil {
		meter = otel.Meter("github.com/full-chaos/dev-health-ops/internal/auth/identity/oidc")
	}
	counter, err := meter.Int64Counter("auth.oidc.outcomes",
		metric.WithDescription("OIDC SSO authorize and callback outcomes by route, outcome and reason code."))
	if err != nil {
		return nil, err
	}
	return &Service{cfg: cfg, outcomes: counter}, nil
}

// AuthorizeResult is the authorize response and the browser-binding cookie.
type AuthorizeResult struct {
	AuthorizationURL string
	State            string
	Cookie           *http.Cookie
}

func callbackPath(providerID uuid.UUID) string {
	return "/api/v1/auth/oidc/" + providerID.String() + "/callback"
}

func loginNonceCookieName(flowID string) string { return "dho_oidc_login_nonce_" + flowID }

// admit reads the provider row once and runs the provider checks on it in
// go-api's order, before the stored config or any request body is decoded:
// 404 for an unknown provider, 402 for an organization without the sso_saml
// entitlement, 400 for another protocol or an inactive provider. The flow
// then decodes this same row, never a second read.
func (s *Service) admit(ctx context.Context, providerID uuid.UUID, spec protocolSpec) (StoredProvider, *Error) {
	row, err := s.cfg.Providers.ProviderRow(ctx, providerID)
	if errors.Is(err, ErrProviderNotFound) {
		return StoredProvider{}, deny(http.StatusNotFound, ReasonMalformed, spec.notFound)
	}
	if err != nil {
		return StoredProvider{}, internal(ReasonStoreUnavailable, err)
	}
	if failure := s.entitled(ctx, row.OrgID); failure != nil {
		return StoredProvider{}, failure
	}
	if failure := spec.check(row); failure != nil {
		return StoredProvider{}, failure
	}
	return row, nil
}

// decodeAdmitted decodes an admitted row; a stored column the flow cannot
// read is a 500.
func decodeAdmitted(stored StoredProvider) (Provider, *Error) {
	row, err := decodeProvider(stored)
	if err != nil {
		return Provider{}, internal(ReasonDependency, err)
	}
	return row, nil
}

// entitled is the sso_saml entitlement gate for an organization (402).
func (s *Service) entitled(ctx context.Context, orgID uuid.UUID) *Error {
	allowed, err := s.cfg.Entitlements.SSOAllowed(ctx, orgID)
	if err != nil {
		s.cfg.Logger.ErrorContext(ctx, "auth oidc: entitlement check failed; denying", "org_id", orgID.String(),
			logging.ErrorAttr(err))
	}
	if !allowed {
		return deny(http.StatusPaymentRequired, ReasonNotEntitled, s.cfg.Entitlements.DeniedDetail())
	}
	return nil
}

// protocolSpec is what differs between the OIDC and the OAuth routes in the
// shared provider checks.
type protocolSpec struct {
	accepts                           func(protocol string) bool
	notFound, wrongProtocol, inactive string
}

var oidcSpec = protocolSpec{
	accepts:  func(p string) bool { return p == protocolOIDC },
	notFound: detailProviderAbsent, wrongProtocol: "Provider is not OIDC", inactive: "SSO provider is not active",
}

// check is the protocol and status part of the provider checks.
func (spec protocolSpec) check(row StoredProvider) *Error {
	if !spec.accepts(row.Protocol) {
		return deny(http.StatusBadRequest, ReasonMalformed, spec.wrongProtocol)
	}
	if row.Status != "active" {
		return deny(http.StatusBadRequest, ReasonMalformed, spec.inactive)
	}
	return nil
}

// Authorize starts a flow: a sealed single-use state, PKCE S256 always, and a
// per-flow login-nonce cookie that binds the state to this browser.
func (s *Service) Authorize(ctx context.Context, providerID uuid.UUID, redirectURI string) (AuthorizeResult, error) {
	row, failure := s.admit(ctx, providerID, oidcSpec)
	if failure != nil {
		s.refused(ctx, routeAuthorize, providerID, failure)
		return AuthorizeResult{}, failure
	}
	return s.authorizeAdmitted(ctx, row, redirectURI)
}

// authorizeAdmitted is Authorize for a row admit already checked.
func (s *Service) authorizeAdmitted(ctx context.Context, admitted StoredProvider, redirectURI string) (AuthorizeResult, error) {
	result, failure := s.authorize(ctx, admitted, redirectURI)
	if failure != nil {
		s.refused(ctx, routeAuthorize, admitted.ID, failure)
		return AuthorizeResult{}, failure
	}
	s.succeeded(ctx, routeAuthorize, admitted.ID, nil)
	return result, nil
}

func (s *Service) authorize(ctx context.Context, admitted StoredProvider, redirectURI string) (AuthorizeResult, *Error) {
	row, failure := decodeAdmitted(admitted)
	if failure != nil {
		return AuthorizeResult{}, failure
	}
	redirect := strings.TrimRight(s.cfg.AppBaseURL, "/") + "/oidc/" + row.ID.String() + "/callback"
	if redirectURI != "" {
		if err := validateHTTPSOrLoopback(redirectURI); err != nil {
			return AuthorizeResult{}, deny(http.StatusBadRequest, ReasonMalformed, "redirect_uri "+err.Error())
		}
		redirect = redirectURI
	}
	nonce, err := randomURLSafe(32)
	if err != nil {
		return AuthorizeResult{}, internal(ReasonDependency, err)
	}
	verifier, err := randomURLSafe(64)
	if err != nil {
		return AuthorizeResult{}, internal(ReasonDependency, err)
	}
	loginNonce, err := randomURLSafe(32)
	if err != nil {
		return AuthorizeResult{}, internal(ReasonDependency, err)
	}
	now := s.cfg.Now()
	token, state, err := sealState(s.cfg.StateSecret.Reveal(), flowState{
		ProviderID: row.ID.String(), OrgID: row.OrgID.String(), Nonce: nonce, CodeVerifier: verifier,
		RedirectURI: redirect, NonceHash: hashLoginNonce(loginNonce),
	}, now)
	if err != nil {
		return AuthorizeResult{}, internal(ReasonDependency, err)
	}
	if err := s.cfg.Flows.Begin(ctx, Flow{Hash: flowHash(state.ID), Protocol: protocolOIDC, ProviderID: row.ID,
		ExpiresAt: time.Unix(state.ExpiresAt, 0)}); err != nil {
		return AuthorizeResult{}, internal(ReasonStoreUnavailable, err)
	}
	params := url.Values{
		"response_type": {"code"}, "client_id": {row.Config.ClientID}, "redirect_uri": {redirect},
		"scope": {strings.Join(row.Config.Scopes, " ")}, "state": {token}, "nonce": {nonce},
		"code_challenge": {pkceChallenge(verifier)}, "code_challenge_method": {"S256"},
	}
	endpoint := row.Config.AuthorizationEndpoint
	if endpoint == "" {
		endpoint = row.Config.Issuer + "/authorize"
	}
	cookie := &http.Cookie{
		Name: loginNonceCookieName(state.ID), Value: loginNonce, Path: callbackPath(row.ID),
		MaxAge: int(stateTTL / time.Second), HttpOnly: true, Secure: true, SameSite: http.SameSiteLaxMode,
	}
	return AuthorizeResult{AuthorizationURL: endpoint + "?" + params.Encode(), State: token, Cookie: cookie}, nil
}

// CallbackInput is the callback request. RequestID is the server-minted audit
// request id (never the client's X-Request-ID); nil when there is none.
type CallbackInput struct {
	Code      string
	State     string
	RequestID *string
	// Cookie returns the request cookie with this name.
	Cookie func(name string) (string, bool)
}

// Login is a successful callback. Every field comes from stored state: the
// account, its stored email and its membership.
type Login struct {
	PrincipalID uuid.UUID
	Email       string
	OrgID       *uuid.UUID
	Role        string
	Tokens      Tokens
}

// Callback completes a flow. The returned cookie, when non-nil, clears the
// flow's login-nonce cookie and must be sent on success and failure alike.
func (s *Service) Callback(ctx context.Context, providerID uuid.UUID, in CallbackInput) (Login, *http.Cookie, error) {
	row, failure := s.admit(ctx, providerID, oidcSpec)
	if failure != nil {
		s.refused(ctx, routeCallback, providerID, failure)
		return Login{}, nil, failure
	}
	return s.callbackAdmitted(ctx, row, in)
}

// callbackAdmitted is Callback for a row admit already checked.
func (s *Service) callbackAdmitted(ctx context.Context, admitted StoredProvider, in CallbackInput) (Login, *http.Cookie, error) {
	login, clear, failure := s.callback(ctx, admitted, in)
	if failure != nil {
		s.refused(ctx, routeCallback, admitted.ID, failure)
		return Login{}, clear, failure
	}
	s.succeeded(ctx, routeCallback, admitted.ID, &login.PrincipalID)
	return login, clear, nil
}

const (
	routeAuthorize = "authorize"
	routeCallback  = "callback"
	outcomeSuccess = "success"
)

// record counts one outcome. Only succeeded and refused call it, so every
// outcome of every route is counted and logged exactly once. It marks the
// request's outcomeMark, so a panic after this point is not counted again.
func (s *Service) record(ctx context.Context, route, outcome string, reason Reason) {
	if mark, ok := ctx.Value(outcomeMarkKey{}).(*outcomeMark); ok {
		mark.recorded = true
	}
	s.outcomes.Add(ctx, 1, metric.WithAttributes(attribute.String("route", route),
		attribute.String("outcome", outcome), attribute.String("reason", string(reason))))
}

// succeeded counts a success and writes its one log line. principal is the
// signed-in account of a callback; an authorize has none.
func (s *Service) succeeded(ctx context.Context, route string, providerID uuid.UUID, principal *uuid.UUID) {
	s.record(ctx, route, outcomeSuccess, ReasonAccepted)
	attrs := []any{"route", route, "outcome", outcomeSuccess, "reason", string(ReasonAccepted),
		"provider_id", providerID.String(), "request_id", requestid.Wire(ctx)}
	if principal != nil {
		attrs = append(attrs, "principal_id", principal.String())
	}
	s.cfg.Logger.InfoContext(ctx, "auth oidc: request succeeded", attrs...)
}

// refused counts a refusal and writes its one log line.
func (s *Service) refused(ctx context.Context, route string, providerID uuid.UUID, failure *Error) {
	outcome, level := "denied", slog.LevelWarn
	if failure.Status >= http.StatusInternalServerError {
		outcome, level = "error", slog.LevelError
	}
	s.record(ctx, route, outcome, failure.Reason)
	attrs := []any{"route", route, "outcome", outcome, "reason", string(failure.Reason),
		"provider_id", providerID.String(), "status", failure.Status, "request_id", requestid.Wire(ctx)}
	if failure.err != nil {
		attrs = append(attrs, logging.ErrorAttr(failure.err))
	}
	s.cfg.Logger.Log(ctx, level, "auth oidc: request refused", attrs...)
}

func (s *Service) callback(ctx context.Context, admitted StoredProvider, in CallbackInput) (Login, *http.Cookie, *Error) {
	row, failure := decodeAdmitted(admitted)
	if failure != nil {
		return Login{}, nil, failure
	}
	if in.Code == "" {
		return Login{}, nil, authFailed(ReasonMalformed, errors.New("empty code"))
	}
	now := s.cfg.Now()
	state, err := openState(s.cfg.StateSecret.Reveal(), in.State, row.ID.String(), now)
	if errors.Is(err, errStateExpired) {
		return Login{}, nil, authFailed(ReasonExpired, err)
	}
	if err != nil || state.OrgID != row.OrgID.String() {
		return Login{}, nil, authFailed(ReasonMalformed, errStateInvalid)
	}
	path := callbackPath(row.ID)
	clear := &http.Cookie{Name: loginNonceCookieName(state.ID), Value: "", Path: path, MaxAge: -1,
		HttpOnly: true, Secure: true, SameSite: http.SameSiteLaxMode}
	cookie := ""
	if in.Cookie != nil {
		cookie, _ = in.Cookie(loginNonceCookieName(state.ID))
	}
	if cookie == "" || !constantTimeEqual(hashLoginNonce(cookie), state.NonceHash) {
		return Login{}, clear, authFailed(ReasonMalformed, errors.New("login nonce cookie missing or wrong"))
	}
	consumed, err := s.cfg.Flows.Consume(ctx, flowHash(state.ID), protocolOIDC, row.ID, now)
	if err != nil {
		return Login{}, clear, internal(ReasonStoreUnavailable, err)
	}
	if !consumed {
		return Login{}, clear, authFailed(ReasonReplayed, errors.New("flow already used or unknown"))
	}

	claims, failure := s.exchangeAndValidate(ctx, row, state, in.Code)
	if failure != nil {
		return Login{}, clear, failure
	}
	login, failure := s.resolve(ctx, row, claims, in.RequestID)
	if failure != nil {
		return Login{}, clear, failure
	}
	return login, clear, nil
}

type validated struct {
	key     ExternalKey
	profile profile
	// legacyAuthProvider is the go-api users.auth_provider value of an account
	// this login created before the identity store: "oidc", or the OAuth type.
	legacyAuthProvider string
}

func (s *Service) exchangeAndValidate(ctx context.Context, row Provider, state flowState, code string) (validated, *Error) {
	meta, err := resolveMetadata(ctx, s.cfg.HTTPClient, row.Config)
	if err != nil {
		return validated{}, authFailed(ReasonDependency, err)
	}
	if meta.Issuer == "" || row.Config.ClientID == "" || meta.JWKSURI == "" {
		return validated{}, authFailed(ReasonMalformed, errors.New("provider configuration missing issuer, client_id or jwks_uri"))
	}
	oauthCfg := oauth2.Config{
		ClientID: row.Config.ClientID, ClientSecret: clientSecret(s.cfg.Secrets, row.EncryptedSecrets),
		RedirectURL: state.RedirectURI,
		Endpoint:    oauth2.Endpoint{TokenURL: meta.TokenEndpoint, AuthStyle: oauth2.AuthStyleInParams},
	}
	token, err := exchangeCode(ctx, s.cfg.HTTPClient, oauthCfg, code, state.CodeVerifier)
	if err != nil {
		return validated{}, authFailed(ReasonDependency, err)
	}
	rawIDToken, _ := token.Extra("id_token").(string)
	if token.AccessToken == "" || rawIDToken == "" {
		return validated{}, authFailed(ReasonMalformed, errors.New("token response missing access_token or id_token"))
	}
	keys := gooidc.NewRemoteKeySet(gooidc.ClientContext(ctx, s.cfg.HTTPClient), meta.JWKSURI)
	claims, err := verifyIDToken(ctx, keys, rawIDToken, claimRules{
		Issuer: meta.Issuer, ClientID: row.Config.ClientID, Nonce: state.Nonce, Now: s.cfg.Now(),
	})
	if err != nil {
		return validated{}, authFailed(reasonOf(err), err)
	}
	var userinfo map[string]any
	if meta.UserinfoEndpoint != "" {
		if userinfo, err = fetchUserinfo(ctx, s.cfg.HTTPClient, meta.UserinfoEndpoint, token.AccessToken); err != nil {
			return validated{}, authFailed(ReasonDependency, err)
		}
		if sub, _ := userinfo["sub"].(string); sub != claims.Subject {
			return validated{}, authFailed(ReasonMalformed, errors.New("userinfo subject differs from the ID token subject"))
		}
	}
	return validated{
		key:                ExternalKey{Issuer: claims.Issuer, Provider: row.ID.String(), Subject: claims.Subject},
		profile:            readProfile(mergeClaims(userinfo, claims.All), row.Config.ClaimMapping),
		legacyAuthProvider: ProviderKind,
	}, nil
}

// resolve turns a validated identity into a session. The key decides; on a
// hit the e-mail is not read. On a miss the e-mail must be verified, allowed
// by the provider, and held by no account; then a new account is created
// only if the provider and the identity store both allow it.
func (s *Service) resolve(ctx context.Context, row Provider, v validated, requestID *string) (Login, *Error) {
	principal, err := s.cfg.Identities.IdentityBySubject(ctx, v.key, LegacyMatch{AuthProvider: v.legacyAuthProvider, MembershipOrg: row.OrgID})
	newAccount := false
	switch {
	case err == nil:
	case errors.Is(err, ErrInvalidKey):
		return Login{}, authFailed(ReasonMalformed, err)
	case errors.Is(err, ErrIdentityAmbiguous):
		return Login{}, deny(http.StatusConflict, ReasonIdentityConflict,
			"More than one account carries this sign-in. Contact your organization administrator.")
	case errors.Is(err, ErrNotOrgMember):
		return Login{}, deny(http.StatusForbidden, ReasonMembershipRequired,
			"This account is not a member of the provider's organization.")
	case errors.Is(err, ErrIdentityNotFound):
		var failure *Error
		if principal, failure = s.provision(ctx, row, v, requestID); failure != nil {
			return Login{}, failure
		}
		newAccount = true
	default:
		return Login{}, internal(ReasonStoreUnavailable, err)
	}
	if !principal.Active {
		return Login{}, deny(http.StatusForbidden, ReasonPrincipalDeactivated, "User account is disabled")
	}

	role, found, err := s.cfg.Memberships.Role(ctx, principal.ID, row.OrgID)
	if err != nil {
		return Login{}, internal(ReasonStoreUnavailable, err)
	}
	if !found && newAccount && s.cfg.MembershipJIT != nil {
		if role, err = s.cfg.MembershipJIT.Join(ctx, principal.ID, row.OrgID); err != nil {
			return Login{}, internal(ReasonStoreUnavailable, err)
		}
		found = true
	}
	login := Login{PrincipalID: principal.ID, Email: principal.Email, Role: "member"}
	if found {
		org := row.OrgID
		login.OrgID = &org
		if role != "" {
			login.Role = role
		}
	}
	tokens, err := s.cfg.Sessions.Issue(ctx, SessionRequest{PrincipalID: principal.ID, OrgID: login.OrgID, Role: login.Role, AMR: AMR})
	if err != nil {
		return Login{}, internal(ReasonStoreUnavailable, err)
	}
	login.Tokens = tokens
	return login, nil
}

func (s *Service) provision(ctx context.Context, row Provider, v validated, requestID *string) (Principal, *Error) {
	p := v.profile
	if p.Email == "" {
		return Principal{}, deny(http.StatusBadRequest, ReasonMalformed, "Email address is missing from OIDC claims")
	}
	if !p.EmailVerified {
		return Principal{}, deny(http.StatusForbidden, ReasonEmailUnverified, "No verified email address is available from this OIDC provider")
	}
	if len(row.AllowedDomains) > 0 && !contains(row.AllowedDomains, emailDomain(p.Email)) {
		return Principal{}, deny(http.StatusForbidden, ReasonDomainNotAllowed, "Email domain is not allowed for this provider")
	}
	inUse, err := s.cfg.Identities.EmailInUse(ctx, p.Email)
	if errors.Is(err, ErrInvalidEmail) {
		return Principal{}, deny(http.StatusBadRequest, ReasonMalformed, "Email address from OIDC claims is malformed")
	}
	if err != nil {
		return Principal{}, internal(ReasonStoreUnavailable, err)
	}
	if inUse {
		return Principal{}, conflict()
	}
	if !row.AutoProvision {
		return Principal{}, deny(http.StatusForbidden, ReasonProvisioningDisabled, "User not found and auto-provisioning is disabled")
	}
	principal, err := s.cfg.Identities.ProvisionFederated(ctx, FederatedAccount{Key: v.key, Email: p.Email, FullName: p.FullName, RequestID: requestID})
	switch {
	case err == nil:
		return principal, nil
	case errors.Is(err, ErrSignupNotAvailable):
		return Principal{}, deny(http.StatusForbidden, ReasonSignupUnavailable, "Sign-up through this provider is not available")
	case errors.Is(err, ErrIdentityConflict):
		return Principal{}, conflict()
	case errors.Is(err, ErrInvalidEmail):
		return Principal{}, deny(http.StatusBadRequest, ReasonMalformed, "Email address from OIDC claims is malformed")
	case errors.Is(err, ErrInvalidKey):
		return Principal{}, authFailed(ReasonMalformed, err)
	default:
		return Principal{}, internal(ReasonStoreUnavailable, err)
	}
}

func conflict() *Error {
	return deny(http.StatusConflict, ReasonEmailCollision,
		"An account with this email address already exists. Sign in with your existing method, then link this provider.")
}

var errInsecureURL = errors.New("must use the https:// scheme (or an http:// loopback address)")

// validateHTTPSOrLoopback is go-api's shared redirect rule: https with a host,
// or http to a loopback host; no userinfo.
func validateHTTPSOrLoopback(candidate string) error {
	parsed, err := url.Parse(candidate)
	if err != nil || parsed.User != nil {
		return errInsecureURL
	}
	switch {
	case strings.EqualFold(parsed.Scheme, "https") && parsed.Host != "":
		return nil
	case strings.EqualFold(parsed.Scheme, "http"):
		switch parsed.Hostname() {
		case "localhost", "127.0.0.1", "::1":
			return nil
		}
	}
	return errInsecureURL
}
