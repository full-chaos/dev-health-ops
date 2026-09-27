// initiateOAuthAuth, oauthCallback and initiateOAuthByType are
// api/auth/sso/router.py:initiate_oauth_auth / oauth_callback /
// initiate_oauth_by_type (CHAOS-6986), backed by services/oauth.py's
// OAuthProvider implementations (GitHub, GitLab, Google).
//
// Same D2725 gate delta as CHAOS-6658/6659: none of these three route
// functions takes a `session` kwarg (initiate_oauth_by_type takes org_id
// but opens its own DB session inline, never as a FastAPI-injected
// `session` kwarg), so require_feature's per-org branch
// (_check_org_feature_async, gating.py:504) always returns False for all
// three -- dead code, exactly like OIDC's two routes. requireEntitlement
// (gate.go) is the real fix. For the two provider_id-keyed routes the org
// id is not known until after the provider lookup, so the gate runs after
// the 404 check (D2725's established shape). initiate_oauth_by_type is
// different: org_id is a query parameter, known before ANY lookup, so the
// gate runs FIRST here -- the one OAuth route where the real check can run
// in the exact position Python's decorator would have run it.
//
// State (D2727/D2727-amended's established design, applied here, not a
// new ruling): see oauthstate.go's doc comment. Python's OAuth `state` is
// a bare secrets.token_urlsafe(32) with no verification anywhere in the
// codebase; this package mints and verifies a real AEAD-sealed one.
//
// D2738 (team-lead, mirrored a third time): a caller who has not proven
// possession of a state token this package minted for this exact
// provider/org is unauthenticated -- recordSSOUnauthenticated, no
// sso_providers mutation. Everything after state verification (token
// exchange, userinfo fetch) legitimately flips the row on failure, via
// recordSSOFailure.
//
// D2742 (team-lead): two deltas from OIDC/SAML's shape were flagged in an
// earlier version of this file and preserved faithfully per Python's
// shipped behavior, pending a ruling. That ruling landed and changed both:
//
//  1. the public detail on an exchange/userinfo failure no longer echoes
//     the upstream error text (router.py:1191's f"OAuth authentication
//     failed: {e}" is NOT replicated -- an information-exposure delta,
//     not a parity target). It carries a fixed message + a stable,
//     non-echoing reason code instead (oauthFailureDetail); the real text
//     still reaches last_error/audit and the structured log, never the
//     client. ssoProcessing (login.go) gained an optional reason field for
//     this; OIDC/SAML's own ssoErr call sites are unaffected (reason
//     stays empty, their messages were already fixed and non-echoing).
//  2. auto-provisioning-disabled is a THIRD bucket, distinct from both
//     recordSSOFailure (flips the row) and recordSSOUnauthenticated (no
//     mutation because the caller proved nothing): the state DID
//     authenticate here, but auto-provisioning-disabled reflects a
//     per-user access decision, not IdP-provider health, so it is
//     audited (recordSSOAuthenticatedDenial, login.go) but still never
//     mutates sso_providers, at its own status code (403, matching
//     router.py's HTTPException(403, ...) shape rather than
//     recordSSOFailure's fixed 400).
package sso

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/api/oauthprovider"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// oauthConfigValues is SSOProvider.get_oauth_config(): the only four keys
// the router ever reads from a stored OAuth provider's config. Python's
// OAuthConfig dataclass also has authorization_url/token_url/userinfo_url
// override fields, but get_oauth_config() never populates them from
// anything this router stores, so every provider created through this API
// always resolves to oauthDefaultEndpoints -- those three overrides are
// dead from this router's perspective and are not ported.
type oauthConfigValues struct {
	ClientID, RedirectURI, BaseURL string
	Scopes                         []string
}

func decodeOAuthConfig(raw *string) (oauthConfigValues, error) {
	object, err := decodeConfigObject(raw)
	if err != nil {
		return oauthConfigValues{}, err
	}
	scopes, err := objStringList(object, "scopes", nil)
	if err != nil {
		return oauthConfigValues{}, err
	}
	cfg := oauthConfigValues{
		ClientID: objString(object, "client_id"), RedirectURI: objString(object, "redirect_uri"),
		BaseURL: objString(object, "base_url"), Scopes: scopes,
	}
	if err := validateOAuthConfigHTTPS(cfg); err != nil {
		return oauthConfigValues{}, err
	}
	return cfg, nil
}

// validateOAuthConfigHTTPS is D2759's single shared URL policy
// (validateHTTPSOrLoopback, loginnonce.go), replacing this function's own
// former ad-hoc `strings.HasPrefix(x, "http://")` gate: an admin-
// overridden base_url (GitLab's self-hosted instance -- the one provider
// Python's own OAuthConfig lets point at an arbitrary URL, this file's
// own doc comment) or redirect_uri is refused here, at config-load time,
// unless it parses as https (or an http loopback address). This runs at
// every decodeOAuthConfig call site (buildOAuthAuthorization at
// /authorize, oauthExchangeAndFetch at /callback), so this is enforced
// at BOTH provider-config-validation time and exchange time from the same
// check, exactly like SAML's validateSAMLConfigHTTPS (saml.go).
//
// Only checked when the admin actually set an override -- an unset
// RedirectURI falls back to appBaseURL()-derived default (an ops-level
// guarantee, not a per-provider one), same scoping as SAML's check.
//
// D2759 (team-lead, r1 on #3355): the OLD raw-prefix version of this
// check was case-sensitive -- "HTTP://attacker.example/..." never
// matched "http://" and sailed straight through, refusing nothing.
// validateHTTPSOrLoopback parses first and compares the PARSED,
// normalized scheme, closing that class of bypass (also: a schemeless
// "//host" value and an opaque "https:evil" value, neither of which the
// old check considered at all, are refused too). D2752's own loopback
// rule (unconditional, no env var/build tag/knob of any kind) is
// unchanged, now enforced by the shared helper instead of this file's own
// copy of isLoopbackHTTPURL (deleted -- see loginnonce.go's
// isLoopbackHostname).
func validateOAuthConfigHTTPS(cfg oauthConfigValues) error {
	for _, candidate := range []string{cfg.BaseURL, cfg.RedirectURI} {
		if candidate == "" {
			continue
		}
		if err := validateHTTPSOrLoopback(candidate); err != nil {
			return fmt.Errorf("OAuth base_url/redirect_uri %w", err)
		}
	}
	return nil
}

// oauthProtocolFor is create_oauth_provider_instance's provider_type ->
// SSOProtocol: the URL path's provider_type ("github"/"gitlab"/"google")
// to the value stored in sso_providers.protocol ("oauth_github"/...).
func oauthProtocolFor(providerType string) (string, bool) {
	switch providerType {
	case oauthprovider.GitHub:
		return "oauth_github", true
	case oauthprovider.GitLab:
		return "oauth_gitlab", true
	case oauthprovider.Google:
		return "oauth_google", true
	}
	return "", false
}

// oauthTypeForProtocol is SSOProvider.oauth_provider_type: a stored
// provider row's protocol back to the type oauthprovider.Client and
// oauthDefaultEndpoints key by.
func oauthTypeForProtocol(protocol string) (string, bool) {
	switch protocol {
	case "oauth_github":
		return oauthprovider.GitHub, true
	case "oauth_gitlab":
		return oauthprovider.GitLab, true
	case "oauth_google":
		return oauthprovider.Google, true
	}
	return "", false
}

// oauthDefaultEndpoints is each OAuthProvider subclass's
// default_authorization_url / default_token_url; oauthprovider.
// DefaultEndpoints already holds the userinfo ones. GitLab's base is
// config.BaseURL when set (a self-hosted instance), else
// oauthprovider.DefaultEndpoints.GitLabBase ("https://gitlab.com"),
// matching create_oauth_provider's own base_url-or-default.
func oauthDefaultEndpoints(providerType, baseURL string) (authorizationURL, tokenURL string) {
	switch providerType {
	case oauthprovider.GitHub:
		return "https://github.com/login/oauth/authorize", "https://github.com/login/oauth/access_token"
	case oauthprovider.GitLab:
		base := strings.TrimRight(baseURL, "/")
		if base == "" {
			base = strings.TrimRight(oauthprovider.DefaultEndpoints.GitLabBase, "/")
		}
		return base + "/oauth/authorize", base + "/oauth/token"
	case oauthprovider.Google:
		return "https://accounts.google.com/o/oauth2/v2/auth", "https://oauth2.googleapis.com/token"
	}
	return "", ""
}

// Python's DEFAULT_SCOPES / get_default_scopes is never actually called by
// the router: every call site passes config.scopes, which defaults to []
// via oauth_config.get("scopes", []) -- an EMPTY list, not DEFAULT_SCOPES.
// This port matches that faithfully (decodeOAuthConfig's own
// objStringList(..., fallback=nil) plus buildOAuthAuthorization's own
// nil-to-[]string{} normalization): an empty or absent stored scopes list
// stays empty, never silently defaulted. DEFAULT_SCOPES itself is not
// ported, since nothing in the router's reachable code path uses it.

// oauthAuthorizationURL is generate_authorization_request: build the
// provider's authorization endpoint URL with the standard OAuth2
// authorization-code params. Google's own override
// (GoogleOAuthProvider.generate_authorization_request) adds
// access_type=offline and prompt=consent unconditionally.
func oauthAuthorizationURL(authorizationURL, providerType, clientID, redirectURI, state string, scopes []string) string {
	params := url.Values{
		"client_id": {clientID}, "redirect_uri": {redirectURI},
		"scope": {strings.Join(scopes, " ")}, "state": {state}, "response_type": {"code"},
	}
	if providerType == oauthprovider.Google {
		params.Set("access_type", "offline")
		params.Set("prompt", "consent")
	}
	return authorizationURL + "?" + params.Encode()
}

// exchangeOAuthCode is OAuthProvider.exchange_code_for_token: a form POST
// (httpx's `data=` is x-www-form-urlencoded, not JSON) with a fixed
// Accept: application/json header, parsed as _parse_token_response reads
// it -- only access_token is ever used by the caller, so only it is
// extracted; a missing/non-string access_token is the same OAuthTokenError
// class as a non-2xx status or a transport failure, all one ssoErr at the
// call site (oauthCallback wraps both this and fetchOAuthUserInfo in the
// same try/except OAuthProviderError Python does).
func (h handlers) exchangeOAuthCode(ctx context.Context, tokenURL, clientID, clientSecret, code, redirectURI string) (string, error) {
	form := url.Values{
		"client_id": {clientID}, "client_secret": {clientSecret}, "code": {code},
		"redirect_uri": {redirectURI}, "grant_type": {"authorization_code"},
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, tokenURL, strings.NewReader(form.Encode()))
	if err != nil {
		return "", errors.New("oauth token exchange request could not be built")
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")
	client := h.HTTPClient
	if client == nil {
		client = defaultOIDCClient()
	}
	resp, err := client.Do(req)
	if err != nil {
		return "", errors.New("token exchange request failed")
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", errors.New("token exchange response could not be read")
	}
	if resp.StatusCode >= 300 {
		return "", fmt.Errorf("token exchange failed: %d", resp.StatusCode)
	}
	text, err := pyjson.DecodeBody(raw)
	if err != nil {
		return "", errors.New("token response missing required field 'access_token'")
	}
	decoded, err := pyjson.DecodeString(text)
	if err != nil {
		return "", errors.New("token response missing required field 'access_token'")
	}
	object, ok := decoded.(*pyjson.Object)
	if !ok {
		return "", errors.New("token response missing required field 'access_token'")
	}
	value, present := object.Get("access_token")
	if !present {
		return "", errors.New("token response missing required field 'access_token'")
	}
	accessToken, ok := value.(string)
	if !ok || accessToken == "" {
		return "", errors.New("token response missing required field 'access_token'")
	}
	return accessToken, nil
}

// oauthClaims is what oauthCallback needs out of OAuthUserInfo.
type oauthClaims struct {
	email, username, fullName, avatarURL, externalID string
}

// fetchOAuthUserInfo wraps oauthprovider.Client.FetchUserInfo, converting
// its loosely-typed pyjson.Value fields the way each Python subclass's own
// usage does: user_info.email is used as a str (router.py:1198's
// .split("@")), so a non-string email is the SAME uncaught-exception shape
// Python has (a bare 500, via errShape here rather than an AttributeError
// there).
//
// r1 review (CHAOS-6986, P1): an EARLIER version of this function called
// oauthprovider.PyStr(info.Username)/PyStr(info.FullName) unconditionally,
// on the (wrong) theory that "str() coercion is close enough and never
// crashes." That is false: Google's fetch_user_info never sets Username
// at all (nil), and router.py:1229 passes user_info.username straight
// into User(username=...) with NO str() coercion -- Python stores SQL
// NULL there. PyStr(nil), by contrast, returns the Python repr of None,
// the literal string "None" (pyRepr's own `case nil: return "None"`) --
// not empty, so the OLD code stored a literal "None" username for EVERY
// auto-provisioned Google user. users.username carries a UNIQUE index
// (0001_initial_schema.py), so the FIRST such user succeeds and every
// subsequent one fails insertion outright. optionalPyStr below returns ""
// for a nil value (this file's own empty-string-means-unset convention,
// e.g. oauthProvisionUser's `if claims.username != ""` checks), never
// stringifying a genuine absence -- used for Username/FullName/AvatarURL,
// the three optional fields; ProviderUserID keeps unconditional PyStr,
// since it is REQUIRED and non-nil for all three providers (confirmed:
// github/gitlab already str() it themselves in Python; Google's provider_
// user_id is the one dataclass field whose `str` type hint Python's own
// runtime does not honor, but it's never nil, so PyStr's coercion there
// is still exactly Python's own eventual str()-on-insert behavior, not a
// parity break -- unlike Username/FullName, which genuinely can be nil).
func fetchOAuthUserInfo(ctx context.Context, client *oauthprovider.Client, providerType, accessToken string) (oauthClaims, error) {
	info, err := client.FetchUserInfo(ctx, providerType, accessToken)
	if err != nil {
		var userInfoErr *oauthprovider.UserInfoError
		if errors.As(err, &userInfoErr) {
			return oauthClaims{}, errors.New(userInfoErr.Reason)
		}
		return oauthClaims{}, err // ErrUnexpected: the bare 500, propagated as-is.
	}
	email, ok := info.Email.(string)
	if !ok {
		return oauthClaims{}, fmt.Errorf("%w: oauth user info email", errShape)
	}
	// D2745 (team-lead, CHAOS-6986 r1 P1-1): never look up or link an
	// existing user, and never auto-provision a new one, by an email the
	// provider itself has not verified -- an attacker who controls an
	// unverified mailbox address could otherwise take over (or provision
	// into) an account under that address. Refused here, before
	// oauthCallback ever calls oauthProvisionUser, which is what makes
	// "never link an existing user by an unverified email" categorical
	// rather than a check the provisioning path also has to remember.
	if !info.EmailVerified {
		return oauthClaims{}, errOAuthNoVerifiedEmail
	}
	return oauthClaims{
		email: email, username: optionalPyStr(info.Username), fullName: optionalPyStr(info.FullName),
		avatarURL:  optionalPyStr(info.AvatarURL),
		externalID: oauthprovider.PyStr(info.ProviderUserID),
	}, nil
}

// errOAuthNoVerifiedEmail is D2745 P1-1's refusal sentinel -- see
// fetchOAuthUserInfo's doc comment just above.
var errOAuthNoVerifiedEmail = errors.New("no verified email address is available from this OAuth provider")

// optionalPyStr is oauthprovider.PyStr, except a nil value (Python's
// None, e.g. Google's always-absent username) stays "" instead of
// becoming the literal string "None" -- see fetchOAuthUserInfo's doc
// comment for the bug this closes.
func optionalPyStr(value pyjson.Value) string {
	if value == nil {
		return ""
	}
	return oauthprovider.PyStr(value)
}

// initiateOAuthAuth is POST /oauth/{provider_id}/authorize, dispatched
// through oauthPair (sso.go), whose route pattern names path segments
// "first"/"second" (not "provider_id"/"authorize") since it also serves
// initiateOAuthByType's GET .../{provider_type}/authorize on the same
// pattern -- "first" is this route's provider_id.
func (h handlers) initiateOAuthAuth(w http.ResponseWriter, r *http.Request) {
	providerID, err := pythonparity.ParseUUID(r.PathValue("first"))
	if err != nil {
		h.fail(w, r, "parse provider id", err)
		return
	}
	rawBody, _ := policy.BodyFrom(r.Context())
	var errs pybody.Errors
	object, ok := errs.Object(rawBody)
	if !ok {
		policy.WriteJSON(w, http.StatusUnprocessableEntity, pybody.Detail(errs), nil)
		return
	}
	_, _ = errs.OptionalString(object, "redirect_uri", 0, 0) // accepted, never read -- see this file's doc comment.
	if len(errs) > 0 {
		policy.WriteJSON(w, http.StatusUnprocessableEntity, pybody.Detail(errs), nil)
		return
	}
	ctx := r.Context()
	row, err := scanOIDCProvider(h.Pool.QueryRow(ctx, `SELECT `+oidcProviderColumns+` FROM sso_providers WHERE id = $1::uuid`, providerID))
	if errorsIsNoRows(err) {
		policy.WriteDetail(w, http.StatusNotFound, "OAuth provider not found", nil)
		return
	}
	if err != nil {
		h.fail(w, r, "load provider", err)
		return
	}
	if !h.requireEntitlement(w, r, row.OrgID) {
		return
	}
	providerType, ok := oauthTypeForProtocol(row.Protocol)
	if !ok {
		policy.WriteDetail(w, http.StatusBadRequest, "Provider is not OAuth", nil)
		return
	}
	if row.Status != "active" {
		policy.WriteDetail(w, http.StatusBadRequest, "OAuth provider is not active", nil)
		return
	}
	out, err := h.buildOAuthAuthorization(w, row, providerID, providerType)
	if err != nil {
		h.fail(w, r, "build oauth authorization", err)
		return
	}
	policy.WriteModel(w, http.StatusOK, out, nil)
}

// buildOAuthAuthorization is generate_authorization_request, shared by
// initiateOAuthAuth and initiateOAuthByType (both build the identical
// response from a resolved provider row). w receives the D2745 P1-3
// login-nonce cookie, D2759's per-flow shape -- see oauthState.NonceHash's
// doc comment (oauthstate.go) for what it defends against.
func (h handlers) buildOAuthAuthorization(w http.ResponseWriter, row *providerRow, providerID uuid.UUID, providerType string) (*pyjson.Object, error) {
	config, err := decodeOAuthConfig(row.Config)
	if err != nil {
		return nil, err
	}
	redirectURI := config.RedirectURI
	if redirectURI == "" {
		redirectURI = appBaseURL() + "/api/v1/auth/oauth/" + row.ID + "/callback"
	}
	scopes := config.Scopes
	if scopes == nil {
		scopes = []string{}
	}
	authorizationURL, _ := oauthDefaultEndpoints(providerType, config.BaseURL)
	now := h.Now()
	loginNonce, err := randomURLSafe(32)
	if err != nil {
		return nil, err
	}
	encrypted, flowID, err := mintOAuthState(h.StateSecret, oauthState{
		ProviderID: row.ID, OrgID: row.OrgID, RedirectURI: redirectURI,
		NonceHash: hashLoginNonce(loginNonce),
	}, now)
	if err != nil {
		return nil, err
	}
	// D2759: named by this flow's own state ID and scoped to this
	// provider's own real callback path (not "/") -- two concurrent OAuth
	// flows in the same browser (two tabs, or two providers, or the
	// provider_id-keyed and by-type routes both used at once) no longer
	// collide on a single fixed cookie name.
	callbackPath := "/api/v1/auth/oauth/" + row.ID + "/callback"
	issueLoginNonceCookie(w, "oauth", flowID, loginNonce, callbackPath, oauthStateTTL)
	out := pyjson.NewObject()
	out.Set("authorization_url", oauthAuthorizationURL(authorizationURL, providerType, config.ClientID, redirectURI, encrypted, scopes))
	out.Set("state", encrypted)
	return out, nil
}

// oauthCallback is POST /oauth/{provider_id}/callback -- "first" is this
// route's provider_id, same naming note as initiateOAuthAuth above.
func (h handlers) oauthCallback(w http.ResponseWriter, r *http.Request) {
	providerID, err := pythonparity.ParseUUID(r.PathValue("first"))
	if err != nil {
		h.fail(w, r, "parse provider id", err)
		return
	}
	body, _ := policy.BodyFrom(r.Context())
	var errs pybody.Errors
	object, ok := errs.Object(body)
	if !ok {
		policy.WriteJSON(w, http.StatusUnprocessableEntity, pybody.Detail(errs), nil)
		return
	}
	code, _ := errs.RequiredString(object, "code", 0, 0)
	stateToken, _ := errs.RequiredString(object, "state", 0, 0)
	if len(errs) > 0 {
		policy.WriteJSON(w, http.StatusUnprocessableEntity, pybody.Detail(errs), nil)
		return
	}
	ctx := r.Context()
	row, err := scanOIDCProvider(h.Pool.QueryRow(ctx, `SELECT `+oidcProviderColumns+` FROM sso_providers WHERE id = $1::uuid`, providerID))
	if errorsIsNoRows(err) {
		policy.WriteDetail(w, http.StatusNotFound, "OAuth provider not found", nil)
		return
	}
	if err != nil {
		h.fail(w, r, "load provider", err)
		return
	}
	if !h.requireEntitlement(w, r, row.OrgID) {
		return
	}
	providerType, ok := oauthTypeForProtocol(row.Protocol)
	if !ok {
		policy.WriteDetail(w, http.StatusBadRequest, "Provider is not OAuth", nil)
		return
	}
	if row.Status != "active" {
		policy.WriteDetail(w, http.StatusBadRequest, "OAuth provider is not active", nil)
		return
	}

	// D2759: the login-nonce cookie is now per-flow (named by the state's
	// own verified ID, not a fixed name), so it can only be looked up
	// AFTER that state verifies -- done inside oauthExchangeAndFetch
	// itself, which also clears it unconditionally
	// (verifyAndClearLoginNonceCookie, loginnonce.go).
	claims, err := h.oauthExchangeAndFetch(ctx, w, r, row, providerID, providerType, stateToken, code)
	if err != nil {
		if errors.Is(err, errOAuthNoVerifiedEmail) {
			h.recordSSOAuthenticatedDenial(ctx, w, r, row.OrgID, providerID, err.Error(), http.StatusForbidden,
				"No verified email address is available from this OAuth provider", "oauth", "email_verification")
			return
		}
		var unauth ssoUnauthenticated
		if asSSOUnauthenticated(err, &unauth) {
			h.recordSSOUnauthenticated(ctx, w, r, row.OrgID, providerID, unauth.msg, http.StatusBadRequest,
				"OAuth authentication failed", "oauth", "state_auth")
			return
		}
		var processing ssoProcessing
		if !asSSOProcessing(err, &processing) {
			h.fail(w, r, "oauth callback", err)
			return
		}
		// D2742 (team-lead): router.py:1191 echoes the underlying error
		// text into the public detail (an information-exposure delta this
		// port does NOT replicate, superseding this file's earlier doc
		// comment). The response carries a fixed message + a stable,
		// non-echoing reason code (processing.reason); the real upstream
		// text still reaches last_error/audit via recordSSOFailure's own
		// errMsg param -- never the client.
		h.recordSSOFailure(ctx, w, r, row.OrgID, providerID, processing.msg, http.StatusBadRequest,
			oauthFailureDetail(processing.reason), "oauth", "")
		return
	}

	allowedDomains, err := decodeAllowedDomains(row.AllowedDomains)
	if err != nil {
		h.fail(w, r, "decode allowed domains", err)
		return
	}
	if len(allowedDomains) > 0 {
		at := strings.LastIndex(claims.email, "@")
		domain := ""
		if at >= 0 {
			domain = claims.email[at+1:]
		}
		if !containsFold(allowedDomains, domain) {
			// D2745 P2-6 (team-lead, D2742 strict): the client-visible
			// detail is now a fixed message + reason code -- the actual
			// domain never reaches the response (a probing surface:
			// iterating domain guesses and reading them back verbatim in
			// a 403 body). It goes to the audit row's error_message only.
			// This also escalates the check from Python's/this file's own
			// prior bare-HTTPException shape (no audit, matching OIDC/
			// SAML's identical unaudited domain check) to D2742's third
			// bucket: the caller HAS authenticated by this point (state,
			// login-nonce cookie, token exchange, userinfo fetch all
			// already succeeded) -- a disallowed domain is a per-account
			// access decision, not IdP-provider health, the same shape as
			// auto-provisioning-disabled/the unverified-email denial just
			// above in this file.
			h.recordSSOAuthenticatedDenial(ctx, w, r, row.OrgID, providerID,
				fmt.Sprintf("Email domain '%s' is not allowed for this provider", domain), http.StatusForbidden,
				oauthDomainDeniedDetail(), "oauth", "domain_check")
			return
		}
	}

	user, membership, err := h.oauthProvisionUser(ctx, row, providerType, claims)
	if err != nil {
		if errors.Is(err, errOAuthAutoProvisionDisabled) {
			// D2742 (team-lead): router.py:1218-1225 raises a bare
			// HTTPException(403) outside the try/except OAuthProviderError
			// block -- no audit, no provider mutation. Superseding this
			// file's earlier doc comment (delta 1): the caller here DID
			// authenticate (the state verified; the IdP vouches for this
			// email), so this is D2742's third bucket
			// (recordSSOAuthenticatedDenial, login.go) -- audited (a
			// defender should see repeated auto-provision denials), still
			// no provider mutation (this says nothing about whether the
			// PROVIDER is broken), still 403.
			h.recordSSOAuthenticatedDenial(ctx, w, r, row.OrgID, providerID, err.Error(), http.StatusForbidden,
				"User not found and auto-provisioning is disabled", "oauth", "provisioning")
			return
		}
		h.fail(w, r, "oauth provisioning", err)
		return
	}

	h.finishSSOLogin(ctx, w, r, providerID, row.OrgID, user, membership, row.Protocol)
}

// errOAuthAutoProvisionDisabled is router.py:1222's HTTPException(403) --
// a distinct sentinel so oauthCallback can route it to
// recordSSOAuthenticatedDenial (D2742) rather than ssoProcessing's
// recordSSOFailure (which flips the row -- wrong here, see the call site).
var errOAuthAutoProvisionDisabled = errors.New("user not found and auto-provisioning is disabled")

// oauthFailureDetail is D2742's client-facing body for an authenticated-
// but-later-failed OAuth exchange: a fixed message + a stable reason code,
// never the upstream error text (ssoProcessing's own doc comment, login.go).
func oauthFailureDetail(reason string) *pyjson.Object {
	detail := pyjson.NewObject()
	detail.Set("message", "OAuth authentication failed")
	detail.Set("reason", reason)
	return detail
}

// oauthDomainDeniedDetail is D2745 P2-6's client-facing body for a
// disallowed-domain denial: a fixed message + a stable reason code, never
// the domain itself -- see the call site's own doc comment.
func oauthDomainDeniedDetail() *pyjson.Object {
	detail := pyjson.NewObject()
	detail.Set("message", "Email domain is not allowed for this provider")
	detail.Set("reason", "oauth_domain_not_allowed")
	return detail
}

// oauthExchangeAndFetch is the callback's own inline body up to (not
// including) the domain-allowlist check: verify state, exchange the code,
// fetch user info.
func (h handlers) oauthExchangeAndFetch(ctx context.Context, w http.ResponseWriter, r *http.Request, row *providerRow, providerID uuid.UUID,
	providerType, stateToken, code string) (oauthClaims, error) {
	now := h.Now()
	state, err := verifyOAuthState(h.StateSecret, stateToken, providerID.String(), now)
	if err != nil {
		if errors.Is(err, errOAuthStateExpired) {
			return oauthClaims{}, ssoAuthErr("OAuth state expired")
		}
		return oauthClaims{}, ssoAuthErr("OAuth state mismatch")
	}
	if state.OrgID != row.OrgID {
		return oauthClaims{}, ssoAuthErr("OAuth state mismatch")
	}
	// D2745 P1-3 browser-binding class ruling, D2759 per-flow fix folded
	// in: the state alone only proves this package minted it, not which
	// browser is presenting it back (oauthState.NonceHash's doc comment,
	// oauthstate.go, has the login-CSRF this closes). The cookie is
	// looked up ONLY now, by the name this VERIFIED state's own ID gives
	// it -- never by a caller-supplied or pre-read value. A missing
	// cookie (withheld by SameSite=Lax on a cross-site submission, or
	// simply never set), one under the wrong per-flow name, or one that
	// hashes to a different value than the state sealed is refused here,
	// before the token exchange ever runs -- unauthenticated (D2738): the
	// caller has proven nothing about the browser binding.
	callbackPath := "/api/v1/auth/oauth/" + row.ID + "/callback"
	if !verifyAndClearLoginNonceCookie(w, r, "oauth", state.ID, callbackPath, state.NonceHash) {
		return oauthClaims{}, ssoAuthErr("OAuth state mismatch")
	}
	config, err := decodeOAuthConfig(row.Config)
	if err != nil {
		return oauthClaims{}, err
	}
	clientSecret := decryptProviderSecret(h.Cipher, row.EncryptedSecrets, "client_secret")
	redirectURI := config.RedirectURI
	if redirectURI == "" {
		redirectURI = appBaseURL() + "/api/v1/auth/oauth/" + row.ID + "/callback"
	}
	_, tokenURL := oauthDefaultEndpoints(providerType, config.BaseURL)
	accessToken, err := h.exchangeOAuthCode(ctx, tokenURL, config.ClientID, clientSecret, code, redirectURI)
	if err != nil {
		return oauthClaims{}, ssoErrReason(err.Error(), "oauth_token_exchange_failed")
	}
	client := &oauthprovider.Client{HTTP: h.HTTPClient, Endpoints: oauthprovider.DefaultEndpoints}
	if config.BaseURL != "" {
		client.Endpoints.GitLabBase = config.BaseURL
	}
	claims, err := fetchOAuthUserInfo(ctx, client, providerType, accessToken)
	if err != nil {
		if errors.Is(err, errShape) || errors.Is(err, oauthprovider.ErrUnexpected) {
			return oauthClaims{}, err // the bare 500, matching an uncaught Python exception.
		}
		if errors.Is(err, errOAuthNoVerifiedEmail) {
			// D2745 P1-1: propagated as-is, NOT wrapped in ssoErrReason --
			// this authenticated (the state verified; the access token
			// exchange succeeded) but a per-account access decision, not
			// IdP-provider health, is oauthCallback's job to route to
			// recordSSOAuthenticatedDenial (no row flip), the same bucket
			// as auto-provisioning-disabled just below in this file.
			return oauthClaims{}, err
		}
		return oauthClaims{}, ssoErrReason(err.Error(), "oauth_userinfo_fetch_failed")
	}
	return claims, nil
}

// oauthAuthProviderValue is auth_provider_map[oauth_provider_type]
// (router.py:1207-1212): the users.auth_provider value this login writes,
// distinct from OIDC/SAML's "oidc"/"saml" (provision.go).
func oauthAuthProviderValue(providerType string) string { return providerType }

// oauthProvisionUser is oauth_callback's OWN inline user lookup/creation
// (router.py:1214-1274) -- NOT SSOService.provision_or_get_user
// (provision.go's provisionOrGetUser), which OIDC/SAML call instead. The
// two paths are genuinely different in Python, not merely differently
// factored: this one sets avatar_url and auth_provider=github/gitlab/
// google (not oidc/saml), and its update-path conditions differ (auth_
// provider/auth_provider_id only rewritten when they actually changed,
// matching router.py:1249-1251 -- provisionOrGetUser instead rewrites them
// unconditionally on every login, per ITS OWN doc comment explaining why
// that is a no-op difference for OIDC/SAML specifically; that reasoning
// does NOT transfer here, since OAuth logins for the SAME email can
// legitimately alternate between providers in a way OIDC/SAML cannot, so
// the conditional check is preserved faithfully instead of simplified
// away).
func (h handlers) oauthProvisionUser(ctx context.Context, row *providerRow, providerType string, claims oauthClaims) (*provisionedUser, *provisionedMembership, error) {
	authProviderValue := oauthAuthProviderValue(providerType)
	var (
		id                                            uuid.UUID
		existingEmail                                 string
		username, existingFullName, existingAvatarURL *string
		currentAuthProvider, currentAuthProviderID    *string
		isSuperuser                                   bool
		tokenVersion                                  int64
	)
	err := h.Pool.QueryRow(ctx, `SELECT id, email, username, full_name, avatar_url, auth_provider, auth_provider_id, is_superuser, token_version
FROM users WHERE email = $1::text`, claims.email).Scan(&id, &existingEmail, &username, &existingFullName,
		&existingAvatarURL, &currentAuthProvider, &currentAuthProviderID, &isSuperuser, &tokenVersion)

	now := h.Now().UTC()
	if errors.Is(err, pgx.ErrNoRows) {
		if !row.AutoProvision {
			return nil, nil, errOAuthAutoProvisionDisabled
		}
		newID := uuid.New()
		var fullNamePtr, avatarURLPtr, externalIDPtr *string
		if claims.fullName != "" {
			fullNamePtr = &claims.fullName
		}
		if claims.avatarURL != "" {
			avatarURLPtr = &claims.avatarURL
		}
		if claims.externalID != "" {
			externalIDPtr = &claims.externalID
		}
		var usernamePtr *string
		if claims.username != "" {
			usernamePtr = &claims.username
		}
		if _, err := h.Pool.Exec(ctx, `INSERT INTO users
	(id, email, username, full_name, avatar_url, auth_provider, auth_provider_id, is_active, is_verified, is_superuser,
	 token_version, last_login_at, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, true, true, false, 0, $8, $8, $8)`,
			newID, claims.email, usernamePtr, fullNamePtr, avatarURLPtr, authProviderValue, externalIDPtr, now); err != nil {
			return nil, nil, err
		}
		return &provisionedUser{id: newID, email: claims.email, username: usernamePtr, fullName: fullNamePtr, tokenVersion: 0}, nil, nil
	}
	if err != nil {
		return nil, nil, err
	}

	newAuthProvider := currentAuthProvider
	newAuthProviderID := currentAuthProviderID
	if stringOrEmpty(currentAuthProvider) != authProviderValue {
		newAuthProvider = &authProviderValue
		if claims.externalID != "" {
			newAuthProviderID = &claims.externalID
		}
	}
	newAvatarURL := existingAvatarURL
	if claims.avatarURL != "" && stringOrEmpty(existingAvatarURL) == "" {
		newAvatarURL = &claims.avatarURL
	}
	newFullName := existingFullName
	if claims.fullName != "" && stringOrEmpty(existingFullName) == "" {
		newFullName = &claims.fullName
	}
	if _, err := h.Pool.Exec(ctx, `UPDATE users SET auth_provider = $2, auth_provider_id = $3, avatar_url = $4, full_name = $5,
	last_login_at = $6, updated_at = $6 WHERE id = $1::uuid`,
		id, newAuthProvider, newAuthProviderID, newAvatarURL, newFullName, now); err != nil {
		return nil, nil, err
	}

	providerOrgID, err := pythonparity.ParseUUID(row.OrgID)
	if err != nil {
		return nil, nil, err
	}
	var membership *provisionedMembership
	var membershipOrgID uuid.UUID
	var role *string
	err = h.Pool.QueryRow(ctx, `SELECT org_id, role FROM memberships WHERE user_id = $1::uuid AND org_id = $2::uuid`,
		id, providerOrgID).Scan(&membershipOrgID, &role)
	switch {
	case err == nil:
		roleText := "member"
		if role != nil {
			roleText = *role
		}
		membership = &provisionedMembership{orgID: membershipOrgID, role: roleText}
	case errors.Is(err, pgx.ErrNoRows):
		// No membership: onboarding required, matching Python's own log
		// line and nil membership (router.py:1269-1274).
	default:
		return nil, nil, err
	}

	return &provisionedUser{id: id, email: claims.email, username: username, fullName: newFullName,
		isSuperuser: isSuperuser, tokenVersion: tokenVersion}, membership, nil
}

// initiateOAuthByType is GET /oauth/{provider_type}/authorize?org_id=...
func (h handlers) initiateOAuthByType(w http.ResponseWriter, r *http.Request) {
	providerType := r.PathValue("first")
	if _, ok := oauthProtocolFor(providerType); !ok {
		policy.WriteDetail(w, http.StatusBadRequest, "Invalid OAuth provider type", nil)
		return
	}
	protocol, _ := oauthProtocolFor(providerType)
	var errs pybody.Errors
	orgIDStr, _ := errs.RequiredQueryString("org_id", pybody.LastQueryValue(r.URL.Query(), "org_id"), 0)
	if len(errs) > 0 {
		policy.WriteJSON(w, http.StatusUnprocessableEntity, pybody.Detail(errs), nil)
		return
	}
	orgID, err := pythonparity.ParseUUID(orgIDStr)
	if err != nil {
		h.fail(w, r, "parse org id", err)
		return
	}
	// Unlike the two provider_id-keyed routes, org_id is known here BEFORE
	// any provider lookup -- the one OAuth route where the real
	// entitlement gate can run in the exact position Python's (dead)
	// decorator would have run it, first. See this file's doc comment.
	if !h.requireEntitlement(w, r, orgID.String()) {
		return
	}
	ctx := r.Context()
	row, err := scanOIDCProvider(h.Pool.QueryRow(ctx, `SELECT `+oidcProviderColumns+` FROM sso_providers
WHERE org_id = $1::uuid AND protocol = $2 AND status = 'active' AND is_default = true`, orgID, protocol))
	if errorsIsNoRows(err) {
		row, err = scanOIDCProvider(h.Pool.QueryRow(ctx, `SELECT `+oidcProviderColumns+` FROM sso_providers
WHERE org_id = $1::uuid AND protocol = $2 AND status = 'active'`, orgID, protocol))
	}
	if errorsIsNoRows(err) {
		policy.WriteDetail(w, http.StatusNotFound, fmt.Sprintf("No active %s OAuth provider found for this organization", providerType), nil)
		return
	}
	if err != nil {
		h.fail(w, r, "load provider", err)
		return
	}
	out, err := h.buildOAuthAuthorization(w, row, mustParseProviderUUID(row.ID), providerType)
	if err != nil {
		h.fail(w, r, "build oauth authorization", err)
		return
	}
	policy.WriteModel(w, http.StatusOK, out, nil)
}

func mustParseProviderUUID(text string) uuid.UUID {
	id, err := pythonparity.ParseUUID(text)
	if err != nil {
		return uuid.UUID{}
	}
	return id
}
