// initiateOIDCAuth and oidcCallback are
// api/auth/sso/router.py:initiate_oidc_auth / oidc_callback
// (CHAOS-6658), backed by services/sso.py's
// generate_oidc_authorization_request and process_oidc_callback.
//
// Three deliberate deltas from Python, each recorded here and in the PR
// body rather than silently replicated or silently fixed:
//
//  1. Entitlement gate order. Python's @require_feature decorator runs
//     before the route body, but it is dead code for this org (see
//     sso.go's package doc): only the process tier ever decided, so the
//     route always answered 402 before touching the database. Once the
//     gate is real (gate.go, D2725) it needs an org id, and these two
//     routes are PUBLIC (no authenticated caller) -- the only org id
//     available is the provider row's own org_id. So requireEntitlement
//     necessarily runs AFTER the provider lookup (404 first), not before
//     it, on both routes. A request for an unknown provider id still 404s
//     regardless of entitlement, matching to the SHAPE Python would answer
//     if its gate were ever satisfied for the wrong reason (a passing
//     process tier): 404 was always the second check in the function
//     body, this just makes it reachable.
//
//  2. The OIDC state is now the fix, not a replica of the break (D2727,
//     amended: team-lead). See state.go's doc comment: Python generates
//     state/nonce/code_verifier and returns only `state` to the caller,
//     then can never verify it on callback (nothing persists them). This
//     package's `state` IS an opaque, AEAD-encrypted (not merely signed)
//     token carrying everything the callback needs, so the round trip
//     actually completes and an intercepted state value discloses
//     nothing (in particular not the PKCE code_verifier). The nonce is
//     consequently always checked (Python's check is `if expected_nonce
//     and ...`, and expected_nonce is always None, so it never checks);
//     this is an acknowledged, deliberate improvement, not a divergence
//     to chase. Replay protection is the IdP's one-time authorization
//     code, not this token -- state.go's doc comment says so explicitly,
//     per the lead's instruction to state it rather than assume it.
//
//  3. No parity target exists for the substantive OIDC exchange logic.
//     AGENTS.md requires a differential oracle for cross-implementation
//     work, but Python's callback path is unreachable (finding 2), so
//     there is no live "real producer" on the Python side to diff
//     against -- oracle-testing a fix against the thing it fixes would
//     prove nothing. This package's tests (oidc_integration_test.go)
//     instead drive the full round trip against a self-signed test IdP
//     fixture (per team-lead's STEP-0 answer), and the shared
//     ssovenue oracle continues to pin that Python still 402s
//     unconditionally on these two routes (the one part of the contract
//     both planes still agree on).
//
// Two accepted, narrower simplifications, not requiring a ruling:
//   - Token-exchange failure (bad status, malformed body, missing
//     access_token) is uniformly a 400 "OIDC token exchange failed" here.
//     Python separately 500s on a response body that fails response.json()
//     (an uncaught JSONDecodeError) versus 400s on a missing access_token
//     key; golang.org/x/oauth2's Exchange does not expose that
//     distinction, and reaching it requires a token endpoint that returns
//     a 2xx with an unparseable body, which no real IdP does.
//   - id_token validation has no exact equivalent of Python's 60-second
//     JWT leeway (OIDC_JWT_LEEWAY_SECONDS): go-oidc/v3's oidc.Config has
//     no leeway knob. A real IdP's own clock skew is well inside typical
//     NTP-synced tolerances; this only matters for a test fixture whose
//     clock is deliberately skewed, which this package's tests do not do.
package sso

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"golang.org/x/oauth2"

	"github.com/full-chaos/dev-health-ops/internal/api/audit"
	"github.com/full-chaos/dev-health-ops/internal/api/credentials"
	"github.com/full-chaos/dev-health-ops/internal/api/externalurl"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// appBaseURLDefault is initiate_oidc_auth/oidc_callback's own
// os.environ.get("APP_BASE_URL", "http://localhost:8000") -- a different
// default than config.AppBaseURL's "https://example.com" (billing.go),
// which is a different call site in Python with its own hardcoded
// default. Read live per request, matching Python's own os.environ.get.
const appBaseURLDefault = "http://localhost:8000"

func appBaseURL() string {
	if value, ok := os.LookupEnv("APP_BASE_URL"); ok {
		return pythonparity.Strip(value)
	}
	return appBaseURLDefault
}

// defaultOIDCClient dials only addresses externalurl's SSRF guard allows
// and follows no redirects, the same posture credentials.Routes gives an
// admin-configured probe URL -- an OIDC issuer is exactly that kind of
// admin-configured external URL.
func defaultOIDCClient() *http.Client {
	return &http.Client{Timeout: 30 * time.Second, Transport: externalurl.GuardedTransport(),
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
}

// decodeConfigObject decodes a stored JSON column into an object; a
// present, non-object value is the wrong shape (errShape, providers.go),
// matching an uncaught Python TypeError -- the bare 500 (h.fail).
func decodeConfigObject(raw *string) (*pyjson.Object, error) {
	if raw == nil {
		return pyjson.NewObject(), nil
	}
	decoded, err := pyjson.DecodeString(*raw)
	if err != nil {
		return nil, err
	}
	switch value := decoded.(type) {
	case nil:
		return pyjson.NewObject(), nil
	case *pyjson.Object:
		return value, nil
	default:
		return nil, fmt.Errorf("%w: config", errShape)
	}
}

func objString(obj *pyjson.Object, key string) string {
	if obj == nil {
		return ""
	}
	value, present := obj.Get(key)
	if !present {
		return ""
	}
	text, _ := value.(string)
	return text
}

// objStringList is config.get(key, default): a present, non-list value or
// a non-string element is the wrong shape (errShape), matching
// _string_list's TypeError; absent is the given default.
func objStringList(obj *pyjson.Object, key string, fallback []string) ([]string, error) {
	if obj == nil {
		return fallback, nil
	}
	value, present := obj.Get(key)
	if !present {
		return fallback, nil
	}
	items, ok := value.([]pyjson.Value)
	if !ok {
		return nil, fmt.Errorf("%w: %s", errShape, key)
	}
	out := make([]string, 0, len(items))
	for _, item := range items {
		text, ok := item.(string)
		if !ok {
			return nil, fmt.Errorf("%w: %s item", errShape, key)
		}
		out = append(out, text)
	}
	return out, nil
}

// objStringMap is config.get(key, {}): a present, non-object value or a
// non-string value inside it is the wrong shape.
func objStringMap(obj *pyjson.Object, key string) (map[string]string, error) {
	out := map[string]string{}
	if obj == nil {
		return out, nil
	}
	value, present := obj.Get(key)
	if !present {
		return out, nil
	}
	nested, ok := value.(*pyjson.Object)
	if !ok {
		return nil, fmt.Errorf("%w: %s", errShape, key)
	}
	for _, k := range nested.Keys() {
		v, _ := nested.Get(k)
		text, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("%w: %s value", errShape, key)
		}
		out[k] = text
	}
	return out, nil
}

// oidcConfigValues is SSOProvider.get_oidc_config().
type oidcConfigValues struct {
	ClientID, Issuer, AuthorizationEndpoint, TokenEndpoint, UserinfoEndpoint, JWKSURI string
	Scopes                                                                            []string
	ClaimMapping                                                                      map[string]string
}

var defaultOIDCScopes = []string{"openid", "profile", "email"}

func decodeOIDCConfig(raw *string) (oidcConfigValues, error) {
	object, err := decodeConfigObject(raw)
	if err != nil {
		return oidcConfigValues{}, err
	}
	scopes, err := objStringList(object, "scopes", defaultOIDCScopes)
	if err != nil {
		return oidcConfigValues{}, err
	}
	mapping, err := objStringMap(object, "claim_mapping")
	if err != nil {
		return oidcConfigValues{}, err
	}
	return oidcConfigValues{
		ClientID: objString(object, "client_id"), Issuer: objString(object, "issuer"),
		AuthorizationEndpoint: objString(object, "authorization_endpoint"),
		TokenEndpoint:         objString(object, "token_endpoint"),
		UserinfoEndpoint:      objString(object, "userinfo_endpoint"),
		JWKSURI:               objString(object, "jwks_uri"),
		Scopes:                scopes, ClaimMapping: mapping,
	}, nil
}

// oidcProcessing is OIDCProcessingError: a 400, its message recorded onto
// the provider (record_error) and into an audit row, never surfaced
// verbatim to the caller (the router always answers its own fixed
// message).
type oidcProcessing struct{ msg string }

func (e oidcProcessing) Error() string { return e.msg }

func oidcErr(msg string) error { return oidcProcessing{msg: msg} }

// decryptProviderSecret is _decrypt_secret(encrypted_secrets, key): decrypt
// with the configured cipher, falling back to the raw stored value on any
// failure (an unconfigured cipher, a wrong key, a pre-encryption legacy
// plaintext value) -- exactly Python's `except Exception: return raw`.
func decryptProviderSecret(cipher credentials.Cipher, raw *string, key string) string {
	object, err := decodeConfigObject(raw)
	if err != nil || object == nil {
		return ""
	}
	value, present := object.Get(key)
	if !present {
		return ""
	}
	text, ok := value.(string)
	if !ok || text == "" {
		return ""
	}
	if cipher == nil || !cipher.Configured() {
		return text
	}
	decoded, err := cipher.Decrypt(secrets.NewValue(text))
	if err != nil {
		return text
	}
	return string(decoded)
}

// initiateOIDCAuth is POST /oidc/{provider_id}/authorize.
func (h handlers) initiateOIDCAuth(w http.ResponseWriter, r *http.Request) {
	providerID, err := pythonparity.ParseUUID(r.PathValue("provider_id"))
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
	redirectURI, redirectSet := errs.OptionalString(object, "redirect_uri", 0, 0)
	usePKCE, pkceSet := errs.DefaultedBool(object, "use_pkce")
	if !pkceSet {
		usePKCE = true
	}
	if len(errs) > 0 {
		policy.WriteJSON(w, http.StatusUnprocessableEntity, pybody.Detail(errs), nil)
		return
	}
	ctx := r.Context()
	row, err := scanProvider(h.Pool.QueryRow(ctx, `SELECT `+providerColumns+` FROM sso_providers WHERE id = $1::uuid`, providerID))
	if errorsIsNoRows(err) {
		policy.WriteDetail(w, http.StatusNotFound, "SSO provider not found", nil)
		return
	}
	if err != nil {
		h.fail(w, r, "load provider", err)
		return
	}
	// See this file's doc comment, delta 1: the entitlement gate needs
	// the provider's own org_id (a public route, no caller), so it runs
	// after the 404 check, not before it.
	if !h.requireEntitlement(w, r, row.OrgID) {
		return
	}
	if row.Protocol != "oidc" {
		policy.WriteDetail(w, http.StatusBadRequest, "Provider is not OIDC", nil)
		return
	}
	if row.Status != "active" {
		policy.WriteDetail(w, http.StatusBadRequest, "SSO provider is not active", nil)
		return
	}
	config, err := decodeOIDCConfig(row.Config)
	if err != nil {
		h.fail(w, r, "decode oidc config", err)
		return
	}
	state, err := randomURLSafe(32)
	if err != nil {
		h.fail(w, r, "generate state", err)
		return
	}
	nonce, err := randomURLSafe(32)
	if err != nil {
		h.fail(w, r, "generate nonce", err)
		return
	}
	redirect := appBaseURL() + "/oidc/" + row.ID + "/callback"
	if redirectSet && redirectURI != "" {
		redirect = redirectURI
	}
	params := url.Values{
		"response_type": {"code"}, "client_id": {config.ClientID}, "redirect_uri": {redirect},
		"scope": {strings.Join(config.Scopes, " ")}, "state": {state}, "nonce": {nonce},
	}
	var codeVerifier string
	if usePKCE {
		codeVerifier, err = randomURLSafe(64)
		if err != nil {
			h.fail(w, r, "generate code verifier", err)
			return
		}
		params.Set("code_challenge", pkceChallenge(codeVerifier))
		params.Set("code_challenge_method", "S256")
	}
	authEndpoint := config.AuthorizationEndpoint
	if authEndpoint == "" {
		authEndpoint = config.Issuer + "/authorize"
	}
	encrypted, err := mintOIDCState(h.StateSecret, oidcState{
		ProviderID: row.ID, OrgID: row.OrgID, Nonce: nonce, CodeVerifier: codeVerifier, RedirectURI: redirect,
	}, h.Now())
	if err != nil {
		h.fail(w, r, "mint oidc state", err)
		return
	}
	out := pyjson.NewObject()
	out.Set("authorization_url", authEndpoint+"?"+params.Encode())
	out.Set("state", encrypted)
	policy.WriteModel(w, http.StatusOK, out, nil)
}

// oidcCallback is POST /oidc/{provider_id}/callback.
func (h handlers) oidcCallback(w http.ResponseWriter, r *http.Request) {
	providerID, err := pythonparity.ParseUUID(r.PathValue("provider_id"))
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
	codeVerifierField, codeVerifierSet := errs.OptionalString(object, "code_verifier", 0, 0)
	if len(errs) > 0 {
		policy.WriteJSON(w, http.StatusUnprocessableEntity, pybody.Detail(errs), nil)
		return
	}
	ctx := r.Context()
	row, err := scanOIDCProvider(h.Pool.QueryRow(ctx, `SELECT `+oidcProviderColumns+` FROM sso_providers WHERE id = $1::uuid`, providerID))
	if errorsIsNoRows(err) {
		policy.WriteDetail(w, http.StatusNotFound, "SSO provider not found", nil)
		return
	}
	if err != nil {
		h.fail(w, r, "load provider", err)
		return
	}
	if !h.requireEntitlement(w, r, row.OrgID) {
		return
	}
	if row.Protocol != "oidc" {
		policy.WriteDetail(w, http.StatusBadRequest, "Provider is not OIDC", nil)
		return
	}
	if row.Status != "active" {
		policy.WriteDetail(w, http.StatusBadRequest, "SSO provider is not active", nil)
		return
	}
	if code == "" {
		policy.WriteDetail(w, http.StatusBadRequest, "OIDC authentication failed", nil)
		return
	}

	claims, err := h.exchangeAndValidate(ctx, row, providerID, stateToken, code, codeVerifierField, codeVerifierSet)
	if err != nil {
		var processing oidcProcessing
		if !asOIDCProcessing(err, &processing) {
			h.fail(w, r, "oidc callback", err)
			return
		}
		h.oidcCallbackFailure(ctx, w, r, row.OrgID, providerID, processing.msg, http.StatusBadRequest,
			"OIDC authentication failed", "")
		return
	}

	email := claims.email
	if email == "" {
		policy.WriteDetail(w, http.StatusBadRequest, "Email address is missing from OIDC claims", nil)
		return
	}
	allowedDomains, err := decodeAllowedDomains(row.AllowedDomains)
	if err != nil {
		h.fail(w, r, "decode allowed domains", err)
		return
	}
	if len(allowedDomains) > 0 {
		at := strings.LastIndex(email, "@")
		if at < 0 {
			policy.WriteDetail(w, http.StatusBadRequest, "Email address from OIDC claims is malformed", nil)
			return
		}
		domain := email[at+1:]
		if !containsFold(allowedDomains, domain) {
			policy.WriteDetail(w, http.StatusForbidden, fmt.Sprintf("Email domain '%s' is not allowed for this provider", domain), nil)
			return
		}
	}

	user, membership, err := h.provisionOrGetUser(ctx, row, email, claims.fullName, providerID, claims.externalID)
	if err != nil {
		var processing oidcProcessing
		if !asOIDCProcessing(err, &processing) {
			h.fail(w, r, "oidc provisioning", err)
			return
		}
		h.oidcCallbackFailure(ctx, w, r, row.OrgID, providerID, processing.msg, http.StatusBadRequest,
			"OIDC user provisioning failed", "provisioning")
		return
	}

	role := "member"
	orgID := ""
	var orgUUID *uuid.UUID
	if membership != nil {
		role = membership.role
		orgID = membership.orgID.String()
		id := membership.orgID
		orgUUID = &id
	}
	now := h.Now()
	jti, err := newJTI()
	if err != nil {
		h.fail(w, r, "mint token", err)
		return
	}
	access, err := h.Signer.Access(edgetoken.AccessClaims{
		UserID: user.id.String(), Email: user.email, OrgID: orgID, Role: role, IsSuperuser: user.isSuperuser,
		Username: user.username, FullName: user.fullName, TokenVersion: user.tokenVersion,
	}, now, jti)
	if err != nil {
		h.fail(w, r, "mint access token", err)
		return
	}
	// r1 review (CHAOS-6658, P1): the earlier version of this function
	// minted a stateless refresh JWT and never inserted a refresh_tokens
	// row, faithfully matching SSOService's own AuthService.
	// create_token_pair (services/auth.py:282-283) -- but /auth/refresh
	// (api/auth/routers/refresh.py:158, ported here as refreshByHash)
	// does a real DB lookup by the token's own jti on BOTH planes, and
	// 401s when no row exists. Python's version of this route was never
	// actually reachable (the pre-CHAOS-6658 dead-state finding), so this
	// Python bug never had a live consequence; once this Go port fixed
	// the state bug and made the route real, the SAME stateless mint
	// would have shipped a login whose OWN refresh_token 401s on first
	// use -- a real, executed, reproduced defect (r1's second P1), not a
	// parity target worth preserving. Fixed here by storing the refresh
	// token the same way the password-login route does (session/tokens.go
	// storeRefresh), inside the same transaction as the audit/login-time
	// writes.
	family := uuid.New()
	refreshJTI, err := newJTI()
	if err != nil {
		h.fail(w, r, "mint token", err)
		return
	}
	refreshExpiresAt := now.Add(7 * 24 * time.Hour)
	refresh, err := h.Signer.RefreshUntil(edgetoken.RefreshClaims{UserID: user.id.String(), OrgID: orgID, FamilyID: family.String()},
		now, refreshExpiresAt, refreshJTI)
	if err != nil {
		h.fail(w, r, "mint refresh token", err)
		return
	}

	tx, err := h.Pool.Begin(ctx)
	if err != nil {
		h.fail(w, r, "begin", err)
		return
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := tx.Exec(ctx, `UPDATE sso_providers SET last_login_at = $2, updated_at = $2 WHERE id = $1::uuid`,
		providerID, now.UTC()); err != nil {
		h.fail(w, r, "record login", err)
		return
	}
	if _, err := tx.Exec(ctx, `INSERT INTO refresh_tokens
	(id, user_id, org_id, token_hash, family_id, expires_at, revoked_at, replaced_by_hash, successor_jti,
	 ip_address, user_agent, created_at)
VALUES ($1, $2, $3, $4, $5, $6, NULL, NULL, NULL, $7, $8, $9)`,
		uuid.New(), user.id, orgUUID, hashRefreshJTI(refreshJTI), family, refreshExpiresAt.UTC(),
		clientHost(r), userAgentHeader(r), now.UTC()); err != nil {
		h.fail(w, r, "store refresh token", err)
		return
	}
	meta := pyjson.NewObject()
	meta.Set("provider_id", providerID.String())
	meta.Set("protocol", "oidc")
	metaJSON, err := pyjson.Dumps(meta)
	if err != nil {
		h.fail(w, r, "encode audit metadata", err)
		return
	}
	if _, err := (audit.PGWriter{Now: h.Now}).Write(ctx, tx, audit.Entry{
		OrgID: mustParseUUID(row.OrgID), UserID: uuidPtr(user.id), Action: audit.ActionSSOLogin,
		ResourceType: audit.ResourceSession, ResourceID: user.id.String(), RequestMetadata: []byte(metaJSON),
	}); err != nil {
		h.fail(w, r, "write audit log", err)
		return
	}
	if err := tx.Commit(ctx); err != nil {
		h.fail(w, r, "commit", err)
		return
	}

	out := pyjson.NewObject()
	out.Set("access_token", access)
	out.Set("refresh_token", refresh)
	out.Set("token_type", "bearer")
	out.Set("expires_in", pyjson.IntOf(int64(edgetoken.AccessLifetime/time.Second)))
	out.Set("user_id", user.id.String())
	out.Set("email", user.email)
	out.Set("org_id", orgID)
	out.Set("role", role)
	policy.WriteModel(w, http.StatusOK, out, nil)
}

// oidcCallbackFailure is process_oidc_callback's / provision_or_get_user's
// except SSOProcessingError block: record_error onto the provider, an
// SSO_LOGIN audit row (status failure), commit, then the response.
func (h handlers) oidcCallbackFailure(ctx context.Context, w http.ResponseWriter, r *http.Request,
	orgID string, providerID uuid.UUID, errMsg string, status int, detail, stage string) {
	tx, err := h.Pool.Begin(ctx)
	if err != nil {
		h.fail(w, r, "begin failure record", err)
		return
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	sanitized := pythonparity.SanitizeErrorText(errMsg, 4000)
	now := h.Now()
	if _, err := tx.Exec(ctx, `UPDATE sso_providers SET last_error = $2, last_error_at = $3, status = 'error', updated_at = $3
WHERE id = $1::uuid`, providerID, sanitized, now.UTC()); err != nil {
		h.fail(w, r, "record provider error", err)
		return
	}
	meta := pyjson.NewObject()
	meta.Set("protocol", "oidc")
	if stage != "" {
		meta.Set("stage", stage)
	}
	metaJSON, err := pyjson.Dumps(meta)
	if err != nil {
		h.fail(w, r, "encode audit metadata", err)
		return
	}
	if _, err := (audit.PGWriter{Now: h.Now}).Write(ctx, tx, audit.Entry{
		OrgID: mustParseUUID(orgID), Action: audit.ActionSSOLogin, ResourceType: audit.ResourceSSOProvider,
		ResourceID: providerID.String(), Status: "failure", ErrorMessage: &sanitized, RequestMetadata: []byte(metaJSON),
	}); err != nil {
		h.fail(w, r, "write audit log", err)
		return
	}
	if err := tx.Commit(ctx); err != nil {
		h.fail(w, r, "commit", err)
		return
	}
	policy.WriteDetail(w, status, detail, nil)
}

// oidcClaims is process_oidc_callback's returned dict, the fields the
// router reads from it.
type oidcClaims struct {
	email, fullName, externalID string
}

// exchangeAndValidate is process_oidc_callback: verify the state, exchange
// the code, validate the id_token, optionally fetch userinfo, map claims.
func (h handlers) exchangeAndValidate(ctx context.Context, row *providerRow, providerID uuid.UUID,
	stateToken, code string, codeVerifierField string, codeVerifierSet bool) (oidcClaims, error) {
	now := h.Now()
	// providerID is the GCM AAD: a state minted for a different provider
	// fails authentication here, before any parsing, identically to a
	// tampered ciphertext -- not merely detected afterward by comparing
	// a decrypted field (state.go's own doc comment).
	state, err := verifyOIDCState(h.StateSecret, stateToken, providerID.String(), now)
	if err != nil {
		if errors.Is(err, errOIDCStateExpired) {
			return oidcClaims{}, oidcErr("OIDC state expired")
		}
		return oidcClaims{}, oidcErr("OIDC state mismatch")
	}
	if state.OrgID != row.OrgID {
		return oidcClaims{}, oidcErr("OIDC state mismatch")
	}
	config, err := decodeOIDCConfig(row.Config)
	if err != nil {
		return oidcClaims{}, err
	}
	metadata, err := h.resolveOIDCMetadata(ctx, config)
	if err != nil {
		return oidcClaims{}, err
	}

	clientSecret := decryptProviderSecret(h.Cipher, row.EncryptedSecrets, "client_secret")
	verifier := state.CodeVerifier
	if codeVerifierSet && codeVerifierField != "" {
		// Python's payload.code_verifier, when the caller supplies one, is
		// what _exchange_oidc_code actually sends -- the state-embedded
		// one is this package's own record of what initiateOIDCAuth
		// generated. A caller that echoes nothing back gets the one this
		// package remembers.
		verifier = codeVerifierField
	}
	oauthCfg := oauth2.Config{
		ClientID: config.ClientID, ClientSecret: clientSecret,
		RedirectURL: state.RedirectURI,
		Endpoint:    oauth2.Endpoint{TokenURL: metadata.TokenEndpoint, AuthStyle: oauth2.AuthStyleInParams},
	}
	exchangeCtx := context.WithValue(ctx, oauth2.HTTPClient, h.HTTPClient)
	var opts []oauth2.AuthCodeOption
	if verifier != "" {
		opts = append(opts, oauth2.VerifierOption(verifier))
	}
	token, err := oauthCfg.Exchange(exchangeCtx, code, opts...)
	if err != nil {
		return oidcClaims{}, oidcErr("OIDC token exchange failed")
	}
	if token.AccessToken == "" {
		return oidcClaims{}, oidcErr("OIDC token response missing access_token")
	}
	rawIDToken, _ := token.Extra("id_token").(string)
	if rawIDToken == "" {
		return oidcClaims{}, oidcErr("OIDC token response missing id_token")
	}
	if metadata.Issuer == "" || config.ClientID == "" || metadata.JWKSURI == "" {
		return oidcClaims{}, oidcErr("OIDC configuration missing issuer/client_id/jwks")
	}
	verifyCtx := oidc.ClientContext(ctx, h.HTTPClient)
	keySet := oidc.NewRemoteKeySet(verifyCtx, metadata.JWKSURI)
	idVerifier := oidc.NewVerifier(metadata.Issuer, keySet, &oidc.Config{
		ClientID:             config.ClientID,
		SupportedSigningAlgs: []string{oidc.RS256, oidc.RS384, oidc.RS512, oidc.ES256, oidc.ES384, oidc.ES512},
		Now:                  h.Now,
	})
	idToken, err := idVerifier.Verify(verifyCtx, rawIDToken)
	if err != nil {
		return oidcClaims{}, oidcErr("OIDC id_token validation failed")
	}
	if idToken.IssuedAt.IsZero() {
		return oidcClaims{}, oidcErr("OIDC id_token validation failed")
	}
	// state.Nonce is always present (this package always generates one);
	// Python's own check is a no-op (state.go's constantTimeEqual doc
	// comment), so this is a real check where Python's is dead.
	if !constantTimeEqual(idToken.Nonce, state.Nonce) {
		return oidcClaims{}, oidcErr("OIDC nonce mismatch")
	}
	var idClaims map[string]any
	if err := idToken.Claims(&idClaims); err != nil {
		return oidcClaims{}, oidcErr("OIDC id_token validation failed")
	}

	merged := map[string]any{}
	if metadata.UserinfoEndpoint != "" {
		userinfo, err := h.fetchUserinfo(ctx, metadata.UserinfoEndpoint, token.AccessToken)
		if err != nil {
			return oidcClaims{}, err
		}
		for k, v := range userinfo {
			merged[k] = v
		}
	}
	for k, v := range idClaims {
		merged[k] = v
	}
	mapped := mapAttributes(merged, config.ClaimMapping)
	email := mapped["email"]
	if email == "" {
		if text, ok := merged["email"].(string); ok {
			email = text
		}
	}
	if email == "" {
		return oidcClaims{}, oidcErr("OIDC claims missing email")
	}
	fullName := mapped["full_name"]
	if fullName == "" {
		fullName = mapped["name"]
	}
	externalID, _ := merged["sub"].(string)
	return oidcClaims{email: email, fullName: fullName, externalID: externalID}, nil
}

func mapAttributes(attrs map[string]any, mapping map[string]string) map[string]string {
	mapped := map[string]string{}
	for target, source := range mapping {
		if value, ok := attrs[source].(string); ok {
			if trimmed := pythonparity.Strip(value); trimmed != "" {
				mapped[target] = trimmed
			}
		}
	}
	return mapped
}

func (h handlers) fetchUserinfo(ctx context.Context, endpoint, accessToken string) (map[string]any, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, oidcErr("OIDC userinfo request failed")
	}
	req.Header.Set("Authorization", "Bearer "+accessToken)
	resp, err := h.HTTPClient.Do(req)
	if err != nil {
		return nil, oidcErr("OIDC userinfo request failed")
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return nil, oidcErr("OIDC userinfo request failed")
	}
	var out map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, oidcErr("OIDC userinfo request failed")
	}
	return out, nil
}

// oidcMetadataValues is OIDCMetadata: the resolved issuer/token/userinfo/
// jwks endpoints.
type oidcMetadataValues struct{ Issuer, TokenEndpoint, UserinfoEndpoint, JWKSURI string }

// resolveOIDCMetadata is _get_oidc_metadata: use the config's own endpoints
// when both token_endpoint and jwks_uri are already set (skipping
// discovery entirely, config.get("issuer") ignored in that branch exactly
// as Python's does -- it defaults to "" when absent); else fetch
// .well-known/openid-configuration over HTTPS.
func (h handlers) resolveOIDCMetadata(ctx context.Context, config oidcConfigValues) (oidcMetadataValues, error) {
	if config.TokenEndpoint != "" && config.JWKSURI != "" {
		return oidcMetadataValues{Issuer: config.Issuer, TokenEndpoint: config.TokenEndpoint,
			UserinfoEndpoint: config.UserinfoEndpoint, JWKSURI: config.JWKSURI}, nil
	}
	if config.Issuer == "" {
		return oidcMetadataValues{}, oidcErr("OIDC issuer is required")
	}
	parsed, err := url.Parse(config.Issuer)
	if err != nil {
		return oidcMetadataValues{}, oidcErr("Invalid OIDC issuer URL")
	}
	if parsed.Scheme != "https" {
		return oidcMetadataValues{}, oidcErr("OIDC issuer must use HTTPS")
	}
	if parsed.Host == "" {
		return oidcMetadataValues{}, oidcErr("OIDC issuer must be a valid URL")
	}
	discoveryURL := strings.TrimRight(config.Issuer, "/") + "/.well-known/openid-configuration"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, discoveryURL, nil)
	if err != nil {
		return oidcMetadataValues{}, oidcErr("Failed to fetch OIDC discovery document")
	}
	resp, err := h.HTTPClient.Do(req)
	if err != nil {
		return oidcMetadataValues{}, oidcErr("Failed to fetch OIDC discovery document")
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return oidcMetadataValues{}, oidcErr("Failed to fetch OIDC discovery document")
	}
	var doc map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&doc); err != nil {
		return oidcMetadataValues{}, err // uncaught JSONDecodeError: the bare 500 (h.fail wraps it upstream).
	}
	resolvedIssuer := config.Issuer
	if text, ok := doc["issuer"].(string); ok && text != "" {
		resolvedIssuer = text
	}
	tokenEndpoint, _ := doc["token_endpoint"].(string)
	userinfoEndpoint, _ := doc["userinfo_endpoint"].(string)
	jwksURI, _ := doc["jwks_uri"].(string)
	if tokenEndpoint == "" || jwksURI == "" {
		return oidcMetadataValues{}, oidcErr("OIDC discovery missing required endpoints")
	}
	return oidcMetadataValues{Issuer: resolvedIssuer, TokenEndpoint: tokenEndpoint, UserinfoEndpoint: userinfoEndpoint, JWKSURI: jwksURI}, nil
}

// decodeAllowedDomains is _string_list(provider.allowed_domains, ...): a
// present, non-list value or a non-string, non-null element is the wrong
// shape; a null element is skipped, matching providerResponse's own read
// of the same column.
func decodeAllowedDomains(raw *string) ([]string, error) {
	if raw == nil {
		return nil, nil
	}
	decoded, err := pyjson.DecodeString(*raw)
	if err != nil {
		return nil, err
	}
	items, ok := decoded.([]pyjson.Value)
	if !ok {
		if decoded == nil {
			return nil, nil
		}
		return nil, fmt.Errorf("%w: allowed_domains", errShape)
	}
	out := make([]string, 0, len(items))
	for _, item := range items {
		switch value := item.(type) {
		case string:
			out = append(out, value)
		case nil:
		default:
			return nil, fmt.Errorf("%w: allowed_domains item", errShape)
		}
	}
	return out, nil
}

func containsFold(list []string, value string) bool {
	for _, item := range list {
		if item == value {
			return true
		}
	}
	return false
}

func errorsIsNoRows(err error) bool { return err != nil && errors.Is(err, pgx.ErrNoRows) }

func mustParseUUID(text string) uuid.UUID {
	id, err := pythonparity.ParseUUID(text)
	if err != nil {
		return uuid.UUID{}
	}
	return id
}

func uuidPtr(id uuid.UUID) *uuid.UUID { return &id }

// hashRefreshJTI is refresh_tokens._hash_token: sha256 of the jti's UTF-8
// bytes, the same hash refreshByHash (internal/api/session/store.go) looks
// up by. Duplicated rather than exported: one call site here, and the
// session package's own copy is unexported for the same reason.
func hashRefreshJTI(jti string) string {
	sum := sha256.Sum256([]byte(jti))
	return hex.EncodeToString(sum[:])
}

// clientHost is request.client.host: the peer's host, nil without one.
func clientHost(r *http.Request) *string {
	if r.RemoteAddr == "" {
		return nil
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	return &host
}

// userAgentHeader is request.headers.get("user-agent"): the first value,
// nil when absent.
func userAgentHeader(r *http.Request) *string {
	values := r.Header.Values("User-Agent")
	if len(values) == 0 {
		return nil
	}
	value := policy.Latin1(values[0])
	return &value
}

func asOIDCProcessing(err error, target *oidcProcessing) bool {
	if processing, ok := err.(oidcProcessing); ok {
		*target = processing
		return true
	}
	return false
}
