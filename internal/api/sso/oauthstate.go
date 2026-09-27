package sso

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"time"
)

// oauthLoginNonceCookieName is the HttpOnly, SameSite=Lax cookie
// initiateOAuthAuth/initiateOAuthByType set alongside the AEAD state --
// see hashOAuthLoginNonce's doc comment for what it defends against
// (D2745 P1-3), and samlLoginNonceCookie (samlstate.go)/D2748 for the
// identical, already-landed treatment on SAML's RelayState this mirrors.
const oauthLoginNonceCookieName = "dho_oauth_login_nonce"

// oauthLoginNonceCookie builds the cookie set at /authorize (value=nonce,
// non-empty maxAge) and the one used to clear it at /callback
// (value="", maxAge=-1). Secure is a literal true, not conditioned on
// appBaseURL()'s scheme at request time -- D2748's already-landed
// reasoning applies identically here: validateOAuthConfigHTTPS (oauth.go,
// D2745 P1-2) refuses an insecure http:// base_url/redirect_uri override
// at config-load time, so a literal true removes a dead branch instead
// of leaving it to rot.
func oauthLoginNonceCookie(value string, maxAge int) *http.Cookie {
	return &http.Cookie{
		Name:     oauthLoginNonceCookieName,
		Value:    value,
		Path:     "/",
		MaxAge:   maxAge,
		HttpOnly: true,
		Secure:   true,
		SameSite: http.SameSiteLaxMode,
	}
}

// hashOAuthLoginNonce mirrors hashSAMLLoginNonce (samlstate.go)/
// pkceChallenge (state.go): SHA-256, base64url, unpadded. D2745 P1-3
// (team-lead, CHAOS-6986 r1): a genuine, unexpired OAuth `state` token
// only proves this package minted it -- not which browser is presenting
// it back at /callback. A login-CSRF works without this: an attacker
// starts their OWN OAuth login, captures the resulting code+state pair
// (both travel through the attacker's own browser), and gets a victim to
// submit that pair to the victim's own browser's /callback -- the state
// verifies fine (a real, unexpired token this package minted), silently
// logging the victim into the attacker's identity. Binding it to a
// same-origin HttpOnly SameSite=Lax cookie closes this: SameSite=Lax
// cookies are not sent on a cross-site submission, so a cross-site-
// replayed state arrives with no matching cookie and is refused in
// oauthExchangeAndFetch before the token exchange ever runs.
func hashOAuthLoginNonce(nonce string) string {
	sum := sha256.Sum256([]byte(nonce))
	return base64.RawURLEncoding.EncodeToString(sum[:])
}

// oauthStateTTL mirrors oidcStateTTL (D2727-amended): 10 minutes, matching
// Google's own authorization code lifetime and applied here for the same
// reason, across all three OAuth providers.
const oauthStateTTL = 10 * time.Minute

// oauthStateHKDFInfo is a DIFFERENT fixed HKDF info string from
// oidcStateHKDFInfo (state.go), deriving an independent AES-256-GCM key
// from the same root secret (Deps.StateSecret) so an OAuth state token and
// an OIDC state token are cryptographically unrelated: neither can even be
// attempted against the other's verify function.
const oauthStateHKDFInfo = "dev-health-ops:oauth-state-aead-v1"

// CHAOS-6986: Python's generate_authorization_request
// (api/services/oauth.py:136-161) mints `state` as a bare
// secrets.token_urlsafe(32) -- a random value with no cryptographic
// binding to anything -- and oauth_callback (router.py:1108-1307) never
// inspects payload.state at all: it is accepted by the request schema
// (OAuthCallbackRequest.state, required) and threaded into
// exchange_code_for_token(code, state=state), whose body never reads its
// own `state` parameter either (services/oauth.py:163-206's request data
// omits it entirely). So OAuth's state is, in production, 100% decorative:
// it provides no CSRF protection and cannot bind a callback to the
// provider/org that issued it.
//
// This is the SAME class of gap D2727/D2727-amended (team-lead) already
// ruled on for OIDC's state ("the OIDC state is now the fix, not a replica
// of the break") -- applying that same established design here, not a new
// ruling: oauthState is a real AEAD-sealed opaque token, structurally
// identical in shape and guarantees to oidcState (state.go), minus the
// fields OAuth's exchange does not need (no nonce -- OAuth here is not
// OIDC and validates no id_token; no code_verifier -- Python's OAuth
// providers never send a PKCE code_challenge, github/gitlab/google's
// server-side confidential-client flow does not need one).
type oauthState struct {
	ProviderID  string `json:"provider_id"`
	OrgID       string `json:"org_id"`
	RedirectURI string `json:"redirect_uri,omitempty"`
	// NonceHash is D2745 P1-3's browser-binding class ruling -- see
	// hashOAuthLoginNonce's doc comment just above.
	NonceHash string `json:"nonce_hash"`
	ID        string `json:"id"` // random, no comparison meaning of its own.
	IssuedAt  int64  `json:"issued_at"`
	ExpiresAt int64  `json:"expires_at"`
}

var (
	errInvalidOAuthState = errors.New("Invalid or expired OAuth state")
	errOAuthStateExpired = errors.New("OAuth state expired")
)

// mintOAuthState seals state (stamping ID/IssuedAt/ExpiresAt) into the
// opaque `state` value initiateOAuthAuth/initiateOAuthByType return,
// AAD-bound to state.ProviderID -- see mintOIDCState's doc comment
// (state.go) for why the AAD binding matters.
func mintOAuthState(secret string, state oauthState, now time.Time) (string, error) {
	gcm, err := newGCM(secret, oauthStateHKDFInfo)
	if err != nil {
		return "", err
	}
	id, err := randomURLSafe(16)
	if err != nil {
		return "", err
	}
	state.ID = id
	state.IssuedAt = now.Unix()
	state.ExpiresAt = now.Add(oauthStateTTL).Unix()
	plaintext, err := json.Marshal(state)
	if err != nil {
		return "", err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return "", err
	}
	sealed := gcm.Seal(nonce, nonce, plaintext, []byte(state.ProviderID))
	return base64.RawURLEncoding.EncodeToString(sealed), nil
}

// verifyOAuthState mirrors verifyOIDCState (state.go): the GCM tag, bound
// to providerID as AAD, authenticates before anything is parsed, then
// expiry is checked. A caller who has not proven possession of a state
// token this package minted for this exact provider gets
// errInvalidOAuthState or errOAuthStateExpired -- both routed, at the
// call site, to D2738's unauthenticated bucket (recordSSOUnauthenticated),
// never a provider row mutation.
func verifyOAuthState(secret, token, providerID string, now time.Time) (oauthState, error) {
	gcm, err := newGCM(secret, oauthStateHKDFInfo)
	if err != nil {
		return oauthState{}, errInvalidOAuthState
	}
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return oauthState{}, errInvalidOAuthState
	}
	if len(raw) < gcm.NonceSize() {
		return oauthState{}, errInvalidOAuthState
	}
	nonce, ciphertext := raw[:gcm.NonceSize()], raw[gcm.NonceSize():]
	plaintext, err := gcm.Open(nil, nonce, ciphertext, []byte(providerID))
	if err != nil {
		return oauthState{}, errInvalidOAuthState
	}
	var state oauthState
	if err := json.Unmarshal(plaintext, &state); err != nil {
		return oauthState{}, errInvalidOAuthState
	}
	if state.ProviderID != providerID || state.OrgID == "" || state.ID == "" || state.NonceHash == "" {
		return oauthState{}, errInvalidOAuthState
	}
	if now.Unix() > state.ExpiresAt {
		return oauthState{}, errOAuthStateExpired
	}
	return state, nil
}
