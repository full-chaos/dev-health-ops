package sso

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"time"
)

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
	// NonceHash is D2745 P1-3's browser-binding class ruling (hashLoginNonce,
	// loginnonce.go): the random login-nonce value initiateOAuthAuth/
	// initiateOAuthByType also places in a per-flow cookie named by THIS
	// state's own ID (loginNonceCookieName("oauth", state.ID), D2759). A
	// genuine, unexpired OAuth `state` token only proves this package
	// minted it -- not which browser is presenting it back at /callback.
	// A login-CSRF works without this: an attacker starts their OWN OAuth
	// login, captures the resulting code+state pair (both travel through
	// the attacker's own browser), and gets a victim to submit that pair
	// to the victim's own browser's /callback -- the state verifies fine
	// (a real, unexpired token this package minted), silently logging the
	// victim into the attacker's identity. Binding it to a same-origin
	// HttpOnly SameSite=Lax cookie closes this: SameSite=Lax cookies are
	// not sent on a cross-site submission, so a cross-site-replayed state
	// arrives with no matching cookie and is refused in
	// oauthExchangeAndFetch before the token exchange ever runs.
	NonceHash string `json:"nonce_hash"`
	ID        string `json:"id"` // random; ALSO this flow's login-nonce cookie name suffix (D2759) -- no longer "no comparison meaning of its own".
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
// Also returns the generated ID directly (D2759): initiateOAuthAuth/
// initiateOAuthByType need it to name this flow's login-nonce cookie, and
// the caller has no other way to learn the value this function stamps
// into state.ID internally.
func mintOAuthState(secret string, state oauthState, now time.Time) (token, flowID string, err error) {
	gcm, err := newGCM(secret, oauthStateHKDFInfo)
	if err != nil {
		return "", "", err
	}
	id, err := randomURLSafe(16)
	if err != nil {
		return "", "", err
	}
	state.ID = id
	state.IssuedAt = now.Unix()
	state.ExpiresAt = now.Add(oauthStateTTL).Unix()
	plaintext, err := json.Marshal(state)
	if err != nil {
		return "", "", err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return "", "", err
	}
	sealed := gcm.Seal(nonce, nonce, plaintext, []byte(state.ProviderID))
	return base64.RawURLEncoding.EncodeToString(sealed), id, nil
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
