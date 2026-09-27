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

// samlLoginNonceCookieName is the HttpOnly, SameSite=Lax cookie initiateSAMLAuth
// sets (only when the provider's own allow_idp_initiated is false) alongside
// the AEAD RelayState -- see the browser-binding doc comment on
// hashSAMLLoginNonce below for what it defends against.
const samlLoginNonceCookieName = "dho_saml_login_nonce"

// samlLoginNonceCookie builds the cookie set at initiate time (value=nonce,
// non-empty maxAge) and the one used to clear it at ACS time (value="",
// maxAge=-1) -- one constructor so both call sites stay identical apart
// from those two fields.
func samlLoginNonceCookie(value string, maxAge int, secure bool) *http.Cookie {
	return &http.Cookie{
		Name:     samlLoginNonceCookieName,
		Value:    value,
		Path:     "/",
		MaxAge:   maxAge,
		HttpOnly: true,
		Secure:   secure,
		SameSite: http.SameSiteLaxMode,
	}
}

// samlStateTTL mirrors oidcStateTTL/oauthStateTTL (D2727-amended's
// established design): 10 minutes.
const samlStateTTL = 10 * time.Minute

// samlStateHKDFInfo is a THIRD distinct HKDF info string, alongside
// oidcStateHKDFInfo and oauthStateHKDFInfo (state.go, oauthstate.go),
// deriving an independent AES-256-GCM key from the same root secret so a
// SAML RelayState token cannot even be attempted against OIDC's or
// OAuth's verify functions.
const samlStateHKDFInfo = "dev-health-ops:saml-state-aead-v1"

// D2744 (team-lead, CHAOS-6659 r1 P1): Python's process_saml_response
// never checks InResponseTo against anything generated at initiate time
// (this file's own package doc comment already recorded that as an
// accepted delta), and this port's ServiceProvider.AllowIDPInitiated was
// hardcoded true for EVERY provider regardless of the provider's own
// allow_idp_initiated column (scanned from the DB, providers.go, but
// never consulted) -- confirmed by r1 to disable crewjam/saml's own
// InResponseTo/possibleRequestIDs enforcement unconditionally, unlike
// Python, which never built the mechanism to begin with. Ruled: honour
// the row's own flag. When it is true, behavior is unchanged (today's
// AllowIDPInitiated=true, no state). When it is false, initiateSAMLAuth
// mints this AEAD-sealed opaque token as the request's RelayState --
// carrying the AuthnRequest's own ID -- and samlACSCallback decrypts it
// back into possibleRequestIDs for ParseXMLResponse, so an unsolicited
// (or replayed, or forged) SAMLResponse presented to an
// allow_idp_initiated=false provider is refused before signature
// verification even matters for InResponseTo purposes.
//
// This intentionally REPLACES the caller-supplied relay_state in that
// mode: samlClaims never surfaces relay_state to any observable outcome
// (this file's own doc comment), so there is nothing to preserve by
// interleaving an arbitrary caller value into the sealed payload, and
// doing so would only add attacker-influenced bytes to a token this
// package must authenticate on the way back in.
type samlState struct {
	ProviderID string `json:"provider_id"`
	OrgID      string `json:"org_id"`
	// RequestID is the SP-generated AuthnRequest's own ID (initiateSAMLAuth's
	// requestID) -- the value ParseXMLResponse's possibleRequestIDs checks
	// the assertion's SubjectConfirmationData.InResponseTo against.
	RequestID string `json:"request_id"`
	// NonceHash is hashSAMLLoginNonce of the random value initiateSAMLAuth
	// also placed in samlLoginNonceCookieName -- D2745's browser-binding
	// class ruling ("SAML's RelayState needs the same treatment", applied
	// here from CHAOS-6986/OAuth's P1-3). A RelayState token authenticates
	// only that IT was minted by this package; it says nothing about which
	// browser is presenting it back. Without this, a login-CSRF works: an
	// attacker starts their OWN IdP login, captures the resulting
	// SAMLResponse+RelayState pair (both travel through the attacker's own
	// browser, so the attacker legitimately holds both), and gets a victim
	// to submit that exact pair to the victim's own browser's /acs -- the
	// RelayState verifies fine (it is a real, unexpired token this package
	// minted), silently logging the victim into the attacker's identity.
	// Binding it to a same-origin HttpOnly SameSite=Lax cookie closes this:
	// SameSite=Lax cookies are not sent on a cross-site POST (the form
	// crewjam's IdP-side flow, and an attacker's replay, both use), so a
	// cross-site-submitted RelayState arrives with no matching cookie and
	// is refused in processSAMLResponse before ParseXMLResponse ever runs.
	NonceHash string `json:"nonce_hash"`
	ID        string `json:"id"` // random, no comparison meaning of its own.
	IssuedAt  int64  `json:"issued_at"`
	ExpiresAt int64  `json:"expires_at"`
}

// hashSAMLLoginNonce mirrors pkceChallenge's shape (state.go): SHA-256,
// base64url, unpadded. Never reversed -- only ever compared, in constant
// time (constantTimeEqual, state.go), against a freshly-hashed cookie value.
func hashSAMLLoginNonce(nonce string) string {
	sum := sha256.Sum256([]byte(nonce))
	return base64.RawURLEncoding.EncodeToString(sum[:])
}

var (
	errInvalidSAMLState = errors.New("Invalid or expired SAML RelayState")
	errSAMLStateExpired = errors.New("SAML RelayState expired")
)

// mintSAMLState seals state (stamping ID/IssuedAt/ExpiresAt) into the
// opaque RelayState value initiateSAMLAuth returns when the provider's
// own allow_idp_initiated is false, AAD-bound to state.ProviderID -- see
// mintOIDCState's doc comment (state.go) for why the AAD binding matters.
func mintSAMLState(secret string, state samlState, now time.Time) (string, error) {
	gcm, err := newGCM(secret, samlStateHKDFInfo)
	if err != nil {
		return "", err
	}
	id, err := randomURLSafe(16)
	if err != nil {
		return "", err
	}
	state.ID = id
	state.IssuedAt = now.Unix()
	state.ExpiresAt = now.Add(samlStateTTL).Unix()
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

// verifySAMLState mirrors verifyOIDCState/verifyOAuthState: the GCM tag,
// bound to providerID as AAD, authenticates before anything is parsed,
// then expiry is checked. A caller who has not proven possession of a
// RelayState token this package minted for this exact provider gets
// errInvalidSAMLState or errSAMLStateExpired -- both routed, at the call
// site, to D2738's unauthenticated bucket, never a provider row mutation.
func verifySAMLState(secret, token, providerID string, now time.Time) (samlState, error) {
	gcm, err := newGCM(secret, samlStateHKDFInfo)
	if err != nil {
		return samlState{}, errInvalidSAMLState
	}
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return samlState{}, errInvalidSAMLState
	}
	if len(raw) < gcm.NonceSize() {
		return samlState{}, errInvalidSAMLState
	}
	nonce, ciphertext := raw[:gcm.NonceSize()], raw[gcm.NonceSize():]
	plaintext, err := gcm.Open(nil, nonce, ciphertext, []byte(providerID))
	if err != nil {
		return samlState{}, errInvalidSAMLState
	}
	var state samlState
	if err := json.Unmarshal(plaintext, &state); err != nil {
		return samlState{}, errInvalidSAMLState
	}
	if state.ProviderID != providerID || state.OrgID == "" || state.RequestID == "" || state.ID == "" || state.NonceHash == "" {
		return samlState{}, errInvalidSAMLState
	}
	if now.Unix() > state.ExpiresAt {
		return samlState{}, errSAMLStateExpired
	}
	return state, nil
}
