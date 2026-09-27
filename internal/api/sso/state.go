package sso

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"time"

	"golang.org/x/crypto/hkdf"
)

// isLoopbackHTTPURL mirrors OAuth's own copy of this check (D2752,
// oauth.go, not yet merged onto main as of this PR -- each provider's
// worktree defines it independently until the branches converge): true
// for an http:// URL whose host is a loopback address (RFC 8252 shape).
// D2749-follow-up (team-lead, "all three providers share one rule"):
// https required except a loopback host, checked unconditionally, no
// env var / build tag / knob of any kind.
func isLoopbackHTTPURL(raw string) bool {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme != "http" {
		return false
	}
	switch parsed.Hostname() {
	case "localhost", "127.0.0.1", "::1":
		return true
	}
	return false
}

// oidcLoginNonceCookieName is the HttpOnly, SameSite=Lax cookie
// initiateOIDCAuth sets alongside the AEAD state -- see
// hashOIDCLoginNonce's doc comment below for what it defends against
// (D2745's browser-binding class ruling, applied here from CHAOS-6659/
// CHAOS-6986's already-landed identical treatment on SAML's RelayState
// and OAuth's own state).
const oidcLoginNonceCookieName = "dho_oidc_login_nonce"

// oidcLoginNonceCookie builds the cookie set at /authorize (value=nonce,
// non-empty maxAge) and the one used to clear it at /callback (value="",
// maxAge=-1). Secure is a literal true -- team-lead's D2749-follow-up
// ruling ("apply the same host-only shape to the OIDC PR's login-nonce
// cookie Secure literal so all three providers share one rule"):
// initiateOIDCAuth now refuses a caller-supplied redirect_uri naming the
// insecure http:// scheme unless its host is loopback
// (isLoopbackHTTPURL), the same rule D2748 (SAML)/D2752 (OAuth) apply to
// their own admin-stored config -- mirroring their reasoning exactly,
// just checked at request time rather than config-load time, since this
// route's redirect_uri is caller-supplied, not admin-configured.
func oidcLoginNonceCookie(value string, maxAge int) *http.Cookie {
	return &http.Cookie{
		Name:     oidcLoginNonceCookieName,
		Value:    value,
		Path:     "/",
		MaxAge:   maxAge,
		HttpOnly: true,
		Secure:   true,
		SameSite: http.SameSiteLaxMode,
	}
}

// hashOIDCLoginNonce mirrors pkceChallenge's shape below: SHA-256,
// base64url, unpadded. D2745 (team-lead, browser-binding class ruling,
// applied here from CHAOS-6986's P1-3 after landing on SAML/OAuth
// first): a genuine, unexpired OIDC state only proves this package
// minted it, not which browser is presenting it back at /callback. A
// login-CSRF works without this: an attacker starts their OWN OIDC
// login, captures the resulting code+state pair (both travel through
// the attacker's own browser), and gets a victim to submit that pair to
// the victim's own browser's /callback -- the state verifies fine (a
// real, unexpired token this package minted), silently logging the
// victim into the attacker's identity. Binding it to a same-origin
// HttpOnly SameSite=Lax cookie closes this: SameSite=Lax cookies are not
// sent on a cross-site submission, so a cross-site-replayed state
// arrives with no matching cookie and is refused in exchangeAndValidate
// before the token exchange ever runs.
func hashOIDCLoginNonce(nonce string) string {
	sum := sha256.Sum256([]byte(nonce))
	return base64.RawURLEncoding.EncodeToString(sum[:])
}

// oidcStateTTL bounds how long a caller has between fetching
// authorization_url and completing the round trip at the IdP. Google's own
// authorization code lifetime is 10 minutes; D2727-amended (team-lead)
// caps this at 10 minutes too.
const oidcStateTTL = 10 * time.Minute

// oidcStateHKDFInfo is the fixed HKDF info string deriving the state's
// AES-256-GCM key from the api's own JWT_SECRET_KEY (Deps.StateSecret; the
// literal value apiservice/service.go passes is
// deps.GitHubStateSigner.Secret, the SAME cfg.APIJWTSecret.Reveal() every
// other short-lived signed/encrypted token in this codebase derives from
// -- no new env var). A fixed, distinct info string is what keeps this
// derived key independent of the access/refresh token key and the
// GitHub-App install-state key even though root secret is shared.
const oidcStateHKDFInfo = "dev-health-ops:oidc-state-aead-v1"

// Python's generate_oidc_authorization_request (services/sso.py:471-519)
// generates state, nonce and, with PKCE, a code_verifier -- then NEVER
// persists any of them (nothing in this repo's SSOProvider model or a
// dedicated table holds a pending-authorization row), and the router
// returns only `state` to the caller (schemas.py OIDCAuthResponse has no
// nonce or code_verifier field). _get_expected_state / _get_expected_nonce
// (sso.py:879-898) read provider.encrypted_secrets / provider.config keys
// ("expected_state", "expected_nonce") that are never written anywhere, so
// they always return None, and process_oidc_callback (sso.py:625-629)
// always raises "Missing OIDC state" for any real round trip. The OIDC
// callback path is structurally unreachable in Python as shipped.
//
// This is the fix (D2727, amended by team-lead after this package's first
// cut used a signed-only JWT -- rejected: a signature alone leaves the
// code_verifier plain, base64url-visible, in a value that transits IdP
// logs, browser history and a possible Referer leak on the IdP's own
// consent page; PKCE's whole point is that an intercepted authorization
// code is useless without the verifier, so a state mechanism that leaks
// the verifier defeats it). oidcState IS the state value initiateOIDCAuth
// returns, sealed with AES-256-GCM: a genuine AEAD, confidentiality AND
// integrity in one primitive, keyed by HKDF-SHA256(APIJWTSecret, info=
// oidcStateHKDFInfo). The AAD is the path's own provider_id, so a
// ciphertext minted for one provider fails authentication -- before any
// JSON parsing -- if presented at a different provider_id's callback path,
// not merely detected afterward by comparing a decrypted field. No
// database row or server-side session exists; the caller only ever holds
// an opaque token, and forging or reading one without the derived key is
// exactly as hard as forging an access token.
//
// Replay protection is NOT this token's job: the underlying authorization
// `code` the IdP issues is itself one-time (every real OIDC provider
// invalidates a code after its first exchange), which is the actual replay
// defense, exactly as it is for every other OAuth/OIDC client. This is
// recorded here and in the PR body's RISK-NOTES, per the lead's explicit
// instruction to state it, not assume it.
//
// This also fixes a second Python gap noted for the record: Python
// generates a nonce and, with PKCE, a code_verifier, but returns neither to
// the caller, so a real client could not supply the code_verifier
// OIDCCallbackRequest.code_verifier asks for even if state were checked.
// Encoding them into the opaque state removes the need for the client to
// remember either.
type oidcState struct {
	ProviderID   string `json:"provider_id"`
	OrgID        string `json:"org_id"`
	Nonce        string `json:"nonce"`                   // the OIDC PROTOCOL nonce (id_token replay check) -- NOT the browser-binding login nonce below.
	CodeVerifier string `json:"code_verifier,omitempty"` // "" when PKCE was not requested.
	RedirectURI  string `json:"redirect_uri,omitempty"`
	// NonceHash is hashOIDCLoginNonce of the random value initiateOIDCAuth
	// also placed in oidcLoginNonceCookieName -- D2745's browser-binding
	// class ruling. Distinct from Nonce above: this one binds the state to
	// the browser presenting it, not to the id_token the IdP returns.
	NonceHash string `json:"nonce_hash"`
	ID        string `json:"id"` // a random value, no comparison meaning of its own; see the doc comment above.
	IssuedAt  int64  `json:"issued_at"`
	ExpiresAt int64  `json:"expires_at"`
}

var (
	errInvalidOIDCState = errors.New("Invalid or expired OIDC state")
	errOIDCStateExpired = errors.New("OIDC state expired")
)

// randomURLSafe is secrets.token_urlsafe(n): n random bytes, base64url,
// unpadded.
func randomURLSafe(n int) (string, error) {
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(buf), nil
}

// deriveStateKey is HKDF-SHA256(secret, salt=nil, info, length=32): the
// AES-256-GCM key. secret must be non-empty; Routes() only mounts the
// real OIDC/SAML/OAuth handlers when Deps.StateSecret is (sso.go). info
// is a fixed, per-protocol constant (oidcStateHKDFInfo, samlStateHKDFInfo,
// oauthStateHKDFInfo) so each protocol's state universe derives an
// independent key from the same root secret -- a state token minted for
// one protocol cannot even be attempted against another's verify
// function, rather than merely failing to unmarshal into a different
// struct's required fields.
func deriveStateKey(secret, info string) ([32]byte, error) {
	var key [32]byte
	if secret == "" {
		return key, errors.New("sso: OIDC state secret is not configured")
	}
	if _, err := io.ReadFull(hkdf.New(sha256.New, []byte(secret), nil, []byte(info)), key[:]); err != nil {
		return key, err
	}
	return key, nil
}

func newGCM(secret, info string) (cipher.AEAD, error) {
	key, err := deriveStateKey(secret, info)
	if err != nil {
		return nil, err
	}
	block, err := aes.NewCipher(key[:])
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

// mintOIDCState seals state (stamping ID/IssuedAt/ExpiresAt) into the
// opaque `state` value initiateOIDCAuth returns, AAD-bound to
// state.ProviderID.
func mintOIDCState(secret string, state oidcState, now time.Time) (string, error) {
	gcm, err := newGCM(secret, oidcStateHKDFInfo)
	if err != nil {
		return "", err
	}
	id, err := randomURLSafe(16)
	if err != nil {
		return "", err
	}
	state.ID = id
	state.IssuedAt = now.Unix()
	state.ExpiresAt = now.Add(oidcStateTTL).Unix()
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

// verifyOIDCState is the callback's read of a state value: the GCM tag,
// bound to providerID as AAD, is authenticated before anything is parsed
// -- gcm.Open refuses a tampered ciphertext, a wrong key and a state
// minted for a different provider_id identically, all before touching the
// plaintext -- then expiry. A malformed token or one whose tag does not
// authenticate answers errInvalidOIDCState; an expired one, whose tag DID
// authenticate, answers errOIDCStateExpired distinctly, so the recorded
// reason (never the caller-visible response, which is fixed either way)
// says which.
func verifyOIDCState(secret, token, providerID string, now time.Time) (oidcState, error) {
	gcm, err := newGCM(secret, oidcStateHKDFInfo)
	if err != nil {
		return oidcState{}, errInvalidOIDCState
	}
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return oidcState{}, errInvalidOIDCState
	}
	if len(raw) < gcm.NonceSize() {
		return oidcState{}, errInvalidOIDCState
	}
	nonce, ciphertext := raw[:gcm.NonceSize()], raw[gcm.NonceSize():]
	plaintext, err := gcm.Open(nil, nonce, ciphertext, []byte(providerID))
	if err != nil {
		return oidcState{}, errInvalidOIDCState
	}
	var state oidcState
	if err := json.Unmarshal(plaintext, &state); err != nil {
		return oidcState{}, errInvalidOIDCState
	}
	if state.ProviderID != providerID || state.OrgID == "" || state.Nonce == "" || state.ID == "" || state.NonceHash == "" {
		return oidcState{}, errInvalidOIDCState
	}
	if now.Unix() > state.ExpiresAt {
		return oidcState{}, errOIDCStateExpired
	}
	return state, nil
}

// constantTimeEqual is hmac.compare_digest's constant-time string equality
// (id_token nonce check): the Python callback only checks the nonce
// `if expected_nonce and claims.get("nonce") != expected_nonce` (a plain
// `!=`, and only when a truthy expected_nonce exists, which it structurally
// never does -- so Python never actually validates a nonce today). This
// package always carries a nonce, so it always validates it, in constant
// time, an improvement recorded for the same reason as the state fix.
func constantTimeEqual(a, b string) bool {
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}

// pkceChallenge is the S256 code_challenge for a code_verifier:
// base64url(sha256(verifier)), unpadded.
func pkceChallenge(verifier string) string {
	sum := sha256.Sum256([]byte(verifier))
	return base64.RawURLEncoding.EncodeToString(sum[:])
}

// newJTI is a random lowercase-hex identifier: the access/refresh tokens'
// own jti (edgetoken.Signer.Access/Refresh), unrelated to oidcState.ID.
func newJTI() (string, error) {
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(random[:]), nil
}
