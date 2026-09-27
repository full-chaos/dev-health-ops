// D2759 (team-lead): SAML/OAuth/OIDC's independently-built browser-binding
// cookie mechanisms (D2744/D2745/D2748/D2749-follow-up/D2752) each had two
// class-level defects r1 caught on the OIDC copy and which turned out to be
// present, byte-for-byte identical, in all three:
//
//  1. A case-sensitive `strings.HasPrefix(x, "http://")` gate that
//     "HTTP://attacker.example/..." (or any other-cased variant) sails
//     straight through, never even reaching the loopback check.
//  2. A single FIXED cookie name per protocol, shared by every concurrent
//     /authorize (or /initiate) call from the same browser -- two tabs, or
//     two providers, and the second flow's Set-Cookie silently overwrites
//     the first's in the jar, so the first flow's callback then reads the
//     WRONG nonce and fails with a false "state mismatch".
//
// This file is the ONE shared fix for both, called by all three protocols'
// own initiate/callback handlers -- not three separately-patched copies.
package sso

import (
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// errInsecureURL is D2759's single URL-scheme policy error, returned by
// validateHTTPSOrLoopback for every admin-overridable or caller-supplied
// URL these three SSO protocols accept (SAML's sp_entity_id/sp_acs_url,
// OAuth's base_url/redirect_uri, OIDC's redirect_uri).
var errInsecureURL = errors.New("must use the https:// scheme (or an http:// loopback address)")

// validateHTTPSOrLoopback replaces every per-protocol `strings.HasPrefix(x,
// "http://")` gate (D2748 SAML, D2752 OAuth, the D2749-follow-up OIDC
// redirect_uri check) with one policy, applied by PARSING the candidate
// first rather than pattern-matching the raw string -- a raw prefix check
// is exactly what let "HTTP://..." (a different case), "//host/path" (no
// scheme at all), and "https:evil" (an opaque URI, no authority) all slip
// through various versions of this check undetected.
//
//   - https, with a real authority (Host != "", so an opaque "https:foo"
//     is refused): always accepted.
//   - http, ONLY when the host is loopback (localhost/127.0.0.1/::1, RFC
//     8252's shape -- the native-app redirect exception): accepted.
//   - anything else -- no scheme, any other scheme (javascript:, ftp://),
//     a URL carrying userinfo, or a value url.Parse refuses outright --
//     refused.
//
// An empty candidate is the caller's own business (it means "field unset,
// use the computed default", not "refuse") -- callers check that before
// calling this, same as every existing call site already did.
func validateHTTPSOrLoopback(candidate string) error {
	parsed, err := url.Parse(candidate)
	if err != nil || parsed.User != nil {
		return errInsecureURL
	}
	switch {
	case strings.EqualFold(parsed.Scheme, "https") && parsed.Host != "":
		return nil
	case strings.EqualFold(parsed.Scheme, "http") && isLoopbackHostname(parsed.Hostname()):
		return nil
	default:
		return errInsecureURL
	}
}

// isLoopbackHostname is url.URL.Hostname() (already port-stripped) checked
// against RFC 8252's loopback shape.
func isLoopbackHostname(host string) bool {
	switch host {
	case "localhost", "127.0.0.1", "::1":
		return true
	}
	return false
}

// loginNonceCookieName is D2759's per-flow cookie name: protocol-scoped
// (so SAML/OAuth/OIDC cookies never collide with each other either) AND
// flow-scoped by the AEAD state's own random ID -- the value each mint*
// function already generates for itself, now also returned to the caller
// so it can be threaded through as this cookie's name suffix. Naming the
// cookie by the state's own ID, rather than a single fixed name, is what
// makes two concurrent flows (two tabs, two providers) never collide:
// each flow's Set-Cookie targets a DIFFERENT name, so neither overwrites
// the other in the jar.
func loginNonceCookieName(protocol, flowID string) string {
	return "dho_" + protocol + "_login_nonce_" + flowID
}

// issueLoginNonceCookie is called at /authorize (or /initiate): sets the
// per-flow cookie holding the random login nonce, scoped to the exact
// callback path this flow's response will be posted back to (not "/" --
// narrowing exposure past what SameSite=Lax already limits), for exactly
// the state's own TTL. Secure is a literal true: paired with
// validateHTTPSOrLoopback, which refuses every admin-overridable URL this
// package could otherwise be reached at over plain http (except a
// loopback dev/test address, where Secure's own browser-side exception
// for "potentially trustworthy" localhost origins already applies).
func issueLoginNonceCookie(w http.ResponseWriter, protocol, flowID, nonce, path string, ttl time.Duration) {
	http.SetCookie(w, &http.Cookie{
		Name: loginNonceCookieName(protocol, flowID), Value: nonce, Path: path,
		MaxAge: int(ttl / time.Second), HttpOnly: true, Secure: true, SameSite: http.SameSiteLaxMode,
	})
}

// clearLoginNonceCookie expires the per-flow cookie on the wire as
// "Max-Age: 0" (D2759's own wording) -- called on BOTH the success and
// failure paths at callback time, since a login-CSRF nonce cookie has no
// reason to outlive the one callback it was minted for either way.
// net/http.Cookie's own MaxAge field is NOT the wire value directly: Go's
// doc comment on the field is explicit that MaxAge<0, not literal 0, is
// what serializes to "Max-Age: 0" ("delete cookie now"); MaxAge==0 instead
// OMITS the attribute and leaves the cookie as a session cookie -- the
// opposite of clearing it. Hence -1 here, not 0.
func clearLoginNonceCookie(w http.ResponseWriter, protocol, flowID, path string) {
	http.SetCookie(w, &http.Cookie{
		Name: loginNonceCookieName(protocol, flowID), Value: "", Path: path,
		MaxAge: -1, HttpOnly: true, Secure: true, SameSite: http.SameSiteLaxMode,
	})
}

// verifyAndClearLoginNonceCookie is called AFTER the caller has already
// verified the AEAD state authenticates -- flowID must come from that
// VERIFIED payload, never an unauthenticated claim, or naming the cookie
// by anything the caller supplies up front would defeat the whole point
// of binding it to one specific flow. Looks up the one cookie name that
// matches this exact flow, constant-time-compares its hash against the
// state's own sealed NonceHash, and clears it unconditionally on the way
// out (success or failure) via a deferred call, so the caller cannot
// forget the clear on an early return.
func verifyAndClearLoginNonceCookie(w http.ResponseWriter, r *http.Request, protocol, flowID, path, sealedHash string) (matched bool) {
	defer clearLoginNonceCookie(w, protocol, flowID, path)
	cookie, err := r.Cookie(loginNonceCookieName(protocol, flowID))
	if err != nil || cookie.Value == "" || sealedHash == "" {
		return false
	}
	return constantTimeEqual(hashLoginNonce(cookie.Value), sealedHash)
}

// hashLoginNonce mirrors pkceChallenge's shape (state.go): SHA-256,
// base64url, unpadded. Never reversed -- only ever compared, in constant
// time (constantTimeEqual, state.go), against a freshly-hashed cookie
// value.
func hashLoginNonce(nonce string) string {
	sum := sha256.Sum256([]byte(nonce))
	return base64.RawURLEncoding.EncodeToString(sum[:])
}
