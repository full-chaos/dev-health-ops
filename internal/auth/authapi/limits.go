package authapi

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"net/netip"
	"strconv"
	"time"

	"golang.org/x/crypto/hkdf"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/clientaddr"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// The limited routes' fixed-window limits. Every id carries the auth_svc_
// prefix: the shared store's ACL login for this service is limited to
// "dho:rl:auth_svc_*" keys and never reaches go-api's counters, so a request
// that passes go-api's limiter and then this one is counted once in each.
var (
	loginIPLimit      = httpapi.Limit{ID: "auth_svc_login_ip", Count: 20, Window: 15 * time.Minute}
	loginAccountLimit = httpapi.Limit{ID: "auth_svc_login_ip_account", Count: 10, Window: 15 * time.Minute}
	verifyLimit       = httpapi.Limit{ID: "auth_svc_verify", Count: 10, Window: time.Hour}
	resendLimit       = httpapi.Limit{ID: "auth_svc_resend_verification", Count: 3, Window: time.Hour}
	forgotLimit       = httpapi.Limit{ID: "auth_svc_forgot_password", Count: 3, Window: time.Hour}
	// resendIPLimit and forgotIPLimit cap the mail one client address can
	// start across ALL addresses it names (go-api has only the per-account
	// budget above, so one address can spray mail at many mailboxes). 20 an
	// hour: an account asks for 3, a shared address (an office, a household)
	// rarely needs more than a handful of recoveries in an hour, and the cap
	// bounds the spray at 20 mails an hour per address.
	resendIPLimit       = httpapi.Limit{ID: "auth_svc_resend_verification_ip", Count: 20, Window: time.Hour}
	forgotIPLimit       = httpapi.Limit{ID: "auth_svc_forgot_password_ip", Count: 20, Window: time.Hour}
	resetIPLimit        = httpapi.Limit{ID: "auth_svc_reset_ip", Count: 20, Window: time.Hour}
	resetTokenLimit     = httpapi.Limit{ID: "auth_svc_reset_ip_token", Count: 5, Window: time.Hour}
	changePasswordLimit = httpapi.Limit{ID: "auth_svc_change_password", Count: 5, Window: time.Hour}
	socialLinkLimit     = httpapi.Limit{ID: "auth_svc_social_link_ip_principal", Count: 10, Window: time.Hour}
)

// rateLimitKeyInfo is the HKDF info of the key that digests identifiers in the
// counter keys. The derived key is used nowhere else and never leaves this
// package: not logged, not exported, not part of any value.
const rateLimitKeyInfo = "dev-health/auth-service/ratelimit-key/v1"

// minRateLimitSecretBytes is authconfig's floor for AUTH_JWT_SECRET_KEY.
const minRateLimitSecretBytes = 32

// Limits is what a limited route counts with: the shared counter store and the
// key that digests an e-mail, a link secret or a principal id before it is
// part of a counter key. The key is HKDF-SHA256 of the session signing secret
// (AUTH_JWT_SECRET_KEY, which every limited route needs to mount), so there
// is no Secret of its own; rotating that secret resets the buckets.
type Limits struct {
	store httpapi.CounterStore
	key   []byte
}

// NewLimits returns the limits over store, keyed from secret. A nil store or a
// secret under authconfig's 32-byte floor is refused.
func NewLimits(store httpapi.CounterStore, secret string) (*Limits, error) {
	if store == nil {
		return nil, errors.New("authapi: the limits need a counter store")
	}
	if len(secret) < minRateLimitSecretBytes {
		return nil, errors.New("authapi: the limits need the session signing secret")
	}
	key := make([]byte, sha256.Size)
	if _, err := io.ReadFull(hkdf.New(sha256.New, []byte(secret), nil, []byte(rateLimitKeyInfo)), key); err != nil {
		return nil, err
	}
	return &Limits{store: store, key: key}, nil
}

// errNoLimits refuses a limited route built without limits: an in-process
// counter is per replica and so no limit at all.
var errNoLimits = errors.New("authapi: a limited route needs the shared limits (a counter store and the session signing secret)")

// digest is HMAC-SHA256 of value under the derived key, in a domain of its own
// (an e-mail and a link secret never collide), cut to 128 bits, in hex.
func (l *Limits) digest(domain, value string) string {
	mac := hmac.New(sha256.New, l.key)
	mac.Write([]byte(domain))
	mac.Write([]byte{0})
	mac.Write([]byte(value))
	return hex.EncodeToString(mac.Sum(nil)[:16])
}

// limiter is the limiter of limit over the store, with its store-error series
// declared at zero.
func (l *Limits) limiter(limit httpapi.Limit) *httpapi.KeyedLimiter {
	limiter := httpapi.NewKeyedLimiter(l.store, limit)
	limiter.Declare()
	return limiter
}

// charge is one hit on one limiter under one caller key.
type charge struct {
	limiter *httpapi.KeyedLimiter
	key     string
}

// admit counts each charge in order and reports whether the request may go
// on. The first refusal answers 429 and stops (the later limiters do not
// count it); a store error answers the generic 500 and stops, never a pass.
// The refusal names no limit and no key. route is the fixed route name the
// counter is scoped to, never the request path, so a path parameter cannot
// multiply the budget.
func admit(w http.ResponseWriter, r *http.Request, write httpapi.ErrorWriter, route string, charges ...charge) bool {
	for _, c := range charges {
		allowed, err := c.limiter.AllowCounted(r.Context(), c.key, route)
		if err != nil {
			write(w, r, httpapi.CodeInternal)
			return false
		}
		if !allowed {
			write(w, r, httpapi.CodeRateLimited)
			return false
		}
	}
	return true
}

// goAPIRefusal is the refusal of a route go-api also serves (/api/v1/auth/*):
// go-api's own bytes, apiservice.WriteError's, so a client cannot tell which
// plane answered. A rate limit is 429 {"detail":{"message":"Rate limit
// exceeded. Please try again later."}} with no Retry-After; a store error is
// the unhandled-error 500 {"detail":"Internal Server Error"} carrying the
// unhandled-error marker. The /v1 routes answer the service's own envelope
// (httpapi.WriteError) instead.
func goAPIRefusal(w http.ResponseWriter, _ *http.Request, code httpapi.Code) {
	status, body := http.StatusInternalServerError, `{"detail":"Internal Server Error"}`
	if code == httpapi.CodeRateLimited {
		status, body = http.StatusTooManyRequests, `{"detail":{"message":"Rate limit exceeded. Please try again later."}}`
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	if status == http.StatusInternalServerError {
		w.Header().Set(policy.UnhandledErrorHeader, "1")
	}
	w.WriteHeader(status)
	_, _ = io.WriteString(w, body)
}

// clientKey is the caller address the limits key on (clientaddr resolves it:
// the socket peer, or the one forwarded address a trusted proxy sent).
//
// An IPv6 client is keyed by its /64: a host holds a whole /64 and can rotate
// through it, so the address alone would hand it a fresh bucket per request.
func clientKey(r *http.Request) string {
	text := clientaddr.Text(r.Context())
	if text == nil || *text == "" {
		return "unknown"
	}
	addr, err := netip.ParseAddr(*text)
	if err != nil || !addr.Is6() || addr.Is4In6() {
		return *text
	}
	prefix, err := addr.Prefix(64)
	if err != nil {
		return *text
	}
	return prefix.String()
}

// accountKey is the client address and the digest of the normalized e-mail,
// "unknown" without one.
func (l *Limits) accountKey(r *http.Request, email string) string {
	account := "unknown"
	// The address is normalized as the flows normalize it (NFC local part,
	// lower-cased domain, display-name form reduced, Python lower and strip),
	// so every spelling of one mailbox lands in one bucket.
	normalized, err := identity.NormalizeEmail(email)
	if err != nil {
		normalized = pythonparity.Lower(pythonparity.Strip(email))
	}
	if normalized != "" {
		account = l.digest("email", normalized)
	}
	return clientKey(r) + ":" + account
}

// secretKey is the client address and the digest of a link secret.
func (l *Limits) secretKey(r *http.Request, secret string) string {
	return clientKey(r) + ":" + l.digest("link-secret", secret)
}

// principalKey is the client address and the digest of a principal id.
func (l *Limits) principalKey(r *http.Request, principal string) string {
	return clientKey(r) + ":" + l.digest("principal", principal)
}
