package authapi

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"io"
	"testing"

	"golang.org/x/crypto/hkdf"

	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
)

// sessionLimitsSuccessorKey is a made-up AUTH_SESSION_SUCCESSOR_KEY (32 bytes, base64url without padding).
var sessionLimitsSuccessorKey = base64.RawURLEncoding.EncodeToString([]byte("session-limits-successor-key-032"))

// hkdfSubkey is HKDF-SHA256 (empty salt) of secret under info, computed here and not by the package.
func hkdfSubkey(t *testing.T, secret []byte, info string) []byte {
	t.Helper()
	key := make([]byte, sha256.Size)
	if _, err := io.ReadFull(hkdf.New(sha256.New, secret, nil, []byte(info)), key); err != nil {
		t.Fatal(err)
	}
	return key
}

// AUTH_RATE_LIMIT_KEY set: the limiter key is the one main's explicit-key path derives (the pinned vector of
// TestTheDigestDerivationIsPinned), whatever the successor key holds.
func TestSessionLimitsWithTheRateLimitKeyKeepTheExplicitKeyPath(t *testing.T) {
	limits, err := NewSessionLimits(httpapi.NewMemoryCounters(nil), testRateLimitSecret, sessionLimitsSuccessorKey)
	if err != nil {
		t.Fatal(err)
	}
	if got := hex.EncodeToString(limits.key); got != "700764053683047808a60fe857857ef78898fddd8e781c25f281363770d34ad7" {
		t.Fatalf("limiter key with AUTH_RATE_LIMIT_KEY set = %s, want the explicit-key path's", got)
	}
}

// AUTH_RATE_LIMIT_KEY unset: the limiter key is the HKDF subkey of the successor key under the limiter's own label
// (dev-health/auth-service/ratelimit-key/v1). It is never the successor key itself (text or bytes), never the subkey
// under the successor key's own label (dh/refresh-successor/v1, session.deriveSuccessor's HMAC label), and it is the
// same on every build, so replicas share their buckets.
func TestSessionLimitsWithoutTheRateLimitKeyUseTheSuccessorKeysOwnSubkey(t *testing.T) {
	store := httpapi.NewMemoryCounters(nil)
	first, err := NewSessionLimits(store, "", sessionLimitsSuccessorKey)
	if err != nil {
		t.Fatalf("the limits without AUTH_RATE_LIMIT_KEY are refused: %v", err)
	}
	second, err := NewSessionLimits(store, "", sessionLimitsSuccessorKey)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first.key, hkdfSubkey(t, []byte(sessionLimitsSuccessorKey), "dev-health/auth-service/ratelimit-key/v1")) {
		t.Fatal("the limiter key is not HKDF(successor key, dev-health/auth-service/ratelimit-key/v1)")
	}
	if !bytes.Equal(first.key, second.key) {
		t.Fatal("two builds over one successor key derive two limiter keys")
	}
	raw, err := base64.RawURLEncoding.DecodeString(sessionLimitsSuccessorKey)
	if err != nil {
		t.Fatal(err)
	}
	successorMAC := hmac.New(sha256.New, raw)
	successorMAC.Write([]byte("dh/refresh-successor/v1"))
	for name, other := range map[string][]byte{
		"the successor key text":               []byte(sessionLimitsSuccessorKey),
		"the successor key bytes":              raw,
		"HKDF(text, dh/refresh-successor/v1)":  hkdfSubkey(t, []byte(sessionLimitsSuccessorKey), "dh/refresh-successor/v1"),
		"HKDF(bytes, dh/refresh-successor/v1)": hkdfSubkey(t, raw, "dh/refresh-successor/v1"),
		"HMAC(bytes, dh/refresh-successor/v1)": successorMAC.Sum(nil),
	} {
		if bytes.Equal(first.key, other) {
			t.Errorf("the limiter key equals %s", name)
		}
	}
}
