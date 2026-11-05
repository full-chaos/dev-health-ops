package authruntime

import (
	"encoding/base64"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/auth/authapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
	"github.com/full-chaos/dev-health-ops/internal/auth/clientaddr"
)

// The well-known CIDR exception names the same two paths the well-known routes mount.
func TestWellKnownExceptionPathsAreTheMountedPatterns(t *testing.T) {
	if clientaddr.WellKnownJWKSPath != authapi.PatternJWKS || clientaddr.WellKnownDiscoveryPath != authapi.PatternDiscovery {
		t.Fatalf("exception paths %q %q, mounted patterns %q %q", clientaddr.WellKnownJWKSPath, clientaddr.WellKnownDiscoveryPath,
			authapi.PatternJWKS, authapi.PatternDiscovery)
	}
}

// In mode signed the caller request id is logged only for a request with a valid proof, never on the CIDR rule alone.
func TestCallerRequestIDFollowsTheClientAddrMode(t *testing.T) {
	entry := "k1:" + base64.RawURLEncoding.EncodeToString([]byte(strings.Repeat("r", 32)))
	keys, err := clientaddr.ParseProofKeys(clientaddr.EnvVerifyKey, entry, clientaddr.MaxVerifyKeys)
	if err != nil {
		t.Fatal(err)
	}
	trust, err := clientaddr.ParseTrust("10.42.0.0/24,!10.42.0.0,!10.42.0.1")
	if err != nil {
		t.Fatal(err)
	}
	const id = "0f6d6c1e-2f43-4d8e-9a43-7e7c8f0b1d2a"
	build := func(peer string, proof bool) *http.Request {
		r := httptest.NewRequest(http.MethodPost, "/api/v1/auth/refresh", nil)
		r.RemoteAddr = peer + ":1"
		r.Header.Set("X-Request-Id", id)
		r.Header.Set("X-Forwarded-For", "203.0.113.9")
		if proof {
			r.Header.Set(clientaddr.ProofHeader, clientaddr.Prove(keys[0], time.Now(), netip.MustParseAddr("203.0.113.9"), r.Method, r.RequestURI, id))
		}
		return r
	}
	for _, c := range []struct {
		mode  clientaddr.Mode
		peer  string
		proof bool
		want  string
	}{
		{clientaddr.ModeCIDR, "10.42.0.7", false, id},
		{clientaddr.ModeCIDR, "192.0.2.50", true, untrustedCallerID},
		{clientaddr.ModeBoth, "192.0.2.50", true, id},
		{clientaddr.ModeSigned, "10.42.0.7", false, untrustedCallerID},
		{clientaddr.ModeSigned, "192.0.2.50", true, id},
	} {
		cfg := authconfig.Config{TrustedProxies: trust, ClientAddrMode: c.mode, ClientAddrKeys: keys}
		policy := clientAddrPolicy(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
		if got := callerRequestID(policy, build(c.peer, c.proof)); got != c.want {
			t.Errorf("mode %s, peer %s, proof %v: caller id %q, want %q", c.mode, c.peer, c.proof, got, c.want)
		}
	}
}
