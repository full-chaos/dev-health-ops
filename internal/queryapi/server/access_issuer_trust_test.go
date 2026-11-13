package server

import (
	"testing"

	"github.com/full-chaos/dev-health-go/authverify"

	"github.com/full-chaos/dev-health-ops/internal/httpguard"
)

// startAccessIssuer's trust must reach the JWKS client when an earlier test in the same process already built
// httpguard.SharedTransport, which copies http.DefaultTransport once per process.
func TestAccessIssuerTrustSurvivesAnEarlierSharedTransport(t *testing.T) {
	_ = httpguard.SharedTransport()

	minter := newAccessMinter(t)
	issuer := startAccessIssuer(t, minter)

	opts, ok, err := accessFromEnv(func(name string) string {
		if name == accessTokenIssuerEnvVar {
			return issuer
		}
		return ""
	})
	if err != nil || !ok {
		t.Fatalf("accessFromEnv: ok %v err %v", ok, err)
	}
	resolver, isResolver := opts.keys.(authverify.KeyResolver)
	if !isResolver {
		t.Fatalf("key source %T is not a KeyResolver", opts.keys)
	}
	if len(minter.keys) == 0 {
		t.Fatal("the minter has no key")
	}
	for kid := range minter.keys {
		if key, err := resolver.ResolveKey(kid); key == nil || err != nil {
			t.Fatalf("the trusted issuer's key %q did not resolve (key %v): %v", kid, key != nil, err)
		}
	}
}
