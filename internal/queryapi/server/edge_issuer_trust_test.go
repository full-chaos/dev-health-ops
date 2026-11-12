package server

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/full-chaos/dev-health-go/authverify"
	"go.opentelemetry.io/otel/metric"

	"github.com/full-chaos/dev-health-ops/internal/httpguard"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/issuerstub"
)

// trustEdgeIssuerJWKS makes the JWKS client Build makes trust an in-test issuer whose certificate chains to roots, until
// the test ends. It hands that client its own transport through jwksTestHook: http.DefaultTransport is not touched,
// because httpguard.SharedTransport (the JWKS client's transport in production) copies it once per process, so a later
// test's CA would never reach it (CHAOS-13613).
func trustEdgeIssuerJWKS(t *testing.T, roots *x509.CertPool) {
	t.Helper()
	if jwksTestHook != nil {
		t.Fatal("jwksTestHook is already set: one in-test issuer per test")
	}
	trusting := httpguard.Transport()
	if trusting.TLSClientConfig == nil {
		trusting.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	trusting.TLSClientConfig.RootCAs = roots
	jwksTestHook = func(cfg *authverify.JWKSClientConfig, _ *metric.Meter) {
		cfg.HTTPClient = &http.Client{Transport: trusting}
	}
	t.Cleanup(func() {
		jwksTestHook = nil
		trusting.CloseIdleConnections()
	})
}

// CHAOS-13613: a test that trusts its issuer must still be trusted when an earlier test in the same process already
// made the process-wide outbound transport (httpguard.SharedTransport is built once, from http.DefaultTransport as it
// was then). The venue oracles TestGraphQLEdgeFrozenVenueOracle then TestMCPClassParityVenueOracle failed that way:
// the second issuer's CA never reached the JWKS client, and every token read as a 503.
func TestEdgeIssuerTrustSurvivesAnEarlierSharedTransport(t *testing.T) {
	_ = httpguard.SharedTransport()

	pub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	jwks, err := json.Marshal(map[string]any{"keys": []map[string]any{{
		"kty": "OKP", "crv": "Ed25519", "kid": "kid-trust", "use": "sig", "alg": "EdDSA",
		"x": base64.RawURLEncoding.EncodeToString(pub),
	}}})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewUnstartedServer(nil)
	srv.StartTLS()
	t.Cleanup(srv.Close)
	srv.Config.Handler = issuerstub.Handler(srv.URL, jwks)
	roots := x509.NewCertPool()
	roots.AddCert(srv.Certificate())

	trustEdgeIssuerJWKS(t, roots)

	opts, ok, err := accessFromEnv(func(name string) string {
		if name == accessTokenIssuerEnvVar {
			return srv.URL
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
	if key, err := resolver.ResolveKey("kid-trust"); key == nil || err != nil {
		t.Fatalf("the trusted issuer's key did not resolve (key %v): %v", key != nil, err)
	}
}
