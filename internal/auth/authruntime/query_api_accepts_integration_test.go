//go:build integration

package authruntime

import (
	"context"
	"crypto/ed25519"
	"crypto/x509"
	"encoding/pem"
	"os"
	"testing"

	"github.com/full-chaos/dev-health-go/authverify"
	"go.opentelemetry.io/otel/metric/noop"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/accessverify"
	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/principal"
)

// queryAPIProfile is a query-api REST row that takes the user access token.
const queryAPIProfile = "GET /api/v1/home"

// queryAPIVerifier is query-api's REST access check as its production builder makes it, over this stack: the
// EdDSA verifier for the auth-service issuer and its signing key, audience dev-health-api, routed by the row,
// then the live users row, session and membership through the go-api tables. It is the consumer that must
// accept what auth-service mints.
func queryAPIVerifier(t *testing.T, cfg authconfig.Config, store policy.Store) *principal.EdgeVerifier {
	t.Helper()
	if cfg.TokenIssuerURL == "" || cfg.SigningKeyPath == "" || cfg.SigningKeyID == "" {
		t.Fatal("query-api's verifier needs the stack's issuer URL and signing key: build the env with newEdDSARoutesEnv")
	}
	raw, err := os.ReadFile(cfg.SigningKeyPath)
	if err != nil {
		t.Fatal(err)
	}
	block, _ := pem.Decode(raw)
	if block == nil {
		t.Fatal("signing key is not PEM")
	}
	parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	private, ok := parsed.(ed25519.PrivateKey)
	if !ok {
		t.Fatal("signing key is not Ed25519")
	}
	eddsa, err := authverify.NewVerifier(authverify.VerifierConfig{Issuer: cfg.TokenIssuerURL, Audience: "dev-health-api",
		Keys: authverify.StaticKeys{cfg.SigningKeyID: private.Public().(ed25519.PublicKey)}})
	if err != nil {
		t.Fatal(err)
	}
	validator, err := accessverify.New(accessverify.Config{
		Rows:  []accessverify.Row{{Service: "dev-health-query-api", Profile: queryAPIProfile, AcceptsUser: true}},
		EdDSA: eddsa, Meter: noop.NewMeterProvider().Meter("authruntime-test"),
	})
	if err != nil {
		t.Fatal(err)
	}
	// This branch's constructor still takes the HS256 secret; VerifyRouted never reads it (CHAOS-13197 removes it).
	edge, err := principal.NewEdgeVerifier(testAccessSecret, "dev-health-ops", "dev-health-api", store)
	if err != nil {
		t.Fatal(err)
	}
	if err := edge.RouteAccess(validator, "dev-health-query-api"); err != nil {
		t.Fatal(err)
	}
	return edge
}

// queryAPIAccepts is query-api's answer for token on its REST row.
func (e *routesEnv) queryAPIAccepts(ctx context.Context, token string) (*principal.EdgeClaims, error) {
	return e.edge.VerifyRouted(ctx, queryAPIProfile, token)
}
