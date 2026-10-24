package apiservice

import (
	"context"
	"log/slog"
	"strings"

	"go.opentelemetry.io/otel"

	"github.com/full-chaos/dev-health-go/authverify"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/accessverify"
)

const (
	envAccessTokenIssuer   = "AUTH_ACCESS_TOKEN_ISSUER"
	accessTokenAudienceAPI = "dev-health-api"
)

// buildAccessVerify constructs the profile-routed access-token validator.
// AUTH_ACCESS_TOKEN_ISSUER unset: no capability (nil). The row table is empty
// until a route adopts the validator, so every Validate call is refused.
func buildAccessVerify(lookup func(string) (string, bool), auth *policy.Authenticator, logger *slog.Logger) (*accessverify.Validator, error) {
	issuer, _ := lookup(envAccessTokenIssuer)
	issuer = strings.TrimSpace(issuer)
	if issuer == "" {
		return nil, nil
	}
	keys, err := authverify.NewJWKSClient(authverify.JWKSClientConfig{Issuer: issuer})
	if err != nil {
		return nil, err
	}
	verifier, err := authverify.NewVerifier(authverify.VerifierConfig{Issuer: issuer, Audience: accessTokenAudienceAPI, Keys: keys})
	if err != nil {
		return nil, err
	}
	cfg := accessverify.Config{
		EdDSA:  verifier,
		Meter:  otel.Meter("github.com/full-chaos/dev-health-ops/internal/auth/accessverify"),
		Logger: logger,
	}
	if auth != nil {
		cfg.Legacy = func(ctx context.Context, token string) (any, error) { return auth.Authenticate(ctx, token) }
	}
	return accessverify.New(cfg)
}
