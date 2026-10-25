package apiservice

import (
	"context"
	"fmt"
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

// buildAccessVerify constructs the profile-routed access-token validator over
// the FULL go-api endpoint-profile row table. AUTH_ACCESS_TOKEN_ISSUER unset:
// no capability (nil), and go-api's access-token path is unchanged.
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
	rows, err := goAPIAccessRows()
	if err != nil {
		return nil, err
	}
	cfg := accessverify.Config{
		Rows:   rows,
		EdDSA:  verifier,
		Meter:  otel.Meter("github.com/full-chaos/dev-health-ops/internal/auth/accessverify"),
		Logger: logger,
	}
	if auth != nil {
		cfg.Legacy = legacyVerifier(auth.AuthenticateLegacy)
	}
	return accessverify.New(cfg)
}

// legacyVerifier maps the HS256 path's errors (acp D3): a refusal and a claim
// of the wrong type are refusals (401); any other store failure is
// unavailable (503), never a pass and never a refusal.
func legacyVerifier(authenticate func(context.Context, string) (*policy.User, error)) accessverify.LegacyVerifier {
	return func(ctx context.Context, token string) (any, error) {
		user, err := authenticate(ctx, token)
		switch {
		case err == nil:
			return user, nil
		case policy.IsRefusal(err), policy.IsClaimTypeError(err):
			return nil, err
		default:
			return nil, fmt.Errorf("%w: %w", accessverify.ErrUnavailable, err)
		}
	}
}
