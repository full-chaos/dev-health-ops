package apiservice

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"go.opentelemetry.io/otel"

	"github.com/full-chaos/dev-health-go/authverify"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/apiservice/acr"
	"github.com/full-chaos/dev-health-ops/internal/auth/accessverify"
)

const (
	envAccessTokenIssuer   = "AUTH_ACCESS_TOKEN_ISSUER"
	accessTokenAudienceAPI = "dev-health-api"
	// envACRWorkloadSubject pins the one workload principal (prn_) the
	// internal acr routes accept. An interim pin: the workload registry
	// replaces it (CHAOS-10980).
	envACRWorkloadSubject = "AUTH_ACR_WORKLOAD_SUBJECT"
)

// buildAccessVerify constructs the profile-routed access-token validator.
// AUTH_ACCESS_TOKEN_ISSUER unset: no capability (nil). The row table is empty
// until a route adopts the validator, so every Validate call is refused.
//
// It also returns the JWKS client it built, so the workload verifier of the
// internal acr routes reads the same keys (nil when the issuer is unset).
func buildAccessVerify(lookup func(string) (string, bool), auth *policy.Authenticator, logger *slog.Logger) (*accessverify.Validator, authverify.KeySource, error) {
	issuer, _ := lookup(envAccessTokenIssuer)
	issuer = strings.TrimSpace(issuer)
	if issuer == "" {
		return nil, nil, nil
	}
	keys, err := authverify.NewJWKSClient(authverify.JWKSClientConfig{Issuer: issuer})
	if err != nil {
		return nil, nil, err
	}
	verifier, err := authverify.NewVerifier(authverify.VerifierConfig{Issuer: issuer, Audience: accessTokenAudienceAPI, Keys: keys})
	if err != nil {
		return nil, nil, err
	}
	cfg := accessverify.Config{
		EdDSA:  verifier,
		Meter:  otel.Meter("github.com/full-chaos/dev-health-ops/internal/auth/accessverify"),
		Logger: logger,
	}
	if auth != nil {
		cfg.Legacy = legacyVerifier(auth.Authenticate)
	}
	validator, err := accessverify.New(cfg)
	return validator, keys, err
}

// buildACRWorkload builds the workload-token verifier of the internal acr
// routes: the access-token issuer and keys, audience dev-health-api only (a
// token for any other audience is refused), leeway 0. No issuer or no
// AUTH_ACR_WORKLOAD_SUBJECT means no workload path (nil): the routes then
// refuse every workload token as not_configured and svc_acr_ is unchanged.
func buildACRWorkload(lookup func(string) (string, bool), keys authverify.KeySource) (*acr.WorkloadAuth, error) {
	issuer, _ := lookup(envAccessTokenIssuer)
	issuer = strings.TrimSpace(issuer)
	subject, _ := lookup(envACRWorkloadSubject)
	subject = strings.TrimSpace(subject)
	if issuer == "" || keys == nil || subject == "" {
		return nil, nil
	}
	if !strings.HasPrefix(subject, "prn_") || len(subject) > 255 {
		return nil, fmt.Errorf("%s is not a prn_ principal id", envACRWorkloadSubject)
	}
	verifier, err := authverify.NewWorkloadVerifier(authverify.WorkloadVerifierConfig{Issuer: issuer, Audience: accessTokenAudienceAPI, Keys: keys})
	if err != nil {
		return nil, err
	}
	return &acr.WorkloadAuth{Verifier: verifier, Subject: subject}, nil
}

func legacyVerifier(authenticate func(context.Context, string) (*policy.User, error)) accessverify.LegacyVerifier {
	return func(ctx context.Context, token string) (any, error) {
		user, err := authenticate(ctx, token)
		if errors.Is(err, policy.ErrUnavailable) {
			return nil, fmt.Errorf("%w: %w", accessverify.ErrUnavailable, err)
		}
		return user, err
	}
}
