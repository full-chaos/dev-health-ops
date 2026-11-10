package apiservice

import (
	"fmt"
	"log/slog"
	"regexp"
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
)

// jwksTestHook lets a test give the JWKS client its HTTP client and clock; nil
// in production.
var jwksTestHook func(*authverify.JWKSClientConfig)

// buildAccessVerify constructs the profile-routed access-token validator over
// the FULL go-api endpoint-profile row table. It is go-api's only access-token
// verifier (the HS256 class is retired), so with protected routes (auth, i.e.
// API_DATABASE_URI set) AUTH_ACCESS_TOKEN_ISSUER is required; with none, an
// unset issuer builds nothing (nil).
//
// It also returns the JWKS client it built, so the workload verifier of the
// internal acr routes reads the same keys (nil when the issuer is unset).
func buildAccessVerify(lookup func(string) (string, bool), auth *policy.Authenticator, logger *slog.Logger) (*accessverify.Validator, authverify.KeySource, error) {
	issuer, _ := lookup(envAccessTokenIssuer)
	issuer = strings.TrimSpace(issuer)
	if issuer == "" {
		if auth != nil {
			return nil, nil, fmt.Errorf("%s is required by dho api when API_DATABASE_URI is set: it is the only access-token verifier", envAccessTokenIssuer)
		}
		return nil, nil, nil
	}
	jwks := accessverify.JWKSConfig(issuer)
	if jwksTestHook != nil {
		jwksTestHook(&jwks)
	}
	keys, err := authverify.NewJWKSClient(jwks)
	if err != nil {
		return nil, nil, err
	}
	verifier, err := authverify.NewVerifier(authverify.VerifierConfig{Issuer: issuer, Audience: accessTokenAudienceAPI, Keys: keys})
	if err != nil {
		return nil, nil, err
	}
	rows, err := goAPIAccessRows()
	if err != nil {
		return nil, nil, err
	}
	cfg := accessverify.Config{
		Rows:   rows,
		EdDSA:  verifier,
		Meter:  otel.Meter("github.com/full-chaos/dev-health-ops/internal/auth/accessverify"),
		Logger: logger,
	}
	validator, err := accessverify.New(cfg)
	return validator, keys, err
}

// principalIDPattern is the principal id of the credential contracts
// (contracts/auth/v1/jsonschema/credential-entry.v1.schema.json).
var principalIDPattern = regexp.MustCompile(`^prn_[A-Za-z0-9_-]{1,64}$`)

// buildACRWorkload builds the workload-token verifier of the internal acr
// routes: the access-token issuer and keys, audience dev-health-api only (a
// token for any other audience is refused), leeway 0. The principal it accepts
// is the source's (the workload registry). No issuer, no key source or no
// source means no workload path (nil): the routes then refuse every workload
// token as not_configured, so they serve no bearer.
func buildACRWorkload(lookup func(string) (string, bool), keys authverify.KeySource, source acr.SubjectSource) (*acr.WorkloadAuth, error) {
	issuer, _ := lookup(envAccessTokenIssuer)
	issuer = strings.TrimSpace(issuer)
	if issuer == "" || keys == nil || source == nil {
		return nil, nil
	}
	verifier, err := authverify.NewWorkloadVerifier(authverify.WorkloadVerifierConfig{Issuer: issuer, Audience: accessTokenAudienceAPI, Keys: keys})
	if err != nil {
		return nil, err
	}
	return &acr.WorkloadAuth{Verifier: verifier, Source: source}, nil
}
