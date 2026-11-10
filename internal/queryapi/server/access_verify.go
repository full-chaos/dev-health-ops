package server

import (
	"log/slog"
	"net/http"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"

	"github.com/full-chaos/dev-health-go/authverify"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/accessverify"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/principal"
)

const (
	// accessTokenIssuerEnvVar is the issuer of EdDSA user access tokens, the
	// same key and value go-api reads. Unset: query-api builds no validator
	// and its bearer paths are the ones it had before.
	accessTokenIssuerEnvVar = "AUTH_ACCESS_TOKEN_ISSUER"
	// accessTokenAudienceAPI is the one audience of a user access token at
	// query-api, REST, /query and /graphql alike: the go-api audience.
	accessTokenAudienceAPI = "dev-health-api"
	accessVerifyMeterName  = "github.com/full-chaos/dev-health-ops/internal/auth/accessverify"
)

// jwksTestHook lets a test give the JWKS client its HTTP client and clock; nil
// in production.
var jwksTestHook func(*authverify.JWKSClientConfig)

// accessOptions builds one profile-routed validator.
type accessOptions struct {
	issuer string
	keys   authverify.KeySource
	denied []string
	now    func() time.Time
	meter  metric.Meter
}

func accessTokenIssuer(getenv getenvFunc) string {
	return strings.TrimSpace(getenv(accessTokenIssuerEnvVar))
}

// accessFromEnv is the validator's key source and clock when
// AUTH_ACCESS_TOKEN_ISSUER is set: a JWKS client pinned to that issuer. ok is
// false when the issuer is unset.
func accessFromEnv(getenv getenvFunc) (accessOptions, bool, error) {
	issuer := accessTokenIssuer(getenv)
	if issuer == "" {
		return accessOptions{}, false, nil
	}
	jwks := accessverify.JWKSConfig(issuer)
	if jwksTestHook != nil {
		jwksTestHook(&jwks)
	}
	keys, err := authverify.NewJWKSClient(jwks)
	if err != nil {
		return accessOptions{}, false, err
	}
	return accessOptions{issuer: issuer, keys: keys, meter: otel.Meter(accessVerifyMeterName)}, true, nil
}

// newQueryAccessValidator builds the validator over query-api's row table: an
// HS256 token is refused on every row.
func newQueryAccessValidator(opts accessOptions) (*accessverify.Validator, error) {
	verifier, err := authverify.NewVerifier(authverify.VerifierConfig{Issuer: opts.issuer, Audience: accessTokenAudienceAPI, Keys: opts.keys, DeniedKids: opts.denied})
	if err != nil {
		return nil, err
	}
	rows, err := queryAPIAccessRows()
	if err != nil {
		return nil, err
	}
	return accessverify.New(accessverify.Config{
		Rows:   rows,
		EdDSA:  verifier,
		Meter:  opts.meter,
		Logger: slog.Default(),
		Now:    opts.now,
	})
}

// routeEdgeVerifier routes a REST edge verifier's tokens by row.
func routeEdgeVerifier(edge *principal.EdgeVerifier, opts accessOptions) error {
	validator, err := newQueryAccessValidator(opts)
	if err != nil {
		return err
	}
	return edge.RouteAccess(validator, queryAPIAccessService)
}

// routeAuthenticator routes a /query or /graphql authenticator's tokens by
// row.
func routeAuthenticator(auth *policy.Authenticator, opts accessOptions) error {
	validator, err := newQueryAccessValidator(opts)
	if err != nil {
		return err
	}
	return auth.RouteAccess(principal.RoutedAccess(validator), queryAPIAccessService)
}

// routeProfile is the endpoint-profile key of the request: its method and the
// mux pattern that routed it. query-api's muxes register paths without a
// method, so the method is added here; policy is not changed for it.
func routeProfile(r *http.Request) string { return r.Method + " " + r.Pattern }

// withRouteProfile records the request's route profile before the scope
// middlewares read the caller, so they authenticate against its row.
func withRouteProfile(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		next.ServeHTTP(w, r.WithContext(policy.WithRouteProfile(r.Context(), routeProfile(r))))
	})
}

// edgeCarrierToken reports whether token goes to the edge carrier rather than
// the envelope verifier: the retired HS256 edge token always (the carrier
// refuses it alg_not_allowed); the EdDSA user access token only once auth
// routes by row.
func edgeCarrierToken(auth *policy.Authenticator, token string) bool {
	switch principal.AccessClassOf(token) {
	case principal.AccessClassEdge:
		return true
	case principal.AccessClassUser:
		return auth != nil && auth.Routed()
	}
	return false
}
