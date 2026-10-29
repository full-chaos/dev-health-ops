package server

import (
	"crypto/rand"
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
	keys, err := authverify.NewJWKSClient(authverify.JWKSClientConfig{Issuer: issuer})
	if err != nil {
		return accessOptions{}, false, err
	}
	return accessOptions{issuer: issuer, keys: keys, meter: otel.Meter(accessVerifyMeterName)}, true, nil
}

// newQueryAccessValidator builds the validator over query-api's row table.
// legacy nil: an HS256 token is refused on every row.
func newQueryAccessValidator(opts accessOptions, legacy accessverify.LegacyVerifier) (*accessverify.Validator, error) {
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
		Legacy: legacy,
		Meter:  opts.meter,
		Logger: slog.Default(),
		Now:    opts.now,
	})
}

// routeEdgeVerifier routes a REST edge verifier's tokens by row. legacyOn
// false (no edge secret): the validator gets no HS256 verifier.
func routeEdgeVerifier(edge *principal.EdgeVerifier, opts accessOptions, legacyOn bool) error {
	var legacy accessverify.LegacyVerifier
	if legacyOn {
		legacy = edge.LegacyAccess()
	}
	validator, err := newQueryAccessValidator(opts, legacy)
	if err != nil {
		return err
	}
	return edge.RouteAccess(validator, queryAPIAccessService)
}

// routeAuthenticator routes a /query or /graphql authenticator's tokens by
// row; its HS256 verifier is AuthenticateLegacy (policy.RouteAccess requires
// that, never Authenticate).
func routeAuthenticator(auth *policy.Authenticator, opts accessOptions, legacyOn bool) error {
	var legacy accessverify.LegacyVerifier
	if legacyOn {
		legacy = principal.LegacyVerifier(auth.AuthenticateLegacy)
	}
	validator, err := newQueryAccessValidator(opts, legacy)
	if err != nil {
		return err
	}
	return auth.RouteAccess(principal.RoutedAccess(validator), queryAPIAccessService)
}

// unusedEdgeSecret keys the HS256 half of an edge verifier built for the
// access-token issuer alone (no GO_API_EDGE_JWT_SECRET). The key is random
// and held by no minter, and nothing consults it: the routed validator of
// such a verifier has no HS256 verifier, so an HS256 token is refused first.
func unusedEdgeSecret() string { return rand.Text() + rand.Text() }

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
// the envelope verifier: the HS256 edge token always; the EdDSA user access
// token only once auth routes by row.
func edgeCarrierToken(auth *policy.Authenticator, token string) bool {
	switch principal.AccessClassOf(token) {
	case principal.AccessClassEdge:
		return true
	case principal.AccessClassUser:
		return auth != nil && auth.Routed()
	}
	return false
}
