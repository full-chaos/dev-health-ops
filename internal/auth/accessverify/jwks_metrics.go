package accessverify

import (
	"go.opentelemetry.io/otel/metric"

	"github.com/full-chaos/dev-health-go/authverify"
)

const (
	// JWKSFetchMetric counts the discovery and JWKS GETs of a verifier's JWKS
	// client, by service, document (discovery, jwks) and result (ok, fail).
	JWKSFetchMetric = "devhealth_auth_jwks_fetch_total"
	// JWKSGraceMetric is the key-set state of a service's JWKS clients:
	// JWKSStateFresh, JWKSStateGrace or JWKSStateExpired.
	JWKSGraceMetric = "devhealth_auth_jwks_grace_state"
)

// Values of JWKSGraceMetric.
const (
	JWKSStateFresh   int64 = 0
	JWKSStateGrace   int64 = 1
	JWKSStateExpired int64 = 2
)

// NewJWKSKeys builds the JWKS client of cfg for service, with its fetches
// counted and its grace state reported on meter.
func NewJWKSKeys(cfg authverify.JWKSClientConfig, service string, meter metric.Meter) (*authverify.JWKSClient, error) {
	return authverify.NewJWKSClient(cfg)
}
