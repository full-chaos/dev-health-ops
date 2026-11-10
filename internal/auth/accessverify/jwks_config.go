package accessverify

import (
	"github.com/full-chaos/dev-health-go/authverify"
)

// JWKSConfig is the JWKS client config go-api and query-api build their
// access-token key source from.
func JWKSConfig(issuer string) authverify.JWKSClientConfig {
	return authverify.JWKSClientConfig{Issuer: issuer}
}
