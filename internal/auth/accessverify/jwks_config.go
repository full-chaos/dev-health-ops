package accessverify

import (
	"time"

	"github.com/full-chaos/dev-health-go/authverify"
)

// JWKS client bounds of go-api and query-api. While fetches succeed, a kid
// that auth-service's JWKS drops is refused at most JWKSCacheTTL +
// JWKSFetchTimeout after the drop. A failed fetch serves only the last
// successful key set, for JWKSOutageGrace past the TTL.
const (
	JWKSCacheTTL     = 5 * time.Second
	JWKSFetchTimeout = 2 * time.Second
	JWKSRefreshFloor = time.Second
	JWKSOutageGrace  = 10 * time.Minute
)

// JWKSConfig is the JWKS client config go-api and query-api build their
// access-token key source from.
func JWKSConfig(issuer string) authverify.JWKSClientConfig {
	return authverify.JWKSClientConfig{
		Issuer:       issuer,
		Timeout:      JWKSFetchTimeout,
		CacheTTL:     JWKSCacheTTL,
		RefreshFloor: JWKSRefreshFloor,
		OutageGrace:  JWKSOutageGrace,
	}
}
