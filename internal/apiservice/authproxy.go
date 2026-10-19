package apiservice

import (
	"log/slog"
	"net/http"
	"net/url"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
)

// AuthAggregate is one routing unit of the /api/v1/auth/* proxy.
type AuthAggregate string

const (
	AuthSessions    AuthAggregate = "sessions"
	AuthLocalLogin  AuthAggregate = "local_login"
	AuthCredentials AuthAggregate = "credentials"
	AuthSocial      AuthAggregate = "social"
	AuthOIDC        AuthAggregate = "oidc"
	AuthSAML        AuthAggregate = "saml"
)

// AuthAggregates is every aggregate, in flag order.
var AuthAggregates = []AuthAggregate{AuthSessions, AuthLocalLogin, AuthCredentials, AuthSocial, AuthOIDC, AuthSAML}

// EnvAuthProxyURL is auth-service's base URL.
const EnvAuthProxyURL = "AUTH_PROXY_URL"

// AuthProxyFlagEnv is the routing flag of an aggregate.
func AuthProxyFlagEnv(AuthAggregate) string { return "" }

// AuthProxyConfig is the proxy's routing state.
type AuthProxyConfig struct {
	Upstream *url.URL
	On       map[AuthAggregate]bool
	Timeout  time.Duration
}

// Enabled reports whether a's routes go to auth-service.
func (c AuthProxyConfig) Enabled(AuthAggregate) bool { return false }

// ParseAuthProxyConfig reads the proxy settings.
func ParseAuthProxyConfig(func(string) (string, bool)) (AuthProxyConfig, error) {
	return AuthProxyConfig{}, nil
}

var (
	sessionProxyRoutes = map[string]AuthAggregate{}
	routeProxyRoutes   = map[string]AuthAggregate{}
)

type authProxy struct{}

func newAuthProxy(AuthProxyConfig, *slog.Logger) *authProxy { return &authProxy{} }

func (p *authProxy) inner(_ string, local http.Handler) http.Handler { return local }

func (p *authProxy) wrap(routes []httpapi.Route) []httpapi.Route { return routes }
