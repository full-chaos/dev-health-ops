package authruntime

import (
	"fmt"
	"log/slog"
	"net/http"

	"github.com/full-chaos/dev-health-ops/internal/auth/authapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
	"github.com/full-chaos/dev-health-ops/internal/auth/clientaddr"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/requestid"
	"github.com/full-chaos/dev-health-ops/internal/auth/workloadexchange"
)

// internalRoutes are the only routes the internal listener serves: the service-to-service calls of the policy
// enforcement points, each made with the caller's own token (the workload-token exchange and the authorize routes).
var internalRoutes = map[string]bool{
	http.MethodPost + " " + workloadexchange.Pattern:      true,
	http.MethodPost + " " + authapi.PatternAuthorize:      true,
	http.MethodPost + " " + authapi.PatternAuthorizeBatch: true,
}

// internalOnlyRoutes ("METHOD pattern" keys) are served on the internal listener and never on the public API listener;
// empty until a route needs it.
var internalOnlyRoutes = map[string]bool{}

// publicRoutes drops every internalOnlyRoutes key, so an internal-only route never reaches the public listener.
func publicRoutes(routes []httpapi.Route) []httpapi.Route {
	var kept []httpapi.Route
	for _, route := range routes {
		if !internalOnlyRoutes[route.Method+" "+route.Pattern] {
			kept = append(kept, route)
		}
	}
	return kept
}

// checkInternalOnlyRoutes fails when an internalOnlyRoutes key is no registered route (a dead key would be silent).
func checkInternalOnlyRoutes(routes []httpapi.Route) error {
	registered := map[string]bool{}
	for _, route := range routes {
		registered[route.Method+" "+route.Pattern] = true
	}
	for key := range internalOnlyRoutes {
		if !registered[key] {
			return fmt.Errorf("internal-only route %q is not a registered route", key)
		}
	}
	return nil
}

// internalServerOptions is the internal listener: the API listener's options over internalRoutes alone, on
// cfg.InternalAddress, with no trusted proxy. Its callers are identified by their tokens, so the client address is
// always the TCP peer and a forwarded X-Forwarded-For or X-Request-ID is never believed (cfg.TrustedProxies is the
// API listener's alone). It has no publicaudit middleware: nothing forwards a request id to it.
func internalServerOptions(cfg authconfig.Config, logger *slog.Logger, routes []httpapi.Route) httpapi.ServerOptions {
	var served []httpapi.Route
	for _, route := range routes {
		key := route.Method + " " + route.Pattern
		if internalRoutes[key] || internalOnlyRoutes[key] {
			served = append(served, route)
		}
	}
	options := apiServerOptions(cfg, logger, served)
	noProxy := clientaddr.Policy{Mode: clientaddr.ModeCIDR}
	options.Address = cfg.InternalAddress
	options.Name = internalHTTPName
	options.Listener = "internal"
	options.Middleware = []func(http.Handler) http.Handler{requestid.Middleware, clientaddr.PolicyMiddleware(noProxy)}
	options.AccessAttrs = accessLogIDs(noProxy)
	return options
}

// internalHTTPName is the internal listener's component name.
const internalHTTPName = "auth-internal-http"
