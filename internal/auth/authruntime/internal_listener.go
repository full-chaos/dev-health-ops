package authruntime

import (
	"context"
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

// internalOnlyRoute is one internalOnlyRoutes entry. An optional route mounts only when its configuration is set, so
// its absence is not a dead key: auth-service starts without it (no start refusal for an unset optional route).
type internalOnlyRoute struct{ optional bool }

// internalOnlyRoutes ("METHOD pattern" keys) are served on the internal listener and never on the public API listener.
// The acr credential events route is a workload-to-auth-service call (acr-api reaches auth-service on the internal
// listener only); it mounts only with AUTH_ACR_EVENTS_WORKLOAD_SUBJECT and its endpoint-profile row.
var internalOnlyRoutes = map[string]internalOnlyRoute{
	http.MethodPost + " " + authapi.PatternAcrCredentialEvents: {optional: true},
}

func isInternalOnly(key string) bool {
	_, ok := internalOnlyRoutes[key]
	return ok
}

// publicRoutes drops every internalOnlyRoutes key, so an internal-only route never reaches the public listener.
func publicRoutes(routes []httpapi.Route) []httpapi.Route {
	var kept []httpapi.Route
	for _, route := range routes {
		if !isInternalOnly(route.Method + " " + route.Pattern) {
			kept = append(kept, route)
		}
	}
	return kept
}

// checkInternalOnlyRoutes fails when a required internalOnlyRoutes key is no registered route (a dead key would be
// silent). An optional key may be unregistered: its route is off by configuration.
func checkInternalOnlyRoutes(routes []httpapi.Route) error {
	registered := map[string]bool{}
	for _, route := range routes {
		registered[route.Method+" "+route.Pattern] = true
	}
	for key, route := range internalOnlyRoutes {
		if !registered[key] && !route.optional {
			return fmt.Errorf("internal-only route %q is not a registered route", key)
		}
	}
	return nil
}

// warnUnservedInternalOnly logs one WARN per mounted internal-only route when the internal listener is off: the
// route is then served on no listener. auth-service still starts and readiness does not change (no start refusal for
// config hygiene).
func warnUnservedInternalOnly(ctx context.Context, logger *slog.Logger, cfg authconfig.Config, routes []httpapi.Route) {
	if cfg.InternalAddress != "" {
		return
	}
	for _, route := range routes {
		if isInternalOnly(route.Method + " " + route.Pattern) {
			logger.WarnContext(ctx, "auth internal-only route served on no listener: AUTH_SERVICE_INTERNAL_HTTP_ADDR is unset",
				"route", route.Pattern)
		}
	}
}

// internalServerOptions is the internal listener: the API listener's options over internalRoutes and internalOnlyRoutes, on
// cfg.InternalAddress, with no trusted proxy. Its callers are identified by their tokens, so the client address is
// always the TCP peer and a forwarded X-Forwarded-For or X-Request-ID is never believed (cfg.TrustedProxies is the
// API listener's alone). It has no publicaudit middleware: nothing forwards a request id to it.
func internalServerOptions(cfg authconfig.Config, logger *slog.Logger, routes []httpapi.Route) httpapi.ServerOptions {
	var served []httpapi.Route
	for _, route := range routes {
		key := route.Method + " " + route.Pattern
		if internalRoutes[key] || isInternalOnly(key) {
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
