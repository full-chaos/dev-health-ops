package authruntime

import (
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

// internalServerOptions is the internal listener: the API listener's options over internalRoutes alone, on
// cfg.InternalAddress, with no trusted proxy. Its callers are identified by their tokens, so the client address is
// always the TCP peer and a forwarded X-Forwarded-For or X-Request-ID is never believed (cfg.TrustedProxies is the
// API listener's alone). It has no publicaudit middleware: nothing forwards a request id to it.
func internalServerOptions(cfg authconfig.Config, logger *slog.Logger, routes []httpapi.Route) httpapi.ServerOptions {
	var served []httpapi.Route
	for _, route := range routes {
		if internalRoutes[route.Method+" "+route.Pattern] {
			served = append(served, route)
		}
	}
	options := apiServerOptions(cfg, logger, served)
	var noProxy clientaddr.Trust
	options.Address = cfg.InternalAddress
	options.Name = internalHTTPName
	options.Listener = "internal"
	options.Middleware = []func(http.Handler) http.Handler{requestid.Middleware, clientaddr.Middleware(noProxy)}
	options.AccessAttrs = accessLogIDs(noProxy)
	return options
}

// internalHTTPName is the internal listener's component name.
const internalHTTPName = "auth-internal-http"
