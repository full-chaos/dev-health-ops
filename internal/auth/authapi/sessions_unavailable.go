package authapi

import (
	"net/http"

	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
)

// SessionsNotEnabledRoutes mounts go-api's refresh and logout on the retryable
// 503 while the session secrets are unset, so a go-api that already proxies
// them never meets a 404 from an auth-service pod of another phase. They answer
// before any limiter, flow or store, so they also skip the server limiter. The
// session issue route is not stubbed: it keeps its service-credential check.
func SessionsNotEnabledRoutes() []httpapi.Route {
	stub := UnavailableRetry()
	return []httpapi.Route{
		{Method: http.MethodPost, Pattern: PatternCompatRefresh, Handler: stub, Unthrottled: true},
		{Method: http.MethodPost, Pattern: PatternCompatLogout, Handler: stub, Unthrottled: true},
	}
}
