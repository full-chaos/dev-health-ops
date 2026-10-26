package authapi

import (
	"net/http"

	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
)

// LoginNotEnabledRoute mounts go-api's local login on the retryable 503 while
// the login lockout key is unset, so a go-api that already proxies login never
// meets a 404 from an auth-service pod of another phase. It answers before any
// limiter, flow or store, so it also skips the server limiter.
func LoginNotEnabledRoute() httpapi.Route {
	return httpapi.Route{Method: http.MethodPost, Pattern: PatternCompatLogin, Handler: UnavailableRetry(), Unthrottled: true}
}
