package authapi

import (
	"net/http"

	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
)

// NotOwnedCredentialRoutes mounts go-api's six credential routes on one stub
// while users are not owned by auth-service, so a go-api that already proxies
// them never meets a 404 from an auth-service pod of an older phase. The stub
// answers the retryable 503 before any limiter, flow, store or mail sender.
func NotOwnedCredentialRoutes() []httpapi.Route {
	stub := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "1")
		goAPIRefusal(w, r, httpapi.CodeUnavailable)
	})
	return []httpapi.Route{
		{Method: http.MethodGet, Pattern: PatternVerifyEmail, Handler: stub},
		{Method: http.MethodPost, Pattern: PatternResendVerification, Handler: stub},
		{Method: http.MethodPost, Pattern: PatternForgotPassword, Handler: stub},
		{Method: http.MethodPost, Pattern: PatternResetPassword, Handler: stub},
		{Method: http.MethodPost, Pattern: PatternChangePassword, Handler: stub},
		{Method: http.MethodPost, Pattern: PatternRegister, Handler: stub},
	}
}
