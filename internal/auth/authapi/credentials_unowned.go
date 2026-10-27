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
	stub := UnavailableRetry()
	return []httpapi.Route{
		{Method: http.MethodGet, Pattern: PatternVerifyEmail, Handler: stub, Unthrottled: true},
		{Method: http.MethodPost, Pattern: PatternResendVerification, Handler: stub, Unthrottled: true},
		{Method: http.MethodPost, Pattern: PatternForgotPassword, Handler: stub, Unthrottled: true},
		{Method: http.MethodPost, Pattern: PatternResetPassword, Handler: stub, Unthrottled: true},
		{Method: http.MethodPost, Pattern: PatternChangePassword, Handler: stub, Unthrottled: true},
		{Method: http.MethodPost, Pattern: PatternRegister, Handler: stub, Unthrottled: true},
	}
}

// UnavailableRetry answers go-api's 503 unavailable shape (goAPIRefusal, which carries Retry-After: 1).
// It reads no body and touches no limiter, flow, store or mail sender.
func UnavailableRetry() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		goAPIRefusal(w, r, httpapi.CodeUnavailable)
	})
}
