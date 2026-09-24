package session

import (
	"context"
	"net/http"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/ratelimit"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
)

// The session routes' @limiter.limit decorators (api/middleware/rate_limit.py
// AUTH_LOGIN_IP_LIMIT, AUTH_REFRESH_LIMIT, AUTH_VALIDATE_LIMIT). Each runs
// through httpapi.ValidateThenLimit: FastAPI validates the body before
// slowapi counts the request, so a refused body costs nothing.
var (
	loginLimit    = httpapi.Limit{ID: "auth_login_ip", Count: 20, Window: 15 * time.Minute}
	refreshLimit  = httpapi.Limit{ID: "auth_refresh", Count: 10, Window: 15 * time.Minute}
	validateLimit = httpapi.Limit{ID: "auth_validate", Count: 30, Window: 15 * time.Minute}
	// verify.py and password_reset.py's literal limits, keyed by
	// get_auth_key; reset-password has none.
	verifyLimit = httpapi.Limit{ID: "auth_verify", Count: 10, Window: time.Hour}
	resendLimit = httpapi.Limit{ID: "auth_resend_verification", Count: 3, Window: time.Hour}
	forgotLimit = httpapi.Limit{ID: "auth_forgot_password", Count: 3, Window: time.Hour}
)

// RegisterLimitID names the register route's counter; AUTH_REGISTER_LIMIT
// sets its count and window.
const RegisterLimitID = "auth_register"

// DefaultRegisterLimit is AUTH_REGISTER_LIMIT's default, "3/hour", keyed by
// get_forwarded_ip.
var DefaultRegisterLimit = httpapi.Limit{ID: RegisterLimitID, Count: 3, Window: time.Hour}

// bodyAuthKey is get_auth_key for a route whose JSON body FastAPI read.
func bodyAuthKey(r *http.Request) string {
	body, _ := policy.BodyFrom(r.Context())
	return authKey(r, body.Value)
}

// queryAuthKey is get_auth_key for a GET route: no body was read, so only
// the query parameter can name the e-mail.
func queryAuthKey(r *http.Request) string { return authKey(r, nil) }

type (
	loginInputKey    struct{}
	refreshInputKey  struct{}
	validateInputKey struct{}
)

type loginInput struct {
	email, password string
	orgID           *string
}

// forwardedIPKey is get_forwarded_ip, the key of the login and refresh limits.
func forwardedIPKey(r *http.Request) string { return ratelimit.ForwardedIP(r) }

func writeValidation(w http.ResponseWriter, errs pybody.Errors) {
	policy.WriteJSON(w, http.StatusUnprocessableEntity, pybody.Detail(errs), nil)
}

// validateLogin is LoginRequest: EmailStr, a password of at most 128
// characters and an optional organization id.
func validateLogin(w http.ResponseWriter, r *http.Request) (*http.Request, bool) {
	body, _ := policy.BodyFrom(r.Context())
	var errs pybody.Errors
	var input loginInput
	if object, ok := errs.Object(body); ok {
		input.email, _ = errs.RequiredEmailStr(object, "email")
		input.password, _ = errs.RequiredString(object, "password", 0, 128)
		if value, present := errs.OptionalString(object, "org_id", 0, 0); present {
			input.orgID = &value
		}
	}
	if len(errs) > 0 {
		writeValidation(w, errs)
		return nil, false
	}
	return r.WithContext(context.WithValue(r.Context(), loginInputKey{}, input)), true
}

// validateRefresh is TokenRefreshRequest.
func validateRefresh(w http.ResponseWriter, r *http.Request) (*http.Request, bool) {
	body, _ := policy.BodyFrom(r.Context())
	var errs pybody.Errors
	var token string
	if object, ok := errs.Object(body); ok {
		token, _ = errs.RequiredString(object, "refresh_token", 0, 0)
	}
	if len(errs) > 0 {
		writeValidation(w, errs)
		return nil, false
	}
	return r.WithContext(context.WithValue(r.Context(), refreshInputKey{}, token)), true
}

// validateTokenBody is TokenValidateRequest, then get_validate_key: the
// limit key is computed from the token, and a token the key function
// cannot encode (a lone surrogate) is its UnicodeEncodeError, the bare
// 500, before the request is counted.
func (h handlers) validateTokenBody(w http.ResponseWriter, r *http.Request) (*http.Request, bool) {
	body, _ := policy.BodyFrom(r.Context())
	var errs pybody.Errors
	var token string
	if object, ok := errs.Object(body); ok {
		token, _ = errs.RequiredString(object, "token", 0, 0)
	}
	if len(errs) > 0 {
		writeValidation(w, errs)
		return nil, false
	}
	key, err := validateKey(r, token)
	if err != nil {
		h.fail(w, r, "rate limit key", err)
		return nil, false
	}
	return r.WithContext(context.WithValue(r.Context(), validateInputKey{}, validateInput{token: token, key: key})), true
}

type validateInput struct{ token, key string }

func validateLimitKey(r *http.Request) string {
	input, _ := r.Context().Value(validateInputKey{}).(validateInput)
	return input.key
}
