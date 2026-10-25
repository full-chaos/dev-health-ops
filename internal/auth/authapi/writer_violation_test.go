package authapi

import (
	"net/http"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity/local"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity/social"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
)

// flaggedHandler serves route's handler the writer the auth-service server
// hands it: one that carries the route's ResponseModel flag.
func flaggedHandler(route httpapi.Route) http.Handler {
	key := route.Method + " " + route.Pattern
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		route.Handler.ServeHTTP(httpapi.NewRouteWriter(w, key, route.ResponseModel), r)
	})
}

func expectNoViolation(t *testing.T, name string, want int, run func() int) {
	t.Helper()
	before := policy.WriterViolations()
	got := run()
	if got != want {
		t.Fatalf("%s: status %d, want %d", name, got, want)
	}
	if delta := policy.WriterViolations() - before; delta != 0 {
		t.Errorf("%s: %d writer violations on a success body", name, delta)
	}
}

func TestCompatSuccessBodiesRaiseNoWriterViolation(t *testing.T) {
	f, fake := newSocialFixture(t)
	fake.result = social.ExchangeResult{PrincipalID: f.principal, Created: true}
	rotated := "dhr1_" + "rrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrr"
	f.sessions.result = session.Result{Reason: session.ReasonAccepted, Issued: session.Issued{
		Session: f.record, Credential: contracts.NewSecretString(rotated), ExpiresAt: time.Now().Add(time.Hour),
	}}
	do := func(path, body string) func() int {
		return func() int { return f.do(http.MethodPost, path, body, nil).Code }
	}
	expectNoViolation(t, "login", http.StatusOK, do(PatternCompatLogin, loginBody(compatEmail, compatPassword, "")))
	expectNoViolation(t, "social-login", http.StatusOK, do(PatternCompatSocialLogin, `{"provider":"github","provider_access_token":"gho_test"}`))
	expectNoViolation(t, "refresh", http.StatusOK, do(PatternCompatRefresh, `{"refresh_token":"`+credential()+`"}`))
	expectNoViolation(t, "logout", http.StatusOK, do(PatternCompatLogout, `{"refresh_token":"x"}`))
}

func TestCredentialSuccessBodiesRaiseNoWriterViolation(t *testing.T) {
	c := newCredFixture(t)
	call := func(method, path, body string, header map[string]string) func() int {
		return func() int { return c.call(method, path, body, header).Code }
	}
	email := `{"email":"` + credKnownEmail + `"}`
	expectNoViolation(t, "resend-verification", http.StatusOK, call(http.MethodPost, PatternResendVerification, email, nil))
	secret, ok := c.linkFor(local.PurposeEmailVerification)
	if !ok {
		t.Fatal("no verification link")
	}
	expectNoViolation(t, "verify", http.StatusOK, call(http.MethodGet, PatternVerifyEmail+"?token="+secret, "", nil))
	expectNoViolation(t, "forgot-password", http.StatusOK, call(http.MethodPost, PatternForgotPassword, email, nil))
	reset, ok := c.linkFor(local.PurposePasswordReset)
	if !ok {
		t.Fatal("no reset link")
	}
	expectNoViolation(t, "reset-password", http.StatusOK, call(http.MethodPost, PatternResetPassword, `{"token":"`+reset+`","new_password":"another-password-1"}`, nil))
}

func TestChangePasswordSuccessBodyRaisesNoWriterViolation(t *testing.T) {
	c := newCredFixture(t)
	expectNoViolation(t, "change-password", http.StatusOK, func() int {
		return c.call(http.MethodPost, PatternChangePassword,
			`{"current_password":"current-password-1","new_password":"Brand-new-pass-77!"}`, c.bearer(c.grant())).Code
	})
}
