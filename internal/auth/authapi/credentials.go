package authapi

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/auth/clientaddr"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity/local"
	"github.com/full-chaos/dev-health-ops/internal/auth/requestid"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// Credentials route patterns: go-api's own paths (internal/api/session/session.go).
const (
	PatternVerifyEmail        = "/api/v1/auth/verify"
	PatternResendVerification = "/api/v1/auth/resend-verification"
	PatternForgotPassword     = "/api/v1/auth/forgot-password"
	PatternResetPassword      = "/api/v1/auth/reset-password"
	PatternChangePassword     = "/api/v1/auth/change-password"
	opVerifyEmail             = "verify_email"
	opResendVerification      = "resend_verification"
	opForgotPassword          = "forgot_password"
	opResetPassword           = "reset_password"
	opChangePassword          = "change_password"
	textVerifyInvalid         = "Invalid or expired verification token"
	textVerified              = "Email verified successfully"
	textResendGeneric         = "If an account exists with that email, a verification link has been sent"
	textForgotGeneric         = "If the account exists, a password reset email has been sent"
	textResetInvalid          = "Invalid or expired token"
	textResetDone             = "Password reset successful"
	textCurrentRejected       = "Current password verification failed"
	textManagedAccount        = "Password is managed by your sign-in provider"
	textChangeNotAllowed      = "Password change is not allowed for this session"
	textChangeInvalid         = "Invalid password change request"
	textPasswordValidation    = "Password validation failed"
	reasonCredentialsRefusal  = "invalid_token"
	reasonSessionsNotRevoked  = "sessions_not_revoked"
	reasonPasswordPolicy      = "password_policy"
	reasonCurrentRejected     = "current_password_rejected"
	reasonManagedElsewhere    = "password_managed_elsewhere"
	reasonChangeNotAllowed    = "not_allowed"
)

// The routes' rate limits (internal/api/session/limits.go).
var (
	verifyLimit         = httpapi.Limit{ID: "auth_verify", Count: 10, Window: time.Hour}
	resendLimit         = httpapi.Limit{ID: "auth_resend_verification", Count: 3, Window: time.Hour}
	forgotLimit         = httpapi.Limit{ID: "auth_forgot_password", Count: 3, Window: time.Hour}
	changePasswordLimit = httpapi.Limit{ID: "auth_change_password", Count: 5, Window: time.Hour}
)

// CredentialFlows is the link and password domain (*local.Flows).
type CredentialFlows interface {
	Verify(ctx context.Context, secret string) (local.FlowOutcome, error)
	Resend(ctx context.Context, email string) error
	Forgot(ctx context.Context, email string) error
	Reset(ctx context.Context, secret, newPassword string) (local.FlowOutcome, error)
	Change(ctx context.Context, req local.ChangeRequest) (local.FlowOutcome, error)
}

// CredentialDeps are the credentials routes' collaborators.
type CredentialDeps struct {
	Flows    CredentialFlows
	Sessions Sessions
	Users    Users
	Issuer   *Issuer
	Logger   *slog.Logger
	Meter    metric.Meter
	// Counters holds the rate-limit windows; nil makes an in-process store
	// (per replica).
	Counters httpapi.CounterStore
}

type credentialHandlers struct {
	handlers
	compat compatHandlers
	change http.Handler
	flows  CredentialFlows
}

type (
	credentialInputKey struct{}
	changeAuthKey      struct{}
	changeBodyKey      struct{}
)

// credentialInput is one validated request: the secret or e-mail, the
// password, and the e-mail the rate-limit key reads.
type credentialInput struct {
	email, token, password, current string
}

type changeCaller struct {
	bearer Bearer
	record session.Record
}

// CredentialRoutes returns go-api's password and e-mail verification routes.
// Each registration is on a line of its own.
func CredentialRoutes(deps CredentialDeps) ([]httpapi.Route, error) {
	if deps.Flows == nil || deps.Sessions == nil || deps.Users == nil || deps.Issuer == nil {
		return nil, errors.New("authapi: credential routes need flows, sessions, users and an issuer")
	}
	if deps.Logger == nil {
		deps.Logger = slog.Default()
	}
	meter := deps.Meter
	if meter == nil {
		meter = otel.Meter("github.com/full-chaos/dev-health-ops/internal/auth/authapi")
	}
	rec, err := newRecorder(deps.Logger, meter)
	if err != nil {
		return nil, err
	}
	store := deps.Counters
	if store == nil {
		store = httpapi.NewMemoryCounters(nil)
	}
	h := credentialHandlers{
		handlers: handlers{Deps: Deps{Sessions: deps.Sessions, Users: deps.Users, Issuer: deps.Issuer, Logger: deps.Logger}, rec: rec},
		compat:   compatHandlers{CompatDeps: CompatDeps{Logger: deps.Logger}, rec: rec},
		flows:    deps.Flows,
	}
	write := func(w http.ResponseWriter, r *http.Request, code httpapi.Code) { httpapi.WriteError(w, r, code) }
	limited := func(validate httpapi.RequestValidator, limit httpapi.Limit, key httpapi.KeyFunc, next http.HandlerFunc) http.Handler {
		return httpapi.ValidateThenLimit(validate, httpapi.NewKeyedLimiter(store, limit), key, write)(next)
	}
	h.change = limited(h.validateChange, changePasswordLimit, changePasswordKey, h.changePassword)
	flows := deps.Flows
	routes := []httpapi.Route{
		{Method: http.MethodGet, Pattern: PatternVerifyEmail, Handler: limited(h.validateVerify, verifyLimit, credentialAuthKey, h.verifyEmail(flows))},
		{Method: http.MethodPost, Pattern: PatternResendVerification, Handler: limited(h.validateEmail(opResendVerification), resendLimit, credentialAuthKey, h.mailLink(opResendVerification, textResendGeneric, flows.Resend))},
		{Method: http.MethodPost, Pattern: PatternForgotPassword, Handler: limited(h.validateEmail(opForgotPassword), forgotLimit, credentialAuthKey, h.mailLink(opForgotPassword, textForgotGeneric, flows.Forgot))},
		{Method: http.MethodPost, Pattern: PatternResetPassword, Handler: h.resetPassword(flows)},
		{Method: http.MethodPost, Pattern: PatternChangePassword, Handler: h.authenticatedChange()},
	}
	return routes, nil
}

func inputOf(r *http.Request) credentialInput {
	input, _ := r.Context().Value(credentialInputKey{}).(credentialInput)
	return input
}

// credentialAuthKey is get_auth_key: the client address and the request's
// normalized e-mail, "unknown" without one.
func credentialAuthKey(r *http.Request) string {
	host := "127.0.0.1"
	if text := clientaddr.Text(r.Context()); text != nil && *text != "" {
		host = *text
	}
	email := "unknown"
	if normalized := pythonparity.Lower(pythonparity.Strip(inputOf(r).email)); normalized != "" {
		email = normalized
	}
	return host + ":" + email
}

// changePasswordKey limits per signed-in principal.
func changePasswordKey(r *http.Request) string {
	if caller, ok := r.Context().Value(changeAuthKey{}).(changeCaller); ok {
		return "change-password:" + caller.bearer.PrincipalID.String()
	}
	return "change-password:unknown"
}

func writeValidation(w http.ResponseWriter, errs pybody.Errors) {
	policy.WriteJSON(w, http.StatusUnprocessableEntity, pybody.Detail(errs), nil)
}

func (h credentialHandlers) withInput(r *http.Request, input credentialInput) *http.Request {
	return r.WithContext(context.WithValue(r.Context(), credentialInputKey{}, input))
}

// validateVerify is verify_email's `token: Query(min_length=1)`.
func (h credentialHandlers) validateVerify(w http.ResponseWriter, r *http.Request) (*http.Request, bool) {
	var errs pybody.Errors
	token, _ := errs.RequiredQueryString("token", pybody.LastQueryValue(r.URL.Query(), "token"), 1)
	if len(errs) > 0 {
		h.compat.rec.record(r.Context(), opVerifyEmail, "denied", reasonMalformed, nil)
		writeValidation(w, errs)
		return nil, false
	}
	input := credentialInput{token: token}
	if value := pybody.LastQueryValue(r.URL.Query(), "email"); value != nil {
		input.email = *value
	}
	return h.withInput(r, input), true
}

// validateEmail is ResendVerificationRequest and ForgotPasswordRequest: one EmailStr.
func (h credentialHandlers) validateEmail(operation string) httpapi.RequestValidator {
	return func(w http.ResponseWriter, r *http.Request) (*http.Request, bool) {
		body, ok := h.compat.readCompatBody(w, r, operation, nil)
		if !ok {
			return nil, false
		}
		var errs pybody.Errors
		var email string
		if object, ok := errs.Object(body); ok {
			email, _ = errs.RequiredEmailStr(object, "email")
		}
		if len(errs) > 0 {
			h.compat.rec.record(r.Context(), operation, "denied", reasonMalformed, nil)
			writeValidation(w, errs)
			return nil, false
		}
		return h.withInput(r, credentialInput{email: email}), true
	}
}

func verifyModel(message string, verified bool) *pyjson.Object {
	out := pyjson.NewObject()
	out.Set("message", message)
	if verified {
		out.Set("verified", true)
	} else {
		out.Set("verified", nil)
	}
	return out
}

func (h credentialHandlers) verifyEmail(flows CredentialFlows) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		outcome, err := flows.Verify(r.Context(), inputOf(r).token)
		if err != nil {
			h.compat.fail(w, r, opVerifyEmail, err)
			return
		}
		if outcome.Kind != local.FlowOK {
			h.compat.refuseCompat(w, r, opVerifyEmail, http.StatusBadRequest, reasonCredentialsRefusal, textVerifyInvalid)
			return
		}
		h.compat.rec.record(r.Context(), opVerifyEmail, "success", reasonAccepted, nil)
		writeCompatModel(w, verifyModel(textVerified, true))
	}
}

// mailLink answers resend-verification and forgot-password: the same 200 for
// every address.
func (h credentialHandlers) mailLink(operation, message string, send func(context.Context, string) error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := send(r.Context(), inputOf(r).email); err != nil {
			h.compat.fail(w, r, operation, err)
			return
		}
		h.compat.rec.record(r.Context(), operation, "success", reasonAccepted, nil)
		writeCompatModel(w, verifyModel(message, false))
	}
}

// violationsDetail is {"detail": {"message": message, "errors": [...]}}.
func violationsDetail(message string, violations []string) *pyjson.Object {
	detail := policy.ErrorDetail(message)
	list := make([]pyjson.Value, len(violations))
	for i, v := range violations {
		list[i] = v
	}
	detail.Set("errors", list)
	return detail
}

// resetPassword is POST /api/v1/auth/reset-password: no rate limit, the
// model's 8 to 128 characters, then the flow.
func (h credentialHandlers) resetPassword(flows CredentialFlows) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, ok := h.compat.readCompatBody(w, r, opResetPassword, nil)
		if !ok {
			return
		}
		var errs pybody.Errors
		var token, password string
		if object, ok := errs.Object(body); ok {
			token, _ = errs.RequiredString(object, "token", 0, 0)
			password, _ = errs.RequiredString(object, "new_password", 8, 128)
		}
		if len(errs) > 0 {
			h.compat.rec.record(r.Context(), opResetPassword, "denied", reasonMalformed, nil)
			writeValidation(w, errs)
			return
		}
		outcome, err := flows.Reset(r.Context(), token, password)
		if err != nil {
			h.compat.fail(w, r, opResetPassword, err)
			return
		}
		switch outcome.Kind {
		case local.FlowOK:
			if outcome.RevokeErr != nil {
				h.compat.rec.record(r.Context(), opResetPassword, "error", reasonSessionsNotRevoked, outcome.RevokeErr)
				policy.WriteInternal(w)
				return
			}
			h.compat.rec.record(r.Context(), opResetPassword, "success", reasonAccepted, nil)
			writeCompatModel(w, verifyModel(textResetDone, false))
		case local.FlowPolicy:
			h.compat.rec.record(r.Context(), opResetPassword, "denied", reasonPasswordPolicy, nil)
			policy.WriteDetail(w, http.StatusUnprocessableEntity, violationsDetail(textPasswordValidation, outcome.Violations), nil)
		default:
			h.compat.refuseCompat(w, r, opResetPassword, http.StatusBadRequest, reasonCredentialsRefusal, textResetInvalid)
		}
	})
}

// authenticatedChange reads the body, then resolves the bearer, as go-api's
// BodyFirst(Authenticated) does: a body that does not decode is answered
// before the bearer is looked at.
func (h credentialHandlers) authenticatedChange() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, ok := h.compat.readCompatBody(w, r, opChangePassword, nil)
		if !ok {
			return
		}
		bearer, record, ok := h.authenticate(w, r, opChangePassword)
		if !ok {
			return
		}
		ctx := context.WithValue(r.Context(), changeAuthKey{}, changeCaller{bearer: bearer, record: record})
		ctx = context.WithValue(ctx, changeBodyKey{}, body)
		h.change.ServeHTTP(w, r.WithContext(ctx))
	})
}

// validateChange is the request model: a current password of 1 to 128
// characters and a new one of 8 to 128. The detail never echoes a field.
func (h credentialHandlers) validateChange(w http.ResponseWriter, r *http.Request) (*http.Request, bool) {
	body, _ := r.Context().Value(changeBodyKey{}).(pybody.Body)
	var errs pybody.Errors
	var input credentialInput
	if object, ok := errs.Object(body); ok {
		input.current, _ = errs.RequiredString(object, "current_password", 1, 128)
		input.password, _ = errs.RequiredString(object, "new_password", 8, 128)
	}
	if len(errs) > 0 {
		h.compat.refuseCompat(w, r, opChangePassword, http.StatusUnprocessableEntity, reasonMalformed, textChangeInvalid)
		return nil, false
	}
	return h.withInput(r, input), true
}

func (h credentialHandlers) changePassword(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	caller, _ := ctx.Value(changeAuthKey{}).(changeCaller)
	input := inputOf(r)
	outcome, err := h.flows.Change(ctx, local.ChangeRequest{
		PrincipalID: caller.bearer.PrincipalID, OrganizationID: caller.record.OrganizationID,
		Current: input.current, New: input.password, Impersonated: caller.bearer.Impersonated,
		Actor: session.Actor{PrincipalID: &caller.bearer.PrincipalID, RequestID: requestid.Audit(ctx)},
	})
	if err != nil {
		h.compat.fail(w, r, opChangePassword, err)
		return
	}
	switch outcome.Kind {
	case local.FlowOK:
		if outcome.RevokeErr != nil {
			h.compat.rec.record(ctx, opChangePassword, "error", reasonSessionsNotRevoked, outcome.RevokeErr)
			policy.WriteInternal(w)
			return
		}
		h.changed(w, r, caller, outcome)
	case local.FlowPolicy:
		h.compat.rec.record(ctx, opChangePassword, "denied", reasonPasswordPolicy, nil)
		list := make([]pyjson.Value, len(outcome.Violations))
		for i, v := range outcome.Violations {
			list[i] = v
		}
		detail := pyjson.NewObject()
		detail.Set("violations", list)
		out := pyjson.NewObject()
		out.Set("detail", detail)
		policy.WriteJSON(w, http.StatusUnprocessableEntity, out, nil)
	case local.FlowCurrentRejected:
		h.compat.refuseCompat(w, r, opChangePassword, http.StatusForbidden, reasonCurrentRejected, textCurrentRejected)
	case local.FlowManagedAccount:
		h.compat.refuseCompat(w, r, opChangePassword, http.StatusForbidden, reasonManagedElsewhere, textManagedAccount)
	default:
		h.compat.refuseCompat(w, r, opChangePassword, http.StatusForbidden, reasonChangeNotAllowed, textChangeNotAllowed)
	}
}

// changed answers go-api's LoginResponse for the caller's new session, in the
// organization it signed in with while that membership stands.
func (h credentialHandlers) changed(w http.ResponseWriter, r *http.Request, caller changeCaller, outcome local.FlowOutcome) {
	ctx := r.Context()
	principal := caller.bearer.PrincipalID
	user, found, err := h.Users.State(ctx, principal)
	if err == nil && !found {
		err = errors.New("authapi: a password change's user is gone")
	}
	if err != nil {
		h.compat.fail(w, r, opChangePassword, err)
		return
	}
	var (
		membership *Membership
		org        *uuid.UUID
	)
	role := roleText(nil, false)
	if caller.record.OrganizationID != nil {
		value, member, err := h.Users.Role(ctx, principal, *caller.record.OrganizationID)
		if err != nil {
			h.compat.fail(w, r, opChangePassword, err)
			return
		}
		if member {
			membership = &Membership{OrgID: *caller.record.OrganizationID, Role: value}
			org, role = &membership.OrgID, roleText(value, true)
		}
	}
	issued := outcome.Issued
	access, err := h.Issuer.Access(Grant{PrincipalID: principal, OrganizationID: org, Role: role, SessionID: issued.Session.ID, User: user})
	if err != nil {
		h.compat.fail(w, r, opChangePassword, err)
		return
	}
	h.compat.rec.record(ctx, opChangePassword, "success", reasonAccepted, nil)
	writeCompatModel(w, loginResponse(access, issued.Credential, membership == nil && !user.IsSuperuser,
		userInfo(principal, user, membership), nil))
}
