package authapi

import (
	"context"
	"errors"
	"log/slog"
	"net/http"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity/local"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity/social"
	"github.com/full-chaos/dev-health-ops/internal/auth/requestid"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
)

// Compatibility route patterns: go-api's own paths, which the go-api proxy
// forwards unchanged when its aggregate flag is on
// (internal/apiservice/authproxy.go). Each answers go-api's wire: the same
// status, body shape and texts as internal/api/session.
const (
	PatternCompatLogin       = "/api/v1/auth/login"
	PatternCompatRefresh     = "/api/v1/auth/refresh"
	PatternCompatLogout      = "/api/v1/auth/logout"
	PatternCompatSocialLogin = "/api/v1/auth/social-login"
)

// CompatSessions is the session domain the compatibility routes drive
// (*session.Store).
type CompatSessions interface {
	Sessions
	RevokeByCredential(ctx context.Context, credential contracts.SecretString, actor session.Actor) (bool, error)
	CreateAudited(ctx context.Context, principalID uuid.UUID, organizationID *uuid.UUID, amr []string, event audit.AuditEvent) (session.Issued, error)
}

// SocialExchange is the social sign-in domain (*social.Service).
type SocialExchange interface {
	Exchange(ctx context.Context, provider social.Provider, token contracts.SecretString, requestID *string) (social.ExchangeResult, error)
}

// Projector makes the auth schema's principal and organization rows agree
// with go-api's before a session or an audit row names them
// (*identity.Store).
type Projector interface {
	Project(ctx context.Context, userID uuid.UUID) error
	ProjectOrganization(ctx context.Context, orgID uuid.UUID) error
}

// LoginConfig mounts the local login route.
type LoginConfig struct {
	Users    local.Users
	Attempts local.Attempts
	// LockoutKey is AUTH_LOGIN_LOCKOUT_KEY (local.Config.LockoutKey).
	LockoutKey contracts.SecretString
	// Compare is the password check; nil means bcrypt. Tests count its calls.
	Compare func(hash, password string) bool
}

// CompatDeps are the compatibility routes' collaborators.
type CompatDeps struct {
	Sessions CompatSessions
	Users    Users
	Issuer   *Issuer
	Logger   *slog.Logger
	// Meter registers the outcome counter; nil uses the process meter.
	Meter     metric.Meter
	Projector Projector
	// AppendAudit writes an append-only audit row (audit.Append).
	AppendAudit func(ctx context.Context, event audit.AuditEvent) error
	// Login mounts POST /api/v1/auth/login; nil leaves it unmounted.
	Login *LoginConfig
	// Social mounts POST /api/v1/auth/social-login; nil leaves it unmounted.
	Social SocialExchange
}

type compatHandlers struct {
	CompatDeps
	rec   recorder
	login *local.Service
}

// CompatRoutes returns go-api's refresh and logout, and login and
// social-login when they are configured. Each registration is on a line of
// its own.
func CompatRoutes(deps CompatDeps) ([]httpapi.Route, error) {
	if deps.Sessions == nil || deps.Users == nil || deps.Issuer == nil || deps.Projector == nil || deps.AppendAudit == nil {
		return nil, errors.New("authapi: compatibility routes need sessions, users, an issuer, a projector and an audit appender")
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
	h := compatHandlers{CompatDeps: deps, rec: rec}
	routes := []httpapi.Route{
		{Method: http.MethodPost, Pattern: PatternCompatRefresh, Handler: http.HandlerFunc(h.compatRefresh)},
		{Method: http.MethodPost, Pattern: PatternCompatLogout, Handler: http.HandlerFunc(h.compatLogout)},
	}
	if deps.Login != nil {
		login, err := local.New(local.Config{
			Users: deps.Login.Users, Attempts: deps.Login.Attempts, LockoutKey: deps.Login.LockoutKey,
			Compare: deps.Login.Compare, Sessions: loginSessions{h: h},
		})
		if err != nil {
			return nil, err
		}
		h.login = login
		routes = append(routes, httpapi.Route{Method: http.MethodPost, Pattern: PatternCompatLogin, Handler: http.HandlerFunc(h.compatLogin)})
	}
	if deps.Social != nil {
		routes = append(routes, httpapi.Route{Method: http.MethodPost, Pattern: PatternCompatSocialLogin, Handler: http.HandlerFunc(h.compatSocialLogin)})
	}
	return routes, nil
}

// auth-outcome.v1 operations of the compatibility routes.
const (
	opCompatLogin   = "login"
	opCompatRefresh = "compat_refresh"
	opCompatLogout  = "logout"
	opSocialLogin   = "social_login"
)

// go-api refusal texts (internal/api/session).
const (
	textInvalidRefresh = "Invalid or expired refresh token"
	textRefreshReused  = "Refresh token reuse detected"
	textDisabled       = "Account is disabled"
	textUserNotFound   = "User not found"
	textLogoutDone     = "Logout successful"
)

// readCompatBody is go-api's policy.BodyFirst: a body that does not decode is
// its 422, bytes that are not UTF-8 its 400, an over-long body its 413. It
// returns false when it answered.
func (h compatHandlers) readCompatBody(w http.ResponseWriter, r *http.Request, operation string) (pybody.Body, bool) {
	body, outcome, failure, err := pybody.Read(r)
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			h.rec.record(r.Context(), operation, "denied", reasonMalformed, nil)
			policy.WriteDetail(w, http.StatusRequestEntityTooLarge, "Request Entity Too Large", nil)
			return pybody.Body{}, false
		}
		h.fail(w, r, operation, err)
		return pybody.Body{}, false
	}
	switch outcome {
	case pybody.DecodeFailed:
		h.rec.record(r.Context(), operation, "denied", reasonMalformed, nil)
		policy.WriteJSON(w, http.StatusUnprocessableEntity, pybody.Detail([]pybody.Error{*failure}), nil)
		return pybody.Body{}, false
	case pybody.ParseFailed:
		h.rec.record(r.Context(), operation, "denied", reasonMalformed, nil)
		policy.WriteDetail(w, http.StatusBadRequest, "There was an error parsing the body", nil)
		return pybody.Body{}, false
	}
	return body, true
}

// readRefreshToken is go-api's TokenRefreshRequest and LogoutRequest: one
// required string member, refresh_token.
func (h compatHandlers) readRefreshToken(w http.ResponseWriter, r *http.Request, operation string) (string, bool) {
	body, ok := h.readCompatBody(w, r, operation)
	if !ok {
		return "", false
	}
	var errs pybody.Errors
	var token string
	if object, ok := errs.Object(body); ok {
		token, _ = errs.RequiredString(object, "refresh_token", 0, 0)
	}
	if len(errs) > 0 {
		h.rec.record(r.Context(), operation, "denied", reasonMalformed, nil)
		policy.WriteJSON(w, http.StatusUnprocessableEntity, pybody.Detail(errs), nil)
		return "", false
	}
	return token, true
}

// refuseCompat writes go-api's {"detail": {"message": text}} refusal.
func (h compatHandlers) refuseCompat(w http.ResponseWriter, r *http.Request, operation string, status int, reason, text string) {
	h.rec.record(r.Context(), operation, "denied", reason, nil)
	policy.WriteDetail(w, status, policy.ErrorDetail(text), nil)
}

// fail records a failure and writes go-api's generic 500. The error is logged
// by class only.
func (h compatHandlers) fail(w http.ResponseWriter, r *http.Request, operation string, err error) {
	h.rec.record(r.Context(), operation, "error", reasonStoreUnavailable, err)
	policy.WriteInternal(w)
}

// compatRefreshMessage is go-api's 401 text for a refused exchange.
func compatRefreshMessage(reason session.Reason) string {
	switch reason {
	case session.ReasonRefreshReused:
		return textRefreshReused
	case session.ReasonPrincipalDeactivated:
		return textDisabled
	default:
		return textInvalidRefresh
	}
}

// compatRefresh is POST /api/v1/auth/refresh with go-api's wire
// (internal/api/session/refresh.go): {"refresh_token"} in, the rotated opaque
// credential and an access token out. Only the auth-service credential is
// accepted; a go-api refresh JWT is an unknown credential.
func (h compatHandlers) compatRefresh(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	token, ok := h.readRefreshToken(w, r, opCompatRefresh)
	if !ok {
		return
	}
	result, err := h.Sessions.Exchange(ctx, contracts.NewSecretString(token))
	if err != nil {
		h.fail(w, r, opCompatRefresh, err)
		return
	}
	if !result.Reason.Success() {
		h.refuseCompat(w, r, opCompatRefresh, http.StatusUnauthorized, string(result.Reason), compatRefreshMessage(result.Reason))
		return
	}
	record := result.Session
	user, found, err := h.Users.State(ctx, record.PrincipalID)
	if err != nil {
		h.fail(w, r, opCompatRefresh, err)
		return
	}
	if !found || !user.IsActive {
		if _, err := h.Sessions.Revoke(ctx, record.ID, session.Actor{RequestID: requestid.Audit(ctx)}); err != nil {
			h.fail(w, r, opCompatRefresh, err)
			return
		}
		if !found {
			h.refuseCompat(w, r, opCompatRefresh, http.StatusUnauthorized, reasonPrincipalNotFound, textUserNotFound)
			return
		}
		h.refuseCompat(w, r, opCompatRefresh, http.StatusUnauthorized, reasonDeactivated, textDisabled)
		return
	}
	role := roleText(nil, false)
	orgText := ""
	if record.OrganizationID != nil {
		value, member, err := h.Users.Role(ctx, record.PrincipalID, *record.OrganizationID)
		if err != nil {
			h.fail(w, r, opCompatRefresh, err)
			return
		}
		role, orgText = roleText(value, member), record.OrganizationID.String()
	}
	access, err := h.Issuer.Access(Grant{
		PrincipalID: record.PrincipalID, OrganizationID: record.OrganizationID, Role: role,
		SessionID: record.ID, User: user,
	})
	if err != nil {
		h.fail(w, r, opCompatRefresh, err)
		return
	}
	h.rec.record(ctx, opCompatRefresh, "success", string(result.Reason), nil)
	out := pyjson.NewObject()
	out.Set("access_token", access)
	out.Set("refresh_token", result.Credential.Reveal())
	out.Set("token_type", "bearer")
	out.Set("expires_in", pyjson.IntOf(int64(AccessExpiresIn)))
	out.Set("user", refreshUserInfo(record.PrincipalID, user, orgText, role))
	writeCompatModel(w, out)
}

// refreshUserInfo is go-api's refresh UserInfo: username and full_name stay
// at their None default and org_id is the session's organization, "" without
// one.
func refreshUserInfo(id uuid.UUID, user UserState, orgID, role string) *pyjson.Object {
	out := pyjson.NewObject()
	out.Set("id", id.String())
	out.Set("email", user.Email)
	out.Set("username", nil)
	out.Set("full_name", nil)
	out.Set("org_id", orgID)
	out.Set("role", role)
	out.Set("is_superuser", user.IsSuperuser)
	return out
}

// compatLogout is POST /api/v1/auth/logout with go-api's wire
// (internal/api/session/routes.go logout): {"refresh_token"} in, and the same
// 200 whether or not the credential revoked a session, so the answer says
// nothing about the credential. The session's revocation is audited by the
// session store.
func (h compatHandlers) compatLogout(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	token, ok := h.readRefreshToken(w, r, opCompatLogout)
	if !ok {
		return
	}
	revoked, err := h.Sessions.RevokeByCredential(ctx, contracts.NewSecretString(token), session.Actor{RequestID: requestid.Audit(ctx)})
	if err != nil {
		h.fail(w, r, opCompatLogout, err)
		return
	}
	reason := reasonAccepted
	if !revoked {
		reason = "nothing_revoked"
	}
	h.rec.record(ctx, opCompatLogout, "success", reason, nil)
	out := pyjson.NewObject()
	out.Set("message", textLogoutDone)
	writeCompatModel(w, out)
}

// writeCompatModel writes a 200 response_model body as go-api does, marked
// never to be cached: every body here carries a token or answers a sign-in.
func writeCompatModel(w http.ResponseWriter, body *pyjson.Object) {
	policy.WriteModel(w, http.StatusOK, body, http.Header{"Cache-Control": {"no-store"}, "Pragma": {"no-cache"}})
}
