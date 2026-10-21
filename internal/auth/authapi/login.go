package authapi

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity/local"
	"github.com/full-chaos/dev-health-ops/internal/auth/requestid"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
)

// EventLoginSucceeded is a sign-in that started a session. It commits with
// the session (session.Store.CreateAudited); a refused sign-in is
// audit.EventLoginFailed, written by audit.Append.
const EventLoginSucceeded = "auth.login.succeeded"

// go-api login texts (internal/api/session/login.go).
const (
	textInvalidCredentials = "Invalid credentials"
	textLocked             = "Too many failed login attempts. Please try again later."
	textNotMember          = "User is not a member of the selected organization"
	textVerifyEmail        = "Please verify your email address before logging in"
)

// providerLocal is the audit provider of a password sign-in.
const providerLocal = "local"

// reasonNotMember is the audit reason of a sign-in to an organization the
// principal is not a member of.
const reasonNotMember = "denied_no_membership"

// errNotMember ends a login whose requested organization the user is not a
// member of. The session is not created.
var errNotMember = errors.New("authapi: the user is not a member of the requested organization")

// loginInput is go-api's LoginRequest.
type loginInput struct {
	email, password string
	orgID           *string
}

// readLogin is go-api's BodyFirst then validateLogin
// (internal/api/session/limits.go): EmailStr, a password of at most 128
// characters and an optional organization id.
func (h compatHandlers) readLogin(w http.ResponseWriter, r *http.Request) (loginInput, bool) {
	body, ok := h.readCompatBody(w, r, opCompatLogin, h.bodyRefused(providerLocal))
	if !ok {
		return loginInput{}, false
	}
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
		h.refuseBody(w, r, opCompatLogin, h.bodyRefused(providerLocal), func() {
			policy.WriteJSON(w, http.StatusUnprocessableEntity, pybody.Detail(errs), nil)
		})
		return loginInput{}, false
	}
	return input, true
}

// loginAttempt carries one login request's organization choice into the
// session the login creates, and the chosen membership and user back out.
// local.Service decides the credential check and calls Sessions.Create only
// on success, so the choice is made there, with the principal known.
type loginAttempt struct {
	requested  *uuid.UUID
	membership *Membership
	user       UserState
}

type loginAttemptKey struct{}

func attemptOf(ctx context.Context) (*loginAttempt, bool) {
	attempt, ok := ctx.Value(loginAttemptKey{}).(*loginAttempt)
	return attempt, ok
}

// compatLogin is POST /api/v1/auth/login with go-api's wire
// (internal/api/session/login.go): every credential refusal is the same 401,
// a lock is 429 with retry_after_seconds, an unverified local account is a 200
// asking for verification, and a requested organization the user is not a
// member of is 401. Each refusal is audited (audit.EventLoginFailed); a
// success commits its audit row with the session.
func (h compatHandlers) compatLogin(w http.ResponseWriter, r *http.Request) {
	input, ok := h.readLogin(w, r)
	if !ok {
		return
	}
	var requested *uuid.UUID
	if input.orgID != nil {
		if parsed, ok := policy.ParsePyUUID(*input.orgID); ok {
			requested = &parsed
		}
	}
	attempt := &loginAttempt{requested: requested}
	ctx := context.WithValue(r.Context(), loginAttemptKey{}, attempt)
	outcome, err := h.login.Login(ctx, local.Credentials{Email: input.email, Password: input.password, OrganizationID: requested})
	var notMember *notMemberError
	switch {
	case errors.As(err, &notMember):
		if err := h.auditSignInFailed(ctx, notMember.principal, requested, reasonNotMember, providerLocal); err != nil {
			h.fail(w, r, opCompatLogin, err)
			return
		}
		h.refuseCompat(w, r, opCompatLogin, http.StatusUnauthorized, reasonNotMember, textNotMember)
		return
	case err != nil:
		h.fail(w, r, opCompatLogin, err)
		return
	}
	switch outcome.Kind {
	case local.KindSuccess:
		h.loginSucceeded(w, r, attempt, outcome)
		return
	case local.KindEmailUnverified:
		h.loginUnverified(w, r, outcome, requested)
		return
	}
	if err := h.auditSignInFailed(ctx, outcome.PrincipalID, requested, string(outcome.Reason), providerLocal); err != nil {
		h.fail(w, r, opCompatLogin, err)
		return
	}
	if outcome.Kind == local.KindLocked {
		h.rec.record(ctx, opCompatLogin, "denied", string(outcome.Reason), nil)
		detail := policy.ErrorDetail(textLocked)
		detail.Set("retry_after_seconds", pyjson.IntOf(lockSeconds(outcome.RetryAfter)))
		policy.WriteDetail(w, http.StatusTooManyRequests, detail, nil)
		return
	}
	h.refuseCompat(w, r, opCompatLogin, http.StatusUnauthorized, string(outcome.Reason), textInvalidCredentials)
}

// lockSeconds is the whole seconds left on a lock, at least 1 (go-api's
// remaining <= 0 -> 1).
func lockSeconds(remaining time.Duration) int64 {
	return max(int64(math.Ceil(remaining.Seconds())), 1)
}

func (h compatHandlers) loginSucceeded(w http.ResponseWriter, r *http.Request, attempt *loginAttempt, outcome local.Outcome) {
	issued := outcome.Issued
	org, role := (*uuid.UUID)(nil), roleText(nil, false)
	if attempt.membership != nil {
		org, role = &attempt.membership.OrgID, roleText(attempt.membership.Role, true)
	}
	access, err := h.Issuer.Access(Grant{
		PrincipalID: outcome.PrincipalID, OrganizationID: org, Role: role, SessionID: issued.Session.ID, User: attempt.user,
	})
	if err != nil {
		h.fail(w, r, opCompatLogin, err)
		return
	}
	h.rec.record(r.Context(), opCompatLogin, "success", string(outcome.Reason), nil)
	writeCompatModel(w, loginResponse(access, *issued, attempt.membership == nil && !attempt.user.IsSuperuser,
		userInfo(outcome.PrincipalID, attempt.user, attempt.membership), nil))
}

// loginUnverified is go-api's email_verification_required answer.
func (h compatHandlers) loginUnverified(w http.ResponseWriter, r *http.Request, outcome local.Outcome, requested *uuid.UUID) {
	ctx := r.Context()
	user, found, err := h.Users.State(ctx, outcome.PrincipalID)
	if err != nil {
		h.fail(w, r, opCompatLogin, err)
		return
	}
	if !found {
		h.fail(w, r, opCompatLogin, errors.New("authapi: an unverified login's user is gone"))
		return
	}
	// go-api's unverified branch audits under the user's first membership,
	// and under the requested organization only when the user has none
	// (internal/api/session/login.go blockedOrg).
	auditRequested := requested
	memberships, err := h.Users.Memberships(ctx, outcome.PrincipalID)
	if err != nil {
		h.fail(w, r, opCompatLogin, err)
		return
	}
	if chooseMembership(memberships, nil) != nil {
		auditRequested = nil
	}
	if err := h.auditSignInFailed(ctx, outcome.PrincipalID, auditRequested, string(outcome.Reason), providerLocal); err != nil {
		h.fail(w, r, opCompatLogin, err)
		return
	}
	h.rec.record(ctx, opCompatLogin, "denied", string(outcome.Reason), nil)
	out := pyjson.NewObject()
	out.Set("status", "email_verification_required")
	out.Set("email", user.Email)
	out.Set("message", textVerifyEmail)
	writeCompatModel(w, out)
}

// auditOrganization is go-api's _resolve_login_audit_org_id: the requested
// organization when it exists, else the user's first membership's, else
// none. It is projected into the auth schema, which the audit row's
// organization_id references.
func (h compatHandlers) auditOrganization(ctx context.Context, principal uuid.UUID, requested *uuid.UUID) (*uuid.UUID, error) {
	if requested != nil {
		// Projected even when the sign-in is refused: the audit row's
		// organization must exist, and the projection mirrors go-api's own
		// organization row, idempotently.
		err := h.Projector.ProjectOrganization(ctx, *requested)
		switch {
		case err == nil:
			return requested, nil
		case !errors.Is(err, identity.ErrOrganizationNotFound):
			return nil, err
		}
	}
	if principal == uuid.Nil {
		return nil, nil
	}
	memberships, err := h.Users.Memberships(ctx, principal)
	if err != nil {
		return nil, err
	}
	first := chooseMembership(memberships, nil)
	if first == nil {
		return nil, nil
	}
	if err := h.Projector.ProjectOrganization(ctx, first.OrgID); err != nil {
		return nil, err
	}
	return &first.OrgID, nil
}

// auditSignInFailed is the one path every refused sign-in (password or
// social) writes its auth.login.failed row through. A known principal is
// projected first, since the row references it; one go-api no longer holds is
// recorded without a subject.
func (h compatHandlers) auditSignInFailed(ctx context.Context, principal uuid.UUID, requested *uuid.UUID, reason, provider string) error {
	org, err := h.auditOrganization(ctx, principal, requested)
	if err != nil {
		return err
	}
	event := audit.AuditEvent{
		EventType: audit.EventLoginFailed, Outcome: audit.OutcomeDenied, RequestID: requestid.Audit(ctx),
		Attributes: map[string]any{"reason_code": reason},
	}
	if provider != "" {
		event.Attributes["provider"] = provider
	}
	if org != nil {
		text := org.String()
		event.OrganizationID = &text
	}
	if principal != uuid.Nil {
		// A refused sign-in still projects the go-api user: the audit row
		// names the principal, the projection mirrors a go-api row that
		// already exists, and it is idempotent.
		err := h.Projector.Project(ctx, principal)
		switch {
		case err == nil:
			text := principal.String()
			event.SubjectPrincipalID = &text
		case !errors.Is(err, identity.ErrUserNotFound):
			return err
		}
	}
	return h.AppendAudit(ctx, event)
}

// notMemberError is errNotMember for one principal.
type notMemberError struct{ principal uuid.UUID }

func (e *notMemberError) Error() string { return errNotMember.Error() }

func (e *notMemberError) Unwrap() error { return errNotMember }

// loginSessions is the session issuer local.Service calls on a successful
// credential check. It picks the membership the session is for, refuses a
// requested organization the user is not a member of, and creates the
// session with its auth.login.succeeded audit row.
type loginSessions struct{ h compatHandlers }

func (s loginSessions) Create(ctx context.Context, principalID uuid.UUID, _ *uuid.UUID, amr []string) (session.Issued, error) {
	attempt, ok := attemptOf(ctx)
	if !ok {
		return session.Issued{}, errors.New("authapi: a login session without its attempt")
	}
	memberships, err := s.h.Users.Memberships(ctx, principalID)
	if err != nil {
		return session.Issued{}, err
	}
	membership := chooseMembership(memberships, attempt.requested)
	if membership == nil && len(memberships) > 0 {
		return session.Issued{}, &notMemberError{principal: principalID}
	}
	user, found, err := s.h.Users.State(ctx, principalID)
	if err != nil {
		return session.Issued{}, err
	}
	if !found {
		return session.Issued{}, errors.New("authapi: a login's user is gone")
	}
	issued, err := s.h.startSession(ctx, principalID, membership, attempt.requested, amr, providerLocal)
	if err != nil {
		return session.Issued{}, err
	}
	attempt.membership, attempt.user = membership, user
	return issued, nil
}

// startSession creates a sign-in's session for membership's organization
// with its auth.login.succeeded row. The row's organization is go-api's
// successful-login audit org: the requested one, else the membership's.
func (h compatHandlers) startSession(ctx context.Context, principalID uuid.UUID, membership *Membership, requested *uuid.UUID,
	amr []string, provider string) (session.Issued, error) {
	var sessionOrg *uuid.UUID
	if membership != nil {
		sessionOrg = &membership.OrgID
		if err := h.Projector.ProjectOrganization(ctx, membership.OrgID); err != nil {
			return session.Issued{}, fmt.Errorf("authapi: projecting the session organization: %w", err)
		}
	}
	auditOrg := sessionOrg
	if auditOrg == nil && requested != nil {
		err := h.Projector.ProjectOrganization(ctx, *requested)
		switch {
		case err == nil:
			auditOrg = requested
		case !errors.Is(err, identity.ErrOrganizationNotFound):
			return session.Issued{}, err
		}
	}
	principal := principalID.String()
	event := audit.AuditEvent{
		EventType: EventLoginSucceeded, Outcome: audit.OutcomeAllowed,
		ActorPrincipalID: &principal, SubjectPrincipalID: &principal, RequestID: requestid.Audit(ctx),
		Attributes: map[string]any{"reason_code": string(local.ReasonAccepted), "target_kind": "session"},
	}
	if provider != "" {
		event.Attributes["provider"] = provider
	}
	if auditOrg != nil {
		text := auditOrg.String()
		event.OrganizationID = &text
	}
	return h.Sessions.CreateAudited(ctx, principalID, sessionOrg, amr, event)
}

// userInfo is go-api's _to_user_info(user, membership).
func userInfo(id uuid.UUID, user UserState, membership *Membership) *pyjson.Object {
	out := pyjson.NewObject()
	out.Set("id", id.String())
	out.Set("email", user.Email)
	out.Set("username", optionalText(user.Username))
	out.Set("full_name", optionalText(user.FullName))
	if membership != nil {
		out.Set("org_id", membership.OrgID.String())
		out.Set("role", roleText(membership.Role, true))
	} else {
		out.Set("org_id", nil)
		out.Set("role", "member")
	}
	out.Set("is_superuser", user.IsSuperuser)
	return out
}

func optionalText(value *string) pyjson.Value {
	if value == nil {
		return nil
	}
	return *value
}

// loginResponse is go-api's LoginResponse; isNewUser appends
// SocialLoginResponse's is_new_user when non-nil.
func loginResponse(access string, issued session.Issued, needsOnboarding bool, user *pyjson.Object, isNewUser *bool) *pyjson.Object {
	out := pyjson.NewObject()
	out.Set("access_token", access)
	out.Set("refresh_token", issued.Credential.Reveal())
	out.Set("token_type", "bearer")
	out.Set("expires_in", pyjson.IntOf(int64(AccessExpiresIn)))
	out.Set("needs_onboarding", needsOnboarding)
	out.Set("user", user)
	if isNewUser != nil {
		out.Set("is_new_user", *isNewUser)
	}
	return out
}
