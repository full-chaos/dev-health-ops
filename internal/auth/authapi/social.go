package authapi

import (
	"errors"
	"net/http"

	"github.com/google/uuid"
)

// socialSessions is the social.SessionWriter: a social sign-in's session for
// the user's earliest membership (go-api's social login takes its first
// membership) with its auth.login.succeeded row, answered as go-api's
// SocialLoginResponse (internal/api/session/social.go).
type socialSessions struct{ h compatHandlers }

func (s socialSessions) WriteSession(w http.ResponseWriter, r *http.Request, principalID uuid.UUID, created bool) {
	h, ctx := s.h, r.Context()
	memberships, err := h.Users.Memberships(ctx, principalID)
	if err != nil {
		h.fail(w, r, opSocialLogin, err)
		return
	}
	membership := chooseMembership(memberships, nil)
	user, found, err := h.Users.State(ctx, principalID)
	if err != nil {
		h.fail(w, r, opSocialLogin, err)
		return
	}
	if !found {
		h.fail(w, r, opSocialLogin, errors.New("authapi: a social sign-in's user is not in public.users"))
		return
	}
	issued, err := h.startSession(ctx, principalID, membership, nil, []string{"fed"}, "")
	if err != nil {
		h.fail(w, r, opSocialLogin, err)
		return
	}
	org, role := (*uuid.UUID)(nil), roleText(nil, false)
	if membership != nil {
		org, role = &membership.OrgID, roleText(membership.Role, true)
	}
	access, err := h.Issuer.Access(Grant{PrincipalID: principalID, OrganizationID: org, Role: role, SessionID: issued.Session.ID, User: user})
	if err != nil {
		h.fail(w, r, opSocialLogin, err)
		return
	}
	h.rec.record(ctx, opSocialLogin, "success", reasonAccepted, nil)
	writeCompatModel(w, loginResponse(access, issued, membership == nil && !user.IsSuperuser,
		userInfo(principalID, user, membership), &created))
}
