package session

import (
	"context"
	"errors"
	"net/http"

	"github.com/full-chaos/dev-health-ops/internal/api/audit"
	"github.com/full-chaos/dev-health-ops/internal/api/authprovision"
	"github.com/full-chaos/dev-health-ops/internal/api/sessionissue"
)

// maxAcceptTokenBytes is the accept route's bound on the token.
const maxAcceptTokenBytes = 256

// acceptTokenUsable: a token the route would refuse as malformed is today's
// invalid invite, answered without a call.
func acceptTokenUsable(token string) bool { return token != "" && len(token) <= maxAcceptTokenBytes }

// acceptRefusalAnswer is today's answer of an auth invite-accept refusal
// (PARITY-10988 row 6); anything else is the 503 of the session proxy.
func acceptRefusalAnswer(err error) (int, string) {
	switch authprovision.ReasonOf(err) {
	case "invite_invalid":
		return http.StatusBadRequest, "Invalid or expired invite"
	case "email_unverified":
		return http.StatusForbidden, reasonInviteEmailUnverified
	case "email_mismatch":
		return http.StatusForbidden, reasonInviteEmailMismatch
	case "already_member":
		return http.StatusBadRequest, "User is already a member of this organization"
	case "organization_not_found":
		return http.StatusNotFound, "Organization not found"
	}
	return http.StatusServiceUnavailable, sessionissue.UnavailableText
}

// joinInAuth is an invite join with AUTH_PROXY_ORG_PROVISIONING on. go-api
// reads the caller (and, onboarding, whether it has a membership) in a
// read-only transaction it ends before the call, so it holds no transaction
// or row lock while auth-service commits the membership, the invite and
// go-api's member_joined audit row. Then it issues the session for the
// organization and role auth-service answered.
func (h handlers) joinInAuth(w http.ResponseWriter, r *http.Request, token, description string, onboarding bool) {
	ctx := r.Context()
	read, err := h.Pool.Begin(ctx)
	if err != nil {
		h.fail(w, r, "begin", err)
		return
	}
	user, ok := h.callerRow(w, r, read)
	onboarded := false
	if ok && onboarding {
		err = read.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM memberships WHERE user_id = $1::uuid)`, user.ID).Scan(&onboarded)
	}
	_ = read.Rollback(context.Background())
	switch {
	case !ok:
		return
	case err != nil:
		h.fail(w, r, "load memberships", err)
		return
	case onboarded:
		refuse(w, http.StatusBadRequest, "Already onboarded")
		return
	case onboarding && token == "":
		refuse(w, http.StatusBadRequest, "invite_code is required")
		return
	case !acceptTokenUsable(token):
		refuse(w, http.StatusBadRequest, "Invalid or expired invite")
		return
	}
	metadata, err := audit.RequestMetadata(r)
	if err != nil {
		h.fail(w, r, "audit", err)
		return
	}
	accepted, err := h.Provision.AcceptInvite(ctx, authprovision.AcceptRequest{Token: token, PrincipalID: user.ID, Description: description,
		RequestMetadata: metadata})
	if err != nil {
		status, text := acceptRefusalAnswer(err)
		if status == http.StatusServiceUnavailable {
			h.Logger.WarnContext(ctx, "api session invite join: auth-service did not accept; nothing was written",
				"refused", errors.Is(err, authprovision.ErrRefused), "reason", authprovision.ReasonOf(err))
		}
		refuse(w, status, text)
		return
	}
	issue, err := h.Pool.Begin(ctx)
	if err != nil {
		h.fail(w, r, "begin", err)
		return
	}
	defer func() { _ = issue.Rollback(context.Background()) }()
	orgName, found, err := organizationName(ctx, issue, accepted.OrgID)
	if err != nil {
		h.fail(w, r, "load organization", err)
		return
	}
	if !found {
		refuse(w, http.StatusNotFound, "Organization not found")
		return
	}
	now := h.Now()
	role := accepted.Role
	h.commitThenIssue(w, r, issue, user, &membershipRow{OrgID: accepted.OrgID, Role: &role, JoinedAt: &now, CreatedAt: now}, orgName)
}
