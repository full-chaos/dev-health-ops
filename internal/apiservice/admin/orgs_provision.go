package admin

import (
	"context"
	"errors"
	"net/http"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/api/audit"
	"github.com/full-chaos/dev-health-ops/internal/api/authprovision"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/api/sessionissue"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity/emailvalidator"
)

// With AUTH_PROXY_ORG_PROVISIONING on (h.orgProvision set) the member, invite
// and delete routes keep their own checks, then ask auth-service, which writes
// both planes, the audit and the outbox in one commit. go-api holds no
// transaction or row lock across the call and writes nothing of its own; a
// success body is read back from the row auth-service projected. Each auth
// refusal maps to the answer go-api gives today (PARITY-10988); any other
// refusal or failure is the 503 of the reactivate route.

// provisionAnswer is the go-api answer of one auth reason code.
type provisionAnswer struct {
	status int
	detail string
}

// actorAnswers are the refusals of the actor rule auth-service re-runs under
// its locks (the caller was demoted, or a target is a superuser).
var actorAnswers = map[string]provisionAnswer{
	"superuser_required": {http.StatusForbidden, "Superuser access required"},
	"admin_required":     {http.StatusForbidden, "Admin access required for organization"},
}

// writeProvisionRefusal writes the answer of err: the route's own table, then
// the actor refusals, else 503.
func (h *handlers) writeProvisionRefusal(ctx context.Context, w http.ResponseWriter, route string, err error, answers map[string]provisionAnswer) {
	reason := authprovision.ReasonOf(err)
	if answer, ok := answers[reason]; ok && reason != "" {
		policy.WriteDetail(w, answer.status, answer.detail, nil)
		return
	}
	if answer, ok := actorAnswers[reason]; ok && reason != "" {
		policy.WriteDetail(w, answer.status, answer.detail, nil)
		return
	}
	h.logger.WarnContext(ctx, "admin: auth-service did not take the change; nothing was written",
		"route", route, "refused", errors.Is(err, authprovision.ErrRefused), "reason", reason)
	policy.WriteDetail(w, http.StatusServiceUnavailable, policy.ErrorDetail(sessionissue.UnavailableText), nil)
}

func (h *handlers) writeProvisionUnavailable(w http.ResponseWriter) {
	policy.WriteDetail(w, http.StatusServiceUnavailable, policy.ErrorDetail(sessionissue.UnavailableText), nil)
}

func invalidRole(role string) map[string]provisionAnswer {
	return map[string]provisionAnswer{"role_invalid": {http.StatusBadRequest, "Invalid role: " + role}}
}

func (h *handlers) addMemberInAuth(ctx context.Context, w http.ResponseWriter, user *policy.User, orgID, target uuid.UUID, role string, invitedBy *uuid.UUID) {
	if !validMemberRoles[role] {
		policy.WriteDetail(w, http.StatusBadRequest, "Invalid role: "+role, nil)
		return
	}
	if _, err := h.orgProvision.AddMember(ctx, orgID, target, user.ID, role, invitedBy); err != nil {
		answers := invalidRole(role)
		answers["already_member"] = provisionAnswer{http.StatusBadRequest, "User is already a member of this organization"}
		h.writeProvisionRefusal(ctx, w, "add_member", err, answers)
		return
	}
	h.writeMembershipReadBack(ctx, w, orgID, target, http.StatusCreated)
}

func (h *handlers) updateMemberRoleInAuth(ctx context.Context, w http.ResponseWriter, user *policy.User, orgID, target uuid.UUID, role string) {
	if _, err := h.orgProvision.ChangeRole(ctx, orgID, target, user.ID, role); err != nil {
		answers := invalidRole(role)
		answers["membership_not_found"] = provisionAnswer{http.StatusNotFound, "Membership not found"}
		answers["last_owner"] = provisionAnswer{http.StatusBadRequest, "Cannot remove the last owner of an organization"}
		h.writeProvisionRefusal(ctx, w, "update_member_role", err, answers)
		return
	}
	h.writeMembershipReadBack(ctx, w, orgID, target, http.StatusOK)
}

// writeMembershipReadBack answers the public row auth-service wrote in its
// commit. A failed read or a row gone by now (a later change) is the 503:
// go-api cannot show the state it was asked for; a retry meets the defined
// refusal.
func (h *handlers) writeMembershipReadBack(ctx context.Context, w http.ResponseWriter, orgID, target uuid.UUID, status int) {
	m, err := h.store.membershipByOrgUser(ctx, orgID, target)
	if err != nil {
		h.logger.ErrorContext(ctx, "admin: read membership after auth change failed", "error", err)
		h.writeProvisionUnavailable(w)
		return
	}
	if m == nil {
		h.writeProvisionUnavailable(w)
		return
	}
	policy.WriteModel(w, status, membershipResponseObject(m), nil)
}

func (h *handlers) removeMemberInAuth(ctx context.Context, w http.ResponseWriter, user *policy.User, orgID, target uuid.UUID) {
	if err := h.orgProvision.RemoveMember(ctx, orgID, target, user.ID); err != nil {
		h.writeProvisionRefusal(ctx, w, "remove_member", err, map[string]provisionAnswer{
			"membership_not_found": {http.StatusNotFound, "Membership not found"},
			"last_owner":           {http.StatusBadRequest, "Cannot remove the last owner of an organization"},
		})
		return
	}
	out := pyjson.NewObject()
	out.Set("deleted", true)
	policy.WriteModel(w, http.StatusOK, out, nil)
}

// transferOwnershipInAuth resolves the current owner as today (one owner, read
// without a lock), then asks auth-service, which re-checks it under its locks.
func (h *handlers) transferOwnershipInAuth(ctx context.Context, w http.ResponseWriter, user *policy.User, orgID, to uuid.UUID) {
	from, err := h.store.soleOwner(ctx, orgID)
	if err != nil && err != errNotAnOwner {
		h.logger.ErrorContext(ctx, "admin: resolve current owner failed", "error", err)
		policy.WriteInternal(w)
		return
	}
	if err == errNotAnOwner {
		targetIsSuperuser := func() (bool, error) { return h.store.isSuperuser(ctx, to) }
		if h.refuseSuperuserWrite(ctx, w, user, nil, targetIsSuperuser, "transfer_ownership") {
			return
		}
		policy.WriteDetail(w, http.StatusBadRequest, "Source user is not an owner", nil)
		return
	}
	if err := h.orgProvision.TransferOwnership(ctx, orgID, from, to, user.ID); err != nil {
		h.writeProvisionRefusal(ctx, w, "transfer_ownership", err, map[string]provisionAnswer{
			"not_owner":         {http.StatusBadRequest, "Source user is not an owner"},
			"target_not_member": {http.StatusBadRequest, "Target user is not a member"},
		})
		return
	}
	out := pyjson.NewObject()
	out.Set("success", true)
	policy.WriteJSON(w, http.StatusOK, out, nil)
}

// createOrgInviteInAuth: auth-service writes the invite, its projection and
// go-api's member_invited audit row; go-api reads the row back and sends the
// mail with the token, best effort, as today.
func (h *handlers) createOrgInviteInAuth(ctx context.Context, w http.ResponseWriter, r *http.Request, orgName, inviter string, orgID uuid.UUID,
	email, role string, invitedBy uuid.UUID) {
	if !validMemberRoles[role] {
		policy.WriteDetail(w, http.StatusBadRequest, "Invalid role: "+role, nil)
		return
	}
	created, err := h.orgProvision.CreateInvite(ctx, authprovision.InviteRequest{OrgID: orgID, Email: email, Role: role, TTL: inviteTTL,
		Actor: invitedBy, Description: "Organization invite created", RequestMetadata: requestAuditEntry(r, audit.Entry{}).RequestMetadata})
	if authprovision.ReasonOf(err) == "email_invalid" {
		policy.WriteDetail(w, http.StatusBadRequest, invalidEmailText(email), nil)
		return
	}
	if err != nil {
		answers := invalidRole(role)
		answers["invite_pending"] = provisionAnswer{http.StatusConflict, errPendingInviteExists.Error()}
		answers["organization_not_found"] = provisionAnswer{http.StatusNotFound, "Organization not found"}
		h.writeProvisionRefusal(ctx, w, "create_org_invite", err, answers)
		return
	}
	// The mail needs only what go-api holds and the answered token, so it goes
	// out before the read-back: a failed read-back loses no invite.
	h.sendInviteEmail(ctx, orgName, inviter, &orgInvite{ID: created.ID, OrgID: orgID, Email: email}, created.Token)
	invite, err := h.store.inviteByID(ctx, created.ID)
	if err != nil {
		h.logger.ErrorContext(ctx, "admin: read invite after auth create failed", "error", err)
		h.writeProvisionUnavailable(w)
		return
	}
	if invite == nil {
		h.writeProvisionUnavailable(w)
		return
	}
	policy.WriteModel(w, http.StatusCreated, orgInviteResponseObject(invite), nil)
}

// invalidEmailText is go-api's invalid-email text for an address auth-service
// refused: a client error, never the 503 of an outage. go-api's own validator
// names its reason when it refuses the address too.
func invalidEmailText(email string) string {
	text := "value is not a valid email address"
	if _, reason, ok := emailvalidator.ValidateEmail([]rune(email)); !ok {
		text += ": " + reason
	}
	return text
}

// orgDeletionAuthError: auth-service did not deprovision the organization, so
// the delete stops before PagerDuty or any purge.
type orgDeletionAuthError struct{ err error }

func (e *orgDeletionAuthError) Error() string { return "auth deprovision: " + e.err.Error() }
func (e *orgDeletionAuthError) Unwrap() error { return e.err }

// deprovisionInAuth denies the organization in auth-service first (G-55). An
// organization auth-service does not hold (404) has nothing to deny, and the
// purge goes on, as today's delete of a missing organization does. A repeat
// after a failed purge is a 200 that changes nothing.
func (h *handlers) deprovisionInAuth(ctx context.Context, orgID uuid.UUID) error {
	user := policy.UserFrom(ctx)
	if user == nil {
		return &orgDeletionAuthError{errors.New("no acting user")}
	}
	err := h.orgProvision.Deprovision(ctx, orgID, user.ID)
	if err == nil || authprovision.ReasonOf(err) == "organization_not_found" {
		return nil
	}
	h.logger.WarnContext(ctx, "admin: auth-service did not deprovision the organization; nothing was deleted",
		"org_id", orgID.String(), "refused", errors.Is(err, authprovision.ErrRefused), "reason", authprovision.ReasonOf(err))
	return &orgDeletionAuthError{err}
}

func writeOrgDeletionAuthError(w http.ResponseWriter) {
	policy.WriteDetail(w, http.StatusServiceUnavailable, policy.ErrorDetail(sessionissue.UnavailableText), nil)
}

// soleOwner is currentOwner's read on the pool, outside any transaction.
func (s pgStore) soleOwner(ctx context.Context, orgID uuid.UUID) (uuid.UUID, error) {
	rows, err := s.Pool.Query(ctx, `SELECT user_id FROM memberships WHERE org_id = $1 AND role = 'owner'`, orgID)
	if err != nil {
		return uuid.UUID{}, err
	}
	defer rows.Close()
	var owners []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return uuid.UUID{}, err
		}
		owners = append(owners, id)
	}
	if err := rows.Err(); err != nil {
		return uuid.UUID{}, err
	}
	if len(owners) != 1 {
		return uuid.UUID{}, errNotAnOwner
	}
	return owners[0], nil
}

func (s pgStore) isSuperuser(ctx context.Context, userID uuid.UUID) (bool, error) {
	var superuser bool
	err := s.Pool.QueryRow(ctx, `SELECT is_superuser FROM users WHERE id = $1`, userID).Scan(&superuser)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	return superuser, err
}

// inviteByID reads one org_invites row; nil when it does not exist.
func (s pgStore) inviteByID(ctx context.Context, id uuid.UUID) (*orgInvite, error) {
	var invite orgInvite
	err := s.Pool.QueryRow(ctx, `SELECT id, org_id, email, role, invited_by_id, status, expires_at, accepted_at, created_at, updated_at
FROM org_invites WHERE id = $1`, id).Scan(&invite.ID, &invite.OrgID, &invite.Email, &invite.Role, &invite.InvitedByID, &invite.Status,
		&invite.ExpiresAt, &invite.AcceptedAt, &invite.CreatedAt, &invite.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &invite, nil
}
