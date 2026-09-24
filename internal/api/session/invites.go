package session

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/api/audit"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/auth/signedtoken"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// errInviteRefused carries services/invites.py accept_invite's ValueError
// text, which the routes answer as a 400.
type errInviteRefused struct{ message string }

func (e errInviteRefused) Error() string { return e.message }

// pendingInvite is the org_invites columns accept_invite reads.
type pendingInvite struct {
	ID          uuid.UUID
	OrgID       uuid.UUID
	Role        string
	InvitedByID *uuid.UUID
	ExpiresAt   time.Time
}

// validateInvite is services/invites.py validate_invite: the pending,
// unexpired invite token names, nil when Python returns None. An expired
// invite is marked expired there, but the route then refuses the request
// and the session rolls back, so the mark is never written.
func (h handlers) validateInvite(ctx context.Context, tx pgx.Tx, token string, now time.Time) (*pendingInvite, error) {
	valid, err := signedtoken.Valid(token, h.Mail.Secret())
	if err != nil || !valid {
		return nil, err
	}
	var invite pendingInvite
	var status string
	err = tx.QueryRow(ctx, `SELECT id, org_id, role, invited_by_id, status, expires_at FROM org_invites WHERE token_hash = $1::text`,
		signedtoken.Hash(token)).Scan(&invite.ID, &invite.OrgID, &invite.Role, &invite.InvitedByID, &status, &invite.ExpiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if status != "pending" || invite.ExpiresAt.Before(now) {
		return nil, nil
	}
	return &invite, nil
}

// acceptInvite is services/invites.py accept_invite (its status and expiry
// re-check cannot fail after validateInvite in the same transaction): the
// new membership, or errInviteRefused when the user already belongs to
// the organization.
func (h handlers) acceptInvite(ctx context.Context, tx pgx.Tx, invite *pendingInvite, userID uuid.UUID, now time.Time) (uuid.UUID, error) {
	var member bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM memberships WHERE org_id = $1::uuid AND user_id = $2::uuid)`,
		invite.OrgID, userID).Scan(&member); err != nil {
		return uuid.UUID{}, err
	}
	if member {
		return uuid.UUID{}, errInviteRefused{"User is already a member of this organization"}
	}
	id := h.NewUUID()
	if err := insertMembership(ctx, tx, id, userID, invite.OrgID, invite.Role, invite.InvitedByID, now); err != nil {
		return uuid.UUID{}, err
	}
	_, err := tx.Exec(ctx, `UPDATE org_invites SET status = 'accepted', accepted_at = $2, updated_at = $2 WHERE id = $1::uuid`,
		invite.ID, now.UTC())
	return id, err
}

// organizationName reads organizations.name; found is false without a row.
func organizationName(ctx context.Context, tx pgx.Tx, id uuid.UUID) (name string, found bool, err error) {
	err = tx.QueryRow(ctx, `SELECT name FROM organizations WHERE id = $1::uuid`, id).Scan(&name)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", false, nil
	}
	return name, err == nil, err
}

// callerRow is the invites routes' head: the caller's users row, or the
// refusal they answer (401 "Invalid user identity" / "User not found").
func (h handlers) callerRow(w http.ResponseWriter, r *http.Request, tx pgx.Tx) (*userRow, bool) {
	caller := policy.UserFrom(r.Context())
	id := parseUUID(&caller.UserID)
	if id == nil {
		refuse(w, http.StatusUnauthorized, "Invalid user identity")
		return nil, false
	}
	user, err := userByID(r.Context(), tx, *id)
	if err != nil {
		h.fail(w, r, "load user", err)
		return nil, false
	}
	if user == nil {
		refuse(w, http.StatusUnauthorized, "User not found")
		return nil, false
	}
	return user, true
}

// joinByInvite validates token, accepts it for user and audits the join. It
// writes the refusal itself and returns false on any failure.
func (h handlers) joinByInvite(w http.ResponseWriter, r *http.Request, tx pgx.Tx, user *userRow, token, description string, now time.Time) (*membershipRow, string, bool) {
	ctx := r.Context()
	invite, err := h.validateInvite(ctx, tx, token, now)
	if err != nil {
		h.fail(w, r, "validate invite", err)
		return nil, "", false
	}
	if invite == nil {
		refuse(w, http.StatusBadRequest, "Invalid or expired invite")
		return nil, "", false
	}
	orgName, found, err := organizationName(ctx, tx, invite.OrgID)
	if err != nil {
		h.fail(w, r, "load organization", err)
		return nil, "", false
	}
	if !found {
		refuse(w, http.StatusNotFound, "Organization not found")
		return nil, "", false
	}
	membershipID, err := h.acceptInvite(ctx, tx, invite, user.ID, now)
	var refused errInviteRefused
	if errors.As(err, &refused) {
		refuse(w, http.StatusBadRequest, refused.message)
		return nil, "", false
	}
	if err != nil {
		h.fail(w, r, "accept invite", err)
		return nil, "", false
	}
	changes := pyjson.NewObject()
	changes.Set("invite_id", invite.ID.String())
	changes.Set("user_id", user.ID.String())
	changes.Set("role", invite.Role)
	userID := user.ID
	if err := h.emitAudit(ctx, tx, r, auditEntry{orgID: invite.OrgID, action: audit.ActionMemberJoined, resourceType: audit.ResourceMembership,
		resourceID: membershipID.String(), userID: &userID, description: description, changes: changes}); err != nil {
		h.fail(w, r, "audit", err)
		return nil, "", false
	}
	role := invite.Role
	joined := now
	return &membershipRow{OrgID: invite.OrgID, Role: &role, JoinedAt: &joined, CreatedAt: now}, orgName, true
}

// commitThenIssue commits tx and then, as _issue_membership_tokens runs
// after the route's commit, mints and stores the token pair in a second
// transaction, and writes the membership token response.
func (h handlers) commitThenIssue(w http.ResponseWriter, r *http.Request, tx pgx.Tx, user *userRow, membership *membershipRow, orgName string) {
	ctx := r.Context()
	if err := tx.Commit(ctx); err != nil {
		h.fail(w, r, "commit", err)
		return
	}
	issue, err := h.Pool.Begin(ctx)
	if err != nil {
		h.fail(w, r, "begin", err)
		return
	}
	defer func() { _ = issue.Rollback(context.Background()) }()
	pair, err := h.issueMembershipTokens(ctx, issue, r, user, membership)
	if err != nil {
		h.fail(w, r, "issue tokens", err)
		return
	}
	if err := issue.Commit(ctx); err != nil {
		h.fail(w, r, "commit tokens", err)
		return
	}
	out := pyjson.NewObject()
	out.Set("access_token", pair.access)
	out.Set("refresh_token", pair.refresh)
	out.Set("token_type", "bearer")
	out.Set("expires_in", accessExpiresIn)
	out.Set("org_id", membership.OrgID.String())
	out.Set("org_name", orgName)
	out.Set("role", roleText(membership.Role))
	policy.WriteModel(w, http.StatusOK, out, nil)
}

// acceptInviteRoute is POST /api/v1/auth/accept-invite (invites.py).
func (h handlers) acceptInviteRoute(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	body, _ := policy.BodyFrom(ctx)
	var errs pybody.Errors
	var token string
	if object, ok := errs.Object(body); ok {
		token, _ = errs.RequiredString(object, "token", 0, 0)
	}
	if len(errs) > 0 {
		writeValidation(w, errs)
		return
	}
	tx, err := h.Pool.Begin(ctx)
	if err != nil {
		h.fail(w, r, "begin", err)
		return
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	user, ok := h.callerRow(w, r, tx)
	if !ok {
		return
	}
	membership, orgName, ok := h.joinByInvite(w, r, tx, user, token, "Invite accepted", h.Now())
	if !ok {
		return
	}
	h.commitThenIssue(w, r, tx, user, membership, orgName)
}

// validateOrganizationName is services/users.py validate_organization_name.
func validateOrganizationName(name *string) (string, error) {
	normalized := ""
	if name != nil {
		normalized = pythonparity.Strip(*name)
	}
	if normalized == "" {
		return "", errors.New("Workspace name is required")
	}
	if len(pyjson.Runes(normalized)) > 100 {
		return "", errors.New("Workspace name must be 100 characters or fewer")
	}
	return normalized, nil
}

// onboard is POST /api/v1/auth/onboard (invites.py).
func (h handlers) onboard(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	body, _ := policy.BodyFrom(ctx)
	var errs pybody.Errors
	var action string
	var orgName, inviteCode *string
	if object, ok := errs.Object(body); ok {
		action, _ = errs.RequiredString(object, "action", 0, 0)
		if value, present := errs.OptionalString(object, "org_name", 0, 0); present {
			orgName = &value
		}
		if value, present := errs.OptionalString(object, "invite_code", 0, 0); present {
			inviteCode = &value
		}
	}
	if len(errs) > 0 {
		writeValidation(w, errs)
		return
	}
	tx, err := h.Pool.Begin(ctx)
	if err != nil {
		h.fail(w, r, "begin", err)
		return
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	user, ok := h.callerRow(w, r, tx)
	if !ok {
		return
	}
	var onboarded bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM memberships WHERE user_id = $1::uuid)`, user.ID).Scan(&onboarded); err != nil {
		h.fail(w, r, "load memberships", err)
		return
	}
	if onboarded {
		refuse(w, http.StatusBadRequest, "Already onboarded")
		return
	}
	now := h.Now()
	if action == "join_org" {
		if inviteCode == nil || *inviteCode == "" {
			refuse(w, http.StatusBadRequest, "invite_code is required")
			return
		}
		membership, name, ok := h.joinByInvite(w, r, tx, user, *inviteCode, "Invite accepted during onboarding", now)
		if !ok {
			return
		}
		h.commitThenIssue(w, r, tx, user, membership, name)
		return
	}
	if action != "create_org" {
		refuse(w, http.StatusBadRequest, "Invalid action. Use 'create_org' or 'join_org'")
		return
	}
	name, err := validateOrganizationName(orgName)
	if err != nil {
		refuseViolations(w, http.StatusUnprocessableEntity, err.Error(), []string{err.Error()})
		return
	}
	orgID := h.NewUUID()
	if err := insertOrganization(ctx, tx, orgID, slugifyOrgName(name)+"-"+user.ID.String()[:8], name, now); err != nil {
		h.fail(w, r, "insert organization", err)
		return
	}
	if err := insertMembership(ctx, tx, h.NewUUID(), user.ID, orgID, "owner", nil, now); err != nil {
		h.fail(w, r, "insert membership", err)
		return
	}
	changes := pyjson.NewObject()
	changes.Set("organization_name", name)
	userID := user.ID
	if err := h.emitAudit(ctx, tx, r, auditEntry{orgID: orgID, action: audit.ActionCreate, resourceType: audit.ResourceOrganization,
		resourceID: orgID.String(), userID: &userID, description: "Organization created during onboarding", changes: changes}); err != nil {
		h.fail(w, r, "audit", err)
		return
	}
	role := "owner"
	h.commitThenIssue(w, r, tx, user, &membershipRow{OrgID: orgID, Role: &role, JoinedAt: &now, CreatedAt: now}, name)
}

// slugifyOrgName is common._slugify_org_name:
// re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")[:50], or
// "my-organization" when that is empty. The class is ASCII, so every run of
// any other code point, "-" included, becomes one "-".
func slugifyOrgName(name string) string {
	var slug []rune
	inRun := false
	for _, r := range pyjson.Runes(pythonparity.Lower(name)) {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			slug = append(slug, r)
			inRun = false
			continue
		}
		if !inRun {
			slug = append(slug, '-')
			inRun = true
		}
	}
	for len(slug) > 0 && slug[0] == '-' {
		slug = slug[1:]
	}
	for len(slug) > 0 && slug[len(slug)-1] == '-' {
		slug = slug[:len(slug)-1]
	}
	if len(slug) > 50 {
		slug = slug[:50]
	}
	if len(slug) == 0 {
		return "my-organization"
	}
	return string(slug)
}
