package admin

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/api/audit"
	"github.com/full-chaos/dev-health-ops/internal/api/authmail"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/auth/signedtoken"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// inviteTTL is create_invite's ttl_hours=72 default, the only value the route
// ever passes.
const inviteTTL = 72 * time.Hour

// errPendingInviteExists is create_invite's ValueError("A pending invite
// already exists for this email"), which the route maps to 409.
var errPendingInviteExists = errors.New("A pending invite already exists for this email")

// InviteConfig is what create_org_invite needs beyond the database: the
// link-mail configuration it shares with e-mail verification and password
// reset.
type InviteConfig = authmail.Config

// orgInvite is one org_invites row, as create_org_invite returns it.
type orgInvite struct {
	ID          uuid.UUID
	OrgID       uuid.UUID
	Email       string
	Role        string
	InvitedByID *uuid.UUID
	Status      string
	ExpiresAt   time.Time
	AcceptedAt  *time.Time
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

func orgInviteResponseObject(invite *orgInvite) *pyjson.Object {
	out := pyjson.NewObject()
	out.Set("id", invite.ID.String())
	out.Set("org_id", invite.OrgID.String())
	out.Set("email", invite.Email)
	out.Set("role", invite.Role)
	if invite.InvitedByID != nil {
		out.Set("invited_by_id", invite.InvitedByID.String())
	} else {
		out.Set("invited_by_id", nil)
	}
	out.Set("status", invite.Status)
	out.Set("expires_at", pyTimeString(invite.ExpiresAt))
	if invite.AcceptedAt != nil {
		out.Set("accepted_at", pyTimeString(*invite.AcceptedAt))
	} else {
		out.Set("accepted_at", nil)
	}
	out.Set("created_at", pyTimeString(invite.CreatedAt))
	out.Set("updated_at", pyTimeString(invite.UpdatedAt))
	return out
}

// inviterName is create_org_invite's display-name chain: the inviter's
// full_name, else their email, else the token's own email claim.
func inviterName(fullName, email *string, claimEmail string) string {
	if fullName != nil && *fullName != "" {
		return *fullName
	}
	if email != nil && *email != "" {
		return *email
	}
	return claimEmail
}

// inviteInput is OrgInviteCreate after validation.
type inviteInput struct {
	Email string
	Role  string
}

type inviteInputKey struct{}

// validateInviteBody is OrgInviteCreate's pydantic validation, the
// httpapi.RequestValidator of the invite route: it runs BEFORE the rate
// limiter and before the org-access check (httpapi.ValidateThenLimit is the
// shared stage that orders them), as FastAPI validates before the endpoint
// slowapi wraps. A malformed body is a 422 that costs no allowance, and it
// is answered before the org-access check (an admin of another org sending a
// malformed body gets 422, not 403).
func validateInviteBody(w http.ResponseWriter, r *http.Request) (*http.Request, bool) {
	var errs pybody.Errors
	object, ok := errs.Object(bodyFromContext(r.Context()))
	input := inviteInput{Role: "member"}
	if ok {
		input.Email, _ = errs.RequiredString(object, "email", 3, 0)
		if value, present := errs.DefaultedString(object, "role", 0, 0); present {
			input.Role = value
		}
	}
	if len(errs) > 0 {
		policy.WriteJSON(w, http.StatusUnprocessableEntity, pybody.Detail(errs), nil)
		return nil, false
	}
	return r.WithContext(context.WithValue(r.Context(), inviteInputKey{}, input)), true
}

// createOrgInvite is orgs.py's create_org_invite: POST
// /orgs/{org_id}/invites. It runs after validateInviteBody and the keyed
// rate limiter.
func (h *handlers) createOrgInvite(w http.ResponseWriter, r *http.Request) {
	user := policy.UserFrom(r.Context())
	ctx := r.Context()
	input, _ := ctx.Value(inviteInputKey{}).(inviteInput)
	orgID, parseErr := uuid.Parse(r.PathValue("org_id"))
	if parseErr != nil {
		policy.WriteInternal(w)
		return
	}
	if !h.ensureOrgAdminAccess(ctx, w, user, orgID) {
		return
	}
	emailRaw, role := input.Email, input.Role

	// `uuid.UUID(current_user.user_id)` cannot fail here: the guard already
	// parsed the token's subject into user.ID (Python's 401 "Invalid user
	// identity" branch is unreachable for an authenticated principal).
	invitedByID := user.ID

	org, err := h.store.orgByID(ctx, orgID)
	if err != nil {
		h.logger.ErrorContext(ctx, "admin: create invite org lookup failed", "error", err)
		policy.WriteInternal(w)
		return
	}
	if org == nil {
		policy.WriteDetail(w, http.StatusNotFound, "Organization not found", nil)
		return
	}
	inviterFullName, inviterEmail, err := h.store.inviterIdentity(ctx, invitedByID)
	if err != nil {
		h.logger.ErrorContext(ctx, "admin: create invite inviter lookup failed", "error", err)
		policy.WriteInternal(w)
		return
	}
	displayName := inviterName(inviterFullName, inviterEmail, user.Email)

	normalizedEmail := pythonparity.Strip(pythonparity.Lower(emailRaw))
	if role == "" {
		role = "member"
	}
	invite, token, err := h.createInvite(ctx, r, orgID, normalizedEmail, role, invitedByID)
	if errors.Is(err, errPendingInviteExists) {
		policy.WriteDetail(w, http.StatusConflict, err.Error(), nil)
		return
	}
	if err != nil {
		h.logger.ErrorContext(ctx, "admin: create invite failed", "error", err)
		policy.WriteInternal(w)
		return
	}

	// Best effort, after the invite and its audit row are committed:
	// send_invite_email's failure is logged and never surfaced, so the
	// response is the same whether or not the mail went out. (Python sends
	// before its request-scoped commit; committing first means a failed
	// commit can no longer leave an email pointing at an invite that was
	// never stored.)
	h.sendInviteEmail(ctx, org.Name, displayName, invite, token)

	policy.WriteModel(w, http.StatusCreated, orgInviteResponseObject(invite), nil)
}

// createInvite is invites.py's create_invite plus the route's audit row, in
// one transaction: the pending-duplicate check, the insert, and the
// member_invited audit entry commit together or not at all.
func (h *handlers) createInvite(ctx context.Context, r *http.Request, orgID uuid.UUID, email, role string, invitedByID uuid.UUID) (*orgInvite, string, error) {
	tx, err := h.store.Pool.Begin(ctx)
	if err != nil {
		return nil, "", err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	now := h.store.now().UTC()
	var exists bool
	if err := tx.QueryRow(ctx, `
SELECT EXISTS (
  SELECT 1 FROM org_invites
  WHERE org_id = $1 AND lower(email) = $2 AND status = 'pending' AND expires_at >= $3
)`, orgID, email, now).Scan(&exists); err != nil {
		return nil, "", err
	}
	if exists {
		return nil, "", errPendingInviteExists
	}

	id := uuid.New()
	token, tokenHash := signedtoken.Build(id, h.invites.Secret())
	stamp := h.store.now().UTC()
	invite := &orgInvite{
		ID: id, OrgID: orgID, Email: email, Role: role, InvitedByID: &invitedByID,
		Status: "pending", ExpiresAt: now.Add(inviteTTL), CreatedAt: stamp, UpdatedAt: stamp,
	}
	if _, err := tx.Exec(ctx, `
INSERT INTO org_invites (id, org_id, email, role, token_hash, invited_by_id, status, expires_at, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
		invite.ID, invite.OrgID, invite.Email, invite.Role, tokenHash, invite.InvitedByID, invite.Status,
		invite.ExpiresAt, invite.CreatedAt, invite.UpdatedAt); err != nil {
		return nil, "", err
	}

	changes := pyjson.NewObject()
	changes.Set("email", invite.Email)
	changes.Set("role", invite.Role)
	changes.Set("status", invite.Status)
	encodedChanges, err := pyjson.Marshal(changes)
	if err != nil {
		return nil, "", err
	}
	description := "Organization invite created"
	if _, err := h.audit.Write(ctx, tx, requestAuditEntry(r, audit.Entry{
		OrgID: orgID, UserID: &invitedByID, Action: audit.ActionMemberInvited,
		ResourceType: audit.ResourceMembership, ResourceID: invite.ID.String(),
		Description: &description, Changes: encodedChanges,
	})); err != nil {
		return nil, "", err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, "", err
	}
	return invite, token, nil
}

// sendInviteEmail is invites.py's send_invite_email, best effort: any failure
// is logged and swallowed.
func (h *handlers) sendInviteEmail(ctx context.Context, orgName, inviter string, invite *orgInvite, token string) {
	// The address is not logged: it is the invitee's, and the invite id
	// identifies the row.
	h.invites.Send(ctx, h.logger, "admin: failed to send invite email", invite.Email,
		"You're invited to join "+orgName, "invite", map[string]string{
			"org_name": orgName, "inviter_name": inviter, "accept_url": h.invites.Link("/accept-invite", token),
		}, "invite_id", invite.ID.String(), "org_id", invite.OrgID.String())
}

// inviterIdentity reads the inviting user's full_name and email; both nil when
// the row does not exist.
func (s pgStore) inviterIdentity(ctx context.Context, userID uuid.UUID) (fullName, email *string, err error) {
	err = s.Pool.QueryRow(ctx, `SELECT full_name, email FROM users WHERE id = $1`, userID).Scan(&fullName, &email)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil, nil
	}
	return fullName, email, err
}
