package admin

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/audit"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
)

const orgsPrefix = "/api/v1/admin"

func (h *handlers) orgRoutes() []httpapi.Route {
	return []httpapi.Route{
		{Method: http.MethodGet, Pattern: orgsPrefix + "/orgs", Handler: h.guard.Wrap(policy.Superuser, http.HandlerFunc(h.listOrganizations))},
		{Method: http.MethodGet, Pattern: orgsPrefix + "/orgs/{org_id}", Handler: h.guard.Wrap(policy.Superuser, http.HandlerFunc(h.getOrganization))},
		{Method: http.MethodPost, Pattern: orgsPrefix + "/orgs", Handler: h.guard.Wrap(policy.Superuser, http.HandlerFunc(h.createOrganization))},
		{Method: http.MethodPatch, Pattern: orgsPrefix + "/orgs/{org_id}", Handler: h.guard.Wrap(policy.Superuser, http.HandlerFunc(h.updateOrganization))},
		{Method: http.MethodDelete, Pattern: orgsPrefix + "/orgs/{org_id}", Handler: h.guard.Wrap(policy.Superuser, http.HandlerFunc(h.deleteOrganizationStub))},
		{Method: http.MethodGet, Pattern: orgsPrefix + "/orgs/{org_id}/members", Handler: h.guard.Wrap(policy.Admin, http.HandlerFunc(h.listMembers))},
		{Method: http.MethodPost, Pattern: orgsPrefix + "/orgs/{org_id}/members", Handler: h.guard.Wrap(policy.Admin, http.HandlerFunc(h.addMember))},
		{Method: http.MethodPost, Pattern: orgsPrefix + "/orgs/{org_id}/invites",
			Handler: h.guard.Wrap(policy.Admin, http.HandlerFunc(h.createOrgInvite)), RateLimitPerSecond: 1, RateLimitBurst: 10},
		{Method: http.MethodPatch, Pattern: orgsPrefix + "/orgs/{org_id}/members/{user_id}", Handler: h.guard.Wrap(policy.Admin, http.HandlerFunc(h.updateMemberRole))},
		{Method: http.MethodDelete, Pattern: orgsPrefix + "/orgs/{org_id}/members/{user_id}", Handler: h.guard.Wrap(policy.Admin, http.HandlerFunc(h.removeMember))},
		{Method: http.MethodPost, Pattern: orgsPrefix + "/orgs/{org_id}/transfer-ownership", Handler: h.guard.Wrap(policy.Admin, http.HandlerFunc(h.transferOwnership))},
	}
}

func organizationResponseObject(org *organization) (*pyjson.Object, error) {
	settings, err := settingsObject(org.Settings)
	if err != nil {
		return nil, err
	}
	out := pyjson.NewObject()
	out.Set("id", org.ID.String())
	out.Set("slug", org.Slug)
	out.Set("name", org.Name)
	out.Set("description", optionalString(org.Description))
	out.Set("tier", org.Tier)
	out.Set("settings", settings)
	out.Set("is_active", org.IsActive)
	out.Set("created_at", pyjson.TimeString(org.CreatedAt))
	out.Set("updated_at", pyjson.TimeString(org.UpdatedAt))
	return out, nil
}

func membershipResponseObject(m *membership) *pyjson.Object {
	out := pyjson.NewObject()
	out.Set("id", m.ID.String())
	out.Set("org_id", m.OrgID.String())
	out.Set("user_id", m.UserID.String())
	out.Set("role", m.Role)
	if m.InvitedByID != nil {
		out.Set("invited_by_id", m.InvitedByID.String())
	} else {
		out.Set("invited_by_id", nil)
	}
	if m.JoinedAt != nil {
		out.Set("joined_at", pyjson.TimeString(*m.JoinedAt))
	} else {
		out.Set("joined_at", nil)
	}
	out.Set("created_at", pyjson.TimeString(m.CreatedAt))
	out.Set("updated_at", pyjson.TimeString(m.UpdatedAt))
	return out
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
	out.Set("expires_at", pyjson.TimeString(invite.ExpiresAt))
	if invite.AcceptedAt != nil {
		out.Set("accepted_at", pyjson.TimeString(*invite.AcceptedAt))
	} else {
		out.Set("accepted_at", nil)
	}
	out.Set("created_at", pyjson.TimeString(invite.CreatedAt))
	out.Set("updated_at", pyjson.TimeString(invite.UpdatedAt))
	return out
}

// listOrganizations is orgs.py's list_organizations.
func (h *handlers) listOrganizations(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	query := r.URL.Query()
	limit, qerr := queryInt(query, "limit", 100)
	if qerr != nil {
		writeQueryError(w, qerr)
		return
	}
	offset, qerr := queryInt(query, "offset", 0)
	if qerr != nil {
		writeQueryError(w, qerr)
		return
	}
	activeOnly, qerr := queryBool(query, "active_only", true)
	if qerr != nil {
		writeQueryError(w, qerr)
		return
	}
	orgs, err := h.store.listOrganizations(ctx, limit, offset, activeOnly)
	if err != nil {
		h.logger.ErrorContext(ctx, "admin: list organizations failed", "error", err)
		policy.WriteInternal(w)
		return
	}
	list := make([]pyjson.Value, len(orgs))
	for i, org := range orgs {
		obj, err := organizationResponseObject(org)
		if err != nil {
			h.logger.ErrorContext(ctx, "admin: encode organization failed", "error", err)
			policy.WriteInternal(w)
			return
		}
		list[i] = obj
	}
	policy.WritePyJSON(w, http.StatusOK, list, nil)
}

// getOrganization is orgs.py's get_organization.
func (h *handlers) getOrganization(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	orgID, parseErr := uuid.Parse(r.PathValue("org_id"))
	if parseErr != nil {
		policy.WriteInternal(w)
		return
	}
	org, err := h.store.orgByID(ctx, orgID)
	if err != nil {
		h.logger.ErrorContext(ctx, "admin: get organization failed", "error", err)
		policy.WriteInternal(w)
		return
	}
	if org == nil {
		policy.WriteDetail(w, http.StatusNotFound, "Organization not found", nil)
		return
	}
	obj, err := organizationResponseObject(org)
	if err != nil {
		h.logger.ErrorContext(ctx, "admin: encode organization failed", "error", err)
		policy.WriteInternal(w)
		return
	}
	policy.WritePyJSON(w, http.StatusOK, obj, nil)
}

// createOrganization is orgs.py's create_organization.
func (h *handlers) createOrganization(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	body, outcome, failure, err := pybody.Read(r)
	if !handleBodyOutcome(w, body, outcome, failure, err) {
		return
	}
	var errs pybody.Errors
	object, ok := errs.Object(body)
	var name, slug, description, tier, ownerUserID string
	if ok {
		name, _ = errs.RequiredString(object, "name", 1, 100)
		slug, _ = errs.OptionalString(object, "slug", 0, 0)
		description, _ = errs.OptionalString(object, "description", 0, 0)
		tier, _ = errs.OptionalString(object, "tier", 0, 0)
		ownerUserID, _ = errs.OptionalString(object, "owner_user_id", 0, 0)
	}
	if len(errs) > 0 {
		policy.WritePyJSON(w, http.StatusUnprocessableEntity, pybody.Detail(errs), nil)
		return
	}
	if tier == "" {
		tier = "community"
	}
	if slug == "" {
		slug = slugify(name)
	}
	if existing, err := h.store.orgBySlug(ctx, slug); err != nil {
		h.logger.ErrorContext(ctx, "admin: slug lookup failed", "error", err)
		policy.WriteInternal(w)
		return
	} else if existing != nil {
		slug = slug + "-" + randomHex4()
	}
	managedBy := "stripe"
	if tier != "community" {
		managedBy = "manual"
	}
	org := &organization{
		ID: uuid.New(), Slug: slug, Name: name, Tier: tier, ManagedBy: managedBy, IsActive: true,
		Settings: []byte("{}"),
	}
	if description != "" {
		org.Description = &description
	}
	if err := h.store.insertOrganization(ctx, org); err != nil {
		h.logger.ErrorContext(ctx, "admin: create organization failed", "error", err)
		policy.WriteInternal(w)
		return
	}
	if ownerUserID != "" {
		ownerID, parseErr := uuid.Parse(ownerUserID)
		if parseErr == nil {
			if _, err := h.store.insertMembership(ctx, org.ID, ownerID, "owner", nil); err != nil {
				h.logger.ErrorContext(ctx, "admin: create organization owner membership failed", "error", err)
				policy.WriteInternal(w)
				return
			}
		}
	}
	obj, err := organizationResponseObject(org)
	if err != nil {
		h.logger.ErrorContext(ctx, "admin: encode organization failed", "error", err)
		policy.WriteInternal(w)
		return
	}
	policy.WritePyJSON(w, http.StatusCreated, obj, nil)
}

// updateOrganization is orgs.py's update_organization.
func (h *handlers) updateOrganization(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	orgID, parseErr := uuid.Parse(r.PathValue("org_id"))
	if parseErr != nil {
		policy.WriteInternal(w)
		return
	}
	body, outcome, failure, err := pybody.Read(r)
	if !handleBodyOutcome(w, body, outcome, failure, err) {
		return
	}
	var errs pybody.Errors
	object, ok := errs.Object(body)
	patch := orgUpdate{}
	if ok {
		if v, present := errs.OptionalString(object, "name", 0, 0); present {
			patch.Name = &v
		}
		if v, present := errs.OptionalString(object, "description", 0, 0); present {
			patch.Description = &v
		}
		if v, present := errs.OptionalString(object, "tier", 0, 0); present {
			patch.Tier = &v
		}
		if v, present := errs.OptionalBool(object, "is_active"); present {
			patch.IsActive = &v
		}
		if raw, present := object.Get("settings"); present && raw != nil {
			if encoded, err := pyjson.Marshal(raw); err == nil {
				patch.Settings = encoded
			}
		}
	}
	if len(errs) > 0 {
		policy.WritePyJSON(w, http.StatusUnprocessableEntity, pybody.Detail(errs), nil)
		return
	}
	updated, err := h.store.updateOrganization(ctx, orgID, patch)
	if err != nil {
		h.logger.ErrorContext(ctx, "admin: update organization failed", "error", err)
		policy.WriteInternal(w)
		return
	}
	if updated == nil {
		policy.WriteDetail(w, http.StatusNotFound, "Organization not found", nil)
		return
	}
	obj, err := organizationResponseObject(updated)
	if err != nil {
		h.logger.ErrorContext(ctx, "admin: encode organization failed", "error", err)
		policy.WriteInternal(w)
		return
	}
	policy.WritePyJSON(w, http.StatusOK, obj, nil)
}

// deleteOrganizationStub: DELETE /orgs/{org_id} is intentionally NOT ported
// in this PR -- see the PR's RISK-NOTES. org_deletion.py is a 660-line
// dynamic scan+purge across ~40 Postgres tables plus every ClickHouse table
// discovered by regexing the migration files for org_id columns, plus a
// PagerDuty OAuth revocation step; it is its own port, not a route handler.
// Stubbed 501, matching this admin surface's existing drift-review-endpoint
// precedent for "not yet ported natively" (see ops AGENTS.md's CS6 note).
func (h *handlers) deleteOrganizationStub(w http.ResponseWriter, r *http.Request) {
	policy.WriteDetail(w, http.StatusNotImplemented, "Organization deletion is not yet available on this endpoint", nil)
}

// listMembers is orgs.py's list_members.
func (h *handlers) listMembers(w http.ResponseWriter, r *http.Request) {
	user := policy.UserFrom(r.Context())
	ctx := r.Context()
	orgID, parseErr := uuid.Parse(r.PathValue("org_id"))
	if parseErr != nil {
		policy.WriteInternal(w)
		return
	}
	if !h.ensureOrgAdminAccess(ctx, w, user, orgID) {
		return
	}
	query := r.URL.Query()
	var role *string
	if raw := query.Get("role"); raw != "" {
		role = &raw
	}
	members, err := h.store.listMemberships(ctx, orgID, role)
	if err != nil {
		h.logger.ErrorContext(ctx, "admin: list members failed", "error", err)
		policy.WriteInternal(w)
		return
	}
	list := make([]pyjson.Value, len(members))
	for i, m := range members {
		list[i] = membershipResponseObject(m)
	}
	policy.WritePyJSON(w, http.StatusOK, list, nil)
}

// addMember is orgs.py's add_member.
func (h *handlers) addMember(w http.ResponseWriter, r *http.Request) {
	user := policy.UserFrom(r.Context())
	ctx := r.Context()
	orgID, parseErr := uuid.Parse(r.PathValue("org_id"))
	if parseErr != nil {
		policy.WriteInternal(w)
		return
	}
	if !h.ensureOrgAdminAccess(ctx, w, user, orgID) {
		return
	}
	body, outcome, failure, err := pybody.Read(r)
	if !handleBodyOutcome(w, body, outcome, failure, err) {
		return
	}
	var errs pybody.Errors
	object, ok := errs.Object(body)
	var targetUserIDRaw, role, invitedByRaw string
	if ok {
		targetUserIDRaw, _ = errs.RequiredString(object, "user_id", 1, 0)
		role, _ = errs.OptionalString(object, "role", 0, 0)
		invitedByRaw, _ = errs.OptionalString(object, "invited_by_id", 0, 0)
	}
	if len(errs) > 0 {
		policy.WritePyJSON(w, http.StatusUnprocessableEntity, pybody.Detail(errs), nil)
		return
	}
	if role == "" {
		role = "member"
	}
	targetUserID, parseErr := uuid.Parse(targetUserIDRaw)
	if parseErr != nil {
		policy.WriteDetail(w, http.StatusBadRequest, "Invalid user_id", nil)
		return
	}
	var invitedByID *uuid.UUID
	if invitedByRaw != "" {
		if id, err := uuid.Parse(invitedByRaw); err == nil {
			invitedByID = &id
		}
	}
	created, err := h.store.insertMembership(ctx, orgID, targetUserID, role, invitedByID)
	if err == errMembershipExists {
		policy.WriteDetail(w, http.StatusBadRequest, "User is already a member of this organization", nil)
		return
	}
	if err != nil {
		h.logger.ErrorContext(ctx, "admin: add member failed", "error", err)
		policy.WriteInternal(w)
		return
	}
	policy.WritePyJSON(w, http.StatusCreated, membershipResponseObject(created), nil)
}

// createOrgInvite is orgs.py's create_org_invite: audited (member_invited)
// and best-effort emails the invite (a send failure never fails the
// request -- matches the Python route's bare `except Exception: log`).
func (h *handlers) createOrgInvite(w http.ResponseWriter, r *http.Request) {
	user := policy.UserFrom(r.Context())
	ctx := r.Context()
	orgID, parseErr := uuid.Parse(r.PathValue("org_id"))
	if parseErr != nil {
		policy.WriteInternal(w)
		return
	}
	if !h.ensureOrgAdminAccess(ctx, w, user, orgID) {
		return
	}
	invitedByID, parseErr := uuid.Parse(user.UserID)
	if parseErr != nil {
		policy.WriteDetail(w, http.StatusUnauthorized, "Invalid user identity", nil)
		return
	}

	body, outcome, failure, err := pybody.Read(r)
	if !handleBodyOutcome(w, body, outcome, failure, err) {
		return
	}
	var errs pybody.Errors
	object, ok := errs.Object(body)
	var email, role string
	if ok {
		email, _ = errs.RequiredString(object, "email", 3, 0)
		role, _ = errs.OptionalString(object, "role", 0, 0)
	}
	if len(errs) > 0 {
		policy.WritePyJSON(w, http.StatusUnprocessableEntity, pybody.Detail(errs), nil)
		return
	}
	if role == "" {
		role = "member"
	}

	orgName, found, err := h.store.orgNameByID(ctx, orgID)
	if err != nil {
		h.logger.ErrorContext(ctx, "admin: invite org lookup failed", "error", err)
		policy.WriteInternal(w)
		return
	}
	if !found {
		policy.WriteDetail(w, http.StatusNotFound, "Organization not found", nil)
		return
	}

	inviterFullName, inviterEmail, err := h.store.inviterDisplay(ctx, invitedByID)
	if err != nil {
		h.logger.ErrorContext(ctx, "admin: inviter lookup failed", "error", err)
		policy.WriteInternal(w)
		return
	}
	inviterName := user.Email
	if inviterFullName != nil && *inviterFullName != "" {
		inviterName = *inviterFullName
	} else if inviterEmail != nil && *inviterEmail != "" {
		inviterName = *inviterEmail
	}

	tokenID := uuid.New()
	token := buildInviteToken(tokenID)
	invite, err := h.store.insertInvite(ctx, orgID, email, role, invitedByID, hashInviteToken(token), 72*time.Hour)
	if err == errPendingInviteExists {
		policy.WriteDetail(w, http.StatusConflict, "A pending invite already exists for this email", nil)
		return
	}
	if err != nil {
		h.logger.ErrorContext(ctx, "admin: create invite failed", "error", err)
		policy.WriteInternal(w)
		return
	}

	changes := pyjson.NewObject()
	changes.Set("email", invite.Email)
	changes.Set("role", invite.Role)
	changes.Set("status", invite.Status)
	changesBytes, _ := pyjson.Marshal(changes)
	description := "Organization invite created"
	if _, err := h.audit.Write(ctx, requestAuditEntry(r, audit.Entry{
		OrgID: orgID, UserID: &invitedByID, Action: audit.ActionMemberInvited, ResourceType: audit.ResourceMembership,
		ResourceID: invite.ID.String(), Description: &description, Changes: changesBytes,
	})); err != nil {
		h.logger.ErrorContext(ctx, "admin: invite audit write failed", "error", err)
		policy.WriteInternal(w)
		return
	}

	if err := sendInviteEmail(ctx, h.logger, invite.Email, orgName, inviterName, token); err != nil {
		h.logger.ErrorContext(ctx, "admin: invite email send failed", "error", err)
	}

	policy.WritePyJSON(w, http.StatusCreated, orgInviteResponseObject(invite), nil)
}

// updateMemberRole is orgs.py's update_member_role.
func (h *handlers) updateMemberRole(w http.ResponseWriter, r *http.Request) {
	user := policy.UserFrom(r.Context())
	ctx := r.Context()
	orgID, parseErr := uuid.Parse(r.PathValue("org_id"))
	if parseErr != nil {
		policy.WriteInternal(w)
		return
	}
	if !h.ensureOrgAdminAccess(ctx, w, user, orgID) {
		return
	}
	targetUserID, parseErr := uuid.Parse(r.PathValue("user_id"))
	if parseErr != nil {
		policy.WriteInternal(w)
		return
	}
	body, outcome, failure, err := pybody.Read(r)
	if !handleBodyOutcome(w, body, outcome, failure, err) {
		return
	}
	var errs pybody.Errors
	object, ok := errs.Object(body)
	var role string
	if ok {
		role, _ = errs.RequiredString(object, "role", 1, 0)
	}
	if len(errs) > 0 {
		policy.WritePyJSON(w, http.StatusUnprocessableEntity, pybody.Detail(errs), nil)
		return
	}
	updated, err := h.store.updateMembershipRole(ctx, orgID, targetUserID, role)
	if err == errInvalidRole {
		policy.WriteDetail(w, http.StatusBadRequest, "Invalid role: "+role, nil)
		return
	}
	if err != nil {
		h.logger.ErrorContext(ctx, "admin: update member role failed", "error", err)
		policy.WriteInternal(w)
		return
	}
	if updated == nil {
		policy.WriteDetail(w, http.StatusNotFound, "Membership not found", nil)
		return
	}
	policy.WritePyJSON(w, http.StatusOK, membershipResponseObject(updated), nil)
}

// removeMember is orgs.py's remove_member.
func (h *handlers) removeMember(w http.ResponseWriter, r *http.Request) {
	user := policy.UserFrom(r.Context())
	ctx := r.Context()
	orgID, parseErr := uuid.Parse(r.PathValue("org_id"))
	if parseErr != nil {
		policy.WriteInternal(w)
		return
	}
	if !h.ensureOrgAdminAccess(ctx, w, user, orgID) {
		return
	}
	targetUserID, parseErr := uuid.Parse(r.PathValue("user_id"))
	if parseErr != nil {
		policy.WriteInternal(w)
		return
	}
	deleted, err := h.store.removeMembership(ctx, orgID, targetUserID)
	if err == errLastOwner {
		policy.WriteDetail(w, http.StatusBadRequest, "Cannot remove the last owner of an organization", nil)
		return
	}
	if err != nil {
		h.logger.ErrorContext(ctx, "admin: remove member failed", "error", err)
		policy.WriteInternal(w)
		return
	}
	if !deleted {
		policy.WriteDetail(w, http.StatusNotFound, "Membership not found", nil)
		return
	}
	out := pyjson.NewObject()
	out.Set("deleted", true)
	policy.WritePyJSON(w, http.StatusOK, out, nil)
}

// transferOwnership is orgs.py's transfer_ownership, INTENTIONALLY
// diverging from the Python route's own URL shape
// (POST /orgs/{org_id}/transfer-ownership/{from_user_id}): the web is the
// only caller and never supplies from_user_id
// (web/src/lib/admin/api/orgs.ts calls POST /orgs/{orgId}/transfer-ownership
// with {new_owner_user_id} only), so the Python path is dead code nobody
// calls. Go serves the web's shape and resolves from_user_id server-side as
// the org's CURRENT owner -- team-lead ruling: record as an intentional
// divergence, no Python follow-up (Python only shrinks).
func (h *handlers) transferOwnership(w http.ResponseWriter, r *http.Request) {
	user := policy.UserFrom(r.Context())
	ctx := r.Context()
	orgID, parseErr := uuid.Parse(r.PathValue("org_id"))
	if parseErr != nil {
		policy.WriteInternal(w)
		return
	}
	if !h.ensureOrgAdminAccess(ctx, w, user, orgID) {
		return
	}
	body, outcome, failure, err := pybody.Read(r)
	if !handleBodyOutcome(w, body, outcome, failure, err) {
		return
	}
	var errs pybody.Errors
	object, ok := errs.Object(body)
	var newOwnerRaw string
	if ok {
		newOwnerRaw, _ = errs.RequiredString(object, "new_owner_user_id", 1, 0)
	}
	if len(errs) > 0 {
		policy.WritePyJSON(w, http.StatusUnprocessableEntity, pybody.Detail(errs), nil)
		return
	}
	toUserID, parseErr := uuid.Parse(newOwnerRaw)
	if parseErr != nil {
		policy.WriteDetail(w, http.StatusBadRequest, "Source user is not an owner", nil)
		return
	}

	tx, txErr := h.store.Pool.Begin(ctx)
	if txErr != nil {
		h.logger.ErrorContext(ctx, "admin: transfer ownership begin tx failed", "error", txErr)
		policy.WriteInternal(w)
		return
	}
	defer func() { _ = tx.Rollback(ctx) }()

	fromUserID, err := h.store.currentOwner(ctx, tx, orgID)
	if err == errNotAnOwner {
		policy.WriteDetail(w, http.StatusBadRequest, "Source user is not an owner", nil)
		return
	}
	if err != nil {
		h.logger.ErrorContext(ctx, "admin: resolve current owner failed", "error", err)
		policy.WriteInternal(w)
		return
	}
	if err := h.store.transferOwnership(ctx, tx, orgID, fromUserID, toUserID); err != nil {
		switch err {
		case errNotAnOwner:
			policy.WriteDetail(w, http.StatusBadRequest, "Source user is not an owner", nil)
		case errTargetNotMember:
			policy.WriteDetail(w, http.StatusBadRequest, "Target user is not a member", nil)
		default:
			h.logger.ErrorContext(ctx, "admin: transfer ownership failed", "error", err)
			policy.WriteInternal(w)
		}
		return
	}
	if err := tx.Commit(ctx); err != nil {
		h.logger.ErrorContext(ctx, "admin: transfer ownership commit failed", "error", err)
		policy.WriteInternal(w)
		return
	}
	out := pyjson.NewObject()
	out.Set("success", true)
	policy.WritePyJSON(w, http.StatusOK, out, nil)
}

func randomHex4() string {
	buf := make([]byte, 4)
	if _, err := rand.Read(buf); err != nil {
		// secrets.token_hex is not expected to fail; fall back to a
		// process-unique-enough value rather than block org creation.
		return hex.EncodeToString([]byte(time.Now().Format("15040502")))[:8]
	}
	return hex.EncodeToString(buf)
}

// buildInviteToken / hashInviteToken port invites.py's HMAC-signed,
// SHA256-hashed invite token exactly: token = "<id-hex>.<hmac-sha256(id)>",
// stored as sha256(token). The secret precedence (JWT_SECRET_KEY, else
// SETTINGS_ENCRYPTION_KEY, else a dev fallback) matches _token_secret.
func buildInviteToken(id uuid.UUID) string {
	idHex := hexNoDashes(id)
	return idHex + "." + signInviteTokenID(idHex)
}

func signInviteTokenID(idHex string) string {
	mac := hmac.New(sha256.New, []byte(inviteTokenSecret()))
	mac.Write([]byte(idHex))
	return hex.EncodeToString(mac.Sum(nil))
}

func hashInviteToken(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

func inviteTokenSecret() string {
	if v := os.Getenv("JWT_SECRET_KEY"); v != "" {
		return v
	}
	if v := os.Getenv("SETTINGS_ENCRYPTION_KEY"); v != "" {
		return v
	}
	return "dev-key-not-for-prod"
}

func hexNoDashes(id uuid.UUID) string {
	b := id[:]
	return hex.EncodeToString(b)
}

// sendInviteEmail is invites.py's send_invite_email; failures are logged,
// never surfaced to the caller (matches create_org_invite's bare `except
// Exception`).
func sendInviteEmail(ctx context.Context, logger *slog.Logger, toEmail, orgName, inviterName, token string) error {
	// The email service itself (dev_health_ops.api.services.email) is not
	// yet ported to Go; this records the send attempt so the audit trail
	// and this route's own test coverage stay honest about what actually
	// happens today. See the PR's RISK-NOTES.
	logger.InfoContext(ctx, "admin: invite email send is a no-op pending the Go email service port",
		"to", toEmail, "org", orgName, "inviter", inviterName)
	return nil
}
