package admin

import (
	"crypto/rand"
	"encoding/hex"
	"net/http"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

const orgsPrefix = "/api/v1/admin"

func (h *handlers) orgRoutes() []httpapi.Route {
	return []httpapi.Route{
		{Method: http.MethodGet, Pattern: orgsPrefix + "/orgs", Handler: h.guard.Wrap(policy.Superuser, http.HandlerFunc(h.listOrganizations))},
		{Method: http.MethodGet, Pattern: orgsPrefix + "/orgs/{org_id}", Handler: h.guard.Wrap(policy.Superuser, http.HandlerFunc(h.getOrganization))},
		{Method: http.MethodPost, Pattern: orgsPrefix + "/orgs", Handler: h.bodyFirst(policy.Superuser, http.HandlerFunc(h.createOrganization))},
		{Method: http.MethodPatch, Pattern: orgsPrefix + "/orgs/{org_id}", Handler: h.bodyFirst(policy.Superuser, http.HandlerFunc(h.updateOrganization))},
		{Method: http.MethodDelete, Pattern: orgsPrefix + "/orgs/{org_id}", Handler: h.guard.Wrap(policy.Superuser, http.HandlerFunc(h.deleteOrganization))},
		{Method: http.MethodGet, Pattern: orgsPrefix + "/orgs/{org_id}/members", Handler: h.guard.Wrap(policy.Admin, http.HandlerFunc(h.listMembers))},
		{Method: http.MethodPost, Pattern: orgsPrefix + "/orgs/{org_id}/members", Handler: h.bodyFirst(policy.Admin, http.HandlerFunc(h.addMember))},
		// create_org_invite: authentication, then body validation, then the
		// rate limit, then the endpoint -- the order FastAPI and slowapi
		// run them in (see validateInviteBody).
		{Method: http.MethodPost, Pattern: orgsPrefix + "/orgs/{org_id}/invites",
			Handler: h.bodyFirst(policy.Admin,
				httpapi.ValidateThenLimit(validateInviteBody, h.inviteLimiter, adminUserKey, h.write)(http.HandlerFunc(h.createOrgInvite)))},
		{Method: http.MethodPatch, Pattern: orgsPrefix + "/orgs/{org_id}/members/{user_id}", Handler: h.bodyFirst(policy.Admin, http.HandlerFunc(h.updateMemberRole))},
		{Method: http.MethodDelete, Pattern: orgsPrefix + "/orgs/{org_id}/members/{user_id}", Handler: h.guard.Wrap(policy.Admin, http.HandlerFunc(h.removeMember))},
		{Method: http.MethodPost, Pattern: orgsPrefix + "/orgs/{org_id}/transfer-ownership", Handler: h.bodyFirst(policy.Admin, http.HandlerFunc(h.transferOwnership))},
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
	out.Set("created_at", pyTimeString(org.CreatedAt))
	out.Set("updated_at", pyTimeString(org.UpdatedAt))
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
		out.Set("joined_at", pyTimeString(*m.JoinedAt))
	} else {
		out.Set("joined_at", nil)
	}
	out.Set("created_at", pyTimeString(m.CreatedAt))
	out.Set("updated_at", pyTimeString(m.UpdatedAt))
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
	policy.WriteModel(w, http.StatusOK, list, nil)
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
	policy.WriteModel(w, http.StatusOK, obj, nil)
}

// createOrganization is orgs.py's create_organization.
func (h *handlers) createOrganization(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	body := bodyFromContext(ctx)
	var errs pybody.Errors
	object, ok := errs.Object(body)
	var name, slug, description, ownerUserID string
	var descriptionPresent bool
	var settingsObj *pyjson.Object
	tier := "community"
	if ok {
		name, _ = errs.RequiredString(object, "name", 1, 100)
		// OrganizationCreate.validate_name: pydantic's own field_validator
		// strips the (already min_length=1-checked) name and rejects an
		// all-whitespace result -- distinct from the min_length check,
		// which only catches a literally empty string. Verified live: the
		// error is "value_error" with ctx {"error": {}} (a ValueError
		// object jsonable_encoder cannot serialize further), input is the
		// UNTRIMMED raw value.
		trimmedName := pythonparity.Strip(name)
		if trimmedName == "" && name != "" {
			errCtx := pyjson.NewObject()
			errCtx.Set("error", pyjson.NewObject())
			errs = append(errs, pybody.Error{Type: "value_error", Loc: []pyjson.Value{"body", "name"},
				Msg: "Value error, Workspace name is required", Input: name, Ctx: errCtx})
		} else {
			name = trimmedName
		}
		slug, _ = errs.OptionalString(object, "slug", 0, 0)
		// description is genuinely Optional (`str | None = None`): absent or
		// explicit null both mean "no description"; a present explicit ""
		// is a REAL value, distinct from absent -- a live round found Go
		// collapsing an explicit "" to null.
		description, descriptionPresent = errs.OptionalString(object, "description", 0, 0)
		// tier is a pydantic DEFAULT (`str = "community"`), not Optional:
		// absent applies the default, a present explicit value (including
		// "") is stored verbatim, and a present null is a type error --
		// same DefaultedString shape auth_provider already uses.
		if value, present := errs.DefaultedString(object, "tier", 0, 0); present {
			tier = value
		}
		ownerUserID, _ = errs.OptionalString(object, "owner_user_id", 0, 0)
		// settings is ALSO a pydantic default (`dict[str, Any] =
		// Field(default_factory=dict)`), never Optional: absent -> {},
		// present non-object (including null) -> dict_type error. A live
		// round found this field silently dropped and always stored as {}.
		if value, present := errs.DefaultedAnyDict(object, "settings"); present {
			settingsObj = value
		}
	}
	if len(errs) > 0 {
		policy.WriteJSON(w, http.StatusUnprocessableEntity, pybody.Detail(errs), nil)
		return
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
	settingsBytes := []byte("{}")
	if settingsObj != nil {
		encoded, err := pyjson.Marshal(settingsObj)
		if err != nil {
			h.logger.ErrorContext(ctx, "admin: encode settings failed", "error", err)
			policy.WriteInternal(w)
			return
		}
		settingsBytes = encoded
	}
	org := &organization{
		ID: uuid.New(), Slug: slug, Name: name, Tier: tier, ManagedBy: managedBy, IsActive: true,
		Settings: settingsBytes,
	}
	if descriptionPresent {
		org.Description = &description
	}

	// OrganizationService.create: the org row and its owner membership (if
	// any) are one SQLAlchemy session, durable only when the router's
	// commit runs -- a failed owner-membership insert must not leave a
	// committed, ownerless organization behind.
	tx, txErr := h.store.Pool.Begin(ctx)
	if txErr != nil {
		h.logger.ErrorContext(ctx, "admin: create organization begin tx failed", "error", txErr)
		policy.WriteInternal(w)
		return
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if err := h.store.insertOrganizationTx(ctx, tx, org); err != nil {
		h.logger.ErrorContext(ctx, "admin: create organization failed", "error", err)
		policy.WriteInternal(w)
		return
	}
	if ownerUserID != "" {
		// OrganizationService.create -> MembershipService.add_member:
		// `uuid.UUID(owner_user_id)` is a bare, un-try/excepted call inside
		// svc.create() itself -- a malformed value raises unhandled all the
		// way out of the route, past the never-reached session.commit(),
		// to FastAPI's generic 500. A live round found Go silently
		// SKIPPING the membership insert instead and still committing the
		// organization.
		ownerID, parseErr := pythonparity.ParseUUID(ownerUserID)
		if parseErr != nil {
			h.logger.ErrorContext(ctx, "admin: create organization owner_user_id invalid", "error", parseErr)
			policy.WriteInternal(w)
			return
		}
		if _, err := h.store.insertMembershipTx(ctx, tx, org.ID, ownerID, "owner", nil); err != nil {
			h.logger.ErrorContext(ctx, "admin: create organization owner membership failed", "error", err)
			policy.WriteInternal(w)
			return
		}
	}
	if err := tx.Commit(ctx); err != nil {
		h.logger.ErrorContext(ctx, "admin: create organization commit failed", "error", err)
		policy.WriteInternal(w)
		return
	}

	obj, err := organizationResponseObject(org)
	if err != nil {
		h.logger.ErrorContext(ctx, "admin: encode organization failed", "error", err)
		policy.WriteInternal(w)
		return
	}
	policy.WriteModel(w, http.StatusCreated, obj, nil)
}

// updateOrganization is orgs.py's update_organization.
func (h *handlers) updateOrganization(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	orgID, parseErr := uuid.Parse(r.PathValue("org_id"))
	if parseErr != nil {
		policy.WriteInternal(w)
		return
	}
	body := bodyFromContext(ctx)
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
		// settings is genuinely Optional here (`dict[str, Any] | None =
		// None`, distinct from create's pydantic-default shape): absent or
		// explicit null both mean "leave it alone"; a present non-object
		// value is a dict_type 422, not silently accepted -- a live round
		// found `{"settings":[]}` stored verbatim as an array.
		if value, present := errs.OptionalAnyDict(object, "settings"); present {
			if encoded, err := pyjson.Marshal(value); err == nil {
				patch.Settings = encoded
			}
		}
	}
	if len(errs) > 0 {
		policy.WriteJSON(w, http.StatusUnprocessableEntity, pybody.Detail(errs), nil)
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
	policy.WriteModel(w, http.StatusOK, obj, nil)
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
	// queryLastValue, not url.Values.Get (which picks the FIRST of a
	// repeated key) -- a live round found `?role=owner&role=member`
	// resolving to the owner row on Go, the member row on Python.
	var role *string
	if raw, present := queryLastValue(r.URL.Query(), "role"); present && raw != "" {
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
	policy.WriteModel(w, http.StatusOK, list, nil)
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
	body := bodyFromContext(ctx)
	var errs pybody.Errors
	object, ok := errs.Object(body)
	var targetUserIDRaw, invitedByRaw string
	role := "member"
	if ok {
		targetUserIDRaw, _ = errs.RequiredString(object, "user_id", 1, 0)
		// role is a pydantic DEFAULT (`str = "member"`), not Optional: absent
		// applies the default, a present explicit "" is stored verbatim (a
		// real value, distinct from the default), and a present null is a
		// string_type error -- a live round found Go collapsing absent AND
		// explicit-empty to the same default, and accepting null instead of
		// 422ing.
		if value, present := errs.DefaultedString(object, "role", 0, 0); present {
			role = value
		}
		invitedByRaw, _ = errs.OptionalString(object, "invited_by_id", 0, 0)
	}
	if len(errs) > 0 {
		policy.WriteJSON(w, http.StatusUnprocessableEntity, pybody.Detail(errs), nil)
		return
	}
	// MembershipService.add_member: `uuid.UUID(user_id)` and (when supplied)
	// `uuid.UUID(invited_by_id)` are the ONLY validation either field gets --
	// no pydantic UUID type, a bare str field -- and any ValueError either
	// raises propagates unhandled to the router's `except ValueError as e:
	// raise HTTPException(400, str(e))`. A live round found invited_by_id's
	// failure silently discarded (the membership created without an
	// inviter) instead of refusing the request.
	targetUserID, userIDErr := pythonparity.ParseUUID(targetUserIDRaw)
	if userIDErr != nil {
		detail, _ := pythonparity.UUIDValidationDetail(targetUserIDRaw)
		policy.WriteDetail(w, http.StatusBadRequest, detail, nil)
		return
	}
	var invitedByID *uuid.UUID
	if invitedByRaw != "" {
		id, err := pythonparity.ParseUUID(invitedByRaw)
		if err != nil {
			detail, _ := pythonparity.UUIDValidationDetail(invitedByRaw)
			policy.WriteDetail(w, http.StatusBadRequest, detail, nil)
			return
		}
		invitedByID = &id
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
	policy.WriteModel(w, http.StatusCreated, membershipResponseObject(created), nil)
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
	body := bodyFromContext(ctx)
	var errs pybody.Errors
	object, ok := errs.Object(body)
	var role string
	if ok {
		role, _ = errs.RequiredString(object, "role", 1, 0)
	}
	if len(errs) > 0 {
		policy.WriteJSON(w, http.StatusUnprocessableEntity, pybody.Detail(errs), nil)
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
	policy.WriteModel(w, http.StatusOK, membershipResponseObject(updated), nil)
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
	policy.WriteModel(w, http.StatusOK, out, nil)
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
	body := bodyFromContext(ctx)
	var errs pybody.Errors
	object, ok := errs.Object(body)
	var newOwnerRaw string
	if ok {
		newOwnerRaw, _ = errs.RequiredString(object, "new_owner_user_id", 1, 0)
	}
	if len(errs) > 0 {
		policy.WriteJSON(w, http.StatusUnprocessableEntity, pybody.Detail(errs), nil)
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
	policy.WriteJSON(w, http.StatusOK, out, nil)
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
