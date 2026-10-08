package admin

import (
	"context"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"golang.org/x/crypto/bcrypt"

	"github.com/full-chaos/dev-health-ops/internal/api/audit"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/passwordhash"
	"github.com/full-chaos/dev-health-ops/internal/auth/passwordpolicy"
)

const usersPrefix = "/api/v1/admin"

// adminUserKey is the Go equivalent of rate_limit.py's get_admin_user_key:
// the CALLING admin's own identity, not the target of the request. It runs
// only where a Guard has already authenticated the caller (policy.UserFrom
// is populated), which every route this key_func is applied to already
// requires -- unlike Python's version, it never falls back to an IP key,
// because httpapi.LimitWith is wired inside the authenticated part of
// the chain and so never sees an unauthenticated request at all.
func adminUserKey(r *http.Request) string {
	user := policy.UserFrom(r.Context())
	if user == nil {
		return ""
	}
	return "admin-user:" + user.ID.String()
}

// setPasswordInput is UserSetPassword after validation.
type setPasswordInput struct{ AdminPassword, Password string }

type setPasswordInputKey struct{}

// validateSetPasswordBody is UserSetPassword's pydantic validation (both
// fields 8 to 128 characters), run BEFORE the keyed rate limiter as FastAPI
// does: a malformed body is a 422 that costs no allowance (CHAOS-6435).
func validateSetPasswordBody(w http.ResponseWriter, r *http.Request) (*http.Request, bool) {
	var errs pybody.Errors
	object, ok := errs.Object(bodyFromContext(r.Context()))
	var input setPasswordInput
	if ok {
		input.AdminPassword, _ = errs.RequiredString(object, "admin_password", 8, 128)
		input.Password, _ = errs.RequiredString(object, "password", 8, 128)
	}
	if len(errs) > 0 {
		policy.WriteJSON(w, http.StatusUnprocessableEntity, pybody.Detail(errs), nil)
		return nil, false
	}
	return r.WithContext(context.WithValue(r.Context(), setPasswordInputKey{}, input)), true
}

func (h *handlers) userRoutes() []httpapi.Route {
	return []httpapi.Route{
		{Method: http.MethodGet, Pattern: usersPrefix + "/users", Handler: h.guard.Wrap(policy.Admin, http.HandlerFunc(h.listUsers))},
		{Method: http.MethodGet, Pattern: usersPrefix + "/users/{user_id}", Handler: h.guard.Wrap(policy.Admin, http.HandlerFunc(h.getUser))},
		{Method: http.MethodPost, Pattern: usersPrefix + "/users", Handler: h.bodyFirst(policy.Admin, http.HandlerFunc(h.createUser))},
		{Method: http.MethodPatch, Pattern: usersPrefix + "/users/{user_id}", Handler: h.bodyFirst(policy.Admin, http.HandlerFunc(h.updateUser))},
		{Method: http.MethodPost, Pattern: usersPrefix + "/users/{user_id}/password",
			Handler: h.bodyFirst(policy.Admin,
				httpapi.ValidateThenLimit(validateSetPasswordBody, h.passwordLimiter, adminUserKey, h.write)(http.HandlerFunc(h.setUserPassword)))},
		{Method: http.MethodDelete, Pattern: usersPrefix + "/users/{user_id}", Handler: h.guard.Wrap(policy.Admin, http.HandlerFunc(h.deleteUser))},
	}
}

func userResponseObject(u *fullUser) *pyjson.Object {
	out := pyjson.NewObject()
	out.Set("id", u.ID.String())
	out.Set("email", u.Email)
	out.Set("username", optionalString(u.Username))
	out.Set("full_name", optionalString(u.FullName))
	out.Set("avatar_url", optionalString(u.AvatarURL))
	out.Set("auth_provider", optionalString(u.AuthProvider))
	out.Set("is_active", u.IsActive)
	out.Set("is_verified", u.IsVerified)
	out.Set("is_superuser", u.IsSuperuser)
	out.Set("last_login_at", optionalTime(u.LastLoginAt))
	out.Set("created_at", pyTimeString(u.CreatedAt))
	out.Set("updated_at", pyTimeString(u.UpdatedAt))
	return out
}

func optionalString(s *string) pyjson.Value {
	if s == nil {
		return nil
	}
	return *s
}

func optionalTime(t *time.Time) pyjson.Value {
	if t == nil {
		return nil
	}
	return pyTimeString(*t)
}

// listUsers is users.py's list_users: X-Org-Id header present -> org-scoped;
// absent + superuser -> global; absent + non-superuser -> the JWT org_id.
func (h *handlers) listUsers(w http.ResponseWriter, r *http.Request) {
	user := policy.UserFrom(r.Context())
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
	search, qerr := querySearch(query, "q")
	if qerr != nil {
		writeQueryError(w, qerr)
		return
	}
	filter := listUsersFilter{Limit: limit, Offset: offset, ActiveOnly: activeOnly, Search: search}

	xOrgID := r.Header.Get("X-Org-Id")
	var (
		users []*fullUser
		err   error
	)
	switch {
	case xOrgID != "":
		orgID, parseErr := uuid.Parse(xOrgID)
		if parseErr != nil {
			policy.WriteInternal(w)
			return
		}
		users, err = h.store.listUsersByOrg(ctx, orgID, filter)
	case user.IsSuperuser:
		users, err = h.store.listAllUsers(ctx, filter)
	default:
		orgIDRaw, ok := orgIDForNonSuperuser(w, user)
		if !ok {
			return
		}
		orgID, parseErr := uuid.Parse(orgIDRaw)
		if parseErr != nil {
			policy.WriteInternal(w)
			return
		}
		users, err = h.store.listUsersByOrg(ctx, orgID, filter)
	}
	if err != nil {
		h.logger.ErrorContext(ctx, "admin: list users failed", "error", err)
		policy.WriteInternal(w)
		return
	}
	list := make([]pyjson.Value, len(users))
	for i, u := range users {
		list[i] = userResponseObject(u)
	}
	policy.WriteModel(w, http.StatusOK, list, nil)
}

// getUser is users.py's get_user.
func (h *handlers) getUser(w http.ResponseWriter, r *http.Request) {
	user := policy.UserFrom(r.Context())
	ctx := r.Context()
	orgIDRaw, ok := orgIDForNonSuperuser(w, user)
	if !ok {
		return
	}
	targetID, err := uuid.Parse(r.PathValue("user_id"))
	if err != nil {
		policy.WriteInternal(w)
		return
	}
	target, err := h.store.fullUserByID(ctx, targetID)
	if err != nil {
		h.logger.ErrorContext(ctx, "admin: get user failed", "error", err)
		policy.WriteInternal(w)
		return
	}
	if target == nil {
		policy.WriteDetail(w, http.StatusNotFound, "User not found", nil)
		return
	}
	orgID, orgErr := parseOptionalOrgID(orgIDRaw)
	if orgErr != nil {
		policy.WriteInternal(w)
		return
	}
	if !h.ensureUserInScope(ctx, w, user, orgID, targetID) {
		return
	}
	policy.WriteModel(w, http.StatusOK, userResponseObject(target), nil)
}

// createUserRoles are the roles an add may give: "owner" moves only through
// transfer-ownership.
var createUserRoles = map[string]bool{"admin": true, "member": true, "viewer": true}

// createUser is users.py's create_user plus the org membership the Python
// route never wrote (CHAOS-8969): an org-scoped create (X-Org-Id, else a
// non-superuser's own org claim) writes the user and its membership in one
// transaction, because the org Users list reads users JOIN memberships. A
// superuser with no org scope creates a platform user with no membership.
func (h *handlers) createUser(w http.ResponseWriter, r *http.Request) {
	caller := policy.UserFrom(r.Context())
	ctx := r.Context()
	body := bodyFromContext(ctx)
	var errs pybody.Errors
	object, ok := errs.Object(body)
	var email, password, username, fullName, authProvider, authProviderID string
	var isVerified, isSuperuser bool
	var fullNamePresent, authProviderPresent, authProviderIDPresent bool
	role, rolePresent := "member", false
	if ok {
		email, _ = errs.RequiredString(object, "email", 1, 0)
		password, _ = errs.OptionalString(object, "password", 8, 128)
		username, _ = errs.OptionalString(object, "username", 0, 0)
		fullName, fullNamePresent = errs.OptionalString(object, "full_name", 0, 0)
		// auth_provider/is_verified/is_superuser are pydantic fields with a
		// DEFAULT, not `| None` -- DefaultedString/DefaultedBool: present is
		// false only when the key is absent (apply the same default Python
		// does), but an explicit null is a type error there, never a silent
		// fall-back to the default (see their doc comments).
		authProvider, authProviderPresent = errs.DefaultedString(object, "auth_provider", 0, 0)
		authProviderID, authProviderIDPresent = errs.OptionalString(object, "auth_provider_id", 0, 0)
		isVerified, _ = errs.DefaultedBool(object, "is_verified")
		isSuperuser, _ = errs.DefaultedBool(object, "is_superuser")
		var roleValue string
		if roleValue, rolePresent = errs.OptionalString(object, "role", 0, 0); rolePresent {
			role = roleValue
		}
	}
	if len(errs) > 0 {
		policy.WriteJSON(w, http.StatusUnprocessableEntity, pybody.Detail(errs), nil)
		return
	}

	orgID, scoped := h.createUserOrg(ctx, w, r, caller)
	if !scoped {
		return
	}
	if orgID == uuid.Nil && rolePresent {
		policy.WriteDetail(w, http.StatusBadRequest, "A role needs an organization: send X-Org-Id", nil)
		return
	}
	if orgID != uuid.Nil && !createUserRoles[role] {
		policy.WriteDetail(w, http.StatusBadRequest, "Invalid role: "+role+" (use admin, member or viewer)", nil)
		return
	}

	in := userCreateInput{Email: &email, IsVerified: isVerified, IsSuperuser: isSuperuser}
	if username != "" {
		in.Username = &username
	}
	// full_name/auth_provider_id are gated on PRESENCE, not on the
	// validated value being non-empty: Python's User() constructor stores
	// an explicitly-empty string verbatim (full_name=full_name,
	// auth_provider_id=auth_provider_id -- no truthy check, unlike
	// username, which the service layer DOES truthy-check; see
	// insertUser's own comment). Gating on the string's own emptiness here
	// silently turned an explicit "" into a stored NULL, diverging from
	// Python's verbatim empty string.
	if fullNamePresent {
		in.FullName = &fullName
	}
	if authProviderPresent {
		in.AuthProvider = &authProvider
	}
	if authProviderIDPresent {
		in.AuthProviderID = &authProviderID
	}
	h.superuserGuardedWrite(ctx, w, policy.UserFrom(ctx), &isSuperuser, "create_user", func(g *superuserTx) func() {
		if g.refuse() {
			return nil
		}
		if password != "" {
			hashed, hashErr := passwordhash.Hash(password)
			if hashErr != nil {
				h.logger.ErrorContext(ctx, "admin: password hash failed", "error", hashErr)
				policy.WriteInternal(w)
				return nil
			}
			in.PasswordHash = &hashed
		}
		created, err := h.insertUserWithMembership(ctx, r, g.tx, g.store, in, orgID, role, caller.ID)
		switch {
		case err == errEmailExists:
			policy.WriteDetail(w, http.StatusBadRequest, "User with email "+email+" already exists", nil)
			return nil
		case err == errUsernameExists:
			policy.WriteDetail(w, http.StatusBadRequest, "User with username "+username+" already exists", nil)
			return nil
		case err != nil:
			h.logger.ErrorContext(ctx, "admin: create user failed", "error", err)
			policy.WriteInternal(w)
			return nil
		}
		return func() { policy.WriteModel(w, http.StatusCreated, userResponseObject(created), nil) }
	})
}

// createUserOrg is the organization an add joins the new user to: X-Org-Id
// when sent, else a non-superuser's org claim; uuid.Nil is a superuser's
// platform create. ok is false when a response was already written.
func (h *handlers) createUserOrg(ctx context.Context, w http.ResponseWriter, r *http.Request, caller *policy.User) (uuid.UUID, bool) {
	raw := r.Header.Get("X-Org-Id")
	if raw == "" {
		if caller.IsSuperuser {
			return uuid.Nil, true
		}
		claim, ok := orgIDForNonSuperuser(w, caller)
		if !ok {
			return uuid.Nil, false
		}
		raw = claim
	}
	orgID, err := uuid.Parse(raw)
	if err != nil {
		policy.WriteDetail(w, http.StatusBadRequest, "Invalid organization id", nil)
		return uuid.Nil, false
	}
	if !h.ensureOrgAdminAccess(ctx, w, caller, orgID) {
		return uuid.Nil, false
	}
	return orgID, true
}

// insertUserWithMembership writes, in the caller's transaction, the user and,
// when orgID is set, its membership and the admin audit row: the transaction
// commits all of them or none. A platform create (orgID nil) writes no audit
// row: audit_logs.org_id is NOT NULL.
func (h *handlers) insertUserWithMembership(ctx context.Context, r *http.Request, tx pgx.Tx, store pgStore, in userCreateInput, orgID uuid.UUID, role string, actor uuid.UUID) (*fullUser, error) {
	created, err := store.insertUser(ctx, in)
	if err != nil || orgID == uuid.Nil {
		return created, err
	}
	if _, err := store.insertMembershipTx(ctx, tx, orgID, created.ID, role, &actor); err != nil {
		return nil, err
	}
	changes := pyjson.NewObject()
	changes.Set("email", created.Email)
	changes.Set("role", role)
	encodedChanges, err := pyjson.Marshal(changes)
	if err != nil {
		return nil, err
	}
	description := "User created and added to organization"
	if _, err := h.audit.Write(ctx, tx, requestAuditEntry(r, audit.Entry{
		OrgID: orgID, UserID: &actor, Action: audit.ActionCreate,
		ResourceType: audit.ResourceUser, ResourceID: created.ID.String(),
		Description: &description, Changes: encodedChanges,
	})); err != nil {
		return nil, err
	}
	return created, nil
}

// updateUser is users.py's update_user.
func (h *handlers) updateUser(w http.ResponseWriter, r *http.Request) {
	user := policy.UserFrom(r.Context())
	ctx := r.Context()
	orgIDRaw, ok := orgIDForNonSuperuser(w, user)
	if !ok {
		return
	}
	targetID, parseErr := uuid.Parse(r.PathValue("user_id"))
	if parseErr != nil {
		policy.WriteInternal(w)
		return
	}
	existing, err := h.store.fullUserByID(ctx, targetID)
	if err != nil {
		h.logger.ErrorContext(ctx, "admin: update user lookup failed", "error", err)
		policy.WriteInternal(w)
		return
	}
	if existing == nil {
		policy.WriteDetail(w, http.StatusNotFound, "User not found", nil)
		return
	}
	orgID, orgErr := parseOptionalOrgID(orgIDRaw)
	if orgErr != nil {
		policy.WriteInternal(w)
		return
	}
	if !h.ensureUserInScope(ctx, w, user, orgID, targetID) {
		return
	}

	body := bodyFromContext(ctx)
	var errs pybody.Errors
	object, ok := errs.Object(body)
	patch := userUpdate{}
	if ok {
		if v, present := errs.OptionalString(object, "email", 0, 0); present {
			patch.Email = &v
		}
		if v, present := errs.OptionalString(object, "username", 0, 0); present {
			patch.Username = &v
		}
		if v, present := errs.OptionalString(object, "full_name", 0, 0); present {
			patch.FullName = &v
		}
		if v, present := errs.OptionalString(object, "avatar_url", 0, 0); present {
			patch.AvatarURL = &v
		}
		if v, present := errs.OptionalBool(object, "is_active"); present {
			patch.IsActive = &v
		}
		if v, present := errs.OptionalBool(object, "is_verified"); present {
			patch.IsVerified = &v
		}
		if v, present := errs.OptionalBool(object, "is_superuser"); present {
			patch.IsSuperuser = &v
		}
	}
	if len(errs) > 0 {
		policy.WriteJSON(w, http.StatusUnprocessableEntity, pybody.Detail(errs), nil)
		return
	}
	h.superuserGuardedWrite(ctx, w, user, patch.IsSuperuser, "update_user", func(g *superuserTx) func() {
		if g.refuse(targetID) {
			return nil
		}
		updated, err := g.store.updateUser(ctx, targetID, patch)
		switch {
		case err == errEmailExists:
			policy.WriteDetail(w, http.StatusBadRequest, "Email "+deref(patch.Email)+" already in use", nil)
			return nil
		case err == errUsernameExists:
			policy.WriteDetail(w, http.StatusBadRequest, "Username "+deref(patch.Username)+" already in use", nil)
			return nil
		case err != nil:
			h.logger.ErrorContext(ctx, "admin: update user failed", "error", err)
			policy.WriteInternal(w)
			return nil
		}
		if updated == nil {
			policy.WriteDetail(w, http.StatusNotFound, "User not found", nil)
			return nil
		}
		return func() { policy.WriteModel(w, http.StatusOK, userResponseObject(updated), nil) }
	})
}

// setUserPassword is users.py's set_user_password: rate-limited
// (ADMIN_PASSWORD_LIMIT), requires the ACTING admin's own password, and
// audits the change.
func (h *handlers) setUserPassword(w http.ResponseWriter, r *http.Request) {
	user := policy.UserFrom(r.Context())
	ctx := r.Context()
	orgIDRaw, ok := orgIDForNonSuperuser(w, user)
	if !ok {
		return
	}
	targetID, parseErr := uuid.Parse(r.PathValue("user_id"))
	if parseErr != nil {
		policy.WriteInternal(w)
		return
	}

	// UserSetPassword's field constraints were validated by
	// validateSetPasswordBody before the rate limiter ran.
	input, _ := ctx.Value(setPasswordInputKey{}).(setPasswordInput)
	adminPassword, newPassword := input.AdminPassword, input.Password

	if violations := passwordpolicy.Validate(newPassword); len(violations) > 0 {
		detail := pyjson.NewObject()
		list := make([]pyjson.Value, len(violations))
		for i, v := range violations {
			list[i] = v
		}
		detail.Set("violations", list)
		out := pyjson.NewObject()
		out.Set("detail", detail)
		policy.WriteJSON(w, http.StatusUnprocessableEntity, out, nil)
		return
	}

	target, err := h.store.fullUserByID(ctx, targetID)
	if err != nil {
		h.logger.ErrorContext(ctx, "admin: set password lookup failed", "error", err)
		policy.WriteInternal(w)
		return
	}
	if target == nil {
		policy.WriteDetail(w, http.StatusNotFound, "User not found", nil)
		return
	}
	orgID, orgErr := parseOptionalOrgID(orgIDRaw)
	if orgErr != nil {
		policy.WriteInternal(w)
		return
	}
	if !h.ensureUserInScope(ctx, w, user, orgID, targetID) {
		return
	}
	// The guard, the password UPDATE, the refresh_tokens revocation, and the
	// audit write all go through ONE transaction: Python's set_password and
	// revoke_all_for_user share the request's own SQLAlchemy session with
	// the router's own emit_audit_log call, one implicit commit for all
	// three.
	h.superuserGuardedWrite(ctx, w, user, nil, "set_user_password", func(g *superuserTx) func() {
		if g.refuse(targetID) {
			return nil
		}

		actingID, parseErr := uuid.Parse(user.UserID)
		if parseErr != nil {
			policy.WriteDetail(w, http.StatusForbidden, "Admin password verification failed", nil)
			return nil
		}
		admin, err := g.store.fullUserByID(ctx, actingID)
		if err != nil {
			h.logger.ErrorContext(ctx, "admin: set password acting-admin lookup failed", "error", err)
			policy.WriteInternal(w)
			return nil
		}
		if admin == nil || admin.PasswordHash == nil {
			policy.WriteDetail(w, http.StatusForbidden, "Admin password verification failed", nil)
			return nil
		}
		if len(adminPassword) > 72 {
			// Python's route calls bcrypt.checkpw directly (not the
			// try/except-wrapped _verify_password helper), so a plaintext
			// longer than 72 bytes RAISES ValueError there -- unhandled, the
			// generic 500, never "verification failed" (verified live:
			// bcrypt.checkpw(b"a"*73, hash) raises "password cannot be longer
			// than 72 bytes"). Go's bcrypt.CompareHashAndPassword has no such
			// check (blowfish's own key expansion silently processes the
			// extra bytes and the compare just falls through to an ordinary
			// mismatch), so this route checks the length itself to match.
			h.logger.ErrorContext(ctx, "admin: set password acting-admin password exceeds bcrypt's 72-byte limit")
			policy.WriteInternal(w)
			return nil
		}
		if bcrypt.CompareHashAndPassword([]byte(*admin.PasswordHash), []byte(adminPassword)) != nil {
			policy.WriteDetail(w, http.StatusForbidden, "Admin password verification failed", nil)
			return nil
		}

		newHash, hashErr := passwordhash.Hash(newPassword)
		if hashErr != nil {
			h.logger.ErrorContext(ctx, "admin: password hash failed", "error", hashErr)
			policy.WriteInternal(w)
			return nil
		}

		auditOrgID, orgErr := parseOptionalOrgID(orgIDRaw)
		if orgErr == nil && auditOrgID == uuid.Nil {
			if fallback, err := g.store.membershipOrgIDForUser(ctx, targetID); err == nil && fallback != nil {
				auditOrgID = *fallback
			}
		}

		success, err := h.store.setUserPassword(ctx, g.tx, targetID, newHash)
		if err != nil {
			h.logger.ErrorContext(ctx, "admin: set password failed", "error", err)
			policy.WriteInternal(w)
			return nil
		}
		if !success {
			policy.WriteDetail(w, http.StatusNotFound, "User not found", nil)
			return nil
		}
		if err := h.store.revokeAllRefreshTokens(ctx, g.tx, targetID); err != nil {
			h.logger.ErrorContext(ctx, "admin: revoke refresh tokens failed", "error", err)
			policy.WriteInternal(w)
			return nil
		}
		if auditOrgID != uuid.Nil {
			description := "Admin changed user password"
			if _, err := h.audit.Write(ctx, g.tx, requestAuditEntry(r, audit.Entry{
				OrgID: auditOrgID, UserID: &actingID, Action: audit.ActionPasswordChanged,
				ResourceType: audit.ResourceUser, ResourceID: target.ID.String(), Description: &description,
			})); err != nil {
				h.logger.ErrorContext(ctx, "admin: password-change audit write failed", "error", err)
				policy.WriteInternal(w)
				return nil
			}
		}
		return func() {
			out := pyjson.NewObject()
			out.Set("success", true)
			policy.WriteModel(w, http.StatusOK, out, nil)
		}
	})
}

// deleteUser is users.py's delete_user.
func (h *handlers) deleteUser(w http.ResponseWriter, r *http.Request) {
	user := policy.UserFrom(r.Context())
	ctx := r.Context()
	orgIDRaw, ok := orgIDForNonSuperuser(w, user)
	if !ok {
		return
	}
	targetID, parseErr := uuid.Parse(r.PathValue("user_id"))
	if parseErr != nil {
		policy.WriteInternal(w)
		return
	}
	existing, err := h.store.fullUserByID(ctx, targetID)
	if err != nil {
		h.logger.ErrorContext(ctx, "admin: delete user lookup failed", "error", err)
		policy.WriteInternal(w)
		return
	}
	if existing == nil {
		policy.WriteDetail(w, http.StatusNotFound, "User not found", nil)
		return
	}
	orgID, orgErr := parseOptionalOrgID(orgIDRaw)
	if orgErr != nil {
		policy.WriteInternal(w)
		return
	}
	if !h.ensureUserInScope(ctx, w, user, orgID, targetID) {
		return
	}
	h.superuserGuardedWrite(ctx, w, user, nil, "delete_user", func(g *superuserTx) func() {
		if g.refuse(targetID) {
			return nil
		}
		deleted, err := g.store.deleteUser(ctx, targetID)
		if err != nil {
			h.logger.ErrorContext(ctx, "admin: delete user failed", "error", err)
			policy.WriteInternal(w)
			return nil
		}
		if !deleted {
			policy.WriteDetail(w, http.StatusNotFound, "User not found", nil)
			return nil
		}
		return func() {
			out := pyjson.NewObject()
			out.Set("deleted", true)
			policy.WriteModel(w, http.StatusOK, out, nil)
		}
	})
}
