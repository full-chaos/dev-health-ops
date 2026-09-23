package admin

import (
	"net/http"
	"time"

	"github.com/google/uuid"
	"golang.org/x/crypto/bcrypt"

	"github.com/full-chaos/dev-health-ops/internal/api/audit"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
)

const usersPrefix = "/api/v1/admin"

func (h *handlers) userRoutes() []httpapi.Route {
	return []httpapi.Route{
		{Method: http.MethodGet, Pattern: usersPrefix + "/users", Handler: h.guard.Wrap(policy.Admin, http.HandlerFunc(h.listUsers))},
		{Method: http.MethodGet, Pattern: usersPrefix + "/users/{user_id}", Handler: h.guard.Wrap(policy.Admin, http.HandlerFunc(h.getUser))},
		{Method: http.MethodPost, Pattern: usersPrefix + "/users", Handler: h.bodyFirst(policy.Admin, http.HandlerFunc(h.createUser))},
		{Method: http.MethodPatch, Pattern: usersPrefix + "/users/{user_id}", Handler: h.bodyFirst(policy.Admin, http.HandlerFunc(h.updateUser))},
		{Method: http.MethodPost, Pattern: usersPrefix + "/users/{user_id}/password",
			Handler: h.bodyFirst(policy.Admin, http.HandlerFunc(h.setUserPassword)), RateLimitPerSecond: 1, RateLimitBurst: 10},
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
	policy.WriteJSON(w, http.StatusOK, list, nil)
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
	policy.WriteJSON(w, http.StatusOK, userResponseObject(target), nil)
}

// createUser is users.py's create_user: unauthenticated by design in the
// Python route (no Depends(require_admin) on this one -- account
// self-registration rides this same admin endpoint).
func (h *handlers) createUser(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	body := bodyFromContext(ctx)
	var errs pybody.Errors
	object, ok := errs.Object(body)
	var email, password, username, fullName, authProvider, authProviderID string
	var isVerified, isSuperuser bool
	var fullNamePresent, authProviderPresent, authProviderIDPresent bool
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
	}
	if len(errs) > 0 {
		policy.WriteJSON(w, http.StatusUnprocessableEntity, pybody.Detail(errs), nil)
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
	if password != "" {
		hash, hashErr := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
		if hashErr != nil {
			h.logger.ErrorContext(ctx, "admin: password hash failed", "error", hashErr)
			policy.WriteInternal(w)
			return
		}
		hashed := string(hash)
		in.PasswordHash = &hashed
	}

	created, err := h.store.insertUser(ctx, in)
	switch {
	case err == errEmailExists:
		policy.WriteDetail(w, http.StatusBadRequest, "User with email "+email+" already exists", nil)
		return
	case err == errUsernameExists:
		policy.WriteDetail(w, http.StatusBadRequest, "User with username "+username+" already exists", nil)
		return
	case err != nil:
		h.logger.ErrorContext(ctx, "admin: create user failed", "error", err)
		policy.WriteInternal(w)
		return
	}
	policy.WriteJSON(w, http.StatusCreated, userResponseObject(created), nil)
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

	updated, err := h.store.updateUser(ctx, targetID, patch)
	switch {
	case err == errEmailExists:
		policy.WriteDetail(w, http.StatusBadRequest, "Email "+deref(patch.Email)+" already in use", nil)
		return
	case err == errUsernameExists:
		policy.WriteDetail(w, http.StatusBadRequest, "Username "+deref(patch.Username)+" already in use", nil)
		return
	case err != nil:
		h.logger.ErrorContext(ctx, "admin: update user failed", "error", err)
		policy.WriteInternal(w)
		return
	}
	if updated == nil {
		policy.WriteDetail(w, http.StatusNotFound, "User not found", nil)
		return
	}
	policy.WriteJSON(w, http.StatusOK, userResponseObject(updated), nil)
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

	body := bodyFromContext(ctx)
	var errs pybody.Errors
	object, ok := errs.Object(body)
	var adminPassword, newPassword string
	if ok {
		adminPassword, _ = errs.RequiredString(object, "admin_password", 8, 128)
		newPassword, _ = errs.RequiredString(object, "password", 8, 128)
	}
	if len(errs) > 0 {
		policy.WriteJSON(w, http.StatusUnprocessableEntity, pybody.Detail(errs), nil)
		return
	}

	if violations := validatePassword(newPassword); len(violations) > 0 {
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

	actingID, parseErr := uuid.Parse(user.UserID)
	if parseErr != nil {
		policy.WriteDetail(w, http.StatusForbidden, "Admin password verification failed", nil)
		return
	}
	admin, err := h.store.fullUserByID(ctx, actingID)
	if err != nil {
		h.logger.ErrorContext(ctx, "admin: set password acting-admin lookup failed", "error", err)
		policy.WriteInternal(w)
		return
	}
	if admin == nil || admin.PasswordHash == nil {
		policy.WriteDetail(w, http.StatusForbidden, "Admin password verification failed", nil)
		return
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
		return
	}
	if bcrypt.CompareHashAndPassword([]byte(*admin.PasswordHash), []byte(adminPassword)) != nil {
		policy.WriteDetail(w, http.StatusForbidden, "Admin password verification failed", nil)
		return
	}

	newHash, hashErr := bcrypt.GenerateFromPassword([]byte(newPassword), bcrypt.DefaultCost)
	if hashErr != nil {
		h.logger.ErrorContext(ctx, "admin: password hash failed", "error", hashErr)
		policy.WriteInternal(w)
		return
	}

	auditOrgID, orgErr := parseOptionalOrgID(orgIDRaw)
	if orgErr == nil && auditOrgID == uuid.Nil {
		if fallback, err := h.store.membershipOrgIDForUser(ctx, targetID); err == nil && fallback != nil {
			auditOrgID = *fallback
		}
	}

	// The password UPDATE, the refresh_tokens revocation, and the audit
	// write all go through ONE transaction: Python's set_password and
	// revoke_all_for_user share the request's own SQLAlchemy session with
	// the router's own emit_audit_log call, one implicit commit for all
	// three.
	tx, txErr := h.store.Pool.Begin(ctx)
	if txErr != nil {
		h.logger.ErrorContext(ctx, "admin: set password begin tx failed", "error", txErr)
		policy.WriteInternal(w)
		return
	}
	defer func() { _ = tx.Rollback(ctx) }()

	success, err := h.store.setUserPassword(ctx, tx, targetID, string(newHash))
	if err != nil {
		h.logger.ErrorContext(ctx, "admin: set password failed", "error", err)
		policy.WriteInternal(w)
		return
	}
	if !success {
		policy.WriteDetail(w, http.StatusNotFound, "User not found", nil)
		return
	}
	if err := h.store.revokeAllRefreshTokens(ctx, tx, targetID); err != nil {
		h.logger.ErrorContext(ctx, "admin: revoke refresh tokens failed", "error", err)
		policy.WriteInternal(w)
		return
	}
	if auditOrgID != uuid.Nil {
		description := "Admin changed user password"
		if _, err := h.audit.Write(ctx, tx, requestAuditEntry(r, audit.Entry{
			OrgID: auditOrgID, UserID: &actingID, Action: audit.ActionPasswordChanged,
			ResourceType: audit.ResourceUser, ResourceID: target.ID.String(), Description: &description,
		})); err != nil {
			h.logger.ErrorContext(ctx, "admin: password-change audit write failed", "error", err)
			policy.WriteInternal(w)
			return
		}
	}
	if err := tx.Commit(ctx); err != nil {
		h.logger.ErrorContext(ctx, "admin: set password commit failed", "error", err)
		policy.WriteInternal(w)
		return
	}

	out := pyjson.NewObject()
	out.Set("success", true)
	policy.WriteJSON(w, http.StatusOK, out, nil)
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
	deleted, err := h.store.deleteUser(ctx, targetID)
	if err != nil {
		h.logger.ErrorContext(ctx, "admin: delete user failed", "error", err)
		policy.WriteInternal(w)
		return
	}
	if !deleted {
		policy.WriteDetail(w, http.StatusNotFound, "User not found", nil)
		return
	}
	out := pyjson.NewObject()
	out.Set("deleted", true)
	policy.WriteJSON(w, http.StatusOK, out, nil)
}
