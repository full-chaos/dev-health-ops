package admin

import (
	"context"
	"errors"
	"net/http"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/audit"
	"github.com/full-chaos/dev-health-ops/internal/api/authprovision"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/auth/passwordhash"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// While auth-service owns users (AUTH_USERS_OWNED), the admin users routes keep every check of their own and send
// the write to auth-service's user-admin routes, which write both schemas, the audit and the outbox in one commit.
// go-api holds no transaction or row lock across the call and writes nothing; it answers from its read of
// public.users. Each auth refusal maps to the answer go-api gives when it writes itself; a refusal with no designed
// legacy answer is a named conflict (409), and a dependency failure is the 503 (PLAN-13380-v4 section 2).

// The messages of the owned-path conflicts that have no legacy text (PLAN-13380-v4 Q15).
const (
	usersLastPlatformAdminText = "the user-admin routes refuse removing the last active platform_admin, on demote and on erase"
	usersStateChangedText      = "identity: the user changed while the update waited for its locks"
)

// usersAnswer writes one go-api answer of an auth refusal.
type usersAnswer func(w http.ResponseWriter)

func usersDetail(status int, detail string) usersAnswer {
	return func(w http.ResponseWriter) { policy.WriteDetail(w, status, detail, nil) }
}

// usersConflict is the 409 of a refusal with no legacy twin, in the moved-operation form: the message and the reason.
func usersConflict(reason, message string) usersAnswer {
	return func(w http.ResponseWriter) {
		detail := pyjson.NewObject()
		detail.Set("message", message)
		detail.Set("reason", reason)
		policy.WriteDetail(w, http.StatusConflict, detail, nil)
	}
}

// usersCommonAnswers are the refusals every user-admin route can give: the actor rule auth-service re-runs under its
// locks, and its lock race.
func usersCommonAnswers() map[string]usersAnswer {
	answers := map[string]usersAnswer{
		"principal_deactivated": usersDetail(http.StatusForbidden, "Account is deactivated"),
		"state_changed":         usersConflict("state_changed", usersStateChangedText),
	}
	for reason, answer := range actorAnswers {
		answers[reason] = usersDetail(answer.status, answer.detail)
	}
	return answers
}

// writeUsersRefusal writes the answer of err: answers, else the 503. A 5xx is a dependency failure whatever its code.
func (h *handlers) writeUsersRefusal(ctx context.Context, w http.ResponseWriter, route string, err error, answers map[string]usersAnswer) {
	reason := authprovision.ReasonOf(err)
	var refused *authprovision.Refused
	if errors.As(err, &refused) && refused.Status >= http.StatusInternalServerError {
		reason = ""
	}
	if answer, ok := answers[reason]; ok && reason != "" {
		answer(w)
		return
	}
	h.logger.WarnContext(ctx, "admin: auth-service did not take the users change; nothing was written",
		"route", route, "refused", errors.Is(err, authprovision.ErrRefused), "reason", reason)
	h.writeProvisionUnavailable(w)
}

// writeUserReadBack answers the public row auth-service wrote, or the 503 when go-api cannot read it.
func (h *handlers) writeUserReadBack(ctx context.Context, w http.ResponseWriter, id uuid.UUID, status int) {
	user, err := h.store.fullUserByID(ctx, id)
	if err != nil {
		h.logger.ErrorContext(ctx, "admin: read user after auth change failed", "error", err)
		h.writeProvisionUnavailable(w)
		return
	}
	if user == nil {
		h.writeProvisionUnavailable(w)
		return
	}
	policy.WriteModel(w, status, userResponseObject(user), nil)
}

func optionalOrg(orgID uuid.UUID) *uuid.UUID {
	if orgID == uuid.Nil {
		return nil
	}
	return &orgID
}

// userUpdateDecision is what a patch changes against the stored row, by the rules updateUser writes with: the email
// compares lower-cased (a blank one changes nothing), the username lower-cased (a blank one clears it), the flags by
// value. Both modes use it: not owned, updateUser checks the duplicates it names; owned, the handler forwards the
// change (a real is_active change too, CHAOS-13684).
type userUpdateDecision struct {
	EmailCheck, UsernameCheck bool
	SuperuserChange           bool
}

func decideUserUpdate(stored *fullUser, patch userUpdate) userUpdateDecision {
	var d userUpdateDecision
	d.EmailCheck = patch.Email != nil && *patch.Email != "" && pythonparity.Lower(*patch.Email) != stored.Email
	if patch.Username != nil && *patch.Username != "" {
		current := ""
		if stored.Username != nil {
			current = pythonparity.Lower(*stored.Username)
		}
		d.UsernameCheck = pythonparity.Lower(*patch.Username) != current
	}
	d.SuperuserChange = patch.IsSuperuser != nil && *patch.IsSuperuser != stored.IsSuperuser
	return d
}

// refuseIfTaken runs go-api's own duplicate checks (insertUser, updateUser) before the call, with their answers.
// It returns true when it wrote an answer.
func (h *handlers) refuseIfTaken(ctx context.Context, w http.ResponseWriter, route, emailLookup string, checkEmail bool, username string,
	checkUsername bool, emailText, usernameText string) bool {
	if checkEmail {
		existing, err := h.store.userByEmail(ctx, emailLookup)
		if err != nil {
			h.logger.ErrorContext(ctx, "admin: users duplicate check failed", "route", route, "error", err)
			policy.WriteInternal(w)
			return true
		}
		if existing != nil {
			policy.WriteDetail(w, http.StatusBadRequest, emailText, nil)
			return true
		}
	}
	if checkUsername {
		existing, err := h.store.userByUsername(ctx, username)
		if err != nil {
			h.logger.ErrorContext(ctx, "admin: users duplicate check failed", "route", route, "error", err)
			policy.WriteInternal(w)
			return true
		}
		if existing != nil {
			policy.WriteDetail(w, http.StatusBadRequest, usernameText, nil)
			return true
		}
	}
	return false
}

// createUserInAuth is createUser's write while users are owned: the superuser guard, the password hash and go-api's
// duplicate checks, then the create at auth-service and the read-back.
func (h *handlers) createUserInAuth(ctx context.Context, w http.ResponseWriter, r *http.Request, caller *policy.User, in userCreateInput,
	password string, orgID uuid.UUID, role string) {
	if h.usersAdmin == nil {
		h.writeProvisionUnavailable(w)
		return
	}
	if in.IsSuperuser && !h.store.PlatformRolesOwned {
		writeUsersOwned(w)
		return
	}
	if h.refuseSuperuserWrite(ctx, w, caller, &in.IsSuperuser, nil, "create_user") {
		return
	}
	if password != "" {
		hashed, err := passwordhash.Hash(password)
		if err != nil {
			h.logger.ErrorContext(ctx, "admin: password hash failed", "error", err)
			policy.WriteInternal(w)
			return
		}
		in.PasswordHash = &hashed
	}
	email, username := deref(in.Email), deref(in.Username)
	if h.refuseIfTaken(ctx, w, "create_user", pythonparity.Strip(pythonparity.Lower(email)), in.Email != nil, username, username != "",
		"User with email "+email+" already exists", "User with username "+username+" already exists") {
		return
	}
	request := authprovision.UserCreateRequest{Actor: caller.ID, Email: email, PasswordHash: in.PasswordHash, Username: in.Username,
		FullName: in.FullName, AuthProvider: in.AuthProvider, AuthProviderID: in.AuthProviderID, IsVerified: in.IsVerified,
		GrantPlatformAdmin: in.IsSuperuser, OrgID: optionalOrg(orgID)}
	if orgID != uuid.Nil {
		request.Role = role
		request.RequestMetadata = requestAuditEntry(r, audit.Entry{}).RequestMetadata
	}
	created, err := h.usersAdmin.CreateUser(ctx, request)
	if err != nil {
		answers := usersCommonAnswers()
		answers["provider_id_invalid"] = func(w http.ResponseWriter) { writeInvalidProviderID(w) }
		answers["email_taken"] = usersConflict("email_taken", "User with email "+email+" already exists")
		answers["username_taken"] = usersConflict("username_taken", "User with username "+username+" already exists")
		answers["organization_not_found"] = usersDetail(http.StatusNotFound, "Organization not found")
		h.writeUsersRefusal(ctx, w, "create_user", err, answers)
		return
	}
	h.writeUserReadBack(ctx, w, created.ID, http.StatusCreated)
}

// writeInvalidProviderID is createUser's 422 for a provider id that is not valid.
func writeInvalidProviderID(w http.ResponseWriter) {
	invalid := pybody.Errors{{Type: "value_error", Loc: []pyjson.Value{"body", "auth_provider_id"},
		Msg: "Value error, auth_provider_id must be a valid provider id when auth_provider is not local"}}
	policy.WriteJSON(w, http.StatusUnprocessableEntity, pybody.Detail(invalid), nil)
}

// updateUserInAuth is updateUser's write while users are owned: a real is_superuser change stays the moved operation
// while the platform roles are not owned; then the superuser guard on the stored row and go-api's duplicate checks,
// the update at auth-service and the read-back. A real is_active change is forwarded as legacy served it (pm D7015):
// auth-service deactivates with the soft delete's writes and reactivates by clearing them, in one commit.
func (h *handlers) updateUserInAuth(ctx context.Context, w http.ResponseWriter, user *policy.User, orgID uuid.UUID, existing *fullUser, patch userUpdate) {
	decided := decideUserUpdate(existing, patch)
	if decided.SuperuserChange && !h.store.PlatformRolesOwned {
		writeUsersOwned(w)
		return
	}
	if h.usersAdmin == nil {
		h.writeProvisionUnavailable(w)
		return
	}
	stored := existing.IsSuperuser
	if h.refuseSuperuserWrite(ctx, w, user, patch.IsSuperuser, func() (bool, error) { return stored, nil }, "update_user") {
		return
	}
	email, username := deref(patch.Email), deref(patch.Username)
	if h.refuseIfTaken(ctx, w, "update_user", email, decided.EmailCheck, username, decided.UsernameCheck,
		"Email "+email+" already in use", "Username "+username+" already in use") {
		return
	}
	request := authprovision.UserUpdateRequest{Actor: user.ID, Principal: existing.ID, OrgID: optionalOrg(orgID), Email: patch.Email,
		Username: patch.Username, FullName: patch.FullName, AvatarURL: patch.AvatarURL, IsVerified: patch.IsVerified}
	// Only a real is_active change is sent: the web edit forms send is_active on every save, and an auth-service that
	// does not take the member yet refuses it as malformed (the 503, nothing written), so an unchanged one must not
	// reach it.
	if patch.IsActive != nil && *patch.IsActive != existing.IsActive {
		request.IsActive = patch.IsActive
	}
	if h.store.PlatformRolesOwned {
		request.IsSuperuser = patch.IsSuperuser
	}
	if err := h.usersAdmin.UpdateUser(ctx, request); err != nil {
		answers := usersCommonAnswers()
		answers[principalNotFound] = usersDetail(http.StatusNotFound, "User not found")
		answers["email_taken"] = usersConflict("email_taken", "Email "+email+" already in use")
		answers["username_taken"] = usersConflict("username_taken", "Username "+username+" already in use")
		answers["last_platform_admin"] = usersConflict("last_platform_admin", usersLastPlatformAdminText)
		h.writeUsersRefusal(ctx, w, "update_user", err, answers)
		return
	}
	updated, err := h.store.fullUserByID(ctx, existing.ID)
	if err != nil {
		h.logger.ErrorContext(ctx, "admin: read user after auth change failed", "error", err)
		h.writeProvisionUnavailable(w)
		return
	}
	if updated == nil {
		policy.WriteDetail(w, http.StatusNotFound, "User not found", nil)
		return
	}
	policy.WriteModel(w, http.StatusOK, userResponseObject(updated), nil)
}

// setUserPasswordInAuth is setUserPassword's write while users are owned. auth-service checks the acting admin's
// password against auth.users (Q-P); go-api keeps its own answers that come before that check: an acting id that does
// not parse, and a password over bcrypt's 72 bytes (the 500 of legacy's unhandled bcrypt error).
func (h *handlers) setUserPasswordInAuth(ctx context.Context, w http.ResponseWriter, r *http.Request, user *policy.User, target *fullUser,
	orgID uuid.UUID, adminPassword, newPassword string) {
	if h.usersAdmin == nil {
		h.writeProvisionUnavailable(w)
		return
	}
	stored := target.IsSuperuser
	if h.refuseSuperuserWrite(ctx, w, user, nil, func() (bool, error) { return stored, nil }, "set_user_password") {
		return
	}
	actingID, err := uuid.Parse(user.UserID)
	if err != nil {
		policy.WriteDetail(w, http.StatusForbidden, "Admin password verification failed", nil)
		return
	}
	if len(adminPassword) > 72 {
		h.logger.ErrorContext(ctx, "admin: set password acting-admin password exceeds bcrypt's 72-byte limit")
		policy.WriteInternal(w)
		return
	}
	newHash, err := passwordhash.Hash(newPassword)
	if err != nil {
		h.logger.ErrorContext(ctx, "admin: password hash failed", "error", err)
		policy.WriteInternal(w)
		return
	}
	err = h.usersAdmin.SetUserPassword(ctx, authprovision.UserPasswordRequest{Actor: actingID, Principal: target.ID, OrgID: optionalOrg(orgID),
		AdminPassword: adminPassword, PasswordHash: newHash, RequestMetadata: requestAuditEntry(r, audit.Entry{}).RequestMetadata})
	if err != nil {
		answers := usersCommonAnswers()
		answers[principalNotFound] = usersDetail(http.StatusNotFound, "User not found")
		answers["admin_password_invalid"] = usersDetail(http.StatusForbidden, "Admin password verification failed")
		h.writeUsersRefusal(ctx, w, "set_user_password", err, answers)
		return
	}
	out := pyjson.NewObject()
	out.Set("success", true)
	policy.WriteModel(w, http.StatusOK, out, nil)
}

// deleteUserInAuth is deleteUser's soft delete while users are owned; a user that was already inactive is the 404 a
// repeat gets.
func (h *handlers) deleteUserInAuth(ctx context.Context, w http.ResponseWriter, user *policy.User, orgID uuid.UUID, existing *fullUser) {
	if h.usersAdmin == nil {
		h.writeProvisionUnavailable(w)
		return
	}
	stored := existing.IsSuperuser
	if h.refuseSuperuserWrite(ctx, w, user, nil, func() (bool, error) { return stored, nil }, "delete_user") {
		return
	}
	deleted, err := h.usersAdmin.DeleteUser(ctx, authprovision.UserDeleteRequest{Actor: user.ID, Principal: existing.ID, OrgID: optionalOrg(orgID),
		Mode: string(DeleteSoft)})
	if err != nil {
		answers := usersCommonAnswers()
		answers[principalNotFound] = usersDetail(http.StatusNotFound, "User not found")
		answers["last_platform_admin"] = usersConflict("last_platform_admin", usersLastPlatformAdminText)
		h.writeUsersRefusal(ctx, w, "delete_user", err, answers)
		return
	}
	if deleted.AlreadyDone {
		policy.WriteDetail(w, http.StatusNotFound, "User not found", nil)
		return
	}
	out := pyjson.NewObject()
	out.Set("deleted", true)
	policy.WriteModel(w, http.StatusOK, out, nil)
}
