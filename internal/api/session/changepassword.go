package session

import (
	"context"
	"net/http"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/api/audit"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/auth/passwordhash"
	"github.com/full-chaos/dev-health-ops/internal/auth/passwordpolicy"
)

// The change-password route has no Python ancestor (CHAOS-9513): its
// conventions are the admin set-password route's (current-password check
// answers 403, policy failures answer 422 with rule names only) and the
// reset route's (token_version bump plus refresh-token revocation).
//
// Every refusal text is a fixed string. No field of the request body is
// ever echoed, logged or audited.

type changePasswordInputKey struct{}

type changePasswordInput struct{ current, next string }

const (
	msgCurrentRejected = "Current password verification failed"
	msgManagedAccount  = "Password is managed by your sign-in provider"
	msgNotAllowed      = "Password change is not allowed for this session"
)

// validateChangePassword is the request model: a current password of 1 to
// 128 characters and a new one of 8 to 128, like set-password's.
func validateChangePassword(w http.ResponseWriter, r *http.Request) (*http.Request, bool) {
	body, _ := policy.BodyFrom(r.Context())
	var errs pybody.Errors
	var input changePasswordInput
	if object, ok := errs.Object(body); ok {
		input.current, _ = errs.RequiredString(object, "current_password", 1, 128)
		input.next, _ = errs.RequiredString(object, "new_password", 8, 128)
	}
	if len(errs) > 0 {
		writeGenericValidation(w)
		return nil, false
	}
	return r.WithContext(context.WithValue(r.Context(), changePasswordInputKey{}, input)), true
}

// writeGenericValidation answers 422 without pydantic's per-field detail:
// that detail carries the submitted value (input) for a bad field.
func writeGenericValidation(w http.ResponseWriter) {
	refuse(w, http.StatusUnprocessableEntity, "Invalid password change request")
}

// changePasswordKey limits per signed-in user: the guard has set the
// principal before the limiter runs.
func changePasswordKey(r *http.Request) string {
	return "change-password:" + policy.UserFrom(r.Context()).UserID
}

// changePassword is POST /api/v1/auth/change-password.
func (h handlers) changePassword(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	caller := policy.UserFrom(ctx)
	input, _ := ctx.Value(changePasswordInputKey{}).(changePasswordInput)

	if policy.ImpersonationFrom(ctx) != nil || caller.ImpersonatedBy != nil {
		h.Logger.WarnContext(ctx, "change-password refused: impersonated session", "user_id", caller.UserID)
		refuse(w, http.StatusForbidden, msgNotAllowed)
		return
	}
	userID, ok := policy.ParsePyUUID(caller.UserID)
	if !ok {
		h.fail(w, r, "change-password sub", nil)
		return
	}

	if violations := passwordpolicy.Validate(input.next); len(violations) > 0 {
		h.Logger.InfoContext(ctx, "change-password refused: policy", "user_id", caller.UserID, "violations", len(violations))
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

	tx, err := h.Pool.Begin(ctx)
	if err != nil {
		h.fail(w, r, "begin", err)
		return
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	// The row lock serializes two concurrent changes of one account: the
	// second one verifies against the first one's new hash.
	if _, err := tx.Exec(ctx, `SELECT 1 FROM users WHERE id = $1::uuid FOR UPDATE`, userID); err != nil {
		h.fail(w, r, "lock user", err)
		return
	}
	user, err := userByID(ctx, tx, userID)
	if err != nil {
		h.fail(w, r, "load user", err)
		return
	}
	if user == nil || !user.IsActive {
		h.Logger.WarnContext(ctx, "change-password refused: user missing or inactive", "user_id", caller.UserID)
		refuse(w, http.StatusForbidden, msgNotAllowed)
		return
	}
	provider := "local"
	if user.AuthProvider != nil {
		provider = *user.AuthProvider
	}
	if provider != "local" || user.PasswordHash == nil {
		h.Logger.WarnContext(ctx, "change-password refused: externally managed account", "user_id", caller.UserID)
		refuse(w, http.StatusForbidden, msgManagedAccount)
		return
	}
	matches, err := passwordMatches(input.current, *user.PasswordHash)
	if err != nil {
		h.fail(w, r, "verify current password", err)
		return
	}
	if !matches {
		h.Logger.WarnContext(ctx, "change-password refused: current password mismatch", "user_id", caller.UserID)
		refuse(w, http.StatusForbidden, msgCurrentRejected)
		return
	}

	hash, err := passwordhash.Hash(input.next)
	if err != nil {
		h.fail(w, r, "hash password", err)
		return
	}
	now := h.Now()
	user.TokenVersion++
	if _, err := tx.Exec(ctx, `UPDATE users SET password_hash = $2, token_version = $3, updated_at = $4 WHERE id = $1::uuid`,
		user.ID, hash, user.TokenVersion, now.UTC()); err != nil {
		h.fail(w, r, "update password", err)
		return
	}
	if err := revokeAllForUser(ctx, tx, user.ID, now); err != nil {
		h.fail(w, r, "revoke refresh tokens", err)
		return
	}

	// The caller's own session continues: it gets a fresh pair minted
	// against the new token_version, in the organization it signed in with.
	membership, err := h.callerMembership(ctx, tx, user.ID, caller.OrgID)
	if err != nil {
		h.fail(w, r, "load membership", err)
		return
	}
	pair, err := h.issueMembershipTokens(ctx, tx, r, user, membership)
	if err != nil {
		h.fail(w, r, "issue tokens", err)
		return
	}
	if err := h.auditUserEvent(ctx, r, tx, user, audit.ActionPasswordChanged, "User changed own password"); err != nil {
		h.fail(w, r, "audit", err)
		return
	}
	if err := tx.Commit(ctx); err != nil {
		h.fail(w, r, "commit", err)
		return
	}
	h.Logger.InfoContext(ctx, "change-password succeeded: other sessions revoked", "user_id", caller.UserID)
	policy.WriteModel(w, http.StatusOK, loginResponse(pair, false, userInfo(user, membership), nil), nil)
}

// callerMembership is the membership of the organization in the caller's
// access token, nil when the token names none or the user is no longer a
// member (the pair then carries no organization, like a login without one).
func (h handlers) callerMembership(ctx context.Context, tx pgx.Tx, userID uuid.UUID, orgText string) (*membershipRow, error) {
	org := parseUUID(&orgText)
	if org == nil {
		return nil, nil
	}
	memberships, err := orderedMemberships(ctx, tx, userID)
	if err != nil {
		return nil, err
	}
	for i := range memberships {
		if memberships[i].OrgID == *org {
			return &memberships[i], nil
		}
	}
	return nil, nil
}
