package session

import (
	"context"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/api/audit"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// resetTTL is create_password_reset_token's ttl_hours=1.
const resetTTL = time.Hour

type (
	verifyInputKey    struct{}
	emailInputKey     struct{}
	resetInputKey     struct{}
	resetPasswordBody struct{ token, password string }
)

// verifyEmailResponse is common.VerifyEmailResponse.
func verifyEmailResponse(message string, verified bool) *pyjson.Object {
	out := pyjson.NewObject()
	out.Set("message", message)
	if verified {
		out.Set("verified", true)
	} else {
		out.Set("verified", nil)
	}
	return out
}

// validateVerify is verify_email's `token: Query(min_length=1)`.
func validateVerify(w http.ResponseWriter, r *http.Request) (*http.Request, bool) {
	var errs pybody.Errors
	token, _ := errs.RequiredQueryString("token", pybody.LastQueryValue(r.URL.Query(), "token"), 1)
	if len(errs) > 0 {
		writeValidation(w, errs)
		return nil, false
	}
	return r.WithContext(context.WithValue(r.Context(), verifyInputKey{}, token)), true
}

// validateEmailBody is ResendVerificationRequest and ForgotPasswordRequest,
// the same model: one EmailStr.
func validateEmailBody(w http.ResponseWriter, r *http.Request) (*http.Request, bool) {
	body, _ := policy.BodyFrom(r.Context())
	var errs pybody.Errors
	var email string
	if object, ok := errs.Object(body); ok {
		email, _ = errs.RequiredEmailStr(object, "email")
	}
	if len(errs) > 0 {
		writeValidation(w, errs)
		return nil, false
	}
	return r.WithContext(context.WithValue(r.Context(), emailInputKey{}, email)), true
}

// validateResetPassword is ResetPasswordRequest.
func validateResetPassword(w http.ResponseWriter, r *http.Request) (*http.Request, bool) {
	body, _ := policy.BodyFrom(r.Context())
	var errs pybody.Errors
	var input resetPasswordBody
	if object, ok := errs.Object(body); ok {
		input.token, _ = errs.RequiredString(object, "token", 0, 0)
		input.password, _ = errs.RequiredString(object, "new_password", 8, 128)
	}
	if len(errs) > 0 {
		writeValidation(w, errs)
		return nil, false
	}
	return r.WithContext(context.WithValue(r.Context(), resetInputKey{}, input)), true
}

// verify is GET /api/v1/auth/verify (verify.py verify_email).
func (h handlers) verify(w http.ResponseWriter, r *http.Request) {
	token, _ := r.Context().Value(verifyInputKey{}).(string)
	ctx := r.Context()
	tx, err := h.Pool.Begin(ctx)
	if err != nil {
		h.fail(w, r, "begin", err)
		return
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	now := h.Now()
	user, err := h.redeemLinkToken(ctx, tx, verificationTokens, token, now)
	if err != nil {
		h.fail(w, r, "redeem verification token", err)
		return
	}
	if user == nil {
		refuse(w, http.StatusBadRequest, "Invalid or expired verification token")
		return
	}
	if _, err := tx.Exec(ctx, `UPDATE users SET is_verified = true, updated_at = $2 WHERE id = $1::uuid`, user.ID, now.UTC()); err != nil {
		h.fail(w, r, "mark verified", err)
		return
	}
	if err := deleteLinkTokens(ctx, tx, verificationTokens, user.ID); err != nil {
		h.fail(w, r, "delete verification tokens", err)
		return
	}
	if err := tx.Commit(ctx); err != nil {
		h.fail(w, r, "commit", err)
		return
	}
	policy.WriteModel(w, http.StatusOK, verifyEmailResponse("Email verified successfully", true), nil)
}

// resendVerification is POST /api/v1/auth/resend-verification.
func (h handlers) resendVerification(w http.ResponseWriter, r *http.Request) {
	email, _ := r.Context().Value(emailInputKey{}).(string)
	ctx := r.Context()
	generic := verifyEmailResponse("If an account exists with that email, a verification link has been sent", false)
	tx, err := h.Pool.Begin(ctx)
	if err != nil {
		h.fail(w, r, "begin", err)
		return
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	user, err := userByEmail(ctx, tx, pythonparity.Strip(pythonparity.Lower(email)))
	if err != nil {
		h.fail(w, r, "load user", err)
		return
	}
	if user == nil || user.IsVerified {
		policy.WriteModel(w, http.StatusOK, generic, nil)
		return
	}
	token, err := h.replaceLinkToken(ctx, tx, verificationTokens, user.ID, h.Now(), verificationTTL)
	if err != nil {
		h.fail(w, r, "verification token", err)
		return
	}
	if err := tx.Commit(ctx); err != nil {
		h.fail(w, r, "commit", err)
		return
	}
	h.sendVerification(ctx, user, token)
	policy.WriteModel(w, http.StatusOK, generic, nil)
}

// auditUserEvent writes a user-resource audit row in the user's first
// membership's organization, and nothing when the user has none (the audit
// row needs an organization).
func (h handlers) auditUserEvent(ctx context.Context, r *http.Request, tx pgx.Tx, user *userRow, action audit.Action, description string) error {
	orgID, err := firstMembershipOrg(ctx, tx, user.ID)
	if err != nil || orgID == nil {
		return err
	}
	userID := user.ID
	return h.emitAudit(ctx, tx, r, auditEntry{orgID: *orgID, action: action, resourceType: audit.ResourceUser,
		resourceID: user.ID.String(), userID: &userID, description: description})
}

// forgotPassword is POST /api/v1/auth/forgot-password.
func (h handlers) forgotPassword(w http.ResponseWriter, r *http.Request) {
	email, _ := r.Context().Value(emailInputKey{}).(string)
	ctx := r.Context()
	generic := verifyEmailResponse("If the account exists, a password reset email has been sent", false)
	tx, err := h.Pool.Begin(ctx)
	if err != nil {
		h.fail(w, r, "begin", err)
		return
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	user, err := userByEmail(ctx, tx, pythonparity.Strip(pythonparity.Lower(email)))
	if err != nil {
		h.fail(w, r, "load user", err)
		return
	}
	if user == nil {
		// No audit row for an unknown address: the row needs an
		// organization, and none may leak whether the account exists.
		policy.WriteModel(w, http.StatusOK, generic, nil)
		return
	}
	token, err := h.replaceLinkToken(ctx, tx, resetTokens, user.ID, h.Now(), resetTTL)
	if err != nil {
		h.fail(w, r, "reset token", err)
		return
	}
	if err := h.auditUserEvent(ctx, r, tx, user, audit.ActionPasswordResetAsked, "Password reset requested"); err != nil {
		h.fail(w, r, "audit", err)
		return
	}
	if err := tx.Commit(ctx); err != nil {
		h.fail(w, r, "commit", err)
		return
	}
	h.sendPasswordReset(ctx, user, token)
	policy.WriteModel(w, http.StatusOK, generic, nil)
}

// resetPassword is POST /api/v1/auth/reset-password. It has no rate limit
// and no password-policy check beyond the model's 8 to 128 characters.
func (h handlers) resetPassword(w http.ResponseWriter, r *http.Request) {
	r, ok := validateResetPassword(w, r)
	if !ok {
		return
	}
	input, _ := r.Context().Value(resetInputKey{}).(resetPasswordBody)
	ctx := r.Context()
	tx, err := h.Pool.Begin(ctx)
	if err != nil {
		h.fail(w, r, "begin", err)
		return
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	now := h.Now()
	user, err := h.redeemLinkToken(ctx, tx, resetTokens, input.token, now)
	if err != nil {
		h.fail(w, r, "redeem reset token", err)
		return
	}
	if user == nil {
		refuse(w, http.StatusBadRequest, "Invalid or expired token")
		return
	}
	hash, err := hashPassword(input.password)
	if err != nil {
		h.fail(w, r, "hash password", err)
		return
	}
	if _, err := tx.Exec(ctx, `UPDATE users SET password_hash = $2, token_version = $3, updated_at = $4 WHERE id = $1::uuid`,
		user.ID, hash, user.TokenVersion+1, now.UTC()); err != nil {
		h.fail(w, r, "update password", err)
		return
	}
	if err := revokeAllForUser(ctx, tx, user.ID, h.Now()); err != nil {
		h.fail(w, r, "revoke refresh tokens", err)
		return
	}
	if err := deleteLinkTokens(ctx, tx, resetTokens, user.ID); err != nil {
		h.fail(w, r, "delete reset tokens", err)
		return
	}
	if err := h.auditUserEvent(ctx, r, tx, user, audit.ActionPasswordReset, "User reset password"); err != nil {
		h.fail(w, r, "audit", err)
		return
	}
	if err := tx.Commit(ctx); err != nil {
		h.fail(w, r, "commit", err)
		return
	}
	policy.WriteModel(w, http.StatusOK, verifyEmailResponse("Password reset successful", false), nil)
}

// revokeAllForUser is refresh_tokens.revoke_all_for_user.
func revokeAllForUser(ctx context.Context, tx pgx.Tx, userID uuid.UUID, now time.Time) error {
	_, err := tx.Exec(ctx, `UPDATE refresh_tokens SET revoked_at = $2 WHERE user_id = $1::uuid AND revoked_at IS NULL`,
		userID, now.UTC())
	return err
}
