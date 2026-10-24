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
	"github.com/full-chaos/dev-health-ops/internal/auth/passwordhash"
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
	if h.refuseWhileUsersOwned(w) {
		return
	}
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

// resendVerification is POST /api/v1/auth/resend-verification. The answer
// follows one lookup and is the same for every address: the token write and
// the mail run off the request, and an address with no unverified account
// runs a decoy job of the same shape.
//
// Stricter than Python (D6129): the known-address failure that Python answered
// as a 500 after the lookup (a token write or mail error) is logged and counted
// off the request, so no failure is visible to the caller.
func (h handlers) resendVerification(w http.ResponseWriter, r *http.Request) {
	email, _ := r.Context().Value(emailInputKey{}).(string)
	ctx := r.Context()
	generic := verifyEmailResponse("If an account exists with that email, a verification link has been sent", false)
	user, err := h.lookupByEmail(ctx, email)
	if err != nil {
		h.fail(w, r, "load user", err)
		return
	}
	h.queueLinkJob(ctx, r, mailVerification, resendActsOn(user), func(ctx context.Context, jobRequest *http.Request) error {
		return h.issueVerificationLink(ctx, user)
	})
	policy.WriteModel(w, http.StatusOK, generic, nil)
}

// resendActsOn is the refusal kept from before: a resend acts only on an
// account that exists and is not verified yet; anything else is a decoy.
func resendActsOn(user *userRow) bool { return user != nil && !user.IsVerified }

// forgotActsOn: a reset link is issued for any account that exists.
func forgotActsOn(user *userRow) bool { return user != nil }

// lookupByEmail is the one state-dependent step left on the request of the
// forgot and resend routes.
func (h handlers) lookupByEmail(ctx context.Context, email string) (*userRow, error) {
	tx, err := h.Pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	return userByEmail(ctx, tx, pythonparity.Strip(pythonparity.Lower(email)))
}

// issueVerificationLink replaces the user's verification token and mails it.
func (h handlers) issueVerificationLink(ctx context.Context, user *userRow) error {
	tx, err := h.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	token, err := h.replaceLinkToken(ctx, tx, verificationTokens, user.ID, h.Now(), verificationTTL)
	if err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	return h.sendRegisterVerification(ctx, user, token)
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

// forgotPassword is POST /api/v1/auth/forgot-password. Like resend, the answer
// follows one lookup and is the same for every address; the token write, the
// audit row and the mail run off the request, and an unknown address runs a
// decoy job. The 500 that only a known address could reach is gone (D6129,
// stricter than Python).
func (h handlers) forgotPassword(w http.ResponseWriter, r *http.Request) {
	email, _ := r.Context().Value(emailInputKey{}).(string)
	ctx := r.Context()
	generic := verifyEmailResponse("If the account exists, a password reset email has been sent", false)
	user, err := h.lookupByEmail(ctx, email)
	if err != nil {
		h.fail(w, r, "load user", err)
		return
	}
	h.queueLinkJob(ctx, r, mailPasswordReset, forgotActsOn(user), func(ctx context.Context, jobRequest *http.Request) error {
		return h.issueResetLink(ctx, jobRequest, user)
	})
	policy.WriteModel(w, http.StatusOK, generic, nil)
}

// issueResetLink replaces the user's reset token, audits the request and mails
// the link. There is no audit row for an address with no organization.
func (h handlers) issueResetLink(ctx context.Context, r *http.Request, user *userRow) error {
	tx, err := h.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	token, err := h.replaceLinkToken(ctx, tx, resetTokens, user.ID, h.Now(), resetTTL)
	if err != nil {
		return err
	}
	if err := h.auditUserEvent(ctx, r, tx, user, audit.ActionPasswordResetAsked, "Password reset requested"); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	return h.sendResetMail(ctx, user, token)
}

// resetPassword is POST /api/v1/auth/reset-password. It has no rate limit;
// the password goes through the shared password policy before redemption.
func (h handlers) resetPassword(w http.ResponseWriter, r *http.Request) {
	if h.refuseWhileUsersOwned(w) {
		return
	}
	r, ok := validateResetPasswordPolicy(w, r)
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
	hash, err := passwordhash.Hash(input.password)
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
