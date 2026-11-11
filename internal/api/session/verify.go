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
	"github.com/full-chaos/dev-health-ops/internal/api/usersowned"
)

type (
	verifyInputKey struct{}
	emailInputKey  struct{}
)

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

// verify, resendVerification, forgotPassword and resetPassword are the link
// routes auth-service serves (the credentials forward): go-api issues and
// redeems no link token, so each answers the moved-operation refusal with the
// users-owned switch off or on.
func (h handlers) verify(w http.ResponseWriter, _ *http.Request) { usersowned.Write(w) }

func (h handlers) resendVerification(w http.ResponseWriter, _ *http.Request) { usersowned.Write(w) }

func (h handlers) forgotPassword(w http.ResponseWriter, _ *http.Request) { usersowned.Write(w) }

func (h handlers) resetPassword(w http.ResponseWriter, _ *http.Request) { usersowned.Write(w) }

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

// revokeAllForUser is refresh_tokens.revoke_all_for_user.
func revokeAllForUser(ctx context.Context, tx pgx.Tx, userID uuid.UUID, now time.Time) error {
	_, err := tx.Exec(ctx, `UPDATE refresh_tokens SET revoked_at = $2 WHERE user_id = $1::uuid AND revoked_at IS NULL`,
		userID, now.UTC())
	return err
}
