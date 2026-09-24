package session

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/auth/signedtoken"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// The two single-use link-token tables. Both have the columns of
// models/email_verification_token.py and models/password_reset_token.py:
// id (the uuid inside the token), user_id, token_hash, expires_at,
// created_at.
const (
	verificationTokens = "email_verification_tokens"
	resetTokens        = "password_reset_tokens"
)

// replaceLinkToken is create_email_verification_token /
// create_password_reset_token: every earlier token of the user is deleted
// and a new one, valid for ttl, is stored by its hash. It returns the token.
func (h handlers) replaceLinkToken(ctx context.Context, tx pgx.Tx, table string, userID uuid.UUID, now time.Time, ttl time.Duration) (string, error) {
	id := h.NewUUID()
	token, tokenHash := signedtoken.Build(id, h.Mail.Secret())
	if _, err := tx.Exec(ctx, `DELETE FROM `+table+` WHERE user_id = $1::uuid`, userID); err != nil {
		return "", err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO `+table+` (id, user_id, token_hash, expires_at, created_at)
VALUES ($1, $2, $3, $4, $5)`, id, userID, tokenHash, now.Add(ttl).UTC(), now.UTC()); err != nil {
		return "", err
	}
	return token, nil
}

// redeemLinkToken is the shared head of verify_email_token and
// reset_password_with_token: the user a valid, unexpired token belongs to,
// nil when Python returns None. The caller deletes the user's tokens once
// it has acted. A token whose signature holds a non-ASCII character is
// hmac.compare_digest's TypeError, returned as an error.
func (h handlers) redeemLinkToken(ctx context.Context, tx pgx.Tx, table, token string, now time.Time) (*userRow, error) {
	valid, err := signedtoken.Valid(token, h.Mail.Secret())
	if err != nil || !valid {
		return nil, err
	}
	var userID uuid.UUID
	err = tx.QueryRow(ctx, `SELECT user_id FROM `+table+` WHERE token_hash = $1::text AND expires_at >= $2`,
		signedtoken.Hash(token), now.UTC()).Scan(&userID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	// A token without its user returns None after deleting the token, but
	// the route then refuses the request and the session rolls back, so
	// nothing is written; the foreign key cascade makes it unreachable.
	return userByID(ctx, tx, userID)
}

func deleteLinkTokens(ctx context.Context, tx pgx.Tx, table string, userID uuid.UUID) error {
	_, err := tx.Exec(ctx, `DELETE FROM `+table+` WHERE user_id = $1::uuid`, userID)
	return err
}

// authKey is rate_limit.py's get_auth_key: the TCP peer (slowapi's
// get_remote_address, "127.0.0.1" without one) and the request's e-mail --
// the body's "email" when it is a string, else the "email" query parameter
// -- stripped and lowered, "unknown" when empty.
func authKey(r *http.Request, body pyjson.Value) string {
	host := "127.0.0.1"
	if peer := clientHost(r); peer != nil && *peer != "" {
		host = *peer
	}
	email := "unknown"
	var raw *string
	if object, ok := body.(*pyjson.Object); ok {
		if value, present := object.Get("email"); present {
			if text, isString := value.(string); isString {
				raw = &text
			}
		}
	}
	if raw == nil {
		if value := pybody.LastQueryValue(r.URL.Query(), "email"); value != nil && *value != "" {
			raw = value
		}
	}
	if raw != nil {
		if normalized := pythonparity.Lower(pythonparity.Strip(*raw)); normalized != "" {
			email = normalized
		}
	}
	return host + ":" + email
}

// sendLink sends one link e-mail for user, best effort.
func (h handlers) sendLink(ctx context.Context, user *userRow, failure, subject, template, urlField, path, token string) {
	name := user.Email
	if user.FullName != nil && *user.FullName != "" {
		name = *user.FullName
	}
	h.Mail.Send(ctx, h.Logger, failure, user.Email, subject, template, map[string]string{
		"full_name": name, urlField: h.Mail.Link(path, token),
	}, "user_id", user.ID.String())
}

// sendVerification is send_verification_email.
func (h handlers) sendVerification(ctx context.Context, user *userRow, token string) {
	h.sendLink(ctx, user, "api: failed to send verification email", "Verify your email address",
		"verification", "verification_url", "/auth/verify", token)
}

// sendPasswordReset is send_password_reset_email.
func (h handlers) sendPasswordReset(ctx context.Context, user *userRow, token string) {
	h.sendLink(ctx, user, "api: failed to send password reset email", "Reset your password",
		"password_reset", "reset_url", "/auth/reset-password", token)
}
