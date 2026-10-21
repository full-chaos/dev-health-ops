//go:build integration

package session_test

import (
	"context"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/authmail"
	"github.com/full-chaos/dev-health-ops/internal/auth/signedtoken"
)

// rotationCase is one link-token rotation state: the mail configuration the
// handlers run with, the secret the presented link was signed with, and
// whether the link is accepted.
type rotationCase struct {
	name     string
	mail     authmail.Config
	signedBy string
	accepted bool
}

var rotationCases = []rotationCase{
	{"signed with the access key before the roll", authmail.Config{TokenSecret: "access-key-before-roll", LinkSecret: "link-new"}, "access-key-before-roll", true},
	{"signed with the retired link secret during the overlap", authmail.Config{LinkSecret: "link-new", PreviousLinkSecret: "link-old"}, "link-old", true},
	{"signed with the retired link secret after the overlap", authmail.Config{LinkSecret: "link-new"}, "link-old", false},
}

func TestInviteAcceptFollowsLinkSecretRotation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	t.Setenv("TRUSTED_PROXIES", "127.0.0.1,::1")
	for _, tc := range rotationCases {
		t.Run(tc.name, func(t *testing.T) {
			st := startStackWithMail(t, ctx, tc.mail)
			userID, userEmail, token := seedInviteSubjectSignedWith(t, ctx, st, inviteCases[0], tc.signedBy)
			access := changeToken(t, st, userID, userEmail, "", "", 0)
			status, body := changeRequest(t, ctx, st, http.MethodPost, "/api/v1/auth/accept-invite", access, `{"token": "`+token+`"}`)
			memberships, inviteStatus := inviteState(t, ctx, st, userID, token)
			if tc.accepted {
				if status != http.StatusOK || memberships != 1 || inviteStatus != "accepted" {
					t.Fatalf("status %d memberships %d invite %s, want accepted", status, memberships, inviteStatus)
				}
				return
			}
			if status == http.StatusOK || memberships != 0 || inviteStatus != "pending" {
				t.Fatalf("status %d memberships %d invite %s body %s, want a refusal with no writes", status, memberships, inviteStatus, body)
			}
		})
	}
}

// seedLinkToken stores a verification or reset token for a fresh unverified
// user, signed with secret, and returns the user id and the token.
func seedLinkToken(t *testing.T, ctx context.Context, st stack, table, secret string) (userID, token string) {
	t.Helper()
	userID = uuid.NewString()
	tokenID := uuid.New()
	token, hash := signedtoken.Build(tokenID, secret)
	if _, err := st.pool.Exec(ctx, `INSERT INTO users (id, email, password_hash, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, NULL, true, false, false, 0, now(), now())`, userID, "link-"+userID[:8]+"@example.com"); err != nil {
		t.Fatal(err)
	}
	var insert string
	switch table {
	case "email_verification_tokens":
		insert = `INSERT INTO email_verification_tokens (id, user_id, token_hash, expires_at, created_at) VALUES ($1, $2::uuid, $3, now() + interval '1 day', now())`
	default:
		insert = `INSERT INTO password_reset_tokens (id, user_id, token_hash, expires_at, created_at) VALUES ($1, $2::uuid, $3, now() + interval '1 day', now())`
	}
	if _, err := st.pool.Exec(ctx, insert, tokenID, userID, hash); err != nil {
		t.Fatal(err)
	}
	return userID, token
}

func TestEmailVerificationFollowsLinkSecretRotation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	t.Setenv("TRUSTED_PROXIES", "127.0.0.1,::1")
	for _, tc := range rotationCases {
		t.Run(tc.name, func(t *testing.T) {
			st := startStackWithMail(t, ctx, tc.mail)
			userID, token := seedLinkToken(t, ctx, st, "email_verification_tokens", tc.signedBy)
			status, body := changeRequest(t, ctx, st, http.MethodGet, "/api/v1/auth/verify?token="+url.QueryEscape(token), "", "")
			var verified bool
			if err := st.pool.QueryRow(ctx, `SELECT is_verified FROM users WHERE id = $1::uuid`, userID).Scan(&verified); err != nil {
				t.Fatal(err)
			}
			if tc.accepted != (status == http.StatusOK) || tc.accepted != verified {
				t.Fatalf("status %d verified %v body %s, want accepted=%v", status, verified, body, tc.accepted)
			}
		})
	}
}

func TestPasswordResetFollowsLinkSecretRotation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	t.Setenv("TRUSTED_PROXIES", "127.0.0.1,::1")
	for _, tc := range rotationCases {
		t.Run(tc.name, func(t *testing.T) {
			st := startStackWithMail(t, ctx, tc.mail)
			userID, token := seedLinkToken(t, ctx, st, "password_reset_tokens", tc.signedBy)
			status, body := changeRequest(t, ctx, st, http.MethodPost, "/api/v1/auth/reset-password", "",
				`{"token": "`+token+`", "new_password": "a-new-password-1"}`)
			var changed bool
			if err := st.pool.QueryRow(ctx, `SELECT password_hash IS NOT NULL FROM users WHERE id = $1::uuid`, userID).Scan(&changed); err != nil {
				t.Fatal(err)
			}
			if tc.accepted != (status == http.StatusOK) || tc.accepted != changed {
				t.Fatalf("status %d password set %v body %s, want accepted=%v", status, changed, body, tc.accepted)
			}
		})
	}
}
