//go:build integration

package session_test

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/signedtoken"
)

type inviteCase struct {
	name        string
	userEmail   string
	verified    bool
	inviteEmail string
	accepted    bool
	reason      string
}

var inviteCases = []inviteCase{
	{"match", "ada@example.com", true, "ada@example.com", true, ""},
	{"mismatch", "ada@example.com", true, "grace@example.com", false, "invite_email_mismatch"},
	{"case and space variant", "Ada@Example.com", true, "  ADA@example.com ", true, ""},
	{"unverified", "ada@example.com", false, "ada@example.com", false, "invite_email_unverified"},
}

func seedInviteSubject(t *testing.T, ctx context.Context, st stack, tc inviteCase) (userID, userEmail, token string) {
	t.Helper()
	return seedInviteSubjectSignedWith(t, ctx, st, tc, signedtoken.Secret("", ""))
}

// seedInviteSubjectSignedWith is seedInviteSubject with the invite token signed
// with secret.
func seedInviteSubjectSignedWith(t *testing.T, ctx context.Context, st stack, tc inviteCase, secret string) (userID, userEmail, token string) {
	t.Helper()
	tag := uuid.NewString()[:8]
	userEmail = strings.ReplaceAll(strings.ReplaceAll(tc.userEmail, "ada", "ada"+tag), "Ada", "Ada"+tag)
	inviteEmail := strings.ReplaceAll(strings.ReplaceAll(strings.ReplaceAll(tc.inviteEmail, "ada", "ada"+tag), "ADA", "ADA"+tag), "grace", "grace"+tag)
	userID = uuid.NewString()
	inviteID := uuid.New()
	token, hash := signedtoken.Build(inviteID, secret)
	for _, statement := range []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO users (id, email, password_hash, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, NULL, true, $3, false, 0, now(), now())`, []any{userID, userEmail, tc.verified}},
		{`INSERT INTO org_invites (id, org_id, email, role, token_hash, invited_by_id, status, expires_at, created_at, updated_at)
VALUES ($1, $2, $3, 'member', $4, $5, 'pending', now() + interval '3 days', now(), now())`,
			[]any{inviteID, st.specs["alice"]["org_id"], inviteEmail, hash, st.specs["alice"]["user_id"]}},
	} {
		if _, err := st.pool.Exec(ctx, statement.sql, statement.args...); err != nil {
			t.Fatal(err)
		}
	}
	return userID, userEmail, token
}

func inviteState(t *testing.T, ctx context.Context, st stack, userID, token string) (memberships int, status string) {
	t.Helper()
	if err := st.pool.QueryRow(ctx, `SELECT count(*) FROM memberships WHERE user_id = $1::uuid`, userID).Scan(&memberships); err != nil {
		t.Fatal(err)
	}
	if err := st.pool.QueryRow(ctx, `SELECT status FROM org_invites WHERE token_hash = $1::text`, signedtoken.Hash(token)).Scan(&status); err != nil {
		t.Fatal(err)
	}
	return memberships, status
}

func TestInviteAcceptRequiresTheInviteEmail(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	t.Setenv("TRUSTED_PROXIES", "127.0.0.1,::1")
	t.Setenv("JWT_SECRET_KEY", "")
	t.Setenv("SETTINGS_ENCRYPTION_KEY", "")
	st := startStack(t, ctx)

	for _, entry := range []struct {
		path string
		body func(token string) string
	}{
		{"/api/v1/auth/accept-invite", func(token string) string { return `{"token": "` + token + `"}` }},
		{"/api/v1/auth/onboard", func(token string) string { return `{"action": "join_org", "invite_code": "` + token + `"}` }},
	} {
		for _, tc := range inviteCases {
			t.Run(entry.path+" "+tc.name, func(t *testing.T) {
				userID, userEmail, token := seedInviteSubject(t, ctx, st, tc)
				access := changeToken(t, st, userID, userEmail, "", "", 0)
				status, body := changeRequest(t, ctx, st, http.MethodPost, entry.path, access, entry.body(token))
				if status == http.StatusOK {
					body = "<token response>"
				}
				memberships, inviteStatus := inviteState(t, ctx, st, userID, token)
				if tc.accepted {
					if status != http.StatusOK || memberships != 1 || inviteStatus != "accepted" {
						t.Fatalf("status %d memberships %d invite %s body %s, want accepted", status, memberships, inviteStatus, body)
					}
					return
				}
				if status != http.StatusForbidden || !strings.Contains(body, tc.reason) || memberships != 0 || inviteStatus != "pending" {
					t.Fatalf("status %d memberships %d invite %s body %s, want %s with no writes", status, memberships, inviteStatus, body, tc.reason)
				}
			})
		}
	}
}
