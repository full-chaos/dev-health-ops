//go:build integration

package session_test

import (
	"context"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/full-chaos/dev-health-ops/internal/api/authmail"
	"github.com/full-chaos/dev-health-ops/internal/auth/signedtoken"
)

// rotationCase is one link-token rotation state: the mail configuration the
// handlers run with, the secret the presented link was signed with, and
// whether the link is accepted, and whether the legacy-key link counter
// counts it.
type rotationCase struct {
	name     string
	mail     authmail.Config
	signedBy string
	accepted bool
	legacy   bool
}

var rotationCases = []rotationCase{
	{"signed with the access key before the roll", authmail.Config{TokenSecret: "access-key-before-roll", LinkSecret: "link-new"}, "access-key-before-roll", true, true},
	{"signed with the link secret after the roll", authmail.Config{TokenSecret: "access-key-before-roll", LinkSecret: "link-new"}, "link-new", true, false},
	{"signed with the access key after the access key is removed", authmail.Config{LinkSecret: "link-new"}, "access-key-before-roll", false, false},
	{"signed with the retired link secret during the overlap", authmail.Config{LinkSecret: "link-new", PreviousLinkSecret: "link-old"}, "link-old", true, false},
	{"signed with the retired link secret after the overlap", authmail.Config{LinkSecret: "link-new"}, "link-old", false, false},
}

// installLegacyLinkCounter installs a readable legacy-key link counter for one test.
func installLegacyLinkCounter(t *testing.T) *sdkmetric.ManualReader {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	counter, err := authmail.NewLegacyLinkCounter(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test"), "dev-health-ops-api")
	if err != nil {
		t.Fatal(err)
	}
	authmail.SetLegacyLinkAccepted(counter)
	t.Cleanup(func() { authmail.SetLegacyLinkAccepted(nil) })
	return reader
}

// legacyLinkCount is the counter's value for purpose.
func legacyLinkCount(t *testing.T, reader *sdkmetric.ManualReader, purpose string) int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	var total int64
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			sum, ok := m.Data.(metricdata.Sum[int64])
			if m.Name != authmail.LegacyLinkAcceptedMetric || !ok {
				continue
			}
			for _, point := range sum.DataPoints {
				if value, _ := point.Attributes.Value(attribute.Key("purpose")); value.AsString() == purpose {
					total += point.Value
				}
			}
		}
	}
	return total
}

func assertLegacyLinkCount(t *testing.T, reader *sdkmetric.ManualReader, purpose string, legacy bool) {
	t.Helper()
	want := int64(0)
	if legacy {
		want = 1
	}
	if got := legacyLinkCount(t, reader, purpose); got != want {
		t.Fatalf("legacy link counter %s = %d, want %d", purpose, got, want)
	}
}

// TestLinkTokensFollowLinkSecretRotation runs invite accept, e-mail
// verification and password reset in each rotation state; one stack per state,
// as the link-mail configuration is fixed per stack.
func TestLinkTokensFollowLinkSecretRotation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	t.Setenv("TRUSTED_PROXIES", "127.0.0.1,::1")
	for _, tc := range rotationCases {
		t.Run(tc.name, func(t *testing.T) {
			st := startStackWithMail(t, ctx, tc.mail)
			links := installLegacyLinkCounter(t)

			t.Run("invite accept", func(t *testing.T) {
				userID, userEmail, token := seedInviteSubjectSignedWith(t, ctx, st, inviteCases[0], tc.signedBy)
				access := changeToken(t, st, userID, userEmail, "", "", 0)
				status, body := changeRequest(t, ctx, st, http.MethodPost, "/api/v1/auth/accept-invite", access, `{"token": "`+token+`"}`)
				memberships, inviteStatus := inviteState(t, ctx, st, userID, token)
				assertLegacyLinkCount(t, links, authmail.PurposeInvite, tc.legacy)
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

			t.Run("email verification", func(t *testing.T) {
				userID, token := seedLinkToken(t, ctx, st, "email_verification_tokens", tc.signedBy)
				status, body := changeRequest(t, ctx, st, http.MethodGet, "/api/v1/auth/verify?token="+url.QueryEscape(token), "", "")
				var verified bool
				if err := st.pool.QueryRow(ctx, `SELECT is_verified FROM users WHERE id = $1::uuid`, userID).Scan(&verified); err != nil {
					t.Fatal(err)
				}
				assertLegacyLinkCount(t, links, authmail.PurposeEmailVerification, tc.legacy)
				if tc.accepted != (status == http.StatusOK) || tc.accepted != verified {
					t.Fatalf("status %d verified %v body %s, want accepted=%v", status, verified, body, tc.accepted)
				}
			})

			t.Run("password reset", func(t *testing.T) {
				userID, token := seedLinkToken(t, ctx, st, "password_reset_tokens", tc.signedBy)
				status, body := changeRequest(t, ctx, st, http.MethodPost, "/api/v1/auth/reset-password", "",
					`{"token": "`+token+`", "new_password": "a-new-password-1"}`)
				var changed bool
				if err := st.pool.QueryRow(ctx, `SELECT password_hash IS NOT NULL FROM users WHERE id = $1::uuid`, userID).Scan(&changed); err != nil {
					t.Fatal(err)
				}
				assertLegacyLinkCount(t, links, authmail.PurposePasswordReset, tc.legacy)
				if tc.accepted != (status == http.StatusOK) || tc.accepted != changed {
					t.Fatalf("status %d password set %v body %s, want accepted=%v", status, changed, body, tc.accepted)
				}
			})
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
