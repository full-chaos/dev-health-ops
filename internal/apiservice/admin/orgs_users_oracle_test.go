//go:build integration

package admin_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/crypto/bcrypt"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// TestOrgInviteAndPasswordChangeAuditMatchThePythonAPI is the venue-oracle
// proof for CHAOS-6250's two other audit-writing routes -- create_org_invite
// (member_invited) and set_user_password (password_changed) -- team-lead
// named these two explicitly for row-diff proof (impersonation start/stop
// have their own test). Response bodies and the audit_logs rows either
// plane's writes produced must compare equal.
func TestOrgInviteAndPasswordChangeAuditMatchThePythonAPI(t *testing.T) {
	ctx := context.Background()
	root := repoRoot(t)
	const jwtKey = "venue-oracle-test-secret-key-for-orgs-users-flow-32-bytes!"
	const adminPlaintextPassword = "correct horse battery staple 9"

	orgID := uuid.New()
	adminID := uuid.New()
	targetID := uuid.New()

	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		Root:   root,
		JWTKey: jwtKey,
		Seed: func(t *testing.T, ctx context.Context, admin *pgxpool.Pool, v *venueoracle.Venue) map[string]map[string]any {
			t.Helper()
			exec := func(sql string, args ...any) {
				t.Helper()
				if _, err := admin.Exec(ctx, sql, args...); err != nil {
					t.Fatalf("seed: %v\n%s", err, sql)
				}
			}
			adminHash, err := bcrypt.GenerateFromPassword([]byte(adminPlaintextPassword), bcrypt.DefaultCost)
			if err != nil {
				t.Fatalf("bcrypt: %v", err)
			}
			exec(`INSERT INTO organizations (id, slug, name, tier, managed_by, is_active, created_at, updated_at)
VALUES ($1, 'venue-org-2', 'Venue Org 2', 'community', 'stripe', true, now(), now())`, orgID)
			exec(`INSERT INTO users (id, email, password_hash, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, 'venue-admin-2@example.com', $2, true, true, true, 0, now(), now())`, adminID, string(adminHash))
			exec(`INSERT INTO users (id, email, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, 'venue-target-2@example.com', true, true, false, 0, now(), now())`, targetID)
			exec(`INSERT INTO memberships (id, org_id, user_id, role, joined_at, created_at, updated_at)
VALUES ($1, $2, $3, 'admin', now(), now(), now())`, uuid.New(), orgID, adminID)
			exec(`INSERT INTO memberships (id, org_id, user_id, role, joined_at, created_at, updated_at)
VALUES ($1, $2, $3, 'member', now(), now(), now())`, uuid.New(), orgID, targetID)
			return map[string]map[string]any{
				"admin": {"user_id": adminID.String(), "email": "venue-admin-2@example.com", "org_id": orgID.String(), "role": "admin"},
			}
		},
	})

	requests := []venueoracle.Request{
		{Name: "create invite", Method: "POST", Path: "/api/v1/admin/orgs/" + orgID.String() + "/invites",
			Headers: map[string]string{"Authorization": "Bearer " + venue.Tokens["admin"], "Content-Type": "application/json"},
			Body:    venueoracle.B64(`{"email":"invitee@example.com","role":"member"}`)},
		{Name: "set password", Method: "POST", Path: "/api/v1/admin/users/" + targetID.String() + "/password",
			Headers: map[string]string{"Authorization": "Bearer " + venue.Tokens["admin"], "Content-Type": "application/json"},
			Body: venueoracle.B64(fmt.Sprintf(`{"admin_password":%q,"password":"a new strong password 42"}`,
				adminPlaintextPassword))},
		{Name: "set password wrong admin password", Method: "POST", Path: "/api/v1/admin/users/" + targetID.String() + "/password",
			Headers: map[string]string{"Authorization": "Bearer " + venue.Tokens["admin"], "Content-Type": "application/json"},
			Body:    venueoracle.B64(`{"admin_password":"totally the wrong password","password":"a new strong password 42"}`)},
	}
	python := venue.ServePython(t, requests)
	goBase, _ := startGoServer(t, ctx, venue, jwtKey)

	receipt := venueoracle.Diff(t, goBase, requests, python, venueoracle.DiffOptions{
		Normalize: func(request venueoracle.Request, body string) string {
			// A fresh invite's id and token-derived fields are random per
			// plane by construction (uuid4 minted independently on each
			// side); expires_at/created_at/updated_at are wall-clock-derived
			// and minted independently on each plane, seconds apart under
			// a live test. None of these are asserted equal between planes
			// -- see the impersonation oracle's identical ruling for
			// expires_at.
			for _, field := range []string{"id", "expires_at", "created_at", "updated_at"} {
				body = redactField(body, field)
			}
			return body
		},
	})
	t.Log(receipt)

	sourceRows := venueoracle.TableRows(t, ctx, venue.AdminURI(t, venue.SourceDB), orgsUsersAuditQuery(orgID, adminID, targetID))
	goRows := venueoracle.TableRows(t, ctx, venue.AdminURI(t, venue.GoDB), orgsUsersAuditQuery(orgID, adminID, targetID))
	if sourceRows != goRows {
		t.Errorf("audit_logs rows differ:\n python: %s\n go:     %s", sourceRows, goRows)
	}
}

func orgsUsersAuditQuery(orgID, adminID, targetID uuid.UUID) string {
	return fmt.Sprintf(`SELECT org_id, user_id, action, resource_type, status, changes
FROM audit_logs WHERE org_id = '%s' AND user_id = '%s' ORDER BY created_at`, orgID, adminID)
}
