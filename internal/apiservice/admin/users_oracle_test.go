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

// TestUserCRUDAndPasswordChangeMatchesThePythonAPI is the venue-oracle
// proof for CHAOS-6304's 6 user routes: list/get/create/patch/delete and
// set_user_password (the audit-writing route team-lead named explicitly
// for row-diff proof). The org and its members are seeded directly by SQL
// -- no org route is exercised here, that is CHAOS-6305's own oracle.
func TestUserCRUDAndPasswordChangeMatchesThePythonAPI(t *testing.T) {
	ctx := context.Background()
	root := repoRoot(t)
	const jwtKey = "venue-oracle-test-secret-key-for-user-crud-flow-32-bytes!!"
	const adminPlaintextPassword = "correct horse battery staple 9"

	orgID := uuid.New()
	adminID := uuid.New()
	memberID := uuid.New()

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
VALUES ($1, 'venue-users-org', 'Venue Users Org', 'community', 'stripe', true, now(), now())`, orgID)
			exec(`INSERT INTO users (id, email, password_hash, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, 'venue-uadmin@example.com', $2, true, true, false, 0, now(), now())`, adminID, string(adminHash))
			exec(`INSERT INTO users (id, email, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, 'venue-umember@example.com', true, true, false, 0, now(), now())`, memberID)
			exec(`INSERT INTO memberships (id, org_id, user_id, role, joined_at, created_at, updated_at)
VALUES ($1, $2, $3, 'admin', now(), now(), now())`, uuid.New(), orgID, adminID)
			exec(`INSERT INTO memberships (id, org_id, user_id, role, joined_at, created_at, updated_at)
VALUES ($1, $2, $3, 'member', now(), now(), now())`, uuid.New(), orgID, memberID)
			return map[string]map[string]any{
				"admin": {"user_id": adminID.String(), "email": "venue-uadmin@example.com", "org_id": orgID.String(), "role": "admin"},
			}
		},
	})

	jsonHeaders := map[string]string{"Authorization": "Bearer " + venue.Tokens["admin"], "Content-Type": "application/json"}
	authHeaders := map[string]string{"Authorization": "Bearer " + venue.Tokens["admin"]}

	requests := []venueoracle.Request{
		{Name: "list users by org", Method: "GET", Path: "/api/v1/admin/users",
			Headers: map[string]string{"Authorization": "Bearer " + venue.Tokens["admin"], "X-Org-Id": orgID.String()}},
		{Name: "get user", Method: "GET", Path: "/api/v1/admin/users/" + memberID.String(), Headers: authHeaders},
		{Name: "get user not found", Method: "GET", Path: "/api/v1/admin/users/" + uuid.New().String(), Headers: authHeaders},
		{Name: "create user", Method: "POST", Path: "/api/v1/admin/users", Headers: jsonHeaders,
			Body: venueoracle.B64(`{"email":"venue-newuser@example.com","password":"a brand new password 7"}`)},
		{Name: "patch user", Method: "PATCH", Path: "/api/v1/admin/users/" + memberID.String(), Headers: jsonHeaders,
			Body: venueoracle.B64(`{"full_name":"A New Name"}`)},
		{Name: "set password", Method: "POST", Path: "/api/v1/admin/users/" + memberID.String() + "/password", Headers: jsonHeaders,
			Body: venueoracle.B64(fmt.Sprintf(`{"admin_password":%q,"password":"a new strong password 42"}`, adminPlaintextPassword))},
		{Name: "set password wrong admin password", Method: "POST", Path: "/api/v1/admin/users/" + memberID.String() + "/password", Headers: jsonHeaders,
			Body: venueoracle.B64(`{"admin_password":"totally the wrong password","password":"a new strong password 42"}`)},
		{Name: "delete user", Method: "DELETE", Path: "/api/v1/admin/users/" + memberID.String(), Headers: authHeaders},
		{Name: "delete user again", Method: "DELETE", Path: "/api/v1/admin/users/" + memberID.String(), Headers: authHeaders},
	}
	python := venue.ServePython(t, requests)
	goBase, _ := startGoServer(t, ctx, venue, jwtKey)

	receipt := venueoracle.Diff(t, goBase, requests, python, venueoracle.DiffOptions{
		Normalize: func(request venueoracle.Request, body string) string {
			for _, field := range []string{"id", "created_at", "updated_at"} {
				body = redactField(body, field)
			}
			return body
		},
	})
	t.Log(receipt)

	sourceRows := venueoracle.TableRows(t, ctx, venue.AdminURI(t, venue.SourceDB), userPasswordAuditQuery(orgID, adminID))
	goRows := venueoracle.TableRows(t, ctx, venue.AdminURI(t, venue.GoDB), userPasswordAuditQuery(orgID, adminID))
	if sourceRows != goRows {
		t.Errorf("audit_logs rows differ:\n python: %s\n go:     %s", sourceRows, goRows)
	}
}

func userPasswordAuditQuery(orgID, adminID uuid.UUID) string {
	return fmt.Sprintf(`SELECT org_id, user_id, action, resource_type, status, changes
FROM audit_logs WHERE org_id = '%s' AND user_id = '%s' ORDER BY created_at`, orgID, adminID)
}
