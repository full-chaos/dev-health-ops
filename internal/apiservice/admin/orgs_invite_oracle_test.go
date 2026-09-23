//go:build integration

package admin_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// TestOrgInviteAuditMatchesThePythonAPI is the venue-oracle proof for
// CHAOS-6305's create_org_invite (member_invited) -- the first of the two
// audit-writing routes team-lead named explicitly for row-diff proof;
// set_user_password (password_changed), the second, has its own test in
// CHAOS-6304 (users_oracle_test.go). Response body and the audit_logs row
// either plane's write produced must compare equal.
func TestOrgInviteAuditMatchesThePythonAPI(t *testing.T) {
	ctx := context.Background()
	root := repoRoot(t)
	const jwtKey = "venue-oracle-test-secret-key-for-org-invite-flow-32-bytes!"

	orgID := uuid.New()
	adminID := uuid.New()

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
			exec(`INSERT INTO organizations (id, slug, name, tier, managed_by, is_active, created_at, updated_at)
VALUES ($1, 'venue-invite-org', 'Venue Invite Org', 'community', 'stripe', true, now(), now())`, orgID)
			exec(`INSERT INTO users (id, email, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, 'venue-invite-admin@example.com', true, true, true, 0, now(), now())`, adminID)
			exec(`INSERT INTO memberships (id, org_id, user_id, role, joined_at, created_at, updated_at)
VALUES ($1, $2, $3, 'admin', now(), now(), now())`, uuid.New(), orgID, adminID)
			return map[string]map[string]any{
				"admin": {"user_id": adminID.String(), "email": "venue-invite-admin@example.com", "org_id": orgID.String(), "role": "admin"},
			}
		},
	})

	requests := []venueoracle.Request{
		// request_metadata is compared row-for-row below (audit_logs), so
		// this request sets explicit, stable User-Agent/X-Forwarded-For/
		// X-Request-ID headers: leaving them unset would let each plane's
		// own default HTTP client identity (httpx TestClient's "testclient"
		// vs Go's http.Client's "Go-http-client/1.1" and 127.0.0.1) leak
		// into the comparison, which is a test-harness artifact, not a
		// product difference -- see the users oracle's identical ruling.
		{Name: "create invite", Method: "POST", Path: "/api/v1/admin/orgs/" + orgID.String() + "/invites",
			Headers: map[string]string{
				"Authorization": "Bearer " + venue.Tokens["admin"], "Content-Type": "application/json",
				"User-Agent": "venue-oracle-test/1.0", "X-Forwarded-For": "203.0.113.42", "X-Request-ID": "venue-create-invite-req",
			},
			Body: venueoracle.B64(`{"email":"invitee@example.com","role":"member"}`)},
		// Unauthenticated + malformed body: FastAPI validates the pydantic
		// body parameter before the auth Depends() ever runs, so this is a
		// 422 on both planes, never a 401 -- see the impersonation and users
		// oracles' identical case for the P1 this pins.
		{Name: "unauthenticated malformed body", Method: "POST", Path: "/api/v1/admin/orgs/" + orgID.String() + "/invites",
			Headers: map[string]string{"Content-Type": "application/json"}, Body: venueoracle.B64(`{`)},
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
				body = redactField(t, body, field)
			}
			return body
		},
	})
	t.Log(receipt)

	sourceRows := venueoracle.TableRows(t, ctx, venue.AdminURI(t, venue.SourceDB), orgInviteAuditQuery(orgID, adminID))
	goRows := venueoracle.TableRows(t, ctx, venue.AdminURI(t, venue.GoDB), orgInviteAuditQuery(orgID, adminID))
	if sourceRows != goRows {
		t.Errorf("audit_logs rows differ:\n python: %s\n go:     %s", sourceRows, goRows)
	}
}

func orgInviteAuditQuery(orgID, adminID uuid.UUID) string {
	return fmt.Sprintf(`SELECT org_id, user_id, action, resource_type, status, changes, request_metadata
FROM audit_logs WHERE org_id = '%s' AND user_id = '%s' ORDER BY created_at`, orgID, adminID)
}
