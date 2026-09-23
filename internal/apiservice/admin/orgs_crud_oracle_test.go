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

// TestOrgCRUDMatchesThePythonAPI is the venue-oracle proof for CHAOS-6305's
// org and member routes: org list/get/create/patch, member
// list/add/patch-role/remove, and transfer-ownership. User routes have
// their own oracle test in CHAOS-6304 (users_oracle_test.go); this test
// seeds users directly by SQL only as membership targets. DELETE
// /orgs/{org_id} is out of scope (stubbed 501, see orgs.go's
// deleteOrganizationStub doc comment).
func TestOrgCRUDMatchesThePythonAPI(t *testing.T) {
	ctx := context.Background()
	root := repoRoot(t)
	const jwtKey = "venue-oracle-test-secret-key-for-org-crud-flow-32-bytes!!!"

	orgID := uuid.New()
	ownerID := uuid.New()
	memberID := uuid.New()
	newMemberID := uuid.New()
	superID := uuid.New()

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
VALUES ($1, 'venue-org-3', 'Venue Org 3', 'community', 'stripe', true, now(), now())`, orgID)
			for _, row := range []struct {
				id    uuid.UUID
				email string
				super bool
			}{
				{ownerID, "venue-owner@example.com", false},
				{memberID, "venue-member@example.com", false},
				{newMemberID, "venue-newmember@example.com", false},
				{superID, "venue-super@example.com", true},
			} {
				exec(`INSERT INTO users (id, email, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, true, true, $3, 0, now(), now())`, row.id, row.email, row.super)
			}
			exec(`INSERT INTO memberships (id, org_id, user_id, role, joined_at, created_at, updated_at)
VALUES ($1, $2, $3, 'owner', now(), now(), now())`, uuid.New(), orgID, ownerID)
			exec(`INSERT INTO memberships (id, org_id, user_id, role, joined_at, created_at, updated_at)
VALUES ($1, $2, $3, 'member', now(), now(), now())`, uuid.New(), orgID, memberID)
			exec(`INSERT INTO memberships (id, org_id, user_id, role, joined_at, created_at, updated_at)
VALUES ($1, $2, $3, 'member', now(), now(), now())`, uuid.New(), orgID, newMemberID)
			return map[string]map[string]any{
				"owner": {"user_id": ownerID.String(), "email": "venue-owner@example.com", "org_id": orgID.String(), "role": "owner"},
				"super": {"user_id": superID.String(), "email": "venue-super@example.com", "is_superuser": true},
			}
		},
	})

	bearer := func(name string) string { return "Bearer " + venue.Tokens[name] }
	jsonHeaders := func(name string) map[string]string {
		return map[string]string{"Authorization": bearer(name), "Content-Type": "application/json"}
	}
	authHeaders := func(name string) map[string]string { return map[string]string{"Authorization": bearer(name)} }

	requests := []venueoracle.Request{
		// Organizations (superuser only).
		{Name: "list orgs", Method: "GET", Path: "/api/v1/admin/orgs", Headers: authHeaders("super")},
		{Name: "get org", Method: "GET", Path: "/api/v1/admin/orgs/" + orgID.String(), Headers: authHeaders("super")},
		{Name: "get org as non-superuser refused", Method: "GET", Path: "/api/v1/admin/orgs/" + orgID.String(), Headers: authHeaders("owner")},
		{Name: "create org", Method: "POST", Path: "/api/v1/admin/orgs", Headers: jsonHeaders("super"),
			Body: venueoracle.B64(`{"name":"A New Org"}`)},
		{Name: "patch org", Method: "PATCH", Path: "/api/v1/admin/orgs/" + orgID.String(), Headers: jsonHeaders("super"),
			Body: venueoracle.B64(`{"description":"updated description"}`)},
		{Name: "patch org not found", Method: "PATCH", Path: "/api/v1/admin/orgs/" + uuid.New().String(), Headers: jsonHeaders("super"),
			Body: venueoracle.B64(`{"description":"x"}`)},

		// Members.
		{Name: "list members", Method: "GET", Path: "/api/v1/admin/orgs/" + orgID.String() + "/members", Headers: authHeaders("owner")},
		{Name: "add member missing user", Method: "POST", Path: "/api/v1/admin/orgs/" + orgID.String() + "/members", Headers: jsonHeaders("owner"),
			Body: venueoracle.B64(fmt.Sprintf(`{"user_id":%q,"role":"member"}`, uuid.New().String()))},
		{Name: "add member already exists", Method: "POST", Path: "/api/v1/admin/orgs/" + orgID.String() + "/members", Headers: jsonHeaders("owner"),
			Body: venueoracle.B64(fmt.Sprintf(`{"user_id":%q,"role":"member"}`, memberID.String()))},
		{Name: "patch member role", Method: "PATCH", Path: "/api/v1/admin/orgs/" + orgID.String() + "/members/" + newMemberID.String(),
			Headers: jsonHeaders("owner"), Body: venueoracle.B64(`{"role":"admin"}`)},
		{Name: "patch member role invalid", Method: "PATCH", Path: "/api/v1/admin/orgs/" + orgID.String() + "/members/" + newMemberID.String(),
			Headers: jsonHeaders("owner"), Body: venueoracle.B64(`{"role":"not-a-role"}`)},
		{Name: "remove member", Method: "DELETE", Path: "/api/v1/admin/orgs/" + orgID.String() + "/members/" + newMemberID.String(),
			Headers: authHeaders("owner")},
		{Name: "remove last owner refused", Method: "DELETE", Path: "/api/v1/admin/orgs/" + orgID.String() + "/members/" + ownerID.String(),
			Headers: authHeaders("owner")},
	}
	python := venue.ServePython(t, requests)
	goBase, _ := startGoServer(t, ctx, venue, jwtKey)

	receipt := venueoracle.Diff(t, goBase, requests, python, venueoracle.DiffOptions{
		Normalize: func(request venueoracle.Request, body string) string {
			for _, field := range []string{"id", "created_at", "updated_at", "expires_at"} {
				body = redactField(body, field)
			}
			return body
		},
	})
	t.Log(receipt)

	// transfer-ownership is NOT diffed against Python: it is the one ruled,
	// intentional shape divergence in this PR (team-lead: "Go serves the
	// web's shape... no Python follow-up"). The web calls
	// POST /orgs/{org_id}/transfer-ownership with no from_user_id path
	// segment; Python's own route is
	// POST /orgs/{org_id}/transfer-ownership/{from_user_id}, so that path on
	// the Python plane matches NO route at all and 404s at the router, never
	// reaching transfer_ownership's own logic -- proving the Python path is
	// genuinely dead code, not a route this test can honestly diff. Assert
	// only the Go plane's own documented behavior: resolve the org's current
	// owner server-side, then fail the same way Python's transfer_ownership
	// would for a non-member target.
	transferToNonMember := venueoracle.Request{
		Name: "transfer ownership to non-member (go-only, web shape)", Method: "POST",
		Path:    "/api/v1/admin/orgs/" + orgID.String() + "/transfer-ownership",
		Headers: jsonHeaders("owner"), Body: venueoracle.B64(fmt.Sprintf(`{"new_owner_user_id":%q}`, uuid.New().String())),
	}
	if got := venueoracle.Do(t, goBase, transferToNonMember); got.Status != 400 || got.Body != `{"detail":"Target user is not a member"}` {
		t.Errorf("transfer ownership to non-member: got %d %s, want 400 Target user is not a member", got.Status, got.Body)
	}
}
