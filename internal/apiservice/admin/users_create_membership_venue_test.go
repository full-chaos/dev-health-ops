//go:build integration

package admin_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// TestOrgScopedCreateUserJoinsTheOrgVenueOracle pins CHAOS-8969: a user an
// org admin adds through POST /api/v1/admin/users must be a member of that
// organisation, because the org Users list reads users JOIN memberships.
func TestOrgScopedCreateUserJoinsTheOrgVenueOracle(t *testing.T) {
	ctx := context.Background()
	id := func(name string) uuid.UUID {
		return uuid.MustParse(venueoracle.StableUUID("CHAOS-8969/create-user/" + name))
	}
	orgID, otherOrgID := id("org"), id("other-org")
	adminID, superID := id("admin"), id("super")

	const jwtKey = "venue-oracle-create-user-membership-32-bytes!!"
	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		Root:   repoRoot(t),
		GoOnly: true,
		JWTKey: jwtKey,
		Seed: func(t *testing.T, ctx context.Context, admin *pgxpool.Pool, _ *venueoracle.Venue) map[string]map[string]any {
			t.Helper()
			exec := func(query string, args ...any) {
				t.Helper()
				if _, err := admin.Exec(ctx, query, args...); err != nil {
					t.Fatalf("seed: %v\n%s", err, query)
				}
			}
			for _, o := range []struct {
				id         uuid.UUID
				slug, name string
			}{{orgID, "create-user-org", "Create User Org"}, {otherOrgID, "create-user-other", "Other"}} {
				exec(`INSERT INTO organizations (id, slug, name, tier, managed_by, is_active, created_at, updated_at)
VALUES ($1, $2, $3, 'enterprise', 'stripe', true, now(), now())`, o.id, o.slug, o.name)
			}
			exec(`INSERT INTO users (id, email, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, 'create-admin@example.com', true, true, false, 0, now(), now())`, adminID)
			exec(`INSERT INTO users (id, email, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, 'create-super@example.com', true, true, true, 0, now(), now())`, superID)
			exec(`INSERT INTO memberships (id, org_id, user_id, role, joined_at, created_at, updated_at)
VALUES ($1, $2, $3, 'admin', now(), now(), now())`, uuid.New(), orgID, adminID)
			return map[string]map[string]any{
				"admin": {"user_id": adminID.String(), "email": "create-admin@example.com", "org_id": orgID.String(), "role": "admin"},
				"super": {"user_id": superID.String(), "email": "create-super@example.com", "is_superuser": true},
			}
		},
	})
	base, pool := startGoServer(t, ctx, venue, jwtKey)

	create := func(token, xOrgID, body string) venueoracle.Response {
		t.Helper()
		headers := map[string]string{"Authorization": "Bearer " + venue.Tokens[token], "Content-Type": "application/json"}
		if xOrgID != "" {
			headers["X-Org-Id"] = xOrgID
		}
		return venueoracle.Do(t, base, venueoracle.Request{
			Name: "create user", Method: http.MethodPost, Path: "/api/v1/admin/users",
			Headers: headers, Body: venueoracle.B64(body),
		})
	}
	orgListEmails := func() map[string]bool {
		t.Helper()
		response := venueoracle.Do(t, base, venueoracle.Request{
			Name: "org users list", Method: http.MethodGet, Path: "/api/v1/admin/users",
			Headers: map[string]string{"Authorization": "Bearer " + venue.Tokens["admin"], "X-Org-Id": orgID.String()},
		})
		if response.Status != http.StatusOK {
			t.Fatalf("org users list status = %d: %s", response.Status, response.Body)
		}
		var users []map[string]any
		if err := json.Unmarshal([]byte(response.Body), &users); err != nil {
			t.Fatalf("decode org users list: %v", err)
		}
		emails := map[string]bool{}
		for _, u := range users {
			emails[u["email"].(string)] = true
		}
		return emails
	}
	membershipRole := func(email string) string {
		t.Helper()
		var role string
		err := pool.QueryRow(ctx, `SELECT coalesce((SELECT m.role FROM memberships m JOIN users u ON u.id = m.user_id
WHERE u.email = $1 AND m.org_id = $2), '')`, email, orgID).Scan(&role)
		if err != nil {
			t.Fatalf("membership read: %v", err)
		}
		return role
	}
	userCount := func(email string) int {
		t.Helper()
		var n int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM users WHERE email = $1`, email).Scan(&n); err != nil {
			t.Fatalf("users read: %v", err)
		}
		return n
	}

	t.Run("org admin add with a role is listed with that role", func(t *testing.T) {
		response := create("admin", orgID.String(), `{"email":"added-admin@example.com","full_name":"Added Admin","role":"admin"}`)
		if response.Status != http.StatusCreated {
			t.Fatalf("status = %d: %s", response.Status, response.Body)
		}
		if !orgListEmails()["added-admin@example.com"] {
			t.Fatalf("the added user is not in the org Users list")
		}
		if role := membershipRole("added-admin@example.com"); role != "admin" {
			t.Fatalf("membership role = %q, want admin", role)
		}
	})

	t.Run("org admin add without a role joins as member", func(t *testing.T) {
		response := create("admin", orgID.String(), `{"email":"added-member@example.com"}`)
		if response.Status != http.StatusCreated {
			t.Fatalf("status = %d: %s", response.Status, response.Body)
		}
		if !orgListEmails()["added-member@example.com"] {
			t.Fatalf("the added user is not in the org Users list")
		}
		if role := membershipRole("added-member@example.com"); role != "member" {
			t.Fatalf("membership role = %q, want member", role)
		}
	})

	t.Run("org claim without X-Org-Id joins the caller's org", func(t *testing.T) {
		response := create("admin", "", `{"email":"added-claim@example.com"}`)
		if response.Status != http.StatusCreated {
			t.Fatalf("status = %d: %s", response.Status, response.Body)
		}
		if role := membershipRole("added-claim@example.com"); role != "member" {
			t.Fatalf("membership role = %q, want member", role)
		}
	})

	t.Run("an unknown or owner role is refused and writes no user", func(t *testing.T) {
		for _, role := range []string{"owner", "boss"} {
			email := "refused-" + role + "@example.com"
			response := create("admin", orgID.String(), `{"email":"`+email+`","role":"`+role+`"}`)
			if response.Status != http.StatusBadRequest {
				t.Fatalf("role %q status = %d, want 400: %s", role, response.Status, response.Body)
			}
			if n := userCount(email); n != 0 {
				t.Fatalf("role %q left %d user rows, want 0", role, n)
			}
		}
	})

	t.Run("an admin of one org cannot add into another org", func(t *testing.T) {
		response := create("admin", otherOrgID.String(), `{"email":"cross-org@example.com"}`)
		if response.Status != http.StatusForbidden {
			t.Fatalf("status = %d, want 403: %s", response.Status, response.Body)
		}
		if n := userCount("cross-org@example.com"); n != 0 {
			t.Fatalf("cross-org add left %d user rows, want 0", n)
		}
	})

	t.Run("a superuser platform create writes no membership", func(t *testing.T) {
		response := create("super", "", `{"email":"platform-user@example.com"}`)
		if response.Status != http.StatusCreated {
			t.Fatalf("status = %d: %s", response.Status, response.Body)
		}
		var n int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM memberships m JOIN users u ON u.id = m.user_id
WHERE u.email = 'platform-user@example.com'`).Scan(&n); err != nil {
			t.Fatalf("membership read: %v", err)
		}
		if n != 0 {
			t.Fatalf("platform create wrote %d memberships, want 0", n)
		}
	})

	t.Run("a role without an organisation is refused", func(t *testing.T) {
		response := create("super", "", `{"email":"platform-role@example.com","role":"admin"}`)
		if response.Status != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400: %s", response.Status, response.Body)
		}
		if n := userCount("platform-role@example.com"); n != 0 {
			t.Fatalf("left %d user rows, want 0", n)
		}
	})

	venueoracle.WriteGoOnlyProof(t, "an org-scoped user create writes the user and its org membership in one transaction; refused roles and cross-org adds write nothing; a superuser platform create writes no membership")
}
