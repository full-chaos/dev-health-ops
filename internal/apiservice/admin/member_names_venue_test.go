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

// TestMemberListServesUserNamesVenueOracle runs the real, migrated Go API
// against Postgres. It is Go-only because the frozen Python member list has
// no user_name or user_email; TestOrgCRUDMatchesThePythonAPI still checks
// every unchanged member field against Python.
func TestMemberListServesUserNamesVenueOracle(t *testing.T) {
	ctx := context.Background()
	id := func(name string) uuid.UUID {
		return uuid.MustParse(venueoracle.StableUUID("CHAOS-8946/member-names/" + name))
	}
	orgID, otherOrgID := id("org"), id("other-org")
	superID, namedID, blankID, otherID := id("super"), id("named"), id("blank"), id("other-user")

	const jwtKey = "venue-oracle-member-names-key-32-bytes-long!!"
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
			}{{orgID, "member-names", "Member Names"}, {otherOrgID, "member-names-other", "Other"}} {
				exec(`INSERT INTO organizations (id, slug, name, tier, managed_by, is_active, created_at, updated_at)
VALUES ($1, $2, $3, 'enterprise', 'stripe', true, now(), now())`, o.id, o.slug, o.name)
			}
			user := func(userID uuid.UUID, email, fullName string, superuser bool) {
				exec(`INSERT INTO users (id, email, full_name, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, $3, true, true, $4, 0, now(), now())`, userID, email, fullName, superuser)
			}
			user(superID, "names-super@example.com", "Names Superuser", true)
			user(namedID, "named@example.com", "Named Member", false)
			user(blankID, "blank@example.com", "", false)
			user(otherID, "other-org-user@example.com", "Other Org User", false)
			member := func(userID, org uuid.UUID) {
				exec(`INSERT INTO memberships (id, org_id, user_id, role, joined_at, created_at, updated_at)
VALUES ($1, $2, $3, 'member', now(), now(), now())`, uuid.New(), org, userID)
			}
			member(namedID, orgID)
			member(blankID, orgID)
			member(otherID, otherOrgID)
			return map[string]map[string]any{
				"super": {"user_id": superID.String(), "email": "names-super@example.com", "is_superuser": true},
			}
		},
	})
	base, _ := startGoServer(t, ctx, venue, jwtKey)
	response := venueoracle.Do(t, base, venueoracle.Request{
		Name: "superadmin member list names", Method: http.MethodGet,
		Path:    "/api/v1/admin/orgs/" + orgID.String() + "/members",
		Headers: map[string]string{"Authorization": "Bearer " + venue.Tokens["super"]},
	})
	if response.Status != http.StatusOK {
		t.Fatalf("member list status = %d: %s", response.Status, response.Body)
	}
	var items []map[string]any
	if err := json.Unmarshal([]byte(response.Body), &items); err != nil {
		t.Fatalf("decode member list: %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("member list items = %d, want 2 (the other org's member is never listed)", len(items))
	}
	want := map[string]struct{ name, email any }{
		namedID.String(): {"Named Member", "named@example.com"},
		blankID.String(): {nil, "blank@example.com"},
	}
	for _, item := range items {
		w, ok := want[item["user_id"].(string)]
		if !ok {
			t.Fatalf("unexpected member %#v", item)
		}
		if item["user_name"] != w.name || item["user_email"] != w.email {
			t.Errorf("member %v names = %#v/%#v, want %#v/%#v", item["user_id"], item["user_name"], item["user_email"], w.name, w.email)
		}
		if item["user_email"] == "other-org-user@example.com" {
			t.Errorf("served another org's user: %#v", item)
		}
	}
	venueoracle.WriteGoOnlyProof(t, "the member list serves a user's name and e-mail only through a membership row of the requested organisation; a blank name stays null")
}
