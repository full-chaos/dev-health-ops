//go:build integration

package admin_test

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

const raceOrgs = 8

// TestLocalMemberRoleChangeLastOwnerGuard holds the local (org provisioning
// off) member role change to the last-owner guard and the same-role no-op.
func TestLocalMemberRoleChangeLastOwnerGuard(t *testing.T) {
	ctx := context.Background()
	id := func(name string) uuid.UUID {
		return uuid.MustParse(venueoracle.StableUUID("member-role-guard/" + name))
	}
	orgA, orgB := id("org-a"), id("org-b")
	ownerA, ownerB, memberA, superID := id("owner-a"), id("owner-b"), id("member-a"), id("super")
	old := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)

	const jwtKey = "venue-oracle-member-role-guard-32-bytes-key!!"
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
			}{{orgA, "role-guard-a", "Role Guard A"}, {orgB, "role-guard-b", "Role Guard B"}} {
				exec(`INSERT INTO organizations (id, slug, name, tier, managed_by, is_active, created_at, updated_at)
VALUES ($1, $2, $3, 'enterprise', 'stripe', true, now(), now())`, o.id, o.slug, o.name)
			}
			for _, u := range []struct {
				id    uuid.UUID
				email string
				super bool
			}{{ownerA, "guard-owner-a@example.com", false}, {ownerB, "guard-owner-b@example.com", false},
				{memberA, "guard-member-a@example.com", false}, {superID, "guard-super@example.com", true}} {
				exec(`INSERT INTO users (id, email, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, true, true, $3, 0, now(), now())`, u.id, u.email, u.super)
			}
			member := func(org, user uuid.UUID, role string) {
				exec(`INSERT INTO memberships (id, org_id, user_id, role, joined_at, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $5, $5)`, uuid.New(), org, user, role, old)
			}
			for i := 0; i < raceOrgs; i++ {
				exec(`INSERT INTO organizations (id, slug, name, tier, managed_by, is_active, created_at, updated_at)
VALUES ($1, $2, $3, 'enterprise', 'stripe', true, now(), now())`, id(fmt.Sprintf("race-org-%d", i)), fmt.Sprintf("role-guard-race-%d", i), "Role Guard Race")
				for j := 0; j < 2; j++ {
					u := id(fmt.Sprintf("race-user-%d-%d", i, j))
					exec(`INSERT INTO users (id, email, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, true, true, false, 0, now(), now())`, u, fmt.Sprintf("guard-race-%d-%d@example.com", i, j))
					member(id(fmt.Sprintf("race-org-%d", i)), u, "owner")
				}
			}
			member(orgA, ownerA, "owner")
			member(orgA, memberA, "member")
			member(orgB, ownerB, "owner")
			return map[string]map[string]any{
				"super": {"user_id": superID.String(), "email": "guard-super@example.com", "is_superuser": true},
			}
		},
	})
	base, pool := startGoServer(t, ctx, venue, jwtKey)

	do := func(method string, org, user uuid.UUID, body *string) venueoracle.Response {
		t.Helper()
		return venueoracle.Do(t, base, venueoracle.Request{
			Name: method + " member", Method: method,
			Path:    "/api/v1/admin/orgs/" + org.String() + "/members/" + user.String(),
			Headers: map[string]string{"Authorization": "Bearer " + venue.Tokens["super"], "Content-Type": "application/json"},
			Body:    body,
		})
	}
	role := func(org, user uuid.UUID) (string, time.Time) {
		t.Helper()
		var r string
		var at time.Time
		if err := pool.QueryRow(ctx, `SELECT role, updated_at FROM memberships WHERE org_id = $1 AND user_id = $2`, org, user).Scan(&r, &at); err != nil {
			t.Fatalf("membership read: %v", err)
		}
		return r, at
	}
	owners := func(org uuid.UUID) int {
		t.Helper()
		var n int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM memberships WHERE org_id = $1 AND role = 'owner'`, org).Scan(&n); err != nil {
			t.Fatalf("owner count: %v", err)
		}
		return n
	}

	t.Run("cell_b_same_role_is_a_noop", func(t *testing.T) {
		_, before := role(orgA, memberA)
		resp := do(http.MethodPatch, orgA, memberA, venueoracle.B64(`{"role":"member"}`))
		r, after := role(orgA, memberA)
		if resp.Status != http.StatusOK || r != "member" || !after.Equal(before) {
			t.Fatalf("same-role change: status=%d role=%s updated_at before=%s after=%s", resp.Status, r, before, after)
		}
	})

	t.Run("cell_a_demote_last_owner_refused", func(t *testing.T) {
		resp := do(http.MethodPatch, orgA, ownerA, venueoracle.B64(`{"role":"member"}`))
		r, _ := role(orgA, ownerA)
		n := owners(orgA)
		if resp.Status != http.StatusBadRequest || r != "owner" || n != 1 {
			t.Fatalf("last owner demotion: status=%d role=%s owners=%d body=%s", resp.Status, r, n, resp.Body)
		}
	})

	t.Run("cell_c_remove_last_owner_local_route", func(t *testing.T) {
		resp := do(http.MethodDelete, orgB, ownerB, nil)
		n := owners(orgB)
		if resp.Status != http.StatusBadRequest || n != 1 {
			t.Fatalf("remove last owner: status=%d owners=%d body=%s", resp.Status, n, resp.Body)
		}
	})

	t.Run("cell_d_concurrent_demotes_leave_one_owner", func(t *testing.T) {
		for i := 0; i < raceOrgs; i++ {
			org := id(fmt.Sprintf("race-org-%d", i))
			statuses := make([]int, 2)
			start := make(chan struct{})
			var wg sync.WaitGroup
			for j := 0; j < 2; j++ {
				wg.Add(1)
				go func(j int) {
					defer wg.Done()
					<-start
					statuses[j] = do(http.MethodPatch, org, id(fmt.Sprintf("race-user-%d-%d", i, j)), venueoracle.B64(`{"role":"member"}`)).Status
				}(j)
			}
			close(start)
			wg.Wait()
			if n := owners(org); n != 1 {
				t.Fatalf("org %d: owners=%d statuses=%v, want 1 owner", i, n, statuses)
			}
		}
	})
}
