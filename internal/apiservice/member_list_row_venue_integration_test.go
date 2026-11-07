//go:build integration

package apiservice

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// The admin member list answers each caller the same on the legacy path and with the members cell on, over the real
// handler and a migrated Postgres, the decision point being the real evaluator: a member and a viewer get the legacy
// 403; an admin, an owner and a platform admin with no membership row (D6773) get 200 and the org's four members.
func TestAdminMemberListAnswersAsLegacyVenueOracle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		GoOnly: true, Root: venueRoot(), JWTKey: venueKey, Logger: quietLogger(),
		Seed: func(t *testing.T, ctx context.Context, admin *pgxpool.Pool, _ *venueoracle.Venue) map[string]map[string]any {
			t.Helper()
			exec := func(query string, args ...any) {
				t.Helper()
				if _, err := admin.Exec(ctx, query, args...); err != nil {
					t.Fatalf("seed: %v\n%s", err, query)
				}
			}
			exec(`INSERT INTO organizations (id, slug, name, tier, managed_by, is_active, created_at, updated_at)
VALUES ($1, 'member-list', 'Member List', 'enterprise', 'stripe', true, now(), now())`, probeOrg)
			for _, user := range []struct {
				id        uuid.UUID
				email     string
				superuser bool
			}{
				{probeMember, "member@example.test", false}, {memberListViewer, "viewer@example.test", false},
				{probeUser, "admin@example.test", false}, {memberListOwner, "owner@example.test", false},
				{cellPlatformNoRow, "pn@example.test", true},
			} {
				exec(`INSERT INTO users (id, email, full_name, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, $2, true, true, $3, 0, now(), now())`, user.id, user.email, user.superuser)
			}
			for user, role := range map[uuid.UUID]string{probeMember: "member", memberListViewer: "viewer", probeUser: "admin", memberListOwner: "owner"} {
				exec(`INSERT INTO memberships (id, org_id, user_id, role, joined_at, created_at, updated_at)
VALUES ($1, $2, $3, $4, now(), now(), now())`, uuid.New(), probeOrg, user, role)
			}
			return nil
		},
	})
	pool, err := pgxpool.New(ctx, venue.GoAPIDatabaseURI(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	serve := func(enforcer func(*testing.T) *policy.Enforcer) (http.Handler, *eddsaMinter) {
		return adminConfigServerOver(t, enforcer, withMemberListCallers, func(_ *testing.T, deps Deps) []httpapi.Route {
			deps.Pool = pool
			return Routes(deps, quietLogger())
		})
	}
	pdp := newCellPDP(t)
	legacy, legacyMinter := serve(nil)
	enforced, enforcedMinter := serve(func(t *testing.T) *policy.Enforcer { return newCellEnforcer(t, pdp, "members", false) })
	legacyCallers, enforcedCallers := memberListCallers(legacyMinter), memberListCallers(enforcedMinter)
	pdp.register(enforcedCallers)
	for name, refused := range map[string]bool{"org member": true, "viewer": true, "org admin": false, "owner": false, "platform admin + no membership": false} {
		pdp.reset("evaluate")
		off := membersDo(legacy, adminMemberListKey, legacyCallers[name].token)
		on := membersDo(enforced, adminMemberListKey, enforcedCallers[name].token)
		leaks := strings.Contains(on.body, "@example.test")
		t.Logf("OBSERVED %s: legacy %d %.80s; enforced %d %.80s; reasons %v; enforced body holds the member list: %v", name, off.status, off.body, on.status, on.body, pdp.reasonsSeen(), leaks)
		if off.status != on.status || off.body != on.body {
			t.Errorf("%s: legacy %d %s, enforced %d %s, want the same answer", name, off.status, off.body, on.status, on.body)
		}
		if refused {
			if leaks {
				t.Errorf("%s: the enforced answer holds the member list", name)
			}
			if on.status != http.StatusForbidden || on.body != adminRequiredBody {
				t.Errorf("%s: enforced %d %s, want the legacy 403 %s", name, on.status, on.body, adminRequiredBody)
			}
			continue
		}
		var members []map[string]any
		if on.status != http.StatusOK || json.Unmarshal([]byte(on.body), &members) != nil || len(members) != 4 {
			t.Errorf("%s: enforced %d %s, want 200 and the org's 4 members", name, on.status, on.body)
		}
	}
	venueoracle.WriteGoOnlyProof(t, "the admin member list answers a member, viewer, admin, owner and no-row platform admin the same with the members cell on as on the legacy path")
}
