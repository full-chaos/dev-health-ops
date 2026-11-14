//go:build integration

package apiservice

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// CHAOS-13603: PATCH /api/v1/orgs/me is judged as org.write and keeps its refusal text. On a real Postgres, for each caller
// class, the answer with the org cell on equals the answer with the cells off, status and body: an org admin updates the
// organization, every other caller (member, viewer, no membership row, a platform admin with no membership row, a
// platform admin who is a member) is refused with the handler's own text (product rule 1: a platform admin never needs a
// membership, and gets the legacy answer, not a wider or stricter one).
func TestLegacyNoRowAndPlatformAdminAnswersOrgsMePatchVenueOracle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		GoOnly: true, Root: venueRoot(), Logger: quietLogger(),
		Seed: func(t *testing.T, ctx context.Context, admin *pgxpool.Pool, _ *venueoracle.Venue) map[string]map[string]any {
			t.Helper()
			exec := func(query string, args ...any) {
				t.Helper()
				if _, err := admin.Exec(ctx, query, args...); err != nil {
					t.Fatalf("seed: %v\n%s", err, query)
				}
			}
			exec(`INSERT INTO organizations (id, slug, name, tier, managed_by, is_active, created_at, updated_at)
VALUES ($1, 'orgs-me-patch', 'Orgs Me Patch', 'enterprise', 'stripe', true, now(), now())`, probeOrg)
			for _, user := range []struct {
				id    any
				email string
				super bool
			}{
				{probeUser, "admin@example.test", false}, {probeMember, "member@example.test", false},
				{usersCellViewer, "viewer@example.test", false}, {cellPlatformNoRow, "pn@example.test", true},
				{cellPlatformMember, "pm@example.test", true}, {deniedCellNoRowUser, "norow@example.test", false},
			} {
				exec(`INSERT INTO users (id, email, full_name, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, $2, true, true, $3, 0, now(), now())`, user.id, user.email, user.super)
			}
			for _, m := range []struct {
				user any
				role string
			}{{probeUser, "admin"}, {probeMember, "member"}, {usersCellViewer, "viewer"}, {cellPlatformMember, "member"}} {
				exec(`INSERT INTO memberships (id, user_id, org_id, role, joined_at, created_at) VALUES (gen_random_uuid(), $1, $2, $3, now(), now())`, m.user, probeOrg, m.role)
			}
			return nil
		},
	})
	pool, err := pgxpool.New(ctx, venue.GoAPIDatabaseURI(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	routes := func(_ *testing.T, deps Deps) []httpapi.Route {
		deps.Pool = pool
		return Routes(deps, quietLogger())
	}
	scoped := func(scope *policy.Scope) { scope.RefuseLegacyImpersonation(true) }
	pdp := newCellPDP(t)
	legacy, legacyMinter := adminConfigServerScoped(t, nil, withDeniedCellCallers, routes, scoped)
	enforced, enforcedMinter := adminConfigServerScoped(t, func(t *testing.T) *policy.Enforcer {
		return newCellEnforcer(t, pdp, "org", false)
	}, withDeniedCellCallers, routes, scoped)
	offClasses, onClasses := denyClasses(t, legacyMinter), denyClasses(t, enforcedMinter)
	const key = "PATCH /api/v1/orgs/me"
	denied := legacyDenyBodies[key]
	for i := range onClasses {
		name := onClasses[i].name
		switch name {
		case "retired HS256 token", "service key bearer", "anonymous":
			continue
		}
		pdp.registerUsers(judgedOf(onClasses))
		pdp.reset("evaluate")
		off := denyDo(legacy, key, offClasses[i].token, offClasses[i].headers)
		on := denyDo(enforced, key, onClasses[i].token, onClasses[i].headers)
		t.Logf("OBSERVED %s legacy %d %s", name, off.status, strings.TrimSpace(off.body))
		t.Logf("OBSERVED %s enforced %d %s", name, on.status, strings.TrimSpace(on.body))
		if on.status != off.status || on.body != off.body {
			t.Errorf("%s: cells on %d %s, cells off %d %s", name, on.status, strings.TrimSpace(on.body), off.status, strings.TrimSpace(off.body))
		}
		switch name {
		case "org member", "org viewer", "platform admin, no membership", "platform admin + org member", noRowClass:
			if off.status != http.StatusForbidden || strings.TrimSpace(off.body) != denied {
				t.Errorf("%s legacy: %d %s, want 403 %s", name, off.status, strings.TrimSpace(off.body), denied)
			}
		case "org admin":
			if off.status != http.StatusOK {
				t.Errorf("%s legacy: %d %s, want 200", name, off.status, strings.TrimSpace(off.body))
			}
		}
	}
	venueoracle.WriteGoOnlyProof(t, "PATCH orgs/me answers every caller class the same with the org cell on and off, and a platform admin with no membership row keeps the legacy 403")
}
