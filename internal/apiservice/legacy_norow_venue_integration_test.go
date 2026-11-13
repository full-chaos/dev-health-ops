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
	"github.com/full-chaos/dev-health-ops/internal/auth/sessionid"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// CHAOS-13604 M-1, a named answer change (acp ruling (b)): for an active user with a live session, a token organization A
// and NO membership row in A, GET /api/v1/orgs/me answers the legacy 200 with the organization's profile with the cells off
// (recorded on a real Postgres, run 3, 11-12) and 403 "Access denied" with the org cell on and the real evaluator behind the
// decision point, asked once on the token organization.
func TestLegacyNoRowTokenOrgAnswersVenueOracle(t *testing.T) {
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
VALUES ($1, 'no-row', 'No Row', 'enterprise', 'stripe', true, now(), now())`, probeOrg)
			exec(`INSERT INTO users (id, email, full_name, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, 'norow@example.test', 'norow@example.test', true, true, false, 0, now(), now())`, deniedCellNoRowUser)
			return nil
		},
	})
	pool, err := pgxpool.New(ctx, venue.GoAPIDatabaseURI(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	serve := func(enforcer func(*testing.T) *policy.Enforcer) (http.Handler, *eddsaMinter) {
		return adminConfigServerOver(t, enforcer, withDeniedCellCallers, func(_ *testing.T, deps Deps) []httpapi.Route {
			deps.Pool = pool
			return Routes(deps, quietLogger())
		})
	}
	pdp := newCellPDP(t)
	legacy, legacyMinter := serve(nil)
	enforced, enforcedMinter := serve(func(t *testing.T) *policy.Enforcer { return newCellEnforcer(t, pdp, "org", false) })
	token := func(m *eddsaMinter) string {
		return m.mint(deniedCellNoRowUser, map[string]any{"sid": sessionid.Wire(deniedCellNoRowSID)})
	}
	legacyToken, enforcedToken := token(legacyMinter), token(enforcedMinter)
	pdp.registerUsers([]usersCaller{{name: "no row", token: enforcedToken, snap: cellSnapshot{}, judged: true}})
	for _, key := range []string{"GET /api/v1/orgs/me"} {
		pdp.reset("evaluate")
		off := handlerDecidedDo(legacy, key, legacyToken, nil)
		on := handlerDecidedDo(enforced, key, enforcedToken, nil)
		t.Logf("OBSERVED %s legacy %d %s", key, off.status, strings.TrimSpace(off.body))
		t.Logf("OBSERVED %s enforced %d %s", key, on.status, strings.TrimSpace(on.body))
		if off.status != http.StatusOK || !strings.Contains(off.body, `"id":"`+probeOrg.String()+`"`) || !strings.Contains(off.body, `"slug":"no-row"`) {
			t.Errorf("%s legacy: %d %s, want 200 with the seeded organization's profile", key, off.status, off.body)
		}
		if on.status != http.StatusForbidden || strings.TrimSpace(on.body) != `{"detail":"Access denied"}` {
			t.Errorf("%s enforced: %d %s, want 403 Access denied", key, on.status, on.body)
		}
		if calls := pdp.calls(); len(calls) != 1 || calls[0].Action != "org.read" || calls[0].OrganizationID != probeOrg.String() {
			t.Errorf("%s: decision point calls %+v, want one org.read on the token organization", key, calls)
		}
	}
	venueoracle.WriteGoOnlyProof(t, "a token-org caller with no membership row gets the legacy 200 profile of orgs/me and, with the org cell on, 403 Access denied")
}
