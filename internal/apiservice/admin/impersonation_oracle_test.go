//go:build integration

package admin_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	valkeygo "github.com/valkey-io/valkey-go"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// TestImpersonationStartStopMatchesThePythonAPI is the venue-oracle proof
// for CHAOS-6250's impersonation routes: the REAL Python api
// (impersonation.py's start_impersonation/impersonation_status/
// stop_impersonation, through the full FastAPI app) and the REAL Go api
// (internal/apiservice/admin's impersonationRoutes) answer the identical
// request sequence against two copies of one seeded database, and the
// audit_logs rows either plane's writes produced compare equal.
func TestImpersonationStartStopMatchesThePythonAPI(t *testing.T) {
	ctx := context.Background()
	golden := venueoracle.OpenGolden(t, adminRunValuesGolden("impersonation", t.Name(), "5987024ab34598026aaeee851ff0f3bc025c4f1efd586f40c166c8dc223892be"))
	root := golden.PythonRoot(t, repoRoot(t))
	nextID := goldenIDs("imp")
	const jwtKey = "venue-oracle-test-secret-key-for-impersonation-flow-32bytes!"

	orgID := nextID()
	adminID := nextID()
	targetID := nextID()
	membershipID := nextID()
	// An admin whose cached session has already expired (the cache holds it for
	// 30 s from the moment it is written; the session's own expiry is past).
	expiredAdminID := nextID()
	// Targets that the start route refuses, each for its own reason
	// (impersonation.py: not found 404, inactive 400, superuser 403, no
	// membership 404).
	missingID := nextID()
	inactiveID := nextID()
	superuserID := nextID()
	noMembershipID := nextID()
	inactiveMembershipID := nextID()
	superuserMembershipID := nextID()

	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		Golden: golden,
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
VALUES ($1, 'venue-org', 'Venue Org', 'community', 'stripe', true, now(), now())`, orgID)
			exec(`INSERT INTO users (id, email, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, 'venue-admin@example.com', true, true, true, 0, now(), now())`, adminID)
			exec(`INSERT INTO users (id, email, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, 'venue-expired-admin@example.com', true, true, true, 0, now(), now())`, expiredAdminID)
			exec(`INSERT INTO users (id, email, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, 'venue-target@example.com', true, true, false, 0, now(), now())`, targetID)
			exec(`INSERT INTO memberships (id, org_id, user_id, role, joined_at, created_at, updated_at)
VALUES ($1, $2, $3, 'member', now(), now(), now())`, membershipID, orgID, targetID)
			for _, target := range []struct {
				id                        uuid.UUID
				email                     string
				active, superuser, member bool
				membershipID              uuid.UUID
			}{
				{inactiveID, "venue-inactive@example.com", false, false, true, inactiveMembershipID},
				{superuserID, "venue-superuser@example.com", true, true, true, superuserMembershipID},
				{noMembershipID, "venue-nomember@example.com", true, false, false, uuid.UUID{}},
			} {
				exec(`INSERT INTO users (id, email, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, $3, true, $4, 0, now(), now())`, target.id, target.email, target.active, target.superuser)
				if target.member {
					exec(`INSERT INTO memberships (id, org_id, user_id, role, joined_at, created_at, updated_at)
VALUES ($1, $2, $3, 'member', now(), now(), now())`, target.membershipID, orgID, target.id)
				}
			}
			return map[string]map[string]any{
				"admin":   {"user_id": adminID.String(), "email": "venue-admin@example.com", "is_superuser": true},
				"expired": {"user_id": expiredAdminID.String(), "email": "venue-expired-admin@example.com", "is_superuser": true},
			}
		},
	})

	// The branch only a cached session can reach: the status route reads the
	// session from the shared cache, and a cached session whose own expiry has
	// passed is "not impersonating" (impersonation.py: expires_at <= now). The
	// database lookup already refuses an expired row, so the entry is written
	// into BOTH planes' caches, in the form the cache writes it.
	expiredSession := fmt.Sprintf(`{"id":%q,"admin_user_id":%q,"target_user_id":%q,"target_org_id":%q,"target_role":"member","target_email":"venue-target@example.com","expires_at":"2020-01-01T00:00:00+00:00"}`,
		nextID().String(), expiredAdminID.String(), targetID.String(), orgID.String())
	for _, uri := range []string{venue.ValkeyURI, venue.PythonValkeyURI} {
		options, err := valkeygo.ParseURL(uri)
		if err != nil {
			t.Fatal(err)
		}
		client, err := valkeygo.NewClient(options)
		if err != nil {
			t.Fatal(err)
		}
		if err := client.Do(ctx, client.B().Set().Key("impersonation:active:"+expiredAdminID.String()).Value(expiredSession).Ex(10*time.Minute).Build()).Error(); err != nil {
			t.Fatalf("seed the expired session into the cache: %v", err)
		}
		client.Close()
	}

	requests := []venueoracle.Request{
		{Name: "status before", Method: "GET", Path: "/api/v1/admin/impersonate/status",
			Headers: map[string]string{"Authorization": "Bearer " + venue.Tokens["admin"]}},
		{Name: "status with an expired cached session", Method: "GET", Path: "/api/v1/admin/impersonate/status",
			Headers: map[string]string{"Authorization": "Bearer " + venue.Tokens["expired"]}},
		// Refusals of the start route, one per target, and a stop with no
		// session open (before any session exists).
		{Name: "stop with no active session", Method: "POST", Path: "/api/v1/admin/impersonate/stop",
			Headers: map[string]string{"Authorization": "Bearer " + venue.Tokens["admin"], "Content-Type": "application/json"}},
		{Name: "target not found", Method: "POST", Path: "/api/v1/admin/impersonate",
			Headers: map[string]string{"Authorization": "Bearer " + venue.Tokens["admin"], "Content-Type": "application/json"},
			Body:    venueoracle.B64(fmt.Sprintf(`{"target_user_id":%q}`, missingID.String()))},
		{Name: "target inactive", Method: "POST", Path: "/api/v1/admin/impersonate",
			Headers: map[string]string{"Authorization": "Bearer " + venue.Tokens["admin"], "Content-Type": "application/json"},
			Body:    venueoracle.B64(fmt.Sprintf(`{"target_user_id":%q}`, inactiveID.String()))},
		{Name: "target superuser", Method: "POST", Path: "/api/v1/admin/impersonate",
			Headers: map[string]string{"Authorization": "Bearer " + venue.Tokens["admin"], "Content-Type": "application/json"},
			Body:    venueoracle.B64(fmt.Sprintf(`{"target_user_id":%q}`, superuserID.String()))},
		{Name: "target with no membership", Method: "POST", Path: "/api/v1/admin/impersonate",
			Headers: map[string]string{"Authorization": "Bearer " + venue.Tokens["admin"], "Content-Type": "application/json"},
			Body:    venueoracle.B64(fmt.Sprintf(`{"target_user_id":%q}`, noMembershipID.String()))},
		{Name: "start", Method: "POST", Path: "/api/v1/admin/impersonate",
			Headers: map[string]string{"Authorization": "Bearer " + venue.Tokens["admin"], "Content-Type": "application/json"},
			Body:    venueoracle.B64(fmt.Sprintf(`{"target_user_id":%q}`, targetID.String()))},
		{Name: "status during", Method: "GET", Path: "/api/v1/admin/impersonate/status",
			Headers: map[string]string{"Authorization": "Bearer " + venue.Tokens["admin"]}},
		{Name: "stop", Method: "POST", Path: "/api/v1/admin/impersonate/stop",
			Headers: map[string]string{"Authorization": "Bearer " + venue.Tokens["admin"], "Content-Type": "application/json"}},
		{Name: "status after", Method: "GET", Path: "/api/v1/admin/impersonate/status",
			Headers: map[string]string{"Authorization": "Bearer " + venue.Tokens["admin"]}},
		{Name: "self-impersonate refused", Method: "POST", Path: "/api/v1/admin/impersonate",
			Headers: map[string]string{"Authorization": "Bearer " + venue.Tokens["admin"], "Content-Type": "application/json"},
			Body:    venueoracle.B64(fmt.Sprintf(`{"target_user_id":%q}`, adminID.String()))},
		// Unauthenticated + malformed body: FastAPI validates the pydantic
		// body parameter before the auth Depends() ever runs, so this is a
		// 422 on both planes, never a 401 (codex round pr2842-r1, P1: this
		// PR originally wrapped the whole handler in Guard first, so an
		// unauthenticated malformed body answered 401 instead).
		{Name: "unauthenticated malformed body", Method: "POST", Path: "/api/v1/admin/impersonate",
			Headers: map[string]string{"Content-Type": "application/json"},
			Body:    venueoracle.B64(`{`)},
	}
	python := golden.Python(t, venue, requests)
	goBase, _ := startGoServer(t, ctx, venue, jwtKey)

	receipt := venueoracle.Diff(t, goBase, requests, python, venueoracle.DiffOptions{
		Golden: golden,
		Normalize: func(request venueoracle.Request, body string) string {
			// expires_at is wall-clock-derived (now + IMPERSONATION_TTL_MINUTES)
			// and the two planes mint it microseconds apart; it is not a
			// value either route asserts equal to the millisecond, so this
			// blanks the field's VALUE, not its presence, matching every
			// other ruled Normalize in this repo's oracle suites.
			return redactField(t, body, "expires_at")
		},
	})
	t.Log(receipt)

	goRows := venueoracle.TableRows(t, ctx, venue.AdminURI(t, venue.GoDB), impersonationAuditQuery(adminID, targetID))
	if source := golden.CompareRows(t, "audit_logs rows", func() string {
		return venueoracle.TableRows(t, ctx, venue.AdminURI(t, venue.SourceDB), impersonationAuditQuery(adminID, targetID))
	}, goRows); source == "" {
		t.Error("audit_logs: the query matched no rows on the Python plane; the comparison proves nothing")
	}
	compareAuditJSONWithSpacingGap(t, ctx, golden, venue, fmt.Sprintf("user_id = '%s' AND resource_id = '%s'", adminID, targetID), "request_metadata")
	golden.Finish(t)
}

func impersonationAuditQuery(adminID, targetID uuid.UUID) string {
	return fmt.Sprintf(`SELECT org_id, user_id, action, resource_type, resource_id, status
FROM audit_logs WHERE user_id = '%s' AND resource_id = '%s' ORDER BY created_at`, adminID, targetID)
}
