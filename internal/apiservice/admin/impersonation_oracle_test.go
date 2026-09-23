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

// TestImpersonationStartStopMatchesThePythonAPI is the venue-oracle proof
// for CHAOS-6250's impersonation routes: the REAL Python api
// (impersonation.py's start_impersonation/impersonation_status/
// stop_impersonation, through the full FastAPI app) and the REAL Go api
// (internal/apiservice/admin's impersonationRoutes) answer the identical
// request sequence against two copies of one seeded database, and the
// audit_logs rows either plane's writes produced compare equal.
func TestImpersonationStartStopMatchesThePythonAPI(t *testing.T) {
	ctx := context.Background()
	root := repoRoot(t)
	const jwtKey = "venue-oracle-test-secret-key-for-impersonation-flow-32bytes!"

	orgID := uuid.New()
	adminID := uuid.New()
	targetID := uuid.New()

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
VALUES ($1, 'venue-org', 'Venue Org', 'community', 'stripe', true, now(), now())`, orgID)
			exec(`INSERT INTO users (id, email, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, 'venue-admin@example.com', true, true, true, 0, now(), now())`, adminID)
			exec(`INSERT INTO users (id, email, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, 'venue-target@example.com', true, true, false, 0, now(), now())`, targetID)
			exec(`INSERT INTO memberships (id, org_id, user_id, role, joined_at, created_at, updated_at)
VALUES ($1, $2, $3, 'member', now(), now(), now())`, uuid.New(), orgID, targetID)
			return map[string]map[string]any{
				"admin": {"user_id": adminID.String(), "email": "venue-admin@example.com", "is_superuser": true},
			}
		},
	})

	requests := []venueoracle.Request{
		{Name: "status before", Method: "GET", Path: "/api/v1/admin/impersonate/status",
			Headers: map[string]string{"Authorization": "Bearer " + venue.Tokens["admin"]}},
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
	}
	python := venue.ServePython(t, requests)
	goBase, _ := startGoServer(t, ctx, venue, jwtKey)

	receipt := venueoracle.Diff(t, goBase, requests, python, venueoracle.DiffOptions{
		Normalize: func(request venueoracle.Request, body string) string {
			// expires_at is wall-clock-derived (now + IMPERSONATION_TTL_MINUTES)
			// and the two planes mint it microseconds apart; it is not a
			// value either route asserts equal to the millisecond, so this
			// blanks the field's VALUE, not its presence, matching every
			// other ruled Normalize in this repo's oracle suites.
			return redactField(body, "expires_at")
		},
	})
	t.Log(receipt)

	sourceRows := venueoracle.TableRows(t, ctx, venue.AdminURI(t, venue.SourceDB), impersonationAuditQuery(adminID, targetID))
	goRows := venueoracle.TableRows(t, ctx, venue.AdminURI(t, venue.GoDB), impersonationAuditQuery(adminID, targetID))
	if sourceRows != goRows {
		t.Errorf("audit_logs rows differ:\n python: %s\n go:     %s", sourceRows, goRows)
	}
}

func impersonationAuditQuery(adminID, targetID uuid.UUID) string {
	return fmt.Sprintf(`SELECT org_id, user_id, action, resource_type, resource_id, status
FROM audit_logs WHERE user_id = '%s' AND resource_id = '%s' ORDER BY created_at`, adminID, targetID)
}
