//go:build integration

package admin_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/crypto/bcrypt"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// TestSetUserPasswordRateLimitMatchesThePythonAPI is CHAOS-6335: a fix PR
// after CHAOS-6304's own merge found set_user_password registered at 1
// request/second, burst 10, where Python carries
// @limiter.limit(ADMIN_PASSWORD_LIMIT = "5/hour", key_func=
// get_admin_user_key).
//
// slowapi's Limiter scopes every hit by (key_func's key, the request's
// exact resolved URL path) -- verified live by instrumenting
// limiter._limiter.hit(item, key, path, ...) directly: two calls to
// set_user_password for two DIFFERENT target users, from the same admin,
// land in two SEPARATE buckets and never exhaust each other, because the
// target's own uuid is part of the path. The five 200s + one 429 shape
// this test proves is therefore for six password changes on the SAME
// target user, not six different ones -- the shape a real admin
// repeatedly resetting one compromised account's password would hit.
//
// This is also the first proof that Go's httpapi rate limiter (a single
// NewBucket built once per route pattern in routeChain, shared by every
// caller and every path instance -- there is no key_func/per-caller
// concept in it at all) is a materially different SHAPE of limit than
// Python's per-(admin, path) scoping, not just a different refill
// number: Go's bucket is GLOBAL across every admin and every target for
// this route, where Python's is scoped per admin per target. This test
// only proves the two agree for the single-target-user case; it does not
// (and today cannot) prove agreement for the multi-target case, where
// they structurally diverge. See the PR's RISK-NOTES.
func TestSetUserPasswordRateLimitMatchesThePythonAPI(t *testing.T) {
	ctx := context.Background()
	root := repoRoot(t)
	const jwtKey = "venue-oracle-test-secret-key-for-password-rate-limit-32-byt"
	const adminPlaintextPassword = "correct horse battery staple rl"
	const attempts = 6

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
			adminHash, err := bcrypt.GenerateFromPassword([]byte(adminPlaintextPassword), bcrypt.DefaultCost)
			if err != nil {
				t.Fatalf("bcrypt: %v", err)
			}
			exec(`INSERT INTO organizations (id, slug, name, tier, managed_by, is_active, created_at, updated_at)
VALUES ($1, 'venue-pwd-ratelimit-org', 'Venue Password Rate Limit Org', 'community', 'stripe', true, now(), now())`, orgID)
			exec(`INSERT INTO users (id, email, password_hash, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, 'venue-pwd-rl-admin@example.com', $2, true, true, false, 0, now(), now())`, adminID, string(adminHash))
			exec(`INSERT INTO memberships (id, org_id, user_id, role, joined_at, created_at, updated_at)
VALUES ($1, $2, $3, 'admin', now(), now(), now())`, uuid.New(), orgID, adminID)
			exec(`INSERT INTO users (id, email, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, 'venue-pwd-rl-target@example.com', true, true, false, 0, now(), now())`, targetID)
			// _ensure_user_in_scope needs the target to be a member of the
			// acting (non-superuser) admin's own org.
			exec(`INSERT INTO memberships (id, org_id, user_id, role, joined_at, created_at, updated_at)
VALUES ($1, $2, $3, 'member', now(), now(), now())`, uuid.New(), orgID, targetID)
			return map[string]map[string]any{
				"admin": {"user_id": adminID.String(), "email": "venue-pwd-rl-admin@example.com", "org_id": orgID.String(), "role": "admin"},
			}
		},
	})

	jsonHeaders := map[string]string{"Authorization": "Bearer " + venue.Tokens["admin"], "Content-Type": "application/json"}
	requests := make([]venueoracle.Request, attempts)
	for i := range requests {
		requests[i] = venueoracle.Request{
			Name: fmt.Sprintf("set password %d/%d", i+1, attempts), Method: "POST",
			Path:    "/api/v1/admin/users/" + targetID.String() + "/password",
			Headers: jsonHeaders,
			Body:    venueoracle.B64(fmt.Sprintf(`{"admin_password":%q,"password":"a new strong password %d"}`, adminPlaintextPassword, i)),
		}
	}
	python := venue.ServePython(t, requests)
	goBase, _ := startGoServer(t, ctx, venue, jwtKey)

	// Diff fires each request against Go exactly once (Do, internally) and
	// compares it to the matching pre-fetched Python response, failing
	// loud on any status/body mismatch -- this is the whole proof: five
	// 200s then a 429, byte-identical on both planes, or the test fails
	// naming which numbered request diverged. Neither response shape
	// carries a wall-clock field (success:true, or slowapi's rate-limit
	// error body), so no Normalize redaction is needed.
	receipt := venueoracle.Diff(t, goBase, requests, python, venueoracle.DiffOptions{})
	t.Log(receipt)
}
