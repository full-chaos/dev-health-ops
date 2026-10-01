//go:build integration

package admin_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// TestImpersonationTTLConfigMatchesThePythonAPI is the venue-oracle proof
// for two IMPERSONATION_TTL_MINUTES shapes a live round found diverging:
// Python's int() accepting a Unicode decimal digit where an ASCII-only Go
// parser refused it, and a TTL within Python's own timedelta range but
// beyond what Go's time.Duration (int64 nanoseconds, ~292 years) can
// literally represent, which used to silently wrap to an already-expired
// session. Each subtest runs its own venue (the TTL is read from the
// environment at request time on both planes, so it must be set before
// either plane answers a request).
func TestImpersonationTTLConfigMatchesThePythonAPI(t *testing.T) {
	unicode := t.Run("unicode digit TTL", func(t *testing.T) {
		runImpersonationTTLCase(t, "unicode_digit", "١٠") // Arabic-Indic "10"
	})
	beyond := t.Run("TTL beyond Go Duration range", func(t *testing.T) {
		// ~292.47 years in minutes -- the smallest value the live round
		// found wrapping time.Duration's int64 nanoseconds negative.
		runImpersonationTTLCase(t, "beyond_duration", "153722868")
	})
	// Each subtest compares Go with its own frozen Python answers; the
	// parent's proof stands for both having run to the end.
	if unicode && beyond {
		venueoracle.WriteGoOnlyProof(t, "both TTL subtests compared Go with the Python answers frozen at "+adminPythonBuild)
	}
}

// impersonationTTLDigests pin each subtest's golden ("PIN:<file>" until its
// first recording).
var impersonationTTLDigests = map[string]string{
	"unicode_digit":   "7ff7ae121398dce7f3aa568ab827ec10299ba3b425c81b336d8f7b492fdd73f7",
	"beyond_duration": "eb5b4fa665184a9df3e66c99d18f5da4d2ca2c2ec8b52a30333e6fd5ea4000dd",
}

func runImpersonationTTLCase(t *testing.T, name, ttlMinutes string) {
	t.Helper()
	ctx := context.Background()
	golden := venueoracle.OpenGolden(t, adminGolden("impersonation_ttl_"+name, t.Name(), impersonationTTLDigests[name]))
	root := golden.PythonRoot(t, repoRoot(t))
	nextID := goldenIDs("impttl-" + name)
	jwtKey := "venue-oracle-test-secret-key-for-impersonation-ttl-32bytes!"

	orgID := nextID()
	adminID := nextID()
	targetID := nextID()

	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		Golden:    golden,
		Root:      root,
		JWTKey:    jwtKey,
		PythonEnv: []string{"IMPERSONATION_TTL_MINUTES=" + ttlMinutes},
		Seed: func(t *testing.T, ctx context.Context, admin *pgxpool.Pool, v *venueoracle.Venue) map[string]map[string]any {
			t.Helper()
			exec := func(sql string, args ...any) {
				t.Helper()
				if _, err := admin.Exec(ctx, sql, args...); err != nil {
					t.Fatalf("seed: %v\n%s", err, sql)
				}
			}
			exec(`INSERT INTO organizations (id, slug, name, tier, managed_by, is_active, created_at, updated_at)
VALUES ($1, 'venue-ttl-org', 'Venue TTL Org', 'community', 'stripe', true, now(), now())`, orgID)
			exec(`INSERT INTO users (id, email, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, 'venue-ttl-admin@example.com', true, true, true, 0, now(), now())`, adminID)
			exec(`INSERT INTO users (id, email, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, 'venue-ttl-target@example.com', true, true, false, 0, now(), now())`, targetID)
			exec(`INSERT INTO memberships (id, org_id, user_id, role, joined_at, created_at, updated_at)
VALUES ($1, $2, $3, 'member', now(), now(), now())`, nextID(), orgID, targetID)
			return map[string]map[string]any{
				"admin": {"user_id": adminID.String(), "email": "venue-ttl-admin@example.com", "is_superuser": true},
			}
		},
	})

	t.Setenv("IMPERSONATION_TTL_MINUTES", ttlMinutes)

	requests := []venueoracle.Request{
		{Name: "start", Method: "POST", Path: "/api/v1/admin/impersonate",
			Headers: map[string]string{"Authorization": "Bearer " + venue.Tokens["admin"], "Content-Type": "application/json"},
			Body:    venueoracle.B64(fmt.Sprintf(`{"target_user_id":%q}`, targetID.String()))},
		{Name: "status during", Method: "GET", Path: "/api/v1/admin/impersonate/status",
			Headers: map[string]string{"Authorization": "Bearer " + venue.Tokens["admin"]}},
		{Name: "stop", Method: "POST", Path: "/api/v1/admin/impersonate/stop",
			Headers: map[string]string{"Authorization": "Bearer " + venue.Tokens["admin"], "Content-Type": "application/json"}},
	}
	python := golden.Python(t, venue, requests)
	goBase, _ := startGoServer(t, context.Background(), venue, jwtKey)

	receipt := venueoracle.Diff(t, goBase, requests, python, venueoracle.DiffOptions{
		Golden: golden,
		Normalize: func(request venueoracle.Request, body string) string {
			return redactField(t, body, "expires_at")
		},
	})
	t.Log(receipt)
	golden.Finish(t)
}
