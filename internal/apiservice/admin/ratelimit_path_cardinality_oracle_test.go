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

// TestRateLimitPathCardinalityVenueOracle is CHAOS-6624's proof. slowapi
// keeps one counter per (caller key, exact request path) and bounds nothing:
// its storage limits itself by TTL, so a caller reading any number of
// DISTINCT paths in one window is never refused for the number of paths. The
// Go limiter used to refuse a caller's 1,001st distinct path in a window (a
// Go-only cardinality guard), so a legitimate client walking a large set of
// ids got 429 where Python answered.
//
// One admin sends one invite request to each of 1,001 distinct organization
// ids (each a different path, each far below the 10/hour limit of its own
// counter); Python answers every one the same way, and Go must too. The
// answer is a 403/404 from the endpoint, not a 429 from the limiter.
func TestRateLimitPathCardinalityVenueOracle(t *testing.T) {
	ctx := context.Background()
	root := repoRoot(t)
	const jwtKey = "venue-oracle-test-secret-key-for-path-cardinality-32-by"
	const adminPlaintextPassword = "correct horse battery staple pc"
	const distinctPaths = 1001

	orgID, adminID, targetID := uuid.New(), uuid.New(), uuid.New()
	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		Root: root, JWTKey: jwtKey,
		Seed: func(t *testing.T, ctx context.Context, admin *pgxpool.Pool, v *venueoracle.Venue) map[string]map[string]any {
			return seedRateLimitVenue(t, ctx, admin, orgID, []uuid.UUID{adminID}, []uuid.UUID{targetID}, adminPlaintextPassword)
		},
	})
	headers := map[string]string{"Authorization": "Bearer " + venue.Tokens["admin0"], "Content-Type": "application/json"}
	requests := make([]venueoracle.Request, 0, distinctPaths)
	for i := range distinctPaths {
		requests = append(requests, venueoracle.Request{
			Name: fmt.Sprintf("invite to distinct org %d/%d", i+1, distinctPaths), Method: "POST",
			Path: "/api/v1/admin/orgs/" + uuid.NewString() + "/invites", Headers: headers,
			Body: venueoracle.B64(fmt.Sprintf(`{"email":"path-cardinality-%d@example.com"}`, i)),
		})
	}
	python := venue.ServePython(t, requests)
	for i, response := range python {
		if response.Status == 429 {
			t.Fatalf("python refused request %d with 429: the oracle case is wrong, Python bounds nothing", i+1)
		}
	}

	replica, _ := startGoServer(t, ctx, venue, jwtKey)
	receipt := venueoracle.Diff(t, replica, requests, python, venueoracle.DiffOptions{})
	t.Log(receipt)
}
