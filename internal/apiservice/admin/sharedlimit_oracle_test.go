//go:build integration

package admin_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// roundRobin serves every request from the next of several Go api servers:
// one caller talking to a fleet of replicas behind a load balancer.
func roundRobin(t *testing.T, backends ...string) string {
	t.Helper()
	proxies := make([]*httputil.ReverseProxy, len(backends))
	for i, backend := range backends {
		target, err := url.Parse(backend)
		if err != nil {
			t.Fatal(err)
		}
		proxies[i] = httputil.NewSingleHostReverseProxy(target)
	}
	var next atomic.Uint64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		proxies[(next.Add(1)-1)%uint64(len(proxies))].ServeHTTP(w, r)
	}))
	t.Cleanup(server.Close)
	return server.URL
}

// TestSharedRateLimitAcrossReplicasVenueOracle is CHAOS-6368's proof: one
// admin's limited requests, sent through a load balancer that alternates
// between TWO real Go api servers, are limited exactly as the single Python
// api limits them -- the eleventh invite (10/hour) and the sixth password
// change (5/hour) are 429 whichever replica answers. With per-process
// counters each replica would admit its own full allowance.
func TestSharedRateLimitAcrossReplicasVenueOracle(t *testing.T) {
	ctx := context.Background()
	root := repoRoot(t)
	const jwtKey = "venue-oracle-test-secret-key-for-shared-rate-limit-32-by"
	const adminPlaintextPassword = "correct horse battery staple sl"
	const invites, resets = 12, 7

	orgID, adminID, targetID := uuid.New(), uuid.New(), uuid.New()
	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		Root: root, JWTKey: jwtKey,
		Seed: func(t *testing.T, ctx context.Context, admin *pgxpool.Pool, v *venueoracle.Venue) map[string]map[string]any {
			return seedRateLimitVenue(t, ctx, admin, orgID, []uuid.UUID{adminID}, []uuid.UUID{targetID}, adminPlaintextPassword)
		},
	})
	headers := map[string]string{"Authorization": "Bearer " + venue.Tokens["admin0"], "Content-Type": "application/json"}
	var requests []venueoracle.Request
	for i := range invites {
		requests = append(requests, venueoracle.Request{
			Name: fmt.Sprintf("invite %d/%d", i+1, invites), Method: "POST",
			Path: "/api/v1/admin/orgs/" + orgID.String() + "/invites", Headers: headers,
			Body: venueoracle.B64(fmt.Sprintf(`{"email":"shared-limit-%d@example.com"}`, i)),
		})
	}
	for i := range resets {
		requests = append(requests, venueoracle.Request{
			Name: fmt.Sprintf("set password %d/%d", i+1, resets), Method: "POST",
			Path: "/api/v1/admin/users/" + targetID.String() + "/password", Headers: headers,
			Body: venueoracle.B64(fmt.Sprintf(`{"admin_password":%q,"password":"a new strong password %d"}`, adminPlaintextPassword, i)),
		})
	}
	python := venue.ServePython(t, requests)

	// Two replicas: separate pools, separate Valkey clients, one Valkey.
	replicaA, _ := startGoServer(t, ctx, venue, jwtKey)
	replicaB, _ := startGoServer(t, ctx, venue, jwtKey)
	balanced := roundRobin(t, replicaA, replicaB)

	receipt := venueoracle.Diff(t, balanced, requests, python, venueoracle.DiffOptions{
		Normalize: func(request venueoracle.Request, body string) string {
			for _, field := range []string{"id", "created_at", "updated_at", "expires_at"} {
				body = redactField(t, body, field)
			}
			return body
		},
	})
	t.Log(receipt)
}
