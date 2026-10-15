//go:build integration

package server

import (
	"crypto/ed25519"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/principal"
)

// TestCapacityForecastRoute_IsServedByTheCatalog proves capacityForecast reaches the Go resolver through
// the real /query handler with an empty go_api_routing_state table (no row at any schema digest), the
// state that made the local stack answer "served by query-api and has no Python implementation".
func TestCapacityForecastRoute_IsServedByTheCatalog(t *testing.T) {
	pool := startTestRegistryPostgres(t)

	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	jwksPath := writeTestJWKS(t, pub)
	verifier, err := principal.NewVerifier(jwksPath, itTestIssuer, itTestAudience)
	if err != nil {
		t.Fatal(err)
	}

	base := time.Now().UTC().Truncate(24*time.Hour).AddDate(0, 0, -10)
	var throughput [][]any
	for i, n := range []uint64{2, 4, 6, 8, 10} {
		throughput = append(throughput, []any{base.AddDate(0, 0, i), n})
	}
	client := &cdClient{responses: []*cdScanner{{rows: throughput}, {rows: [][]any{{uint64(30)}}}}}

	handler, _, _, _, _ := newQueryHandler(client, pool, verifier, itTestSchemaDigest, os.Getenv)
	token := signTestEnvelope(t, priv, "org-1")

	t.Run("no_routing_row_is_served_by_the_catalog_rule", func(t *testing.T) {
		rec := postGraphQLWithVariables(t, handler, registeredCapacityCompletionDistributionDocument, token,
			map[string]any{"orgId": "org-1", "input": map[string]any{"teamId": "team-a"}})
		if rec.Code != http.StatusOK {
			t.Fatalf("no routing-state row: got %d, want 200, body=%s", rec.Code, rec.Body.String())
		}
		body := rec.Body.String()
		if strings.Contains(body, `"errors"`) || strings.Contains(body, "no Python implementation") {
			t.Fatalf("capacityForecast answered with errors: %s", body)
		}
		if !strings.Contains(body, `"completionDistribution"`) || !strings.Contains(body, `"days"`) {
			t.Fatalf("capacityForecast carries no completionDistribution days: %s", body)
		}
	})
}
