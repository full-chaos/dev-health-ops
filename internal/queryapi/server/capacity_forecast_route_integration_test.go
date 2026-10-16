//go:build integration

package server

import (
	"crypto/ed25519"
	"encoding/json"
	"net/http"
	"os"
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
		rec := postGraphQLWithVariables(t, handler, registeredCapacityForecastDocument, token,
			map[string]any{"orgId": "org-1", "input": map[string]any{"teamId": "team-a"}})
		if rec.Code != http.StatusOK {
			t.Fatalf("no routing-state row: got %d, want 200, body=%s", rec.Code, rec.Body.String())
		}
		var out struct {
			Data struct {
				CapacityForecast *struct {
					ForecastID             string `json:"forecastId"`
					TeamID                 string `json:"teamId"`
					BacklogSize            int    `json:"backlogSize"`
					HistoryDays            int    `json:"historyDays"`
					CompletionDistribution *struct {
						Days []struct {
							Count int `json:"count"`
						} `json:"days"`
					} `json:"completionDistribution"`
				} `json:"capacityForecast"`
			} `json:"data"`
			Errors []struct {
				Message string `json:"message"`
			} `json:"errors"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
			t.Fatalf("response is not GraphQL JSON: %v, body=%s", err, rec.Body.String())
		}
		if len(out.Errors) > 0 {
			t.Fatalf("capacityForecast answered with GraphQL errors %v", out.Errors)
		}
		f := out.Data.CapacityForecast
		if f == nil {
			t.Fatalf("data.capacityForecast is null: %s", rec.Body.String())
		}
		if f.ForecastID == "" || f.TeamID != "team-a" || f.BacklogSize != 30 || f.HistoryDays != 5 {
			t.Fatalf("forecast = %+v, want an id, teamId team-a, backlogSize 30 (fake count), historyDays 5 (the five fake throughput days)", *f)
		}
		if f.CompletionDistribution == nil || len(f.CompletionDistribution.Days) == 0 {
			t.Fatalf("completionDistribution carries no day bins: %s", rec.Body.String())
		}
		total := 0
		for _, bin := range f.CompletionDistribution.Days {
			total += bin.Count
		}
		if total == 0 {
			t.Fatalf("completionDistribution day counts sum to 0: %s", rec.Body.String())
		}
	})
}
