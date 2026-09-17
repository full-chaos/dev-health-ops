//go:build integration

package goapiproof

import (
	"context"
	"testing"
	"time"
)

// A class-proven capacityForecast receipt, written by the real writer, is
// read as enablement proof by the production predicate for both target
// modes; the same run with one non-stochastic difference is read as
// nothing.
func TestAClassProvenReceiptIsEnablementProofAndNothingElseIs(t *testing.T) {
	ctx := context.Background()
	for _, c := range []struct {
		name      string
		candidate map[string]any
		admitted  bool
	}{
		{"only drawn values differ", map[string]any{"p85Days": 4, "p85Date": forecastDay(4)}, true},
		{"a drawn value and backlogSize differ", map[string]any{"p85Days": 4, "p85Date": forecastDay(4), "backlogSize": 13}, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			pool := startRegistryPostgres(t)
			edge := &fakeEdge{
				pythonBody: forecastBody(t, "python", nil),
				goBody:     forecastBody(t, "go", c.candidate),
			}
			runner := capacityForecastRunner(t, edge)
			runner.Registry.SchemaDigest = testSchemaDigest
			runner.Registry.BuildIdentity = testCandidateBuild
			runner.Registry.DocumentDigest = map[string]string{"capacityForecast": testDocumentDigest}
			runner.Routing = map[string]RoutingRow{"capacityForecast": {Mode: "canary", CandidateBuild: testCandidateBuild}}
			runner.Config.RecordedBy = "test"
			runner.Config.ReviewEvidence = "why"
			edge.goBuild = testCandidateBuild
			if _, _, err := runner.Run(ctx); err != nil {
				t.Fatalf("Run: %v", err)
			}
			receipts, err := runner.ReceiptsFor(time.Now().UTC())
			if err != nil {
				t.Fatalf("ReceiptsFor: %v", err)
			}
			if _, err := WriteReceipts(ctx, pool, receipts); err != nil {
				t.Fatalf("WriteReceipts: %v", err)
			}
			var state string
			var citations []string
			if err := pool.QueryRow(ctx,
				`SELECT terminal_state, baseline_defect FROM go_api_proof_run WHERE selected_operation = 'capacityForecast'`,
			).Scan(&state, &citations); err != nil {
				t.Fatalf("read back: %v", err)
			}
			if state != TerminalStateMismatch || len(citations) != 1 || citations[0] != capacityForecastStochasticLeaves.Citation() {
				t.Fatalf("stored terminal_state=%q baseline_defect=%v", state, citations)
			}
			for _, mode := range []string{TargetModeCanary, TargetModePrimary} {
				found, err := OperationsWithEnablementProof(ctx, pool, testSchemaDigest, testCandidateBuild,
					mode, map[string]string{"capacityForecast": testDocumentDigest})
				if err != nil {
					t.Fatalf("OperationsWithEnablementProof(%s): %v", mode, err)
				}
				if found["capacityForecast"] != c.admitted {
					t.Fatalf("%s: admitted=%v, want %v", mode, found["capacityForecast"], c.admitted)
				}
			}
		})
	}
}
