//go:build integration

package goapiproof

import (
	"context"
	"testing"
	"time"
)

// A go-only receipt, written by the real writer, is read as enablement proof
// by the production predicate for both target modes -- and a receipt for a
// different build, or one the same run left inert by an unbound edge
// measurement, is read as nothing.
func TestAGoOnlyReceiptIsEnablementProofForItsBuildOnly(t *testing.T) {
	ctx := context.Background()
	for _, c := range []struct {
		name     string
		bound    bool
		admitted bool
	}{
		{"build bound per request", true, true},
		{"build absent on the edge", false, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			pool := startRegistryPostgres(t)
			build := ""
			if c.bound {
				build = testCandidateBuild
			}
			runner := goOnlyRunnerAtBuild(t, defaultLedgerForTest(t), build)
			runner.Registry.SchemaDigest = testSchemaDigest
			runner.Registry.BuildIdentity = testCandidateBuild
			runner.Registry.DocumentDigest = map[string]string{"capacityForecast": testDocumentDigest}
			runner.Routing = map[string]RoutingRow{"capacityForecast": {Mode: "canary", CandidateBuild: testCandidateBuild}}
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
			other, err := OperationsWithEnablementProof(ctx, pool, testSchemaDigest, "0000000000000000000000000000000000000000",
				TargetModeCanary, map[string]string{"capacityForecast": testDocumentDigest})
			if err != nil {
				t.Fatal(err)
			}
			if other["capacityForecast"] {
				t.Fatal("a go-only receipt authorised a different build")
			}
		})
	}
}
