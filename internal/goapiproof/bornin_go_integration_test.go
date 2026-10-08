//go:build integration

package goapiproof

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/mcpclass"
)

// The class receipt MCPClassReceipts builds for a born-in-Go root is the one
// `enable` admits, with no -allow-excluded and no named limit.
func TestEnableAdmitsTheClassReceiptOfABornInGoRoot(t *testing.T) {
	pool := startAuditedRegistryPostgres(t)
	ctx := context.Background()
	op := mcpclass.Operation("sourceHealth")
	runner := classRunner([]sealedOutcome{sealedMatch("sourceHealth", "")})
	runner.Config.DocRouteReference = true
	runner.GoServed = defaultLedgerForTest(t)
	receipts, verdicts, err := runner.MCPClassReceipts([]Outcome{executedOutcome("sourceHealth", "")},
		map[string][]string{op: {"sourceHealth"}}, map[string]bool{}, time.Now().UTC())
	if err != nil || len(receipts) != 1 || verdicts[0].Executed != 1 || len(verdicts[0].Excluded) != 0 {
		t.Fatalf("receipts=%d verdicts=%+v err=%v, want one receipt, executed 1, excluded 0", len(receipts), verdicts, err)
	}
	if _, err := Seed(ctx, pool, SeedRequest{
		SchemaDigest: testSchemaDigest, RunningBuild: testCandidateBuild, Operations: []string{op},
		DocumentDigest: mcpclass.Digests(), RecordedBy: "test", ReviewEvidence: "class row", PrincipalID: "test-principal",
	}); err != nil {
		t.Fatal(err)
	}
	receipt := receipts[0]
	receipt.SchemaDigest, receipt.CandidateBuild, receipt.OrgID = testSchemaDigest, testCandidateBuild, "70d529e0"
	receipt.Stage, receipt.TerminalState = EnablementProofStage, EnablementProofTerminalState
	if _, err := Write(ctx, pool, receipt); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := enableClass(pool, op); err != nil {
		t.Fatalf("enable refused the born-in-Go class receipt: %v", err)
	}
	if mode := classRowMode(t, pool, op); mode != TargetModeCanary {
		t.Fatalf("class row mode %q, want canary", mode)
	}
}

// -allow-excluded gives a born-in-Go shape nothing: a class receipt that lists
// it as excluded is refused exactly as on the base.
func TestAllowExcludedDoesNotAdmitABornInGoShape(t *testing.T) {
	pool := startAuditedRegistryPostgres(t)
	for root, shape := range map[string]string{"analytics": "investmentEvidenceQuality", "capacityForecast": "capacityCompletionDistribution"} {
		op := seedAndProveClass(t, pool, root, classProvenance(t, root, []string{shape + "=doc_operation_not_receipt_backed"}))
		if _, err := enableClass(pool, op, shape); err == nil {
			t.Errorf("%s: enable with -allow-excluded %s succeeded: a born-in-Go shape must be measured, not allowed", root, shape)
		}
	}
}
