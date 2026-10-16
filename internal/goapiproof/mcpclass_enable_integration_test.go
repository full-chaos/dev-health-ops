//go:build integration

package goapiproof

// CHAOS-9802: `enable` lights an MCP class root as the operator's recorded decision, with no proof run in front of it.
// The per-root receipt is retired (D6024); MCP and GraphQL parity is held by TestMCPClassParityVenueOracle.

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/mcpclass"
)

func enableClass(pool *pgxpool.Pool, op string) ([]EnableOutcome, error) {
	return Enable(context.Background(), pool, EnableRequest{
		SchemaDigest: testSchemaDigest, RunningBuild: testCandidateBuild, Operations: []string{op},
		OperationKinds: map[string]string{op: OperationKindMCPClass}, DocumentDigest: mcpclass.Digests(), Mode: TargetModeCanary,
		RolloutPercentage: EnforcedRolloutPercentage, RecordedBy: "test", ReviewEvidence: "enable", PrincipalID: "test-principal",
	})
}

func classRowMode(t *testing.T, pool *pgxpool.Pool, op string) string {
	t.Helper()
	var mode string
	if err := pool.QueryRow(context.Background(),
		`SELECT mode FROM go_api_class_decision WHERE operation = $1`, op).Scan(&mode); err != nil {
		t.Fatal(err)
	}
	return mode
}

func TestEnableLightsAClassRootWithNoReceiptAndAuditsIt(t *testing.T) {
	pool := startAuditedRegistryPostgres(t)
	op := mcpclass.Operation("hotspots")
	var receipts int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM go_api_proof_run`).Scan(&receipts); err != nil || receipts != 0 {
		t.Fatalf("precondition: %d proof receipts (err %v), want none", receipts, err)
	}
	outcomes, err := enableClass(pool, op)
	if err != nil {
		t.Fatalf("enable with no receipt: %v", err)
	}
	if len(outcomes) != 1 || outcomes[0].Mode != TargetModeCanary || outcomes[0].CandidateBuild != testCandidateBuild || outcomes[0].HadRowBefore {
		t.Fatalf("outcomes %+v, want one canary row at the running build, written fresh", outcomes)
	}
	if mode := classRowMode(t, pool, op); mode != TargetModeCanary {
		t.Fatalf("class row mode %q, want canary", mode)
	}
	var audited int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM public.go_api_routing_audits WHERE action = 'enable'`).Scan(&audited); err != nil || audited != 1 {
		t.Fatalf("audit rows for the enable = %d (err %v), want 1", audited, err)
	}
}
