//go:build integration

package goapiproof

// CHAOS-7512: `enable` admits an MCP class root on its MEASURED shapes only. A class receipt whose provenance lists an excluded shape is
// refused (whole enable, nothing written) unless the operator named that shape's operation AND the go-served ledger holds an unproven
// named-limit entry for it.

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/mcpclass"
)

// excludedUnprovenOp has an unproven named-limit entry in the checked-in ledger; excludedOtherOp has none.
const (
	excludedUnprovenOp = "featureFlagTimeseries"
	excludedOtherOp    = "flowMatrix"
)

func classProvenance(t *testing.T, root string, excluded []string) string {
	t.Helper()
	encoded, err := json.Marshal(ReceiptProvenance{
		MeasurementRoute: RouteProof, EdgeBuildBinding: EdgeBuildPresent, EdgeMode: EdgeModeDocRoute,
		MCPClass: &MCPClassProvenance{Root: root, Reference: "go_document_route", Executed: 3, Matched: 3, Excluded: excluded},
	})
	if err != nil {
		t.Fatal(err)
	}
	return string(encoded)
}

func seedAndProveClass(t *testing.T, pool *pgxpool.Pool, root, evidence string) string {
	t.Helper()
	ctx := context.Background()
	op := mcpclass.Operation(root)
	if _, err := Seed(ctx, pool, SeedRequest{
		SchemaDigest: testSchemaDigest, RunningBuild: testCandidateBuild, Operations: []string{op},
		DocumentDigest: mcpclass.Digests(), RecordedBy: "test", ReviewEvidence: "class row", PrincipalID: "test-principal",
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if _, err := Write(ctx, pool, Receipt{
		SchemaDigest: testSchemaDigest, DocumentDigest: mcpclass.DocumentDigest(), SelectedOperation: op,
		CandidateBuild: testCandidateBuild, RequestIdentity: "req-" + root, Stage: EnablementProofStage,
		TerminalState: EnablementProofTerminalState, MeasurementRoute: RouteProof, BuildBinding: EdgeBuildPresent,
		OrgID: "70d529e0", RecordedBy: "test", ReviewEvidence: evidence, ObservedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("receipt: %v", err)
	}
	return op
}

func enableClass(pool *pgxpool.Pool, op string, allow ...string) ([]EnableOutcome, error) {
	return enableClassWith(pool, op, nil, allow...)
}

func enableClassWith(pool *pgxpool.Pool, op string, ledger *GoServedLedger, allow ...string) ([]EnableOutcome, error) {
	return Enable(context.Background(), pool, EnableRequest{
		SchemaDigest: testSchemaDigest, RunningBuild: testCandidateBuild, Operations: []string{op},
		OperationKinds: map[string]string{op: OperationKindMCPClass}, DocumentDigest: mcpclass.Digests(), Mode: TargetModeCanary,
		RolloutPercentage: EnforcedRolloutPercentage, RecordedBy: "test", ReviewEvidence: "enable", PrincipalID: "test-principal",
		AllowExcluded: allow, Ledger: ledger,
	})
}

func classRowMode(t *testing.T, pool *pgxpool.Pool, op string) string {
	t.Helper()
	var mode string
	if err := pool.QueryRow(context.Background(),
		`SELECT mode FROM go_api_routing_state WHERE selected_operation = $1 AND schema_digest = $2`, op, testSchemaDigest).Scan(&mode); err != nil {
		t.Fatal(err)
	}
	return mode
}

func TestEnableRefusesAClassReceiptThatListsAnExcludedShape(t *testing.T) {
	pool := startAuditedRegistryPostgres(t)
	op := seedAndProveClass(t, pool, "analytics", classProvenance(t, "analytics", []string{excludedUnprovenOp + ":V=doc_operation_not_receipt_backed", excludedOtherOp + "=needs_instance_identifier"}))
	for name, allow := range map[string][]string{
		"empty allow-list": nil,
		"only the unproven op named: the other remains": {excludedUnprovenOp},
		"a regex-looking entry is just a name":          {".*"},
		"an op with no ledger entry named":              {excludedOtherOp},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := enableClass(pool, op, allow...)
			if err == nil || !errors.Is(err, ErrEnableRequestRefused) || !strings.Contains(err.Error(), "NOT measured") {
				t.Fatalf("err = %v, want the unmeasured-shape refusal", err)
			}
			if got := classRowMode(t, pool, op); got != "shadow" {
				t.Fatalf("the refused enable wrote the row: mode = %s", got)
			}
		})
	}
}

func TestEnableAdmitsAnExcludedShapeOnlyWhenNamedAndLedgerBacked(t *testing.T) {
	pool := startAuditedRegistryPostgres(t)
	op := seedAndProveClass(t, pool, "analytics", classProvenance(t, "analytics", []string{excludedUnprovenOp + ":A=doc_operation_not_receipt_backed", excludedUnprovenOp + "=doc_operation_not_receipt_backed"}))
	outcomes, err := enableClass(pool, op, excludedUnprovenOp)
	if err != nil {
		t.Fatalf("enable with the unproven operation named: %v", err)
	}
	if got := classRowMode(t, pool, op); got != "canary" {
		t.Fatalf("mode = %s, want canary", got)
	}
	if len(outcomes) != 1 || !strings.Contains(outcomes[0].ReviewEvidence, "[allow-excluded: "+excludedUnprovenOp+"]") {
		t.Fatalf("the allow-list used is not in the evidence: %+v", outcomes)
	}
}

func TestEnableRefusesAClassReceiptWithNoReadableProvenance(t *testing.T) {
	pool := startAuditedRegistryPostgres(t)
	op := seedAndProveClass(t, pool, "hotspots", "")
	if _, err := enableClass(pool, op); err == nil || !errors.Is(err, ErrEnableRequestRefused) || !strings.Contains(err.Error(), "no readable provenance") {
		t.Fatalf("err = %v, want the no-provenance refusal", err)
	}
	if got := classRowMode(t, pool, op); got != "shadow" {
		t.Fatalf("mode = %s", got)
	}
}

func TestEnableAdmitsAClassReceiptWithNothingExcluded(t *testing.T) {
	pool := startAuditedRegistryPostgres(t)
	op := seedAndProveClass(t, pool, "catalog", classProvenance(t, "catalog", nil))
	if _, err := enableClass(pool, op); err != nil {
		t.Fatalf("a fully measured class receipt must enable: %v", err)
	}
	if got := classRowMode(t, pool, op); got != "canary" {
		t.Fatalf("mode = %s", got)
	}
}

// Each clause alone: naming an operation the ledger does not hold as UNPROVEN is not enough; an UNPROVEN ledger entry that the operator did
// not name is not enough; a ledger entry that is only an enable limit (no unproven_reason) is not an unproven named limit.
func TestEnableExcludedShapeNeedsBothTheLedgerEntryAndTheOperatorsName(t *testing.T) {
	refused := func(t *testing.T, excluded string, ledger *GoServedLedger, allow ...string) {
		t.Helper()
		pool := startAuditedRegistryPostgres(t)
		op := seedAndProveClass(t, pool, "analytics", classProvenance(t, "analytics", []string{excluded}))
		if _, err := enableClassWith(pool, op, ledger, allow...); err == nil || !errors.Is(err, ErrEnableRequestRefused) {
			t.Fatalf("err = %v, want the unmeasured-shape refusal", err)
		}
		if got := classRowMode(t, pool, op); got != "shadow" {
			t.Fatalf("the refused enable wrote the row: mode = %s", got)
		}
	}
	t.Run("named, but no ledger entry for the operation", func(t *testing.T) {
		refused(t, excludedOtherOp+"=needs_instance_identifier", nil, excludedOtherOp)
	})
	t.Run("a ledger entry, but the operator did not name it", func(t *testing.T) {
		refused(t, excludedUnprovenOp+"=doc_operation_not_receipt_backed", nil)
	})
	t.Run("named, ledger entry is only an enable limit (not unproven)", func(t *testing.T) {
		ledger := &GoServedLedger{Entries: []GoServedEntry{{Operation: excludedOtherOp, EnableLimit: "an enable limit that is not an unproven named limit, written for the test"}}}
		refused(t, excludedOtherOp+"=needs_instance_identifier", ledger, excludedOtherOp)
	})
}

// r1: the provenance is bound to the root being enabled: a receipt whose provenance names ANOTHER root authorizes nothing.
func TestEnableRefusesAClassReceiptWhoseProvenanceNamesAnotherRoot(t *testing.T) {
	pool := startAuditedRegistryPostgres(t)
	op := seedAndProveClass(t, pool, "hotspots", classProvenance(t, "workGraphEdges", nil))
	_, err := enableClass(pool, op)
	if err == nil || !errors.Is(err, ErrEnableRequestRefused) || !strings.Contains(err.Error(), "names root workGraphEdges, not hotspots") {
		t.Fatalf("err = %v, want the wrong-root provenance refusal", err)
	}
	if got := classRowMode(t, pool, op); got != "shadow" {
		t.Fatalf("mode = %s", got)
	}
}

// vetter: an UNPROVEN class root (no receipt at all) is answered by the unproven gate, never by the provenance check (which only judges roots
// whose receipt was admitted).
func TestEnableOfAnUnprovenClassRootIsAnsweredByTheUnprovenGate(t *testing.T) {
	pool := startAuditedRegistryPostgres(t)
	op := mcpclass.Operation("hotspots")
	if _, err := Seed(context.Background(), pool, SeedRequest{
		SchemaDigest: testSchemaDigest, RunningBuild: testCandidateBuild, Operations: []string{op},
		DocumentDigest: mcpclass.Digests(), RecordedBy: "test", ReviewEvidence: "class row", PrincipalID: "test-principal",
	}); err != nil {
		t.Fatal(err)
	}
	_, err := enableClass(pool, op)
	if err == nil || !errors.Is(err, ErrEnableUnproven) || errors.Is(err, ErrEnableRequestRefused) || strings.Contains(err.Error(), "provenance") {
		t.Fatalf("err = %v, want ErrEnableUnproven from the unproven gate (not the provenance refusal)", err)
	}
}
