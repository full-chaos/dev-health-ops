//go:build integration

package routing

// CHAOS-7766: pins that the `-allow-excluded` flag of the REAL `enable` verb reaches goapiproof.Enable (CHAOS-7512 enable.go: AllowExcluded:
// allowedNames), and that each name is whitespace-trimmed. Run end to end against a real Postgres: a class receipt that lists an excluded shape
// is refused without the flag, admitted with it, and a spaced list is accepted.

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/goapiproof"
	"github.com/full-chaos/dev-health-ops/internal/mcpclass"
)

const allowExcludedOp = "featureFlagTimeseries" // has an unproven named-limit entry in the checked-in go-served ledger

func seedClassRootWithExcludedShape(t *testing.T, pool *pgxpool.Pool) string {
	t.Helper()
	ctx := context.Background()
	op := mcpclass.Operation("analytics")
	provenance, err := json.Marshal(goapiproof.ReceiptProvenance{
		MeasurementRoute: goapiproof.RouteProof, EdgeBuildBinding: goapiproof.EdgeBuildPresent, EdgeMode: goapiproof.EdgeModeDocRoute,
		MCPClass: &goapiproof.MCPClassProvenance{Root: "analytics", Reference: "go_document_route", Executed: 3, Matched: 3,
			Excluded: []string{allowExcludedOp + ":A=doc_operation_not_receipt_backed"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := goapiproof.Seed(ctx, pool, goapiproof.SeedRequest{
		SchemaDigest: localSchemaDigest(), RunningBuild: verbTestBuild, Operations: []string{op},
		DocumentDigest: mcpclass.Digests(), RecordedBy: "test", ReviewEvidence: "class row", PrincipalID: verbTestPrincipalID,
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if _, err := goapiproof.Write(ctx, pool, goapiproof.Receipt{
		SchemaDigest: localSchemaDigest(), DocumentDigest: mcpclass.DocumentDigest(), SelectedOperation: op,
		CandidateBuild: verbTestBuild, RequestIdentity: "req-analytics", Stage: goapiproof.EnablementProofStage,
		TerminalState: goapiproof.EnablementProofTerminalState, MeasurementRoute: goapiproof.RouteProof, BuildBinding: goapiproof.EdgeBuildPresent,
		OrgID: "70d529e0", RecordedBy: "test", ReviewEvidence: string(provenance), ObservedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("receipt: %v", err)
	}
	return op
}

func classEnableArgs(server *httptest.Server, dsn string, extra ...string) []string {
	return append([]string{
		"enable", "-operations", mcpclass.Operation("analytics"), "-mode", "canary",
		"-registry-url", server.URL + "/registry", "-buildinfo-url", server.URL + "/buildinfo",
		"-postgres-uri", dsn, "-recorded-by", "lane-routing-verbs", "-review-evidence", "the allow-excluded fixture",
	}, extra...)
}

func classRowModeOf(t *testing.T, pool *pgxpool.Pool, op string) string {
	t.Helper()
	var mode string
	if err := pool.QueryRow(context.Background(), `SELECT mode FROM go_api_class_decision WHERE operation = $1`, op).Scan(&mode); err != nil {
		t.Fatal(err)
	}
	return mode
}

func TestEnableVerbRefusesAnExcludedShapeWithoutTheFlagAndAdmitsItWithIt(t *testing.T) {
	pool, dsn := startVerbPostgres(t)
	t.Setenv(bearerEnvVar, verbTestBearer)
	op := seedClassRootWithExcludedShape(t, pool)
	server := startQueryAPI(t, localSchemaDigest(), map[string]string{verbTestOperation: "9999999999999999999999999999999999999999999999999999999999999999"})

	_, errOut, err := captureVerb(t, classEnableArgs(server, dsn)...)
	if err == nil || !strings.Contains(err.Error()+errOut, "NOT measured") {
		t.Fatalf("enable without -allow-excluded = %v / %s, want the unmeasured-shape refusal", err, errOut)
	}
	if got := classRowModeOf(t, pool, op); got != "shadow" {
		t.Fatalf("the refused enable wrote the row: mode = %s", got)
	}

	out, _, err := captureVerb(t, classEnableArgs(server, dsn, "-allow-excluded", allowExcludedOp)...)
	if err != nil {
		t.Fatalf("enable with -allow-excluded %s: %v", allowExcludedOp, err)
	}
	if !strings.Contains(out, "allow-excluded in use: "+allowExcludedOp) {
		t.Fatalf("the verb did not report the allow-list it used:\n%s", out)
	}
	if got := classRowModeOf(t, pool, op); got != "canary" {
		t.Fatalf("mode = %s, want canary: the flag did not reach Enable", got)
	}
}

func TestEnableVerbTrimsEachNameOfASpacedAllowExcludedList(t *testing.T) {
	pool, dsn := startVerbPostgres(t)
	t.Setenv(bearerEnvVar, verbTestBearer)
	op := seedClassRootWithExcludedShape(t, pool)
	server := startQueryAPI(t, localSchemaDigest(), map[string]string{verbTestOperation: "9999999999999999999999999999999999999999999999999999999999999999"})

	// "a, b" with a space after the comma: each name is trimmed before it is matched against the operation-name pattern.
	if _, _, err := captureVerb(t, classEnableArgs(server, dsn, "-allow-excluded", allowExcludedOp+", "+allowExcludedOp)...); err != nil {
		t.Fatalf("a spaced allow-list was refused: %v", err)
	}
	if got := classRowModeOf(t, pool, op); got != "canary" {
		t.Fatalf("mode = %s, want canary", got)
	}
}
