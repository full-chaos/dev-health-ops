//go:build integration

package goapiproof

// The routing verbs on MCP class decisions (go_api_class_decision) and their refusal of catalog operations.

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/mcpclass"
)

const verbsRunningBuild = "ffd9e5d5dc8ee21de5befa1bae47ba9195be135e"

func seedClassDecisionRow(t *testing.T, pool *pgxpool.Pool, operation, mode, build string) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), `
		INSERT INTO go_api_class_decision (operation, mode, current_candidate_build, schema_digest, review_evidence, recorded_by)
		VALUES ($1, $2, $3, $4, 'test seed', 'test')`, operation, mode, build, testSchemaDigest); err != nil {
		t.Fatalf("seed the decision of %s: %v", operation, err)
	}
}

func classBuild(t *testing.T, pool *pgxpool.Pool, operation string) string {
	t.Helper()
	var build string
	if err := pool.QueryRow(context.Background(),
		`SELECT current_candidate_build FROM go_api_class_decision WHERE operation = $1`, operation).Scan(&build); err != nil {
		t.Fatal(err)
	}
	return build
}

func countRows(t *testing.T, pool *pgxpool.Pool, table string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM `+table).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func disableClassRequest(op string, apply bool) DisableRequest {
	return DisableRequest{
		SchemaDigest: testSchemaDigest, Operations: []string{op}, DocumentDigest: mcpclass.Digests(), NewMode: "python",
		RecordedBy: "lane-routing-verbs", ReviewEvidence: "CHAOS-8705 disable", Apply: apply,
	}
}

func repointClassRequest(dryRun bool, operations ...string) RepointRequest {
	return RepointRequest{
		SchemaDigest: testSchemaDigest, RunningBuild: verbsRunningBuild, Operations: operations,
		RecordedBy: "lane-routing-verbs", ReviewEvidence: "CHAOS-8705 repoint", PrincipalID: testPrincipalID, DryRun: dryRun,
	}
}

func TestDisableTurnsAClassDecisionOffWithoutTouchingItsBuild(t *testing.T) {
	pool := startAuditedRegistryPostgres(t)
	op := mcpclass.Operation("analytics")
	seedClassDecisionRow(t, pool, op, "canary", testCandidateBuild)

	changes, err := Disable(context.Background(), pool, disableClassRequest(op, true))
	if err != nil {
		t.Fatal(err)
	}
	if len(changes) != 1 || !changes[0].Applied || changes[0].CurrentMode != "canary" {
		t.Fatalf("changes = %+v, want one applied change from canary", changes)
	}
	if mode := classRowMode(t, pool, op); mode != "python" {
		t.Fatalf("mode = %s, want python", mode)
	}
	if build := classBuild(t, pool, op); build != testCandidateBuild {
		t.Fatalf("build = %s: disable must never write the candidate build", build)
	}
	audits := readAuditRows(t, context.Background(), pool)
	if len(audits) != 1 || audits[0].action != AuditActionDisable || audits[0].principalID != nil ||
		audits[0].modeBefore == nil || *audits[0].modeBefore != "canary" || audits[0].modeAfter != "python" {
		t.Fatalf("audit rows = %+v, want one operator-direct disable canary -> python", audits)
	}
}

func TestDisablePlanWritesNothingAndNeverInsertsADecision(t *testing.T) {
	pool := startAuditedRegistryPostgres(t)
	withDecision, without := mcpclass.Operation("analytics"), mcpclass.Operation("hotspots")
	seedClassDecisionRow(t, pool, withDecision, "canary", testCandidateBuild)

	request := disableClassRequest(withDecision, false)
	request.Operations = []string{withDecision, without}
	changes, err := Disable(context.Background(), pool, request)
	if err != nil {
		t.Fatal(err)
	}
	if len(changes) != 2 {
		t.Fatalf("changes = %+v, want the full plan of both operations", changes)
	}
	if mode := classRowMode(t, pool, withDecision); mode != "canary" {
		t.Fatalf("a plan wrote: mode = %s", mode)
	}
	request.Apply = true
	if _, err := Disable(context.Background(), pool, request); err != nil {
		t.Fatal(err)
	}
	if n := countRows(t, pool, "go_api_class_decision"); n != 1 {
		t.Fatalf("%d decisions, want 1: disable must never invent a decision for a root that has none", n)
	}
	if audits := readAuditRows(t, context.Background(), pool); len(audits) != 1 {
		t.Fatalf("%d audit rows, want one for the row actually written", len(audits))
	}
}

func TestDisableGuardRefusesADecisionThatMovedAndWritesNothing(t *testing.T) {
	pool := startAuditedRegistryPostgres(t)
	op := mcpclass.Operation("analytics")
	seedClassDecisionRow(t, pool, op, "canary", testCandidateBuild)

	request := disableClassRequest(op, true)
	request.ExpectedCandidateBuild = verbsRunningBuild
	_, err := Disable(context.Background(), pool, request)
	if !errors.Is(err, ErrDisableGuardMismatch) {
		t.Fatalf("err = %v, want ErrDisableGuardMismatch", err)
	}
	if mode := classRowMode(t, pool, op); mode != "canary" {
		t.Fatalf("mode = %s: a refused guard wrote", mode)
	}
	if n := countRows(t, pool, "go_api_routing_audits"); n != 0 {
		t.Fatalf("%d audit rows after a refusal", n)
	}
}

func TestRepointMovesAClassDecisionsBuildAndKeepsItsMode(t *testing.T) {
	pool := startAuditedRegistryPostgres(t)
	shadow, canary := mcpclass.Operation("analytics"), mcpclass.Operation("hotspots")
	seedClassDecisionRow(t, pool, shadow, "shadow", testCandidateBuild)
	seedClassDecisionRow(t, pool, canary, "canary", testCandidateBuild)

	outcomes, err := Repoint(context.Background(), pool, repointClassRequest(false))
	if err != nil {
		t.Fatal(err)
	}
	if got := Summarize(outcomes); got.Total != 2 || got.Changed != 2 {
		t.Fatalf("summary = %+v, want both moved", got)
	}
	for op, mode := range map[string]string{shadow: "shadow", canary: "canary"} {
		if got := classRowMode(t, pool, op); got != mode {
			t.Fatalf("%s mode = %s, want %s: repoint must never touch reachability", op, got, mode)
		}
		if build := classBuild(t, pool, op); build != verbsRunningBuild {
			t.Fatalf("%s build = %s, want the running build", op, build)
		}
	}
	again, err := Repoint(context.Background(), pool, repointClassRequest(false))
	if err != nil {
		t.Fatal(err)
	}
	if got := Summarize(again); got.Changed != 0 || got.Unchanged != 2 {
		t.Fatalf("a second repoint = %+v, want no change", got)
	}
	if audits := readAuditRows(t, context.Background(), pool); len(audits) != 2 {
		t.Fatalf("%d audit rows, want only the two rows that moved", len(audits))
	}
}

func TestRepointDryRunWritesNothingAndANamedRootWithNoDecisionIsRefused(t *testing.T) {
	pool := startAuditedRegistryPostgres(t)
	op := mcpclass.Operation("analytics")
	seedClassDecisionRow(t, pool, op, "canary", testCandidateBuild)

	if _, err := Repoint(context.Background(), pool, repointClassRequest(true)); err != nil {
		t.Fatal(err)
	}
	if build := classBuild(t, pool, op); build != testCandidateBuild {
		t.Fatalf("a dry run wrote: build = %s", build)
	}
	_, err := Repoint(context.Background(), pool, repointClassRequest(false, op, mcpclass.Operation("hotspots")))
	if !errors.Is(err, ErrRepointUnknownOperation) {
		t.Fatalf("err = %v, want ErrRepointUnknownOperation for a root with no decision", err)
	}
	if build := classBuild(t, pool, op); build != testCandidateBuild {
		t.Fatalf("a refused repoint wrote: build = %s", build)
	}
}

func TestSeedCreatesAShadowDecisionAndNeverChangesAnExistingOne(t *testing.T) {
	pool := startAuditedRegistryPostgres(t)
	fresh, live := mcpclass.Operation("analytics"), mcpclass.Operation("hotspots")
	seedClassDecisionRow(t, pool, live, "canary", testCandidateBuild)
	request := SeedRequest{
		SchemaDigest: testSchemaDigest, RunningBuild: verbsRunningBuild, Operations: []string{fresh},
		DocumentDigest: mcpclass.Digests(), RecordedBy: "lane-routing-verbs", ReviewEvidence: "CHAOS-8705 seed", PrincipalID: testPrincipalID,
	}
	outcomes, err := Seed(context.Background(), pool, request)
	if err != nil || len(outcomes) != 1 || outcomes[0].Action != SeedActionCreated {
		t.Fatalf("outcomes = %+v, err = %v, want one created", outcomes, err)
	}
	if mode := classRowMode(t, pool, fresh); mode != SeedMode {
		t.Fatalf("mode = %s, want shadow", mode)
	}
	again, err := Seed(context.Background(), pool, request)
	if err != nil || again[0].Action != SeedActionAlreadyPresent {
		t.Fatalf("second seed = %+v, err = %v, want already-present", again, err)
	}
	request.Operations = []string{live}
	refused, err := Seed(context.Background(), pool, request)
	if !errors.Is(err, ErrSeedRefused) || refused[0].Action != SeedActionRefused {
		t.Fatalf("seed over a canary decision = %+v, err = %v, want a refusal", refused, err)
	}
	if mode := classRowMode(t, pool, live); mode != "canary" {
		t.Fatalf("seed changed an existing decision: mode = %s", mode)
	}
}

// A catalog operation is served by query-api whatever any row says, so every verb refuses it before touching the database.
func TestEveryVerbRefusesACatalogOperationAndWritesNothing(t *testing.T) {
	pool := startAuditedRegistryPostgres(t)
	ctx := context.Background()
	document, class := "flowMatrix", mcpclass.Operation("analytics")
	seedClassDecisionRow(t, pool, class, "canary", testCandidateBuild)

	enable := EnableRequest{
		SchemaDigest: testSchemaDigest, RunningBuild: verbsRunningBuild, Operations: []string{document},
		DocumentDigest: map[string]string{document: testDocumentDigest}, OperationKinds: queryKinds(document), Mode: "canary",
		RolloutPercentage: EnforcedRolloutPercentage, RecordedBy: "lane-routing-verbs", ReviewEvidence: "CHAOS-8705", PrincipalID: testPrincipalID,
	}
	disable := disableClassRequest(document, true)
	repoint := repointClassRequest(false, document)
	seed := SeedRequest{
		SchemaDigest: testSchemaDigest, RunningBuild: verbsRunningBuild, Operations: []string{document},
		DocumentDigest: map[string]string{document: testDocumentDigest}, RecordedBy: "lane-routing-verbs", ReviewEvidence: "CHAOS-8705", PrincipalID: testPrincipalID,
	}
	_, enableErr := Enable(ctx, pool, enable)
	_, disableErr := Disable(ctx, pool, disable)
	_, repointErr := Repoint(ctx, pool, repoint)
	_, seedErr := Seed(ctx, pool, seed)
	for verb, err := range map[string]error{"enable": enableErr, "disable": disableErr, "repoint": repointErr, "seed": seedErr} {
		if !errors.Is(err, ErrDocumentOperationNotRouted) {
			t.Fatalf("%s = %v, want ErrDocumentOperationNotRouted", verb, err)
		}
	}
	if !errors.Is(enableErr, ErrEnableRequestRefused) || !errors.Is(seedErr, ErrSeedRequestRefused) {
		t.Fatalf("enable and seed must classify the refusal as a request refusal: %v, %v", enableErr, seedErr)
	}

	mixed := disableClassRequest(class, true)
	mixed.Operations = []string{class, document}
	if _, err := Disable(ctx, pool, mixed); !errors.Is(err, errMixedClassAndDocument) {
		t.Fatalf("a request naming both classes of operation = %v, want errMixedClassAndDocument", err)
	}
	if mode := classRowMode(t, pool, class); mode != "canary" {
		t.Fatalf("a refused request changed a decision: mode = %s", mode)
	}
	for _, table := range []string{"go_api_routing_state", "go_api_routing_audits"} {
		if n := countRows(t, pool, table); n != 0 {
			t.Fatalf("%s holds %d row(s) after refusals: a refused verb must write nothing", table, n)
		}
	}
}
