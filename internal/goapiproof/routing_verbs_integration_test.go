//go:build integration

package goapiproof

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const verbsRunningBuild = "ffd9e5d5dc8ee21de5befa1bae47ba9195be135e"

// testDocumentDigest2 is a SECOND registered document, so a test can put
// two operations in one invocation without giving them the same digest --
// the routing table's primary key is (schema, document, operation), and
// two operations sharing a document digest is not a shape the edge's
// catalog can produce (its loader refuses a duplicate digest).
const testDocumentDigest2 = "77c998975b27c6d14f0927c167464edaa01d702a3b1960b7a2f5bfd746f213c2"

// seedProof writes the exact receipt `enable`'s preflight selects on, so
// the two halves meet: what go-api-prove WRITES must be what this verb
// READS. A hand-built INSERT here would prove only that the test agrees
// with itself.
func seedProof(t *testing.T, ctx context.Context, pool *pgxpool.Pool, operation, documentDigest, build, stage, terminalState string) {
	t.Helper()
	if _, err := pool.Exec(ctx, registerCandidateBuildSQL, testSchemaDigest, documentDigest, operation, build); err != nil {
		t.Fatalf("register candidate build for %s: %v", operation, err)
	}
	if _, err := Write(ctx, pool, Receipt{
		SchemaDigest:      testSchemaDigest,
		DocumentDigest:    documentDigest,
		SelectedOperation: operation,
		CandidateBuild:    build,
		RequestIdentity:   "identity-" + operation,
		Stage:             stage,
		TerminalState:     terminalState,
		MeasurementRoute:  RouteEdge,
		BuildBinding:      EdgeBuildPresent,
		RecordedBy:        "lane-routing-verbs",
		ReviewEvidence:    "CHAOS-5486 integration test",
	}); err != nil {
		t.Fatalf("write receipt for %s: %v", operation, err)
	}
}

func readRow(t *testing.T, ctx context.Context, pool *pgxpool.Pool, operation string) (mode, build, evidence, recordedBy string, rollout int) {
	t.Helper()
	if err := pool.QueryRow(ctx, `
		SELECT mode, current_candidate_build, COALESCE(review_evidence, ''), COALESCE(recorded_by, ''), rollout_percentage
		  FROM go_api_routing_state
		 WHERE schema_digest = $1 AND selected_operation = $2`, testSchemaDigest, operation).
		Scan(&mode, &build, &evidence, &recordedBy, &rollout); err != nil {
		t.Fatalf("read %s: %v", operation, err)
	}
	return mode, build, evidence, recordedBy, rollout
}

func enableRequest(operations ...string) EnableRequest {
	digests := map[string]string{}
	for _, operation := range operations {
		digests[operation] = testDocumentDigest
	}
	return EnableRequest{
		PrincipalID:       testPrincipalID,
		SchemaDigest:      testSchemaDigest,
		RunningBuild:      verbsRunningBuild,
		Operations:        operations,
		DocumentDigest:    digests,
		Mode:              "canary",
		RolloutPercentage: 100,
		RecordedBy:        "lane-routing-verbs",
		ReviewEvidence:    "CHAOS-5486 enable",
	}
}

// THE GATE. An operation with no deployed_executed/match receipt for THIS
// build is refused outright: plan section 5 stage 3 requires the exact
// candidate build to have served it through real ingress, auth,
// parse/validate, dispatch and a real database.
func TestEnableRefusesAnOperationWithNoProofForThisBuild(t *testing.T) {
	ctx := t.Context()
	pool := startAuditedRegistryPostgres(t)

	if _, err := Enable(ctx, pool, enableRequest("featureFlags")); !errors.Is(err, ErrEnableUnproven) {
		t.Fatalf("Enable = %v, want ErrEnableUnproven", err)
	}
	var rows int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM go_api_routing_state`).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 0 {
		t.Fatalf("a refused enable wrote %d row(s) -- it must write NOTHING", rows)
	}
}

// A receipt for a DIFFERENT build, or in a non-terminal-match state, is
// not proof. Plan §8.3: a proof is evidence for exactly one 4-column key
// and is never carried forward across any of the four changing.
func TestEnableRefusesAProofRecordedAgainstAnythingElse(t *testing.T) {
	cases := map[string]struct{ build, stage, terminalState string }{
		"an older candidate build": {testCandidateBuild, EnablementProofStage, EnablementProofTerminalState},
		"an earlier stage":         {verbsRunningBuild, "dual_run", EnablementProofTerminalState},
		"a mismatch verdict":       {verbsRunningBuild, EnablementProofStage, "mismatch"},
		"a fallback verdict":       {verbsRunningBuild, EnablementProofStage, "fallback"},
	}
	for name, seed := range cases {
		t.Run(name, func(t *testing.T) {
			ctx := t.Context()
			pool := startAuditedRegistryPostgres(t)
			seedProof(t, ctx, pool, "featureFlags", testDocumentDigest, seed.build, seed.stage, seed.terminalState)

			if _, err := Enable(ctx, pool, enableRequest("featureFlags")); !errors.Is(err, ErrEnableUnproven) {
				t.Fatalf("Enable with %s = %v, want ErrEnableUnproven", name, err)
			}
		})
	}
}

// The happy path, both halves: a real receipt satisfies the gate, and the
// row it authorizes carries the build READ from the running process plus
// the durable who/why.
func TestEnableWritesTheProvenRowWithItsProvenance(t *testing.T) {
	ctx := t.Context()
	pool := startAuditedRegistryPostgres(t)
	seedProof(t, ctx, pool, "featureFlags", testDocumentDigest, verbsRunningBuild, EnablementProofStage, EnablementProofTerminalState)

	outcomes, err := Enable(ctx, pool, enableRequest("featureFlags"))
	if err != nil {
		t.Fatalf("Enable: %v", err)
	}
	if len(outcomes) != 1 || !outcomes[0].Proven {
		t.Fatalf("outcomes = %+v, want one PROVEN outcome", outcomes)
	}
	mode, build, evidence, recordedBy, rollout := readRow(t, ctx, pool, "featureFlags")
	if mode != "canary" || build != verbsRunningBuild || rollout != 100 {
		t.Fatalf("row = mode=%q build=%q rollout=%d, want canary/%s/100", mode, build, rollout, verbsRunningBuild)
	}
	if recordedBy != "lane-routing-verbs" || evidence != "CHAOS-5486 enable" {
		t.Fatalf("provenance not recorded: recorded_by=%q review_evidence=%q", recordedBy, evidence)
	}
	// A proven row must NOT carry the acknowledged prefix -- the marker is
	// what makes an unproven enablement findable, and applying it to
	// everything would make it mean nothing.
	if got := evidence; len(got) >= len(UnprovenEvidencePrefix) && got[:len(UnprovenEvidencePrefix)] == UnprovenEvidencePrefix {
		t.Fatalf("a PROVEN row must not be marked ACKNOWLEDGED-UNPROVEN: %q", got)
	}
}

// -acknowledge-unproven writes the reason DURABLY on the row, so `status`
// can report it for as long as the enablement is in force. On 2026-09-07
// fifteen operations were enabled on a ruling that lived in a chat
// message; this is the fix for that.
func TestEnableAcknowledgedUnprovenMarksTheRowDurably(t *testing.T) {
	ctx := t.Context()
	pool := startAuditedRegistryPostgres(t)

	request := enableRequest("featureFlags")
	request.AcknowledgeUnproven = true
	outcomes, err := Enable(ctx, pool, request)
	if err != nil {
		t.Fatalf("Enable: %v", err)
	}
	if len(outcomes) != 1 || outcomes[0].Proven {
		t.Fatalf("outcomes = %+v, want one UNPROVEN outcome", outcomes)
	}
	_, _, evidence, _, _ := readRow(t, ctx, pool, "featureFlags")
	if evidence != UnprovenEvidencePrefix+"CHAOS-5486 enable" {
		t.Fatalf("review_evidence = %q, want the ACKNOWLEDGED-UNPROVEN prefix in front of the operator's own words", evidence)
	}

	statuses, err := RoutingStatusRows(ctx, pool, testSchemaDigest, map[string]string{"featureFlags": testDocumentDigest})
	if err != nil {
		t.Fatal(err)
	}
	if len(statuses) != 1 || statuses[0].Proven {
		t.Fatalf("status = %+v, want the row reported UNPROVEN for as long as it is in force", statuses)
	}
	if !statuses[0].Reachable() {
		t.Fatal("an acknowledged-unproven canary row IS reachable -- that is exactly why status must keep flagging it")
	}
}

// A dry run runs the whole gate and writes nothing. "It would have been
// refused" and "it would have been written" must be distinguishable
// BEFORE anything is written.
func TestEnableDryRunRunsTheGateAndWritesNothing(t *testing.T) {
	ctx := t.Context()
	pool := startAuditedRegistryPostgres(t)
	seedProof(t, ctx, pool, "featureFlags", testDocumentDigest, verbsRunningBuild, EnablementProofStage, EnablementProofTerminalState)

	request := enableRequest("featureFlags")
	request.DryRun = true
	if _, err := Enable(ctx, pool, request); err != nil {
		t.Fatalf("Enable dry-run: %v", err)
	}
	var rows int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM go_api_routing_state`).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 0 {
		t.Fatalf("a dry run wrote %d routing row(s)", rows)
	}

	request.DryRun = false
	request.AcknowledgeUnproven = false
	request.Operations = []string{"featureFlags", "hotspots"}
	request.DocumentDigest["hotspots"] = "1111111111111111111111111111111111111111111111111111111111111111"
	if _, err := Enable(ctx, pool, request); !errors.Is(err, ErrEnableUnproven) {
		t.Fatalf("Enable = %v, want ErrEnableUnproven for the unproven half", err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM go_api_routing_state`).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 0 {
		t.Fatalf("a partially-unproven enable wrote %d row(s) -- it is all or nothing", rows)
	}
}

// A re-run of the same enablement is a no-op in effect, not an error: an
// operator recovering from a digest move must be able to run the same
// command twice without thinking about it.
func TestEnableIsIdempotentOnItsOwnPrimaryKey(t *testing.T) {
	ctx := t.Context()
	pool := startAuditedRegistryPostgres(t)
	seedProof(t, ctx, pool, "featureFlags", testDocumentDigest, verbsRunningBuild, EnablementProofStage, EnablementProofTerminalState)

	for attempt := 0; attempt < 2; attempt++ {
		if _, err := Enable(ctx, pool, enableRequest("featureFlags")); err != nil {
			t.Fatalf("Enable attempt %d: %v", attempt+1, err)
		}
	}
	var rows int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM go_api_routing_state`).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 1 {
		t.Fatalf("two identical enables produced %d rows, want 1", rows)
	}
}

// The off-ramp. It must work with no query-api, no credential and no
// candidate build -- only a database.
func TestDisableTurnsARowOffWithoutTouchingItsBuild(t *testing.T) {
	ctx := t.Context()
	pool := startAuditedRegistryPostgres(t)
	seedRow(t, ctx, "featureFlags", testDocumentDigest, "primary", verbsRunningBuild, pool)

	changes, err := Disable(ctx, pool, DisableRequest{
		SchemaDigest:   testSchemaDigest,
		Operations:     []string{"featureFlags"},
		DocumentDigest: map[string]string{"featureFlags": testDocumentDigest},
		NewMode:        "disabled",
		RecordedBy:     "lane-routing-verbs",
		ReviewEvidence: "CHAOS-5486 rollback",
		Apply:          true,
	})
	if err != nil {
		t.Fatalf("Disable: %v", err)
	}
	if len(changes) != 1 || !changes[0].Applied {
		t.Fatalf("changes = %+v, want one applied change", changes)
	}
	mode, build, evidence, recordedBy, _ := readRow(t, ctx, pool, "featureFlags")
	if mode != "disabled" {
		t.Fatalf("mode = %q, want disabled", mode)
	}
	if build != verbsRunningBuild {
		t.Fatalf("current_candidate_build = %q, want it UNCHANGED at %q -- disable changes mode only", build, verbsRunningBuild)
	}
	if recordedBy != "lane-routing-verbs" || evidence != "CHAOS-5486 rollback" {
		t.Fatalf("provenance not recorded: recorded_by=%q review_evidence=%q", recordedBy, evidence)
	}
}

// A dry run reports the whole plan -- including the rows it will not
// touch -- and writes nothing.
func TestDisableDryRunReportsEveryRowAndWritesNothing(t *testing.T) {
	ctx := t.Context()
	pool := startAuditedRegistryPostgres(t)
	seedRow(t, ctx, "featureFlags", testDocumentDigest, "canary", verbsRunningBuild, pool)

	changes, err := Disable(ctx, pool, DisableRequest{
		SchemaDigest: testSchemaDigest,
		Operations:   []string{"featureFlags", "hotspots"},
		DocumentDigest: map[string]string{
			"featureFlags": testDocumentDigest,
			"hotspots":     "1111111111111111111111111111111111111111111111111111111111111111",
		},
		NewMode: "python",
	})
	if err != nil {
		t.Fatalf("Disable dry-run: %v", err)
	}
	summary := SummarizeDisable(changes)
	if summary.Total != 2 || summary.Actionable != 1 || summary.NoRow != 1 || summary.Applied != 0 {
		t.Fatalf("summary = %+v, want total=2 actionable=1 norow=1 applied=0", summary)
	}
	mode, _, _, _, _ := readRow(t, ctx, pool, "featureFlags")
	if mode != "canary" {
		t.Fatalf("a dry run changed the row to %q", mode)
	}
}

// disable must NEVER insert. Writing a `python` row for an operation that
// was never enabled manufactures history.
func TestDisableNeverInsertsARowForAnOperationThatHasNone(t *testing.T) {
	ctx := t.Context()
	pool := startAuditedRegistryPostgres(t)

	changes, err := Disable(ctx, pool, DisableRequest{
		SchemaDigest:   testSchemaDigest,
		Operations:     []string{"featureFlags"},
		DocumentDigest: map[string]string{"featureFlags": testDocumentDigest},
		NewMode:        "python",
		RecordedBy:     "lane-routing-verbs",
		ReviewEvidence: "CHAOS-5486 rollback",
		Apply:          true,
	})
	if err != nil {
		t.Fatalf("Disable: %v", err)
	}
	if len(changes) != 1 || changes[0].Applied || changes[0].CurrentMode != "" {
		t.Fatalf("changes = %+v, want one unapplied no-row change", changes)
	}
	var rows int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM go_api_routing_state`).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 0 {
		t.Fatalf("disable inserted %d row(s) -- turning something off must never turn something on", rows)
	}
}

// The -candidate-build guard: "refuse if somebody repointed this row
// since I looked". It is never WRITTEN, only compared.
func TestDisableGuardRefusesARowThatMovedAndWritesNothing(t *testing.T) {
	ctx := t.Context()
	pool := startAuditedRegistryPostgres(t)
	seedRow(t, ctx, "featureFlags", testDocumentDigest, "canary", verbsRunningBuild, pool)

	_, err := Disable(ctx, pool, DisableRequest{
		SchemaDigest:           testSchemaDigest,
		Operations:             []string{"featureFlags"},
		DocumentDigest:         map[string]string{"featureFlags": testDocumentDigest},
		NewMode:                "python",
		ExpectedCandidateBuild: testCandidateBuild,
		RecordedBy:             "lane-routing-verbs",
		ReviewEvidence:         "CHAOS-5486 rollback",
		Apply:                  true,
	})
	if !errors.Is(err, ErrDisableGuardMismatch) {
		t.Fatalf("Disable = %v, want ErrDisableGuardMismatch", err)
	}
	mode, build, _, _, _ := readRow(t, ctx, pool, "featureFlags")
	if mode != "canary" || build != verbsRunningBuild {
		t.Fatalf("a refused guarded disable changed the row: mode=%q build=%q", mode, build)
	}
}

// status classifies against the LIVE digest. A row at another digest is
// STALE -- present in psql, unreachable in fact, which is exactly how the
// CHAOS-5416 outage stayed invisible for six days.
func TestRoutingStatusRowsSeparatesMatchStaleAndMissing(t *testing.T) {
	ctx := t.Context()
	pool := startAuditedRegistryPostgres(t)
	seedRow(t, ctx, "featureFlags", testDocumentDigest, "canary", verbsRunningBuild, pool)

	const staleDigest = "sha256:0000000000000000000000000000000000000000000000000000000000000000"
	if _, err := pool.Exec(ctx, registerCandidateBuildSQL, staleDigest, testDocumentDigest, "hotspots", testCandidateBuild); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO go_api_routing_state
			(schema_digest, document_digest, selected_operation, current_candidate_build, owner, mode, rollout_percentage)
		VALUES ($1, $2, 'hotspots', $3, 'go', 'canary', 100)`,
		staleDigest, testDocumentDigest, testCandidateBuild); err != nil {
		t.Fatal(err)
	}

	catalog := map[string]string{
		"featureFlags": testDocumentDigest,
		"hotspots":     testDocumentDigest,
		"flowMatrix":   "1111111111111111111111111111111111111111111111111111111111111111",
	}
	statuses, err := RoutingStatusRows(ctx, pool, testSchemaDigest, catalog)
	if err != nil {
		t.Fatalf("RoutingStatusRows: %v", err)
	}
	byOperation := map[string]OperationStatus{}
	for _, status := range statuses {
		byOperation[status.Operation] = status
	}
	if got := byOperation["featureFlags"]; got.DigestState != DigestMatch || !got.Reachable() {
		t.Fatalf("featureFlags = %+v, want MATCH and reachable", got)
	}
	if got := byOperation["hotspots"]; got.DigestState != DigestStale || got.Reachable() {
		t.Fatalf("hotspots = %+v, want STALE and NOT reachable", got)
	} else if len(got.StaleDigests) != 1 || got.StaleDigests[0] != staleDigest {
		t.Fatalf("hotspots stale digests = %v, want [%s]", got.StaleDigests, staleDigest)
	}
	if got := byOperation["flowMatrix"]; got.DigestState != DigestMissing {
		t.Fatalf("flowMatrix = %+v, want MISSING -- an operation with no row anywhere must be REPORTED, not silently absent", got)
	}

	counts, err := CountRowsBySchemaDigest(ctx, pool)
	if err != nil {
		t.Fatalf("CountRowsBySchemaDigest: %v", err)
	}
	if counts[testSchemaDigest] != 1 || counts[staleDigest] != 1 {
		t.Fatalf("census = %v, want one row at each digest -- the census is what surfaces the digests nobody asked about", counts)
	}
}

// A row still naming an OLDER build must not borrow a newer build's
// proof: the proof key is four columns and is never carried forward
// across any of them changing (plan §8.3).
func TestRoutingStatusRowsNeverBorrowsAnotherBuildsProof(t *testing.T) {
	ctx := t.Context()
	pool := startAuditedRegistryPostgres(t)
	seedRow(t, ctx, "featureFlags", testDocumentDigest, "canary", testCandidateBuild, pool)
	seedProof(t, ctx, pool, "featureFlags", testDocumentDigest, verbsRunningBuild, EnablementProofStage, EnablementProofTerminalState)

	statuses, err := RoutingStatusRows(ctx, pool, testSchemaDigest, map[string]string{"featureFlags": testDocumentDigest})
	if err != nil {
		t.Fatal(err)
	}
	if len(statuses) != 1 || statuses[0].Proven {
		t.Fatalf("status = %+v: a row pointing at %s must NOT be proven by a receipt for %s",
			statuses, testCandidateBuild, verbsRunningBuild)
	}
}

// `status`'s proof check must use the SAME route rule `enable` would if
// asked to write into this row's own mode: a `primary` row is held to the
// strict edge-only rule, every other mode (canary here) to the permissive
// any-route rule. One receipt, recorded over the non-edge `RouteProof`
// route, must therefore prove a canary row and leave a primary row at the
// identical build UNPROVEN.
func TestRoutingStatusRowsAppliesThePrimaryRowsStricterRouteRule(t *testing.T) {
	ctx := t.Context()
	pool := startRegistryPostgres(t)
	seedRow(t, ctx, "featureFlags", testDocumentDigest, "canary", testCandidateBuild, pool)
	seedRow(t, ctx, "hotspots", testDocumentDigest, "primary", testCandidateBuild, pool)
	for _, operation := range []string{"featureFlags", "hotspots"} {
		if _, err := Write(ctx, pool, Receipt{
			SchemaDigest:      testSchemaDigest,
			DocumentDigest:    testDocumentDigest,
			SelectedOperation: operation,
			CandidateBuild:    testCandidateBuild,
			RequestIdentity:   "identity-" + operation,
			Stage:             EnablementProofStage,
			TerminalState:     EnablementProofTerminalState,
			MeasurementRoute:  RouteProof,
			BuildBinding:      EdgeBuildPresent,
			RecordedBy:        "lane-routing-verbs",
			ReviewEvidence:    "route-rule split integration test",
		}); err != nil {
			t.Fatalf("write receipt for %s: %v", operation, err)
		}
	}

	catalog := map[string]string{"featureFlags": testDocumentDigest, "hotspots": testDocumentDigest}
	statuses, err := RoutingStatusRows(ctx, pool, testSchemaDigest, catalog)
	if err != nil {
		t.Fatalf("RoutingStatusRows: %v", err)
	}
	byOperation := map[string]OperationStatus{}
	for _, status := range statuses {
		byOperation[status.Operation] = status
	}
	if got := byOperation["featureFlags"]; !got.Proven {
		t.Fatalf("featureFlags (canary) = %+v, want Proven -- the any-route rule admits a %s-route receipt", got, RouteProof)
	}
	if got := byOperation["hotspots"]; got.Proven {
		t.Fatalf("hotspots (primary) = %+v, want UNPROVEN -- the edge-only rule must refuse a %s-route receipt", got, RouteProof)
	}
}

// A DELIBERATE tightening over the Python verb, and the docs claim it, so
// it is pinned here.
//
// go_api_routing_admin.plan_disable builds every ModeChange with the
// CATALOG's document digest and then keys apply_disable's UPDATE on it --
// so a row whose document digest has drifted from the catalog is reported
// as present, matches nothing, and the off-ramp silently does not work on
// exactly the row something has already gone wrong with. This verb keys
// the UPDATE on the ROW's own document digest instead.
func TestDisableTurnsOffARowWhoseDocumentDigestDriftedFromTheCatalog(t *testing.T) {
	ctx := t.Context()
	pool := startAuditedRegistryPostgres(t)
	const driftedDigest = "2222222222222222222222222222222222222222222222222222222222222222"
	seedRow(t, ctx, "featureFlags", driftedDigest, "canary", verbsRunningBuild, pool)

	changes, err := Disable(ctx, pool, DisableRequest{
		SchemaDigest: testSchemaDigest,
		Operations:   []string{"featureFlags"},
		// The CATALOG's digest, which is NOT what the row carries.
		DocumentDigest: map[string]string{"featureFlags": testDocumentDigest},
		NewMode:        "python",
		RecordedBy:     "lane-routing-verbs",
		ReviewEvidence: "CHAOS-5486 rollback across a drifted document digest",
		Apply:          true,
	})
	if err != nil {
		t.Fatalf("Disable: %v", err)
	}
	if len(changes) != 1 || !changes[0].Applied {
		t.Fatalf("changes = %+v, want the drifted row turned OFF -- an off-ramp that stops working when something drifted has a hole in it", changes)
	}
	mode, _, _, _, _ := readRow(t, ctx, pool, "featureFlags")
	if mode != "python" {
		t.Fatalf("mode = %q, want python", mode)
	}
}

// The routing table's primary key is
// (schema_digest, document_digest, selected_operation), so ONE operation
// can legitimately have several rows at the live digest under different
// document digests -- a state a document-digest change produces without
// cleaning up the old row.
//
// Both planes keyed their maps by operation alone and silently kept
// whichever row came last. For `disable` that is the worst possible
// failure: it reports success while leaving a reachable row behind.
func TestDisableTurnsOffEveryRowAnOperationHasAtTheLiveDigest(t *testing.T) {
	ctx := t.Context()
	pool := startAuditedRegistryPostgres(t)
	const otherDigest = "5555555555555555555555555555555555555555555555555555555555555555"
	seedRow(t, ctx, "featureFlags", testDocumentDigest, "canary", verbsRunningBuild, pool)
	seedRow(t, ctx, "featureFlags", otherDigest, "primary", verbsRunningBuild, pool)

	changes, err := Disable(ctx, pool, DisableRequest{
		SchemaDigest:   testSchemaDigest,
		Operations:     []string{"featureFlags"},
		DocumentDigest: map[string]string{"featureFlags": testDocumentDigest},
		NewMode:        "python",
		RecordedBy:     "lane-routing-verbs",
		ReviewEvidence: "CHAOS-5486 r1 F5",
		Apply:          true,
	})
	if err != nil {
		t.Fatalf("Disable: %v", err)
	}
	if summary := SummarizeDisable(changes); summary.Applied != 2 {
		t.Fatalf("summary = %+v, want BOTH rows applied -- an off-ramp that leaves one reachable row behind reports success and is wrong", summary)
	}

	rows, err := pool.Query(ctx, `
		SELECT document_digest, mode FROM go_api_routing_state
		 WHERE schema_digest = $1 AND selected_operation = 'featureFlags'
		 ORDER BY document_digest`, testSchemaDigest)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	seen := 0
	for rows.Next() {
		var documentDigest, mode string
		if err := rows.Scan(&documentDigest, &mode); err != nil {
			t.Fatal(err)
		}
		if mode != "python" {
			t.Fatalf("row %s is still %q -- every row for the operation must be turned off", documentDigest, mode)
		}
		seen++
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if seen != 2 {
		t.Fatalf("read %d rows, want 2 -- the fixture itself must carry the duplicate this test is about", seen)
	}
}

// The status half of the same fix.
//
// Only ONE row per operation is reachable: the edge resolves a request to
// an operation through the catalog, then looks the row up by the
// CATALOG's document digest. Every other row at the live schema digest is
// dead in exactly the way a stale schema digest is dead. r1 fixed the
// arbitrary-pick half (a reader keeping whichever row came last); r2
// found that a row with a NON-catalog document digest was still being
// promoted to MATCH, and could be reported REACHABLE, when nothing can
// reach it.
func TestStatusReportsTheReachableRowAndNamesTheRestAsUnreachable(t *testing.T) {
	ctx := t.Context()
	pool := startRegistryPostgres(t)
	const otherDigest = "5555555555555555555555555555555555555555555555555555555555555555"
	seedRow(t, ctx, "featureFlags", testDocumentDigest, "canary", verbsRunningBuild, pool)
	seedRow(t, ctx, "featureFlags", otherDigest, "primary", testCandidateBuild, pool)

	statuses, err := RoutingStatusRows(ctx, pool, testSchemaDigest,
		map[string]string{"featureFlags": testDocumentDigest})
	if err != nil {
		t.Fatalf("RoutingStatusRows: %v", err)
	}
	if len(statuses) != 1 {
		t.Fatalf("statuses = %+v, want one per CATALOG operation", statuses)
	}
	got := statuses[0]
	if got.DigestState != DigestMatch || got.Mode != "canary" {
		t.Fatalf("status = %+v, want the CATALOG-digest row reported as MATCH", got)
	}
	if len(got.UnreachableDocumentDigests) != 1 || got.UnreachableDocumentDigests[0] != otherDigest {
		t.Fatalf("unreachable = %v, want [%s] -- the other row exists and an operator must be able to see it", got.UnreachableDocumentDigests, otherDigest)
	}
	if !got.Reachable() {
		t.Fatal("the catalog-digest canary row IS reachable")
	}
}

// r2 R2-03, the half r1 missed: a LONE row at the live schema digest
// whose document digest is not the catalog's. Nothing can reach it -- the
// edge never looks that key up -- so reporting MATCH, and worse
// REACHABLE, is the CHAOS-5416 lie in a new column.
func TestALoneRowWithTheWrongDocumentDigestIsStaleNotMatch(t *testing.T) {
	ctx := t.Context()
	pool := startRegistryPostgres(t)
	const otherDigest = "5555555555555555555555555555555555555555555555555555555555555555"
	seedRow(t, ctx, "featureFlags", otherDigest, "primary", verbsRunningBuild, pool)

	statuses, err := RoutingStatusRows(ctx, pool, testSchemaDigest,
		map[string]string{"featureFlags": testDocumentDigest})
	if err != nil {
		t.Fatalf("RoutingStatusRows: %v", err)
	}
	got := statuses[0]
	if got.DigestState != DigestStale {
		t.Fatalf("digest_state = %q, want STALE -- the edge looks this operation up by the CATALOG's document digest, so this row is never consulted", got.DigestState)
	}
	if got.Reachable() {
		t.Fatal("a row the edge can never look up must NEVER be reported reachable, whatever its mode says")
	}
	if len(got.UnreachableDocumentDigests) != 1 || got.UnreachableDocumentDigests[0] != otherDigest {
		t.Fatalf("unreachable = %v, want [%s]", got.UnreachableDocumentDigests, otherDigest)
	}
	if got.Mode != "" {
		t.Fatalf("mode = %q: a STALE row's mode must not be reported as if it decided anything", got.Mode)
	}
}

// r2 R2-07: a filter naming an operation with no row must refuse, not
// re-point the rest and report success. `enable` and `disable` already
// refused an unknown name via the catalog; `repoint` takes raw names and
// did not.
func TestRepointRefusesAFilterNamingAnOperationWithNoRow(t *testing.T) {
	ctx := t.Context()
	pool := startRegistryPostgres(t)
	seedRow(t, ctx, "featureFlags", testDocumentDigest, "shadow", testCandidateBuild, pool)

	_, err := Repoint(ctx, pool, RepointRequest{
		PrincipalID:    testPrincipalID,
		SchemaDigest:   testSchemaDigest,
		RunningBuild:   verbsRunningBuild,
		Operations:     []string{"featureFlags", "typo"},
		RecordedBy:     "lane-routing-verbs",
		ReviewEvidence: "CHAOS-5486 r2 R2-07",
	})
	if !errors.Is(err, ErrRepointUnknownOperation) {
		t.Fatalf("Repoint = %v, want ErrRepointUnknownOperation", err)
	}
	if !strings.Contains(err.Error(), "typo") {
		t.Fatalf("the refusal must name the operation that has no row, got %q", err)
	}
	// And NOTHING moved: the check runs before any write, so a filter
	// with a typo in it changes nothing at all.
	var build string
	if err := pool.QueryRow(ctx, `
		SELECT current_candidate_build FROM go_api_routing_state
		 WHERE schema_digest = $1 AND selected_operation = 'featureFlags'`, testSchemaDigest).
		Scan(&build); err != nil {
		t.Fatal(err)
	}
	if build != testCandidateBuild {
		t.Fatalf("featureFlags moved to %q despite the refusal -- the check must precede every write", build)
	}
}

// r2 R2-08, reproduced then fixed. `disable` read its rows WITHOUT a
// lock, so a concurrent `enable` could commit between the read and the
// write and leave the operation reachable while `disable` reported
// success. For an off-ramp that is the worst failure available.
//
// The interleaving is driven deterministically: an `enable`-shaped
// transaction holds the row locked, `disable` is launched and blocks, and
// only then does the holder commit. Under the unlocked read `disable`
// planned against the pre-enable state and reported a change it had not
// really made; under the locking read it blocks, then sees and writes the
// post-enable state.
func TestDisableCannotReportSuccessOverAConcurrentEnable(t *testing.T) {
	ctx := t.Context()
	pool := startAuditedRegistryPostgres(t)
	seedRow(t, ctx, "featureFlags", testDocumentDigest, "python", testCandidateBuild, pool)

	holder, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// Trap #110: an explicit commit below must not be the only release.
	defer func() { _ = holder.Rollback(ctx) }()
	if _, err := holder.Exec(ctx, registerCandidateBuildSQL,
		testSchemaDigest, testDocumentDigest, "featureFlags", verbsRunningBuild); err != nil {
		t.Fatal(err)
	}
	if _, err := holder.Exec(ctx, upsertRoutingStateSQL,
		testSchemaDigest, testDocumentDigest, "featureFlags", verbsRunningBuild,
		"canary", 100, "concurrent enable", "other-operator", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}

	type result struct {
		changes []DisableChange
		err     error
	}
	done := make(chan result, 1)
	go func() {
		changes, err := Disable(ctx, pool, DisableRequest{
			SchemaDigest:   testSchemaDigest,
			Operations:     []string{"featureFlags"},
			DocumentDigest: map[string]string{"featureFlags": testDocumentDigest},
			NewMode:        "python",
			RecordedBy:     "lane-routing-verbs",
			ReviewEvidence: "CHAOS-5486 r2 R2-08",
			Apply:          true,
		})
		done <- result{changes, err}
	}()

	waitForALockWaiter(t, ctx, pool)
	if err := holder.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	got := <-done
	if got.err != nil {
		t.Fatalf("Disable: %v", got.err)
	}
	if summary := SummarizeDisable(got.changes); summary.Applied != 1 {
		t.Fatalf("summary = %+v, want the row applied", summary)
	}
	// The whole point: whatever `disable` reported, the row must actually
	// be unreachable afterwards.
	var mode string
	if err := pool.QueryRow(ctx, `
		SELECT mode FROM go_api_routing_state
		 WHERE schema_digest = $1 AND selected_operation = 'featureFlags'`, testSchemaDigest).
		Scan(&mode); err != nil {
		t.Fatal(err)
	}
	if mode != "python" {
		t.Fatalf("mode = %q after a disable that reported success -- the enable won, and the off-ramp lied", mode)
	}
	// And the plan it reported must describe what it ACTUALLY replaced --
	// the concurrent enable's canary, not the pre-enable python it would
	// have seen through an unlocked read.
	if got.changes[0].CurrentMode != "canary" {
		t.Fatalf("reported previous mode %q, want canary -- an unlocked read reports the state it saw before the other writer committed", got.changes[0].CurrentMode)
	}
}

// waitForALockWaiter blocks until some backend in this database is
// waiting on a lock, so a concurrency test can act at a KNOWN point
// instead of after a sleep long enough to "probably" work.
func waitForALockWaiter(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		var waiting int
		if err := pool.QueryRow(ctx, `
			SELECT count(*) FROM pg_stat_activity
			 WHERE datname = current_database()
			   AND wait_event_type = 'Lock'`).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting > 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("no backend ever blocked on a lock: the interleaving this test needs did not happen, so it proves nothing")
}

// codex r3 CONC-01. The mode assertion at the end of a re-point exists to
// prove the verb never touched reachability. Keyed by operation alone it
// collapsed the several rows one operation can have at a schema digest,
// so it could compare one row's mode against another row's before-value
// -- an assertion that can compare the wrong row proves nothing.
//
// Two rows for one operation, in DIFFERENT modes, is the shape that
// discriminates: with the collapsing map the assertion sees one mode for
// both outcomes and the re-point either fails spuriously or passes
// vacuously, depending which row won.
// The astra shape, reproduced exactly: ONE operation carrying one
// `canary` row and one `primary` row at the same schema digest.
//
// This is the sibling of the test below and not a duplicate of it. That
// one seeds `shadow` + `canary`, and under the defective keying it fails
// on `featureFlags` first -- `Repoint` returns on the first refusal, so
// the operation named in the message is whichever sorts earliest, and the
// specific line an astra round reported against main,
//
//	goapiproof: flowMatrix mode changed "canary" -> "primary" during a
//	re-point, which must never touch reachability
//
// never appears. A killer for a REPORTED defect has to produce the
// reported failure, not a cousin of it, so this seeds that pair alone.
//
// canary and primary are also the only two modes `go_api_dispatcher.py`
// treats as REACHABLE, which is what makes this pair the expensive one:
// the refusal aborts the whole transaction, so an operator mid-rollout
// gets a re-point that writes nothing and blames a reachability change
// that never happened -- on a statement whose SET list does not carry
// `mode` at all.
func TestRepointNeverBorrowsASiblingRowsModeAcrossTheTwoReachableModes(t *testing.T) {
	ctx := t.Context()
	pool := startRegistryPostgres(t)
	const canaryDigest = "7777777777777777777777777777777777777777777777777777777777777777"
	const primaryDigest = "8888888888888888888888888888888888888888888888888888888888888888"
	seedRow(t, ctx, "flowMatrix", canaryDigest, "canary", testCandidateBuild, pool)
	seedRow(t, ctx, "flowMatrix", primaryDigest, "primary", testCandidateBuild, pool)

	outcomes, err := Repoint(ctx, pool, RepointRequest{
		SchemaDigest:   testSchemaDigest,
		RunningBuild:   verbsRunningBuild,
		RecordedBy:     "lane-routing-verbs",
		ReviewEvidence: "CHAOS-5486: the astra canary/primary re-point shape",
	})
	if err != nil {
		t.Fatalf("Repoint: %v", err)
	}
	if len(outcomes) != 2 {
		t.Fatalf("outcomes = %+v, want one per ROW", outcomes)
	}
	byDigest := map[string]RepointOutcome{}
	for _, outcome := range outcomes {
		byDigest[outcome.DocumentDigest] = outcome
	}
	// Both directions. The database chooses the read's row order within a
	// tie, so a test asserting only the row that fails under one order
	// catches the defect only half the time.
	if got := byDigest[canaryDigest]; got.ModeBefore != "canary" || got.ModeAfter != "canary" {
		t.Fatalf("canary row = %+v, want canary on both sides -- this is the row reported as changed \"canary\" -> \"primary\"", got)
	}
	if got := byDigest[primaryDigest]; got.ModeBefore != "primary" || got.ModeAfter != "primary" {
		t.Fatalf("primary row = %+v, want primary on both sides", got)
	}

	// ...and the durable state agrees: both builds moved, neither mode did.
	rows, err := pool.Query(ctx, `
		SELECT document_digest, mode, current_candidate_build FROM go_api_routing_state
		 WHERE schema_digest = $1 AND selected_operation = 'flowMatrix'
		 ORDER BY document_digest`, testSchemaDigest)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	seen := map[string]string{}
	for rows.Next() {
		var documentDigest, mode, build string
		if err := rows.Scan(&documentDigest, &mode, &build); err != nil {
			t.Fatal(err)
		}
		if build != verbsRunningBuild {
			t.Fatalf("row %s did not move: build = %q", documentDigest, build)
		}
		seen[documentDigest] = mode
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(seen) != 2 || seen[canaryDigest] != "canary" || seen[primaryDigest] != "primary" {
		t.Fatalf("modes after the re-point = %v, want each row's own mode preserved", seen)
	}
}

func TestRepointAssertsEachDuplicateRowsModeAgainstItsOwnBeforeValue(t *testing.T) {
	ctx := t.Context()
	pool := startAuditedRegistryPostgres(t)
	const otherDigest = "6666666666666666666666666666666666666666666666666666666666666666"
	seedRow(t, ctx, "featureFlags", testDocumentDigest, "shadow", testCandidateBuild, pool)
	seedRow(t, ctx, "featureFlags", otherDigest, "canary", testCandidateBuild, pool)

	outcomes, err := Repoint(ctx, pool, RepointRequest{
		PrincipalID:    testPrincipalID,
		SchemaDigest:   testSchemaDigest,
		RunningBuild:   verbsRunningBuild,
		RecordedBy:     "lane-routing-verbs",
		ReviewEvidence: "CHAOS-5486 r3 CONC-01",
	})
	if err != nil {
		t.Fatalf("Repoint: %v", err)
	}
	if len(outcomes) != 2 {
		t.Fatalf("outcomes = %+v, want one per ROW -- an operation with two rows has two of them", outcomes)
	}
	// Each outcome must carry its own row's identity and its own mode.
	byDigest := map[string]RepointOutcome{}
	for _, outcome := range outcomes {
		if outcome.DocumentDigest == "" {
			t.Fatalf("outcome %+v carries no document digest, so it cannot be matched back to the row it describes", outcome)
		}
		byDigest[outcome.DocumentDigest] = outcome
	}
	if got := byDigest[testDocumentDigest]; got.ModeBefore != "shadow" || got.ModeAfter != "shadow" {
		t.Fatalf("catalog-digest row = %+v, want shadow on both sides", got)
	}
	if got := byDigest[otherDigest]; got.ModeBefore != "canary" || got.ModeAfter != "canary" {
		t.Fatalf("other-digest row = %+v, want canary on both sides -- its mode must be asserted against ITS OWN before-value, not the sibling row's", got)
	}

	// And both rows actually moved, with their modes untouched.
	rows, err := pool.Query(ctx, `
		SELECT document_digest, mode, current_candidate_build FROM go_api_routing_state
		 WHERE schema_digest = $1 AND selected_operation = 'featureFlags'
		 ORDER BY document_digest`, testSchemaDigest)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	seen := map[string]string{}
	for rows.Next() {
		var documentDigest, mode, build string
		if err := rows.Scan(&documentDigest, &mode, &build); err != nil {
			t.Fatal(err)
		}
		if build != verbsRunningBuild {
			t.Fatalf("row %s did not move: build = %q", documentDigest, build)
		}
		seen[documentDigest] = mode
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if seen[testDocumentDigest] != "shadow" || seen[otherDigest] != "canary" {
		t.Fatalf("modes after the re-point = %v, want each row's own mode preserved", seen)
	}
}

// The shared read returns rows in (selected_operation, document_digest)
// order, proved by READING THEM from a real server.
//
// r1's M12 dropped `document_digest` from the ORDER BY while leaving the
// phrase "ORDER BY selected_operation, document_digest" in the SQL
// COMMENT beside it, and the previous version of this pin -- which
// searched the SQL TEXT for that phrase -- stayed green. A guard that
// inspects text instead of behaviour is not a guard; it is a spell
// checker for comments.
//
// The seeded order is deliberately the REVERSE of the expected one within
// each operation, and the operations themselves are inserted out of
// order, so an unordered or partially-ordered read cannot come back
// correct by insertion accident. Postgres is free to return an unordered
// query in any order at all, which is precisely why the ORDER BY has to
// be proved rather than read.
func TestTheSharedReadReturnsRowsInTheirFullIdentityOrder(t *testing.T) {
	ctx := t.Context()
	pool := startRegistryPostgres(t)

	const (
		digestA = "1111111111111111111111111111111111111111111111111111111111111111"
		digestB = "2222222222222222222222222222222222222222222222222222222222222222"
		digestC = "3333333333333333333333333333333333333333333333333333333333333333"
	)
	// Inserted worst-first for the intended order.
	seedRow(t, ctx, "hotspots", digestC, "canary", testCandidateBuild, pool)
	seedRow(t, ctx, "hotspots", digestA, "shadow", testCandidateBuild, pool)
	seedRow(t, ctx, "featureFlags", digestC, "primary", testCandidateBuild, pool)
	seedRow(t, ctx, "featureFlags", digestB, "canary", testCandidateBuild, pool)
	seedRow(t, ctx, "featureFlags", digestA, "python", testCandidateBuild, pool)

	want := [][2]string{
		{"featureFlags", digestA},
		{"featureFlags", digestB},
		{"featureFlags", digestC},
		{"hotspots", digestA},
		{"hotspots", digestC},
	}

	// Every read that shares the predicate, so a tiebreak dropped from any
	// one of them fails here. They are aliases of one statement today; if
	// that ever stops being true this is what notices.
	for name, sql := range map[string]string{
		"repoint candidates": selectRepointCandidatesSQL,
		"disable candidates": selectDisableCandidatesSQL,
	} {
		t.Run(name, func(t *testing.T) {
			tx, err := pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = tx.Rollback(ctx) }()

			// The planner's freedom is REMOVED for the length of this
			// transaction, and that is the difference between this test
			// and the one it replaces.
			//
			// The primary key is (schema_digest, document_digest,
			// selected_operation), so an index scan hands the sort its
			// input ALREADY in document-digest order -- and a sort on
			// `selected_operation` alone, being stable, then returns the
			// fully-ordered result anyway. Measured: with the tiebreak
			// deleted from the ORDER BY, this test still passed. The
			// assertion was true, the statement was wrong, and the plan
			// was the only reason they agreed.
			//
			// A TOTAL order is order-correct under EVERY plan. Forcing the
			// sequential scan makes the sort's input the HEAP order --
			// the deliberately adversarial insertion order below -- so an
			// incomplete ORDER BY has nowhere to hide. This narrows what
			// the database may do; it does not change what is asserted.
			for _, off := range []string{"enable_indexscan", "enable_bitmapscan", "enable_indexonlyscan"} {
				if _, err := tx.Exec(ctx, "SET LOCAL "+off+" = off"); err != nil {
					t.Fatal(err)
				}
			}

			rows, err := readRoutingRows(ctx, tx, sql, testSchemaDigest)
			if err != nil {
				t.Fatal(err)
			}
			if len(rows) != len(want) {
				t.Fatalf("read %d row(s), want %d", len(rows), len(want))
			}
			for index, expected := range want {
				got := [2]string{rows[index].operation, rows[index].documentDigest}
				if got != expected {
					t.Fatalf("row %d = %v, want %v -- the read's order must be TOTAL. Two writers visiting these rows in different orders deadlock on this table alone, and `selected_operation` TIES whenever an operation has several document digests.\nfull order returned: %v",
						index, got, expected, rows)
				}
			}
		})
	}
}

// The same class as routing_repoint_integration_test.go's mode-drift
// trigger, at Enable's
// OWN call site. A BEFORE INSERT trigger that swallows the routing-row
// write (RowsAffected() == 0) must refuse the whole enable, leaving NO
// orphan candidate-build row committed -- not silently report
// "enabled total=1" while zero routing rows and one orphan candidate
// build land, which is exactly the CHAOS-5416 silent-success shape.
func TestEnableRefusesWhenTheRoutingRowWriteIsSwallowed(t *testing.T) {
	ctx := t.Context()
	pool := startRegistryPostgres(t)

	if _, err := pool.Exec(ctx, `
		CREATE OR REPLACE FUNCTION test_swallow_insert() RETURNS trigger AS $$
		BEGIN
			RETURN NULL; -- BEFORE INSERT returning NULL skips the row entirely
		END;
		$$ LANGUAGE plpgsql;
		CREATE TRIGGER test_swallow_insert BEFORE INSERT ON go_api_routing_state
			FOR EACH ROW EXECUTE FUNCTION test_swallow_insert();
	`); err != nil {
		t.Fatal(err)
	}

	request := enableRequest("featureFlags")
	request.AcknowledgeUnproven = true
	_, err := Enable(ctx, pool, request)
	if err == nil {
		t.Fatal("Enable must refuse when the routing-row write affected 0 rows -- CHAOS-5416's exact silent-success shape")
	}
	if !strings.Contains(err.Error(), "affected 0 rows") {
		t.Fatalf("refused for a different reason, so the RowsAffected() check is not what caught it: %v", err)
	}

	// The candidate-build insert happens FIRST in the same transaction --
	// the refusal must roll it back too, leaving no orphan.
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM go_api_candidate_build`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("candidate_build rows = %d, want 0 -- the whole transaction must roll back, not leave an orphan build registered for a rollout that never happened", count)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM go_api_routing_state`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("routing_state rows = %d, want 0", count)
	}
}

// EnableOutcome carries the row's OWN prior state,
// read under the same lock the write itself takes -- this is the
// non-concurrent half: does it read back what was actually there.
func TestEnableOutcomeReportsThePriorRowWhenOneExisted(t *testing.T) {
	ctx := t.Context()
	pool := startRegistryPostgres(t)
	seedRow(t, ctx, "flowMatrix", testDocumentDigest, "python", "deaddeaddeaddeaddeaddeaddeaddeaddeaddead", pool)
	seedProof(t, ctx, pool, "flowMatrix", testDocumentDigest, verbsRunningBuild, EnablementProofStage, EnablementProofTerminalState)

	outcomes, err := Enable(ctx, pool, enableRequest("flowMatrix"))
	if err != nil {
		t.Fatalf("Enable: %v", err)
	}
	if len(outcomes) != 1 {
		t.Fatalf("outcomes = %+v, want exactly 1", outcomes)
	}
	if !outcomes[0].HadRowBefore {
		t.Fatalf("HadRowBefore = false, want true -- a row existed before this write")
	}
	if outcomes[0].ModeBefore != "python" {
		t.Fatalf("ModeBefore = %q, want %q (the row's mode before enable replaced it)", outcomes[0].ModeBefore, "python")
	}
	if outcomes[0].CandidateBuildBefore != "deaddeaddeaddeaddeaddeaddeaddeaddeaddead" {
		t.Fatalf("CandidateBuildBefore = %q, want the seeded build", outcomes[0].CandidateBuildBefore)
	}
}

// The other half: no row existed at all.
func TestEnableOutcomeReportsNoPriorRowWhenNoneExisted(t *testing.T) {
	ctx := t.Context()
	pool := startRegistryPostgres(t)
	seedProof(t, ctx, pool, "flowMatrix", testDocumentDigest, verbsRunningBuild, EnablementProofStage, EnablementProofTerminalState)

	outcomes, err := Enable(ctx, pool, enableRequest("flowMatrix"))
	if err != nil {
		t.Fatalf("Enable: %v", err)
	}
	if len(outcomes) != 1 {
		t.Fatalf("outcomes = %+v, want exactly 1", outcomes)
	}
	if outcomes[0].HadRowBefore {
		t.Fatalf("HadRowBefore = true, want false -- no row existed before this write")
	}
	if outcomes[0].ModeBefore != "" || outcomes[0].CandidateBuildBefore != "" {
		t.Fatalf("ModeBefore/CandidateBuildBefore = %q/%q, want both empty when no row existed", outcomes[0].ModeBefore, outcomes[0].CandidateBuildBefore)
	}
}

// The concurrency repro itself: a genuinely
// deterministic proof that the before-state read sees a CONCURRENT
// writer's COMMITTED value, not a stale snapshot -- the exact shape
// opus r8 found with two real binaries (a disable landing between an
// unlocked pre-read and enable's own write, logging "mode_before=canary
// mode_after=canary" for what was actually a re-enable of a just-rolled-
// back operation). Driven at the SQL level, like
// TestEnableAndRepointLockOrderInversionDeadlocks above, for a
// deterministic interleaving rather than a timing-dependent race between
// two real subprocess invocations.
func TestEnablesBeforeStateReadSeesADisableThatCommittedWhileItWasBlocked(t *testing.T) {
	ctx := t.Context()
	pool := startRegistryPostgres(t)
	const operation = "flowMatrix"
	const build = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	seedRow(t, ctx, operation, testDocumentDigest, "canary", build, pool)

	// Step 1: disable's real plan read locks the row -- selectDisableCandidatesSQL,
	// `FOR UPDATE`, iterated to completion so the lock is held server-side.
	disableTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin disable side: %v", err)
	}
	defer func() { _ = disableTx.Rollback(ctx) }()
	rows, err := disableTx.Query(ctx, selectDisableCandidatesSQL, testSchemaDigest)
	if err != nil {
		t.Fatalf("disable FOR UPDATE select: %v", err)
	}
	rowCount := 0
	for rows.Next() {
		rowCount++
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		t.Fatalf("disable FOR UPDATE select: %v", err)
	}
	if rowCount == 0 {
		t.Fatal("the seeded row must be visible to disable's own read")
	}

	// Step 2: enable's OWN before-state read (selectEnableBeforeStateSQL)
	// starts concurrently and BLOCKS on the same row -- launched in a
	// goroutine because it does not return until step 3 releases the lock.
	type readResult struct {
		mode, build string
		err         error
	}
	results := make(chan readResult, 1)
	enableTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin enable side: %v", err)
	}
	defer func() { _ = enableTx.Rollback(ctx) }()
	go func() {
		var mode, build string
		err := enableTx.QueryRow(ctx, selectEnableBeforeStateSQL, testSchemaDigest, testDocumentDigest, operation).Scan(&mode, &build)
		results <- readResult{mode, build, err}
	}()

	// Give the goroutine a moment to actually reach the blocked state
	// server-side before step 3 releases the lock -- otherwise step 3
	// could race ahead of the SELECT even being sent.
	time.Sleep(200 * time.Millisecond)

	// Step 3: disable's real write lands and COMMITS -- exactly
	// disableRoutingRowSQL, unguarded (NULL candidate-build guard).
	now := time.Now().UTC()
	if _, err := disableTx.Exec(ctx, disableRoutingRowSQL,
		testSchemaDigest, testDocumentDigest, operation, "python", "r8 F1 killer", nil, "lane-routing-verbs", now); err != nil {
		t.Fatalf("disable's write: %v", err)
	}
	if err := disableTx.Commit(ctx); err != nil {
		t.Fatalf("disable's commit: %v", err)
	}

	// Step 4: enable's before-state read, unblocked by the commit above,
	// must now see disable's COMMITTED value -- "python", never the
	// "canary" it would have seen from a stale, unlocked read taken
	// before disable ever ran.
	var result readResult
	select {
	case result = <-results:
	case <-time.After(15 * time.Second):
		t.Fatal("enable's before-state read did not return within 15s of disable's commit")
	}
	if result.err != nil {
		t.Fatalf("enable's before-state read: %v", result.err)
	}
	if result.mode != "python" {
		t.Fatalf("enable's before-state read saw mode=%q, want %q -- it must reflect disable's COMMITTED write, not a stale pre-disable snapshot", result.mode, "python")
	}
}
