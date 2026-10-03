//go:build integration

package goapiproof

import (
	"context"
	"errors"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/mcpclass"
)

// The two digests one carry moves between: what the deployed process
// computes today, and what the image about to roll computes.
const (
	carryIntegrationLiveDigest   = testSchemaDigest
	carryIntegrationTargetDigest = "sha256:898250a9000000000000000000000000000000000000000000000000000000ff"
)

// seedCarryRow puts one routing row at a digest, with the full column set
// a carry copies -- eligible_orgs included, since NULL and a JSON value
// are the two cases the copy must keep apart.
func seedCarryRow(t *testing.T, ctx context.Context, pool *pgxpool.Pool, schemaDigest string, row CarryRow) {
	t.Helper()
	if _, err := pool.Exec(ctx, registerCandidateBuildSQL,
		schemaDigest, row.DocumentDigest, row.Operation, row.Build); err != nil {
		t.Fatalf("register %s: %v", row.Operation, err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO go_api_routing_state
			(schema_digest, document_digest, selected_operation, current_candidate_build,
			 owner, mode, eligible_orgs, rollout_percentage, review_evidence, recorded_by)
		VALUES ($1, $2, $3, $4, $5, $6, $7::json, $8, $9, 'seed')`,
		schemaDigest, row.DocumentDigest, row.Operation, row.Build, row.Owner, row.Mode,
		row.EligibleOrgs, row.RolloutPercentage, row.ReviewEvidence); err != nil {
		t.Fatalf("seed %s at %s: %v", row.Operation, schemaDigest, err)
	}
}

func rowsAtDigest(t *testing.T, ctx context.Context, pool *pgxpool.Pool, schemaDigest string) []CarryRow {
	t.Helper()
	rows, err := pool.Query(ctx, surveyCarryRowsSQL, schemaDigest)
	if err != nil {
		t.Fatalf("read rows at %s: %v", schemaDigest, err)
	}
	defer rows.Close()
	var found []CarryRow
	for rows.Next() {
		row, err := scanCarryRow(rows)
		if err != nil {
			t.Fatal(err)
		}
		found = append(found, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return found
}

func carryRowByOperation(rows []CarryRow, operation string) (CarryRow, bool) {
	for _, row := range rows {
		if row.Operation == operation {
			return row, true
		}
	}
	return CarryRow{}, false
}

// carryIntegrationRequest is the request every case below starts from:
// both planes agree that the two live documents are unchanged in the
// image being rolled to.
func carryIntegrationRequest() CarryRequest {
	documents := map[string]string{
		"featureFlags": testDocumentDigest,
		"hotspots":     testDocumentDigest2,
	}
	catalog := map[string]string{
		"featureFlags": testDocumentDigest,
		"hotspots":     testDocumentDigest2,
	}
	live := map[string]string{
		"featureFlags": testDocumentDigest,
		"hotspots":     testDocumentDigest2,
	}
	return CarryRequest{
		LiveSchemaDigest:   carryIntegrationLiveDigest,
		TargetSchemaDigest: carryIntegrationTargetDigest,
		RunningBuild:       verbsRunningBuild,
		RecordedBy:         "lane-gwc-api-digestcarry",
		ReviewEvidence:     "CHAOS-6107 carry before the roll",
		PrincipalID:        testPrincipalID,
		Inputs: CarryInputs{
			LiveDocumentDigest:    live,
			TargetDocumentDigest:  documents,
			CatalogDocumentDigest: catalog,
		},
	}
}

// THE HARM THIS VERB EXISTS FOR, executed end to end: two operations are
// reachable at the live digest, the SDL moves, and without this verb the
// first new pod serves neither. After a carry both digests hold the same
// reachability -- the live rows for the pods still running, the new rows
// for the pods about to start -- and the live rows are untouched, which
// is what makes a rollback need no routing command at all.
func TestCarryPreservesEveryReachableRowAndTouchesNothingAtTheLiveDigest(t *testing.T) {
	ctx := t.Context()
	pool := startAuditedRegistryPostgres(t)
	orgs := `{"orgs": ["org-1"]}`
	seedCarryRow(t, ctx, pool, carryIntegrationLiveDigest, CarryRow{
		Operation: "featureFlags", DocumentDigest: testDocumentDigest, Mode: "canary",
		Build: verbsRunningBuild, Owner: "go", RolloutPercentage: 100, ReviewEvidence: "the original decision",
	})
	seedCarryRow(t, ctx, pool, carryIntegrationLiveDigest, CarryRow{
		Operation: "hotspots", DocumentDigest: testDocumentDigest2, Mode: "primary",
		Build: verbsRunningBuild, Owner: "go", RolloutPercentage: 50, EligibleOrgs: &orgs,
		ReviewEvidence: NamedLimitEvidence("the ledger reason", "chris ruled it"),
	})
	before := rowsAtDigest(t, ctx, pool, carryIntegrationLiveDigest)

	outcomes, err := Carry(ctx, pool, carryIntegrationRequest())
	if err != nil {
		t.Fatalf("Carry: %v", err)
	}
	if summary := SummarizeCarry(outcomes); summary.Carried != 2 || summary.Total != 2 {
		t.Fatalf("summary = %+v, want both rows carried", summary)
	}

	carried := rowsAtDigest(t, ctx, pool, carryIntegrationTargetDigest)
	if len(carried) != 2 {
		t.Fatalf("got %d row(s) at the target digest, want 2: %+v", len(carried), carried)
	}
	for _, source := range before {
		target, ok := carryRowByOperation(carried, source.Operation)
		if !ok {
			t.Fatalf("%s was not carried", source.Operation)
		}
		if !sameCarriedState(source, target) {
			t.Fatalf("%s carried as %+v, want every copied column identical to %+v", source.Operation, target, source)
		}
		// The evidence is the one column that MUST differ, and it keeps
		// the source reason (prefix included) behind the carry prefix.
		if !strings.HasPrefix(target.ReviewEvidence, CarriedEvidencePrefix) {
			t.Fatalf("%s carried with evidence %q, which does not say it was carried", source.Operation, target.ReviewEvidence)
		}
		if !strings.Contains(target.ReviewEvidence, source.ReviewEvidence) {
			t.Fatalf("%s carried with evidence %q, which lost the source row's own reason %q", source.Operation, target.ReviewEvidence, source.ReviewEvidence)
		}
	}
	// eligible_orgs is copied byte-for-byte, and NULL stays NULL.
	flags, _ := carryRowByOperation(carried, "featureFlags")
	if flags.EligibleOrgs != nil {
		t.Fatalf("featureFlags carried eligible_orgs=%q, want the NULL it had", *flags.EligibleOrgs)
	}
	spots, _ := carryRowByOperation(carried, "hotspots")
	if spots.EligibleOrgs == nil || *spots.EligibleOrgs != orgs {
		t.Fatalf("hotspots carried eligible_orgs=%v, want %q byte for byte", spots.EligibleOrgs, orgs)
	}

	// The live digest is untouched: that is what a helm rollback finds.
	after := rowsAtDigest(t, ctx, pool, carryIntegrationLiveDigest)
	if len(after) != len(before) {
		t.Fatalf("the live digest now holds %d row(s), want the %d it had", len(after), len(before))
	}
	for index := range before {
		if !sameCarriedState(before[index], after[index]) || before[index].ReviewEvidence != after[index].ReviewEvidence {
			t.Fatalf("live row %+v was modified to %+v -- a carry writes only at the target digest", before[index], after[index])
		}
	}

	audits := readAuditRows(t, ctx, pool)
	if len(audits) != 2 {
		t.Fatalf("got %d audit row(s), want one per carried row: %+v", len(audits), audits)
	}
	for _, row := range audits {
		// alembic 0130's CHECK admits enable|disable|repoint, so a carry
		// records `enable` and the evidence prefix on the ROW is what
		// tells the two apart.
		if row.action != AuditActionEnable || row.credentialClass != CredentialClassEnvelope {
			t.Fatalf("audit row = %+v, want an enable under the envelope class", row)
		}
		if row.schemaDigest != carryIntegrationTargetDigest {
			t.Fatalf("audit row names schema_digest %q, want the digest the row was written at", row.schemaDigest)
		}
		if row.modeBefore != nil || row.buildBefore != nil {
			t.Fatalf("audit row = %+v: no row existed at this digest, so both before-values must be NULL", row)
		}
		if row.principalID == nil || *row.principalID != testPrincipalID {
			t.Fatalf("audit row = %+v, want the envelope subject", row)
		}
	}
	if audits[0].correlationID != audits[1].correlationID {
		t.Fatal("one invocation's audit rows must share a correlation id")
	}
}

// A rerun is how an operator recovers from a partial deploy, so it must
// be safe: the same command twice writes once.
func TestCarryRerunWritesNothingASecondTime(t *testing.T) {
	ctx := t.Context()
	pool := startAuditedRegistryPostgres(t)
	seedCarryRow(t, ctx, pool, carryIntegrationLiveDigest, CarryRow{
		Operation: "featureFlags", DocumentDigest: testDocumentDigest, Mode: "canary",
		Build: verbsRunningBuild, Owner: "go", RolloutPercentage: 100, ReviewEvidence: "the original decision",
	})
	if _, err := Carry(ctx, pool, carryIntegrationRequest()); err != nil {
		t.Fatalf("first Carry: %v", err)
	}
	first := rowsAtDigest(t, ctx, pool, carryIntegrationTargetDigest)

	outcomes, err := Carry(ctx, pool, carryIntegrationRequest())
	if err != nil {
		t.Fatalf("second Carry: %v", err)
	}
	if summary := SummarizeCarry(outcomes); summary.Carried != 0 || summary.Unchanged != 1 {
		t.Fatalf("summary = %+v, want the rerun to report the row unchanged and write nothing", summary)
	}
	second := rowsAtDigest(t, ctx, pool, carryIntegrationTargetDigest)
	if len(second) != 1 || first[0].ReviewEvidence != second[0].ReviewEvidence {
		t.Fatalf("the rerun rewrote the row: %+v -> %+v", first, second)
	}
	if audits := readAuditRows(t, ctx, pool); len(audits) != 1 {
		t.Fatalf("got %d audit row(s), want the one the first run wrote -- an audit entry for a write that did not happen is a false entry", len(audits))
	}
}

// A changed document is the case where carrying would write a row
// nothing ever looks up. The run refuses by name and, because it is one
// transaction, the operations that WOULD have carried are not written
// either -- a half-carried fleet is the state this verb exists to avoid.
func TestCarryRefusesAChangedDocumentAndWritesNothingAtAll(t *testing.T) {
	ctx := t.Context()
	pool := startAuditedRegistryPostgres(t)
	seedCarryRow(t, ctx, pool, carryIntegrationLiveDigest, CarryRow{
		Operation: "featureFlags", DocumentDigest: testDocumentDigest, Mode: "canary",
		Build: verbsRunningBuild, Owner: "go", RolloutPercentage: 100, ReviewEvidence: "the original decision",
	})
	seedCarryRow(t, ctx, pool, carryIntegrationLiveDigest, CarryRow{
		Operation: "hotspots", DocumentDigest: testDocumentDigest2, Mode: "canary",
		Build: verbsRunningBuild, Owner: "go", RolloutPercentage: 100, ReviewEvidence: "the original decision",
	})
	request := carryIntegrationRequest()
	// The image being rolled to registers hotspots under a different
	// document text.
	request.Inputs.TargetDocumentDigest["hotspots"] = strings.Repeat("c", 64)
	request.Inputs.CatalogDocumentDigest["hotspots"] = strings.Repeat("c", 64)

	_, err := Carry(ctx, pool, request)
	if !errors.Is(err, ErrCarryDocumentMoved) {
		t.Fatalf("err = %v, want a refusal naming the moved document", err)
	}
	if !strings.Contains(err.Error(), "hotspots") {
		t.Fatalf("refusal %q must name the operation it is about", err)
	}
	if rows := rowsAtDigest(t, ctx, pool, carryIntegrationTargetDigest); len(rows) != 0 {
		t.Fatalf("a refused carry wrote %d row(s) at the target digest: %+v", len(rows), rows)
	}
	if audits := readAuditRows(t, ctx, pool); len(audits) != 0 {
		t.Fatalf("a refused carry wrote %d audit row(s)", len(audits))
	}
}

// CHAOS-8000 dual accept, executed: the image being rolled to registers
// hotspots under a NEW document and still accepts the old one as a legacy
// text. That is the same row shape as the refusal just above plus the two
// legacy lists -- and the row is carried under its OWN digest, verbatim,
// so the new image (which reads a row under any accepted digest) serves
// both texts from it. Nothing is re-keyed to the new digest, and the live
// digest is untouched.
func TestCarryCopiesARowKeyedToALegacyDigestUnderItsOwnDigest(t *testing.T) {
	ctx := t.Context()
	newDigest := strings.Repeat("c", 64)
	for _, mode := range []string{"canary", "primary", "shadow"} {
		t.Run(mode, func(t *testing.T) {
			pool := startAuditedRegistryPostgres(t)
			seedCarryRow(t, ctx, pool, carryIntegrationLiveDigest, CarryRow{
				Operation: "featureFlags", DocumentDigest: testDocumentDigest, Mode: "canary",
				Build: verbsRunningBuild, Owner: "go", RolloutPercentage: 100, ReviewEvidence: "served anchor",
			})
			orgs := `{"orgs": ["org-1"]}`
			seedCarryRow(t, ctx, pool, carryIntegrationLiveDigest, CarryRow{
				Operation: "hotspots", DocumentDigest: testDocumentDigest2, Mode: mode,
				Build: verbsRunningBuild, Owner: "go", RolloutPercentage: 50, EligibleOrgs: &orgs,
				ReviewEvidence: "the original decision",
			})
			before := rowsAtDigest(t, ctx, pool, carryIntegrationLiveDigest)

			request := carryIntegrationRequest()
			request.Inputs.TargetDocumentDigest["hotspots"] = newDigest
			request.Inputs.CatalogDocumentDigest["hotspots"] = newDigest
			request.Inputs.TargetLegacyDocumentDigests = map[string][]string{"hotspots": {testDocumentDigest2}}
			request.Inputs.CatalogLegacyDocumentDigests = map[string][]string{"hotspots": {testDocumentDigest2}}

			outcomes, err := Carry(ctx, pool, request)
			if err != nil {
				t.Fatalf("Carry: %v -- a swapped document whose old text is still accepted must not stop the roll", err)
			}
			if summary := SummarizeCarry(outcomes); summary.Carried != 2 || summary.Total != 2 {
				t.Fatalf("summary = %+v, want both rows carried", summary)
			}
			for _, outcome := range outcomes {
				if wantLegacy := outcome.Operation == "hotspots"; outcome.LegacyDigest != wantLegacy {
					t.Fatalf("%s: legacy digest = %t, want %t", outcome.Operation, outcome.LegacyDigest, wantLegacy)
				}
			}

			// THE STATE THE VERB EXISTS TO REACH: one row for hotspots at the
			// target schema digest, keyed to the digest the row always had.
			carried := rowsAtDigest(t, ctx, pool, carryIntegrationTargetDigest)
			if len(carried) != 2 {
				t.Fatalf("got %d row(s) at the target digest, want 2: %+v", len(carried), carried)
			}
			source, _ := carryRowByOperation(before, "hotspots")
			target, ok := carryRowByOperation(carried, "hotspots")
			if !ok {
				t.Fatal("hotspots has no row at the target digest: the roll would un-route it")
			}
			if target.DocumentDigest != testDocumentDigest2 {
				t.Fatalf("hotspots carried under %s, want its own digest %s -- not the image's current one", target.DocumentDigest, testDocumentDigest2)
			}
			if !sameCarriedState(source, target) {
				t.Fatalf("hotspots carried as %+v, want every copied column identical to %+v", target, source)
			}
			if !strings.HasPrefix(target.ReviewEvidence, CarriedEvidencePrefix) || !strings.Contains(target.ReviewEvidence, source.ReviewEvidence) {
				t.Fatalf("hotspots carried with evidence %q, want the carry prefix in front of the source reason", target.ReviewEvidence)
			}

			after := rowsAtDigest(t, ctx, pool, carryIntegrationLiveDigest)
			if len(after) != len(before) {
				t.Fatalf("the live digest now holds %d row(s), want the %d it had", len(after), len(before))
			}
			for index := range before {
				if !sameCarriedState(before[index], after[index]) || before[index].ReviewEvidence != after[index].ReviewEvidence {
					t.Fatalf("live row %+v was modified to %+v -- a carry writes only at the target digest", before[index], after[index])
				}
			}

			// A rerun writes nothing and still names the row as a legacy-digest one.
			again, err := Carry(ctx, pool, request)
			if err != nil {
				t.Fatalf("second Carry: %v", err)
			}
			if summary := SummarizeCarry(again); summary.Carried != 0 || summary.Unchanged != 2 {
				t.Fatalf("rerun summary = %+v, want both rows unchanged", summary)
			}
			if rows := rowsAtDigest(t, ctx, pool, carryIntegrationTargetDigest); len(rows) != 2 {
				t.Fatalf("the rerun left %d row(s) at the target digest, want 2", len(rows))
			}
		})
	}
}

// `carry` copies provenance rather than re-reading it, so a row still
// naming a build the process is not running would have that stale claim
// copied to the new digest and outlive the roll that made it wrong.
func TestCarryRefusesARowWhoseBuildIsNotTheRunningOne(t *testing.T) {
	ctx := t.Context()
	pool := startAuditedRegistryPostgres(t)
	seedCarryRow(t, ctx, pool, carryIntegrationLiveDigest, CarryRow{
		Operation: "featureFlags", DocumentDigest: testDocumentDigest, Mode: "canary",
		Build: testCandidateBuild, Owner: "go", RolloutPercentage: 100, ReviewEvidence: "the original decision",
	})

	_, err := Carry(ctx, pool, carryIntegrationRequest())
	if !errors.Is(err, ErrCarryBuildNotRunning) {
		t.Fatalf("err = %v, want the refusal that names `repoint`", err)
	}
	if !strings.Contains(err.Error(), "repoint") {
		t.Fatalf("refusal %q must name the verb that fixes it", err)
	}
	if rows := rowsAtDigest(t, ctx, pool, carryIntegrationTargetDigest); len(rows) != 0 {
		t.Fatalf("a refused carry wrote %d row(s)", len(rows))
	}
}

// A row at the target digest is a decision somebody took there. A carry
// has no intent of its own to justify overwriting it.
func TestCarryNeverOverwritesADecisionTakenAtTheTargetDigest(t *testing.T) {
	ctx := t.Context()
	pool := startAuditedRegistryPostgres(t)
	seedCarryRow(t, ctx, pool, carryIntegrationLiveDigest, CarryRow{
		Operation: "featureFlags", DocumentDigest: testDocumentDigest, Mode: "canary",
		Build: verbsRunningBuild, Owner: "go", RolloutPercentage: 100, ReviewEvidence: "the original decision",
	})
	seedCarryRow(t, ctx, pool, carryIntegrationTargetDigest, CarryRow{
		Operation: "featureFlags", DocumentDigest: testDocumentDigest, Mode: "python",
		Build: verbsRunningBuild, Owner: "go", RolloutPercentage: 0, ReviewEvidence: "deliberately off at the new digest",
	})

	_, err := Carry(ctx, pool, carryIntegrationRequest())
	if !errors.Is(err, ErrCarryTargetRowExists) {
		t.Fatalf("err = %v, want the refusal that names the existing row", err)
	}
	rows := rowsAtDigest(t, ctx, pool, carryIntegrationTargetDigest)
	if len(rows) != 1 || rows[0].Mode != "python" || rows[0].ReviewEvidence != "deliberately off at the new digest" {
		t.Fatalf("the decision at the target digest was modified: %+v", rows)
	}
}

// Modes that reach no client have no reachability to preserve, and a row
// already unreachable at the live digest is dead wherever it is copied.
// Both are REPORTED by name -- an operator must be able to see what was
// left behind and why -- and neither is fatal.
func TestCarrySkipsRowsWithNoReachabilityToPreserveAndKeepsShadowRows(t *testing.T) {
	ctx := t.Context()
	pool := startAuditedRegistryPostgres(t)
	seedCarryRow(t, ctx, pool, carryIntegrationLiveDigest, CarryRow{
		Operation: "featureFlags", DocumentDigest: testDocumentDigest, Mode: "canary",
		Build: verbsRunningBuild, Owner: "go", RolloutPercentage: 100, ReviewEvidence: "the original decision",
	})
	// Reachable mode, but keyed to a document the deployed process does
	// not register: dead already.
	seedCarryRow(t, ctx, pool, carryIntegrationLiveDigest, CarryRow{
		Operation: "hotspots", DocumentDigest: strings.Repeat("d", 64), Mode: "canary",
		Build: verbsRunningBuild, Owner: "go", RolloutPercentage: 100, ReviewEvidence: "drifted",
	})
	request := carryIntegrationRequest()
	// A shadow row: recorded, never served to a client by either plane.
	request.Inputs.LiveDocumentDigest["savedReports"] = strings.Repeat("e", 64)
	request.Inputs.TargetDocumentDigest["savedReports"] = strings.Repeat("e", 64)
	request.Inputs.CatalogDocumentDigest["savedReports"] = strings.Repeat("e", 64)
	seedCarryRow(t, ctx, pool, carryIntegrationLiveDigest, CarryRow{
		Operation: "savedReports", DocumentDigest: strings.Repeat("e", 64), Mode: "shadow",
		Build: verbsRunningBuild, Owner: "go", RolloutPercentage: 100, ReviewEvidence: "shadowing",
	})

	outcomes, err := Carry(ctx, pool, request)
	if err != nil {
		t.Fatalf("Carry: %v", err)
	}
	summary := SummarizeCarry(outcomes)
	// CHAOS-8144: the shadow row is a registration and is now carried
	// (verbatim, still shadow); only the dead-document row is skipped.
	if summary.Carried != 2 || summary.Skipped != 1 {
		t.Fatalf("summary = %+v, want two carried (featureFlags, the shadow savedReports) and one skipped (the drifted hotspots): %+v", summary, outcomes)
	}
	for _, outcome := range outcomes {
		if outcome.Action == CarryActionSkip && outcome.Reason == "" {
			t.Fatalf("%s was skipped with no reason, so an operator cannot tell what was left behind", outcome.Operation)
		}
	}
	rows := rowsAtDigest(t, ctx, pool, carryIntegrationTargetDigest)
	if len(rows) != 2 {
		t.Fatalf("rows at the target digest = %+v, want the served row and the shadow row", rows)
	}
	if shadow, ok := carryRowByOperation(rows, "savedReports"); !ok || shadow.Mode != "shadow" {
		t.Fatalf("savedReports at the target digest = %+v (present=%t), want it carried as mode shadow", shadow, ok)
	}
	if _, ok := carryRowByOperation(rows, "hotspots"); ok {
		t.Fatal("the drifted-document row was carried")
	}
}

// Rows exist, none of them reachable: the operator believes something is
// being served and must be told plainly that nothing is, rather than
// reading a successful run over an empty list.
func TestCarryRefusesWhenNothingAtTheLiveDigestIsReachable(t *testing.T) {
	ctx := t.Context()
	pool := startAuditedRegistryPostgres(t)
	seedCarryRow(t, ctx, pool, carryIntegrationLiveDigest, CarryRow{
		Operation: "featureFlags", DocumentDigest: testDocumentDigest, Mode: "python",
		Build: verbsRunningBuild, Owner: "go", RolloutPercentage: 0, ReviewEvidence: "off",
	})
	if _, err := Carry(ctx, pool, carryIntegrationRequest()); !errors.Is(err, ErrCarryNothingReachable) {
		t.Fatalf("err = %v, want the refusal that says nothing is reachable", err)
	}
	if rows := rowsAtDigest(t, ctx, pool, carryIntegrationTargetDigest); len(rows) != 0 {
		t.Fatalf("wrote %d row(s) on a refusal", len(rows))
	}
}

// An empty table and a table whose rows all died must not read alike
// (CHAOS-8543). Until this change both answered ErrCarryNoLiveRows, and this
// test -- then TestCarryRefusesWhenTheLiveDigestHasNoRowAtAll -- pinned that
// for the empty table.
//
// The empty table is a valid state since the catalog rule: nothing to carry,
// nothing wrong, and its own value that is not a refusal. Rows that exist only
// at another schema digest hold their operations dark by the same rule, so
// that state keeps the refusal, whatever the modes of those rows.
func TestCarryTellsAnEmptyTableFromATableWhoseRowsAreAllElsewhere(t *testing.T) {
	ctx := t.Context()
	pool := startAuditedRegistryPostgres(t)

	outcomes, err := Carry(ctx, pool, carryIntegrationRequest())
	if !errors.Is(err, ErrRoutingTableEmpty) {
		t.Fatalf("empty table: err = %v, want ErrRoutingTableEmpty", err)
	}
	if errors.Is(err, ErrCarryNoLiveRows) || errors.Is(err, ErrCarryRequestRefused) || len(outcomes) != 0 {
		t.Fatalf("empty table: err = %v outcomes = %+v, want a value that is neither refusal and no outcome", err, outcomes)
	}
	// A named operation changes nothing: there is no row for it to be missing from.
	named := carryIntegrationRequest()
	named.Operations = []string{"featureFlags"}
	if _, err := Carry(ctx, pool, named); !errors.Is(err, ErrRoutingTableEmpty) {
		t.Fatalf("empty table, one named operation: err = %v, want ErrRoutingTableEmpty", err)
	}

	const elsewhere = "sha256:0000000000000000000000000000000000000000000000000000000000000e15"
	for _, mode := range []string{"disabled", "canary"} {
		seedCarryRow(t, ctx, pool, elsewhere, CarryRow{
			Operation: "op_" + mode, DocumentDigest: testDocumentDigest, Mode: mode,
			Build: verbsRunningBuild, Owner: "go", RolloutPercentage: 100, ReviewEvidence: "left at another digest",
		})
		_, err := Carry(ctx, pool, carryIntegrationRequest())
		if !errors.Is(err, ErrCarryNoLiveRows) || errors.Is(err, ErrRoutingTableEmpty) {
			t.Fatalf("a %s row only at another schema digest: err = %v, want the refusal ErrCarryNoLiveRows", mode, err)
		}
	}
	for _, digest := range []string{carryIntegrationLiveDigest, carryIntegrationTargetDigest} {
		if rows := rowsAtDigest(t, ctx, pool, digest); len(rows) != 0 {
			t.Fatalf("%d row(s) written at %s by runs that carried nothing", len(rows), digest)
		}
	}
	var audits int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM go_api_routing_audits`).Scan(&audits); err != nil || audits != 0 {
		t.Fatalf("audit rows = %d (err %v), want none: nothing was written", audits, err)
	}
}

// -dry-run answers the question an operator asks before a roll -- "what
// would this do" -- and must answer it without doing any of it.
func TestCarryDryRunReportsThePlanAndWritesNothing(t *testing.T) {
	ctx := t.Context()
	pool := startAuditedRegistryPostgres(t)
	seedCarryRow(t, ctx, pool, carryIntegrationLiveDigest, CarryRow{
		Operation: "featureFlags", DocumentDigest: testDocumentDigest, Mode: "canary",
		Build: verbsRunningBuild, Owner: "go", RolloutPercentage: 100, ReviewEvidence: "the original decision",
	})
	request := carryIntegrationRequest()
	request.DryRun = true

	outcomes, err := Carry(ctx, pool, request)
	if err != nil {
		t.Fatalf("Carry: %v", err)
	}
	if summary := SummarizeCarry(outcomes); summary.Carried != 1 {
		t.Fatalf("summary = %+v, want the plan to name the row it would carry", summary)
	}
	if rows := rowsAtDigest(t, ctx, pool, carryIntegrationTargetDigest); len(rows) != 0 {
		t.Fatalf("a dry run wrote %d row(s)", len(rows))
	}
	if audits := readAuditRows(t, ctx, pool); len(audits) != 0 {
		t.Fatalf("a dry run wrote %d audit row(s)", len(audits))
	}
	if builds := candidateBuildCount(t, ctx, pool, carryIntegrationTargetDigest); builds != 0 {
		t.Fatalf("a dry run registered %d candidate build(s) at the target digest", builds)
	}
}

// An -operations name with no row at the live digest is refused, not
// silently dropped: carrying the rest and saying nothing is how an
// operator learns too late that half a rollout moved.
func TestCarryRefusesANamedOperationWithNoLiveRow(t *testing.T) {
	ctx := t.Context()
	pool := startAuditedRegistryPostgres(t)
	seedCarryRow(t, ctx, pool, carryIntegrationLiveDigest, CarryRow{
		Operation: "featureFlags", DocumentDigest: testDocumentDigest, Mode: "canary",
		Build: verbsRunningBuild, Owner: "go", RolloutPercentage: 100, ReviewEvidence: "the original decision",
	})
	request := carryIntegrationRequest()
	request.Operations = []string{"featureFlags", "hotspots"}

	_, err := Carry(ctx, pool, request)
	if !errors.Is(err, ErrCarryUnknownOperation) || !strings.Contains(err.Error(), "hotspots") {
		t.Fatalf("err = %v, want a refusal naming hotspots", err)
	}
	if rows := rowsAtDigest(t, ctx, pool, carryIntegrationTargetDigest); len(rows) != 0 {
		t.Fatalf("wrote %d row(s) on a refusal", len(rows))
	}
}

func candidateBuildCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, schemaDigest string) int {
	t.Helper()
	var count int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM go_api_candidate_build WHERE schema_digest = $1`, schemaDigest).Scan(&count); err != nil {
		t.Fatalf("count candidate builds: %v", err)
	}
	return count
}

// The race the survey cannot see: a row lands at the target key AFTER
// this carry read that digest and BEFORE it writes. Driven at the seam
// with the racing row committed first, so both answers are executed
// rather than reasoned about.
func TestCarryAnswersOnTheRowThatIsActuallyAtTheTargetKey(t *testing.T) {
	ctx := t.Context()
	carried := CarryRow{
		Operation: "featureFlags", DocumentDigest: testDocumentDigest, Mode: "canary",
		Build: verbsRunningBuild, Owner: "go", RolloutPercentage: 100,
	}

	t.Run("an identical row is somebody else's carry of the same decision", func(t *testing.T) {
		pool := startAuditedRegistryPostgres(t)
		seedCarryRow(t, ctx, pool, carryIntegrationTargetDigest, carried)

		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		outcome := CarryOutcome{
			Operation: carried.Operation, DocumentDigest: carried.DocumentDigest, Action: CarryActionCarry,
			Mode: carried.Mode, Build: carried.Build, Owner: carried.Owner,
			RolloutPercentage: carried.RolloutPercentage, ReviewEvidence: "carried",
		}
		wrote, err := carryOneRow(ctx, tx, carryIntegrationTargetDigest, "lane", time.Now().UTC(), &outcome)
		if err != nil {
			t.Fatalf("carryOneRow: %v", err)
		}
		if wrote {
			t.Fatal("the row was already there, so nothing can have been written")
		}
		if outcome.Action != CarryActionUnchanged {
			t.Fatalf("action = %s, want it reported as unchanged so no audit row claims a write", outcome.Action)
		}
	})

	t.Run("a different row is a decision this verb never overwrites", func(t *testing.T) {
		pool := startAuditedRegistryPostgres(t)
		different := carried
		different.Mode = "python"
		different.RolloutPercentage = 0
		seedCarryRow(t, ctx, pool, carryIntegrationTargetDigest, different)

		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		outcome := CarryOutcome{
			Operation: carried.Operation, DocumentDigest: carried.DocumentDigest, Action: CarryActionCarry,
			Mode: carried.Mode, Build: carried.Build, Owner: carried.Owner,
			RolloutPercentage: carried.RolloutPercentage, ReviewEvidence: "carried",
		}
		_, err = carryOneRow(ctx, tx, carryIntegrationTargetDigest, "lane", time.Now().UTC(), &outcome)
		if !errors.Is(err, ErrCarryTargetRowExists) {
			t.Fatalf("err = %v, want the refusal that names the row already there", err)
		}
	})
}

// r1 F1 (the SOURCE half): a carry must copy the decision that is
// STANDING when it commits, not the one it happened to read first.
//
// Under READ COMMITTED -- pgx's default, and what every verb in this
// package runs under -- the unlocked survey read sees the live rows as
// they were at the instant IT ran. Before this guard, an operator who
// changed a live row between that read and the commit had the SUPERSEDED
// decision written to the target digest: the roll would then serve Go
// for an operation they had just turned off, and `status` would show a
// row at the new digest that matches nothing at the live one.
//
// The change is made by a trigger rather than a second goroutine on
// purpose. A goroutine racing a millisecond-wide window is a test that
// passes for whichever reason the scheduler chose that day; a trigger
// puts the table in EXACTLY the state the guard exists to catch, every
// run, and still exercises the real comparison against a real Postgres.
func TestCarryRefusesWhenALiveRowChangesUnderneathItAndWritesNothing(t *testing.T) {
	ctx := t.Context()
	pool := startAuditedRegistryPostgres(t)
	seedCarryRow(t, ctx, pool, carryIntegrationLiveDigest, CarryRow{
		Operation: "featureFlags", DocumentDigest: testDocumentDigest, Mode: "canary",
		Build: verbsRunningBuild, Owner: "go", RolloutPercentage: 100, ReviewEvidence: "the original decision",
	})
	seedCarryRow(t, ctx, pool, carryIntegrationLiveDigest, CarryRow{
		Operation: "hotspots", DocumentDigest: testDocumentDigest2, Mode: "primary",
		Build: verbsRunningBuild, Owner: "go", RolloutPercentage: 50, ReviewEvidence: "the other decision",
	})

	// Fires when the carry writes its FIRST target row -- i.e. after the
	// survey has already decided what to copy, and before the commit.
	if _, err := pool.Exec(ctx, `
		CREATE FUNCTION carry_race_guard() RETURNS trigger AS $$
		BEGIN
			UPDATE public.go_api_routing_state
			   SET mode = 'python'
			 WHERE schema_digest = `+quoteLiteral(carryIntegrationLiveDigest)+`
			   AND selected_operation = 'featureFlags';
			RETURN NEW;
		END;
		$$ LANGUAGE plpgsql;
		CREATE TRIGGER carry_race_guard_trigger
			BEFORE INSERT ON public.go_api_routing_state
			FOR EACH ROW
			WHEN (NEW.schema_digest = `+quoteLiteral(carryIntegrationTargetDigest)+`)
			EXECUTE FUNCTION carry_race_guard();`); err != nil {
		t.Fatalf("install the racing writer: %v", err)
	}

	_, err := Carry(ctx, pool, carryIntegrationRequest())
	if !errors.Is(err, ErrCarrySourceRowChanged) {
		t.Fatalf("Carry err = %v, want ErrCarrySourceRowChanged", err)
	}
	// The refusal must NAME the operation and both readings. A refusal
	// that says only "something moved" leaves the operator with nothing
	// to act on in front of a roll.
	for _, want := range []string{"featureFlags", "python", "canary", "NOTHING was written"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("refusal must contain %q, got: %v", want, err)
		}
	}

	// ALL OR NOTHING: not one row, and not one audit entry, survives.
	if rows := rowsAtDigest(t, ctx, pool, carryIntegrationTargetDigest); len(rows) != 0 {
		t.Fatalf("the whole run must roll back, found %d row(s) at the target digest: %+v", len(rows), rows)
	}
	var audits int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM public.go_api_routing_audits`).Scan(&audits); err != nil {
		t.Fatalf("count audits: %v", err)
	}
	if audits != 0 {
		t.Fatalf("a rolled-back carry must leave no audit row, got %d", audits)
	}
}

// The other direction of the same guard: the live row does not change,
// it DISAPPEARS. "The operator changed their mind" and "the operator
// removed the decision entirely" are different facts and both mean the
// row about to commit at the target digest is no longer backed by
// anything live.
func TestCarryRefusesWhenALiveRowIsRemovedUnderneathIt(t *testing.T) {
	ctx := t.Context()
	pool := startAuditedRegistryPostgres(t)
	seedCarryRow(t, ctx, pool, carryIntegrationLiveDigest, CarryRow{
		Operation: "featureFlags", DocumentDigest: testDocumentDigest, Mode: "canary",
		Build: verbsRunningBuild, Owner: "go", RolloutPercentage: 100, ReviewEvidence: "the original decision",
	})
	if _, err := pool.Exec(ctx, `
		CREATE FUNCTION carry_delete_guard() RETURNS trigger AS $$
		BEGIN
			DELETE FROM public.go_api_routing_state
			 WHERE schema_digest = `+quoteLiteral(carryIntegrationLiveDigest)+`
			   AND selected_operation = 'featureFlags';
			RETURN NEW;
		END;
		$$ LANGUAGE plpgsql;
		CREATE TRIGGER carry_delete_guard_trigger
			BEFORE INSERT ON public.go_api_routing_state
			FOR EACH ROW
			WHEN (NEW.schema_digest = `+quoteLiteral(carryIntegrationTargetDigest)+`)
			EXECUTE FUNCTION carry_delete_guard();`); err != nil {
		t.Fatalf("install the racing deleter: %v", err)
	}

	_, err := Carry(ctx, pool, carryIntegrationRequest())
	if !errors.Is(err, ErrCarrySourceRowChanged) {
		t.Fatalf("Carry err = %v, want ErrCarrySourceRowChanged", err)
	}
	if !strings.Contains(err.Error(), "was REMOVED at the live digest") {
		t.Fatalf("the refusal must say the row was removed, got: %v", err)
	}
	if rows := rowsAtDigest(t, ctx, pool, carryIntegrationTargetDigest); len(rows) != 0 {
		t.Fatalf("the whole run must roll back, found %d row(s): %+v", len(rows), rows)
	}
}

// CONTROL for both tests above, and the reason they are not vacuous: an
// UNCHANGED live row must still carry cleanly with the revalidation in
// place. A guard that refused everything would pass both tests above and
// break the verb entirely.
func TestCarryStillSucceedsWhenTheLiveRowsDoNotMove(t *testing.T) {
	ctx := t.Context()
	pool := startAuditedRegistryPostgres(t)
	seedCarryRow(t, ctx, pool, carryIntegrationLiveDigest, CarryRow{
		Operation: "featureFlags", DocumentDigest: testDocumentDigest, Mode: "canary",
		Build: verbsRunningBuild, Owner: "go", RolloutPercentage: 100, ReviewEvidence: "the original decision",
	})
	outcomes, err := Carry(ctx, pool, carryIntegrationRequest())
	if err != nil {
		t.Fatalf("an untouched live row must carry cleanly: %v", err)
	}
	if summary := SummarizeCarry(outcomes); summary.Carried != 1 {
		t.Fatalf("summary = %+v, want one row carried", summary)
	}
	if rows := rowsAtDigest(t, ctx, pool, carryIntegrationTargetDigest); len(rows) != 1 {
		t.Fatalf("got %d row(s) at the target digest, want 1", len(rows))
	}
}

// quoteLiteral renders a SQL string literal for the trigger bodies
// above. Test-only, and the values are test constants -- but spelled
// with a real quoting rule rather than bare concatenation so a constant
// that ever gains an apostrophe fails loudly instead of silently
// changing what the trigger does.
func quoteLiteral(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

// The LOCK itself, proven from outside the transaction that takes it.
//
// The two tests above pin the COMPARISON: they put the table in the
// state the guard must catch and check that it refuses. Neither of them
// can kill `FOR SHARE` -- a trigger's change is made inside the carry's
// own transaction, so the re-read sees it with or without a lock. That
// is a real gap, and this closes it at the one seam where the lock's
// effect is observable: with the rows locked, a DIFFERENT session's
// UPDATE of them must not be able to proceed.
//
// The second session sets its own lock_timeout so this test cannot hang
// on a failure; the failure mode without the lock is the UPDATE
// SUCCEEDING, which is asserted directly rather than inferred from a
// timing.
func TestCarrySourceReadActuallyLocksTheLiveRowsAgainstOtherWriters(t *testing.T) {
	ctx := t.Context()
	pool := startAuditedRegistryPostgres(t)
	seedCarryRow(t, ctx, pool, carryIntegrationLiveDigest, CarryRow{
		Operation: "featureFlags", DocumentDigest: testDocumentDigest, Mode: "canary",
		Build: verbsRunningBuild, Owner: "go", RolloutPercentage: 100, ReviewEvidence: "the original decision",
	})

	holder, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = holder.Rollback(context.Background()) }()
	if _, err := readCarryRows(ctx, holder, lockCarrySourceRowsSQL, carryIntegrationLiveDigest); err != nil {
		t.Fatalf("locking read: %v", err)
	}

	writer, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = writer.Rollback(context.Background()) }()
	if _, err := writer.Exec(ctx, `SET LOCAL lock_timeout = '750ms'`); err != nil {
		t.Fatalf("set lock_timeout: %v", err)
	}
	_, err = writer.Exec(ctx, `
		UPDATE public.go_api_routing_state SET mode = 'python'
		 WHERE schema_digest = $1 AND selected_operation = 'featureFlags'`, carryIntegrationLiveDigest)
	if err == nil {
		t.Fatal("another session updated a row this transaction holds FOR SHARE -- the source rows are not actually locked, so a concurrent operator change can still land between the survey and the commit")
	}
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "55P03" {
		t.Fatalf("want lock_not_available (55P03) from the blocked writer, got %v", err)
	}

	// CONTROL: once the holder releases, the same UPDATE succeeds. Without
	// this the test would also pass if the row were simply unwritable for
	// some unrelated reason.
	if err := holder.Rollback(ctx); err != nil {
		t.Fatalf("release the share lock: %v", err)
	}
	// A FRESH transaction: the blocked one above is aborted by its own
	// failed statement (SQLSTATE 25P02 on anything further), so reusing
	// it would fail for a reason that has nothing to do with the lock and
	// the control would prove nothing.
	if err := writer.Rollback(ctx); err != nil {
		t.Fatalf("discard the aborted writer transaction: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE public.go_api_routing_state SET mode = 'python'
		 WHERE schema_digest = $1 AND selected_operation = 'featureFlags'`, carryIntegrationLiveDigest); err != nil {
		t.Fatalf("control: the update must succeed once the share lock is gone, got %v", err)
	}
}

// The revalidation must cover rows whose outcome is UNCHANGED, not only
// the ones this run actually inserted.
//
// UNCHANGED means "the target digest already holds exactly this row" --
// which a partly-completed earlier carry, or a rerun, produces routinely.
// If the guard watched only the rows IT wrote, a live row that moved
// under an UNCHANGED outcome would leave the target digest holding a
// superseded decision while the run reported success.
//
// The run here is MIXED on purpose: `hotspots` is carried (so a write
// happens, and with it the trigger's anchor) while `featureFlags` is
// already at the target digest and reports UNCHANGED. The trigger moves
// the UNCHANGED one. Only the clause under test can see that.
func TestCarryRevalidatesEvenRowsItDidNotWriteThisRun(t *testing.T) {
	ctx := t.Context()
	pool := startAuditedRegistryPostgres(t)
	seedCarryRow(t, ctx, pool, carryIntegrationLiveDigest, CarryRow{
		Operation: "featureFlags", DocumentDigest: testDocumentDigest, Mode: "canary",
		Build: verbsRunningBuild, Owner: "go", RolloutPercentage: 100, ReviewEvidence: "the original decision",
	})
	seedCarryRow(t, ctx, pool, carryIntegrationLiveDigest, CarryRow{
		Operation: "hotspots", DocumentDigest: testDocumentDigest2, Mode: "primary",
		Build: verbsRunningBuild, Owner: "go", RolloutPercentage: 50, ReviewEvidence: "the other decision",
	})
	// featureFlags is ALREADY at the target digest, carrying the evidence
	// a real carry would have written -- so this run reports it UNCHANGED
	// and writes nothing for it.
	seedCarryRow(t, ctx, pool, carryIntegrationTargetDigest, CarryRow{
		Operation: "featureFlags", DocumentDigest: testDocumentDigest, Mode: "canary",
		Build: verbsRunningBuild, Owner: "go", RolloutPercentage: 100,
		ReviewEvidence: CarriedEvidence(carryIntegrationLiveDigest, verbsRunningBuild, time.Now().UTC(), "the original decision"),
	})

	// Anchored on the candidate-build insert that carrying `hotspots`
	// performs; it moves `featureFlags`, the UNCHANGED row.
	if _, err := pool.Exec(ctx, `
		CREATE FUNCTION carry_rerun_guard() RETURNS trigger AS $$
		BEGIN
			UPDATE public.go_api_routing_state
			   SET rollout_percentage = 5
			 WHERE schema_digest = `+quoteLiteral(carryIntegrationLiveDigest)+`
			   AND selected_operation = 'featureFlags';
			RETURN NEW;
		END;
		$$ LANGUAGE plpgsql;
		CREATE TRIGGER carry_rerun_guard_trigger
			BEFORE INSERT ON public.go_api_candidate_build
			FOR EACH ROW
			WHEN (NEW.schema_digest = `+quoteLiteral(carryIntegrationTargetDigest)+`)
			EXECUTE FUNCTION carry_rerun_guard();`); err != nil {
		t.Fatalf("install the racing writer: %v", err)
	}

	outcomes, err := Carry(ctx, pool, carryIntegrationRequest())
	if !errors.Is(err, ErrCarrySourceRowChanged) {
		t.Fatalf("err = %v (outcomes %+v), want ErrCarrySourceRowChanged", err, SummarizeCarry(outcomes))
	}
	if !strings.Contains(err.Error(), "featureFlags") {
		t.Fatalf("the refusal must name the moved UNCHANGED operation, got: %v", err)
	}
	// And the whole run rolled back: `hotspots`, which DID write, is gone.
	rows := rowsAtDigest(t, ctx, pool, carryIntegrationTargetDigest)
	if _, found := carryRowByOperation(rows, "hotspots"); found {
		t.Fatalf("the run must roll back whole; hotspots survived at the target digest: %+v", rows)
	}
}

// r2 F2: a live row that APPEARS during the run is the quiet way to
// break the invariant.
//
// Everything the run copied is still correct, so every other check
// passes and the carry commits reporting success -- while an operation
// somebody enabled while it was running has no row at the digest about
// to go live, and the roll un-routes it. The verb reports that it
// prevented exactly the harm it just failed to prevent.
func TestCarryRefusesWhenANewLiveRowAppearsWhileItRuns(t *testing.T) {
	ctx := t.Context()
	pool := startAuditedRegistryPostgres(t)
	seedCarryRow(t, ctx, pool, carryIntegrationLiveDigest, CarryRow{
		Operation: "featureFlags", DocumentDigest: testDocumentDigest, Mode: "canary",
		Build: verbsRunningBuild, Owner: "go", RolloutPercentage: 100, ReviewEvidence: "the original decision",
	})

	// Anchored on the target-digest insert, so it lands after the survey
	// and before the commit -- the whole window.
	if _, err := pool.Exec(ctx, `
		CREATE FUNCTION carry_new_row_guard() RETURNS trigger AS $$
		BEGIN
			INSERT INTO public.go_api_candidate_build (schema_digest, document_digest, selected_operation, candidate_build)
			VALUES (`+quoteLiteral(carryIntegrationLiveDigest)+`, `+quoteLiteral(testDocumentDigest2)+`, 'hotspots', `+quoteLiteral(verbsRunningBuild)+`)
			ON CONFLICT DO NOTHING;
			INSERT INTO public.go_api_routing_state
				(schema_digest, document_digest, selected_operation, current_candidate_build,
				 owner, mode, rollout_percentage, review_evidence, recorded_by, updated_at)
			VALUES (`+quoteLiteral(carryIntegrationLiveDigest)+`, `+quoteLiteral(testDocumentDigest2)+`, 'hotspots', `+quoteLiteral(verbsRunningBuild)+`,
				'go', 'canary', 100, 'enabled while the carry was running', 'somebody else', now())
			ON CONFLICT DO NOTHING;
			RETURN NEW;
		END;
		$$ LANGUAGE plpgsql;
		CREATE TRIGGER carry_new_row_guard_trigger
			BEFORE INSERT ON public.go_api_routing_state
			FOR EACH ROW
			WHEN (NEW.schema_digest = `+quoteLiteral(carryIntegrationTargetDigest)+`)
			EXECUTE FUNCTION carry_new_row_guard();`); err != nil {
		t.Fatalf("install the racing enabler: %v", err)
	}

	outcomes, err := Carry(ctx, pool, carryIntegrationRequest())
	if !errors.Is(err, ErrCarrySourceRowChanged) {
		t.Fatalf("err = %v (outcomes %+v), want ErrCarrySourceRowChanged -- a live row appeared and was not carried", err, SummarizeCarry(outcomes))
	}
	for _, want := range []string{"hotspots", "APPEARED at the live digest", "the roll would un-route it"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("the refusal must contain %q, got: %v", want, err)
		}
	}
	// ALL OR NOTHING still: the row that DID carry is rolled back too.
	if rows := rowsAtDigest(t, ctx, pool, carryIntegrationTargetDigest); len(rows) != 0 {
		t.Fatalf("the run must roll back whole, found %d row(s): %+v", len(rows), rows)
	}
}

// CONTROL: a row the survey DID see and deliberately skipped must not be
// mistaken for one that appeared. Without this, the new-row check would
// refuse every run that skips anything -- which is most of them -- and
// the verb would be unusable.
func TestCarryDoesNotMistakeASkippedRowForANewOne(t *testing.T) {
	ctx := t.Context()
	pool := startAuditedRegistryPostgres(t)
	seedCarryRow(t, ctx, pool, carryIntegrationLiveDigest, CarryRow{
		Operation: "featureFlags", DocumentDigest: testDocumentDigest, Mode: "canary",
		Build: verbsRunningBuild, Owner: "go", RolloutPercentage: 100, ReviewEvidence: "the original decision",
	})
	// Present at the live digest, surveyed, and SKIPPED: mode python is
	// not reachable, so there is nothing to preserve.
	seedCarryRow(t, ctx, pool, carryIntegrationLiveDigest, CarryRow{
		Operation: "hotspots", DocumentDigest: testDocumentDigest2, Mode: "python",
		Build: verbsRunningBuild, Owner: "python", RolloutPercentage: 0, ReviewEvidence: "deliberately off",
	})

	outcomes, err := Carry(ctx, pool, carryIntegrationRequest())
	if err != nil {
		t.Fatalf("a surveyed-and-skipped row must not be read as a new one: %v", err)
	}
	summary := SummarizeCarry(outcomes)
	if summary.Carried != 1 || summary.Skipped != 1 {
		t.Fatalf("summary = %+v, want one carried and one skipped", summary)
	}
	if rows := rowsAtDigest(t, ctx, pool, carryIntegrationTargetDigest); len(rows) != 1 {
		t.Fatalf("got %d row(s) at the target digest, want exactly the carried one", len(rows))
	}
}

// CHAOS-8144, executed end to end against real Postgres: shadow rows (an
// operation row and an MCP class row) survive a digest change verbatim,
// never become a served mode, are audited as an enable-at-new-key whose
// mode_after is shadow, and the source rows stay untouched.
func TestCarryPreservesShadowRowsVerbatimAndNeverServesThem(t *testing.T) {
	ctx := t.Context()
	pool := startAuditedRegistryPostgres(t)
	orgs := `{"orgs": ["org-9"]}`
	classOp, classDoc := mcpclass.Operation("hotspots"), mcpclass.DocumentDigest()
	seedCarryRow(t, ctx, pool, carryIntegrationLiveDigest, CarryRow{
		Operation: "featureFlags", DocumentDigest: testDocumentDigest, Mode: "canary",
		Build: verbsRunningBuild, Owner: "go", RolloutPercentage: 100, ReviewEvidence: "served",
	})
	seedCarryRow(t, ctx, pool, carryIntegrationLiveDigest, CarryRow{
		Operation: "hotspots", DocumentDigest: testDocumentDigest2, Mode: "shadow",
		Build: verbsRunningBuild, Owner: "go", RolloutPercentage: 0, EligibleOrgs: &orgs, ReviewEvidence: "registered by seed",
	})
	seedCarryRow(t, ctx, pool, carryIntegrationLiveDigest, CarryRow{
		Operation: classOp, DocumentDigest: classDoc, Mode: "shadow",
		Build: verbsRunningBuild, Owner: "go", RolloutPercentage: 0, ReviewEvidence: "class registered by seed",
	})
	request := carryIntegrationRequest()
	request.Inputs.MCPRoots = map[string]bool{"hotspots": true}
	before := rowsAtDigest(t, ctx, pool, carryIntegrationLiveDigest)

	outcomes, err := Carry(ctx, pool, request)
	if err != nil {
		t.Fatalf("Carry: %v", err)
	}
	if summary := SummarizeCarry(outcomes); summary.Carried != 3 || summary.Skipped != 0 {
		t.Fatalf("summary = %+v, want all three carried: %+v", summary, outcomes)
	}
	carried := rowsAtDigest(t, ctx, pool, carryIntegrationTargetDigest)
	if len(carried) != 3 {
		t.Fatalf("got %d row(s) at the target digest, want 3: %+v", len(carried), carried)
	}
	for _, source := range before {
		target, ok := carryRowByOperation(carried, source.Operation)
		if !ok || !sameCarriedState(source, target) {
			t.Fatalf("%s carried as %+v (present=%t), want every copied column identical to %+v", source.Operation, target, ok, source)
		}
		if source.Mode == "shadow" && target.Mode != "shadow" {
			t.Fatalf("%s: a shadow row was carried as %q -- a mode change is an enablement", source.Operation, target.Mode)
		}
		if !strings.HasPrefix(target.ReviewEvidence, CarriedEvidencePrefix) || !strings.Contains(target.ReviewEvidence, source.ReviewEvidence) {
			t.Fatalf("%s carried with evidence %q, want the carry prefix plus the source reason %q", source.Operation, target.ReviewEvidence, source.ReviewEvidence)
		}
	}
	after := rowsAtDigest(t, ctx, pool, carryIntegrationLiveDigest)
	for index := range before {
		if !sameCarriedState(before[index], after[index]) {
			t.Fatalf("live row %+v was modified to %+v", before[index], after[index])
		}
	}
	for _, audit := range readAuditRows(t, ctx, pool) {
		if audit.action != AuditActionEnable || audit.schemaDigest != carryIntegrationTargetDigest {
			t.Fatalf("audit row = %+v, want an enable at the target digest", audit)
		}
		if source, _ := carryRowByOperation(before, audit.operation); audit.modeAfter != source.Mode {
			t.Fatalf("audit for %s says mode_after=%q, want the source mode %q", audit.operation, audit.modeAfter, source.Mode)
		}
	}
}

// CHAOS-8144 / MCP ask (b): a shadow row whose target-digest row DIFFERS
// is skipped with a named reason -- never refused (a served row would
// REFUSE) -- and the different row at the target digest is left untouched.
func TestCarryShadowRowWithADifferentTargetRowIsSkippedNotRefused(t *testing.T) {
	ctx := t.Context()
	pool := startAuditedRegistryPostgres(t)
	seedCarryRow(t, ctx, pool, carryIntegrationLiveDigest, CarryRow{
		Operation: "featureFlags", DocumentDigest: testDocumentDigest, Mode: "canary",
		Build: verbsRunningBuild, Owner: "go", RolloutPercentage: 100, ReviewEvidence: "served",
	})
	seedCarryRow(t, ctx, pool, carryIntegrationLiveDigest, CarryRow{
		Operation: "hotspots", DocumentDigest: testDocumentDigest2, Mode: "shadow",
		Build: verbsRunningBuild, Owner: "go", RolloutPercentage: 0, ReviewEvidence: "shadow",
	})
	decided := CarryRow{
		Operation: "hotspots", DocumentDigest: testDocumentDigest2, Mode: "canary",
		Build: verbsRunningBuild, Owner: "go", RolloutPercentage: 10, ReviewEvidence: "somebody decided this at the target",
	}
	seedCarryRow(t, ctx, pool, carryIntegrationTargetDigest, decided)

	outcomes, err := Carry(ctx, pool, carryIntegrationRequest())
	if err != nil {
		t.Fatalf("Carry: %v (a shadow row must never refuse a roll)", err)
	}
	var skipped *CarryOutcome
	for index := range outcomes {
		if outcomes[index].Operation == "hotspots" {
			skipped = &outcomes[index]
		}
	}
	if skipped == nil || skipped.Action != CarryActionSkip || !strings.Contains(skipped.Reason, "already holds a different row") {
		t.Fatalf("hotspots outcome = %+v, want a SKIP naming the different target row", skipped)
	}
	row, _ := carryRowByOperation(rowsAtDigest(t, ctx, pool, carryIntegrationTargetDigest), "hotspots")
	if !sameCarriedState(row, decided) || row.ReviewEvidence != decided.ReviewEvidence {
		t.Fatalf("target row = %+v, want the decision taken there untouched (%+v)", row, decided)
	}
}

// CHAOS-8144: a STALE shadow row (document moved, operation gone from the
// image, build not running) is skipped with a reason and never blocks the
// roll; the same three defects on a SERVED row still refuse.
func TestCarryStaleShadowRowsSkipWhereServedRowsRefuse(t *testing.T) {
	ctx := t.Context()
	for name, tc := range map[string]struct {
		mutate  func(*CarryRequest, *CarryRow)
		refuses error
		reason  string
	}{
		"document moved at target": {
			mutate: func(r *CarryRequest, row *CarryRow) {
				r.Inputs.TargetDocumentDigest[row.Operation] = strings.Repeat("7", 64)
			},
			refuses: ErrCarryDocumentMoved, reason: "the registered document changed",
		},
		"operation gone from the image": {
			mutate:  func(r *CarryRequest, row *CarryRow) { delete(r.Inputs.TargetDocumentDigest, row.Operation) },
			refuses: ErrCarryDocumentMoved, reason: "does not register this operation",
		},
		"build is not the running one": {
			mutate:  func(r *CarryRequest, row *CarryRow) { row.Build = "some-older-build" },
			refuses: ErrCarryBuildNotRunning, reason: "names build some-older-build",
		},
	} {
		for _, mode := range []string{"shadow", "canary"} {
			t.Run(name+"/"+mode, func(t *testing.T) {
				pool := startAuditedRegistryPostgres(t)
				seedCarryRow(t, ctx, pool, carryIntegrationLiveDigest, CarryRow{
					Operation: "featureFlags", DocumentDigest: testDocumentDigest, Mode: "canary",
					Build: verbsRunningBuild, Owner: "go", RolloutPercentage: 100, ReviewEvidence: "served anchor",
				})
				row := CarryRow{
					Operation: "hotspots", DocumentDigest: testDocumentDigest2, Mode: mode,
					Build: verbsRunningBuild, Owner: "go", RolloutPercentage: 100, ReviewEvidence: "subject",
				}
				request := carryIntegrationRequest()
				tc.mutate(&request, &row)
				seedCarryRow(t, ctx, pool, carryIntegrationLiveDigest, row)
				outcomes, err := Carry(ctx, pool, request)
				if mode == "canary" {
					if !errors.Is(err, tc.refuses) {
						t.Fatalf("served row: err = %v, want %v", err, tc.refuses)
					}
					return
				}
				if err != nil {
					t.Fatalf("shadow row blocked the carry: %v", err)
				}
				for _, outcome := range outcomes {
					if outcome.Operation == "hotspots" && (outcome.Action != CarryActionSkip || !strings.Contains(outcome.Reason, tc.reason)) {
						t.Fatalf("hotspots outcome = %+v, want a SKIP naming %q", outcome, tc.reason)
					}
				}
				if _, ok := carryRowByOperation(rowsAtDigest(t, ctx, pool, carryIntegrationTargetDigest), "hotspots"); ok {
					t.Fatal("a stale shadow row was written at the target digest")
				}
			})
		}
	}
}

// CHAOS-8144 (r1 P1): a digest whose live rows are ALL shadow is never
// refused. A valid shadow-only run writes its registrations (the refusal used
// to fire after the plan had printed CARRY lines, leaving the target row
// absent behind a report that said it was written); a stale shadow-only run
// writes nothing, names every skip, and still succeeds. The refusal survives
// where a non-shadow live row exists and every row was skipped.
func TestCarryShadowOnlyDigestIsNeverRefused(t *testing.T) {
	ctx := t.Context()
	t.Run("valid shadow row is written", func(t *testing.T) {
		pool := startAuditedRegistryPostgres(t)
		seedCarryRow(t, ctx, pool, carryIntegrationLiveDigest, CarryRow{
			Operation: "featureFlags", DocumentDigest: testDocumentDigest, Mode: "shadow",
			Build: verbsRunningBuild, Owner: "go", RolloutPercentage: 0, ReviewEvidence: "shadow only",
		})
		outcomes, err := Carry(ctx, pool, carryIntegrationRequest())
		if err != nil {
			t.Fatalf("Carry: %v (a valid shadow-only digest must not be refused)", err)
		}
		if summary := SummarizeCarry(outcomes); summary.Carried != 1 {
			t.Fatalf("summary = %+v, want the shadow row carried", summary)
		}
		row, ok := carryRowByOperation(rowsAtDigest(t, ctx, pool, carryIntegrationTargetDigest), "featureFlags")
		if !ok || row.Mode != "shadow" || row.RolloutPercentage != 0 {
			t.Fatalf("target row = %+v (present=%t), want the shadow registration written verbatim", row, ok)
		}
	})
	t.Run("stale shadow row is skipped and the run succeeds", func(t *testing.T) {
		pool := startAuditedRegistryPostgres(t)
		seedCarryRow(t, ctx, pool, carryIntegrationLiveDigest, CarryRow{
			Operation: "featureFlags", DocumentDigest: testDocumentDigest, Mode: "shadow",
			Build: "some-older-build", Owner: "go", RolloutPercentage: 0, ReviewEvidence: "stale shadow only",
		})
		outcomes, err := Carry(ctx, pool, carryIntegrationRequest())
		if err != nil {
			t.Fatalf("Carry: %v (a stale shadow row must never block a roll)", err)
		}
		if len(outcomes) != 1 || outcomes[0].Action != CarryActionSkip || outcomes[0].Reason == "" {
			t.Fatalf("outcomes = %+v, want one named SKIP", outcomes)
		}
		if rows := rowsAtDigest(t, ctx, pool, carryIntegrationTargetDigest); len(rows) != 0 {
			t.Fatalf("wrote %d row(s) for a skipped shadow row", len(rows))
		}
	})
	t.Run("a non-shadow live row with every row skipped still refuses", func(t *testing.T) {
		pool := startAuditedRegistryPostgres(t)
		seedCarryRow(t, ctx, pool, carryIntegrationLiveDigest, CarryRow{
			Operation: "featureFlags", DocumentDigest: testDocumentDigest, Mode: "shadow",
			Build: "some-older-build", Owner: "go", RolloutPercentage: 0, ReviewEvidence: "stale shadow",
		})
		seedCarryRow(t, ctx, pool, carryIntegrationLiveDigest, CarryRow{
			Operation: "hotspots", DocumentDigest: testDocumentDigest2, Mode: "python",
			Build: verbsRunningBuild, Owner: "go", RolloutPercentage: 0, ReviewEvidence: "off",
		})
		if _, err := Carry(ctx, pool, carryIntegrationRequest()); !errors.Is(err, ErrCarryNothingReachable) {
			t.Fatalf("err = %v, want the nothing-reachable refusal", err)
		}
	})
}

// MCP ask (a): every live row ends in exactly one named outcome, so a
// post-roll readback can fail on a pre-roll row with no row at the target
// unless the plan NAMED it SKIP.
func TestCarryAccountsForEveryLiveRowWithOneOutcome(t *testing.T) {
	ctx := t.Context()
	pool := startAuditedRegistryPostgres(t)
	seeds := []CarryRow{
		{Operation: "featureFlags", DocumentDigest: testDocumentDigest, Mode: "canary", Build: verbsRunningBuild, Owner: "go", RolloutPercentage: 100, ReviewEvidence: "a"},
		{Operation: "hotspots", DocumentDigest: testDocumentDigest2, Mode: "shadow", Build: verbsRunningBuild, Owner: "go", ReviewEvidence: "b"},
		{Operation: "dropped", DocumentDigest: strings.Repeat("c", 64), Mode: "python", Build: verbsRunningBuild, Owner: "go", ReviewEvidence: "c"},
		{Operation: mcpclass.Operation("hotspots"), DocumentDigest: mcpclass.DocumentDigest(), Mode: "shadow", Build: verbsRunningBuild, Owner: "go", ReviewEvidence: "d"},
	}
	for _, seed := range seeds {
		seedCarryRow(t, ctx, pool, carryIntegrationLiveDigest, seed)
	}
	request := carryIntegrationRequest()
	request.Inputs.MCPRoots = map[string]bool{} // root NOT served: the class shadow row is dropped
	outcomes, err := Carry(ctx, pool, request)
	if err != nil {
		t.Fatalf("Carry: %v", err)
	}
	if len(outcomes) != len(seeds) {
		t.Fatalf("got %d outcome(s) for %d live row(s): %+v", len(outcomes), len(seeds), outcomes)
	}
	target := rowsAtDigest(t, ctx, pool, carryIntegrationTargetDigest)
	for _, outcome := range outcomes {
		_, atTarget := carryRowByOperation(target, outcome.Operation)
		switch outcome.Action {
		case CarryActionCarry, CarryActionUnchanged:
			if !atTarget {
				t.Fatalf("%s reported %s but has no row at the target digest", outcome.Operation, outcome.Action)
			}
		case CarryActionSkip:
			if atTarget || outcome.Reason == "" {
				t.Fatalf("%s is SKIP but atTarget=%t reason=%q: a dropped row must be absent AND named", outcome.Operation, atTarget, outcome.Reason)
			}
		default:
			t.Fatalf("%s ended as %s", outcome.Operation, outcome.Action)
		}
	}
}

// CHAOS-8144: a run whose only kept row is PRIMARY still counts as having
// preserved reachability (the served-mode clause must admit primary, not
// just canary), and a stale shadow row beside it is skipped with no
// evidence text written for it.
func TestCarryPrimaryOnlyRunSucceedsAndASkippedShadowCarriesNoEvidence(t *testing.T) {
	ctx := t.Context()
	pool := startAuditedRegistryPostgres(t)
	seedCarryRow(t, ctx, pool, carryIntegrationLiveDigest, CarryRow{
		Operation: "featureFlags", DocumentDigest: testDocumentDigest, Mode: "primary",
		Build: verbsRunningBuild, Owner: "go", RolloutPercentage: 100, ReviewEvidence: "primary",
	})
	seedCarryRow(t, ctx, pool, carryIntegrationLiveDigest, CarryRow{
		Operation: "hotspots", DocumentDigest: testDocumentDigest2, Mode: "shadow",
		Build: "some-older-build", Owner: "go", RolloutPercentage: 0, ReviewEvidence: "stale shadow",
	})
	outcomes, err := Carry(ctx, pool, carryIntegrationRequest())
	if err != nil {
		t.Fatalf("Carry: %v (a primary row alone is reachability)", err)
	}
	for _, outcome := range outcomes {
		switch outcome.Operation {
		case "featureFlags":
			if outcome.Action != CarryActionCarry {
				t.Fatalf("featureFlags = %+v, want CARRY", outcome)
			}
		case "hotspots":
			if outcome.Action != CarryActionSkip || outcome.ReviewEvidence != "" {
				t.Fatalf("hotspots = %+v, want a SKIP with no would-be evidence text", outcome)
			}
		}
	}
}

// CHAOS-8144 (R311, after r1 found a path the body did not name): the RUN-LEVEL
// decision table, every cell executed against real Postgres. Axes: the kinds of
// live row present (served valid, served unfaithful, shadow valid, shadow stale,
// python, a mix) x the run options (-operations, dry run). Row-level outcomes
// are enumerated by TestDecideCarryOverItsWholeInputDomain; this table is what
// the whole run does with them: refuse, succeed with writes, or succeed with
// none. The same table is in the lane's invariant.md.
func TestCarryRunLevelDecisionTable(t *testing.T) {
	ctx := t.Context()
	served := CarryRow{Operation: "featureFlags", DocumentDigest: testDocumentDigest, Mode: "canary", Build: verbsRunningBuild, Owner: "go", RolloutPercentage: 100, ReviewEvidence: "served"}
	shadow := CarryRow{Operation: "hotspots", DocumentDigest: testDocumentDigest2, Mode: "shadow", Build: verbsRunningBuild, Owner: "go", ReviewEvidence: "shadow"}
	staleShadow := shadow
	staleShadow.Build = "some-older-build"
	python := CarryRow{Operation: "savedReports", DocumentDigest: strings.Repeat("e", 64), Mode: "python", Build: verbsRunningBuild, Owner: "go", ReviewEvidence: "off"}
	for name, tc := range map[string]struct {
		rows     []CarryRow
		mutate   func(*CarryRequest)
		wantErr  error
		wantRows []string // operations at the target digest after the run
	}{
		"R1 served valid only":                      {rows: []CarryRow{served}, wantRows: []string{"featureFlags"}},
		"R2 shadow valid only":                      {rows: []CarryRow{shadow}, wantRows: []string{"hotspots"}},
		"R3 shadow stale only (succeeds, no write)": {rows: []CarryRow{staleShadow}, wantRows: nil},
		"R4 stale shadow + python (nothing kept)":   {rows: []CarryRow{staleShadow, python}, wantErr: ErrCarryNothingReachable},
		"R5 served valid + shadow valid":            {rows: []CarryRow{served, shadow}, wantRows: []string{"featureFlags", "hotspots"}},
		"R6 served valid + shadow stale":            {rows: []CarryRow{served, staleShadow}, wantRows: []string{"featureFlags"}},
		"R7 served unfaithful + shadow valid (REFUSE, nothing written)": {
			rows:    []CarryRow{served, shadow},
			mutate:  func(r *CarryRequest) { r.Inputs.TargetDocumentDigest["featureFlags"] = strings.Repeat("7", 64) },
			wantErr: ErrCarryDocumentMoved,
		},
		"R8 python only":                         {rows: []CarryRow{python}, wantErr: ErrCarryNothingReachable},
		"R9 shadow valid + python (shadow kept)": {rows: []CarryRow{shadow, python}, wantRows: []string{"hotspots"}},
		"R10 shadow valid + python, -operations on the python row only": {
			rows:    []CarryRow{shadow, python},
			mutate:  func(r *CarryRequest) { r.Operations = []string{"savedReports"} },
			wantErr: ErrCarryNothingReachable,
		},
		"R11 shadow valid + python, -operations on the shadow row only": {
			rows:     []CarryRow{shadow, python},
			mutate:   func(r *CarryRequest) { r.Operations = []string{"hotspots"} },
			wantRows: []string{"hotspots"},
		},
		"R12 shadow valid only, dry run (plan, no write)": {
			rows:   []CarryRow{shadow},
			mutate: func(r *CarryRequest) { r.DryRun = true },
		},
	} {
		t.Run(name, func(t *testing.T) {
			pool := startAuditedRegistryPostgres(t)
			request := carryIntegrationRequest()
			for _, row := range tc.rows {
				seedCarryRow(t, ctx, pool, carryIntegrationLiveDigest, row)
			}
			if tc.mutate != nil {
				tc.mutate(&request)
			}
			outcomes, err := Carry(ctx, pool, request)
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("err = %v, want %v", err, tc.wantErr)
				}
			} else if err != nil {
				t.Fatalf("Carry: %v", err)
			}
			if tc.wantErr == nil {
				for _, outcome := range outcomes {
					if outcome.Action == CarryActionSkip && outcome.Reason == "" {
						t.Fatalf("%s skipped with no reason", outcome.Operation)
					}
				}
			}
			var got []string
			for _, row := range rowsAtDigest(t, ctx, pool, carryIntegrationTargetDigest) {
				got = append(got, row.Operation)
			}
			sort.Strings(got)
			want := append([]string(nil), tc.wantRows...)
			sort.Strings(want)
			if strings.Join(got, ",") != strings.Join(want, ",") {
				t.Fatalf("rows at the target digest = %v, want %v", got, want)
			}
		})
	}
}
