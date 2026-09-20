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
		ReviewEvidence: UnprovenEvidencePrefix + "chris ruled it",
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
func TestCarrySkipsRowsWithNoReachabilityToPreserve(t *testing.T) {
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
	if summary.Carried != 1 || summary.Skipped != 2 {
		t.Fatalf("summary = %+v, want one carried and two skipped: %+v", summary, outcomes)
	}
	for _, outcome := range outcomes {
		if outcome.Action == CarryActionSkip && outcome.Reason == "" {
			t.Fatalf("%s was skipped with no reason, so an operator cannot tell what was left behind", outcome.Operation)
		}
	}
	if rows := rowsAtDigest(t, ctx, pool, carryIntegrationTargetDigest); len(rows) != 1 || rows[0].Operation != "featureFlags" {
		t.Fatalf("rows at the target digest = %+v, want only the reachable one", rows)
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

// An empty table and a table whose rows all died must not read alike.
func TestCarryRefusesWhenTheLiveDigestHasNoRowAtAll(t *testing.T) {
	ctx := t.Context()
	pool := startAuditedRegistryPostgres(t)
	if _, err := Carry(ctx, pool, carryIntegrationRequest()); !errors.Is(err, ErrCarryNoLiveRows) {
		t.Fatalf("err = %v, want the refusal that names the empty live digest", err)
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
