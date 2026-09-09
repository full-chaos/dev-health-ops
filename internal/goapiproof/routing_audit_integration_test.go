//go:build integration

package goapiproof

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// auditDDL mirrors alembic 0129, which creates go_api_routing_audits.
//
// The constraints are NOT trimmed to "what the test needs", for the same
// reason registryDDL's are not: the action and credential-class CHECKs are
// the DATABASE-level reason a routing write cannot be recorded under a
// verb or a credential class nobody authorized, and the pairing CHECK is
// the reason "a verified credential named this subject" cannot be
// confused with "no credential was presented". A test against a relaxed
// schema would pass while the real one rejected every write.
//
// The drift check for this mirror lives in PYTHON
// (tests/test_0129_go_api_routing_audits_migration.py), deliberately, for
// the reason registryDDL's own comment gives: reading the alembic files
// FROM a Go test makes them inputs to the Go workflow, and go.yml's path
// filters do not cover src/dev_health_ops/alembic -- so a PR changing only
// a migration would satisfy go-quality vacuously.
const auditDDL = `
CREATE TABLE go_api_routing_audits (
	id BIGSERIAL NOT NULL PRIMARY KEY,
	correlation_id UUID NOT NULL,
	action TEXT NOT NULL,
	credential_class TEXT NOT NULL,
	principal_id TEXT,
	recorded_by TEXT NOT NULL,
	review_evidence TEXT NOT NULL,
	schema_digest TEXT NOT NULL,
	document_digest TEXT NOT NULL,
	selected_operation TEXT NOT NULL,
	candidate_build_before TEXT,
	candidate_build_after TEXT NOT NULL,
	mode_before TEXT,
	mode_after TEXT NOT NULL,
	recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
	CONSTRAINT ck_go_api_routing_audits_action
		CHECK (action IN ('enable', 'disable', 'repoint')),
	CONSTRAINT ck_go_api_routing_audits_credential_class
		CHECK (credential_class IN ('effective_principal_envelope', 'operator_direct')),
	CONSTRAINT ck_go_api_routing_audits_principal_pairing
		CHECK ((credential_class = 'effective_principal_envelope') = (principal_id IS NOT NULL)),
	CONSTRAINT ck_go_api_routing_audits_mode_before
		CHECK (mode_before IS NULL OR mode_before IN ('python', 'shadow', 'canary', 'primary', 'disabled')),
	CONSTRAINT ck_go_api_routing_audits_mode_after
		CHECK (mode_after IN ('python', 'shadow', 'canary', 'primary', 'disabled')),
	CONSTRAINT ck_go_api_routing_audits_review_evidence_bounded
		CHECK (char_length(review_evidence) BETWEEN 1 AND 2000),
	CONSTRAINT ck_go_api_routing_audits_recorded_by_bounded
		CHECK (char_length(recorded_by) BETWEEN 1 AND 128)
);
CREATE INDEX ix_go_api_routing_audits_correlation ON go_api_routing_audits (correlation_id);
CREATE INDEX ix_go_api_routing_audits_operation_recorded
	ON go_api_routing_audits (schema_digest, selected_operation, recorded_at DESC);
`

// startAuditedRegistryPostgres is startRegistryPostgres plus the audit
// table, so a verb's routing write and its audit row meet the SAME
// constraints Postgres will apply in production.
func startAuditedRegistryPostgres(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool := startRegistryPostgres(t)
	if _, err := pool.Exec(context.Background(), auditDDL); err != nil {
		t.Fatalf("create audit schema: %v", err)
	}
	return pool
}

type auditRow struct {
	correlationID, action, credentialClass  string
	principalID                             *string
	recordedBy, reviewEvidence              string
	schemaDigest, documentDigest, operation string
	buildBefore, modeBefore                 *string
	buildAfter, modeAfter                   string
}

func readAuditRows(t *testing.T, ctx context.Context, pool *pgxpool.Pool) []auditRow {
	t.Helper()
	rows, err := pool.Query(ctx, `
		SELECT correlation_id::text, action, credential_class, principal_id, recorded_by,
		       review_evidence, schema_digest, document_digest, selected_operation,
		       candidate_build_before, candidate_build_after, mode_before, mode_after
		  FROM go_api_routing_audits
		 ORDER BY selected_operation, id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var found []auditRow
	for rows.Next() {
		var row auditRow
		if err := rows.Scan(&row.correlationID, &row.action, &row.credentialClass, &row.principalID,
			&row.recordedBy, &row.reviewEvidence, &row.schemaDigest, &row.documentDigest,
			&row.operation, &row.buildBefore, &row.buildAfter, &row.modeBefore, &row.modeAfter); err != nil {
			t.Fatal(err)
		}
		found = append(found, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return found
}

const testPrincipalID = "b0a1c2d3-0000-4000-8000-000000000001"

// THE GAP CHAOS-5505 CLOSES. Before this, the only record of a routing
// write was recorded_by/review_evidence on the MUTABLE routing row, which
// the next write overwrites.
func TestEnableWritesAnAuditRowPerOperationSharingOneCorrelationID(t *testing.T) {
	ctx := t.Context()
	pool := startAuditedRegistryPostgres(t)
	seedProof(t, ctx, pool, "featureFlags", testDocumentDigest, verbsRunningBuild, EnablementProofStage, EnablementProofTerminalState)
	seedProof(t, ctx, pool, "hotspots", testDocumentDigest2, verbsRunningBuild, EnablementProofStage, EnablementProofTerminalState)

	request := enableRequest("featureFlags", "hotspots")
	request.DocumentDigest["hotspots"] = testDocumentDigest2
	if _, err := Enable(ctx, pool, request); err != nil {
		t.Fatalf("Enable: %v", err)
	}

	audits := readAuditRows(t, ctx, pool)
	if len(audits) != 2 {
		t.Fatalf("got %d audit row(s), want one per operation: %+v", len(audits), audits)
	}
	if audits[0].correlationID != audits[1].correlationID || audits[0].correlationID == "" {
		t.Fatalf("one invocation's rows must share a correlation id, got %q and %q", audits[0].correlationID, audits[1].correlationID)
	}
	for _, row := range audits {
		if row.action != AuditActionEnable || row.credentialClass != CredentialClassEnvelope {
			t.Fatalf("row = %+v, want an enable under the envelope class", row)
		}
		// The two identity columns answer DIFFERENT questions and must
		// not be conflated: principal_id is what the verified credential
		// said, recorded_by is what the operator typed about themselves.
		if row.principalID == nil || *row.principalID != testPrincipalID {
			t.Fatalf("principal_id = %v, want the envelope subject %q", row.principalID, testPrincipalID)
		}
		if row.recordedBy != "lane-routing-verbs" {
			t.Fatalf("recorded_by = %q, want the operator's own -recorded-by", row.recordedBy)
		}
		if row.schemaDigest != testSchemaDigest {
			t.Fatalf("schema_digest = %q -- without it a row cannot tell a live write from one against a dead digest", row.schemaDigest)
		}
		if row.buildAfter != verbsRunningBuild || row.modeAfter != "canary" {
			t.Fatalf("row = %+v, want the after-state it actually wrote", row)
		}
		// No row existed before, so both before-values are NULL rather
		// than an invented default.
		if row.buildBefore != nil || row.modeBefore != nil {
			t.Fatalf("row = %+v: an operation with NO prior row must record NULL before-values, not a guess", row)
		}
	}
}

// An enable over an EXISTING row records what it replaced. "There was no
// row" and "the row said python" are different facts.
func TestEnableRecordsTheStateItReplaced(t *testing.T) {
	ctx := t.Context()
	pool := startAuditedRegistryPostgres(t)
	seedRow(t, ctx, "featureFlags", testDocumentDigest, "shadow", testCandidateBuild, pool)
	seedProof(t, ctx, pool, "featureFlags", testDocumentDigest, verbsRunningBuild, EnablementProofStage, EnablementProofTerminalState)

	if _, err := Enable(ctx, pool, enableRequest("featureFlags")); err != nil {
		t.Fatalf("Enable: %v", err)
	}
	audits := readAuditRows(t, ctx, pool)
	if len(audits) != 1 {
		t.Fatalf("audits = %+v, want exactly one", audits)
	}
	row := audits[0]
	if row.modeBefore == nil || *row.modeBefore != "shadow" || row.modeAfter != "canary" {
		t.Fatalf("mode before/after = %v -> %q, want shadow -> canary", row.modeBefore, row.modeAfter)
	}
	if row.buildBefore == nil || *row.buildBefore != testCandidateBuild || row.buildAfter != verbsRunningBuild {
		t.Fatalf("build before/after = %v -> %q, want %s -> %s", row.buildBefore, row.buildAfter, testCandidateBuild, verbsRunningBuild)
	}
}

// The audit row commits with the routing write or not at all. A REFUSED
// enable must leave no trace claiming an operator enabled something.
func TestARefusedEnableWritesNeitherARoutingRowNorAnAuditRow(t *testing.T) {
	ctx := t.Context()
	pool := startAuditedRegistryPostgres(t)

	if _, err := Enable(ctx, pool, enableRequest("featureFlags")); !errors.Is(err, ErrEnableUnproven) {
		t.Fatalf("Enable = %v, want ErrEnableUnproven", err)
	}
	if audits := readAuditRows(t, ctx, pool); len(audits) != 0 {
		t.Fatalf("a refused enable wrote %d audit row(s): %+v", len(audits), audits)
	}
}

// A dry run writes nothing at all, audit rows included.
func TestADryRunWritesNoAuditRows(t *testing.T) {
	ctx := t.Context()
	pool := startAuditedRegistryPostgres(t)
	seedProof(t, ctx, pool, "featureFlags", testDocumentDigest, verbsRunningBuild, EnablementProofStage, EnablementProofTerminalState)

	request := enableRequest("featureFlags")
	request.DryRun = true
	if _, err := Enable(ctx, pool, request); err != nil {
		t.Fatalf("Enable dry-run: %v", err)
	}
	if audits := readAuditRows(t, ctx, pool); len(audits) != 0 {
		t.Fatalf("a dry run wrote %d audit row(s): %+v", len(audits), audits)
	}
}

// `disable` verified NO credential, so its row says so: class
// operator_direct and principal_id NULL. recorded_by still carries what
// the operator typed -- it is simply not dressed up as something a
// credential asserted.
func TestDisableRecordsOperatorDirectWithNoPrincipal(t *testing.T) {
	for _, mode := range DisableModes {
		t.Run(mode, func(t *testing.T) {
			ctx := t.Context()
			pool := startAuditedRegistryPostgres(t)
			seedRow(t, ctx, "featureFlags", testDocumentDigest, "canary", verbsRunningBuild, pool)

			if _, err := Disable(ctx, pool, DisableRequest{
				SchemaDigest:   testSchemaDigest,
				Operations:     []string{"featureFlags"},
				DocumentDigest: map[string]string{"featureFlags": testDocumentDigest},
				NewMode:        mode,
				RecordedBy:     "lane-routing-verbs",
				ReviewEvidence: "CHAOS-5505 rollback",
				Apply:          true,
			}); err != nil {
				t.Fatalf("Disable: %v", err)
			}
			audits := readAuditRows(t, ctx, pool)
			if len(audits) != 1 {
				t.Fatalf("audits = %+v, want exactly one", audits)
			}
			row := audits[0]
			if row.action != AuditActionDisable || row.credentialClass != CredentialClassOperatorDirect {
				t.Fatalf("row = %+v, want a disable under operator_direct", row)
			}
			if row.principalID != nil {
				t.Fatalf("principal_id = %v: this verb verified no credential, so there is no subject to name", *row.principalID)
			}
			if row.recordedBy != "lane-routing-verbs" {
				t.Fatalf("recorded_by = %q, want the operator's self-assertion", row.recordedBy)
			}
			if row.modeBefore == nil || *row.modeBefore != "canary" || row.modeAfter != mode {
				t.Fatalf("mode before/after = %v -> %q, want canary -> %s", row.modeBefore, row.modeAfter, mode)
			}
			// disable changes mode ONLY, and the row proves it rather
			// than a comment asserting it.
			if row.buildBefore == nil || *row.buildBefore != row.buildAfter {
				t.Fatalf("build before/after = %v -> %q, want them EQUAL: disable never writes the candidate build", row.buildBefore, row.buildAfter)
			}
		})
	}
}

// Only rows that ACTUALLY moved are audited. An operation with no row was
// never touched, and an entry for it would record a change that did not
// happen in a table nothing can later correct.
func TestDisableAuditsOnlyTheRowsItActuallyWrote(t *testing.T) {
	ctx := t.Context()
	pool := startAuditedRegistryPostgres(t)
	seedRow(t, ctx, "featureFlags", testDocumentDigest, "canary", verbsRunningBuild, pool)

	changes, err := Disable(ctx, pool, DisableRequest{
		SchemaDigest: testSchemaDigest,
		Operations:   []string{"featureFlags", "hotspots"},
		DocumentDigest: map[string]string{
			"featureFlags": testDocumentDigest,
			"hotspots":     testDocumentDigest2,
		},
		NewMode:        "python",
		RecordedBy:     "lane-routing-verbs",
		ReviewEvidence: "CHAOS-5505 rollback",
		Apply:          true,
	})
	if err != nil {
		t.Fatalf("Disable: %v", err)
	}
	if summary := SummarizeDisable(changes); summary.Applied != 1 || summary.NoRow != 1 {
		t.Fatalf("summary = %+v, want one applied and one with no row", summary)
	}
	audits := readAuditRows(t, ctx, pool)
	if len(audits) != 1 || audits[0].operation != "featureFlags" {
		t.Fatalf("audits = %+v, want exactly one, for featureFlags -- hotspots was never touched", audits)
	}
}

// A re-point that moves nothing wrote nothing, so it audits nothing.
func TestRepointAuditsOnlyTheRowsThatMoved(t *testing.T) {
	ctx := t.Context()
	pool := startAuditedRegistryPostgres(t)
	seedRow(t, ctx, "featureFlags", testDocumentDigest, "shadow", testCandidateBuild, pool)
	seedRow(t, ctx, "hotspots", testDocumentDigest2, "canary", verbsRunningBuild, pool)

	outcomes, err := Repoint(ctx, pool, RepointRequest{
		SchemaDigest:   testSchemaDigest,
		RunningBuild:   verbsRunningBuild,
		RecordedBy:     "lane-routing-verbs",
		ReviewEvidence: "CHAOS-5505 re-point",
		PrincipalID:    testPrincipalID,
	})
	if err != nil {
		t.Fatalf("Repoint: %v", err)
	}
	if summary := Summarize(outcomes); summary.Changed != 1 || summary.Unchanged != 1 {
		t.Fatalf("summary = %+v, want one changed and one already correct", summary)
	}
	audits := readAuditRows(t, ctx, pool)
	if len(audits) != 1 {
		t.Fatalf("audits = %+v, want exactly one -- hotspots already named the running build", audits)
	}
	row := audits[0]
	if row.action != AuditActionRepoint || row.credentialClass != CredentialClassEnvelope {
		t.Fatalf("row = %+v, want a repoint under the envelope class", row)
	}
	// The whole contract of a re-point, now checkable from the row.
	if row.modeBefore == nil || *row.modeBefore != "shadow" || row.modeAfter != "shadow" {
		t.Fatalf("mode before/after = %v -> %q, want shadow -> shadow: a re-point never touches reachability", row.modeBefore, row.modeAfter)
	}
	if row.buildBefore == nil || *row.buildBefore != testCandidateBuild || row.buildAfter != verbsRunningBuild {
		t.Fatalf("build before/after = %v -> %q, want %s -> %s", row.buildBefore, row.buildAfter, testCandidateBuild, verbsRunningBuild)
	}

	// Idempotence: the second run moves nothing, so it must add nothing.
	if _, err := Repoint(ctx, pool, RepointRequest{
		SchemaDigest:   testSchemaDigest,
		RunningBuild:   verbsRunningBuild,
		RecordedBy:     "lane-routing-verbs",
		ReviewEvidence: "CHAOS-5505 re-point again",
		PrincipalID:    testPrincipalID,
	}); err != nil {
		t.Fatalf("Repoint (second): %v", err)
	}
	if audits := readAuditRows(t, ctx, pool); len(audits) != 1 {
		t.Fatalf("a re-run that moved nothing added audit rows: %+v", audits)
	}
}

// The pairing CHECK 0129 adds is real, and this proves it against the
// database rather than trusting the writer to remember.
func TestTheDatabaseEnforcesTheCredentialPairing(t *testing.T) {
	ctx := t.Context()
	pool := startAuditedRegistryPostgres(t)

	insert := func(class string, principal any) error {
		_, err := pool.Exec(ctx, `
			INSERT INTO go_api_routing_audits
				(correlation_id, action, credential_class, principal_id, recorded_by,
				 review_evidence, schema_digest, document_digest, selected_operation,
				 candidate_build_after, mode_after, recorded_at)
			VALUES (gen_random_uuid(), 'enable', $1, $2, 'who', 'why', 's', 'd', 'op', 'b', 'canary', now())`,
			class, principal)
		return err
	}
	if err := insert(CredentialClassEnvelope, nil); err == nil {
		t.Fatal("an envelope-class row with NO principal must be refused -- the class means a verified credential named a subject")
	}
	if err := insert(CredentialClassOperatorDirect, "someone"); err == nil {
		t.Fatal("an operator_direct row that names a principal must be refused -- there was no credential to have carried one")
	}
	if err := insert(CredentialClassEnvelope, testPrincipalID); err != nil {
		t.Fatalf("the legal envelope shape must write: %v", err)
	}
	if err := insert(CredentialClassOperatorDirect, nil); err != nil {
		t.Fatalf("the legal operator_direct shape must write: %v", err)
	}
	if err := insert("something_else", nil); err == nil {
		t.Fatal("the credential-class vocabulary is closed: an unlisted class must be refused by the database")
	}
}

// preMigrationSchema is the registry WITHOUT go_api_routing_audits: a
// binary ahead of its schema. The refusal must name the migration rather
// than surface a raw Postgres error an operator cannot act on.
func TestAVerbAgainstAPre0129SchemaNamesTheMissingMigration(t *testing.T) {
	ctx := t.Context()
	pool := startRegistryPostgres(t) // deliberately NOT the audited variant
	seedRow(t, ctx, "featureFlags", testDocumentDigest, "canary", verbsRunningBuild, pool)

	_, err := Disable(ctx, pool, DisableRequest{
		SchemaDigest:   testSchemaDigest,
		Operations:     []string{"featureFlags"},
		DocumentDigest: map[string]string{"featureFlags": testDocumentDigest},
		NewMode:        "python",
		RecordedBy:     "lane-routing-verbs",
		ReviewEvidence: "CHAOS-5505 against an unmigrated schema",
		Apply:          true,
	})
	if !errors.Is(err, ErrAuditTableNotMigrated) {
		t.Fatalf("Disable = %v, want ErrAuditTableNotMigrated -- an operator reading a raw 42P01 cannot tell a missing migration from a code bug", err)
	}
	if !strings.Contains(err.Error(), "0129") {
		t.Fatalf("the refusal must name the migration to apply, got %q", err)
	}
	var mode string
	if err := pool.QueryRow(ctx, `
		SELECT mode FROM go_api_routing_state
		 WHERE schema_digest = $1 AND selected_operation = 'featureFlags'`, testSchemaDigest).
		Scan(&mode); err != nil {
		t.Fatal(err)
	}
	if mode != "canary" {
		t.Fatalf("mode = %q: the routing write must have rolled back with its audit row", mode)
	}
}

// review_evidence is bounded by the database, and the writer refuses
// BEFORE reaching it so the message says what to do.
func TestAnOverlongReviewEvidenceIsRefusedRatherThanTruncated(t *testing.T) {
	ctx := t.Context()
	pool := startAuditedRegistryPostgres(t)
	seedRow(t, ctx, "featureFlags", testDocumentDigest, "canary", verbsRunningBuild, pool)

	_, err := Disable(ctx, pool, DisableRequest{
		SchemaDigest:   testSchemaDigest,
		Operations:     []string{"featureFlags"},
		DocumentDigest: map[string]string{"featureFlags": testDocumentDigest},
		NewMode:        "python",
		RecordedBy:     "lane-routing-verbs",
		ReviewEvidence: strings.Repeat("x", auditReviewEvidenceMax+1),
		Apply:          true,
	})
	if !errors.Is(err, ErrAuditRowRefused) {
		t.Fatalf("Disable = %v, want ErrAuditRowRefused", err)
	}
	if audits := readAuditRows(t, ctx, pool); len(audits) != 0 {
		t.Fatalf("a refused write left %d audit row(s)", len(audits))
	}
}
