//go:build integration

package goapiproof

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/mcpclass"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/routingauditschema"
)

// auditDDL now lives in internal/testsupport/routingauditschema,
// shared with cmd/go-api-routing's end-to-end verb tests, for the same
// reason registryDDL does (this file's alias above). The alias keeps
// this file's existing references and its one name for the thing.
const auditDDL = routingauditschema.DDL

// startAuditedRegistryPostgres is startRegistryPostgres plus the audit
// table, so a verb's routing write and its audit row meet the SAME
// constraints Postgres will apply in production.
func startAuditedRegistryPostgres(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool := startRegistryPostgres(t)
	if err := routingauditschema.Create(context.Background(), pool); err != nil {
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

// preMigrationSchema is the registry WITHOUT go_api_routing_audits: a binary ahead of its schema. The refusal must
// name the migration rather than surface a raw Postgres error an operator cannot act on.
func TestAVerbAgainstAPre0130SchemaNamesTheMissingMigration(t *testing.T) {
	ctx := t.Context()
	pool := startRegistryPostgres(t) // deliberately NOT the audited variant
	op := mcpclass.Operation("analytics")
	seedClassDecisionRow(t, pool, op, "canary", verbsRunningBuild)

	_, err := Disable(ctx, pool, DisableRequest{
		SchemaDigest:   testSchemaDigest,
		Operations:     []string{op},
		DocumentDigest: mcpclass.Digests(),
		NewMode:        "python",
		RecordedBy:     "lane-routing-verbs",
		ReviewEvidence: "CHAOS-5505 against an unmigrated schema",
		Apply:          true,
	})
	if !errors.Is(err, ErrAuditTableNotMigrated) {
		t.Fatalf("Disable = %v, want ErrAuditTableNotMigrated -- an operator reading a raw 42P01 cannot tell a missing migration from a code bug", err)
	}
	if !strings.Contains(err.Error(), "0130") {
		t.Fatalf("the refusal must name the migration to apply, got %q", err)
	}
	if mode := classRowMode(t, pool, op); mode != "canary" {
		t.Fatalf("mode = %q: the decision write must have rolled back with its audit row", mode)
	}
}

// review_evidence is bounded by the database, and the writer refuses BEFORE reaching it so the message says what to do.
func TestAnOverlongReviewEvidenceIsRefusedRatherThanTruncated(t *testing.T) {
	ctx := t.Context()
	pool := startAuditedRegistryPostgres(t)
	op := mcpclass.Operation("analytics")
	seedClassDecisionRow(t, pool, op, "canary", verbsRunningBuild)

	_, err := Disable(ctx, pool, DisableRequest{
		SchemaDigest:   testSchemaDigest,
		Operations:     []string{op},
		DocumentDigest: mcpclass.Digests(),
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
