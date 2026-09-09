package goapiproof

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// EnablementProofStage and EnablementProofTerminalState are the exact
// pair `dev-hops go-api routing enable`'s preflight selects on
// (go_api_routing_admin.build_enablement_proof_select: stage =
// 'deployed_executed' AND terminal_state = 'match'). A receipt written
// with any other pair is recorded evidence but authorizes nothing --
// which is precisely what a shadow operation's recorded divergence
// should do.
const (
	EnablementProofStage         = "deployed_executed"
	EnablementProofTerminalState = "match"
)

// Receipt is one immutable proof-run row.
//
// The four-column key (SchemaDigest, DocumentDigest, SelectedOperation,
// CandidateBuild) is the whole point: plan §8.3's rule is that a proof is
// evidence for exactly one such tuple and is "never carried forward
// across any of the four changing". CandidateBuild here is always the
// build identity READ FROM THE RUNNING PROCESS, never an operator-typed
// name -- see BuildIdentity.
type Receipt struct {
	SchemaDigest         string
	DocumentDigest       string
	SelectedOperation    string
	CandidateBuild       string
	RequestIdentity      string
	Stage                string
	TerminalState        string
	BaselineResponseRef  string
	CandidateResponseRef string
	DataWatermark        string
	OrgID                string
	ReviewEvidence       string
	RecordedBy           string
	ObservedAt           time.Time

	// MeasurementRoute is "edge" or "proof" -- which route executed the
	// candidate leg. A proof-route receipt must never read as served
	// traffic (team-lead ruling R50, 2026-09-09), and a receipt that did
	// not SAY which route produced it would leave that distinction in a
	// chat message instead of in the row.
	MeasurementRoute string

	// BaselineDefects names the tickets whose declared field paths cover
	// this comparison's differences. It NEVER softens TerminalState --
	// see BaselineDefect for why that separation is the whole point.
	BaselineDefects []string

	// DifferencesOutsideBaselineDefect is written even when zero, so
	// "every difference here is a known Python defect" is a claim a
	// reader can check rather than take on trust.
	DifferencesOutsideBaselineDefect int
}

// Querier is the subset of pgx this package needs, so a test can pass a
// pool, a connection, or a transaction.
type Querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Write records one receipt, registering its candidate build first.
//
// Two statements, in this order and inside the caller's transaction:
//
//  1. go_api_candidate_build INSERT ... ON CONFLICT DO NOTHING. That
//     table is immutable and append-only (never an UPDATE), and
//     go_api_proof_run carries a 4-column composite FK to it -- so a
//     receipt for a build that was never registered would be rejected by
//     the database, not silently dropped.
//  2. go_api_proof_run INSERT. Append-only by contract: a proof is a
//     historical fact, so a re-run adds a row, never overwrites one. Two
//     receipts for the same tuple with different verdicts is a legible
//     history ("it matched on Monday and mismatched on Tuesday"), which
//     an upsert would destroy.
//
// The stage/terminal_state vocabularies are enforced by CHECK
// constraints; this validates them first so a programming error at the
// call site fails with a specific message rather than a constraint name.
func Write(ctx context.Context, db Querier, receipt Receipt) (uuid.UUID, error) {
	if err := validateVocabulary(receipt); err != nil {
		return uuid.Nil, err
	}
	if receipt.CandidateBuild == "" {
		return uuid.Nil, fmt.Errorf("goapiproof: refusing to write a receipt with an empty candidate build")
	}
	if !measurementRoutes[receipt.MeasurementRoute] {
		// A receipt with no route cannot be told apart from served
		// traffic later, which is the entire reason the column exists.
		return uuid.Nil, fmt.Errorf("goapiproof: refusing to write a receipt with measurement route %q -- expected %q or %q", receipt.MeasurementRoute, RouteEdge, RouteProof)
	}

	if _, err := db.Exec(ctx,
		`INSERT INTO go_api_candidate_build
		   (schema_digest, document_digest, selected_operation, candidate_build, registered_at)
		 VALUES ($1, $2, $3, $4, $5)
		 ON CONFLICT (schema_digest, document_digest, selected_operation, candidate_build) DO NOTHING`,
		receipt.SchemaDigest, receipt.DocumentDigest, receipt.SelectedOperation,
		receipt.CandidateBuild, receipt.ObservedAt,
	); err != nil {
		return uuid.Nil, fmt.Errorf("goapiproof: register candidate build for %s: %w", receipt.SelectedOperation, err)
	}

	id := uuid.New()
	if _, err := db.Exec(ctx,
		`INSERT INTO go_api_proof_run
		   (id, schema_digest, document_digest, selected_operation, candidate_build,
		    request_identity, stage, terminal_state,
		    baseline_response_ref, candidate_response_ref,
		    data_watermark, org_id, review_evidence, recorded_by, observed_at,
		    measurement_route, baseline_defect, differences_outside_baseline_defect)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)`,
		id, receipt.SchemaDigest, receipt.DocumentDigest, receipt.SelectedOperation, receipt.CandidateBuild,
		receipt.RequestIdentity, receipt.Stage, receipt.TerminalState,
		nullIfEmpty(receipt.BaselineResponseRef), nullIfEmpty(receipt.CandidateResponseRef),
		nullIfEmpty(receipt.DataWatermark), nullIfEmpty(receipt.OrgID),
		nullIfEmpty(receipt.ReviewEvidence), nullIfEmpty(receipt.RecordedBy), receipt.ObservedAt,
		nullIfEmpty(receipt.MeasurementRoute), receipt.BaselineDefects,
		receipt.DifferencesOutsideBaselineDefect,
	); err != nil {
		return uuid.Nil, fmt.Errorf("goapiproof: record proof run for %s: %w", receipt.SelectedOperation, err)
	}
	return id, nil
}

// stages and terminalStates mirror
// src/dev_health_ops/models/go_api_registry.py's STAGES and
// TERMINAL_STATES exactly. The DB CHECK constraints are the backstop;
// this is the fast, specific failure for a programming error.
var stages = map[string]bool{
	"dual_run": true, "deployed_executed": true, "shadow": true, "canary": true,
}

var measurementRoutes = map[string]bool{RouteEdge: true, RouteProof: true}

var terminalStates = map[string]bool{
	"match": true, "mismatch": true, "auth_rejected": true, "validation_rejected": true,
	"dependency_failed": true, "timeout": true, "cancelled": true, "resource_exhausted": true,
	"fallback": true, "unsupported": true, "proof_failed": true,
}

func validateVocabulary(receipt Receipt) error {
	if !stages[receipt.Stage] {
		return fmt.Errorf("goapiproof: invalid proof-run stage %q", receipt.Stage)
	}
	if !terminalStates[receipt.TerminalState] {
		return fmt.Errorf("goapiproof: invalid proof-run terminal_state %q", receipt.TerminalState)
	}
	// The DB carries the same rule as a CHECK
	// (ck_go_api_proof_run_shadow_requires_watermark); stating it here
	// makes the reason legible at the call site.
	if receipt.Stage == "shadow" && receipt.DataWatermark == "" {
		return fmt.Errorf("goapiproof: a shadow-stage receipt requires a data watermark (same-watermark comparison is what the stage claims)")
	}
	return nil
}

func nullIfEmpty(value string) any {
	if value == "" {
		return nil
	}
	return value
}

// OperationsWithEnablementProof is the Go reader for the exact predicate
// `enable`'s preflight uses (go_api_routing_admin.build_enablement_proof_select).
//
// It exists so an integration test can assert the receipts this package
// WRITES are the ones that command READS -- proving the two halves meet,
// rather than each agreeing with itself. Scoped to the exact
// (schema_digest, candidate_build) tuple, and matching operation and
// document digest as a tuple-OR rather than two independent IN lists: the
// cross-product form would accept a proof recorded against a DIFFERENT
// registered document.
func OperationsWithEnablementProof(
	ctx context.Context,
	db Querier,
	schemaDigest string,
	candidateBuild string,
	documentDigestByOperation map[string]string,
) (map[string]bool, error) {
	found := map[string]bool{}
	if len(documentDigestByOperation) == 0 {
		return found, nil
	}

	operations := make([]string, 0, len(documentDigestByOperation))
	digests := make([]string, 0, len(documentDigestByOperation))
	for operation, digest := range documentDigestByOperation {
		operations = append(operations, operation)
		digests = append(digests, digest)
	}

	rows, err := db.Query(ctx,
		`SELECT DISTINCT p.selected_operation
		   FROM go_api_proof_run AS p
		   JOIN unnest($4::text[], $5::text[]) AS want(operation, document_digest)
		     ON want.operation = p.selected_operation
		    AND want.document_digest = p.document_digest
		  WHERE p.schema_digest = $1
		    AND p.candidate_build = $2
		    AND p.stage = $3
		    AND p.terminal_state = $6`,
		schemaDigest, candidateBuild, EnablementProofStage,
		operations, digests, EnablementProofTerminalState,
	)
	if err != nil {
		return nil, fmt.Errorf("goapiproof: read enablement proof: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var operation string
		if err := rows.Scan(&operation); err != nil {
			return nil, fmt.Errorf("goapiproof: scan enablement proof row: %w", err)
		}
		found[operation] = true
	}
	return found, rows.Err()
}
