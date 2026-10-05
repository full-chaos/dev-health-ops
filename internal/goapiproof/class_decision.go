package goapiproof

// The MCP class decision (CHAOS-8735, owner ruling D4797): whether `mcp:<root>` is served, held in shadow for
// the proof route, or dark is ONE row per operation in go_api_class_decision, never a row keyed to a schema
// digest. The class switch, the proof switch, `status`, `enable`, `disable`, `seed` and `repoint` all read and
// write this table, so a schema change cannot change what any of them sees. `decided_at` is set by the
// database when a verb changes the mode; `repoint` rewrites current_candidate_build and nothing else.
//
// What stays digest-keyed is the PROOF: a receipt is a fact about a build at a schema digest, and `enable`
// still requires one at the live digest before it writes a decision.

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/mcpclass"
)

// classDecisionUpsertSQL writes a decision. decided_at is now() of the database transaction, never the
// application clock.
const classDecisionUpsertSQL = `
INSERT INTO public.go_api_class_decision
	(operation, mode, current_candidate_build, schema_digest, review_evidence, recorded_by, decided_at)
VALUES ($1, $2, $3, $4, $5, $6, now())
ON CONFLICT (operation) DO UPDATE
   SET mode = EXCLUDED.mode,
       current_candidate_build = EXCLUDED.current_candidate_build,
       schema_digest = EXCLUDED.schema_digest,
       review_evidence = EXCLUDED.review_evidence,
       recorded_by = EXCLUDED.recorded_by,
       decided_at = now()`

// classDecisionLockSQL reads a decision under the lock the write below takes.
const classDecisionLockSQL = `
SELECT mode, current_candidate_build
  FROM public.go_api_class_decision
 WHERE operation = $1
   FOR UPDATE`

// classDecisionDisableSQL changes the mode and the provenance of the write, nothing else.
const classDecisionDisableSQL = `
UPDATE public.go_api_class_decision
   SET mode = $2,
       review_evidence = $3,
       recorded_by = $5,
       schema_digest = $6,
       decided_at = now()
 WHERE operation = $1
   AND ($4::text IS NULL OR current_candidate_build = $4)`

// classDecisionRepointSQL rewrites the build the decision names. It never touches mode or decided_at: a
// provenance write is not a decision.
const classDecisionRepointSQL = `
UPDATE public.go_api_class_decision
   SET current_candidate_build = $2,
       review_evidence = $3,
       recorded_by = $4
 WHERE operation = $1`

// ClassDecision is one operation's decision.
type ClassDecision struct {
	Operation string
	Mode      string
	Build     string
	DecidedAt time.Time
}

// ReadClassDecisions returns every class decision, by operation.
func ReadClassDecisions(ctx context.Context, db Querier) (map[string]ClassDecision, error) {
	rows, err := db.Query(ctx, `
		SELECT operation, mode, current_candidate_build, decided_at
		  FROM public.go_api_class_decision`)
	if err != nil {
		return nil, fmt.Errorf("goapiproof: read MCP class decisions: %w", err)
	}
	defer rows.Close()
	out := map[string]ClassDecision{}
	for rows.Next() {
		var decision ClassDecision
		if err := rows.Scan(&decision.Operation, &decision.Mode, &decision.Build, &decision.DecidedAt); err != nil {
			return nil, fmt.Errorf("goapiproof: scan an MCP class decision: %w", err)
		}
		out[decision.Operation] = decision
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("goapiproof: read MCP class decisions: %w", err)
	}
	return out, nil
}

// splitClassOperations separates class operations from document operations, each sorted.
func splitClassOperations(operations []string) (class, document []string) {
	for _, operation := range operations {
		if mcpclass.IsOperation(operation) {
			class = append(class, operation)
		} else {
			document = append(document, operation)
		}
	}
	sort.Strings(class)
	sort.Strings(document)
	return class, document
}

var errMixedClassAndDocument = errors.New("goapiproof: a request names MCP class operations and document operations together: they are decided by different tables, so run them separately")

// disableClassDecisions plans and (with Apply) writes the mode changes of class decisions, in ONE transaction.
func disableClassDecisions(ctx context.Context, pool *pgxpool.Pool, request DisableRequest) ([]DisableChange, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("goapiproof: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	operations, _ := splitClassOperations(request.Operations)
	changes := make([]DisableChange, 0, len(operations))
	var guardProblems []string
	for _, operation := range operations {
		change := DisableChange{Operation: operation, DocumentDigest: request.DocumentDigest[operation], NewMode: request.NewMode}
		var mode, build string
		switch err := tx.QueryRow(ctx, classDecisionLockSQL, operation).Scan(&mode, &build); {
		case err == nil:
			if request.ExpectedCandidateBuild != "" && build != request.ExpectedCandidateBuild {
				guardProblems = append(guardProblems, fmt.Sprintf(
					"%s points at %s, not the %s you named -- somebody has repointed it since you looked; re-run `status` and decide again",
					operation, build, request.ExpectedCandidateBuild))
				continue
			}
			change.CurrentMode, change.CandidateBuild = mode, build
		case errors.Is(err, pgx.ErrNoRows):
		default:
			return nil, fmt.Errorf("goapiproof: read the decision of %s: %w", operation, err)
		}
		changes = append(changes, change)
	}
	if len(guardProblems) > 0 {
		return nil, fmt.Errorf("%w: %v", ErrDisableGuardMismatch, guardProblems)
	}
	if !request.Apply {
		return changes, nil
	}

	now := time.Now().UTC()
	audit := RoutingAudit{
		Action:          AuditActionDisable,
		CredentialClass: CredentialClassOperatorDirect,
		RecordedBy:      request.RecordedBy,
		ReviewEvidence:  request.ReviewEvidence,
		SchemaDigest:    request.SchemaDigest,
	}
	for index := range changes {
		change := &changes[index]
		if change.CurrentMode == "" {
			continue
		}
		var guard any
		if request.ExpectedCandidateBuild != "" {
			guard = request.ExpectedCandidateBuild
		}
		if _, err := tx.Exec(ctx, classDecisionDisableSQL, change.Operation, request.NewMode, request.ReviewEvidence, guard, request.RecordedBy, request.SchemaDigest); err != nil {
			return nil, fmt.Errorf("goapiproof: disable %s: %w", change.Operation, err)
		}
		change.Applied = true
		modeBefore, buildBefore := change.CurrentMode, change.CandidateBuild
		audit.Entries = append(audit.Entries, RoutingAuditEntry{
			DocumentDigest:       change.DocumentDigest,
			Operation:            change.Operation,
			CandidateBuildBefore: &buildBefore,
			CandidateBuildAfter:  buildBefore,
			ModeBefore:           &modeBefore,
			ModeAfter:            change.NewMode,
		})
	}
	if len(audit.Entries) > 0 {
		if _, err := writeRoutingAudit(ctx, tx, audit, now); err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("goapiproof: commit: %w", err)
	}
	return changes, nil
}

// seedClassDecision creates the shadow decision of one class operation when it has none. An existing decision is
// never changed: a shadow one is already present, any other mode is a refusal.
func seedClassDecision(ctx context.Context, tx pgx.Tx, request SeedRequest, evidence, operation string) (SeedOutcome, error) {
	outcome := SeedOutcome{Operation: operation, DocumentDigest: request.DocumentDigest[operation]}
	var mode string
	switch err := tx.QueryRow(ctx, `SELECT mode FROM public.go_api_class_decision WHERE operation = $1 FOR UPDATE`, operation).Scan(&mode); {
	case err == nil && mode == SeedMode:
		outcome.Action = SeedActionAlreadyPresent
		outcome.Reason = "shadow decision already present"
		return outcome, nil
	case err == nil:
		outcome.Action = SeedActionRefused
		outcome.Reason = fmt.Sprintf("a decision exists in mode %s; seed writes a shadow decision only and never changes one", mode)
		return outcome, nil
	case !errors.Is(err, pgx.ErrNoRows):
		return SeedOutcome{}, fmt.Errorf("goapiproof: read the decision of %s: %w", operation, err)
	}
	if request.DryRun {
		outcome.Action = SeedActionWouldCreate
		return outcome, nil
	}
	tag, err := tx.Exec(ctx, `
INSERT INTO public.go_api_class_decision
	(operation, mode, current_candidate_build, schema_digest, review_evidence, recorded_by, decided_at)
VALUES ($1, $2, $3, $4, $5, $6, now())
ON CONFLICT (operation) DO NOTHING`,
		operation, SeedMode, request.RunningBuild, request.SchemaDigest, evidence, request.RecordedBy)
	if err != nil {
		return SeedOutcome{}, fmt.Errorf("goapiproof: seed %s: %w", operation, err)
	}
	if tag.RowsAffected() != 1 {
		return SeedOutcome{}, fmt.Errorf("goapiproof: seed %s inserted 0 rows although no decision was readable", operation)
	}
	outcome.Action = SeedActionCreated
	return outcome, nil
}

// repointClassDecisions points every (or each named) class decision at the running build, provenance only.
func repointClassDecisions(ctx context.Context, pool *pgxpool.Pool, request RepointRequest, named []string) ([]RepointOutcome, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("goapiproof: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	rows, err := tx.Query(ctx, `
		SELECT operation, mode, current_candidate_build
		  FROM public.go_api_class_decision
		 ORDER BY operation
		   FOR UPDATE`)
	if err != nil {
		return nil, fmt.Errorf("goapiproof: read the MCP class decisions: %w", err)
	}
	wanted := map[string]bool{}
	for _, operation := range named {
		wanted[operation] = true
	}
	var outcomes []RepointOutcome
	found := map[string]bool{}
	for rows.Next() {
		var operation, mode, build string
		if err := rows.Scan(&operation, &mode, &build); err != nil {
			rows.Close()
			return nil, fmt.Errorf("goapiproof: scan an MCP class decision: %w", err)
		}
		if len(wanted) > 0 && !wanted[operation] {
			continue
		}
		found[operation] = true
		outcomes = append(outcomes, RepointOutcome{
			Operation: operation, DocumentDigest: mcpclass.DocumentDigest(),
			ModeBefore: mode, ModeAfter: mode, BuildFrom: build, BuildTo: request.RunningBuild, Changed: build != request.RunningBuild,
		})
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("goapiproof: read the MCP class decisions: %w", err)
	}
	var missing []string
	for operation := range wanted {
		if !found[operation] {
			missing = append(missing, operation)
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		return nil, fmt.Errorf("%w: %s have no class decision", ErrRepointUnknownOperation, strings.Join(missing, ", "))
	}
	if request.DryRun {
		return outcomes, nil
	}
	now := time.Now().UTC()
	audit := RoutingAudit{
		Action:          AuditActionRepoint,
		CredentialClass: CredentialClassEnvelope,
		PrincipalID:     request.PrincipalID,
		RecordedBy:      request.RecordedBy,
		ReviewEvidence:  request.ReviewEvidence,
		SchemaDigest:    request.SchemaDigest,
	}
	for _, outcome := range outcomes {
		if !outcome.Changed {
			continue
		}
		if _, err := tx.Exec(ctx, classDecisionRepointSQL, outcome.Operation, request.RunningBuild, request.ReviewEvidence, request.RecordedBy); err != nil {
			return nil, fmt.Errorf("goapiproof: repoint %s: %w", outcome.Operation, err)
		}
		before, mode := outcome.BuildFrom, outcome.ModeBefore
		audit.Entries = append(audit.Entries, RoutingAuditEntry{
			DocumentDigest: outcome.DocumentDigest, Operation: outcome.Operation,
			CandidateBuildBefore: &before, CandidateBuildAfter: request.RunningBuild, ModeBefore: &mode, ModeAfter: mode,
		})
	}
	if len(audit.Entries) > 0 {
		if _, err := writeRoutingAudit(ctx, tx, audit, now); err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("goapiproof: commit: %w", err)
	}
	return outcomes, nil
}
