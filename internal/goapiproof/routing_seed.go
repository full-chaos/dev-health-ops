package goapiproof

// `seed`: the first routing row for an operation that has none, in the one
// mode that cannot route a client anywhere -- shadow.
//
// WHY THIS EXISTS (CHAOS-7165). `enable` upserts, so it can create a row --
// but it demands a proof receipt or a compiled named limit, and a read
// operation cannot be proven on production before it has a row (the proof
// route does not mount on production, and the edge dispatches only routed
// rows). `disable -mode shadow` only UPDATEs, so on an operation with no row
// it writes nothing and exits clean. Until this verb the only way to create
// the first row was hand-typed SQL against production.
//
// WHAT IT WILL NEVER DO. It never changes an existing row, never writes a
// mode other than shadow, and never moves a row between schema digests
// (that is `carry`). A shadow row does not make an operation reachable: the
// gates on shadow->canary (`enable`'s receipt / named limit) are untouched.

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// SeedMode is the only mode `seed` writes.
const SeedMode = "shadow"

// Seed actions, one per operation.
const (
	SeedActionCreated        = "created"
	SeedActionAlreadyPresent = "already-present"
	SeedActionRefused        = "refused"
	SeedActionWouldCreate    = "would-create"
)

// ErrSeedRefused reports that at least one operation was refused. Rows for
// operations that were not refused are still committed (one transaction per
// operation); every outcome names its own reason.
var ErrSeedRefused = errors.New("goapiproof: seed refused")

// ErrSeedRequestRefused reports a request that is malformed before any row
// is read.
var ErrSeedRequestRefused = errors.New("goapiproof: seed request refused")

// SeedRequest is what the verb derived from the running system. Nothing in
// it is typed by the operator except the operation names, provenance, and
// the dry-run switch.
type SeedRequest struct {
	SchemaDigest   string
	RunningBuild   string
	Operations     []string
	DocumentDigest map[string]string
	RecordedBy     string
	ReviewEvidence string
	PrincipalID    string
	DryRun         bool
}

// SeedOutcome is one operation's result.
type SeedOutcome struct {
	Operation      string
	DocumentDigest string
	Action         string
	// Reason is set on a refusal, and on already-present (which mode/digest
	// was found).
	Reason string
}

const seedLockedRowsSQL = `
SELECT schema_digest, document_digest, mode
  FROM public.go_api_routing_state
 WHERE selected_operation = $1
   FOR UPDATE`

const seedInsertRoutingStateSQL = `
INSERT INTO public.go_api_routing_state
	(schema_digest, document_digest, selected_operation, current_candidate_build,
	 owner, mode, rollout_percentage, review_evidence, recorded_by, updated_at)
VALUES ($1, $2, $3, $4, 'go', 'shadow', 0, $5, $6, $7)
ON CONFLICT (schema_digest, document_digest, selected_operation) DO NOTHING`

// UnroutedOperations returns, sorted, every registered operation that has no
// row at ANY schema digest. rowOperations is the set of operations that do.
func UnroutedOperations(registered map[string]string, rowOperations map[string]bool) []string {
	var out []string
	for operation := range registered {
		if !rowOperations[operation] {
			out = append(out, operation)
		}
	}
	sort.Strings(out)
	return out
}

// RoutedOperations reads every selected_operation that has a routing row at
// any schema digest.
func RoutedOperations(ctx context.Context, pool *pgxpool.Pool) (map[string]bool, error) {
	if pool == nil {
		return nil, fmt.Errorf("%w: nil pool", ErrSeedRequestRefused)
	}
	rows, err := pool.Query(ctx, `SELECT DISTINCT selected_operation FROM public.go_api_routing_state`)
	if err != nil {
		return nil, fmt.Errorf("goapiproof: read routed operations: %w", err)
	}
	defer rows.Close()
	out := map[string]bool{}
	for rows.Next() {
		var operation string
		if err := rows.Scan(&operation); err != nil {
			return nil, fmt.Errorf("goapiproof: scan routed operation: %w", err)
		}
		out[operation] = true
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("goapiproof: read routed operations: %w", err)
	}
	return out, nil
}

func (r SeedRequest) validate() error {
	switch {
	case r.SchemaDigest == "" || r.RunningBuild == "":
		return fmt.Errorf("%w: schema digest and running build are required", ErrSeedRequestRefused)
	case len(r.Operations) == 0:
		return fmt.Errorf("%w: no operations to seed", ErrSeedRequestRefused)
	case r.RecordedBy == "" || r.ReviewEvidence == "":
		return fmt.Errorf("%w: recorded-by and review-evidence are required", ErrSeedRequestRefused)
	case r.PrincipalID == "":
		return fmt.Errorf("%w: the envelope subject is required -- every created row is audited", ErrSeedRequestRefused)
	}
	for _, operation := range r.Operations {
		if r.DocumentDigest[operation] == "" {
			return fmt.Errorf("%w: %s has no document digest", ErrSeedRequestRefused, operation)
		}
	}
	return nil
}

// Seed creates the first shadow row for each named operation, one
// transaction per operation. It returns a non-nil error wrapping
// ErrSeedRefused if any operation was refused; outcomes are always
// returned.
func Seed(ctx context.Context, pool *pgxpool.Pool, request SeedRequest) ([]SeedOutcome, error) {
	if pool == nil {
		return nil, fmt.Errorf("%w: nil pool", ErrSeedRequestRefused)
	}
	if err := request.validate(); err != nil {
		return nil, err
	}
	operations := append([]string(nil), request.Operations...)
	sort.Strings(operations)
	outcomes := make([]SeedOutcome, 0, len(operations))
	refused := 0
	for _, operation := range operations {
		outcome, err := seedOne(ctx, pool, request, operation)
		if err != nil {
			return outcomes, err
		}
		if outcome.Action == SeedActionRefused {
			refused++
		}
		outcomes = append(outcomes, outcome)
	}
	if refused > 0 {
		return outcomes, fmt.Errorf("%w: %d of %d operation(s) refused", ErrSeedRefused, refused, len(outcomes))
	}
	return outcomes, nil
}

type seedStateRow struct{ schemaDigest, documentDigest, mode string }

func classifySeed(rows []seedStateRow, request SeedRequest, operation, documentDigest string) (SeedOutcome, bool) {
	outcome := SeedOutcome{Operation: operation, DocumentDigest: documentDigest}
	var atRunning *seedStateRow
	var others []string
	for i := range rows {
		if rows[i].schemaDigest == request.SchemaDigest {
			atRunning = &rows[i]
			continue
		}
		others = append(others, rows[i].schemaDigest)
	}
	switch {
	case atRunning != nil && atRunning.documentDigest != documentDigest:
		outcome.Action = SeedActionRefused
		outcome.Reason = fmt.Sprintf("a row exists at the running schema digest under a DIFFERENT document digest (%s, want %s); seed never touches an existing row",
			atRunning.documentDigest, documentDigest)
	case atRunning != nil && atRunning.mode != SeedMode:
		outcome.Action = SeedActionRefused
		outcome.Reason = fmt.Sprintf("a row exists at the running schema digest in mode %s; seed writes shadow rows only and never changes a row", atRunning.mode)
	case atRunning != nil:
		outcome.Action = SeedActionAlreadyPresent
		outcome.Reason = "shadow row already at the running schema digest"
	case len(others) > 0:
		outcome.Action = SeedActionRefused
		outcome.Reason = fmt.Sprintf("a row exists only at an older schema digest (%v); run routing carry instead", others)
	default:
		return outcome, false
	}
	return outcome, true
}

func seedOne(ctx context.Context, pool *pgxpool.Pool, request SeedRequest, operation string) (SeedOutcome, error) {
	documentDigest := request.DocumentDigest[operation]
	tx, err := pool.Begin(ctx)
	if err != nil {
		return SeedOutcome{}, fmt.Errorf("goapiproof: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	read := func() ([]seedStateRow, error) {
		rs, err := tx.Query(ctx, seedLockedRowsSQL, operation)
		if err != nil {
			return nil, fmt.Errorf("goapiproof: read rows for %s: %w", operation, err)
		}
		defer rs.Close()
		var rows []seedStateRow
		for rs.Next() {
			var r seedStateRow
			if err := rs.Scan(&r.schemaDigest, &r.documentDigest, &r.mode); err != nil {
				return nil, fmt.Errorf("goapiproof: scan row for %s: %w", operation, err)
			}
			rows = append(rows, r)
		}
		return rows, rs.Err()
	}

	rows, err := read()
	if err != nil {
		return SeedOutcome{}, err
	}
	if outcome, decided := classifySeed(rows, request, operation, documentDigest); decided {
		return outcome, nil
	}
	if request.DryRun {
		return SeedOutcome{Operation: operation, DocumentDigest: documentDigest, Action: SeedActionWouldCreate}, nil
	}

	now := time.Now().UTC()
	// Candidate build first: the routing row's 4-column foreign key makes it
	// mandatory, and it is the lock order every writer in this package uses.
	if _, err := tx.Exec(ctx, registerCandidateBuildSQL,
		request.SchemaDigest, documentDigest, operation, request.RunningBuild); err != nil {
		return SeedOutcome{}, fmt.Errorf("goapiproof: register candidate build for %s: %w", operation, err)
	}
	tag, err := tx.Exec(ctx, seedInsertRoutingStateSQL,
		request.SchemaDigest, documentDigest, operation, request.RunningBuild,
		request.ReviewEvidence, request.RecordedBy, now)
	if err != nil {
		return SeedOutcome{}, fmt.Errorf("goapiproof: seed %s: %w", operation, err)
	}
	if tag.RowsAffected() != 1 {
		// A concurrent seed committed the key between our read and our
		// insert. Read what is actually there and answer on that.
		rows, err := read()
		if err != nil {
			return SeedOutcome{}, err
		}
		if outcome, decided := classifySeed(rows, request, operation, documentDigest); decided {
			return outcome, nil
		}
		return SeedOutcome{}, fmt.Errorf("goapiproof: seed %s inserted 0 rows and no row is readable", operation)
	}
	{
		audit := RoutingAudit{
			Action:          AuditActionEnable,
			CredentialClass: CredentialClassEnvelope,
			PrincipalID:     request.PrincipalID,
			RecordedBy:      request.RecordedBy,
			ReviewEvidence:  request.ReviewEvidence,
			SchemaDigest:    request.SchemaDigest,
			Entries: []RoutingAuditEntry{{
				DocumentDigest:      documentDigest,
				Operation:           operation,
				CandidateBuildAfter: request.RunningBuild,
				ModeAfter:           SeedMode,
			}},
		}
		if _, err := writeRoutingAudit(ctx, tx, audit, now); err != nil {
			return SeedOutcome{}, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return SeedOutcome{}, fmt.Errorf("goapiproof: commit: %w", err)
	}
	return SeedOutcome{Operation: operation, DocumentDigest: documentDigest, Action: SeedActionCreated}, nil
}
