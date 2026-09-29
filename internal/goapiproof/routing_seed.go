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

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// SeedMode is the only mode `seed` writes.
const SeedMode = "shadow"

// Seed actions, one per operation. would-create is a dry run, or a created
// row that was rolled back because another operation in the run was refused.
const (
	SeedActionCreated        = "created"
	SeedActionAlreadyPresent = "already-present"
	SeedActionRefused        = "refused"
	SeedActionWouldCreate    = "would-create"
)

// ErrSeedRefused reports that at least one operation was refused. The whole
// invocation is one transaction: after a refusal nothing was written for any
// operation, and every outcome names its own reason.
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
	// CorrelationID ties every row this run created to its audit rows.
	CorrelationID string
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

// RunningRow is one routing row at a schema digest.
type RunningRow struct{ Operation, DocumentDigest string }

// RunningDigestRows reads EVERY routing row at schemaDigest (an operation can
// hold several, one per document digest). `-all-unrouted` checks them all
// against the registry and the catalog before it writes anything.
func RunningDigestRows(ctx context.Context, pool *pgxpool.Pool, schemaDigest string) ([]RunningRow, error) {
	if pool == nil {
		return nil, fmt.Errorf("%w: nil pool", ErrSeedRequestRefused)
	}
	rows, err := pool.Query(ctx, `SELECT selected_operation, document_digest FROM public.go_api_routing_state WHERE schema_digest = $1 ORDER BY selected_operation, document_digest`, schemaDigest)
	if err != nil {
		return nil, fmt.Errorf("goapiproof: read routing rows at the running digest: %w", err)
	}
	defer rows.Close()
	var out []RunningRow
	for rows.Next() {
		var row RunningRow
		if err := rows.Scan(&row.Operation, &row.DocumentDigest); err != nil {
			return nil, fmt.Errorf("goapiproof: scan routing row: %w", err)
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

// AllUnroutedDisagreements is the whole-picture preflight of `seed
// -all-unrouted`: it does not look only at the operations about to be seeded.
// It returns, sorted, every disagreement among the three views of the same
// digests -- the running query-api's registry, the edge catalog, and the
// routing rows already at the running schema digest:
//   - a registered operation the catalog does not list, or lists under a
//     different document digest;
//   - a row at the running schema digest for an operation the registry does
//     not register, or under a document digest the registry does not report
//     (each row is checked; an operation with two rows is not collapsed).
//
// Any disagreement means the state is not the one seed was written for, so the
// caller refuses the whole run before any write.
func AllUnroutedDisagreements(registry, catalog map[string]string, rows []RunningRow) []string {
	var out []string
	for operation, registered := range registry {
		switch cataloged, ok := catalog[operation]; {
		case !ok:
			out = append(out, fmt.Sprintf("%s: registered but outside the catalog", operation))
		case cataloged != registered:
			out = append(out, fmt.Sprintf("%s: catalog=%s registry=%s", operation, cataloged, registered))
		}
	}
	for _, row := range rows {
		registered, ok := registry[row.Operation]
		switch {
		case !ok:
			out = append(out, fmt.Sprintf("%s: routing row at the running schema digest but the registry does not register it", row.Operation))
		case registered != row.DocumentDigest:
			out = append(out, fmt.Sprintf("%s: routing row document=%s registry=%s", row.Operation, row.DocumentDigest, registered))
		}
	}
	sort.Strings(out)
	return out
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

// SeedEvidencePrefix opens review_evidence on every row and audit entry seed
// writes, so a reader of either table can tell a seeded row from an enabled one
// (the audit action vocabulary admits only enable|disable|repoint).
const SeedEvidencePrefix = "seed: "

// Seed creates the first shadow row for each named operation in ONE
// transaction: if any operation is refused, nothing is written for any of them
// (a half-applied batch is a state an operator cannot describe). It returns a
// non-nil error wrapping ErrSeedRefused if any operation was refused; outcomes
// are always returned, and after a refusal none of them was written.
func Seed(ctx context.Context, pool *pgxpool.Pool, request SeedRequest) ([]SeedOutcome, error) {
	if pool == nil {
		return nil, fmt.Errorf("%w: nil pool", ErrSeedRequestRefused)
	}
	if err := request.validate(); err != nil {
		return nil, err
	}
	operations := append([]string(nil), request.Operations...)
	sort.Strings(operations)
	tx, err := pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("goapiproof: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	now := time.Now().UTC()
	evidence := SeedEvidencePrefix + request.ReviewEvidence
	outcomes := make([]SeedOutcome, 0, len(operations))
	var entries []RoutingAuditEntry
	refused := 0
	for _, operation := range operations {
		outcome, err := seedOne(ctx, tx, request, evidence, operation, now)
		if err != nil {
			return outcomes, err
		}
		switch outcome.Action {
		case SeedActionRefused:
			refused++
		case SeedActionCreated:
			entries = append(entries, RoutingAuditEntry{
				DocumentDigest:      outcome.DocumentDigest,
				Operation:           operation,
				CandidateBuildAfter: request.RunningBuild,
				ModeAfter:           SeedMode,
			})
		}
		outcomes = append(outcomes, outcome)
	}
	if refused > 0 {
		for i := range outcomes {
			if outcomes[i].Action == SeedActionCreated {
				outcomes[i].Action = SeedActionWouldCreate
				outcomes[i].Reason = "not written: another operation in this run was refused"
			}
		}
		return outcomes, fmt.Errorf("%w: %d of %d operation(s) refused; nothing was written", ErrSeedRefused, refused, len(outcomes))
	}
	if len(entries) == 0 || request.DryRun {
		return outcomes, nil
	}
	correlationID, err := writeRoutingAudit(ctx, tx, RoutingAudit{
		Action:          AuditActionEnable,
		CredentialClass: CredentialClassEnvelope,
		PrincipalID:     request.PrincipalID,
		RecordedBy:      request.RecordedBy,
		ReviewEvidence:  evidence,
		SchemaDigest:    request.SchemaDigest,
		Entries:         entries,
	}, now)
	if err != nil {
		return outcomes, err
	}
	if err := tx.Commit(ctx); err != nil {
		return outcomes, fmt.Errorf("goapiproof: commit: %w", err)
	}
	for i := range outcomes {
		if outcomes[i].Action == SeedActionCreated {
			outcomes[i].CorrelationID = correlationID
		}
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

func seedOne(ctx context.Context, tx pgx.Tx, request SeedRequest, evidence, operation string, now time.Time) (SeedOutcome, error) {
	documentDigest := request.DocumentDigest[operation]
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
	// Candidate build first: the routing row's 4-column foreign key makes it
	// mandatory, and it is the lock order every writer in this package uses.
	if _, err := tx.Exec(ctx, registerCandidateBuildSQL,
		request.SchemaDigest, documentDigest, operation, request.RunningBuild); err != nil {
		return SeedOutcome{}, fmt.Errorf("goapiproof: register candidate build for %s: %w", operation, err)
	}
	tag, err := tx.Exec(ctx, seedInsertRoutingStateSQL,
		request.SchemaDigest, documentDigest, operation, request.RunningBuild,
		evidence, request.RecordedBy, now)
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
	return SeedOutcome{Operation: operation, DocumentDigest: documentDigest, Action: SeedActionCreated}, nil
}
