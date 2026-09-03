// Package audit commits a security-state mutation, its outbox event and its
// audit row in ONE PostgreSQL transaction.
//
// WHAT THIS GUARANTEES, stated at the strength three review rounds measured
// rather than the strength it is tempting to claim:
//
//	It makes breaking atomicity BY ACCIDENT impossible. It DETECTS several
//	deliberate breaks. It CANNOT prevent a caller who reaches around it, and
//	no in-process API can.
//
// The last clause is not modesty. A caller in this process has ambient
// authority: the pool passed to Commit is in scope for any closure they write,
// and a second connection from it commits state the helper's transaction knows
// nothing about. Three rounds each closed a mechanism and found the next one --
// an interface type-asserted back to Commit, a status check that detected a
// self-commit it could not undo, an exec mode unreachable for parameterless
// queries, a raw connection handed out by a return value. Each was correct
// about the object it named and wrong about the property claimed. The pattern
// stopped being evidence about the implementations and became evidence about
// the approach.
//
// WHY THE THREE ARE INSEPARABLE. G-53 requires a state change and its outbox
// event to commit together, so an event can never describe a mutation that did
// not commit and a committed mutation can never be missing its event. The
// audit row joins them by ruling (team-lead, 2026-09-03): a security mutation
// without its audit row is the gap G-65 exists to close, and the audit's own
// retention is enforced by a separate reaper rather than by this write path.
// Delivery-time audit -- attempts, outcomes, failures -- belongs to the worker,
// not here.
//
// WHY A CALLBACK RATHER THAN AN EXPORTED TRANSACTION. The obvious API hands the
// caller a transaction and trusts them to write all three. That is the
// ordering-claim shape CHAOS-4917 removed from the migration package: true
// while everyone remembers, unenforced, and silently false the first time
// someone does not. Here the mutation runs INSIDE Commit, the outbox and audit
// inserts are unexported, and there is no exported path that writes one without
// the others. A caller cannot forget the outbox event because a caller cannot
// reach it.
//
// The bound this does NOT claim: within this package the inserts are callable
// directly, exactly as ValidatedIdentifier is constructible within its own.
// The guarantee is over the package boundary; inside it, review is the control.
package audit

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// ErrMutationFailed wraps any failure that leaves the database unchanged.
var ErrMutationFailed = errors.New("audit: security mutation failed")

// Outcome is the closed vocabulary security_audit_events.outcome permits. The
// column carries a CHECK constraint, so a value outside this set is refused by
// PostgreSQL as well as by this type -- the type is the early, legible half.
type Outcome string

const (
	OutcomeAllowed Outcome = "allowed"
	OutcomeDenied  Outcome = "denied"
	OutcomeError   Outcome = "error"
)

// Valid reports whether o is one the schema will accept.
func (o Outcome) Valid() bool {
	return o == OutcomeAllowed || o == OutcomeDenied || o == OutcomeError
}

// AuditEvent is one row of the append-only security trail.
//
// Attributes must carry no credential material. The lineage's naming guard
// does not inspect jsonb contents, so that rule is the writer's to keep and
// PR 2's allowlist is what will enforce it (G-65).
type AuditEvent struct {
	EventType          string
	Outcome            Outcome
	ActorPrincipalID   *string
	SubjectPrincipalID *string
	OrganizationID     *string
	PolicyRevision     *int64
	Attributes         map[string]any
	RequestID          *string
}

// OutboxEvent is the durable record a consumer will later receive.
//
// IdempotencyKey is what makes redelivery safe: a consumer that has already
// applied this key must recognise it without inspecting the payload. The
// column is uniquely indexed, so a duplicate key fails the whole transaction
// -- which is the correct outcome, because two mutations sharing a key would
// be indistinguishable to every consumer downstream.
type OutboxEvent struct {
	AggregateType  string
	AggregateID    string
	EventType      string
	Payload        map[string]any
	IdempotencyKey string
}

// TxOps is what a mutation may do inside the transaction.
//
// A STRUCT, not an interface, and that is the whole point. Round 1 replaced
// pgx.Tx with a narrow INTERFACE; round 2 walked through it in one line:
//
//	raw := tx.(interface{ Commit(context.Context) error })
//	raw.Commit(ctx)
//
// An interface hides methods from the METHOD SET while leaving the dynamic
// value intact, so a type assertion recovers everything the narrowing removed.
// Three of us checked the method set and asserted a property of the program --
// the same substitution the round before had just blocked, committed inside the
// fix for it.
//
// A concrete struct has no dynamic value to recover. `tx.(anything)` does not
// compile, because a type assertion requires an interface operand.
//
// It does NOT follow that the transaction is reachable only through these three
// methods, and this comment claimed that until round 3 disproved it: Query
// returned a pgx.Rows carrying a public Conn(), so the raw connection left
// through a RETURN TYPE while the receiver looked sealed. Query now returns a
// Rows of our own with no Conn at any depth. What remains reachable is
// everything the caller already holds -- see the package doc.
//
// TRANSACTION CONTROL IS REFUSED BEFORE IT IS SENT. Exec must exist for a
// mutation to write anything, so round 2 reached the server with
// Exec(ctx, "COMMIT"). The three methods now reject a statement whose first
// token is transaction control, so the state is never committed and
// "all three or none" stays true rather than becoming a boundary to explain.
//
// THE RESIDUES, stated so none is read as closed.
//
//   - Dynamic SQL the lexer cannot see -- a statement assembled at runtime
//     whose first token is computed -- reaches the server. Commit's TxStatus
//     check DETECTS a transaction ended that way before reporting success, but
//     state such a mutation already committed is durable and cannot be undone.
//   - Retention of the wrapper past the callback is possible and only
//     characterised, by TestRetainedTxOpsIsUnusableAfterCommit.
//   - Anything the caller already holds. The pool passed to Commit is in scope
//     for any closure they write; a second connection from it commits state
//     this transaction never sees, and TxStatus reads 'T' throughout. This is
//     the one no narrowing can reach.
//
// Multi-statement strings are NOT in this list: refuseMultipleStatements
// rejects them before the send. QueryExecModeExec is a second line that catches
// the parameterised case at the server, and it was wrongly credited here as the
// control until that claim was measured.

type TxOps struct{ tx pgx.Tx }

// EVERY forward carries pgx.QueryExecModeExec, and that is a second control
// rather than a performance choice.
//
// It is a SECOND line, not the control, and this comment said otherwise until
// the claim was measured. Forcing the extended protocol makes the server refuse
// a multi-statement string -- but ONLY when the query carries bind arguments.
// Measured: `SELECT 1; SELECT 2;` with no arguments is accepted, because pgx
// takes a parameterless path that never builds a prepared statement, so the
// mode is unreachable rather than ignored. The attack statement has no
// parameters. refuseMultipleStatements is what actually refuses it.
//
// It goes first in args because pgx reads exec-mode options from the leading
// arguments.
func (t TxOps) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	if err := refuseTransactionControl(sql); err != nil {
		return pgconn.CommandTag{}, err
	}
	return t.tx.Exec(ctx, sql, prependExecMode(args)...)
}

func (t TxOps) Query(ctx context.Context, sql string, args ...any) (Rows, error) {
	if err := refuseTransactionControl(sql); err != nil {
		return Rows{}, err
	}
	rows, err := t.tx.Query(ctx, sql, prependExecMode(args)...)
	if err != nil {
		return Rows{}, err
	}
	return Rows{rows}, nil
}

// Rows delegates pgx.Rows WITHOUT Conn().
//
// Round 3's fifth escape: pgx.Rows carries a public Conn() returning the raw
// *pgx.Conn, so a callback could Query, take rows.Conn(), and Exec a COMMIT on
// it -- past the first-token check, past the semicolon rule, and detected only
// by the TxStatus backstop after the state was already durable.
//
// Hiding Conn on TxOps was never enough, because the capability left through a
// RETURN TYPE rather than through the receiver. Explicit methods, no embedding:
// embedding pgx.Rows would promote Conn() and reopen the hole silently the next
// time pgx adds a method.
type Rows struct{ rows pgx.Rows }

// A zero Rows is what an errored Query returns. Its methods must not panic --
// a caller that ignores the error and iterates gets an empty result and an
// error from Err(), which is pgx's own convention.
func (r Rows) valid() bool { return r.rows != nil }

func (r Rows) Close() {
	if r.valid() {
		r.rows.Close()
	}
}
func (r Rows) Err() error {
	if !r.valid() {
		return ErrMutationFailed
	}
	return r.rows.Err()
}
func (r Rows) CommandTag() pgconn.CommandTag                { return r.rows.CommandTag() }
func (r Rows) FieldDescriptions() []pgconn.FieldDescription { return r.rows.FieldDescriptions() }
func (r Rows) Next() bool                                   { return r.valid() && r.rows.Next() }
func (r Rows) Scan(dest ...any) error                       { return r.rows.Scan(dest...) }
func (r Rows) Values() ([]any, error)                       { return r.rows.Values() }
func (r Rows) RawValues() [][]byte                          { return r.rows.RawValues() }

func prependExecMode(args []any) []any {
	return append([]any{pgx.QueryExecModeExec}, args...)
}

// QueryRow cannot return an error, so a refused statement is carried as a Row
// whose Scan fails. That is pgx's own convention for a query that never ran.
func (t TxOps) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	if err := refuseTransactionControl(sql); err != nil {
		return errRow{err}
	}
	return t.tx.QueryRow(ctx, sql, prependExecMode(args)...)
}

type errRow struct{ err error }

func (e errRow) Scan(...any) error { return e.err }

// Mutation is the state change and the two records that must accompany it.
//
// Apply runs inside the transaction and receives it. Anything it writes commits
// with the outbox event and the audit row, or none of them do.
type Mutation struct {
	Apply func(ctx context.Context, tx TxOps) error
	Audit AuditEvent
	Event OutboxEvent
}

func (m Mutation) validate() error {
	switch {
	case m.Apply == nil:
		return fmt.Errorf("%w: no state mutation", ErrMutationFailed)
	case m.Audit.EventType == "":
		return fmt.Errorf("%w: audit event_type is empty", ErrMutationFailed)
	case !m.Audit.Outcome.Valid():
		return fmt.Errorf("%w: audit outcome %q is outside the permitted set", ErrMutationFailed, m.Audit.Outcome)
	case m.Event.AggregateType == "" || m.Event.AggregateID == "":
		return fmt.Errorf("%w: outbox event names no aggregate", ErrMutationFailed)
	case m.Event.EventType == "":
		return fmt.Errorf("%w: outbox event_type is empty", ErrMutationFailed)
	case m.Event.IdempotencyKey == "":
		// An empty key is worse than a duplicate one: the unique index would
		// accept exactly one such row ever, and every later mutation would
		// fail for a reason that names the wrong cause.
		return fmt.Errorf("%w: outbox idempotency_key is empty", ErrMutationFailed)
	}
	return nil
}

// Commit applies the mutation, its outbox event and its audit row in one
// transaction.
//
// On any failure the transaction is rolled back and NOTHING is written -- not
// the state change, not the event, not the audit row. A partially applied
// security mutation is the failure this package exists to make impossible.
func Commit(ctx context.Context, pool *pgxpool.Pool, schema authschema.ValidatedIdentifier, m Mutation) error {
	if pool == nil {
		return fmt.Errorf("%w: no connection pool", ErrMutationFailed)
	}
	if err := m.validate(); err != nil {
		return err
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("%w: beginning the transaction", ErrMutationFailed)
	}
	// Rollback on every path that does not reach Commit. It is a no-op after a
	// successful commit, so this is correct rather than merely defensive.
	defer func() { _ = tx.Rollback(ctx) }()

	if err := m.Apply(ctx, TxOps{tx: tx}); err != nil {
		return fmt.Errorf("%w: applying the state mutation: %w", ErrMutationFailed, err)
	}
	// The callback could have ended the transaction with raw SQL -- Exec must
	// exist, so Exec(ctx, "COMMIT") and Exec(ctx, "ROLLBACK") reach the server
	// and no Go type prevents them. Round 2 showed the damage: after a raw
	// ROLLBACK the pgx wrapper stays logically open, so the outbox and audit
	// inserts below would run OUTSIDE the rolled-back transaction and Commit
	// could return success -- records for a mutation that never committed.
	//
	// So this asks the CONNECTION what state it is really in rather than
	// trusting that the callback left it alone. 'T' is "in a transaction
	// block"; anything else means the callback ended it and nothing after this
	// point would be atomic with the state it wrote.
	if status := tx.Conn().PgConn().TxStatus(); status != 'T' {
		return fmt.Errorf("%w: the mutation ended the transaction itself (status %q); "+
			"the outbox event and audit row could not be written atomically with it",
			ErrMutationFailed, string(status))
	}
	if err := insertOutboxEvent(ctx, tx, schema, m.Event); err != nil {
		return err
	}
	if err := insertAuditEvent(ctx, tx, schema, m.Audit); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("%w: committing", ErrMutationFailed)
	}
	return nil
}

func insertOutboxEvent(ctx context.Context, tx pgx.Tx, schema authschema.ValidatedIdentifier, e OutboxEvent) error {
	payload, err := json.Marshal(orEmpty(e.Payload))
	if err != nil {
		return fmt.Errorf("%w: encoding the outbox payload", ErrMutationFailed)
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO `+authschema.Quote(schema)+`.auth_outbox_events
		    (aggregate_type, aggregate_id, event_type, payload, idempotency_key)
		VALUES ($1, $2, $3, $4, $5)`,
		e.AggregateType, e.AggregateID, e.EventType, payload, e.IdempotencyKey)
	if err != nil {
		return fmt.Errorf("%w: inserting the outbox event: %w", ErrMutationFailed, err)
	}
	return nil
}

func insertAuditEvent(ctx context.Context, tx pgx.Tx, schema authschema.ValidatedIdentifier, a AuditEvent) error {
	attributes, err := json.Marshal(orEmpty(a.Attributes))
	if err != nil {
		return fmt.Errorf("%w: encoding the audit attributes", ErrMutationFailed)
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO `+authschema.Quote(schema)+`.security_audit_events
		    (event_type, outcome, actor_principal_id, subject_principal_id,
		     organization_id, policy_revision, attributes, request_id)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		a.EventType, string(a.Outcome), a.ActorPrincipalID, a.SubjectPrincipalID,
		a.OrganizationID, a.PolicyRevision, attributes, a.RequestID)
	if err != nil {
		return fmt.Errorf("%w: inserting the audit row: %w", ErrMutationFailed, err)
	}
	return nil
}

func orEmpty(m map[string]any) map[string]any {
	if m == nil {
		return map[string]any{}
	}
	return m
}
