// Package audit commits a security-state mutation, its outbox event and its
// audit row in ONE PostgreSQL transaction.
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
// Deliberately NOT pgx.Tx. The lifecycle methods are the ones that break "all
// three or none": a callback given the real transaction can Commit it, at which
// point the state is durable, the outbox insert fails on a closed transaction,
// and the deferred rollback is a no-op -- Commit returns an error with the
// mutation committed and no event and no audit row. Codex round 1 found that
// path; lane-auth-contracts had named the shape an hour earlier. pgx.Tx
// satisfies this interface, so the helper still passes the real transaction and
// the caller simply cannot reach Commit, Rollback or Conn.
//
// WHAT THIS DOES NOT CLOSE, stated because a boundary left implicit is the
// defect this package keeps finding in its own comments:
//
//   - Raw SQL. Exec is necessarily present, so `Exec(ctx, "COMMIT")` still
//     reaches the server. No Go type can prevent that; it is a caller writing
//     transaction control by hand, which is visible in review in a way that
//     calling a method is not.
//   - Retention. A callback can keep the value and use it after Commit
//     returns. A type cannot express "only during this call". The subtest
//     TestRetainedTxOpsIsUnusableAfterCommit pins what happens when it does.
type TxOps interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

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

	if err := m.Apply(ctx, tx); err != nil {
		return fmt.Errorf("%w: applying the state mutation: %w", ErrMutationFailed, err)
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
