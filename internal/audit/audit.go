// Package audit commits a security-state mutation, its outbox events and its
// audit row in ONE PostgreSQL transaction.
//
// WHAT THIS GUARANTEES, stated at the strength three review rounds measured
// rather than the strength it is tempting to claim:
//
//	Atomicity for the guarded tables is enforced by PostgreSQL (migration
//	0006), not by this package's callers. For an unguarded table it is a
//	convention the caller keeps.
//
// The pre-0006 history below is why the control moved into the database. A
// caller in this process has ambient authority: the pool passed to Commit is in scope for any closure they write,
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
// WHERE THE GUARANTEE IS HELD. Migration 0006 puts a BEFORE INSERT/UPDATE/
// DELETE trigger on the audit and outbox tables and on the security-state
// tables (authschema's commit_guard_integration_test.go lists and classifies
// every table). Commit sets a transaction-local setting to the current
// transaction id; the trigger refuses a write made under any other id. So a
// second connection from the pool, SQL assembled at run time, or a write
// from another package is refused by PostgreSQL. The Go-side checks (TxOps,
// first-token refusal, the TxStatus check) remain as early, legible refusals;
// the source scan in writer_guard_test.go is a lint, not a control.
//
// WHAT STAYS OUTSIDE IT. The tables classified unguarded (sessions, refresh
// credentials, users, principals and the read-only vocabulary). A callback that
// forges the setting, or ends the transaction with SQL the lexer cannot read
// (Commit's TxStatus check then reports it but the state is already durable),
// is deliberate misuse by code already holding the runtime role's write privilege (the bound is exactly that: authschema's TestOnlyTheRuntimeAndMigrationRolesCanWriteGuardedTables asserts no other role, PUBLIC included, can write a guarded table), inside the transaction Commit owns: reviewable code, not
// an accident, and no database trigger can tell it from the caller's own SQL.
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
	"log/slog"
	"sort"
	"strconv"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/auth/clientaddr"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts/provisioning"
	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// commitGuardSetting is the transaction-local setting migration 0006's trigger
// compares with pg_current_xact_id(). Only this package may name it.
const commitGuardSetting = "dev_health.audit_txid"

// changedSetting is the transaction-local counter, stored as "<txid>:<n>", the
// guard trigger advances for every row written to a guarded state table inside
// Commit's transaction; a value tagged with another transaction id reads as 0.
const changedSetting = "dev_health.audit_changed"

// ErrAttributeNotAllowed reports an audit attribute outside the G-65 allowlist.
// It wraps ErrMutationFailed: the mutation is refused before anything is
// written.
var ErrAttributeNotAllowed = fmt.Errorf("%w: audit attributes", ErrMutationFailed)

// ErrMutationFailed wraps any failure that leaves the database unchanged.
var ErrMutationFailed = errors.New("audit: security mutation failed")

// ErrNoStateChange is returned, wrapping ErrMutationFailed, when Apply reports
// that it changed nothing. An outbox event and audit row for a mutation that
// did not happen would tell every consumer something false.
var ErrNoStateChange = fmt.Errorf("%w: the mutation changed no state", ErrMutationFailed)

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
// PR 2's allowlist is what will enforce it (G-65). The row's client_ip is the
// client address bound to the context (internal/auth/clientaddr), or NULL.
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

func (a AuditEvent) validate() error {
	switch {
	case a.EventType == "":
		return fmt.Errorf("%w: audit event_type is empty", ErrMutationFailed)
	case !a.Outcome.Valid():
		return fmt.Errorf("%w: audit outcome %q is outside the permitted set", ErrMutationFailed, a.Outcome)
	case ValidateAttributes(a.Attributes) != nil:
		return ValidateAttributes(a.Attributes)
	case validateEventType(a.EventType) != nil:
		return validateEventType(a.EventType)
	case a.RequestID != nil && validateRequestID(*a.RequestID) != nil:
		return validateRequestID(*a.RequestID)
	}
	return nil
}

// OutboxEvent is one provisioning-event.v1 event (the outbox row is the
// envelope). The caller names the aggregate by its wire id
// (provisioning.WireID) and gives the verb and the allowlisted change members;
// Commit owns the rest: the per-aggregate revision, the idempotency key
// aggregate_type:aggregate_id:revision, and the payload envelope. A caller
// cannot set a revision or a key, so it cannot write a false one.
type OutboxEvent struct {
	AggregateType string
	AggregateID   string
	EventType     string
	Change        map[string]any
}

// OffStreamSessionLifecycle marks a mutation that is deliberately not a
// provisioning event: session and refresh-credential lifecycle (the
// provisioning stream has no session aggregate).
const OffStreamSessionLifecycle = "session_lifecycle"

// OffStreamLinkCredentialLifecycle marks the consume or supersede of a
// single-use link credential: the credential's own lifecycle is not a
// provisioning event. The account change it authorizes (a verified e-mail, a
// new password) is a provisioning event written by the identity store.
const OffStreamLinkCredentialLifecycle = "link_credential_lifecycle"

var offStreamReasons = map[string]bool{OffStreamSessionLifecycle: true, OffStreamLinkCredentialLifecycle: true}

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

// A zero Rows is what an errored Query returns, and NONE of its methods may
// panic.
//
// The first version guarded three of eight and the comment claimed all of them.
// Close, Err and Next were safe, so the iterate path described in that comment
// really was fine -- but CommandTag, FieldDescriptions, Scan, Values and
// RawValues dereferenced a nil pgx.Rows, and calling CommandTag after a Query
// without iterating is an ordinary thing to do. A claim about all methods
// backed by a check on three: found by lane-auth-contracts.
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
func (r Rows) CommandTag() pgconn.CommandTag {
	if !r.valid() {
		return pgconn.CommandTag{}
	}
	return r.rows.CommandTag()
}
func (r Rows) FieldDescriptions() []pgconn.FieldDescription {
	if !r.valid() {
		return nil
	}
	return r.rows.FieldDescriptions()
}
func (r Rows) Next() bool { return r.valid() && r.rows.Next() }
func (r Rows) Scan(dest ...any) error {
	if !r.valid() {
		return ErrMutationFailed
	}
	return r.rows.Scan(dest...)
}
func (r Rows) Values() ([]any, error) {
	if !r.valid() {
		return nil, ErrMutationFailed
	}
	return r.rows.Values()
}
func (r Rows) RawValues() [][]byte {
	if !r.valid() {
		return nil
	}
	return r.rows.RawValues()
}

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

// Mutation is the state change and the records that must accompany it.
//
// Apply runs inside the transaction and receives it. Anything it writes commits
// with the outbox events and the audit row, or none of them do. It returns the
// error. The number of state rows it changed is counted by the database, not
// reported by the callback: the guard trigger of migration 0006 counts every
// row written to a guarded table other than the audit and outbox tables, and
// migration 0013's create guard counts every principal, user or (0014)
// session row a mutation inserts. Zero rolls everything back with ErrNoStateChange.
//
// Events lists the provisioning events of the mutation, one or more (one
// mutation may create a principal, its user and its external identity).
// OffStream, set instead of Events, names why the mutation has none. Exactly
// one of the two is set: a mutation cannot forget its events by leaving a
// slice empty.
type Mutation struct {
	Apply     func(ctx context.Context, tx TxOps) error
	Audit     AuditEvent
	Events    []OutboxEvent
	OffStream string
}

func (m Mutation) validate() error {
	if m.Apply == nil {
		return fmt.Errorf("%w: no state mutation", ErrMutationFailed)
	}
	if err := m.Audit.validate(); err != nil {
		return err
	}
	if appendOnlyEventTypes[m.Audit.EventType] {
		return fmt.Errorf("%w: audit event_type %q is append-only and has no state mutation", ErrMutationFailed, m.Audit.EventType)
	}
	switch {
	case len(m.Events) > 0 && m.OffStream != "":
		return fmt.Errorf("%w: a mutation has outbox events or is off the stream, not both", ErrMutationFailed)
	case len(m.Events) == 0 && m.OffStream == "":
		return fmt.Errorf("%w: a mutation names its outbox events or why it has none", ErrMutationFailed)
	case m.OffStream != "" && !offStreamReasons[m.OffStream]:
		return fmt.Errorf("%w: off-stream reason %q is outside the permitted set", ErrMutationFailed, m.OffStream)
	}
	for _, e := range m.Events {
		if err := provisioning.CheckEvent(e.AggregateType, e.AggregateID, e.EventType, e.Change); err != nil {
			return fmt.Errorf("%w: %w", ErrMutationFailed, err)
		}
	}
	for _, ref := range []struct {
		aggregate string
		id        *string
	}{{"organization", m.Audit.OrganizationID}, {"principal", m.Audit.ActorPrincipalID}} {
		if len(m.Events) > 0 && ref.id != nil && provisioning.WireID(ref.aggregate, *ref.id) == "" {
			return fmt.Errorf("%w: audit %s id cannot be carried on the wire", ErrMutationFailed, ref.aggregate)
		}
	}
	return nil
}

// Commit applies the mutation, its outbox events and its audit row in one
// transaction.
//
// On any failure the transaction is rolled back and NOTHING is written -- not
// the state change, not the event, not the audit row. A partially applied
// security mutation is the failure this package exists to make impossible.
func Commit(ctx context.Context, pool *pgxpool.Pool, schema authschema.ValidatedIdentifier, m Mutation) (err error) {
	defer func() {
		if err != nil {
			slog.ErrorContext(ctx, "audit: Commit failed; its own transaction rolled back",
				"audit_event_type", m.Audit.EventType, "outbox_events", len(m.Events),
				"aggregate_types", aggregateTypes(m.Events), "off_stream", m.OffStream, logging.ErrorAttr(err))
		}
	}()
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

	// Migration 0006's triggers refuse a write to a guarded table unless this
	// setting equals the current transaction id, so the guarantee is held by
	// PostgreSQL rather than by this package's callers. It is transaction-local.
	if _, err := tx.Exec(ctx, `SELECT set_config('`+commitGuardSetting+`', pg_current_xact_id()::text, true),
		set_config('`+changedSetting+`', pg_current_xact_id()::text || ':0', true)`); err != nil {
		return fmt.Errorf("%w: arming the commit guard: %w", ErrMutationFailed, err)
	}

	if applyErr := m.Apply(ctx, TxOps{tx: tx}); applyErr != nil {
		return fmt.Errorf("%w: applying the state mutation: %w", ErrMutationFailed, applyErr)
	}
	var changed int64
	if err := tx.QueryRow(ctx,
		`SELECT CASE WHEN current_setting('`+changedSetting+`', true) LIKE pg_current_xact_id()::text || ':%'
		THEN split_part(current_setting('`+changedSetting+`', true), ':', 2)::bigint ELSE 0 END`).Scan(&changed); err != nil {
		return fmt.Errorf("%w: reading the changed-row count: %w", ErrMutationFailed, err)
	}
	if changed <= 0 {
		return ErrNoStateChange
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
	// After the changed-row count: the revision rows are guarded, so a bump
	// counted before it would make a no-op mutation look like a change.
	if err := insertOutboxEvents(ctx, tx, schema, m); err != nil {
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

// insertOutboxEvents bumps each event's aggregate revision and writes the
// event with that revision. Events are taken in (aggregate_type, aggregate_id)
// order, so every transaction locks revision rows in one order and two
// mutations over the same aggregates cannot deadlock; events of one aggregate
// keep the caller's order. The upsert's row lock is held to commit, so the
// next writer of an aggregate reads the committed value (contiguous), and a
// rollback undoes the bump (no gap).
func insertOutboxEvents(ctx context.Context, tx pgx.Tx, schema authschema.ValidatedIdentifier, m Mutation) error {
	events := append([]OutboxEvent(nil), m.Events...)
	sort.SliceStable(events, func(i, j int) bool {
		if events[i].AggregateType != events[j].AggregateType {
			return events[i].AggregateType < events[j].AggregateType
		}
		return events[i].AggregateID < events[j].AggregateID
	})
	q := authschema.Quote(schema)
	for _, e := range events {
		var revision int64
		if err := tx.QueryRow(ctx, `
			INSERT INTO `+q+`.aggregate_revisions AS r (aggregate_type, aggregate_id, revision)
			VALUES ($1, $2, 1)
			ON CONFLICT (aggregate_type, aggregate_id)
			DO UPDATE SET revision = r.revision + 1, updated_at = clock_timestamp()
			RETURNING r.revision`, e.AggregateType, e.AggregateID).Scan(&revision); err != nil {
			return fmt.Errorf("%w: bumping the %s revision: %w", ErrMutationFailed, e.AggregateType, err)
		}
		payload := map[string]any{"revision": revision, "change": orEmpty(e.Change)}
		if m.Audit.OrganizationID != nil {
			payload["organization_id"] = provisioning.WireID("organization", *m.Audit.OrganizationID)
		}
		if m.Audit.ActorPrincipalID != nil {
			payload["actor_principal_id"] = provisioning.WireID("principal", *m.Audit.ActorPrincipalID)
		}
		encoded, err := json.Marshal(payload)
		if err != nil {
			return fmt.Errorf("%w: encoding the outbox payload", ErrMutationFailed)
		}
		key := e.AggregateType + ":" + e.AggregateID + ":" + strconv.FormatInt(revision, 10)
		if _, err := tx.Exec(ctx, `
			INSERT INTO `+q+`.auth_outbox_events
			    (aggregate_type, aggregate_id, event_type, payload, idempotency_key)
			VALUES ($1, $2, $3, $4, $5)`,
			e.AggregateType, e.AggregateID, e.EventType, encoded, key); err != nil {
			return fmt.Errorf("%w: inserting the outbox event: %w", ErrMutationFailed, err)
		}
		slog.DebugContext(ctx, "audit: outbox event written",
			"aggregate_type", e.AggregateType, "event_type", e.EventType, "revision", revision)
	}
	return nil
}

func aggregateTypes(events []OutboxEvent) []string {
	out := make([]string, 0, len(events))
	for _, e := range events {
		out = append(out, e.AggregateType)
	}
	return out
}

func insertAuditEvent(ctx context.Context, tx pgx.Tx, schema authschema.ValidatedIdentifier, a AuditEvent) error {
	attributes, err := json.Marshal(orEmpty(a.Attributes))
	if err != nil {
		return fmt.Errorf("%w: encoding the audit attributes", ErrMutationFailed)
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO `+authschema.Quote(schema)+`.security_audit_events
		    (event_type, outcome, actor_principal_id, subject_principal_id,
		     organization_id, policy_revision, attributes, request_id, client_ip)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::inet)`,
		a.EventType, string(a.Outcome), a.ActorPrincipalID, a.SubjectPrincipalID,
		a.OrganizationID, a.PolicyRevision, attributes, a.RequestID, clientaddr.Text(ctx))
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
