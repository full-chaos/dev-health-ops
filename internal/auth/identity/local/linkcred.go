package local

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

var errNotRedeemable = errors.New("local: the credential is not redeemable")

// PGLinks is Links over auth.link_credentials (0010). A consume or a
// supersede writes consumed_at, a security column: it commits through
// audit.Commit. An insert is unguarded.
type PGLinks struct {
	pool   *pgxpool.Pool
	schema authschema.ValidatedIdentifier
	q      string
}

// NewPGLinks builds the store.
func NewPGLinks(pool *pgxpool.Pool, schema authschema.ValidatedIdentifier) *PGLinks {
	return &PGLinks{pool: pool, schema: schema, q: authschema.Quote(schema)}
}

// lockKey is the advisory lock key text of one serialized operation.
func lockKey(scope string, principal string) string { return scope + ":" + principal }

// Issue stores one credential. Issuers of one principal and purpose run one at
// a time (a transaction advisory lock). Every call, decoy included, runs the
// same statements in the same order: begin, lock, insert, commit. A decoy
// (zero PrincipalID) inserts no row. Statement-shape parity is the invariant;
// timing parity is not claimed. Only the newest credential of a principal and
// purpose redeems (see Redeem), so nothing is updated or deleted here.
func (l *PGLinks) Issue(ctx context.Context, req IssueRequest) error {
	real := req.PrincipalID != uuid.Nil
	principal := req.PrincipalID
	if !real {
		principal = uuid.New()
	}
	tx, err := l.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("local: storing a link credential: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, lockKey("link:"+string(req.Purpose), principal.String())); err != nil {
		return fmt.Errorf("local: locking the issue: %w", err)
	}
	// $5 false is a decoy: the same statement text, no row. created_at is the
	// database clock read after the lock, so "newest" follows the lock order.
	if _, err := tx.Exec(ctx, `INSERT INTO `+l.q+`.link_credentials (principal_id, purpose, token_hash, expires_at, created_at)
SELECT $1::uuid, $2, $3, now() + make_interval(secs => $4::double precision), clock_timestamp() WHERE $5::boolean`, principal.String(), string(req.Purpose), req.TokenHash[:], req.TTL.Seconds(), real); err != nil {
		return fmt.Errorf("local: storing a link credential: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("local: storing a link credential: %w", err)
	}
	return nil
}

// WithPrincipalLock runs fn inside a transaction that holds a per-principal
// advisory lock; the lock ends with the transaction, also when the connection
// is lost. fn runs on other pool connections.
func (l *PGLinks) WithPrincipalLock(ctx context.Context, principalID uuid.UUID, fn func(ctx context.Context) error) error {
	tx, err := l.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("local: locking the principal: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, lockKey("password", principalID.String())); err != nil {
		return fmt.Errorf("local: locking the principal: %w", err)
	}
	if err := fn(ctx); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("local: releasing the principal lock: %w", err)
	}
	return nil
}

var _ Links = (*PGLinks)(nil)
var _ Locker = (*PGLinks)(nil)

var redeemEvents = map[Purpose]struct{ auditEvent, reason string }{
	PurposeEmailVerification: {"auth.email.verified", "email_verified"},
	PurposePasswordReset:     {"auth.password.reset", "password_reset"},
}

// newestCredential is the SQL of the id of the newest credential of the
// principal and purpose of the row aliased l, consumed ones included.
func (l *PGLinks) newestCredential() string {
	return `(SELECT n.id FROM ` + l.q + `.link_credentials n
 WHERE n.principal_id = l.principal_id AND n.purpose = l.purpose ORDER BY n.created_at DESC, n.id DESC LIMIT 1)`
}

// Redeem consumes the credential: UPDATE .. WHERE consumed_at IS NULL AND
// expires_at > now() AND it is the newest of its principal and purpose,
// exactly one row, on the database clock. A replay, an expired or an unknown
// credential is (zero, false, nil). A live credential that a newer one
// replaced is refused with ErrLinkSuperseded; the refusal burns it and writes
// a denied audit row.
func (l *PGLinks) Redeem(ctx context.Context, req RedeemRequest) (uuid.UUID, bool, error) {
	events, known := redeemEvents[req.Purpose]
	if !known {
		return uuid.Nil, false, fmt.Errorf("local: unknown link purpose %q", req.Purpose)
	}
	hash := req.TokenHash[:]
	var principal string
	var newest bool
	// Cheap read first: a garbage secret never opens a Commit (which logs its failures).
	err := l.pool.QueryRow(ctx, `SELECT l.principal_id::text, l.id = `+l.newestCredential()+`
FROM `+l.q+`.link_credentials l
WHERE l.token_hash = $1 AND l.purpose = $2 AND l.consumed_at IS NULL AND l.expires_at > now()`, hash, string(req.Purpose)).Scan(&principal, &newest)
	if errors.Is(err, pgx.ErrNoRows) {
		return uuid.Nil, false, nil
	}
	if err != nil {
		return uuid.Nil, false, fmt.Errorf("local: reading a link credential: %w", err)
	}
	if !newest {
		return uuid.Nil, false, l.burnSuperseded(ctx, hash, req.Purpose, principal)
	}
	err = audit.Commit(ctx, l.pool, l.schema, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			tag, err := tx.Exec(ctx, `UPDATE `+l.q+`.link_credentials l SET consumed_at = now()
WHERE l.token_hash = $1 AND l.purpose = $2 AND l.consumed_at IS NULL AND l.expires_at > now()
AND l.id = `+l.newestCredential(), hash, string(req.Purpose))
			if err != nil {
				return err
			}
			if tag.RowsAffected() != 1 {
				return errNotRedeemable
			}
			return nil
		},
		Audit: audit.AuditEvent{
			EventType: events.auditEvent, Outcome: audit.OutcomeAllowed,
			ActorPrincipalID: &principal, SubjectPrincipalID: &principal,
			Attributes: map[string]any{"reason_code": events.reason, "target_kind": "principal", "target_id": principal},
		},
		OffStream: audit.OffStreamLinkCredentialLifecycle,
	})
	switch {
	case err == nil:
		id, perr := uuid.Parse(principal)
		return id, true, perr
	case errors.Is(err, errNotRedeemable):
		return uuid.Nil, false, nil
	default:
		return uuid.Nil, false, fmt.Errorf("local: consuming a link credential: %w", err)
	}
}

// burnSuperseded marks a replaced credential consumed with a denied audit row
// and reports it as superseded.
func (l *PGLinks) burnSuperseded(ctx context.Context, hash []byte, purpose Purpose, principal string) error {
	err := audit.Commit(ctx, l.pool, l.schema, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			tag, err := tx.Exec(ctx, `UPDATE `+l.q+`.link_credentials SET consumed_at = now()
WHERE token_hash = $1 AND purpose = $2 AND consumed_at IS NULL`, hash, string(purpose))
			if err != nil {
				return err
			}
			if tag.RowsAffected() != 1 {
				return errNotRedeemable
			}
			return nil
		},
		Audit: audit.AuditEvent{
			EventType: "auth.link_credential.superseded", Outcome: audit.OutcomeDenied,
			SubjectPrincipalID: &principal,
			Attributes:         map[string]any{"reason_code": "credential_revoked", "target_kind": "principal", "target_id": principal},
		},
		OffStream: audit.OffStreamLinkCredentialLifecycle,
	})
	switch {
	case err == nil:
		return ErrLinkSuperseded
	case errors.Is(err, errNotRedeemable):
		return nil
	default:
		return fmt.Errorf("local: refusing a superseded link credential: %w", err)
	}
}
