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

// PGLinks is Links over auth.link_credentials (0010). A consume writes
// consumed_at, a security column: it commits through audit.Commit. An insert
// is unguarded, and a reissue deletes the prior unconsumed rows.
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
// same statements in the same order: begin, lock, delete, insert, commit. The
// delete removes the prior unconsumed credentials of the principal and
// purpose, so a reissue leaves exactly one unconsumed row and a replaced link
// is simply unknown; consumed rows stay. A decoy (zero PrincipalID) runs the
// same statements with the delete and the insert matching no row.
// Statement-shape parity is the invariant; timing parity is not claimed.
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
	if _, err := tx.Exec(ctx, `DELETE FROM `+l.q+`.link_credentials
WHERE principal_id = $1::uuid AND purpose = $2 AND consumed_at IS NULL AND $3::boolean`, principal.String(), string(req.Purpose), real); err != nil {
		return fmt.Errorf("local: replacing link credentials: %w", err)
	}
	// $5 false is a decoy: the same statement text, no row.
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

// Redeem consumes the credential: UPDATE .. WHERE consumed_at IS NULL AND
// expires_at > now(), exactly one row, on the database clock. A replay, an
// expired, a replaced or an unknown credential is (zero, false, nil): the
// same refusal for all of them.
func (l *PGLinks) Redeem(ctx context.Context, req RedeemRequest) (uuid.UUID, bool, error) {
	events, known := redeemEvents[req.Purpose]
	if !known {
		return uuid.Nil, false, fmt.Errorf("local: unknown link purpose %q", req.Purpose)
	}
	hash := req.TokenHash[:]
	var principal string
	// Cheap read first: a garbage secret never opens a Commit (which logs its failures).
	// A reissue that deletes this row between the read and the consume leaves the
	// consume with no row: the answer and the audit rows equal the unknown-
	// credential path, and Commit logs that one failure.
	err := l.pool.QueryRow(ctx, `SELECT principal_id::text FROM `+l.q+`.link_credentials
WHERE token_hash = $1 AND purpose = $2 AND consumed_at IS NULL AND expires_at > now()`, hash, string(req.Purpose)).Scan(&principal)
	if errors.Is(err, pgx.ErrNoRows) {
		return uuid.Nil, false, nil
	}
	if err != nil {
		return uuid.Nil, false, fmt.Errorf("local: reading a link credential: %w", err)
	}
	err = audit.Commit(ctx, l.pool, l.schema, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			tag, err := tx.Exec(ctx, `UPDATE `+l.q+`.link_credentials SET consumed_at = now()
WHERE token_hash = $1 AND purpose = $2 AND consumed_at IS NULL AND expires_at > now()`, hash, string(req.Purpose))
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
