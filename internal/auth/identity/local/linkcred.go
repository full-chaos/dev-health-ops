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

// issueAttempts bounds the retries of Issue when a concurrent issuer leaves a
// credential between its check and its insert.
const issueAttempts = 3

// lockKey is the advisory lock key text of one serialized operation.
func lockKey(scope string, principal string) string { return scope + ":" + principal }

// Issue stores one credential. Issuers of one principal and purpose run one at
// a time (a transaction advisory lock). Older unconsumed credentials of the
// principal and purpose are marked consumed in the same transaction as the
// insert; nothing is deleted (the sweep owns deletes). A decoy request reads
// only.
func (l *PGLinks) Issue(ctx context.Context, req IssueRequest) error {
	hash := req.TokenHash[:]
	if req.PrincipalID == uuid.Nil {
		var n int
		if err := l.pool.QueryRow(ctx, `SELECT count(*) FROM `+l.q+`.link_credentials WHERE token_hash = $1`, hash).Scan(&n); err != nil {
			return fmt.Errorf("local: link credential decoy: %w", err)
		}
		return nil
	}
	principal := req.PrincipalID.String()
	key := lockKey("link:"+string(req.Purpose), principal)
	insert := `INSERT INTO ` + l.q + `.link_credentials (principal_id, purpose, token_hash, expires_at)
VALUES ($1, $2, $3, now() + make_interval(secs => $4))`
	secs := req.TTL.Seconds()
	const lock = `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`

	for attempt := 0; attempt < issueAttempts; attempt++ {
		// Read without the lock: audit.Commit logs an error for a mutation
		// that changes nothing, so the first issuance skips it.
		var older bool
		if err := l.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM `+l.q+`.link_credentials
WHERE principal_id = $1 AND purpose = $2 AND consumed_at IS NULL)`, principal, string(req.Purpose)).Scan(&older); err != nil {
			return fmt.Errorf("local: reading older link credentials: %w", err)
		}
		if !older {
			inserted, err := l.insertIfNoneLive(ctx, key, principal, req.Purpose, insert, hash, secs)
			if err != nil {
				return err
			}
			if inserted {
				return nil
			}
			continue
		}
		err := audit.Commit(ctx, l.pool, l.schema, audit.Mutation{
			Apply: func(ctx context.Context, tx audit.TxOps) error {
				if _, err := tx.Exec(ctx, lock, key); err != nil {
					return err
				}
				if _, err := tx.Exec(ctx, `UPDATE `+l.q+`.link_credentials SET consumed_at = now()
WHERE principal_id = $1 AND purpose = $2 AND consumed_at IS NULL`, principal, string(req.Purpose)); err != nil {
					return err
				}
				_, err := tx.Exec(ctx, insert, principal, string(req.Purpose), hash, secs)
				return err
			},
			Audit: audit.AuditEvent{
				EventType: "auth.link_credential.superseded", Outcome: audit.OutcomeAllowed,
				SubjectPrincipalID: &principal,
				Attributes:         map[string]any{"reason_code": "credential_revoked", "target_kind": "principal", "target_id": principal},
			},
			Event: audit.OutboxEvent{
				AggregateType: "principal", AggregateID: principal, EventType: "principal.link_credential_superseded",
				Payload:        map[string]any{"principal_id": principal, "purpose": string(req.Purpose)},
				IdempotencyKey: "principal.link_credential_superseded:" + principal + ":" + uuid.NewString(),
			},
		})
		if err == nil {
			return nil
		}
		if !errors.Is(err, audit.ErrNoStateChange) {
			return fmt.Errorf("local: superseding link credentials: %w", err)
		}
		// Nothing was left to supersede. Insert under the same lock, unless a
		// concurrent issuer stored one meanwhile (then supersede it).
		inserted, err := l.insertIfNoneLive(ctx, key, principal, req.Purpose, insert, hash, secs)
		if err != nil {
			return err
		}
		if inserted {
			return nil
		}
	}
	return errors.New("local: link credential issue kept racing another issuer")
}

// insertIfNoneLive stores the credential under the issue lock only when the
// principal has no unconsumed credential of the purpose.
func (l *PGLinks) insertIfNoneLive(ctx context.Context, key, principal string, purpose Purpose, insert string, hash []byte, secs float64) (bool, error) {
	tx, err := l.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("local: storing a link credential: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, key); err != nil {
		return false, fmt.Errorf("local: locking the issue: %w", err)
	}
	var live bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM `+l.q+`.link_credentials
WHERE principal_id = $1 AND purpose = $2 AND consumed_at IS NULL)`, principal, string(purpose)).Scan(&live); err != nil {
		return false, fmt.Errorf("local: reading link credentials: %w", err)
	}
	if live {
		return false, nil
	}
	if _, err := tx.Exec(ctx, insert, principal, string(purpose), hash, secs); err != nil {
		return false, fmt.Errorf("local: storing a link credential: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("local: storing a link credential: %w", err)
	}
	return true, nil
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

var redeemEvents = map[Purpose]struct{ auditEvent, outboxEvent, reason string }{
	PurposeEmailVerification: {"auth.email.verified", "principal.email_verified", "email_verified"},
	PurposePasswordReset:     {"auth.password.reset", "principal.password_reset", "password_reset"},
}

// Redeem consumes the credential: UPDATE .. WHERE consumed_at IS NULL AND
// expires_at > now(), exactly one row, on the database clock. A replay, an
// expired or an unknown credential is (zero, false, nil).
func (l *PGLinks) Redeem(ctx context.Context, req RedeemRequest) (uuid.UUID, bool, error) {
	events, known := redeemEvents[req.Purpose]
	if !known {
		return uuid.Nil, false, fmt.Errorf("local: unknown link purpose %q", req.Purpose)
	}
	hash := req.TokenHash[:]
	var credID, principal string
	// Cheap read first: a garbage secret never opens a Commit (which logs its failures).
	err := l.pool.QueryRow(ctx, `SELECT id::text, principal_id::text FROM `+l.q+`.link_credentials
WHERE token_hash = $1 AND purpose = $2 AND consumed_at IS NULL AND expires_at > now()`, hash, string(req.Purpose)).Scan(&credID, &principal)
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
		Event: audit.OutboxEvent{
			AggregateType: "principal", AggregateID: principal, EventType: events.outboxEvent,
			Payload:        map[string]any{"principal_id": principal},
			IdempotencyKey: events.outboxEvent + ":" + credID,
		},
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
