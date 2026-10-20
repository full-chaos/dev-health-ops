package session

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/clientaddr"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/requestid"
	"github.com/full-chaos/dev-health-ops/internal/auth/useragent"
)

// CreateAudited is Create with event committed in the same transaction
// (audit.Commit, off the outbox stream): a sign-in's audit row and its
// session exist together or not at all. The event's request id falls back to
// the context's, as every session writer's does.
func (s *Store) CreateAudited(ctx context.Context, principalID uuid.UUID, organizationID *uuid.UUID, amr []string, event audit.AuditEvent) (Issued, error) {
	if !validAMR(amr) {
		return Issued{}, ErrInvalidAMR
	}
	if event.RequestID == nil {
		event.RequestID = requestid.Audit(ctx)
	}
	var out Issued
	err := audit.Commit(ctx, s.pool, s.schema, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			var err error
			out, err = s.insertSession(ctx, tx, principalID, organizationID, amr)
			return err
		},
		Audit:     event,
		OffStream: audit.OffStreamSessionLifecycle,
	})
	if err != nil {
		if errors.Is(err, ErrPrincipalUnavailable) {
			return Issued{}, ErrPrincipalUnavailable
		}
		return Issued{}, fmt.Errorf("%w: creating a session: %w", ErrStore, err)
	}
	s.logger.DebugContext(ctx, "session created", "session_id", out.Session.ID.String())
	return out, nil
}

// insertSession writes a session and its first refresh credential, the rows
// Create writes, inside the caller's transaction. The session's client_ip is
// the request's client address and its user_agent the request's user agent,
// when they are bound.
func (s *Store) insertSession(ctx context.Context, tx queryRower, principalID uuid.UUID, organizationID *uuid.UUID, amr []string) (Issued, error) {
	credential, err := newCredential()
	if err != nil {
		return Issued{}, err
	}
	handle, err := newCredential()
	if err != nil {
		return Issued{}, err
	}
	var org *string
	if organizationID != nil {
		text := organizationID.String()
		org = &text
	}
	// The session token_hash is a digest of a value never returned: there is
	// no session bearer in wave 2, and the column is NOT NULL and unique.
	// An uncapped session stores 'infinity'; the record reports the
	// effective expiry (see sessionColumns).
	var sessionID string
	err = tx.QueryRow(ctx, `
		INSERT INTO `+s.q+`.sessions (principal_id, organization_id, token_hash, issued_at, created_at, updated_at, expires_at, auth_time, amr, client_ip, user_agent)
		SELECT p.id, $2::uuid, $3, t.at, t.at, t.at,
		       CASE WHEN $4::double precision > 0 THEN t.at + make_interval(secs => $4::double precision) ELSE 'infinity' END,
		       t.at, $5, $6::inet, $7
		FROM `+s.q+`.principals p, (SELECT clock_timestamp() AS at) t
		WHERE p.id = $1 AND p.disabled_at IS NULL
		RETURNING id::text`,
		principalID.String(), org, hashCredential(handle), s.sessionLifetime.Seconds(), amr, clientaddr.Text(ctx), useragent.Text(ctx)).Scan(&sessionID)
	if errors.Is(err, pgx.ErrNoRows) {
		return Issued{}, ErrPrincipalUnavailable
	}
	if err != nil {
		return Issued{}, err
	}
	var out Issued
	if err := tx.QueryRow(ctx, `
		INSERT INTO `+s.q+`.refresh_credentials (session_id, principal_id, token_hash, expires_at)
		SELECT s.id, s.principal_id, $2, LEAST(clock_timestamp() + make_interval(secs => $3::double precision), s.expires_at)
		FROM `+s.q+`.sessions s WHERE s.id = $1
		RETURNING expires_at`,
		sessionID, hashCredential(credential), s.refreshTTL.Seconds(),
	).Scan(&out.ExpiresAt); err != nil {
		return Issued{}, err
	}
	out.Session, err = scanRecord(tx.QueryRow(ctx, `SELECT `+s.sessionColumns()+` FROM `+s.q+`.sessions s WHERE s.id = $1`, sessionID))
	if err != nil {
		return Issued{}, err
	}
	out.Credential = credential
	return out, nil
}

// RevokeByCredential ends the session of a live refresh credential: one that
// is unconsumed, unrevoked and unexpired, re-read under the session lock. It
// reports false, writing nothing, for a malformed, unknown, consumed, revoked
// or expired credential, or a session that is no longer live: a sign-out with
// a credential that no longer works changes nothing (go-api's logout revokes
// only the presented token).
func (s *Store) RevokeByCredential(ctx context.Context, credential contracts.SecretString, actor Actor) (bool, error) {
	if !wellFormed(credential) {
		return false, nil
	}
	hash := hashCredential(credential)
	var id, principal string
	err := s.pool.QueryRow(ctx, `SELECT session_id::text, principal_id::text FROM `+s.q+`.refresh_credentials WHERE token_hash = $1`, hash).
		Scan(&id, &principal)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("%w: reading the refresh credential: %w", ErrStore, err)
	}
	err = audit.Commit(ctx, s.pool, s.schema, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			locked, err := s.lockSession(ctx, tx, id)
			if errors.Is(err, pgx.ErrNoRows) {
				return nil
			}
			if err != nil {
				return err
			}
			if locked.RevokedAt != nil || locked.expired {
				return nil
			}
			var live bool
			if err := tx.QueryRow(ctx, `
				SELECT consumed_at IS NULL AND revoked_at IS NULL AND expires_at > clock_timestamp()
				FROM `+s.q+`.refresh_credentials WHERE token_hash = $1 AND session_id = $2`, hash, id).Scan(&live); err != nil {
				return err
			}
			if !live {
				return nil
			}
			return s.revokeSessions(ctx, tx, []string{id}, revokedByCaller)
		},
		Audit: audit.AuditEvent{
			EventType: "auth.session.revoked", Outcome: audit.OutcomeAllowed,
			ActorPrincipalID: actorID(actor), SubjectPrincipalID: &principal, RequestID: requestIDOf(ctx, actor),
			Attributes: map[string]any{
				"reason_code": "session_revoked", "target_kind": "session", "target_id": id,
				"credential_class": "refresh_credential",
			},
		},
		OffStream: audit.OffStreamSessionLifecycle,
	})
	switch {
	case err == nil:
		s.revocations.Add(ctx, 1, metric.WithAttributes(attribute.String("reason", revokedByCaller)))
		s.logger.InfoContext(ctx, "session revoked by its refresh credential", "session_id", id)
		return true, nil
	case errors.Is(err, audit.ErrNoStateChange):
		return false, nil
	default:
		return false, fmt.Errorf("%w: revoking a session by its credential: %w", ErrStore, err)
	}
}
