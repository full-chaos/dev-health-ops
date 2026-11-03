package session

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/clientaddr"
)

// ClientSessionLifetime is the absolute life of an OAuth client's session
// (ACP-ADR-13 decision 5; acp, CHAOS-12251). The idle life is the OAuth
// credential's, and comes with the token endpoint.
const ClientSessionLifetime = 90 * 24 * time.Hour

var (
	// ErrWebSessionNotLive refuses a client session under a web session that is
	// missing, revoked, expired, itself a client session, or whose principal is
	// disabled.
	ErrWebSessionNotLive = errors.New("session: the web session is not live")
	// ErrWebSessionDelegated refuses a client session under a delegated session.
	ErrWebSessionDelegated = errors.New("session: the web session is delegated")
	// ErrWebSessionNoOrganization refuses a client session under a web session
	// bound to no organization: the live access read needs one.
	ErrWebSessionNoOrganization = errors.New("session: the web session has no organization")
)

// ClientSessionInput is a new OAuth client session.
type ClientSessionInput struct {
	// ID is the new session's id.
	ID uuid.UUID
	// WebSession is the web session the user consented under.
	WebSession uuid.UUID
	// Linked is true for a browser client: its session ends with the web session.
	// An MCP client's session has no link, so a web logout, an organization switch
	// and web idle expiry leave it live.
	Linked bool
}

// InsertClientSession writes an OAuth client's own session row inside the
// caller's transaction, under the web session's row lock. A web sid is never a
// client's sid. The row takes the web session's principal, organization,
// version pin, auth_time and amr, so a stale pin stays stale and a client session
// reads as old as the sign-in it came from. No refresh credential is written:
// that is the OAuth credential, with the token endpoint.
func (s *Store) InsertClientSession(ctx context.Context, tx audit.TxOps, in ClientSessionInput) (Record, error) {
	web, err := s.lockSession(ctx, tx, in.WebSession.String())
	if errors.Is(err, pgx.ErrNoRows) {
		return Record{}, ErrWebSessionNotLive
	}
	if err != nil {
		return Record{}, err
	}
	switch {
	case web.RevokedAt != nil, web.expired, web.principalDisabled:
		return Record{}, ErrWebSessionNotLive
	case web.Delegation != nil:
		return Record{}, ErrWebSessionDelegated
	case web.OrganizationID == nil:
		return Record{}, ErrWebSessionNoOrganization
	}
	handle, err := newCredential()
	if err != nil {
		return Record{}, err
	}
	var id string
	err = tx.QueryRow(ctx, `
		INSERT INTO `+s.q+`.sessions (id, principal_id, organization_id, token_hash, issued_at, created_at, updated_at, expires_at,
			auth_time, amr, client_ip, user_agent, legacy_token_version, web_session_id)
		SELECT $1::uuid, w.principal_id, w.organization_id, $2, t.at, t.at, t.at, t.at + make_interval(secs => $3::double precision),
			w.auth_time, w.amr, $4::inet, $5, w.legacy_token_version, CASE WHEN $6::boolean THEN w.id END
		FROM `+s.q+`.sessions w CROSS JOIN (SELECT clock_timestamp() AS at) t
		WHERE w.id = $7::uuid AND w.web_session_id IS NULL
		  AND NOT EXISTS (SELECT 1 FROM `+s.q+`.oauth_grants g WHERE g.session_id = w.id)
		RETURNING id::text`,
		in.ID.String(), hashCredential(handle), ClientSessionLifetime.Seconds(), clientaddr.Text(ctx), userAgentOf(ctx),
		in.Linked, in.WebSession.String()).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return Record{}, ErrWebSessionNotLive
	}
	if err != nil {
		return Record{}, fmt.Errorf("inserting a client session: %w", err)
	}
	return scanRecord(tx.QueryRow(ctx, `SELECT `+s.sessionColumns()+` FROM `+s.q+`.sessions s WHERE s.id = $1`, id))
}

// ActorInDelegation is the delegated side of the actor check (ACP-ADR-13; acp,
// CHAOS-12251): true when session is delegated, or when principal is the real
// actor of a live delegated session, through the effective expressions. A session
// row that does not exist is an error, never false.
func (s *Store) ActorInDelegation(ctx context.Context, principal, session uuid.UUID) (bool, error) {
	var delegated *bool
	var actor bool
	err := s.pool.QueryRow(ctx, `
		SELECT (SELECT d.actor_principal_id IS NOT NULL FROM `+s.q+`.sessions d WHERE d.id = $2::uuid),
			EXISTS (SELECT 1 FROM `+s.q+`.sessions s
				WHERE s.actor_principal_id = $1::uuid
				  AND `+s.effectiveRevokedAt()+` IS NULL
				  AND `+s.effectiveExpiry()+` > clock_timestamp())`,
		principal.String(), session.String()).Scan(&delegated, &actor)
	if err != nil {
		return false, fmt.Errorf("%w: reading the delegated side: %w", ErrStore, err)
	}
	if delegated == nil {
		return false, fmt.Errorf("%w: reading the delegated side: session %s does not exist", ErrStore, session)
	}
	return *delegated || actor, nil
}
