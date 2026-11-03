package session

import (
	"context"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/requestid"
)

// PublicRow writes, in the store's own audited transaction, the rows that must
// commit with its state change: go-api's public.audit_logs row of the
// compatibility route that took the change over from go-api (CHAOS-11400).
// principal and organization are the session's. An error rolls back
// everything, the state change and the auth audit row included.
type PublicRow func(ctx context.Context, tx audit.TxOps, principal uuid.UUID, organization *uuid.UUID) error

type publicRowKey struct{}

// WithPublicRow binds public to ctx: CreateAudited, ExchangeGuarded,
// ExchangeLegacy, RevokeByCredential and RevokeLegacy called with ctx run it as
// their *With form does. A compatibility route binds go-api's row this way,
// so its session interface stays the one the native routes use.
func WithPublicRow(ctx context.Context, public PublicRow) context.Context {
	return context.WithValue(ctx, publicRowKey{}, public)
}

func publicRowOf(ctx context.Context) PublicRow {
	public, _ := ctx.Value(publicRowKey{}).(PublicRow)
	return public
}

// EventSessionRefreshed is an accepted refresh exchange: one rotation of a
// session's refresh credential (or a go-api refresh token's first exchange).
// A refusal and a grace replay rotate nothing and write none.
const EventSessionRefreshed = "auth.session.refreshed"

// refreshedEvent is the auth.session.refreshed row of an exchange of
// sessionID's credential. It is fixed before the transaction; the exchange
// commits it only when it rotates.
func refreshedEvent(ctx context.Context, principal, sessionID string) audit.AuditEvent {
	return audit.AuditEvent{
		EventType: EventSessionRefreshed, Outcome: audit.OutcomeAllowed,
		SubjectPrincipalID: &principal, RequestID: requestid.Audit(ctx),
		Attributes: map[string]any{"reason_code": "accepted", "target_kind": "session", "target_id": sessionID},
	}
}

// txWriter is the reads and writes rotate makes: a pgx.Tx or an audit.TxOps.
type txWriter interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}
