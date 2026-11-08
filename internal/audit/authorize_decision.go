package audit

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// EventAuthorizeHighRiskDecided is one decision of POST /v1/authorize on an
// action the policy catalog classes high risk, allow or deny.
const EventAuthorizeHighRiskDecided = "auth.authorize.high_risk_decided"

// EventAuthorizeDelegatedDecided is one decision of POST /v1/authorize for a
// delegated (impersonation) session, allow or deny, at any risk class. The
// actor is the real actor, the subject the target user.
const EventAuthorizeDelegatedDecided = "auth.authorize.delegated_decided"

// Registered here, not in the appendOnlyEventTypes literal, so the line anchors
// that contracts/auth/v1 pins in append.go stay where they are.
func init() {
	appendOnlyEventTypes[EventAuthorizeHighRiskDecided] = true
	appendOnlyEventTypes[EventAuthorizeDelegatedDecided] = true
}

// AppendAll writes the audit rows of append-only event types in ONE
// transaction: all rows or none. Each row is validated as Append validates it,
// and nothing is written when any row is refused.
func AppendAll(ctx context.Context, pool *pgxpool.Pool, schema authschema.ValidatedIdentifier, events ...AuditEvent) (err error) {
	defer func() {
		if err != nil {
			slog.ErrorContext(ctx, "audit: AppendAll failed; nothing was written",
				"audit_rows", len(events), logging.ErrorAttr(err))
		}
	}()
	if len(events) == 0 {
		return fmt.Errorf("%w: no audit rows", ErrMutationFailed)
	}
	for _, event := range events {
		if err := event.validate(); err != nil {
			return err
		}
		if !appendOnlyEventTypes[event.EventType] {
			return ErrNotAppendOnly
		}
	}
	if pool == nil {
		return fmt.Errorf("%w: no connection pool", ErrMutationFailed)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("%w: beginning the transaction", ErrMutationFailed)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := armTx(ctx, tx); err != nil {
		return err
	}
	for _, event := range events {
		if err := insertAuditEvent(ctx, tx, schema, event); err != nil {
			return err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("%w: committing", ErrMutationFailed)
	}
	return nil
}
