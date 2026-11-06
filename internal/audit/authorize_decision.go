package audit

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// EventAuthorizeHighRiskDecided is one decision of POST /v1/authorize on an
// action the policy catalog classes high risk, allow or deny.
const EventAuthorizeHighRiskDecided = "auth.authorize.high_risk_decided"

// Registered here, not in the appendOnlyEventTypes literal, so the line anchors
// that contracts/auth/v1 pins in append.go stay where they are.
func init() { appendOnlyEventTypes[EventAuthorizeHighRiskDecided] = true }

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
	if err := validateAppendOnly(events); err != nil {
		return err
	}
	if pool == nil {
		return fmt.Errorf("%w: no connection pool", ErrMutationFailed)
	}
	return appendTx(ctx, pool.Begin, schema, callRows{ctx: ctx, events: events})
}

// validateAppendOnly refuses an empty set, a row Append would refuse and a
// type that is not append-only, before anything is written.
func validateAppendOnly(events []AuditEvent) error {
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
	return nil
}

// callRows are the rows of one call and the call's own context, whose request
// values (client address, user agent) its rows record.
type callRows struct {
	ctx    context.Context
	events []AuditEvent
}

// rowContext has the shared transaction's deadline and cancellation and one
// call's request values.
type rowContext struct {
	context.Context
	values context.Context
}

func (c rowContext) Value(key any) any { return c.values.Value(key) }

// appendTx writes the rows of calls in one armed transaction opened by begin:
// all rows or none. ctx bounds the transaction; each row records its own
// call's request values.
func appendTx(ctx context.Context, begin func(context.Context) (pgx.Tx, error), schema authschema.ValidatedIdentifier,
	calls ...callRows) error {
	tx, err := begin(ctx)
	if err != nil {
		return fmt.Errorf("%w: beginning the transaction", ErrMutationFailed)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := armTx(ctx, tx); err != nil {
		return err
	}
	for _, call := range calls {
		rctx := rowContext{Context: ctx, values: call.ctx}
		for _, event := range call.events {
			if err := insertAuditEvent(rctx, tx, schema, event); err != nil {
				return err
			}
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("%w: committing", ErrMutationFailed)
	}
	return nil
}
