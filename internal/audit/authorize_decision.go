package audit

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// EventAuthorizeHighRiskDecided is one decision of POST /v1/authorize on an
// action the policy catalog classes high risk, allow or deny.
const EventAuthorizeHighRiskDecided = "auth.authorize.high_risk_decided"

// AppendAll writes the audit rows of append-only event types in ONE
// transaction: all rows or none.
func AppendAll(ctx context.Context, pool *pgxpool.Pool, schema authschema.ValidatedIdentifier, events ...AuditEvent) error {
	return errors.New("not implemented")
}
