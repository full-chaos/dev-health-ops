package joboperator

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrDomainPreconditionUnsupported = errors.New("authoritative domain precondition is not implemented for this job kind")

// PostgresDomainGuard is intentionally fail-closed in Phase 1. The frozen
// system contracts name schedule_occurrence and maintenance_run domain links,
// but neither has an authoritative semantic table yet. Treating the envelope
// UUID itself as domain truth would make cancellation/retry unsafe.
type PostgresDomainGuard struct {
	pool *pgxpool.Pool
}

func NewPostgresDomainGuard(pool *pgxpool.Pool) (*PostgresDomainGuard, error) {
	if pool == nil {
		return nil, ErrDomainPreconditionUnsupported
	}
	return &PostgresDomainGuard{pool: pool}, nil
}

func (guard *PostgresDomainGuard) Check(ctx context.Context, _ Action, job JobSummary) error {
	if guard == nil || guard.pool == nil {
		return ErrDomainPreconditionUnsupported
	}
	// Prove the semantic endpoint is reachable before returning the stable
	// unsupported precondition. This prevents configuration failures from being
	// mistaken for a safely evaluated domain decision.
	var reachable bool
	if err := guard.pool.QueryRow(ctx, "SELECT true").Scan(&reachable); err != nil || !reachable {
		return ErrDomainPreconditionUnsupported
	}
	switch job.Domain.Type {
	case "schedule_occurrence", "maintenance_run":
		return ErrDomainPreconditionUnsupported
	default:
		return ErrDomainPreconditionUnsupported
	}
}
