package identity

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/full-chaos/dev-health-ops/internal/audit"
)

var (
	// ErrOrganizationNotFound reports a ProjectOrganization of an id
	// public.organizations does not hold.
	ErrOrganizationNotFound = errors.New("identity: organization not found")
	// ErrOrganizationSlugCollision reports a projection whose slug another
	// organization already holds in the auth schema. Nothing is written.
	ErrOrganizationSlugCollision = errors.New("identity: the organization slug belongs to another organization")
)

const organizationsSlugKey = "organizations_slug_key"

// errOrganizationProjectedConcurrently ends a first projection that a
// concurrent call already inserted; it is success.
var errOrganizationProjectedConcurrently = errors.New("identity: organization projected concurrently")

type orgState struct {
	slug   string
	name   string
	active bool
}

// ProjectOrganization makes auth.organizations agree with public.organizations
// for one org (id, slug, name, and disabled_at from is_active). go-api stays
// the organizations writer until Wave 4; this is a read projection, so it is
// not gated by UsersOwnedByAuth. auth.organizations is guarded on every write,
// so the first projection (reason organization_created) and a change (reason
// organization_updated) each commit through audit.Commit with one audit row
// and one outbox event. An unchanged org writes nothing.
func (s *Store) ProjectOrganization(ctx context.Context, orgID uuid.UUID) (err error) {
	outcome := "unchanged"
	defer func() {
		switch {
		case errors.Is(err, ErrOrganizationNotFound):
			outcome = "not_found"
		case errors.Is(err, ErrOrganizationSlugCollision):
			outcome = "slug_collision"
		case err != nil:
			outcome = "error"
		}
		s.projections.Add(ctx, 1, metric.WithAttributes(attribute.String("kind", "organization"), attribute.String("outcome", outcome)))
	}()
	var src orgState
	err = s.pool.QueryRow(ctx, `SELECT slug, name, is_active FROM public.organizations WHERE id = $1`, orgID).
		Scan(&src.slug, &src.name, &src.active)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrOrganizationNotFound
	}
	if err != nil {
		return s.storeErr(ctx, "read public.organizations", err)
	}
	var cur orgState
	err = s.pool.QueryRow(ctx, `SELECT slug, name, disabled_at IS NULL FROM `+s.q+`.organizations WHERE id = $1`, orgID).
		Scan(&cur.slug, &cur.name, &cur.active)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		if err := s.insertOrganization(ctx, orgID, src); err != nil {
			return err
		}
		outcome = "inserted"
		return nil
	case err != nil:
		return s.storeErr(ctx, "read auth.organizations", err)
	case cur == src:
		return nil
	}
	if err := s.updateOrganization(ctx, orgID, src); err != nil {
		return err
	}
	outcome = "updated"
	return nil
}

func (s *Store) insertOrganization(ctx context.Context, orgID uuid.UUID, src orgState) error {
	org := orgID.String()
	err := audit.Commit(ctx, s.pool, s.schema, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			var inserted uuid.UUID
			err := tx.QueryRow(ctx, `INSERT INTO `+s.q+`.organizations (id, slug, name, disabled_at)
VALUES ($1, $2, $3, CASE WHEN $4::boolean THEN NULL ELSE clock_timestamp() END)
ON CONFLICT DO NOTHING
RETURNING id`, orgID, src.slug, src.name, src.active).Scan(&inserted)
			if !errors.Is(err, pgx.ErrNoRows) {
				return err
			}
			// A concurrent first projection of the same org conflicts on the
			// slug as well as the id, so the arbiter is any unique index and
			// the id decides which case this is.
			var exists bool
			if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM `+s.q+`.organizations WHERE id = $1)`, orgID).Scan(&exists); err != nil {
				return err
			}
			if exists {
				return errOrganizationProjectedConcurrently
			}
			return ErrOrganizationSlugCollision
		},
		Audit: organizationAudit("auth.organization.projected", "organization_created", org),
		Event: audit.OutboxEvent{
			AggregateType: "organization", AggregateID: org, EventType: "organization.projected",
			IdempotencyKey: "organization.projected:" + org,
		},
	})
	switch {
	case err == nil:
		s.logger.InfoContext(ctx, "identity: organization projected", "organization_id", org)
		return nil
	case errors.Is(err, errOrganizationProjectedConcurrently):
		return nil
	case errors.Is(err, ErrOrganizationSlugCollision):
		return ErrOrganizationSlugCollision
	}
	return s.storeErr(ctx, "insert organization", err)
}

func (s *Store) updateOrganization(ctx context.Context, orgID uuid.UUID, src orgState) error {
	org := orgID.String()
	eventID := uuid.New()
	err := audit.Commit(ctx, s.pool, s.schema, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			_, err := tx.Exec(ctx, `UPDATE `+s.q+`.organizations
SET slug = $2, name = $3, disabled_at = CASE WHEN $4::boolean THEN NULL ELSE COALESCE(disabled_at, clock_timestamp()) END,
    updated_at = clock_timestamp()
WHERE id = $1 AND (slug IS DISTINCT FROM $2 OR name IS DISTINCT FROM $3 OR (disabled_at IS NULL) IS DISTINCT FROM $4::boolean)`,
				orgID, src.slug, src.name, src.active)
			return err
		},
		Audit: organizationAudit("auth.organization.projection_updated", "organization_updated", org),
		Event: audit.OutboxEvent{
			AggregateType: "organization", AggregateID: org, EventType: "organization.projection_updated",
			IdempotencyKey: "organization.projection_updated:" + org + ":" + eventID.String(),
		},
	})
	switch {
	case err == nil:
		s.logger.InfoContext(ctx, "identity: organization projection updated", "organization_id", org)
		return nil
	case errors.Is(err, audit.ErrNoStateChange):
		// A concurrent projection applied the same change first.
		return nil
	case isUniqueViolation(err, organizationsSlugKey):
		return ErrOrganizationSlugCollision
	}
	return s.storeErr(ctx, "update organization", err)
}

func organizationAudit(eventType, reason, org string) audit.AuditEvent {
	return audit.AuditEvent{
		EventType: eventType, Outcome: audit.OutcomeAllowed, OrganizationID: &org,
		Attributes: map[string]any{"reason_code": reason, "target_kind": "organization", "target_id": org},
	}
}
