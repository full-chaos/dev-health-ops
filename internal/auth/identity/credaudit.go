package identity

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/audit"
)

// RowQuerier is what AuditOrganization reads through: a pool or a transaction.
type RowQuerier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// AuditOrganization is the organization the audit row of a principal's
// credential event names: the one login picks without organization activity
// (authapi chooseMembership): the earliest join, joined_at falling back to
// created_at, ties in the order joined_at, created_at, id; nil when there is no
// membership. A backfill keeps the source joined_at and stamps created_at with
// the projection time, so created_at alone would choose by projection order. It
// reads the auth schema's own tables, which the auth runtime role can select,
// and the audit row's organization column references auth.organizations, so a
// named organization always satisfies it. go-api names the first row of an
// unordered LIMIT 1 read; the order here is fixed, so the same principal always
// gets the same organization. schema is the quoted auth schema name.
func AuditOrganization(ctx context.Context, q RowQuerier, schema string, principal uuid.UUID) (*string, error) {
	var org string
	err := q.QueryRow(ctx, `SELECT o.id::text FROM `+schema+`.memberships m
JOIN `+schema+`.organizations o ON o.id = m.organization_id
WHERE m.principal_id = $1 AND m.status <> 'removed'
ORDER BY COALESCE(m.joined_at, m.created_at), m.joined_at NULLS LAST, m.created_at, m.id LIMIT 1`, principal).Scan(&org)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &org, nil
}

// provisionAudit is a registration's event. The new organization is named in
// changed_fields and not in the organization column: the column references
// auth.organizations, which does not hold the new organization yet. The address
// is not stored in the event, only the names of the fields the registration set.
func provisionAudit(subject string, actor Actor, organizationCreated bool) audit.AuditEvent {
	fields := []string{"email"}
	if organizationCreated {
		fields = append(fields, "organization_id")
	}
	return withChangedFields(localAudit("auth.principal.provisioned", "principal_provisioned", subject, actor), fields...)
}

func verifiedAudit(subject string, actor Actor, org *string) audit.AuditEvent {
	event := withChangedFields(localAudit("auth.user.email_verified", "email_verified", subject, actor), "email_verified_at")
	event.OrganizationID = org
	return event
}

func passwordAudit(cause PasswordCause, subject string, actor Actor, org *string) audit.AuditEvent {
	event := localAudit(cause.eventType, cause.reason, subject, actor)
	event.OrganizationID = org
	return event
}
