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
// credential event names: the principal's earliest membership by (created_at,
// id), or nil when there is none or that organization is not in the auth schema
// yet (the audit row's organization column references it; login projects an
// organization before it names one). go-api names the first row of an unordered
// LIMIT 1 read; the order here is fixed, so the same principal always gets the
// same organization. schema is the quoted auth schema name.
func AuditOrganization(ctx context.Context, q RowQuerier, schema string, principal uuid.UUID) (*string, error) {
	var org *string
	err := q.QueryRow(ctx, `SELECT o.id::text FROM (SELECT org_id FROM public.memberships WHERE user_id = $1 ORDER BY created_at, id LIMIT 1) m
LEFT JOIN `+schema+`.organizations o ON o.id = m.org_id`, principal).Scan(&org)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	return org, err
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
