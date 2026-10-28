package identity

import "github.com/full-chaos/dev-health-ops/internal/audit"

func provisionAudit(subject string, actor Actor, org *string) audit.AuditEvent {
	return localAudit("auth.principal.provisioned", "principal_provisioned", subject, actor)
}

func verifiedAudit(subject string, actor Actor, org *string) audit.AuditEvent {
	return withChangedFields(localAudit("auth.user.email_verified", "email_verified", subject, actor), "email_verified_at")
}

func passwordAudit(cause PasswordCause, subject string, actor Actor, org *string) audit.AuditEvent {
	return localAudit(cause.eventType, cause.reason, subject, actor)
}
