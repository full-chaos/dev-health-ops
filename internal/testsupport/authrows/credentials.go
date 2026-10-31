package authrows

import (
	"fmt"
	"strings"
)

// AuditMapping is the golden's audit columns for one auth-service security event.
type AuditMapping struct{ Action, ResourceType, Description string }

type credentialAuditEvent struct {
	eventType string
	AuditMapping
}

// credentialAuditEvents are the security events the credentials aggregate writes, in the golden's audit columns.
var credentialAuditEvents = []credentialAuditEvent{
	{"auth.principal.provisioned", AuditMapping{"create", "user", "User registered"}},
	{"auth.password.reset", AuditMapping{"password_reset", "user", "User reset password"}},
	{"auth.user.password_changed", AuditMapping{"password_changed", "user", "User changed own password"}},
	{"auth.password.reset_requested", AuditMapping{"password_reset_requested", "user", "Password reset requested"}},
}

// CredentialAuditEvents is the event type -> golden audit columns mapping of the credentials audit arm.
func CredentialAuditEvents() map[string]AuditMapping {
	out := map[string]AuditMapping{}
	for _, e := range credentialAuditEvents {
		out[e.eventType] = e.AuditMapping
	}
	return out
}

// credentialAuditCase is a CASE over the event type; an event the arm selects and does not map fails the query,
// naming the event, because the integer cast of its type text reports the text.
func credentialAuditCase(column func(AuditMapping) string) string {
	var b strings.Builder
	b.WriteString("CASE e.event_type")
	for _, e := range credentialAuditEvents {
		fmt.Fprintf(&b, " WHEN '%s' THEN '%s'", e.eventType, column(e.AuditMapping))
	}
	b.WriteString(" ELSE e.event_type::int::text END")
	return b.String()
}

// noPublicRowEvents are credentials events go-api has no audit row for: its verify route (internal/api/session
// verify.go) writes none, so the arm projects none (CHAOS-11400).
const noPublicRowEvents = `'auth.email.verified', 'auth.user.email_verified'`

// ignoredPrincipalEvents are the principal events other aggregates write; the golden's audit holds no row for them.
const ignoredPrincipalEvents = `'auth.principal.projected', 'auth.principal.projection_updated'`

func credentialAuditArm() string {
	var types []string
	for _, e := range credentialAuditEvents {
		types = append(types, "'"+e.eventType+"'")
	}
	return `	SELECT coalesce(o.name, e.organization_id::text), coalesce(u.email, '<null>'),
		` + credentialAuditCase(func(m AuditMapping) string { return m.Action }) + `,
		` + credentialAuditCase(func(m AuditMapping) string { return m.ResourceType }) + `,
		'user:' || u.email,
		` + credentialAuditCase(func(m AuditMapping) string { return m.Description }) + `,
		'{}',
		jsonb_strip_nulls(jsonb_build_object('ip_address', host(e.client_ip), 'user_agent', e.user_agent))::text,
		'success', '<null>', e.occurred_at::timestamptz
		FROM auth.security_audit_events e LEFT JOIN organizations o ON o.id = e.organization_id
		LEFT JOIN users u ON u.id = coalesce(e.subject_principal_id, e.actor_principal_id)
		WHERE (e.event_type IN (` + strings.Join(types, ", ") + `)
		   OR e.event_type ~ '^auth\.(user|password|email)\.'
		   OR (e.event_type LIKE 'auth.principal.%' AND e.event_type NOT IN (` + ignoredPrincipalEvents + `)))
		  AND e.event_type NOT IN (` + noPublicRowEvents + `)`
}

const auditLogsTail = "\n) t ORDER BY ts, action"

// auditLogsCredentials is auditLogsACP with the credentials arm.
func auditLogsCredentials() string {
	return strings.TrimSuffix(auditLogsACP, auditLogsTail) + "\n\tUNION ALL\n" + credentialAuditArm() + auditLogsTail
}

// linkTokensACP reads go-api's link tokens and auth-service's unconsumed link credentials of purpose in the shape
// of go-api's own query. go-api deletes a redeemed token, so a consumed credential has no row to compare; the hash
// is the 32 bytes of a SHA-256, read as the 64 hex characters go-api stores.
func linkTokensACP(table, purpose string) string {
	return `SELECT email, grew, live, hash_length, minutes FROM (
	SELECT u.email AS email, t.expires_at - t.created_at > interval '0' AS grew, t.expires_at > now() AS live, length(t.token_hash) AS hash_length,
		extract(epoch FROM t.expires_at - t.created_at)::int / 60 AS minutes, t.expires_at AS ts
		FROM ` + table + ` t JOIN users u ON u.id = t.user_id
	UNION ALL
	SELECT u.email, lc.expires_at - lc.created_at > interval '0', lc.expires_at > now(), octet_length(lc.token_hash) * 2,
		extract(epoch FROM lc.expires_at - lc.created_at)::int / 60, lc.expires_at
		FROM auth.link_credentials lc JOIN users u ON u.id = lc.principal_id
		WHERE lc.purpose = '` + purpose + `' AND lc.consumed_at IS NULL
) t ORDER BY email, ts`
}
