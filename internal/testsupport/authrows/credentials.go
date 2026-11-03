package authrows

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

// CredentialAuditEvents is the event type -> golden audit columns mapping: the go-api row each credentials event goes
// with. auth-service writes that row itself in the event's transaction (CHAOS-11400); the comparison reads it there.
func CredentialAuditEvents() map[string]AuditMapping {
	out := map[string]AuditMapping{}
	for _, e := range credentialAuditEvents {
		out[e.eventType] = e.AuditMapping
	}
	return out
}

// auditLogsCredentials is the audit comparison with the credentials aggregate forwarded: auth-service writes go-api's
// public.audit_logs row itself (CHAOS-11400), so the comparison reads that table alone, as with no aggregate
// forwarded. An auth.* event projected into it as well would double each row.
func auditLogsCredentials() string { return auditLogsACP }

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
