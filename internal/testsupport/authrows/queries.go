package authrows

import (
	"fmt"
	"strings"
)

// The corpus's row comparisons, by the name each has in the golden.
const (
	TableUsers                   = "users"
	TableOrganizations           = "organizations"
	TableMemberships             = "memberships"
	TableEmailVerificationTokens = "email_verification_tokens"
	TablePasswordResetTokens     = "password_reset_tokens"
	TableOrgInvites              = "org_invites"
	TableRefreshTokens           = "refresh_tokens"
	TableAuditLogs               = "audit_logs"
)

// uuidScrub is the corpus's own rendering of a generated id inside stored JSON.
const uuidScrub = `[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}`

// lastLoginMarker is the one place the corpus's users query reads a last login. The ACP projection widens it, and
// refuses a query that does not hold it exactly once.
const lastLoginMarker = "last_login_at IS NOT NULL"

// lastLoginProjected is a last login stamped on go-api's row or on the principal's auth.users row.
const lastLoginProjected = `(last_login_at IS NOT NULL OR EXISTS (SELECT 1 FROM auth.users al WHERE al.principal_id = users.id AND al.last_login_at IS NOT NULL))`

// refreshTokensACP reads go-api's refresh tokens and auth-service's refresh credentials (one per issued
// credential, through the session that holds its principal and organization) in the shape of go-api's own
// query: email, organization slug, revoked, count. A credential is revoked once consumed, revoked, or when its
// session is revoked.
const refreshTokensACP = `SELECT email, org, revoked, count(*) FROM (
	SELECT u.email AS email, coalesce(o.slug, '<null>') AS org, rt.revoked_at IS NOT NULL AS revoked
		FROM refresh_tokens rt JOIN users u ON u.id = rt.user_id LEFT JOIN organizations o ON o.id = rt.org_id
	UNION ALL
	SELECT u.email, coalesce(o.slug, '<null>'), (rc.consumed_at IS NOT NULL OR rc.revoked_at IS NOT NULL OR s.revoked_at IS NOT NULL)
		FROM auth.refresh_credentials rc JOIN auth.sessions s ON s.id = rc.session_id
		JOIN users u ON u.id = s.principal_id LEFT JOIN organizations o ON o.id = s.organization_id
) t GROUP BY 1, 2, 3 ORDER BY 1, 2, 3`

// auditLogsACP reads public.audit_logs in the shape of go-api's own query, ordered by time. auth-service writes
// go-api's row of every route it took over there itself (CHAOS-11400), so no auth.* event is projected into it;
// request_metadata's request_id is left out, since the go-api proxy always forwards one and the golden's requests
// sent none.
const auditLogsACP = `SELECT org, email, action, rtype, resource, description, changes, metadata, status, errmsg FROM (
	SELECT coalesce(o.name, a.org_id::text) AS org, coalesce(u.email, '<null>') AS email, a.action::text AS action,
		a.resource_type::text AS rtype,
		coalesce((SELECT 'user:' || email FROM users WHERE id::text = a.resource_id),
		         (SELECT 'org:' || name FROM organizations WHERE id::text = a.resource_id),
		         (SELECT 'membership:' || mu.email || '@' || mo.name FROM memberships mm JOIN users mu ON mu.id = mm.user_id
		            JOIN organizations mo ON mo.id = mm.org_id WHERE mm.id::text = a.resource_id), a.resource_id) AS resource,
		a.description AS description,
		coalesce(regexp_replace(a.changes::text, '` + uuidScrub + `', '<uuid>', 'g'), '<null>') AS changes,
		coalesce((a.request_metadata::jsonb - 'request_id')::text, '<null>') AS metadata, a.status::text AS status,
		coalesce(a.error_message, '<null>') AS errmsg, a.created_at::timestamptz AS ts
		FROM audit_logs a LEFT JOIN organizations o ON o.id = a.org_id LEFT JOIN users u ON u.id = a.user_id
) t ORDER BY ts, action`

// publicOnly are the corpus's row comparisons no aggregate of auth-service writes: go-api writes them whichever
// aggregate is forwarded.
var publicOnly = map[string]bool{TableOrganizations: true, TableMemberships: true, TableOrgInvites: true}

// linkCredentialPurposes are the comparisons written to auth.link_credentials when the credentials aggregate is
// forwarded, with the purpose of the credential.
var linkCredentialPurposes = map[string]string{TableEmailVerificationTokens: "email_verification", TablePasswordResetTokens: "password_reset"}

// projected are the comparisons an auth-service sign-in changes.
var projected = map[string]bool{TableUsers: true, TableRefreshTokens: true, TableAuditLogs: true}

// unprojectedAggregates are the forwarded aggregates whose rows have no projection yet. A run that forwards one
// fails on the first table it could change: nothing is compared against the wrong rows and nothing is skipped.
var unprojectedAggregates = []Aggregate{OIDC, SAML}

// Query is the SQL the corpus runs for table's row comparison when f is forwarded, and whether it differs from
// public (the query go-api's own rows are read with). With nothing forwarded it is public, byte for byte. An
// unknown table, or a table an unprojected aggregate could change, is an error.
func Query(table, public string, f Forwarded) (query string, isProjected bool, err error) {
	if !f.Any() {
		return public, false, nil
	}
	known := projected[table] || publicOnly[table] || linkCredentialPurposes[table] != ""
	if !known {
		return "", false, fmt.Errorf("authrows: no row comparison named %q", table)
	}
	if purpose := linkCredentialPurposes[table]; purpose != "" && f.Has(Credentials) {
		return linkTokensACP(table, purpose), true, nil
	}
	if projected[table] {
		for _, a := range unprojectedAggregates {
			if f.Has(a) {
				return "", false, fmt.Errorf("authrows: aggregate %s is forwarded and %s has no projection for it; forwarded: %s", a, table, f)
			}
		}
	}
	switch table {
	case TableUsers:
		if strings.Count(public, lastLoginMarker) != 1 {
			return "", false, fmt.Errorf("authrows: the users query holds %q %d times, want exactly once", lastLoginMarker, strings.Count(public, lastLoginMarker))
		}
		return strings.Replace(public, lastLoginMarker, lastLoginProjected, 1), true, nil
	case TableRefreshTokens:
		return refreshTokensACP, true, nil
	case TableAuditLogs:
		if f.Has(Credentials) {
			return auditLogsCredentials(), true, nil
		}
		return auditLogsACP, true, nil
	}
	return public, false, nil
}
