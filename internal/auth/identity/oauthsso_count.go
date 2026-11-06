package identity

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

// OAuthSSOCount is the pre-flip count of the users-owned runbook (CHAOS-11576). Counts only: no id, e-mail or
// organization. Total: users with an active OAuth provider row of their type in an organization they belong to.
// Keyed: the user's one row has its key (issuer: the social issuer, or for GitLab the instance host of the row's
// base_url). Missing: that row has no key. Ambiguous: the user has two or more candidate rows, so none is carried. A
// user with no candidate row is a plain social user and is not counted.
type OAuthSSOCount struct {
	Total, Keyed, Missing, Ambiguous int64
}

// Line renders the count as one stable line.
func (c OAuthSSOCount) Line() string {
	return fmt.Sprintf("oauth_sso total=%d keyed=%d missing=%d ambiguous=%d", c.Total, c.Keyed, c.Missing, c.Ambiguous)
}

func sqlLiteral(v string) string { return "'" + strings.ReplaceAll(v, "'", "''") + "'" }

// oauthSSOWithSQL is the candidate and flag CTEs over the quoted auth schema. The issuer of a row is the fixed social
// issuer of its type; a GitLab row's is its instance issuer (GitLabInstanceIssuerSQL, the twin of OAuthIssuer), NULL
// when the base_url names no host, so such a row is never keyed and counts as missing.
func oauthSSOWithSQL(quotedSchema string) string {
	gh, _ := SocialIssuer(ProviderGitHub)
	gg, _ := SocialIssuer(ProviderGoogle)
	return `WITH cand AS (
  SELECT u.id AS uid, u.auth_provider_id AS sub, sp.id AS spid,
         count(*) OVER (PARTITION BY u.id) AS n,
         CASE sp.protocol WHEN 'oauth_github' THEN ` + sqlLiteral(gh) + `::text WHEN 'oauth_gitlab' THEN ` + GitLabInstanceIssuerSQL("sp.config::jsonb->>'base_url'") +
		` WHEN 'oauth_google' THEN ` + sqlLiteral(gg) + `::text END AS iss
  FROM public.users u
  JOIN public.memberships m ON m.user_id = u.id
  JOIN public.sso_providers sp ON sp.org_id = m.org_id AND sp.status = 'active' AND sp.protocol = 'oauth_' || u.auth_provider
  WHERE u.auth_provider IN ('github', 'gitlab', 'google') AND coalesce(u.auth_provider_id, '') <> ''
), flag AS (
  SELECT c.uid,
         bool_or(c.n > 1) AS ambiguous,
         bool_or(c.n = 1) AS social,
         bool_or(c.n = 1 AND e.id IS NULL) AS missing
  FROM cand c
  LEFT JOIN ` + quotedSchema + `.external_identities e
    ON e.principal_id = c.uid AND e.issuer = c.iss AND e.provider = c.spid::text AND e.subject = c.sub
  GROUP BY c.uid
)`
}

// OAuthSSOCountSQL is the read-only count statement: one row, four counts.
func OAuthSSOCountSQL(quotedSchema string) string {
	return oauthSSOWithSQL(quotedSchema) + `
SELECT count(*), count(*) FILTER (WHERE social AND NOT missing), count(*) FILTER (WHERE missing), count(*) FILTER (WHERE ambiguous) FROM flag`
}

// OAuthSSOMissingKeySubquery is a scalar subquery: the Missing count, for the
// users-owned guard.
func OAuthSSOMissingKeySubquery(quotedSchema string) string {
	return "(" + oauthSSOWithSQL(quotedSchema) + ` SELECT count(*) FROM flag WHERE missing)`
}

// OAuthSSOAmbiguousSubquery is a scalar subquery: the Ambiguous count, for the
// users-owned guard.
func OAuthSSOAmbiguousSubquery(quotedSchema string) string {
	return "(" + oauthSSOWithSQL(quotedSchema) + ` SELECT count(*) FROM flag WHERE ambiguous)`
}

// OAuthSSOReadColumns lists the public columns the count and the carry read.
func OAuthSSOReadColumns() map[string][]string {
	return map[string][]string{
		"users":         {"id", "auth_provider", "auth_provider_id"},
		"memberships":   {"user_id", "org_id"},
		"sso_providers": {"id", "org_id", "protocol", "status", "config"},
	}
}

type countQuerier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// ReadOAuthSSOCount runs OAuthSSOCountSQL.
func ReadOAuthSSOCount(ctx context.Context, db countQuerier, quotedSchema string) (OAuthSSOCount, error) {
	var c OAuthSSOCount
	if err := db.QueryRow(ctx, OAuthSSOCountSQL(quotedSchema)).Scan(&c.Total, &c.Keyed, &c.Missing, &c.Ambiguous); err != nil {
		return OAuthSSOCount{}, errors.New("identity: reading the oauth sso count failed")
	}
	return c, nil
}
