package usersbackfill

import (
	"context"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
)

// querier is what a page read needs: a pool, or a transaction.
type querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// PGSource reads public.users with the columns the auth runtime role may
// SELECT (authschema.PublicReadColumns).
type PGSource struct{ Pool *pgxpool.Pool }

// Page returns up to limit users with id greater than after (all users when after is nil), in id order.
func (s PGSource) Page(ctx context.Context, after *uuid.UUID, limit int) ([]Row, error) {
	return pageUsers(ctx, s.Pool, after, limit)
}

// selfHostedGitLabSQL is Row.SelfHostedGitLab over the users row u and its bound provider row sp. The host of an
// instance is read from the provider config (identity.GitLabInstanceIssuerSQL), never from the users row.
func selfHostedGitLabSQL() string {
	social, _ := identity.SocialIssuer(identity.ProviderGitLab)
	notSocial := func(row string) string {
		return identity.GitLabInstanceIssuerSQL(row+".config::jsonb->>'base_url'") + ` IS DISTINCT FROM '` + social + `'::text`
	}
	return `(u.auth_provider = 'gitlab' AND ((sp.protocol = 'oauth_gitlab' AND ` + notSocial("sp") + `)
  OR (u.sso_provider_id IS NULL AND coalesce((SELECT count(DISTINCT c.id) = 1 AND bool_and(` + notSocial("c") + `)
    FROM public.memberships m JOIN public.sso_providers c ON c.org_id = m.org_id AND c.status = 'active' AND c.protocol = 'oauth_gitlab'
    WHERE m.user_id = u.id), false))))`
}

func pageUsers(ctx context.Context, q querier, after *uuid.UUID, limit int) ([]Row, error) {
	rows, err := q.Query(ctx, `SELECT u.id, u.email, u.full_name, u.password_hash, u.is_active, u.is_verified, u.auth_provider, u.auth_provider_id,
       COALESCE(u.is_superuser, false), COALESCE(u.token_version, 0), u.last_login_at, u.sso_provider_id, sp.protocol,
       u.saml_nameid_format, u.saml_sp_name_qualifier, `+selfHostedGitLabSQL()+`
FROM public.users u LEFT JOIN public.sso_providers sp ON sp.id = u.sso_provider_id
WHERE ($1::uuid IS NULL OR u.id > $1::uuid) ORDER BY u.id LIMIT $2`, after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Row
	for rows.Next() {
		var r Row
		if err := rows.Scan(&r.ID, &r.Email, &r.FullName, &r.PasswordHash, &r.Active, &r.Verified, &r.AuthProvider, &r.AuthProviderID, &r.Superuser, &r.TokenVersion, &r.LastLoginAt, &r.SSOProviderID, &r.SSOProviderProtocol,
			&r.SAMLNameIDFormat, &r.SAMLSPNameQualifier, &r.SelfHostedGitLab); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
