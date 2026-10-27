package usersbackfill

import (
	"context"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
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

func pageUsers(ctx context.Context, q querier, after *uuid.UUID, limit int) ([]Row, error) {
	rows, err := q.Query(ctx, `SELECT u.id, u.email, u.full_name, u.password_hash, u.is_active, u.is_verified, u.auth_provider, u.auth_provider_id,
       COALESCE(u.is_superuser, false), COALESCE(u.token_version, 0), u.last_login_at, u.sso_provider_id, sp.protocol,
       u.saml_nameid_format, u.saml_sp_name_qualifier
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
			&r.SAMLNameIDFormat, &r.SAMLSPNameQualifier); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
