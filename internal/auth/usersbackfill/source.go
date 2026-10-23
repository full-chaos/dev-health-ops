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
	rows, err := q.Query(ctx, `SELECT id, email, full_name, password_hash, is_active, is_verified, auth_provider, auth_provider_id,
       COALESCE(is_superuser, false), COALESCE(token_version, 0), last_login_at
FROM public.users WHERE ($1::uuid IS NULL OR id > $1::uuid) ORDER BY id LIMIT $2`, after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Row
	for rows.Next() {
		var r Row
		if err := rows.Scan(&r.ID, &r.Email, &r.FullName, &r.PasswordHash, &r.Active, &r.Verified, &r.AuthProvider, &r.AuthProviderID, &r.Superuser, &r.TokenVersion, &r.LastLoginAt); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
