package producttelemetry

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// PGQuerier is the read-only Postgres surface the organization-name read uses;
// *pgxpool.Pool satisfies it.
type PGQuerier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// The organizations table is the tenant list itself: it has no org column to
// scope by, and the platform dashboard is cross-org by design, so the read is
// unscoped and the platform gate is its only guard. A missing slug or name
// reads as empty.
const orgNamesSQL = `SELECT id::text, COALESCE(slug, ''), COALESCE(name, '') FROM organizations`

// LoadOrgNames reads every organization's id, slug and name.
func LoadOrgNames(ctx context.Context, pg PGQuerier) ([]OrgName, error) {
	if pg == nil {
		return nil, fmt.Errorf("producttelemetry: no postgres reader for the organization names")
	}
	rows, err := pg.Query(ctx, orgNamesSQL)
	if err != nil {
		return nil, fmt.Errorf("producttelemetry: organization names query: %w", err)
	}
	defer rows.Close()
	out := []OrgName{}
	for rows.Next() {
		var o OrgName
		if err := rows.Scan(&o.ID, &o.Slug, &o.Name); err != nil {
			return nil, fmt.Errorf("producttelemetry: organization names scan: %w", err)
		}
		out = append(out, o)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("producttelemetry: organization names rows: %w", err)
	}
	return out, nil
}

// Principal is the verified caller as far as the platform gate reads it.
type Principal struct {
	Present             bool
	IsSuperuser         bool
	ImpersonationActive bool
}

// AccessError is a platform-gate refusal.
type AccessError struct{ Message string }

func (e *AccessError) Error() string { return e.Message }

// RequirePlatformAdmin admits only a present principal carrying the superuser
// flag while no impersonation session is active: an impersonated identity is
// never a platform administrator even though the real user is one.
func RequirePlatformAdmin(p Principal) error {
	if !p.Present {
		return &AccessError{Message: "Authentication required"}
	}
	if p.ImpersonationActive || !p.IsSuperuser {
		return &AccessError{Message: "Platform admin access required"}
	}
	return nil
}
