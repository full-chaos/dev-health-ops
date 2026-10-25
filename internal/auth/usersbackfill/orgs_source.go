package usersbackfill

import (
	"context"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// PGOrgSource reads public.organizations and public.memberships with the
// columns the auth runtime role may SELECT (authschema.PublicReadColumns).
type PGOrgSource struct{ Pool *pgxpool.Pool }

// PageOrgs returns up to limit organizations with id greater than after, in id order.
func (s PGOrgSource) PageOrgs(ctx context.Context, after *uuid.UUID, limit int) ([]OrgRow, error) {
	rows, err := s.Pool.Query(ctx, `SELECT id, slug, name, is_active
FROM public.organizations WHERE ($1::uuid IS NULL OR id > $1::uuid) ORDER BY id LIMIT $2`, after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []OrgRow
	for rows.Next() {
		var r OrgRow
		if err := rows.Scan(&r.ID, &r.Slug, &r.Name, &r.Active); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// PageMemberships returns up to limit memberships after the key, in (org, user)
// order. Copies counts the rows that share a key over the whole table.
func (s PGOrgSource) PageMemberships(ctx context.Context, after *MemberKey, limit int) ([]MemberRow, error) {
	var org, user *uuid.UUID
	if after != nil {
		org, user = &after.OrgID, &after.UserID
	}
	rows, err := s.Pool.Query(ctx, `SELECT org_id, user_id, role, joined_at, created_at, copies FROM (
  SELECT org_id, user_id, role, joined_at, created_at, count(*) OVER (PARTITION BY org_id, user_id) AS copies
  FROM public.memberships) m
WHERE ($1::uuid IS NULL OR (org_id, user_id) > ($1::uuid, $2::uuid))
ORDER BY org_id, user_id LIMIT $3`, org, user, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []MemberRow
	for rows.Next() {
		var r MemberRow
		if err := rows.Scan(&r.OrgID, &r.UserID, &r.Role, &r.JoinedAt, &r.CreatedAt, &r.Copies); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// PGAuthOnly counts auth rows that public has no row for.
type PGAuthOnly struct {
	Pool   *pgxpool.Pool
	Schema authschema.ValidatedIdentifier
}

// AuthOnly counts auth organizations and memberships without a public row.
func (a PGAuthOnly) AuthOnly(ctx context.Context) (AuthOnly, error) {
	q := authschema.Quote(a.Schema)
	var out AuthOnly
	err := a.Pool.QueryRow(ctx, `SELECT
  (SELECT count(*) FROM `+q+`.organizations o WHERE NOT EXISTS (SELECT 1 FROM public.organizations p WHERE p.id = o.id)),
  (SELECT count(*) FROM `+q+`.memberships m WHERE NOT EXISTS
     (SELECT 1 FROM public.memberships p WHERE p.org_id = m.organization_id AND p.user_id = m.principal_id))`).
		Scan(&out.Orgs, &out.Memberships)
	return out, err
}
