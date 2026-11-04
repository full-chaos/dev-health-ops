// Package publicread is the read side of the versioned read API for
// organizations, memberships and user principals. Its record is go-api's
// public.users, public.organizations and public.memberships: it reads no
// auth-schema copy of them. It reads the auth schema for three things only:
// the aggregate revision counter, and the principals of the service account and
// workload kinds, which have no public row.
package publicread

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

var (
	// ErrNotFound is a row that does not exist (or is not served).
	ErrNotFound = errors.New("publicread: not found")
	// ErrUnknownRole is a public membership role outside the closed catalog.
	ErrUnknownRole = errors.New("publicread: membership role outside the catalog")
	// ErrStore is a failed read.
	ErrStore = errors.New("publicread: store unavailable")
)

// Querier is the pool or connection a Reader reads through.
type Querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Organization is one public.organizations row with its revision (0 = none).
type Organization struct {
	ID        uuid.UUID
	Slug      string
	Name      string
	Active    bool
	CreatedAt time.Time
	UpdatedAt time.Time
	Revision  int64
}

// Membership is one active public.memberships row with its revision (0 = none).
type Membership struct {
	ID          uuid.UUID
	OrgID       uuid.UUID
	PrincipalID uuid.UUID
	Role        string
	JoinedAt    *time.Time
	CreatedAt   time.Time
	UpdatedAt   time.Time
	Revision    int64
}

// OrganizationOf pairs an organization with the membership of the principal asked about.
type OrganizationOf struct {
	Organization Organization
	Membership   Membership
}

// Principal is a principal's id, kind, status and revision, and nothing else.
type Principal struct {
	ID       uuid.UUID
	Kind     string
	Active   bool
	Revision int64
}

// PrincipalOf is a listed principal with its membership of the organization asked about. A user holds one; a service account
// and a workload hold none (their organization binding is their own row, and no membership row exists for them).
type PrincipalOf struct {
	Principal  Principal
	Membership *Membership
}

// Reader reads the public tables.
type Reader struct {
	q        Querier
	auth     string
	logger   *slog.Logger
	badRoles metric.Int64Counter
}

// Config builds a Reader. AuthSchema is the quoted auth schema name.
type Config struct {
	Querier    Querier
	AuthSchema string
	Logger     *slog.Logger
	Meter      metric.Meter
}

// New returns a Reader.
func New(cfg Config) (*Reader, error) {
	if cfg.Querier == nil || cfg.AuthSchema == "" {
		return nil, errors.New("publicread: a querier and the auth schema are required")
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	meter := cfg.Meter
	if meter == nil {
		meter = otel.Meter("github.com/full-chaos/dev-health-ops/internal/auth/publicread")
	}
	badRoles, err := meter.Int64Counter("auth.publicread.unknown_role",
		metric.WithDescription("Reads refused because a public membership row holds a role outside the closed catalog."))
	if err != nil {
		return nil, err
	}
	badRoles.Add(context.Background(), 0)
	return &Reader{q: cfg.Querier, auth: cfg.AuthSchema, logger: logger, badRoles: badRoles}, nil
}

const revisionOf = `LEFT JOIN {A}.aggregate_revisions %s ON %s.aggregate_type = '%s' AND %s.aggregate_id = '%s' || %s::text`

func revisionJoin(alias, aggregate, prefix, id string) string {
	return fmt.Sprintf(revisionOf, alias, alias, aggregate, alias, prefix, id)
}

const (
	orgColumns = `o.id, o.slug, o.name, o.is_active, o.created_at, o.updated_at, COALESCE(ro.revision, 0)`
	memColumns = `m.id, m.org_id, m.user_id, m.role, m.joined_at, m.created_at, m.updated_at, COALESCE(rm.revision, 0)`
)

// statements are the reader's SQL by name. The record is public.*: every ops table is schema-qualified, and the auth schema
// ({A}) is read for the revision counter and for the principals of the service account and workload kinds only. Each text starts
// with a marker the tests read.
var statements = map[string]string{
	"org_get": `/* publicread.org_get */
SELECT ` + orgColumns + `
FROM public.organizations o ` + revisionJoin("ro", "organization", "org_", "o.id") + `
WHERE o.id = $1`,

	"orgs_of": `/* publicread.orgs_of */
SELECT ` + orgColumns + `, m.id, m.role, m.joined_at, m.created_at, m.updated_at, COALESCE(rm.revision, 0)
FROM public.memberships m JOIN public.organizations o ON o.id = m.org_id
` + revisionJoin("ro", "organization", "org_", "o.id") + `
` + revisionJoin("rm", "membership", "mbr_", "m.id") + `
WHERE m.user_id = $1 AND ($2::uuid IS NULL OR o.id > $2)
ORDER BY o.id
LIMIT $3`,

	"membership_get": `/* publicread.membership_get */
SELECT ` + memColumns + `
FROM public.memberships m ` + revisionJoin("rm", "membership", "mbr_", "m.id") + `
WHERE m.org_id = $1 AND m.user_id = $2`,

	"memberships_list": `/* publicread.memberships_list */
SELECT ` + memColumns + `
FROM public.memberships m ` + revisionJoin("rm", "membership", "mbr_", "m.id") + `
WHERE m.org_id = $1 AND ($2::uuid IS NULL OR m.id > $2)
ORDER BY m.id
LIMIT $3`,

	"principal_get_user": `/* publicread.principal_get_user */
SELECT u.id, COALESCE(u.is_active, false), COALESCE(rp.revision, 0)
FROM public.users u ` + revisionJoin("rp", "principal", "prn_", "u.id") + `
WHERE u.id = $1`,

	"principal_get_other": `/* publicread.principal_get_other */
SELECT p.id, p.kind, p.disabled_at IS NULL, COALESCE(rp.revision, 0)
FROM {A}.principals p ` + revisionJoin("rp", "principal", "prn_", "p.id") + `
WHERE p.id = $1 AND p.kind <> 'user'`,

	"principals_list": `/* publicread.principals_list */
SELECT x.id, x.kind, x.active, COALESCE(rp.revision, 0), x.mid, x.role, x.joined_at, x.created_at, x.updated_at, COALESCE(rm.revision, 0)
FROM (
  SELECT u.id AS id, 'user' AS kind, COALESCE(u.is_active, false) AS active,
         m.id AS mid, m.role AS role, m.joined_at AS joined_at, m.created_at AS created_at, m.updated_at AS updated_at
  FROM public.memberships m JOIN public.users u ON u.id = m.user_id
  WHERE m.org_id = $1
  UNION ALL
  SELECT p.id, p.kind, p.disabled_at IS NULL, NULL::uuid, NULL::text, NULL::timestamptz, NULL::timestamptz, NULL::timestamptz
  FROM {A}.service_accounts sa JOIN {A}.principals p ON p.id = sa.principal_id
  WHERE sa.organization_id = $1 AND p.kind = 'service_account'
  UNION ALL
  SELECT p.id, p.kind, p.disabled_at IS NULL, NULL::uuid, NULL::text, NULL::timestamptz, NULL::timestamptz, NULL::timestamptz
  FROM {A}.workloads w JOIN {A}.principals p ON p.id = w.principal_id
  WHERE w.organization_id = $1 AND p.kind = 'workload'
) x ` + revisionJoin("rp", "principal", "prn_", "x.id") + `
` + revisionJoin("rm", "membership", "mbr_", "x.mid") + `
WHERE ($2::text IS NULL OR x.kind = $2) AND ($3::uuid IS NULL OR x.id > $3)
ORDER BY x.id
LIMIT $4`,
}

// Statements returns the reader's SQL by name with the auth schema (quoted) filled in.
func Statements(authSchema string) map[string]string {
	out := make(map[string]string, len(statements))
	for name, sql := range statements {
		out[name] = strings.ReplaceAll(sql, "{A}", authSchema)
	}
	return out
}

func (r *Reader) sql(name string) string { return strings.ReplaceAll(statements[name], "{A}", r.auth) }

func storeErr(what string, err error) error { return fmt.Errorf("%w: %s: %w", ErrStore, what, err) }

var roles = [...]string{"viewer", "member", "admin", "owner"}

// roleOf is the closed catalog check. A role outside it is data the contract cannot carry: the read fails closed, loudly.
func (r *Reader) roleOf(ctx context.Context, org uuid.UUID, text *string) (string, error) {
	if text != nil {
		for _, role := range roles {
			if *text == role {
				return role, nil
			}
		}
	}
	seen := "<null>"
	if text != nil {
		seen = *text
	}
	r.badRoles.Add(ctx, 1)
	r.logger.ErrorContext(ctx, "public membership role outside the catalog: read refused",
		slog.String("organization_id", "org_"+org.String()), slog.String("role", seen))
	return "", ErrUnknownRole
}

func scanOrganization(row pgx.Row, extra ...any) (Organization, error) {
	var o Organization
	err := row.Scan(append([]any{&o.ID, &o.Slug, &o.Name, &o.Active, &o.CreatedAt, &o.UpdatedAt, &o.Revision}, extra...)...)
	return o, err
}

// Organization reads one organization.
func (r *Reader) Organization(ctx context.Context, id uuid.UUID) (Organization, error) {
	o, err := scanOrganization(r.q.QueryRow(ctx, r.sql("org_get"), id))
	if errors.Is(err, pgx.ErrNoRows) {
		return Organization{}, ErrNotFound
	}
	if err != nil {
		return Organization{}, storeErr("organization", err)
	}
	return o, nil
}

// OrganizationsOf lists the organizations a principal holds an active membership in, by organization id after the cursor, one
// page; more is true when another page follows.
func (r *Reader) OrganizationsOf(ctx context.Context, principal uuid.UUID, after *uuid.UUID, limit int) ([]OrganizationOf, bool, error) {
	rows, err := r.q.Query(ctx, r.sql("orgs_of"), principal, after, limit+1)
	if err != nil {
		return nil, false, storeErr("organizations of", err)
	}
	defer rows.Close()
	var out []OrganizationOf
	for rows.Next() {
		var item OrganizationOf
		var role *string
		o, m := &item.Organization, &item.Membership
		if err := rows.Scan(&o.ID, &o.Slug, &o.Name, &o.Active, &o.CreatedAt, &o.UpdatedAt, &o.Revision,
			&m.ID, &role, &m.JoinedAt, &m.CreatedAt, &m.UpdatedAt, &m.Revision); err != nil {
			return nil, false, storeErr("organizations of", err)
		}
		m.OrgID, m.PrincipalID = o.ID, principal
		if m.Role, err = r.roleOf(ctx, o.ID, role); err != nil {
			return nil, false, err
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, false, storeErr("organizations of", err)
	}
	more := len(out) > limit
	if more {
		out = out[:limit]
	}
	return out, more, nil
}

func (r *Reader) scanMembership(ctx context.Context, row pgx.Row) (Membership, error) {
	var m Membership
	var role *string
	if err := row.Scan(&m.ID, &m.OrgID, &m.PrincipalID, &role, &m.JoinedAt, &m.CreatedAt, &m.UpdatedAt, &m.Revision); err != nil {
		return Membership{}, err
	}
	var err error
	m.Role, err = r.roleOf(ctx, m.OrgID, role)
	return m, err
}

// Membership reads the active membership of a principal in an organization.
func (r *Reader) Membership(ctx context.Context, org, principal uuid.UUID) (Membership, error) {
	m, err := r.scanMembership(ctx, r.q.QueryRow(ctx, r.sql("membership_get"), org, principal))
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return Membership{}, ErrNotFound
	case errors.Is(err, ErrUnknownRole):
		return Membership{}, err
	case err != nil:
		return Membership{}, storeErr("membership", err)
	}
	return m, nil
}

// Memberships lists the active memberships of an organization by id after the cursor, one page.
func (r *Reader) Memberships(ctx context.Context, org uuid.UUID, after *uuid.UUID, limit int) ([]Membership, bool, error) {
	rows, err := r.q.Query(ctx, r.sql("memberships_list"), org, after, limit+1)
	if err != nil {
		return nil, false, storeErr("memberships", err)
	}
	defer rows.Close()
	var out []Membership
	for rows.Next() {
		var m Membership
		var role *string
		if err := rows.Scan(&m.ID, &m.OrgID, &m.PrincipalID, &role, &m.JoinedAt, &m.CreatedAt, &m.UpdatedAt, &m.Revision); err != nil {
			return nil, false, storeErr("memberships", err)
		}
		if m.Role, err = r.roleOf(ctx, m.OrgID, role); err != nil {
			return nil, false, err
		}
		out = append(out, m)
	}
	if err := rows.Err(); err != nil {
		return nil, false, storeErr("memberships", err)
	}
	more := len(out) > limit
	if more {
		out = out[:limit]
	}
	return out, more, nil
}

// Principal reads a principal: a user from public.users, else a service account or workload from the auth principals.
func (r *Reader) Principal(ctx context.Context, id uuid.UUID) (Principal, error) {
	p := Principal{Kind: "user"}
	err := r.q.QueryRow(ctx, r.sql("principal_get_user"), id).Scan(&p.ID, &p.Active, &p.Revision)
	if err == nil {
		return p, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return Principal{}, storeErr("principal", err)
	}
	err = r.q.QueryRow(ctx, r.sql("principal_get_other"), id).Scan(&p.ID, &p.Kind, &p.Active, &p.Revision)
	if errors.Is(err, pgx.ErrNoRows) {
		return Principal{}, ErrNotFound
	}
	if err != nil {
		return Principal{}, storeErr("principal", err)
	}
	return p, nil
}

// Principals lists the principals of an organization by id after the cursor, one page: its members from public.users with their
// membership, its service accounts and workloads from the auth principals without one. kind narrows it when not empty.
func (r *Reader) Principals(ctx context.Context, org uuid.UUID, kind string, after *uuid.UUID, limit int) ([]PrincipalOf, bool, error) {
	var kindArg *string
	if kind != "" {
		kindArg = &kind
	}
	rows, err := r.q.Query(ctx, r.sql("principals_list"), org, kindArg, after, limit+1)
	if err != nil {
		return nil, false, storeErr("principals", err)
	}
	defer rows.Close()
	var out []PrincipalOf
	for rows.Next() {
		var (
			p                       Principal
			mid                     *uuid.UUID
			role                    *string
			joined, created, update *time.Time
			memRevision             int64
		)
		if err := rows.Scan(&p.ID, &p.Kind, &p.Active, &p.Revision, &mid, &role, &joined, &created, &update, &memRevision); err != nil {
			return nil, false, storeErr("principals", err)
		}
		item := PrincipalOf{Principal: p}
		if p.Kind == "user" {
			if mid == nil || created == nil || update == nil {
				return nil, false, storeErr("principals", errors.New("a user item without its membership row"))
			}
			m := Membership{ID: *mid, OrgID: org, PrincipalID: p.ID, JoinedAt: joined, CreatedAt: *created, UpdatedAt: *update, Revision: memRevision}
			if m.Role, err = r.roleOf(ctx, org, role); err != nil {
				return nil, false, err
			}
			item.Membership = &m
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, false, storeErr("principals", err)
	}
	more := len(out) > limit
	if more {
		out = out[:limit]
	}
	return out, more, nil
}
