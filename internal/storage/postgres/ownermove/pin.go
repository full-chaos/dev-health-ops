package ownermove

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/platform/pgident"
)

// EnvOwnerRole pins the role the ops migrators (migrate, open-units-index, streak-index, river-migrate) run as and
// that owns the database, schema river and every object of public and river. Unset or empty turns the pin off.
const EnvOwnerRole = "MIGRATION_OWNER_ROLE"

// PinVerifiedMessage is the log line of a pin that holds.
const PinVerifiedMessage = "verified ops schema owner pin"

// ErrOwnerPin reports a pin that does not hold; the migrator runs no DDL.
var ErrOwnerPin = errors.New("ops schema owner pin")

// Querier is what VerifyPin reads through: a connection, a pool or a transaction.
type Querier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// PinnedRole reads EnvOwnerRole: "" when the pin is off.
func PinnedRole(lookup func(string) (string, bool)) (string, error) {
	role, _ := lookup(EnvOwnerRole)
	role = strings.TrimSpace(role)
	if role == "" {
		return "", nil
	}
	if pgident.Check(role) != pgident.OK {
		return "", fmt.Errorf("%w: %s is not a plain role name", ErrOwnerPin, EnvOwnerRole)
	}
	return role, nil
}

// VerifyPinFromEnv reads EnvOwnerRole and RIVER_DATABASE_SCHEMA (default river) and, when the pin is on, runs
// VerifyPin. It returns the pinned role, "" when the pin is off.
func VerifyPinFromEnv(ctx context.Context, q Querier, lookup func(string) (string, bool)) (string, error) {
	role, err := PinnedRole(lookup)
	if err != nil || role == "" {
		return role, err
	}
	schema := "river"
	if value, _ := lookup("RIVER_DATABASE_SCHEMA"); strings.TrimSpace(value) != "" {
		schema = strings.TrimSpace(value)
	}
	return role, VerifyPin(ctx, q, role, schema)
}

// notOwnedSQL counts, by kind, the objects of schemas $1 that no extension owns and role $2 does not own: relations,
// routines, types, the other named kinds and the schemas themselves. Schema public owned by pg_database_owner counts
// as owned: it follows the database owner, which VerifyPin checks on its own.
var notOwnedSQL = `
SELECT kind, count(*) FROM (
  SELECT 'relation' AS kind, c.relowner AS o FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = ANY($1) AND c.relkind <> 't' AND ` + notExtensionMember("pg_class", "c.oid") + `
  UNION ALL SELECT 'routine', p.proowner FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
    WHERE n.nspname = ANY($1) AND ` + notExtensionMember("pg_proc", "p.oid") + `
  UNION ALL SELECT 'type', t.typowner FROM pg_type t JOIN pg_namespace n ON n.oid = t.typnamespace
    WHERE n.nspname = ANY($1) AND NOT EXISTS (SELECT 1 FROM pg_depend e WHERE e.deptype = 'e'
      AND ((e.classid = 'pg_type'::regclass AND e.objid IN (t.oid, t.typelem)) OR (e.classid = 'pg_class'::regclass AND e.objid = t.typrelid)))
  UNION ALL SELECT 'collation', x.collowner FROM pg_collation x JOIN pg_namespace n ON n.oid = x.collnamespace
    WHERE n.nspname = ANY($1) AND ` + notExtensionMember("pg_collation", "x.oid") + `
  UNION ALL SELECT 'statistics', x.stxowner FROM pg_statistic_ext x JOIN pg_namespace n ON n.oid = x.stxnamespace WHERE n.nspname = ANY($1)
  UNION ALL SELECT 'text search dictionary', x.dictowner FROM pg_ts_dict x JOIN pg_namespace n ON n.oid = x.dictnamespace
    WHERE n.nspname = ANY($1) AND ` + notExtensionMember("pg_ts_dict", "x.oid") + `
  UNION ALL SELECT 'text search configuration', x.cfgowner FROM pg_ts_config x JOIN pg_namespace n ON n.oid = x.cfgnamespace
    WHERE n.nspname = ANY($1) AND ` + notExtensionMember("pg_ts_config", "x.oid") + `
  UNION ALL SELECT 'operator', x.oprowner FROM pg_operator x JOIN pg_namespace n ON n.oid = x.oprnamespace
    WHERE n.nspname = ANY($1) AND ` + notExtensionMember("pg_operator", "x.oid") + `
  UNION ALL SELECT 'operator class', x.opcowner FROM pg_opclass x JOIN pg_namespace n ON n.oid = x.opcnamespace
    WHERE n.nspname = ANY($1) AND ` + notExtensionMember("pg_opclass", "x.oid") + `
  UNION ALL SELECT 'operator family', x.opfowner FROM pg_opfamily x JOIN pg_namespace n ON n.oid = x.opfnamespace
    WHERE n.nspname = ANY($1) AND ` + notExtensionMember("pg_opfamily", "x.oid") + `
  UNION ALL SELECT 'conversion', x.conowner FROM pg_conversion x JOIN pg_namespace n ON n.oid = x.connamespace
    WHERE n.nspname = ANY($1) AND ` + notExtensionMember("pg_conversion", "x.oid") + `
  UNION ALL SELECT 'schema', n.nspowner FROM pg_namespace n
    WHERE n.nspname = ANY($1) AND n.nspowner <> 'pg_database_owner'::regrole
) objs WHERE o <> (SELECT oid FROM pg_roles WHERE rolname = $2) GROUP BY kind ORDER BY kind`

// VerifyPin checks the pin of role before any DDL: the connected login IS role (so a superuser DSN fails closed),
// role holds no SUPERUSER, CREATEDB, CREATEROLE, REPLICATION or BYPASSRLS, owns the current database, and owns
// riverSchema (when it exists) and every object of public and riverSchema that no extension owns.
func VerifyPin(ctx context.Context, q Querier, role, riverSchema string) error {
	if role == "" {
		return fmt.Errorf("%w: no pinned role", ErrOwnerPin)
	}
	if pgident.Check(riverSchema) != pgident.OK {
		return fmt.Errorf("%w: the River schema name is not a plain identifier", ErrOwnerPin)
	}
	var login string
	var exists, super, createRole, createDB, replication, bypass, ownsDatabase bool
	if err := q.QueryRow(ctx, `SELECT current_user::text,
		EXISTS (SELECT 1 FROM pg_roles WHERE rolname = $1),
		coalesce((SELECT rolsuper FROM pg_roles WHERE rolname = $1), false),
		coalesce((SELECT rolcreaterole FROM pg_roles WHERE rolname = $1), false),
		coalesce((SELECT rolcreatedb FROM pg_roles WHERE rolname = $1), false),
		coalesce((SELECT rolreplication FROM pg_roles WHERE rolname = $1), false),
		coalesce((SELECT rolbypassrls FROM pg_roles WHERE rolname = $1), false),
		EXISTS (SELECT 1 FROM pg_database d JOIN pg_roles r ON r.oid = d.datdba WHERE d.datname = current_database() AND r.rolname = $1)`,
		role).Scan(&login, &exists, &super, &createRole, &createDB, &replication, &bypass, &ownsDatabase); err != nil {
		return fmt.Errorf("%w: reading the pinned role", ErrOwnerPin)
	}
	if !exists {
		return fmt.Errorf("%w: the pinned role %q does not exist", ErrOwnerPin, role)
	}
	if login != role {
		return fmt.Errorf("%w: the migration login is %q, not the pinned role %q; point MIGRATION_DATABASE_URI at the pinned login", ErrOwnerPin, login, role)
	}
	var attributes []string
	for name, set := range map[string]bool{"SUPERUSER": super, "CREATEROLE": createRole, "CREATEDB": createDB, "REPLICATION": replication, "BYPASSRLS": bypass} {
		if set {
			attributes = append(attributes, name)
		}
	}
	if len(attributes) > 0 {
		sort.Strings(attributes)
		return fmt.Errorf("%w: the pinned role %q holds %s; it must be NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS",
			ErrOwnerPin, role, strings.Join(attributes, ", "))
	}
	if !ownsDatabase {
		return fmt.Errorf("%w: the pinned role %q does not own the database; run dho migrate owner-move -database-owner with the superuser login", ErrOwnerPin, role)
	}
	rows, err := q.Query(ctx, notOwnedSQL, []string{"public", riverSchema}, role)
	if err != nil {
		return fmt.Errorf("%w: reading the owners of the ops schemas", ErrOwnerPin)
	}
	defer rows.Close()
	var lines []string
	for rows.Next() {
		var kind string
		var n int
		if err := rows.Scan(&kind, &n); err != nil {
			return fmt.Errorf("%w: reading an owner count", ErrOwnerPin)
		}
		lines = append(lines, fmt.Sprintf("%d %s", n, kind))
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("%w: reading the owners of the ops schemas", ErrOwnerPin)
	}
	if len(lines) > 0 {
		return fmt.Errorf("%w: public and %s hold objects not owned by %q (%s); run dho migrate owner-move with the superuser login",
			ErrOwnerPin, riverSchema, role, strings.Join(lines, ", "))
	}
	return nil
}
