package authschema

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
)

// ErrSchemaOwnerPin reports an auth schema whose objects are not all owned by the pinned owner role, or a pinned
// owner role that holds an attribute a schema owner must not hold.
var ErrSchemaOwnerPin = errors.New("auth schema owner pin")

func (o Options) validateSchemaOwnerRole() error {
	if o.SchemaOwnerRole == "" {
		return nil
	}
	if err := ValidateIdentifier(o.SchemaOwnerRole); err != nil {
		return err
	}
	if o.SchemaOwnerRole == o.RuntimeRole {
		return fmt.Errorf("%w: the pinned schema owner %q must differ from the runtime role", ErrInvalidOptions, o.SchemaOwnerRole)
	}
	for _, reader := range o.SessionReaderRoles {
		if reader == o.SchemaOwnerRole {
			return fmt.Errorf("%w: the pinned schema owner %q must differ from every session reader role", ErrInvalidOptions, o.SchemaOwnerRole)
		}
	}
	return nil
}

// schemaObjectsNotOwnedSQL counts, by kind, the objects of the schema ($1) owned by a role other than $2, except the
// append function ($4) when the push lifecycle writer role ($3) owns it. It reads
// every catalog an owner can differ on: relations, routines, types, collations, statistics, text search objects,
// operators, operator classes and families, conversions and the schema itself. An index, a TOAST table, an array
// type and a table's row type follow their table, so they are not read.
const schemaObjectsNotOwnedSQL = `
SELECT kind, count(*) FROM (
  SELECT CASE c.relkind WHEN 'r' THEN 'table' WHEN 'p' THEN 'table' WHEN 'v' THEN 'view' WHEN 'm' THEN 'materialized view'
                        WHEN 'S' THEN 'sequence' WHEN 'f' THEN 'foreign table' ELSE 'type' END AS kind, c.relowner AS o
  FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
  WHERE n.nspname = $1 AND c.relkind IN ('r','p','v','m','S','f','c')
  UNION ALL
  SELECT CASE p.prokind WHEN 'p' THEN 'procedure' WHEN 'a' THEN 'aggregate' ELSE 'function' END, p.proowner
  FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = $1
    AND NOT (p.proname = $4 AND p.proowner IS NOT DISTINCT FROM (SELECT oid FROM pg_roles WHERE rolname = NULLIF($3, '')))
  UNION ALL
  SELECT CASE t.typtype WHEN 'd' THEN 'domain' ELSE 'type' END, t.typowner
  FROM pg_type t JOIN pg_namespace n ON n.oid = t.typnamespace
  WHERE n.nspname = $1 AND t.typcategory <> 'A' AND t.typtype IN ('d','e','r','b')
  UNION ALL SELECT 'collation', x.collowner FROM pg_collation x JOIN pg_namespace n ON n.oid = x.collnamespace WHERE n.nspname = $1
  UNION ALL SELECT 'statistics', x.stxowner FROM pg_statistic_ext x JOIN pg_namespace n ON n.oid = x.stxnamespace WHERE n.nspname = $1
  UNION ALL SELECT 'text search dictionary', x.dictowner FROM pg_ts_dict x JOIN pg_namespace n ON n.oid = x.dictnamespace WHERE n.nspname = $1
  UNION ALL SELECT 'text search configuration', x.cfgowner FROM pg_ts_config x JOIN pg_namespace n ON n.oid = x.cfgnamespace WHERE n.nspname = $1
  UNION ALL SELECT 'operator', x.oprowner FROM pg_operator x JOIN pg_namespace n ON n.oid = x.oprnamespace WHERE n.nspname = $1
  UNION ALL SELECT 'operator class', x.opcowner FROM pg_opclass x JOIN pg_namespace n ON n.oid = x.opcnamespace WHERE n.nspname = $1
  UNION ALL SELECT 'operator family', x.opfowner FROM pg_opfamily x JOIN pg_namespace n ON n.oid = x.opfnamespace WHERE n.nspname = $1
  UNION ALL SELECT 'conversion', x.conowner FROM pg_conversion x JOIN pg_namespace n ON n.oid = x.connamespace WHERE n.nspname = $1
  UNION ALL SELECT 'schema', n.nspowner FROM pg_namespace n WHERE n.nspname = $1
) objs WHERE o <> (SELECT oid FROM pg_roles WHERE rolname = $2)
GROUP BY kind ORDER BY kind`

// verifySchemaOwnerPin checks the pin of Options.SchemaOwnerRole. It checks the OWNER role, not the connected login,
// so a rollback to the old login still passes while the objects stay with the pinned owner.
func verifySchemaOwnerPin(ctx context.Context, conn *pgx.Conn, options Options) error {
	if options.SchemaOwnerRole == "" {
		return nil
	}
	var super, createRole, createDB, replication, bypass bool
	err := conn.QueryRow(ctx, `SELECT rolsuper, rolcreaterole, rolcreatedb, rolreplication, rolbypassrls FROM pg_roles WHERE rolname = $1`,
		options.SchemaOwnerRole).Scan(&super, &createRole, &createDB, &replication, &bypass)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("%w: the pinned schema owner %q does not exist", ErrSchemaOwnerPin, options.SchemaOwnerRole)
	}
	if err != nil {
		return fmt.Errorf("%w: reading the pinned schema owner", ErrMigrationFailed)
	}
	var attributes []string
	for name, set := range map[string]bool{"SUPERUSER": super, "CREATEROLE": createRole, "CREATEDB": createDB, "REPLICATION": replication, "BYPASSRLS": bypass} {
		if set {
			attributes = append(attributes, name)
		}
	}
	if len(attributes) > 0 {
		sort.Strings(attributes)
		return fmt.Errorf("%w: the pinned schema owner %q holds %s; it must be NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS",
			ErrSchemaOwnerPin, options.SchemaOwnerRole, strings.Join(attributes, ", "))
	}
	rows, err := conn.Query(ctx, schemaObjectsNotOwnedSQL, options.Schema, options.SchemaOwnerRole,
		options.PushLifecycleWriterRole, PushLifecycleFunction)
	if err != nil {
		return fmt.Errorf("%w: reading the owners of the schema's objects", ErrMigrationFailed)
	}
	defer rows.Close()
	var lines []string
	for rows.Next() {
		var kind string
		var n int
		if err := rows.Scan(&kind, &n); err != nil {
			return fmt.Errorf("%w: reading an owner count", ErrMigrationFailed)
		}
		lines = append(lines, fmt.Sprintf("%d %s", n, kind))
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("%w: reading the owners of the schema's objects", ErrMigrationFailed)
	}
	if len(lines) > 0 {
		return fmt.Errorf("%w: schema %q holds objects not owned by %q (%s); run auth-migrate owner-move with the superuser login",
			ErrSchemaOwnerPin, options.Schema, options.SchemaOwnerRole, strings.Join(lines, ", "))
	}
	return nil
}
