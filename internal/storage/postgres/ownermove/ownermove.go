// Package ownermove moves the ops schemas (public, river) and the database from one owner role to another, and pins
// the owner the ops migrators may run as (MIGRATION_OWNER_ROLE). The auth schema has its own verb
// (authschema.OwnerMove); this one never touches it.
//
// ALTER ... OWNER rewrites every ACL entry whose grantor or grantee is the old owner to the new owner in the same
// statement, on relations, columns, routines, types, schemas and the database: the runtime grants keep their privilege
// sets and only their grantor changes, so the move needs no re-grant pass.
package ownermove

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/full-chaos/dev-health-ops/internal/platform/pgident"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/aclguard"
)

const (
	// PostgresWalkLockKey is the transaction lock `dho migrate postgres` takes for its walk (pgmigrate).
	PostgresWalkLockKey int64 = 0x64686f5f7067
	// RiverMigrationLockKey is the session lock `dho migrate river` holds while it applies (riverstore).
	RiverMigrationLockKey int64 = 30330037
	// DefaultLockTimeout bounds every lock wait of the move transaction.
	DefaultLockTimeout = 5 * time.Second
)

var (
	// ErrOwnerMove reports a move this package refuses or could not finish; nothing was changed.
	ErrOwnerMove = errors.New("ops owner move")
	// ErrLockTimeout reports a lock wait that passed the lock timeout; the transaction was rolled back.
	ErrLockTimeout = errors.New("ops owner move: lock timeout")
)

// Mode is how far a run goes.
type Mode int

const (
	// DryRun runs every ALTER in a transaction and rolls it back. It takes the locks the apply takes.
	DryRun Mode = iota
	// Plan only reads the catalog, in a READ ONLY transaction: no ALTER and no lock beyond catalog reads.
	Plan
	// Apply runs every ALTER and commits.
	Apply
)

func (m Mode) String() string {
	switch m {
	case Plan:
		return "plan"
	case Apply:
		return "apply"
	}
	return "dry-run"
}

// Options configures one run.
type Options struct {
	// Schemas are the schemas whose objects move (public, river).
	Schemas []string
	// DatabaseOwner also moves the current database; schema public, owned by pg_database_owner, follows it.
	DatabaseOwner bool
	// From owns the objects now; To owns them afterwards.
	From, To string
	Mode     Mode
	// Restore is the rollback: To may be any role (the superuser that owned the objects before).
	Restore bool
	// LockTimeout bounds each lock wait; zero means DefaultLockTimeout.
	LockTimeout time.Duration

	hookBeforeCommit func(context.Context, pgx.Tx)
}

// DefaultACL is one pg_default_acl row of From: Schema is "*" for a database-wide row.
type DefaultACL struct {
	Schema, ObjectType, ACL string
}

// Report counts what a run changed or, in DryRun and Plan, would change.
type Report struct {
	Mode          Mode
	Applied       bool
	Schemas       []string
	DatabaseOwner bool
	// Moved counts, by kind, the objects that take their own ALTER ... OWNER (schema and database included).
	Moved map[string]int
	// Followed counts the objects that change owner with their table (indexes, owned sequences).
	Followed map[string]int
	// ExtensionSkipped counts objects of From that an extension owns: they stay with From.
	ExtensionSkipped int
	// GrantorRows counts the stored ACL entries with grantor From that the move gives the grantor To.
	GrantorRows int
	// OldGrantorRowsLeft counts what is still From's after the ALTERs (objects, schema, database, ACL grantor rows);
	// -1 in Plan, which runs no ALTER.
	OldGrantorRowsLeft int
	// DefaultACLs are From's default privilege rows in the moved schemas and database-wide: ALTER OWNER does not
	// touch them.
	DefaultACLs []DefaultACL
}

// Line is the one output line of a run.
func (r Report) Line() string {
	left := "n/a"
	if r.Mode != Plan {
		left = strconv.Itoa(r.OldGrantorRowsLeft)
	}
	acls := make([]string, 0, len(r.DefaultACLs))
	for _, d := range r.DefaultACLs {
		acls = append(acls, d.Schema+"/"+d.ObjectType+":"+d.ACL)
	}
	return fmt.Sprintf("owner-move mode=%s applied=%t schemas=[%s] database_owner=%t moved=%s followed=%s extension_skipped=%d grantor_rows=%d old_grantor_rows_left=%s default_acl=[%s]",
		r.Mode, r.Applied, strings.Join(r.Schemas, ","), r.DatabaseOwner, countList(r.Moved), countList(r.Followed),
		r.ExtensionSkipped, r.GrantorRows, left, strings.Join(acls, ";"))
}

func countList(counts map[string]int) string {
	keys := make([]string, 0, len(counts))
	for kind := range counts {
		keys = append(keys, kind)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, kind := range keys {
		parts = append(parts, strings.ReplaceAll(kind, " ", "_")+":"+strconv.Itoa(counts[kind]))
	}
	return "[" + strings.Join(parts, ",") + "]"
}

// Validate checks the options without touching the database.
func (o Options) Validate() error {
	if len(o.Schemas) == 0 {
		return fmt.Errorf("%w: at least one schema is required", ErrOwnerMove)
	}
	seen := map[string]bool{}
	for _, schema := range o.Schemas {
		if err := checkIdentifier("schema", schema); err != nil {
			return err
		}
		if seen[schema] {
			return fmt.Errorf("%w: schema %q is named twice", ErrOwnerMove, schema)
		}
		seen[schema] = true
	}
	for name, value := range map[string]string{"from": o.From, "to": o.To} {
		if err := checkIdentifier(name, value); err != nil {
			return err
		}
	}
	if o.From == o.To {
		return fmt.Errorf("%w: from and to are the same role", ErrOwnerMove)
	}
	if o.Mode != DryRun && o.Mode != Plan && o.Mode != Apply {
		return fmt.Errorf("%w: unknown mode", ErrOwnerMove)
	}
	if o.LockTimeout < 0 || (o.LockTimeout > 0 && o.LockTimeout < time.Millisecond) {
		return fmt.Errorf("%w: lock timeout %s must be zero (the default) or at least 1ms", ErrOwnerMove, o.LockTimeout)
	}
	return nil
}

func checkIdentifier(name, value string) error {
	if value == "" {
		return fmt.Errorf("%w: %s is required", ErrOwnerMove, name)
	}
	if pgident.Check(value) != pgident.OK {
		return fmt.Errorf("%w: %s %q is not a plain identifier ([a-z][a-z0-9_]*, not reserved)", ErrOwnerMove, name, value)
	}
	return nil
}

func quote(name string) string { return pgx.Identifier{name}.Sanitize() }

// notExtensionMember is true for a row of catalog (its oid column oid) that no extension owns.
func notExtensionMember(catalog, oid string) string {
	return "NOT EXISTS (SELECT 1 FROM pg_depend e WHERE e.classid = '" + catalog + "'::regclass AND e.objid = " + oid + " AND e.deptype = 'e')"
}

// objectsSQL lists, for the schema ($1), the objects owned by role $2 that need their own ALTER ... OWNER, with the
// identity quoted by the server and whether an extension owns the object. Indexes, TOAST tables, array and row
// types, and sequences linked to a column follow their owner and are not listed (followedSQL counts them).
var objectsSQL = `
SELECT kind, ident, ext FROM (
  SELECT CASE c.relkind WHEN 'r' THEN 'table' WHEN 'p' THEN 'table' WHEN 'v' THEN 'view' WHEN 'm' THEN 'materialized view'
                        WHEN 'S' THEN 'sequence' WHEN 'f' THEN 'foreign table' ELSE 'type' END AS kind,
         quote_ident(n.nspname) || '.' || quote_ident(c.relname) AS ident, c.relowner AS o,
         NOT ` + notExtensionMember("pg_class", "c.oid") + ` AS ext
  FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
  WHERE n.nspname = $1 AND c.relkind IN ('r','p','v','m','S','f','c')
    AND NOT (c.relkind = 'S' AND EXISTS (SELECT 1 FROM pg_depend d WHERE d.classid = 'pg_class'::regclass AND d.objid = c.oid
             AND d.refclassid = 'pg_class'::regclass AND d.deptype IN ('a','i')))
  UNION ALL
  SELECT CASE p.prokind WHEN 'p' THEN 'procedure' WHEN 'a' THEN 'aggregate' ELSE 'function' END,
         quote_ident(n.nspname) || '.' || quote_ident(p.proname) || '(' || pg_get_function_identity_arguments(p.oid) || ')', p.proowner,
         NOT ` + notExtensionMember("pg_proc", "p.oid") + `
  FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = $1
  UNION ALL
  SELECT CASE t.typtype WHEN 'd' THEN 'domain' ELSE 'type' END, quote_ident(n.nspname) || '.' || quote_ident(t.typname), t.typowner,
         NOT ` + notExtensionMember("pg_type", "t.oid") + `
  FROM pg_type t JOIN pg_namespace n ON n.oid = t.typnamespace
  WHERE n.nspname = $1 AND t.typcategory <> 'A' AND t.typtype IN ('d','e','r','b')
  UNION ALL SELECT 'collation', quote_ident(n.nspname) || '.' || quote_ident(x.collname), x.collowner, NOT ` + notExtensionMember("pg_collation", "x.oid") + `
    FROM pg_collation x JOIN pg_namespace n ON n.oid = x.collnamespace WHERE n.nspname = $1
  UNION ALL SELECT 'statistics', quote_ident(n.nspname) || '.' || quote_ident(x.stxname), x.stxowner, NOT ` + notExtensionMember("pg_statistic_ext", "x.oid") + `
    FROM pg_statistic_ext x JOIN pg_namespace n ON n.oid = x.stxnamespace WHERE n.nspname = $1
  UNION ALL SELECT 'text search dictionary', quote_ident(n.nspname) || '.' || quote_ident(x.dictname), x.dictowner, NOT ` + notExtensionMember("pg_ts_dict", "x.oid") + `
    FROM pg_ts_dict x JOIN pg_namespace n ON n.oid = x.dictnamespace WHERE n.nspname = $1
  UNION ALL SELECT 'text search configuration', quote_ident(n.nspname) || '.' || quote_ident(x.cfgname), x.cfgowner, NOT ` + notExtensionMember("pg_ts_config", "x.oid") + `
    FROM pg_ts_config x JOIN pg_namespace n ON n.oid = x.cfgnamespace WHERE n.nspname = $1
  UNION ALL SELECT 'operator', quote_ident(n.nspname) || '.' || quote_ident(x.oprname), x.oprowner, NOT ` + notExtensionMember("pg_operator", "x.oid") + `
    FROM pg_operator x JOIN pg_namespace n ON n.oid = x.oprnamespace WHERE n.nspname = $1
  UNION ALL SELECT 'operator class', quote_ident(n.nspname) || '.' || quote_ident(x.opcname), x.opcowner, NOT ` + notExtensionMember("pg_opclass", "x.oid") + `
    FROM pg_opclass x JOIN pg_namespace n ON n.oid = x.opcnamespace WHERE n.nspname = $1
  UNION ALL SELECT 'operator family', quote_ident(n.nspname) || '.' || quote_ident(x.opfname), x.opfowner, NOT ` + notExtensionMember("pg_opfamily", "x.oid") + `
    FROM pg_opfamily x JOIN pg_namespace n ON n.oid = x.opfnamespace WHERE n.nspname = $1
  UNION ALL SELECT 'conversion', quote_ident(n.nspname) || '.' || quote_ident(x.conname), x.conowner, NOT ` + notExtensionMember("pg_conversion", "x.oid") + `
    FROM pg_conversion x JOIN pg_namespace n ON n.oid = x.connamespace WHERE n.nspname = $1
) objs WHERE o = (SELECT oid FROM pg_roles WHERE rolname = $2) ORDER BY kind, ident`

// followedSQL counts the objects of the schema ($1) owned by $2 that change owner with their table.
var followedSQL = `
SELECT CASE WHEN c.relkind IN ('i','I') THEN 'index' ELSE 'owned sequence' END, count(*)
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = $1 AND c.relowner = (SELECT oid FROM pg_roles WHERE rolname = $2)
  AND ` + notExtensionMember("pg_class", "c.oid") + `
  AND (c.relkind IN ('i','I') OR (c.relkind = 'S' AND EXISTS (SELECT 1 FROM pg_depend d WHERE d.classid = 'pg_class'::regclass
       AND d.objid = c.oid AND d.refclassid = 'pg_class'::regclass AND d.deptype IN ('a','i'))))
GROUP BY 1 ORDER BY 1`

// grantorRowsSQL counts the stored ACL entries with grantor $2 on objects of the schemas ($1) that no extension owns,
// plus the schemas' own ACLs, plus the current database's when $3.
var grantorRowsSQL = `
SELECT
  (SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace CROSS JOIN LATERAL aclexplode(c.relacl) a
     WHERE n.nspname = ANY($1) AND c.relacl IS NOT NULL AND ` + notExtensionMember("pg_class", "c.oid") + ` AND a.grantor = r.oid)
+ (SELECT count(*) FROM pg_attribute att JOIN pg_class c ON c.oid = att.attrelid JOIN pg_namespace n ON n.oid = c.relnamespace
     CROSS JOIN LATERAL aclexplode(att.attacl) a
     WHERE n.nspname = ANY($1) AND att.attacl IS NOT NULL AND att.attnum > 0 AND ` + notExtensionMember("pg_class", "c.oid") + ` AND a.grantor = r.oid)
+ (SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace CROSS JOIN LATERAL aclexplode(p.proacl) a
     WHERE n.nspname = ANY($1) AND p.proacl IS NOT NULL AND ` + notExtensionMember("pg_proc", "p.oid") + ` AND a.grantor = r.oid)
+ (SELECT count(*) FROM pg_type t JOIN pg_namespace n ON n.oid = t.typnamespace CROSS JOIN LATERAL aclexplode(t.typacl) a
     WHERE n.nspname = ANY($1) AND t.typacl IS NOT NULL AND ` + notExtensionMember("pg_type", "t.oid") + ` AND a.grantor = r.oid)
+ (SELECT count(*) FROM pg_namespace n CROSS JOIN LATERAL aclexplode(n.nspacl) a
     WHERE n.nspname = ANY($1) AND n.nspacl IS NOT NULL AND a.grantor = r.oid)
+ (SELECT count(*) FROM pg_database d CROSS JOIN LATERAL aclexplode(d.datacl) a
     WHERE $3 AND d.datname = current_database() AND d.datacl IS NOT NULL AND a.grantor = r.oid)
FROM pg_roles r WHERE r.rolname = $2`

// ownedLeftSQL counts what role $2 still owns in the schemas ($1) and no extension owns: relations (indexes and owned
// sequences included), routines, types, the other named kinds and the schemas themselves; plus the database when $3.
var ownedLeftSQL = `
SELECT
  (SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
     WHERE n.nspname = ANY($1) AND c.relowner = r.oid AND c.relkind <> 't' AND ` + notExtensionMember("pg_class", "c.oid") + `)
+ (SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
     WHERE n.nspname = ANY($1) AND p.proowner = r.oid AND ` + notExtensionMember("pg_proc", "p.oid") + `)
+ (SELECT count(*) FROM pg_type t JOIN pg_namespace n ON n.oid = t.typnamespace
     WHERE n.nspname = ANY($1) AND t.typowner = r.oid AND NOT EXISTS (SELECT 1 FROM pg_depend e WHERE e.deptype = 'e'
       AND ((e.classid = 'pg_type'::regclass AND e.objid IN (t.oid, t.typelem)) OR (e.classid = 'pg_class'::regclass AND e.objid = t.typrelid))))
+ (SELECT count(*) FROM pg_collation x JOIN pg_namespace n ON n.oid = x.collnamespace WHERE n.nspname = ANY($1) AND x.collowner = r.oid)
+ (SELECT count(*) FROM pg_statistic_ext x JOIN pg_namespace n ON n.oid = x.stxnamespace WHERE n.nspname = ANY($1) AND x.stxowner = r.oid)
+ (SELECT count(*) FROM pg_ts_dict x JOIN pg_namespace n ON n.oid = x.dictnamespace WHERE n.nspname = ANY($1) AND x.dictowner = r.oid)
+ (SELECT count(*) FROM pg_ts_config x JOIN pg_namespace n ON n.oid = x.cfgnamespace WHERE n.nspname = ANY($1) AND x.cfgowner = r.oid)
+ (SELECT count(*) FROM pg_namespace n WHERE n.nspname = ANY($1) AND n.nspowner = r.oid)
+ (SELECT count(*) FROM pg_database d WHERE $3 AND d.datname = current_database() AND d.datdba = r.oid)
FROM pg_roles r WHERE r.rolname = $2`

// defaultACLSQL lists role $2's default privilege rows in the schemas ($1) and database-wide.
const defaultACLSQL = `
SELECT coalesce(n.nspname, '*'), d.defaclobjtype::text, d.defaclacl::text
FROM pg_default_acl d LEFT JOIN pg_namespace n ON n.oid = d.defaclnamespace
WHERE d.defaclrole = (SELECT oid FROM pg_roles WHERE rolname = $2) AND (d.defaclnamespace = 0 OR n.nspname = ANY($1))
ORDER BY 1, 2`

// unsupportedKinds are kinds whose ALTER needs more than a name; a schema that holds one owned by From is refused.
var unsupportedKinds = map[string]bool{"operator": true, "operator class": true, "operator family": true, "conversion": true}

func alterStatement(kind, ident, owner string) (string, bool) {
	verb := map[string]string{
		"table": "ALTER TABLE", "view": "ALTER VIEW", "materialized view": "ALTER MATERIALIZED VIEW", "sequence": "ALTER SEQUENCE",
		"foreign table": "ALTER FOREIGN TABLE", "function": "ALTER FUNCTION", "procedure": "ALTER PROCEDURE",
		"aggregate": "ALTER AGGREGATE", "type": "ALTER TYPE", "domain": "ALTER DOMAIN", "collation": "ALTER COLLATION",
		"statistics": "ALTER STATISTICS", "text search dictionary": "ALTER TEXT SEARCH DICTIONARY",
		"text search configuration": "ALTER TEXT SEARCH CONFIGURATION",
	}[kind]
	if verb == "" {
		return "", false
	}
	return verb + " " + ident + " OWNER TO " + owner, true
}

// Move runs one owner move on conn, which must be a superuser login connected to the application database. Plan reads
// the catalog only. DryRun and Apply run in ONE transaction that first takes the walk lock of `dho migrate postgres`,
// the River migration lock and the privilege-writer lock, under SET LOCAL lock_timeout; a lock wait past it returns
// ErrLockTimeout with the transaction rolled back. There is no retry inside the run.
func Move(ctx context.Context, conn *pgx.Conn, options Options) (Report, error) {
	report := Report{Mode: options.Mode, Schemas: append([]string(nil), options.Schemas...), DatabaseOwner: options.DatabaseOwner,
		Moved: map[string]int{}, Followed: map[string]int{}, OldGrantorRowsLeft: -1}
	if conn == nil {
		return report, fmt.Errorf("%w: no connection", ErrOwnerMove)
	}
	if err := options.Validate(); err != nil {
		return report, err
	}
	if err := checkRoles(ctx, conn, options); err != nil {
		return report, err
	}
	lockTimeout := options.LockTimeout
	if lockTimeout == 0 {
		lockTimeout = DefaultLockTimeout
	}

	var tx pgx.Tx
	var err error
	if options.Mode == Plan {
		tx, err = conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	} else {
		tx, err = conn.Begin(ctx)
	}
	if err != nil {
		return report, fmt.Errorf("%w: beginning the transaction", ErrOwnerMove)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if _, err := tx.Exec(ctx, `SELECT set_config('lock_timeout', $1, true), set_config('search_path', 'pg_catalog', true)`,
		strconv.FormatInt(lockTimeout.Milliseconds(), 10)+"ms"); err != nil {
		return report, fmt.Errorf("%w: setting the lock timeout", ErrOwnerMove)
	}
	if options.Mode != Plan {
		for _, lock := range []struct {
			what string
			key  int64
		}{{"the postgres migration walk lock", PostgresWalkLockKey}, {"the River migration lock", RiverMigrationLockKey}} {
			if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, lock.key); err != nil {
				return report, lockError("taking "+lock.what, err)
			}
		}
		if err := aclguard.Lock(ctx, tx); err != nil {
			return report, lockError("taking the privilege writer lock", err)
		}
	}

	statements, err := plan(ctx, tx, options, &report)
	if err != nil {
		return report, err
	}
	if options.Mode == Plan {
		return report, nil
	}
	for _, statement := range statements {
		if _, err := tx.Exec(ctx, statement); err != nil {
			return report, lockError(statement, err)
		}
	}
	left, err := ownedOrGrantedLeft(ctx, tx, options)
	if err != nil {
		return report, err
	}
	report.OldGrantorRowsLeft = left
	if left > 0 {
		return report, fmt.Errorf("%w: %d objects or ACL entries are still the old owner's after the move; nothing was changed", ErrOwnerMove, left)
	}
	if options.Mode != Apply {
		return report, nil
	}
	if options.hookBeforeCommit != nil {
		options.hookBeforeCommit(ctx, tx)
	}
	if err := tx.Commit(ctx); err != nil {
		return report, lockError("committing", err)
	}
	report.Applied = true
	return report, nil
}

// lockError maps a lock timeout (SQLSTATE 55P03) to ErrLockTimeout and any other failure to ErrOwnerMove, with the
// SQLSTATE and never the server's message text.
func lockError(what string, err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		if pgErr.Code == "55P03" {
			return fmt.Errorf("%w while %s: another session holds a lock; the transaction was rolled back and nothing changed; rerun later (up to 3 times, 30 s apart)", ErrLockTimeout, what)
		}
		return fmt.Errorf("%w: %s: SQLSTATE %s", ErrOwnerMove, what, pgErr.Code)
	}
	return fmt.Errorf("%w: %s", ErrOwnerMove, what)
}

// checkRoles refuses a login that is not a superuser, a missing role, and (Restore excepted) a new owner that is not a
// plain login.
func checkRoles(ctx context.Context, conn *pgx.Conn, options Options) error {
	var superuser bool
	if err := conn.QueryRow(ctx, `SELECT rolsuper FROM pg_roles WHERE rolname = current_user`).Scan(&superuser); err != nil || !superuser {
		return fmt.Errorf("%w: run it with a superuser login: it changes the owners of objects the migration login does not own", ErrOwnerMove)
	}
	for _, role := range []string{options.From, options.To} {
		var exists bool
		if err := conn.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = $1)`, role).Scan(&exists); err != nil {
			return fmt.Errorf("%w: reading roles", ErrOwnerMove)
		}
		if !exists {
			return fmt.Errorf("%w: role %q does not exist", ErrOwnerMove, role)
		}
	}
	if options.Restore {
		return nil
	}
	var login, super, createRole, createDB, replication, bypass bool
	if err := conn.QueryRow(ctx, `SELECT rolcanlogin, rolsuper, rolcreaterole, rolcreatedb, rolreplication, rolbypassrls FROM pg_roles WHERE rolname = $1`,
		options.To).Scan(&login, &super, &createRole, &createDB, &replication, &bypass); err != nil {
		return fmt.Errorf("%w: reading the new owner", ErrOwnerMove)
	}
	switch {
	case !login:
		return fmt.Errorf("%w: the new owner %q cannot log in; it is the migration login", ErrOwnerMove, options.To)
	case super || createRole || createDB || replication || bypass:
		return fmt.Errorf("%w: the new owner %q must be NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS", ErrOwnerMove, options.To)
	}
	return nil
}

// plan reads the catalog, fills the report and returns the ALTER statements in order. It changes nothing.
func plan(ctx context.Context, tx pgx.Tx, options Options, report *Report) ([]string, error) {
	to := quote(options.To)
	var statements []string
	for _, schema := range options.Schemas {
		var owner string
		err := tx.QueryRow(ctx, `SELECT pg_get_userbyid(nspowner) FROM pg_namespace WHERE nspname = $1`, schema).Scan(&owner)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("%w: schema %q does not exist", ErrOwnerMove, schema)
		}
		if err != nil {
			return nil, fmt.Errorf("%w: reading the owner of schema %q", ErrOwnerMove, schema)
		}
		switch owner {
		case options.From:
			statements = append(statements, "ALTER SCHEMA "+quote(schema)+" OWNER TO "+to)
			report.Moved["schema"]++
		case options.To:
		case "pg_database_owner":
			if !options.DatabaseOwner {
				return nil, fmt.Errorf("%w: schema %q is owned by pg_database_owner: it follows the database owner, so move it with -database-owner", ErrOwnerMove, schema)
			}
		default:
			return nil, fmt.Errorf("%w: schema %q is owned by neither the old nor the new owner", ErrOwnerMove, schema)
		}

		rows, err := tx.Query(ctx, objectsSQL, schema, options.From)
		if err != nil {
			return nil, fmt.Errorf("%w: listing the objects of schema %q", ErrOwnerMove, schema)
		}
		var unsupported []string
		for rows.Next() {
			var kind, ident string
			var extension bool
			if err := rows.Scan(&kind, &ident, &extension); err != nil {
				rows.Close()
				return nil, fmt.Errorf("%w: reading an object", ErrOwnerMove)
			}
			if extension {
				report.ExtensionSkipped++
				continue
			}
			report.Moved[kind]++
			if unsupportedKinds[kind] {
				unsupported = append(unsupported, kind)
				continue
			}
			statement, _ := alterStatement(kind, ident, to)
			statements = append(statements, statement)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("%w: listing the objects of schema %q", ErrOwnerMove, schema)
		}
		if len(unsupported) > 0 {
			sort.Strings(unsupported)
			return nil, fmt.Errorf("%w: schema %q holds object kinds this move cannot carry (%s); nothing was changed", ErrOwnerMove, schema, strings.Join(unsupported, ", "))
		}
		followed, err := tx.Query(ctx, followedSQL, schema, options.From)
		if err != nil {
			return nil, fmt.Errorf("%w: counting the objects that follow their table", ErrOwnerMove)
		}
		for followed.Next() {
			var kind string
			var n int
			if err := followed.Scan(&kind, &n); err != nil {
				followed.Close()
				return nil, fmt.Errorf("%w: reading a count", ErrOwnerMove)
			}
			report.Followed[kind] += n
		}
		followed.Close()
		if err := followed.Err(); err != nil {
			return nil, fmt.Errorf("%w: counting the objects that follow their table", ErrOwnerMove)
		}
	}
	if options.DatabaseOwner {
		var database, owner string
		if err := tx.QueryRow(ctx, `SELECT current_database(), pg_get_userbyid(datdba) FROM pg_database WHERE datname = current_database()`).Scan(&database, &owner); err != nil {
			return nil, fmt.Errorf("%w: reading the database owner", ErrOwnerMove)
		}
		switch owner {
		case options.From:
			statements = append(statements, "ALTER DATABASE "+quote(database)+" OWNER TO "+to)
			report.Moved["database"]++
		case options.To:
		default:
			return nil, fmt.Errorf("%w: the database is owned by neither the old nor the new owner", ErrOwnerMove)
		}
	}
	if err := tx.QueryRow(ctx, grantorRowsSQL, options.Schemas, options.From, options.DatabaseOwner).Scan(&report.GrantorRows); err != nil {
		return nil, fmt.Errorf("%w: counting the ACL entries the old owner granted", ErrOwnerMove)
	}
	rows, err := tx.Query(ctx, defaultACLSQL, options.Schemas, options.From)
	if err != nil {
		return nil, fmt.Errorf("%w: reading the old owner's default privileges", ErrOwnerMove)
	}
	for rows.Next() {
		var d DefaultACL
		if err := rows.Scan(&d.Schema, &d.ObjectType, &d.ACL); err != nil {
			rows.Close()
			return nil, fmt.Errorf("%w: reading a default privilege row", ErrOwnerMove)
		}
		report.DefaultACLs = append(report.DefaultACLs, d)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%w: reading the old owner's default privileges", ErrOwnerMove)
	}
	return statements, nil
}

// ownedOrGrantedLeft counts what is still From's in scope after the ALTERs: owned objects, the schemas, the database
// and stored ACL entries with grantor From.
func ownedOrGrantedLeft(ctx context.Context, tx pgx.Tx, options Options) (int, error) {
	var owned, granted int
	if err := tx.QueryRow(ctx, ownedLeftSQL, options.Schemas, options.From, options.DatabaseOwner).Scan(&owned); err != nil {
		return 0, fmt.Errorf("%w: counting what the old owner still owns", ErrOwnerMove)
	}
	if err := tx.QueryRow(ctx, grantorRowsSQL, options.Schemas, options.From, options.DatabaseOwner).Scan(&granted); err != nil {
		return 0, fmt.Errorf("%w: counting the ACL entries the old owner still grants", ErrOwnerMove)
	}
	return owned + granted, nil
}
