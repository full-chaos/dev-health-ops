package authschema

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/aclguard"
)

// ErrOwnerMove reports an owner move this package refuses or could not finish.
var ErrOwnerMove = errors.New("auth owner move")

// PublicPrivilege is one privilege on one ops-schema table that the auth migrator holds WITH GRANT OPTION.
// Columns is nil for a table-level privilege.
type PublicPrivilege struct {
	Table     string
	Privilege string
	Columns   []string
}

// PublicPrivilegeSet is everything the grants pass may give the runtime role in the ops schema, under every switch:
// the one list the migrator's own authority there is cut from. A unit test holds it equal to what the grant builders
// emit, both ways.
func PublicPrivilegeSet() []PublicPrivilege {
	type key struct{ table, privilege string }
	columns := map[key][]string{}
	tableLevel := map[key]bool{}
	add := func(table, privilege string, names []string) {
		k := key{table, privilege}
		for _, name := range names {
			if !slicesContains(columns[k], name) {
				columns[k] = append(columns[k], name)
			}
		}
	}
	for _, entry := range PublicReadColumns() {
		add(entry.Table, "SELECT", entry.Columns)
	}
	refresh := LegacyRefreshTokenGrant()
	add(refresh.Table, "SELECT", refresh.Select)
	add(refresh.Table, "UPDATE", refresh.Update)
	usersInsert, usersUpdate := UsersWriteGrant()
	add("users", "INSERT", usersInsert)
	add("users", "UPDATE", usersUpdate)
	for _, grant := range RegisterOrganizationGrants() {
		add(grant.Table, "INSERT", grant.Columns)
	}
	for _, grant := range LegacyLinkPurgeGrants() {
		tableLevel[key{grant.Table, "DELETE"}] = true
		add(grant.Table, "SELECT", grant.Select)
	}
	member := MembershipWriteGrants()
	if member.Delete {
		tableLevel[key{member.Table, "DELETE"}] = true
	}
	add(member.Table, "SELECT", member.Select)
	add(member.Table, "UPDATE", member.Update)
	add(member.Table, "INSERT", member.Insert)
	for _, grant := range InviteWriteGrants() {
		add(grant.Table, "SELECT", grant.Select)
		add(grant.Table, "INSERT", grant.Insert)
		add(grant.Table, "UPDATE", grant.Update)
	}
	add(publicAuditTable, "INSERT", PublicAuditWriteColumns())
	add(ssoProvidersTable, "UPDATE", SSOProviderWriteColumns())

	var set []PublicPrivilege
	for k, names := range columns {
		if len(names) > 0 {
			set = append(set, PublicPrivilege{Table: k.table, Privilege: k.privilege, Columns: names})
		}
	}
	for k := range tableLevel {
		set = append(set, PublicPrivilege{Table: k.table, Privilege: k.privilege})
	}
	sort.Slice(set, func(i, j int) bool {
		if set[i].Table != set[j].Table {
			return set[i].Table < set[j].Table
		}
		return set[i].Privilege < set[j].Privilege
	})
	return set
}

func slicesContains(list []string, item string) bool {
	for _, v := range list {
		if v == item {
			return true
		}
	}
	return false
}

// OwnerMoveOptions configures one OwnerMove run.
type OwnerMoveOptions struct {
	// Schema is the auth schema.
	Schema string
	// From owns the schema and its objects now; To owns them afterwards.
	From, To string
	// RuntimeRole is the auth runtime login whose public-schema privileges are re-granted by To without a gap.
	RuntimeRole string
	// Apply makes the changes; without it the run reports what it would change and rolls back.
	Apply bool
	// GrantsOnly gives To the public-schema privileges of PublicPrivilegeSet and changes no owner and no runtime row.
	GrantsOnly bool
	// Restore is the reverse move (the rollback): To may be any role, and the runtime role's public rows return to the
	// table owner as grantor, with every privilege of From on those tables taken away.
	Restore bool
	// LockTimeout bounds a lock wait inside the transaction; zero means DefaultMigrationLockTimeout.
	LockTimeout time.Duration

	hookBeforeCommit func(context.Context)
	hookAfterCommit  func(context.Context)
}

// OwnerMoveReport counts what a run changed (or, without Apply, would change). It carries counts only.
type OwnerMoveReport struct {
	Applied bool
	// Moved counts, by kind, the objects of the schema From owned.
	Moved map[string]int
	// Followed counts the objects that change owner with their table (indexes, owned sequences).
	Followed map[string]int
	// PublicTables counts the ops tables of the privilege set that exist.
	PublicTables int
	// GrantRowsMoved counts the runtime role's public privilege rows given to a new grantor.
	GrantRowsMoved int
}

// Validate checks the options without touching the database.
func (o OwnerMoveOptions) Validate() error {
	for name, value := range map[string]string{"schema": o.Schema, "from": o.From, "to": o.To, "runtime role": o.RuntimeRole} {
		if value == "" {
			return fmt.Errorf("%w: %s is required", ErrOwnerMove, name)
		}
		if err := ValidateIdentifier(value); err != nil {
			return fmt.Errorf("%w: %s: %v", ErrOwnerMove, name, err)
		}
	}
	if o.From == o.To {
		return fmt.Errorf("%w: from and to are the same role", ErrOwnerMove)
	}
	if o.To == o.RuntimeRole || o.From == o.RuntimeRole {
		return fmt.Errorf("%w: the runtime role cannot be the old or the new owner", ErrOwnerMove)
	}
	if o.LockTimeout < 0 || (o.LockTimeout > 0 && o.LockTimeout < time.Millisecond) {
		return fmt.Errorf("%w: lock timeout %s must be zero (the default) or at least 1ms", ErrOwnerMove, o.LockTimeout)
	}
	return nil
}

// unsupportedOwnerMoveKinds are kinds the move cannot carry (their ALTER needs more than a name); a schema that holds
// one owned by From is refused before any change.
var unsupportedOwnerMoveKinds = map[string]bool{"operator": true, "operator class": true, "operator family": true, "conversion": true}

// movableObjectsSQL lists, for the schema ($1), the objects owned by role $2 that need their own ALTER ... OWNER,
// with the identity quoted by the server. Indexes, TOAST tables, array and row types, and sequences linked to a
// column follow their owner and are not listed (followedObjectsSQL counts them).
const movableObjectsSQL = `
SELECT kind, ident FROM (
  SELECT CASE c.relkind WHEN 'r' THEN 'table' WHEN 'p' THEN 'table' WHEN 'v' THEN 'view' WHEN 'm' THEN 'materialized view'
                        WHEN 'S' THEN 'sequence' WHEN 'f' THEN 'foreign table' ELSE 'type' END AS kind,
         quote_ident(n.nspname) || '.' || quote_ident(c.relname) AS ident, c.relowner AS o
  FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
  WHERE n.nspname = $1 AND c.relkind IN ('r','p','v','m','S','f','c')
    AND NOT (c.relkind = 'S' AND EXISTS (SELECT 1 FROM pg_depend d WHERE d.classid = 'pg_class'::regclass AND d.objid = c.oid
             AND d.refclassid = 'pg_class'::regclass AND d.deptype IN ('a','i')))
  UNION ALL
  SELECT CASE p.prokind WHEN 'p' THEN 'procedure' WHEN 'a' THEN 'aggregate' ELSE 'function' END,
         quote_ident(n.nspname) || '.' || quote_ident(p.proname) || '(' || pg_get_function_identity_arguments(p.oid) || ')', p.proowner
  FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = $1
  UNION ALL
  SELECT CASE t.typtype WHEN 'd' THEN 'domain' ELSE 'type' END, quote_ident(n.nspname) || '.' || quote_ident(t.typname), t.typowner
  FROM pg_type t JOIN pg_namespace n ON n.oid = t.typnamespace
  WHERE n.nspname = $1 AND t.typcategory <> 'A' AND t.typtype IN ('d','e','r','b')
  UNION ALL SELECT 'collation', quote_ident(n.nspname) || '.' || quote_ident(x.collname), x.collowner FROM pg_collation x JOIN pg_namespace n ON n.oid = x.collnamespace WHERE n.nspname = $1
  UNION ALL SELECT 'statistics', quote_ident(n.nspname) || '.' || quote_ident(x.stxname), x.stxowner FROM pg_statistic_ext x JOIN pg_namespace n ON n.oid = x.stxnamespace WHERE n.nspname = $1
  UNION ALL SELECT 'text search dictionary', quote_ident(n.nspname) || '.' || quote_ident(x.dictname), x.dictowner FROM pg_ts_dict x JOIN pg_namespace n ON n.oid = x.dictnamespace WHERE n.nspname = $1
  UNION ALL SELECT 'text search configuration', quote_ident(n.nspname) || '.' || quote_ident(x.cfgname), x.cfgowner FROM pg_ts_config x JOIN pg_namespace n ON n.oid = x.cfgnamespace WHERE n.nspname = $1
  UNION ALL SELECT 'operator', quote_ident(n.nspname) || '.' || quote_ident(x.oprname), x.oprowner FROM pg_operator x JOIN pg_namespace n ON n.oid = x.oprnamespace WHERE n.nspname = $1
  UNION ALL SELECT 'operator class', quote_ident(n.nspname) || '.' || quote_ident(x.opcname), x.opcowner FROM pg_opclass x JOIN pg_namespace n ON n.oid = x.opcnamespace WHERE n.nspname = $1
  UNION ALL SELECT 'operator family', quote_ident(n.nspname) || '.' || quote_ident(x.opfname), x.opfowner FROM pg_opfamily x JOIN pg_namespace n ON n.oid = x.opfnamespace WHERE n.nspname = $1
  UNION ALL SELECT 'conversion', quote_ident(n.nspname) || '.' || quote_ident(x.conname), x.conowner FROM pg_conversion x JOIN pg_namespace n ON n.oid = x.connamespace WHERE n.nspname = $1
) objs WHERE o = (SELECT oid FROM pg_roles WHERE rolname = $2) ORDER BY kind, ident`

// followedObjectsSQL counts the objects of the schema ($1) owned by $2 that change owner with their table.
const followedObjectsSQL = `
SELECT CASE WHEN c.relkind IN ('i','I') THEN 'index' ELSE 'owned sequence' END, count(*)
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = $1 AND c.relowner = (SELECT oid FROM pg_roles WHERE rolname = $2)
  AND (c.relkind IN ('i','I') OR (c.relkind = 'S' AND EXISTS (SELECT 1 FROM pg_depend d WHERE d.classid = 'pg_class'::regclass
       AND d.objid = c.oid AND d.refclassid = 'pg_class'::regclass AND d.deptype IN ('a','i'))))
GROUP BY 1 ORDER BY 1`

// runtimePublicRowsSQL lists the privilege rows the runtime role ($3) holds on public.$1 granted by the role $2.
const runtimePublicRowsSQL = `
SELECT priv, col, grantable FROM (
  SELECT a.privilege_type AS priv, ''::text AS col, a.is_grantable AS grantable, a.grantor AS gr, a.grantee AS ge
  FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
  CROSS JOIN LATERAL aclexplode(coalesce(c.relacl, acldefault('r', c.relowner))) a
  WHERE n.nspname = 'public' AND c.relname = $1
  UNION ALL
  SELECT a.privilege_type, quote_ident(att.attname), a.is_grantable, a.grantor, a.grantee
  FROM pg_attribute att JOIN pg_class c ON c.oid = att.attrelid JOIN pg_namespace n ON n.oid = c.relnamespace
  CROSS JOIN LATERAL aclexplode(att.attacl) a
  WHERE n.nspname = 'public' AND c.relname = $1 AND att.attnum > 0 AND NOT att.attisdropped
) s WHERE gr = (SELECT oid FROM pg_roles WHERE rolname = $2) AND ge = (SELECT oid FROM pg_roles WHERE rolname = $3)
ORDER BY col, priv`

func ownerMoveAlter(kind, ident, owner string) (string, bool) {
	var verb string
	switch kind {
	case "table":
		verb = "ALTER TABLE"
	case "view":
		verb = "ALTER VIEW"
	case "materialized view":
		verb = "ALTER MATERIALIZED VIEW"
	case "sequence":
		verb = "ALTER SEQUENCE"
	case "foreign table":
		verb = "ALTER FOREIGN TABLE"
	case "function":
		verb = "ALTER FUNCTION"
	case "procedure":
		verb = "ALTER PROCEDURE"
	case "aggregate":
		verb = "ALTER AGGREGATE"
	case "type":
		verb = "ALTER TYPE"
	case "domain":
		verb = "ALTER DOMAIN"
	case "collation":
		verb = "ALTER COLLATION"
	case "statistics":
		verb = "ALTER STATISTICS"
	case "text search dictionary":
		verb = "ALTER TEXT SEARCH DICTIONARY"
	case "text search configuration":
		verb = "ALTER TEXT SEARCH CONFIGURATION"
	default:
		return "", false
	}
	return verb + " " + ident + " OWNER TO " + owner, true
}

// OwnerMove moves the auth schema, and every object in it owned by From, to To, and makes To the grantor of the
// runtime role's privileges in the ops schema, in one transaction. It must run as a superuser. It takes the same
// advisory lock as Apply and the privilege-writer lock of the other GRANT sites.
//
// Forward (To becomes the owner): To is given PublicPrivilegeSet WITH GRANT OPTION; then, as To, the runtime role's
// rows that the table owner granted are granted again; then the table owner's rows are revoked. A reader of the
// runtime role sees the rows before the commit or after it, never neither. Restore (the rollback) does the same the
// other way and takes every privilege of From on those tables away.
func OwnerMove(ctx context.Context, pool *pgxpool.Pool, options OwnerMoveOptions) (OwnerMoveReport, error) {
	report := OwnerMoveReport{Moved: map[string]int{}, Followed: map[string]int{}}
	if pool == nil {
		return report, fmt.Errorf("%w: no connection pool", ErrOwnerMove)
	}
	if err := options.Validate(); err != nil {
		return report, err
	}
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return report, fmt.Errorf("%w: acquiring a connection", ErrOwnerMove)
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, advisoryLockKey); err != nil {
		return report, fmt.Errorf("%w: acquiring the migration lock", ErrOwnerMove)
	}
	defer func() {
		_, _ = conn.Exec(context.WithoutCancel(ctx), `SELECT pg_advisory_unlock($1)`, advisoryLockKey)
	}()

	var superuser bool
	if err := conn.QueryRow(ctx, `SELECT rolsuper FROM pg_roles WHERE rolname = current_user`).Scan(&superuser); err != nil || !superuser {
		return report, fmt.Errorf("%w: run it with a superuser login: it changes owners and grants as the table owner", ErrOwnerMove)
	}
	if err := checkOwnerMoveRoles(ctx, conn, options); err != nil {
		return report, err
	}

	tx, err := conn.Begin(ctx)
	if err != nil {
		return report, fmt.Errorf("%w: beginning the transaction", ErrOwnerMove)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := aclguard.Lock(ctx, tx); err != nil {
		return report, fmt.Errorf("%w: taking the privilege writer lock", ErrOwnerMove)
	}
	lockTimeout := options.LockTimeout
	if lockTimeout == 0 {
		lockTimeout = DefaultMigrationLockTimeout
	}
	if _, err := tx.Exec(ctx, `SELECT set_config('lock_timeout', $1, true), set_config('search_path', 'pg_catalog', true)`,
		strconv.FormatInt(lockTimeout.Milliseconds(), 10)+"ms"); err != nil {
		return report, fmt.Errorf("%w: setting the lock timeout", ErrOwnerMove)
	}

	schemaID, err := NewValidatedIdentifier(options.Schema)
	if err != nil {
		return report, err
	}
	fromID, toID, runtimeID := mustValidatedIdentifier(options.From), mustValidatedIdentifier(options.To), mustValidatedIdentifier(options.RuntimeRole)
	from, to, runtime := quoteIdentifier(fromID), quoteIdentifier(toID), quoteIdentifier(runtimeID)

	var statements []string
	if !options.GrantsOnly {
		var schemaOwner string
		err := tx.QueryRow(ctx, `SELECT r.rolname::text FROM pg_namespace n JOIN pg_roles r ON r.oid = n.nspowner WHERE n.nspname = $1`, options.Schema).Scan(&schemaOwner)
		if errors.Is(err, pgx.ErrNoRows) {
			return report, fmt.Errorf("%w: schema %q does not exist", ErrOwnerMove, options.Schema)
		}
		if err != nil {
			return report, fmt.Errorf("%w: reading the schema owner", ErrOwnerMove)
		}
		if schemaOwner != options.From && schemaOwner != options.To {
			return report, fmt.Errorf("%w: schema %q is owned by neither the old nor the new owner", ErrOwnerMove, options.Schema)
		}
		rows, err := tx.Query(ctx, movableObjectsSQL, options.Schema, options.From)
		if err != nil {
			return report, fmt.Errorf("%w: listing the objects to move", ErrOwnerMove)
		}
		type object struct{ kind, ident string }
		var objects []object
		for rows.Next() {
			var o object
			if err := rows.Scan(&o.kind, &o.ident); err != nil {
				rows.Close()
				return report, fmt.Errorf("%w: reading an object", ErrOwnerMove)
			}
			objects = append(objects, o)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return report, fmt.Errorf("%w: listing the objects to move", ErrOwnerMove)
		}
		var unsupported []string
		for _, o := range objects {
			report.Moved[o.kind]++
			if unsupportedOwnerMoveKinds[o.kind] {
				unsupported = append(unsupported, o.kind)
			}
		}
		if len(unsupported) > 0 {
			sort.Strings(unsupported)
			return report, fmt.Errorf("%w: the schema holds object kinds this move cannot carry (%s); nothing was changed", ErrOwnerMove, strings.Join(slicesCompact(unsupported), ", "))
		}
		followed, err := tx.Query(ctx, followedObjectsSQL, options.Schema, options.From)
		if err != nil {
			return report, fmt.Errorf("%w: counting the objects that follow their table", ErrOwnerMove)
		}
		for followed.Next() {
			var kind string
			var n int
			if err := followed.Scan(&kind, &n); err != nil {
				followed.Close()
				return report, fmt.Errorf("%w: reading a count", ErrOwnerMove)
			}
			report.Followed[kind] = n
		}
		followed.Close()
		for _, o := range objects {
			statement, _ := ownerMoveAlter(o.kind, o.ident, to)
			statements = append(statements, statement)
		}
		if schemaOwner == options.From {
			statements = append(statements, "ALTER SCHEMA "+quoteIdentifier(schemaID)+" OWNER TO "+to)
			report.Moved["schema"]++
		}
	}
	publicStatements, err := ownerMovePublicStatements(ctx, tx, options, from, to, runtime, &report)
	if err != nil {
		return report, err
	}
	statements = append(statements, publicStatements...)
	if options.Apply {
		for _, statement := range statements {
			if _, err := tx.Exec(ctx, statement); err != nil {
				return report, fmt.Errorf("%w: %s: %v", ErrOwnerMove, statement, redactPGError(err))
			}
		}
	}

	if options.Apply && !options.GrantsOnly {
		rows, err := tx.Query(ctx, schemaObjectsOwnedSQL, options.Schema, options.From, "", PushLifecycleFunction)
		if err != nil {
			return report, fmt.Errorf("%w: counting what is left", ErrOwnerMove)
		}
		var left []string
		for rows.Next() {
			var kind string
			var n int
			if err := rows.Scan(&kind, &n); err != nil {
				rows.Close()
				return report, fmt.Errorf("%w: reading a count", ErrOwnerMove)
			}
			left = append(left, fmt.Sprintf("%d %s", n, kind))
		}
		rows.Close()
		if len(left) > 0 {
			return report, fmt.Errorf("%w: objects are still owned by the old owner after the move (%s); nothing was changed", ErrOwnerMove, strings.Join(left, ", "))
		}
	}

	if !options.Apply {
		return report, nil
	}
	if options.hookBeforeCommit != nil {
		options.hookBeforeCommit(ctx)
	}
	if err := tx.Commit(ctx); err != nil {
		return report, fmt.Errorf("%w: committing", ErrOwnerMove)
	}
	report.Applied = true
	if options.hookAfterCommit != nil {
		options.hookAfterCommit(ctx)
	}
	return report, nil
}

func slicesCompact(sorted []string) []string {
	out := sorted[:0:0]
	for i, v := range sorted {
		if i == 0 || v != sorted[i-1] {
			out = append(out, v)
		}
	}
	return out
}

// checkOwnerMoveRoles refuses a new owner that is not a plain login (Restore excepted), and a missing role.
func checkOwnerMoveRoles(ctx context.Context, conn *pgxpool.Conn, options OwnerMoveOptions) error {
	for _, role := range []string{options.From, options.To, options.RuntimeRole} {
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

type publicTableInfo struct {
	name    string
	columns map[string]bool
}

// privilegeKeyword returns the keyword of a privilege of PublicPrivilegeSet or of a catalog ACL row; any other value
// is refused, so no text but one of four constants reaches a statement.
func privilegeKeyword(privilege string) (string, bool) {
	switch privilege {
	case "SELECT":
		return "SELECT", true
	case "INSERT":
		return "INSERT", true
	case "UPDATE":
		return "UPDATE", true
	case "DELETE":
		return "DELETE", true
	case "TRUNCATE":
		return "TRUNCATE", true
	case "REFERENCES":
		return "REFERENCES", true
	case "TRIGGER":
		return "TRIGGER", true
	}
	return "", false
}

// ownerMovePublicStatements returns, in order, the statements that give To the privilege set and move the runtime
// role's public rows to To as grantor (Restore: back to the table owner). It reads the catalog and changes nothing.
func ownerMovePublicStatements(ctx context.Context, tx pgx.Tx, options OwnerMoveOptions, from, to, runtime string, report *OwnerMoveReport) ([]string, error) {
	tables := map[string]*publicTableInfo{}
	for _, entry := range PublicPrivilegeSet() {
		if tables[entry.Table] != nil {
			continue
		}
		exists, err := publicTableState(ctx, tx, entry.Table)
		if err != nil {
			return nil, err
		}
		if !exists {
			continue
		}
		info := &publicTableInfo{name: entry.Table, columns: map[string]bool{}}
		rows, err := tx.Query(ctx, `SELECT attname::text FROM pg_attribute WHERE attrelid = format('public.%I', $1::text)::regclass AND attnum > 0 AND NOT attisdropped`, entry.Table)
		if err != nil {
			return nil, fmt.Errorf("%w: reading the columns of public.%s", ErrOwnerMove, entry.Table)
		}
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				rows.Close()
				return nil, fmt.Errorf("%w: reading a column", ErrOwnerMove)
			}
			info.columns[name] = true
		}
		rows.Close()
		tables[entry.Table] = info
	}
	report.PublicTables = len(tables)
	qualified := func(table string) string {
		return quoteIdentifier(mustValidatedIdentifier(publicSchema)) + "." + quoteIdentifier(mustValidatedIdentifier(table))
	}

	var statements []string
	if !options.Restore {
		for _, entry := range PublicPrivilegeSet() {
			info := tables[entry.Table]
			keyword, ok := privilegeKeyword(entry.Privilege)
			if info == nil || !ok {
				continue
			}
			if entry.Columns == nil {
				statements = append(statements, "GRANT "+keyword+" ON TABLE "+qualified(entry.Table)+" TO "+to+" WITH GRANT OPTION")
				continue
			}
			var present []string
			for _, column := range entry.Columns {
				if info.columns[column] {
					present = append(present, quoteIdentifier(mustValidatedIdentifier(column)))
				}
			}
			if len(present) > 0 {
				statements = append(statements, "GRANT "+keyword+" ("+strings.Join(present, ", ")+") ON TABLE "+qualified(entry.Table)+" TO "+to+" WITH GRANT OPTION")
			}
		}
	}
	if options.GrantsOnly {
		return statements, nil
	}

	names := make([]string, 0, len(tables))
	for name := range tables {
		names = append(names, name)
	}
	sort.Strings(names)
	type row struct {
		priv, col string
		grantable bool
	}
	for _, table := range names {
		var owner string
		if err := tx.QueryRow(ctx, `SELECT r.rolname::text FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace JOIN pg_roles r ON r.oid = c.relowner
			WHERE n.nspname = 'public' AND c.relname = $1`, table).Scan(&owner); err != nil {
			return nil, fmt.Errorf("%w: reading the owner of public.%s", ErrOwnerMove, table)
		}
		oldGrantor := owner
		if options.Restore {
			oldGrantor = options.From
		} else if owner == options.To {
			continue
		}
		rows, err := tx.Query(ctx, runtimePublicRowsSQL, table, oldGrantor, options.RuntimeRole)
		if err != nil {
			return nil, fmt.Errorf("%w: reading the runtime privileges of public.%s", ErrOwnerMove, table)
		}
		var held []row
		for rows.Next() {
			var r row
			if err := rows.Scan(&r.priv, &r.col, &r.grantable); err != nil {
				rows.Close()
				return nil, fmt.Errorf("%w: reading a privilege row", ErrOwnerMove)
			}
			held = append(held, r)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("%w: reading the runtime privileges of public.%s", ErrOwnerMove, table)
		}
		form := func(r row) (string, error) {
			keyword, ok := privilegeKeyword(r.priv)
			if !ok {
				return "", fmt.Errorf("%w: the runtime role holds a %s privilege on public.%s that this move does not know", ErrOwnerMove, r.priv, table)
			}
			if r.col == "" {
				return keyword + " ON TABLE " + qualified(table), nil
			}
			return keyword + " (" + r.col + ") ON TABLE " + qualified(table), nil
		}
		var regrant, revoke []string
		for _, r := range held {
			target, err := form(r)
			if err != nil {
				return nil, err
			}
			grant := "GRANT " + target + " TO " + runtime
			if r.grantable {
				grant += " WITH GRANT OPTION"
			}
			regrant = append(regrant, grant)
			revoke = append(revoke, "REVOKE "+target+" FROM "+runtime)
		}
		report.GrantRowsMoved += len(held)
		if len(held) > 0 {
			if options.Restore {
				statements = append(statements, regrant...)
				statements = append(statements, "SET LOCAL ROLE "+from)
				statements = append(statements, revoke...)
				statements = append(statements, "RESET ROLE")
			} else {
				statements = append(statements, "SET LOCAL ROLE "+to)
				statements = append(statements, regrant...)
				statements = append(statements, "RESET ROLE")
				statements = append(statements, revoke...)
			}
		}
		if options.Restore {
			statements = append(statements, "REVOKE ALL ON TABLE "+qualified(table)+" FROM "+from)
		}
	}
	return statements, nil
}
