// Package roleprovision creates and bootstraps the unprivileged PostgreSQL logins
// the Go runtime connects as (CHAOS-6901). It is the Go replacement for
// scripts/worker/provision_river_roles.sql, which the chart's provision-roles Job
// and Compose's go-river-provision ran through psql: `dho migrate roles` runs
// Apply, so the roles are provisioned by the same binary, the same role manifest
// vocabulary (internal/storage/roleacl) and the same secret handling as every
// other migrate step.
//
// What it does is exactly what the script did, statement for statement, and NOTHING
// else -- in particular it never grants a table privilege (those belong to
// `dho migrate river`, derived from the posture manifests), never runs DROP OWNED BY
// and never revokes a grant a later step applied:
//
//   - domain, queue-control and coordinator logins (always), plus the optional api,
//     query-api and KEDA read-only logins: created LOGIN NOSUPERUSER NOCREATEDB
//     NOCREATEROLE NOREPLICATION NOBYPASSRLS with the given password only when
//     missing (an existing role keeps its attributes and password, except KEDA's
//     password, which is re-applied every run so a rotation takes effect);
//
//   - CONNECT on the application database; TEMPORARY revoked (from PUBLIC too, for
//     the mandatory block and the api and query-api blocks); USAGE and no CREATE on
//     schema public, for the role only;
//
//   - KEDA: USAGE on the River schema, SELECT on river_job and (CHAOS-6946) USAGE on
//     schema public plus SELECT on public.sync_run_units -- the go-sync ScaledObject's
//     planned-backlog trigger reads it -- nothing else. The psql script never granted
//     the last two: every OTHER role gets an EXPLICIT USAGE on schema public (never
//     relying on PUBLIC's own default), and KEDA now reads a table there too, so it
//     gets the same explicit grant rather than the ambient PUBLIC default a hardened
//     database may have revoked.
//
//   - monitor (CHAOS-9369, optional, no script counterpart): the OpenTelemetry
//     collector's postgresql-receiver login. CONNECT on the application database,
//     TEMPORARY revoked, and membership of the built-in pg_monitor -- no schema USAGE,
//     no table privilege; Verify asserts exactly that (member of pg_monitor and of no
//     other role, no table privilege in public or the River schema, no own-name grant
//     but CONNECT).
//
// One deliberate difference from the script: Apply runs in ONE transaction, so a
// failure part-way leaves nothing half-applied (psql ran each statement on its own).
// The end state on success is identical, which the executed parity test proves
// against the real script.
package roleprovision

import (
	"context"
	"errors"
	"fmt"
	"strings"

	riverstore "github.com/full-chaos/dev-health-ops/internal/storage/river"
	"github.com/full-chaos/dev-health-ops/internal/storage/roleacl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// MaxIdentifierBytes is PostgreSQL's identifier limit (NAMEDATALEN-1). The server
// truncates a longer name silently; refusing it here means the name a caller
// configured is the name that exists.
const MaxIdentifierBytes = 63

// kedaLabel is the label (never the name) the KEDA login carries in errors and Verify.
const kedaLabel = "keda"

// monitorLabel is the label (never the name) the pg_monitor-only login carries in
// errors and Verify (CHAOS-9369).
const monitorLabel = "monitor"

// monitorBuiltinRole is the one role the monitor login is a member of: the built-in
// pg_monitor, which is what the OpenTelemetry collector's postgresql receiver needs
// (pg_stat_* views) and nothing more.
const monitorBuiltinRole = "pg_monitor"

// kedaSyncRunUnits is the application table the go-sync KEDA trigger counts planned
// units in (CHAOS-6946). It is Alembic's, in schema public, so it exists before this
// command runs in the chart and in Compose; the KEDA login gets SELECT on it and on
// nothing else in public.
const kedaSyncRunUnits = "public.sync_run_units"

// Role is one login to provision.
type Role struct {
	Name     string
	Password string
}

// Options is the whole provisioning request. Domain, Queue and Coordinator are
// mandatory; the others are provisioned only when their Name is set.
type Options struct {
	Domain      Role
	Queue       Role
	Coordinator Role
	API         Role
	QueryAPI    Role
	// Keda is the KEDA postgresql scaler's read-only login: it reads river_job in
	// RiverSchema (default "river") for the queue-backlog trigger, and (CHAOS-6946)
	// public.sync_run_units for the go-sync planned-backlog trigger.
	Keda Role
	// Monitor is the OpenTelemetry collector's postgresql-receiver login (CHAOS-9369):
	// LOGIN, CONNECT on the application database, member of the built-in pg_monitor,
	// and nothing else -- no schema USAGE, no table privilege of any kind.
	Monitor     Role
	RiverSchema string
	// Authenticate, when set, is asked to log in as a role with the password it was
	// given; Verify uses it to notice a supplied password that does not work for a
	// login that already existed (Apply never rotates one).
	Authenticate func(ctx context.Context, role Role) error
}

// Errors carry no password, and no DSN.
var (
	ErrInvalidOptions = errors.New("invalid role provisioning options")
	ErrProvisioning   = errors.New("role provisioning failed")
	ErrPostcondition  = errors.New("provisioned roles do not meet the bootstrap postconditions")
)

type labelled struct {
	label string
	role  Role
}

func (o Options) configured() []labelled {
	all := []labelled{
		{"domain", o.Domain}, {"queue", o.Queue}, {"coordinator", o.Coordinator},
		{"api", o.API}, {"query_api", o.QueryAPI}, {"keda", o.Keda}, {monitorLabel, o.Monitor},
	}
	out := make([]labelled, 0, len(all))
	for index, entry := range all {
		// The first three are mandatory and always listed, so an unset one is
		// reported by Validate rather than silently skipped.
		if index < 3 || entry.role.Name != "" {
			out = append(out, entry)
		}
	}
	return out
}

func (o Options) schema() string {
	if o.RiverSchema == "" {
		return "river"
	}
	return o.RiverSchema
}

// Validate refuses what would create a wrong or ambiguous role set. It is stricter
// than the script in the ways that can only prevent a mistake: every configured
// name must be a valid, distinct identifier (the script compared only some pairs,
// so a KEDA role named like the domain role had its password rewritten), and every
// configured role needs a password (the script accepted an empty one and created a
// role nobody could log in as). Messages name the role LABELS, never a password.
func (o Options) Validate() error {
	seen := map[string]string{}
	for _, entry := range o.configured() {
		name := entry.role.Name
		switch {
		case name == "":
			return fmt.Errorf("%w: the %s role name is required", ErrInvalidOptions, entry.label)
		case len(name) > MaxIdentifierBytes:
			return fmt.Errorf("%w: the %s role name exceeds %d bytes", ErrInvalidOptions, entry.label, MaxIdentifierBytes)
		case strings.ContainsRune(name, 0):
			return fmt.Errorf("%w: the %s role name contains a NUL byte", ErrInvalidOptions, entry.label)
		case entry.role.Password == "":
			return fmt.Errorf("%w: the %s role password is required", ErrInvalidOptions, entry.label)
		case strings.ContainsRune(entry.role.Password, 0):
			return fmt.Errorf("%w: the %s role password contains a NUL byte", ErrInvalidOptions, entry.label)
		}
		if other, dup := seen[name]; dup {
			return fmt.Errorf("%w: the %s and %s roles must be distinct", ErrInvalidOptions, other, entry.label)
		}
		seen[name] = entry.label
	}
	// The runtime roles are the ones `dho migrate river` grants on and refuses by
	// its own identifier rule, with the misleading "must be distinct" message. A
	// login this command could create but river then rejects is a dead end, so the
	// same rule applies here, up front. The KEDA and monitor logins are never read by
	// river and stay free-form.
	for _, entry := range o.configured() {
		if entry.label != "keda" && entry.label != monitorLabel && !riverstore.ValidIdentifier(entry.role.Name) {
			return fmt.Errorf("%w: the %s role name must match [a-z_][a-z0-9_]* and be at most %d bytes (the rule `dho migrate river` applies to runtime roles)",
				ErrInvalidOptions, entry.label, MaxIdentifierBytes)
		}
	}
	if o.Keda.Name != "" && !riverstore.ValidIdentifier(o.schema()) {
		return fmt.Errorf("%w: the River schema name must match [a-z_][a-z0-9_]* and be at most %d bytes", ErrInvalidOptions, MaxIdentifierBytes)
	}
	return nil
}

// createRoleSQL is the script's `SELECT format(CREATE ROLE ...) WHERE NOT EXISTS ...
// \gexec`: the SERVER quotes the identifier (%I) and the password literal (%L), so
// no client-side quoting of either exists to get wrong.
const createRoleSQL = `SELECT format(
	'CREATE ROLE %I LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD %L',
	$1::text, $2::text)
WHERE NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = $1::text)`

const setPasswordSQL = `SELECT format('ALTER ROLE %I PASSWORD %L', $1::text, $2::text)`

func ident(name string) string { return pgx.Identifier{name}.Sanitize() }

// statementRunner adapts a pgx.Tx to what apply needs.
type statementRunner struct{ tx pgx.Tx }

func (r statementRunner) exec(ctx context.Context, sql string) error {
	_, err := r.tx.Exec(ctx, sql)
	return err
}

// generated runs a server-formatted statement (createRoleSQL, setPasswordSQL). A
// row means "run this"; no row means the guard said skip.
func (r statementRunner) generated(ctx context.Context, query string, role Role) error {
	var statement string
	err := r.tx.QueryRow(ctx, query, role.Name, role.Password).Scan(&statement)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	return r.exec(ctx, statement)
}

// Apply provisions the roles in one transaction. The pool must be the elevated
// migration endpoint (the database owner or a superuser), direct, never a
// transaction pooler.
func Apply(ctx context.Context, pool interface {
	Begin(context.Context) (pgx.Tx, error)
}, options Options) (err error) {
	if err := options.Validate(); err != nil {
		return err
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("%w: cannot open a transaction", ErrProvisioning)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	run := statementRunner{tx: tx}

	var database string
	if err := tx.QueryRow(ctx, `SELECT current_database()`).Scan(&database); err != nil {
		return fmt.Errorf("%w: cannot read the current database", ErrProvisioning)
	}
	db := ident(database)

	// PREFLIGHT, before any statement changes anything: a configured role that already
	// exists must be an eligible unprivileged login. Granting River access to (or
	// bootstrapping) an existing NOLOGIN group, or a role with CREATEDB/SUPERUSER/...,
	// would commit before the closing check could refuse it, and members of a group
	// inherit whatever it is granted. The script never looked; this is a stated
	// deviation. The refusal names the LABEL.
	for _, entry := range options.configured() {
		var ineligible bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = $1) AND NOT `+roleacl.RoleAttributesSQL,
			entry.role.Name).Scan(&ineligible); err != nil {
			return fmt.Errorf("%w: cannot read the existing %s role", ErrProvisioning, entry.label)
		}
		if ineligible {
			return fmt.Errorf("%w: the %s role already exists and is not an unprivileged login (NOLOGIN, or SUPERUSER/BYPASSRLS/CREATEROLE/CREATEDB/REPLICATION); nothing was changed",
				ErrProvisioning, entry.label)
		}
	}

	step := func(what string, err error) error {
		if err != nil {
			// The cause is the server's text; it can quote a statement, and a
			// CREATE ROLE statement carries the password, so it is never wrapped.
			return fmt.Errorf("%w: %s", ErrProvisioning, what)
		}
		return nil
	}

	// ONE create path for every role: the error names the LABEL, never the name
	// (Compose defaults each role's password to the role's own name).
	create := func(entry labelled) error {
		return step("create the "+entry.label+" role", run.generated(ctx, createRoleSQL, entry.role))
	}

	// The mandatory block, in the script's order.
	for _, entry := range []labelled{{"domain", options.Domain}, {"queue", options.Queue}, {"coordinator", options.Coordinator}} {
		if err := create(entry); err != nil {
			return err
		}
	}
	for _, role := range []Role{options.Domain, options.Queue, options.Coordinator} {
		if err := step("grant connect", run.exec(ctx, "GRANT CONNECT ON DATABASE "+db+" TO "+ident(role.Name))); err != nil {
			return err
		}
	}
	if err := step("revoke temporary", run.exec(ctx, "REVOKE TEMPORARY ON DATABASE "+db+" FROM PUBLIC, "+
		ident(options.Domain.Name)+", "+ident(options.Queue.Name)+", "+ident(options.Coordinator.Name))); err != nil {
		return err
	}
	for _, role := range []Role{options.Coordinator, options.Domain, options.Queue} {
		if err := step("public schema usage", run.exec(ctx, "GRANT USAGE ON SCHEMA public TO "+ident(role.Name))); err != nil {
			return err
		}
		if err := step("public schema create", run.exec(ctx, "REVOKE CREATE ON SCHEMA public FROM "+ident(role.Name))); err != nil {
			return err
		}
	}

	// api and query-api: the same bootstrap shape, TEMPORARY also revoked from PUBLIC.
	for _, entry := range []labelled{{"api", options.API}, {"query_api", options.QueryAPI}} {
		role := entry.role
		if role.Name == "" {
			continue
		}
		if err := create(entry); err != nil {
			return err
		}
		name := ident(role.Name)
		for _, item := range []struct{ what, statement string }{
			{"grant connect", "GRANT CONNECT ON DATABASE " + db + " TO " + name},
			{"revoke temporary", "REVOKE TEMPORARY ON DATABASE " + db + " FROM PUBLIC, " + name},
			{"public schema usage", "GRANT USAGE ON SCHEMA public TO " + name},
			{"public schema create", "REVOKE CREATE ON SCHEMA public FROM " + name},
		} {
			if err := step(item.what, run.exec(ctx, item.statement)); err != nil {
				return err
			}
		}
	}

	// KEDA: read-only on river_job; the password is re-applied every run.
	if options.Keda.Name != "" {
		name := ident(options.Keda.Name)
		schema := ident(options.schema())
		if err := create(labelled{kedaLabel, options.Keda}); err != nil {
			return err
		}
		if err := step("keda password", run.generated(ctx, setPasswordSQL, options.Keda)); err != nil {
			return err
		}
		for _, item := range []struct{ what, statement string }{
			{"grant connect", "GRANT CONNECT ON DATABASE " + db + " TO " + name},
			{"revoke temporary", "REVOKE TEMPORARY ON DATABASE " + db + " FROM " + name},
			{"river schema usage", "GRANT USAGE ON SCHEMA " + schema + " TO " + name},
			{"river_job select", "GRANT SELECT ON " + schema + ".river_job TO " + name},
			{"public schema usage", "GRANT USAGE ON SCHEMA public TO " + name},
			{"sync_run_units select", "GRANT SELECT ON " + kedaSyncRunUnits + " TO " + name},
		} {
			if err := step(item.what, run.exec(ctx, item.statement)); err != nil {
				return err
			}
		}
	}

	// monitor: pg_monitor membership and CONNECT, nothing else. No schema USAGE and no
	// table grant is given (the built-in role's own privileges are statistics views and
	// settings); TEMPORARY is revoked like every other login's. The password is NOT
	// re-applied: an existing login keeps its own, like the other roles.
	if options.Monitor.Name != "" {
		name := ident(options.Monitor.Name)
		if err := create(labelled{monitorLabel, options.Monitor}); err != nil {
			return err
		}
		for _, item := range []struct{ what, statement string }{
			{"grant connect", "GRANT CONNECT ON DATABASE " + db + " TO " + name},
			{"revoke temporary", "REVOKE TEMPORARY ON DATABASE " + db + " FROM " + name},
			{"grant pg_monitor", "GRANT " + monitorBuiltinRole + " TO " + name},
		} {
			if err := step(item.what, run.exec(ctx, item.statement)); err != nil {
				return err
			}
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("%w: commit", ErrProvisioning)
	}
	return nil
}

// Problem is one unmet bootstrap postcondition. Role is a LABEL (domain, queue, ...),
// Detail a fixed phrase: nothing here can carry a password.
type Problem struct {
	Role   string
	Detail string
}

func (p Problem) String() string { return p.Role + ": " + p.Detail }

// Verify re-reads the catalog and returns every unmet bootstrap postcondition for
// the configured roles, plus (Warnings) what only a human can decide. It asserts
// exactly what Apply is responsible for and nothing that a later step owns, so it
// is green both right after Apply and after `dho migrate river` has granted the
// table posture: the role exists as an unprivileged login (roleacl's own
// definition), can CONNECT, holds no TEMPORARY, holds USAGE and no CREATE on
// schema public, and -- KEDA -- can read river_job and public.sync_run_units. A CREATE on schema public that
// arrives only through PUBLIC (the PostgreSQL default before v15) is a warning, not
// a failure: revoking it would strip every other role of it, and the readiness
// checks refuse the role for it loudly (the script's stated position).
func Verify(ctx context.Context, q roleacl.Querier, options Options) (problems []Problem, warnings []Problem, err error) {
	if err := options.Validate(); err != nil {
		return nil, nil, err
	}
	var database string
	if err := q.QueryRow(ctx, `SELECT current_database()`).Scan(&database); err != nil {
		return nil, nil, fmt.Errorf("%w: cannot read the current database", ErrProvisioning)
	}
	for _, entry := range options.configured() {
		var login bool
		if err := q.QueryRow(ctx, `SELECT `+roleacl.RoleAttributesSQL, entry.role.Name).Scan(&login); err != nil {
			return nil, nil, fmt.Errorf("%w: cannot read role attributes", ErrProvisioning)
		}
		if !login {
			problems = append(problems, Problem{entry.label, "is not an unprivileged login (missing, or SUPERUSER/BYPASSRLS/CREATEROLE/CREATEDB/REPLICATION, or NOLOGIN)"})
			continue
		}
		// The supplied password must actually work for the login (an existing login keeps
		// its old password: Apply never rotates one). A wrong password is definitive
		// (SQLSTATE 28P01); anything else (pg_hba, network) only means this endpoint
		// cannot check, which must not block a deploy.
		if options.Authenticate != nil {
			if authErr := options.Authenticate(ctx, entry.role); authErr != nil {
				var pgErr *pgconn.PgError
				if errors.As(authErr, &pgErr) && pgErr.Code == "28P01" {
					problems = append(problems, Problem{entry.label, "the supplied password does not authenticate (an existing role keeps its old password; this command never rotates one)"})
				} else {
					warnings = append(warnings, Problem{entry.label, "could not verify the supplied password from this endpoint"})
				}
			}
		}
		var connect, temporary, usage, create bool
		var direct bool
		if err := q.QueryRow(ctx, `SELECT
			has_database_privilege($1::name, $2::name, 'CONNECT'),
			has_database_privilege($1::name, $2::name, 'TEMPORARY'),
			has_schema_privilege($1::name, 'public', 'USAGE'),
			has_schema_privilege($1::name, 'public', 'CREATE'),
			EXISTS (
				SELECT 1 FROM pg_catalog.pg_namespace AS namespace,
					LATERAL pg_catalog.aclexplode(COALESCE(namespace.nspacl, pg_catalog.acldefault('n', namespace.nspowner))) AS entry
				WHERE namespace.nspname = 'public'
					AND entry.privilege_type = 'CREATE'
					AND entry.grantee = (SELECT oid FROM pg_catalog.pg_roles WHERE rolname = $1))`,
			entry.role.Name, database).Scan(&connect, &temporary, &usage, &create, &direct); err != nil {
			return nil, nil, fmt.Errorf("%w: cannot read role privileges", ErrProvisioning)
		}
		if !connect {
			problems = append(problems, Problem{entry.label, "cannot CONNECT to the application database"})
		}
		if temporary {
			problems = append(problems, Problem{entry.label, "holds TEMPORARY on the application database"})
		}
		// The roleacl closure, the SAME for EVERY role including KEDA (lead D2616): the
		// identity pieces the readiness check uses (member of no role, owns nothing) and
		// the effective-grant enumeration with its ACL-dependency self-check, PUBLIC
		// counted. Only the JUDGEMENT of the enumerated grants differs per role
		// (judgeGrant); a per-role shortcut around the closure is what
		// TestVerifyRunsTheRoleaclClosureForEveryConfiguredRole exists to catch.
		var memberFree, ownsNothing bool
		if err := q.QueryRow(ctx, `SELECT `+roleacl.MembershipFreeSQL+`, `+roleacl.OwnsNothingSQL, entry.role.Name).Scan(&memberFree, &ownsNothing); err != nil {
			return nil, nil, fmt.Errorf("%w: cannot read role membership and ownership", ErrProvisioning)
		}
		if entry.label == monitorLabel {
			// The monitor login is the one login that MUST be a member of a role -- the
			// built-in pg_monitor -- and of no other (nothing reachable through
			// pg_monitor's own built-in members counts). Its table-privilege and
			// grant checks below are the same closure every other login gets.
			problems = append(problems, verifyMonitorMembership(ctx, q, entry)...)
		} else if !memberFree {
			problems = append(problems, Problem{entry.label, "is a member of another role (privileges it inherits are invisible to its own grants; readiness refuses it)"})
		}
		if !ownsNothing {
			problems = append(problems, Problem{entry.label, "owns an object (readiness refuses it)"})
		}
		grants, err := roleacl.Enumerate(ctx, q, entry.role.Name)
		if err != nil {
			return nil, nil, fmt.Errorf("%w: cannot enumerate the %s role's grants", ErrProvisioning, entry.label)
		}
		for _, grant := range grants {
			if grant.Class == "setting" {
				continue
			}
			if detail := judgeGrant(entry.label, grant, database, options.schema()); detail != "" {
				problems = append(problems, Problem{entry.label, detail})
			}
		}
		if entry.label == monitorLabel {
			tableProblems, err := verifyMonitorTables(ctx, q, entry, options.schema())
			if err != nil {
				return nil, nil, err
			}
			problems = append(problems, tableProblems...)
			// No USAGE check (the login is not meant to hold one).
			// CREATE through PUBLIC counts too (PostgreSQL before v15 grants it by
			// default): unlike the runtime logins, this one is documented as holding
			// no schema privilege at all, so the effective privilege is the check.
			if direct || create {
				problems = append(problems, Problem{entry.label, "can CREATE in schema public (in its own name or through PUBLIC)"})
			}
			continue
		}
		if entry.label == "keda" {
			var readsJobs bool
			if err := q.QueryRow(ctx, `SELECT has_schema_privilege($1::name, $2::name, 'USAGE')
				AND has_table_privilege($1::name, format('%I.river_job', $2::text), 'SELECT')`,
				entry.role.Name, options.schema()).Scan(&readsJobs); err != nil {
				return nil, nil, fmt.Errorf("%w: cannot read the KEDA role's River privileges", ErrProvisioning)
			}
			if !readsJobs {
				problems = append(problems, Problem{entry.label, "cannot SELECT river_job"})
			}
			// to_regclass is NULL for a missing table, and has_table_privilege(NULL) is
			// NULL: a missing table is a NAMED problem, never a query error or a pass.
			// has_table_privilege alone is not enough: it is a pure ACL check, blind to
			// schema visibility. The role must ALSO hold effective USAGE on schema
			// public (own grant or through PUBLIC) -- otherwise the query the table
			// grant exists to serve fails to resolve the name at all ("permission
			// denied for schema public"), even though this login holds a real SELECT
			// grant on the table (r1 finding, CHAOS-6946).
			var readsUnits, publicUsage bool
			if err := q.QueryRow(ctx, `SELECT COALESCE(has_table_privilege($1::name, to_regclass($2), 'SELECT'), false)`,
				entry.role.Name, kedaSyncRunUnits).Scan(&readsUnits); err != nil {
				return nil, nil, fmt.Errorf("%w: cannot read the KEDA role's sync_run_units privilege", ErrProvisioning)
			}
			if err := q.QueryRow(ctx, `SELECT has_schema_privilege($1::name, 'public', 'USAGE')`, entry.role.Name).Scan(&publicUsage); err != nil {
				return nil, nil, fmt.Errorf("%w: cannot read the KEDA role's schema public privilege", ErrProvisioning)
			}
			if !readsUnits || !publicUsage {
				problems = append(problems, Problem{entry.label, "cannot SELECT sync_run_units"})
			}
			continue
		}
		if !usage {
			problems = append(problems, Problem{entry.label, "lacks USAGE on schema public"})
		}
		if direct {
			problems = append(problems, Problem{entry.label, "holds CREATE on schema public in its own name"})
		} else if create {
			warnings = append(warnings, Problem{entry.label, "holds CREATE on schema public only through PUBLIC (a target-wide default a human must revoke)"})
		}
	}
	return problems, warnings, nil
}

// monitorQuerier is what the monitor checks need.
type monitorQuerier = roleacl.Querier

// verifyMonitorMembership asserts the login is a member of pg_monitor and of no
// role outside pg_monitor and its three built-in members.
func verifyMonitorMembership(ctx context.Context, q monitorQuerier, entry labelled) []Problem {
	var isMember bool
	var others int
	err := q.QueryRow(ctx, `SELECT pg_catalog.pg_has_role($1::name, $2::name, 'MEMBER'),
		(WITH RECURSIVE reachable(oid) AS (
			SELECT membership.roleid FROM pg_catalog.pg_auth_members AS membership
			JOIN pg_catalog.pg_roles AS identity ON identity.oid = membership.member
			WHERE identity.rolname = $1
			UNION
			SELECT membership.roleid FROM pg_catalog.pg_auth_members AS membership
			JOIN reachable ON reachable.oid = membership.member)
		SELECT count(*) FROM reachable JOIN pg_catalog.pg_roles AS granted ON granted.oid = reachable.oid
		WHERE granted.rolname <> ALL (ARRAY['pg_monitor','pg_read_all_settings','pg_read_all_stats','pg_stat_scan_tables']))`,
		entry.role.Name, monitorBuiltinRole).Scan(&isMember, &others)
	if err != nil {
		return []Problem{{entry.label, "could not read its role membership"}}
	}
	var problems []Problem
	if !isMember {
		problems = append(problems, Problem{entry.label, "is not a member of pg_monitor"})
	}
	if others > 0 {
		problems = append(problems, Problem{entry.label, "is a member of a role other than pg_monitor"})
	}
	return problems
}

// verifyMonitorTables asserts the login holds no SELECT, INSERT, UPDATE or DELETE on
// any table-like relation in schema public or the River schema, in its own name,
// through PUBLIC or through a role it is a member of.
func verifyMonitorTables(ctx context.Context, q monitorQuerier, entry labelled, riverSchema string) ([]Problem, error) {
	var problems []Problem
	for _, schema := range []string{"public", riverSchema} {
		var count int
		if err := q.QueryRow(ctx, `SELECT count(*) FROM pg_catalog.pg_class AS relation
			JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = relation.relnamespace
			WHERE namespace.nspname = $2 AND relation.relkind IN ('r', 'p', 'v', 'm', 'f')
				AND (pg_catalog.has_table_privilege($1::name, relation.oid, 'SELECT')
					OR pg_catalog.has_table_privilege($1::name, relation.oid, 'INSERT')
					OR pg_catalog.has_table_privilege($1::name, relation.oid, 'UPDATE')
					OR pg_catalog.has_table_privilege($1::name, relation.oid, 'DELETE'))`,
			entry.role.Name, schema).Scan(&count); err != nil {
			return nil, fmt.Errorf("%w: cannot read the %s role's table privileges", ErrProvisioning, entry.label)
		}
		if count > 0 {
			problems = append(problems, Problem{entry.label, "holds SELECT, INSERT, UPDATE or DELETE on a table in schema " + schema})
		}
	}
	return problems, nil
}

// monitorGrantExpected is true for the one grant Apply gives the monitor login in
// its own name: CONNECT on the application database (pg_monitor membership is not
// an ACL entry).
func monitorGrantExpected(grant roleacl.Grant, database string) bool {
	return grant.Class == "database" && grant.Object == database && grant.Privilege == "CONNECT"
}

// kedaGrantExpected is true for the five grants Apply gives the KEDA login.
func kedaGrantExpected(grant roleacl.Grant, database, schema string) bool {
	switch {
	case grant.Class == "database" && grant.Object == database && grant.Privilege == "CONNECT":
		return true
	case grant.Class == "schema" && grant.Object == schema && grant.Privilege == "USAGE":
		return true
	case grant.Class == "relation" && grant.Object == schema+".river_job" && grant.Privilege == "SELECT":
		return true
	case grant.Class == "relation" && grant.Object == kedaSyncRunUnits && grant.Privilege == "SELECT":
		return true
	case grant.Class == "schema" && grant.Object == "public" && grant.Privilege == "USAGE":
		return true
	}
	return false
}

// runtimeGrantOutOfScope is true for a privilege a runtime role holds in its own name
// that NEITHER command grants and that `dho migrate river` never does either: a
// database privilege other than CONNECT on this database, any schema privilege but
// USAGE, and every class river has no reason to touch (tablespaces, foreign data
// wrappers and servers, parameters, large objects, languages), plus anything the
// closure self-check could not explain. Relation, column, function, type, default
// privilege and schema-USAGE grants are `dho migrate river`'s (the queue role holds
// EXECUTE and default privileges in the River schema by design), and whether the
// exact set is right is that role's own readiness check, which the end-to-end test
// runs: judging them here would fail a healthy database.
func runtimeGrantOutOfScope(grant roleacl.Grant, database string) bool {
	switch grant.Class {
	case "database":
		return !(grant.Object == database && grant.Privilege == "CONNECT")
	case "schema":
		return grant.Privilege != "USAGE"
	case "tablespace", "foreign data wrapper", "foreign server", "parameter", "large object", "language",
		roleacl.UnexplainedClass:
		return true
	}
	return false
}

// publicAmbient is what PUBLIC gives every role on a stock database: CONNECT on it and
// USAGE on schema public.
func publicAmbient(grant roleacl.Grant, database string) bool {
	return (grant.Class == "database" && grant.Object == database && grant.Privilege == "CONNECT") ||
		(grant.Class == "schema" && grant.Object == "public" && grant.Privilege == "USAGE")
}

// judgeGrant is the per-role judgement of one enumerated grant; "" means acceptable.
//
//   - KEDA holds exactly CONNECT here, USAGE on the River schema, USAGE on schema
//     public, SELECT on river_job and SELECT on public.sync_run_units. It has no
//     later readiness check, so PUBLIC-derived grants count
//     (ambient CONNECT and USAGE on public excepted; CREATE on public through PUBLIC
//     is the target-wide default the other roles warn about, not a KEDA problem).
//   - The runtime roles: what neither command grants (runtimeGrantOutOfScope), in
//     their own name; PUBLIC-derived ones are the readiness checks' and the
//     CREATE-on-public warning's business.
func judgeGrant(label string, grant roleacl.Grant, database, schema string) string {
	unexpected := "holds an unexpected privilege: " + grant.String()
	// PUBLIC-derived grants are judged the SAME way for every role (lead D2648): PUBLIC is
	// never part of any posture, so beyond the ambient CONNECT on this database and USAGE
	// on schema public (and CREATE on public, the target-wide default reported as a
	// warning elsewhere) a privilege arriving through PUBLIC, relation and column
	// privileges included, is a problem.
	if grant.ViaPublic {
		if publicAmbient(grant, database) || (grant.Class == "schema" && grant.Object == "public" && grant.Privilege == "CREATE") {
			return ""
		}
		return unexpected
	}
	if label == monitorLabel {
		if monitorGrantExpected(grant, database) {
			return ""
		}
		return unexpected
	}
	if label == "keda" {
		if kedaGrantExpected(grant, database, schema) {
			return ""
		}
		return unexpected
	}
	if !runtimeGrantOutOfScope(grant, database) {
		return ""
	}
	return unexpected
}
