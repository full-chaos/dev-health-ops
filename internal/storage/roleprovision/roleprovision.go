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
//   - CONNECT on the application database; TEMPORARY revoked (from PUBLIC too, for
//     the mandatory block and the api and query-api blocks); USAGE and no CREATE on
//     schema public, for the role only;
//   - KEDA: USAGE on the River schema and SELECT on river_job, nothing else.
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
	// Keda is the KEDA postgresql scaler's read-only login. RiverSchema names the
	// schema it may read (default "river").
	Keda        Role
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
		{"api", o.API}, {"query_api", o.QueryAPI}, {"keda", o.Keda},
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
	// same rule applies here, up front. The KEDA login is never read by river and
	// stays free-form.
	for _, entry := range o.configured() {
		if entry.label != "keda" && !riverstore.ValidIdentifier(entry.role.Name) {
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

	step := func(what string, err error) error {
		if err != nil {
			// The cause is the server's text; it can quote a statement, and a
			// CREATE ROLE statement carries the password, so it is never wrapped.
			return fmt.Errorf("%w: %s", ErrProvisioning, what)
		}
		return nil
	}

	// The mandatory block, in the script's order.
	// Errors name the role LABEL, never its name: Compose defaults each role's
	// password to the role's own name, so a name in an error or log line can be a
	// password.
	for _, entry := range []labelled{{"domain", options.Domain}, {"queue", options.Queue}, {"coordinator", options.Coordinator}} {
		if err := step("create the "+entry.label+" role", run.generated(ctx, createRoleSQL, entry.role)); err != nil {
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
		if err := step("create the "+entry.label+" role", run.generated(ctx, createRoleSQL, role)); err != nil {
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
		if err := step("create the keda role", run.generated(ctx, createRoleSQL, options.Keda)); err != nil {
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
// schema public, and -- KEDA -- can read river_job. A CREATE on schema public that
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
		if entry.label != "keda" {
			// The readiness identity check (roleacl.IdentityPredicateSQL) refuses a
			// role that is a member of another role or owns any object; a green
			// closing check must not promise what readiness will then refuse.
			var memberFree, ownsNothing bool
			if err := q.QueryRow(ctx, `SELECT `+roleacl.MembershipFreeSQL+`, `+roleacl.OwnsNothingSQL, entry.role.Name).Scan(&memberFree, &ownsNothing); err != nil {
				return nil, nil, fmt.Errorf("%w: cannot read role membership and ownership", ErrProvisioning)
			}
			if !memberFree {
				problems = append(problems, Problem{entry.label, "is a member of another role (readiness refuses it)"})
			}
			if !ownsNothing {
				problems = append(problems, Problem{entry.label, "owns an object (readiness refuses it)"})
			}
			// Read the SAME enumeration and closure self-check the readiness checks
			// use, so a privilege KIND nobody thought to list cannot hide; report what
			// neither this command nor `dho migrate river` grants (runtimeGrantOutOfScope).
			grants, err := roleacl.Enumerate(ctx, q, entry.role.Name)
			if err != nil {
				return nil, nil, fmt.Errorf("%w: cannot enumerate the %s role's grants", ErrProvisioning, entry.label)
			}
			for _, grant := range grants {
				if grant.ViaPublic || grant.Class == "setting" || !runtimeGrantOutOfScope(grant, database) {
					continue
				}
				problems = append(problems, Problem{entry.label, "holds an unexpected privilege: " + grant.String()})
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
		if entry.label == "keda" {
			// Read-only in EFFECT: a role membership hides privileges from the role's own
			// ACL entries, so KEDA is membership-free like the runtime roles.
			var kedaMemberFree bool
			if err := q.QueryRow(ctx, `SELECT `+roleacl.MembershipFreeSQL, entry.role.Name).Scan(&kedaMemberFree); err != nil {
				return nil, nil, fmt.Errorf("%w: cannot read the KEDA role's memberships", ErrProvisioning)
			}
			if !kedaMemberFree {
				problems = append(problems, Problem{entry.label, "is a member of another role (privileges it inherits are invisible to its own grants)"})
			}
			var readsJobs bool
			if err := q.QueryRow(ctx, `SELECT has_schema_privilege($1::name, $2::name, 'USAGE')
				AND has_table_privilege($1::name, format('%I.river_job', $2::text), 'SELECT')`,
				entry.role.Name, options.schema()).Scan(&readsJobs); err != nil {
				return nil, nil, fmt.Errorf("%w: cannot read the KEDA role's River privileges", ErrProvisioning)
			}
			if !readsJobs {
				problems = append(problems, Problem{entry.label, "cannot SELECT river_job"})
			}
			// Read-only means read-only: every privilege the login holds in its own
			// name must be one of the three Apply grants. Apply, like the script,
			// never revokes an extra (it would be a guess about someone else's
			// grant), so an extra is reported here for a human to remove.
			grants, err := roleacl.Enumerate(ctx, q, entry.role.Name)
			if err != nil {
				return nil, nil, fmt.Errorf("%w: cannot enumerate the KEDA role's grants", ErrProvisioning)
			}
			for _, grant := range grants {
				if grant.Class == "setting" || kedaGrantExpected(grant, database, options.schema()) {
					continue
				}
				// PUBLIC-derived grants count for KEDA (it has no later readiness check):
				// what PUBLIC hands EVERY role (CONNECT here, USAGE on public) is ambient,
				// CREATE on public is the target-wide default the others warn about, and
				// anything else it holds through PUBLIC is a problem like a direct grant.
				if grant.ViaPublic {
					if publicAmbient(grant, database) {
						continue
					}
					if grant.Class == "schema" && grant.Object == "public" && grant.Privilege == "CREATE" {
						continue
					}
				}
				problems = append(problems, Problem{entry.label, "holds an unexpected privilege: " + grant.String()})
			}
		}
	}
	return problems, warnings, nil
}

// kedaGrantExpected is true for the three grants Apply gives the KEDA login.
func kedaGrantExpected(grant roleacl.Grant, database, schema string) bool {
	switch {
	case grant.Class == "database" && grant.Object == database && grant.Privilege == "CONNECT":
		return true
	case grant.Class == "schema" && grant.Object == schema && grant.Privilege == "USAGE":
		return true
	case grant.Class == "relation" && grant.Object == schema+".river_job" && grant.Privilege == "SELECT":
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

// publicAmbient is what PUBLIC gives every role on a stock database.
func publicAmbient(grant roleacl.Grant, database string) bool {
	return (grant.Class == "database" && grant.Object == database && grant.Privilege == "CONNECT") ||
		(grant.Class == "schema" && grant.Object == "public" && grant.Privilege == "USAGE")
}
