package authschema

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/aclguard"
)

// PushLifecycleFunction is the one function go-api and the expiry sweeper's
// worker login may execute in the auth schema (migration 0044), and PushLifecycleFunctionSignature its argument
// list. It runs as PushLifecycleWriterRole and appends one External Push
// lifecycle event to the outbox under the commit guard.
const (
	PushLifecycleFunction          = "append_external_push_lifecycle_event"
	PushLifecycleFunctionSignature = "(text, text, jsonb, jsonb)"
)

// PushLifecycleCallerCount is the number of runtime logins that may execute the
// function: go-api's API database role and the worker domain login.
const PushLifecycleCallerCount = 2

// ErrPushLifecycleWriter reports a push lifecycle writer role that is missing
// or not set up as the migration needs; the message says which step to run.
var ErrPushLifecycleWriter = errors.New("push lifecycle writer role")

// pushWriterTables is the writer role's complete table privilege set, and
// pushWriterSequences its complete sequence privilege set. A privilege outside
// them is a posture violation (pushWriterViolations).
var (
	pushWriterTables    = map[string][]string{"auth_outbox_events": {"INSERT"}, "aggregate_revisions": {"INSERT", "SELECT", "UPDATE"}}
	pushWriterSequences = map[string][]string{"auth_outbox_events_id_seq": {"USAGE"}}
)

func (o Options) validatePushLifecycle(migrationRole string) error {
	if o.PushLifecycleWriterRole == "" {
		if len(o.PushLifecycleCallerRoles) > 0 {
			return fmt.Errorf("%w: caller roles are set without a writer role", ErrInvalidOptions)
		}
		return nil
	}
	if err := ValidateIdentifier(o.PushLifecycleWriterRole); err != nil {
		return err
	}
	if n := len(o.PushLifecycleCallerRoles); n != PushLifecycleCallerCount {
		return fmt.Errorf("%w: the push lifecycle writer role needs exactly %d caller roles (go-api's login and the worker domain login), got %d",
			ErrInvalidOptions, PushLifecycleCallerCount, n)
	}
	if w := o.PushLifecycleWriterRole; w == o.RuntimeRole || (migrationRole != "" && w == migrationRole) {
		return fmt.Errorf("%w: the push lifecycle writer role %q must differ from the runtime role and the migration role", ErrInvalidOptions, w)
	}
	seen := map[string]bool{}
	for _, caller := range o.PushLifecycleCallerRoles {
		if err := ValidateIdentifier(caller); err != nil {
			return err
		}
		if caller == o.PushLifecycleWriterRole || caller == o.RuntimeRole || (migrationRole != "" && caller == migrationRole) || seen[caller] {
			return fmt.Errorf("%w: push lifecycle caller role %q must differ from the writer, runtime and migration roles and every other caller", ErrInvalidOptions, caller)
		}
		seen[caller] = true
	}
	return nil
}

// functionWriter is one NOLOGIN role that owns exactly one SECURITY DEFINER append function of the auth schema, the
// logins that may execute it, and the role's complete table and sequence privilege sets.
type functionWriter struct {
	label     string
	err       error
	role      string
	callers   []string
	function  string
	signature string
	tables    map[string][]string
	sequences map[string][]string
}

func (o Options) pushWriter() functionWriter {
	return functionWriter{
		label: "push lifecycle", err: ErrPushLifecycleWriter, role: o.PushLifecycleWriterRole, callers: o.PushLifecycleCallerRoles,
		function: PushLifecycleFunction, signature: PushLifecycleFunctionSignature, tables: pushWriterTables, sequences: pushWriterSequences,
	}
}

func (o Options) repoIdentityWriter() functionWriter {
	return functionWriter{
		label: "repository identity", err: ErrRepoIdentityWriter, role: o.RepoIdentityWriterRole, callers: o.RepoIdentityCallerRoles,
		function: RepoIdentityFunction, signature: RepoIdentityFunctionSignature, tables: repoIdentityWriterTables, sequences: repoIdentityWriterSequences,
	}
}

// functionWriters are every function writer, in a fixed order.
func (o Options) functionWriters() []functionWriter {
	return []functionWriter{o.pushWriter(), o.repoIdentityWriter()}
}

func applyFunctionWriters(ctx context.Context, conn pooledConn, options Options, logger *slog.Logger) error {
	for _, w := range options.functionWriters() {
		if err := applyFunctionWriter(ctx, conn, options, w, logger); err != nil {
			return err
		}
	}
	return nil
}

func functionWritersViolations(ctx context.Context, conn pooledConn, options Options) ([]Violation, error) {
	var out []Violation
	for _, w := range options.functionWriters() {
		violations, err := functionWriterViolations(ctx, conn, options, w)
		if err != nil {
			return nil, err
		}
		out = append(out, violations...)
	}
	return out, nil
}

// applyFunctionWriter hands the function to the writer role and sets the
// exact grants of the writer and of the callers, in ONE transaction.
//
// ALTER FUNCTION OWNER needs two things PostgreSQL will not waive: the
// migration role must be able to SET ROLE to the new owner (a membership that
// role provisioning creates, see docs), and the new owner must hold CREATE on
// the schema. The writer gets CREATE inside this transaction and loses it
// before the commit, so no committed state shows it and the writer owns
// exactly this function (a posture check proves both afterwards).
func applyFunctionWriter(ctx context.Context, conn pooledConn, options Options, w functionWriter, logger *slog.Logger) error {
	if w.role == "" {
		logger.InfoContext(ctx, w.label+" writer role not configured; the append function stays unexecutable",
			"schema", options.Schema)
		return nil
	}
	schemaID, err := NewValidatedIdentifier(options.Schema)
	if err != nil {
		return err
	}
	writerID, err := NewValidatedIdentifier(w.role)
	if err != nil {
		return err
	}
	schema, writer := quoteIdentifier(schemaID), quoteIdentifier(writerID)
	function := schema + "." + w.function + w.signature

	var canLogin, isMember bool
	err = conn.QueryRow(ctx, `SELECT r.rolcanlogin, pg_has_role(current_user, r.oid, 'USAGE')
		FROM pg_roles r WHERE r.rolname = $1`, w.role).Scan(&canLogin, &isMember)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return fmt.Errorf("%w: role %q does not exist; create it first (CREATE ROLE %s NOLOGIN) and GRANT it to the migration role, "+
			"as a privileged step before this migration (docs: auth control plane deployment)",
			w.err, w.role, w.role)
	case err != nil:
		return fmt.Errorf("%w: reading the %s writer role", ErrMigrationFailed, w.label)
	case canLogin:
		return fmt.Errorf("%w: role %q can log in; it must be NOLOGIN", w.err, w.role)
	case !isMember:
		return fmt.Errorf("%w: the migration role is not a member of %q; GRANT %s TO the migration role as a privileged step before this migration",
			w.err, w.role, w.role)
	}

	statements := []string{
		fmt.Sprintf(`GRANT CREATE ON SCHEMA %s TO %s`, schema, writer),
		fmt.Sprintf(`ALTER FUNCTION %s OWNER TO %s`, function, writer),
		fmt.Sprintf(`REVOKE CREATE ON SCHEMA %s FROM %s`, schema, writer),
		fmt.Sprintf(`GRANT USAGE ON SCHEMA %s TO %s`, schema, writer),
		fmt.Sprintf(`REVOKE ALL ON ALL TABLES IN SCHEMA %s FROM %s`, schema, writer),
		fmt.Sprintf(`REVOKE ALL ON ALL SEQUENCES IN SCHEMA %s FROM %s`, schema, writer),
	}
	for _, table := range sortedKeys(w.tables) {
		tableID, err := NewValidatedIdentifier(table)
		if err != nil {
			return err
		}
		statements = append(statements, fmt.Sprintf(`GRANT %s ON %s.%s TO %s`,
			strings.Join(w.tables[table], ", "), schema, quoteIdentifier(tableID), writer))
	}
	for _, sequence := range sortedKeys(w.sequences) {
		sequenceID, err := NewValidatedIdentifier(sequence)
		if err != nil {
			return err
		}
		statements = append(statements, fmt.Sprintf(`GRANT %s ON SEQUENCE %s.%s TO %s`,
			strings.Join(w.sequences[sequence], ", "), schema, quoteIdentifier(sequenceID), writer))
	}

	// EXECUTE: the callers and nobody else. PUBLIC, the runtime role and every
	// other session reader are revoked by name, so a drifted grant is corrected;
	// any other direct grantee found in the function's ACL is revoked inside the
	// transaction, after the owner change (revokeUndeclaredExecuteGrants).
	preStatements := statements
	statements = nil
	statements = append(statements, fmt.Sprintf(`REVOKE ALL ON FUNCTION %s FROM PUBLIC`, function))
	executors := map[string]bool{}
	for _, caller := range w.callers {
		executors[caller] = true
	}
	for _, other := range append(append([]string{options.RuntimeRole}, options.SessionReaderRoles...), options.ProjectionReaderRoles...) {
		if executors[other] {
			continue
		}
		otherID, err := NewValidatedIdentifier(other)
		if err != nil {
			return err
		}
		statements = append(statements, fmt.Sprintf(`REVOKE ALL ON FUNCTION %s FROM %s`, function, quoteIdentifier(otherID)))
	}
	for _, caller := range w.callers {
		callerID, err := NewValidatedIdentifier(caller)
		if err != nil {
			return err
		}
		role := quoteIdentifier(callerID)
		statements = append(statements,
			fmt.Sprintf(`GRANT USAGE ON SCHEMA %s TO %s`, schema, role),
			fmt.Sprintf(`GRANT EXECUTE ON FUNCTION %s TO %s`, function, role))
	}

	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("%w: beginning the %s writer transaction", ErrMigrationFailed, w.label)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := aclguard.Lock(ctx, tx); err != nil {
		return fmt.Errorf("%w: taking the privilege writer lock", ErrMigrationFailed)
	}
	for _, statement := range preStatements {
		if _, err := tx.Exec(ctx, statement); err != nil {
			return fmt.Errorf("%w: %s: %v", ErrMigrationFailed, statement, redactPGError(err))
		}
	}
	undeclared, err := undeclaredExecuteGrantees(ctx, tx, function, w.callers)
	if err != nil {
		return err
	}
	for _, grant := range undeclared {
		statement := grant.revoke
		if _, err := tx.Exec(ctx, statement); err != nil {
			return fmt.Errorf("%w: %s: %v", ErrMigrationFailed, statement, redactPGError(err))
		}
	}
	for _, statement := range statements {
		if _, err := tx.Exec(ctx, statement); err != nil {
			return fmt.Errorf("%w: %s: %v", ErrMigrationFailed, statement, redactPGError(err))
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("%w: committing the %s writer grants", ErrMigrationFailed, w.label)
	}
	logger.InfoContext(ctx, "applied "+w.label+" writer grants", "schema", options.Schema,
		"writer_role", w.role, "callers", len(w.callers))
	return nil
}

// executeGranteeQuery lists every named role (PUBLIC excluded) that holds a
// direct EXECUTE entry in the function's ACL, with the function's owner.
const executeGranteeQuery = `SELECT r.rolname, a.grantee = p.proowner, format('REVOKE ALL ON FUNCTION %s FROM %I', $2::text, r.rolname)
	FROM pg_proc p CROSS JOIN LATERAL aclexplode(coalesce(p.proacl, acldefault('f', p.proowner))) a
	JOIN pg_roles r ON r.oid = a.grantee
	WHERE p.oid = $1::regprocedure AND a.privilege_type = 'EXECUTE' ORDER BY r.rolname`

type queryer interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// undeclaredExecuteGrantees returns, for each role that holds a direct EXECUTE
// grant on the function and is neither a declared caller nor its owner, the
// REVOKE statement the server formats (%I quotes any role name safely).
type undeclaredGrant struct{ role, revoke string }

func undeclaredExecuteGrantees(ctx context.Context, q queryer, function string, callers []string) ([]undeclaredGrant, error) {
	rows, err := q.Query(ctx, executeGranteeQuery, function, function)
	if err != nil {
		return nil, fmt.Errorf("%w: reading the append function's ACL", ErrMigrationFailed)
	}
	defer rows.Close()
	var out []undeclaredGrant
	for rows.Next() {
		var name, revoke string
		var owner bool
		if err := rows.Scan(&name, &owner, &revoke); err != nil {
			return nil, fmt.Errorf("%w: reading an ACL entry of the append function", ErrMigrationFailed)
		}
		if !owner && !listContains(callers, name) {
			out = append(out, undeclaredGrant{name, revoke})
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%w: reading the append function's ACL", ErrMigrationFailed)
	}
	return out, nil
}

func sortedKeys(m map[string][]string) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// pushWriterViolations is the closed-world check of the writer role: it must
// be NOLOGIN with no attribute or membership, own exactly the one function,
// hold exactly pushWriterTables and pushWriterSequences and USAGE on the auth
// schema, and CREATE nowhere; and EXECUTE on the function must be held by
// exactly the callers. Any other grant to the role fails the run.
func pushWriterViolations(ctx context.Context, conn pooledConn, options Options) ([]Violation, error) {
	return functionWriterViolations(ctx, conn, options, options.pushWriter())
}

// functionWriterViolations is the closed-world check of one function writer role (pushWriterViolations names the
// rules).
func functionWriterViolations(ctx context.Context, conn pooledConn, options Options, w functionWriter) ([]Violation, error) {
	if w.role == "" {
		return nil, nil
	}
	var violations []Violation
	add := func(kind, schema, object, privilege, detail string) {
		violations = append(violations, Violation{Kind: kind, Schema: schema, Object: object, Privilege: privilege, Detail: detail})
	}
	writer := w.role
	label := w.label

	var login, super, createdb, createrole, replication, bypass bool
	if err := conn.QueryRow(ctx, `SELECT rolcanlogin, rolsuper, rolcreatedb, rolcreaterole, rolreplication, rolbypassrls
		FROM pg_roles WHERE rolname = $1`, writer).Scan(&login, &super, &createdb, &createrole, &replication, &bypass); err != nil {
		return nil, fmt.Errorf("%w: reading the %s writer role", ErrMigrationFailed, label)
	}
	for attribute, set := range map[string]bool{"LOGIN": login, "SUPERUSER": super, "CREATEDB": createdb, "CREATEROLE": createrole, "REPLICATION": replication, "BYPASSRLS": bypass} {
		if set {
			add("role", "", writer, attribute, "the "+label+" writer role must be NOLOGIN with no attribute")
		}
	}

	rows, err := conn.Query(ctx, `SELECT parent.rolname FROM pg_auth_members m
		JOIN pg_roles member ON member.oid = m.member JOIN pg_roles parent ON parent.oid = m.roleid WHERE member.rolname = $1`, writer)
	if err != nil {
		return nil, fmt.Errorf("%w: reading the %s writer memberships", ErrMigrationFailed, label)
	}
	for rows.Next() {
		var parent string
		if err := rows.Scan(&parent); err != nil {
			rows.Close()
			return nil, fmt.Errorf("%w: reading a membership", ErrMigrationFailed)
		}
		add("membership", "", writer, "MEMBER", "the "+label+" writer role is a member of "+parent)
	}
	rows.Close()

	rows, err = conn.Query(ctx, `SELECT n.nspname, p.proname FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
		JOIN pg_roles o ON o.oid = p.proowner WHERE o.rolname = $1`, writer)
	if err != nil {
		return nil, fmt.Errorf("%w: reading the objects the writer owns", ErrMigrationFailed)
	}
	owned := 0
	for rows.Next() {
		var schema, name string
		if err := rows.Scan(&schema, &name); err != nil {
			rows.Close()
			return nil, fmt.Errorf("%w: reading an owned function", ErrMigrationFailed)
		}
		owned++
		if schema != options.Schema || name != w.function {
			add("function", schema, name+"()", "OWNER", "the "+label+" writer role owns a function other than the append function")
		}
	}
	rows.Close()
	if owned == 0 {
		add("function", options.Schema, w.function+"()", "OWNER", "the "+label+" writer role does not own the append function")
	}
	var ownedRelations int
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM pg_class c JOIN pg_roles o ON o.oid = c.relowner WHERE o.rolname = $1`, writer).Scan(&ownedRelations); err != nil {
		return nil, fmt.Errorf("%w: reading the relations the writer owns", ErrMigrationFailed)
	}
	if ownedRelations > 0 {
		add("relation", "", writer, "OWNER", fmt.Sprintf("the "+label+" writer role owns %d relation(s)", ownedRelations))
	}

	rows, err = conn.Query(ctx, `SELECT n.nspname, c.relname, c.relkind::text, priv
		FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		CROSS JOIN unnest(ARRAY['SELECT','INSERT','UPDATE','DELETE','TRUNCATE','REFERENCES','TRIGGER','USAGE']) AS priv
		WHERE `+systemSchemaFilter+` AND c.relkind IN ('r','p','v','m','f','S')
		  AND ((c.relkind = 'S' AND priv IN ('USAGE','SELECT','UPDATE') AND has_sequence_privilege($1, c.oid, priv))
		    OR (c.relkind <> 'S' AND priv <> 'USAGE' AND has_table_privilege($1, c.oid, priv)))`, writer)
	if err != nil {
		return nil, fmt.Errorf("%w: reading the writer's effective privileges", ErrMigrationFailed)
	}
	for rows.Next() {
		var schema, name, kind, privilege string
		if err := rows.Scan(&schema, &name, &kind, &privilege); err != nil {
			rows.Close()
			return nil, fmt.Errorf("%w: reading a privilege", ErrMigrationFailed)
		}
		allowed := w.tables
		if kind == "S" {
			allowed = w.sequences
		}
		if schema == options.Schema && listContains(allowed[name], privilege) {
			continue
		}
		add("relation", schema, name, privilege, "the "+label+" writer role holds a privilege outside its declared set")
	}
	rows.Close()

	rows, err = conn.Query(ctx, `SELECT n.nspname, priv FROM pg_namespace n
		CROSS JOIN unnest(ARRAY['USAGE','CREATE']) AS priv
		WHERE `+systemSchemaFilter+` AND has_schema_privilege($1, n.oid, priv)`, writer)
	if err != nil {
		return nil, fmt.Errorf("%w: reading the writer's schema privileges", ErrMigrationFailed)
	}
	for rows.Next() {
		var schema, privilege string
		if err := rows.Scan(&schema, &privilege); err != nil {
			rows.Close()
			return nil, fmt.Errorf("%w: reading a schema privilege", ErrMigrationFailed)
		}
		if schema == options.Schema && privilege == "USAGE" {
			continue
		}
		if schema == "public" && privilege == "USAGE" {
			continue
		}
		add("schema", schema, schema, privilege, "the "+label+" writer role holds a schema privilege outside its declared set")
	}
	rows.Close()

	var databaseCreate bool
	if err := conn.QueryRow(ctx, `SELECT has_database_privilege($1, current_database(), 'CREATE')`, writer).Scan(&databaseCreate); err != nil {
		return nil, fmt.Errorf("%w: reading the writer's database privilege", ErrMigrationFailed)
	}
	if databaseCreate {
		add("database", "", "current_database()", "CREATE", "the "+label+" writer role can create schemas")
	}

	holders := map[string]bool{}
	for _, caller := range w.callers {
		holders[caller] = true
	}
	function := quoteIdentifierRaw(options.Schema) + "." + w.function + w.signature
	var public bool
	if err := conn.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_proc p, aclexplode(coalesce(p.proacl, acldefault('f', p.proowner))) a
		WHERE p.oid = $1::regprocedure AND a.grantee = 0 AND a.privilege_type = 'EXECUTE')`, function).Scan(&public); err != nil {
		return nil, fmt.Errorf("%w: reading the append function's grants", ErrMigrationFailed)
	}
	if public {
		add("function", options.Schema, w.function+"()", "EXECUTE", "PUBLIC can execute the append function")
	}
	undeclared, err := undeclaredExecuteGrantees(ctx, conn, function, w.callers)
	if err != nil {
		return nil, err
	}
	for _, grant := range undeclared {
		role := grant.role
		add("function", options.Schema, w.function+"()", "EXECUTE", role+" holds a direct EXECUTE grant on the append function and is not a caller")
	}
	rows, err = conn.Query(ctx, `SELECT r.rolname, has_function_privilege(r.oid, $1::regprocedure, 'EXECUTE')
		FROM pg_roles r WHERE r.rolname = ANY($2) AND r.rolname <> $3`,
		function, append(append(append(append([]string{options.RuntimeRole}, options.SessionReaderRoles...), options.ProjectionReaderRoles...), options.PushLifecycleCallerRoles...), options.RepoIdentityCallerRoles...), writer)
	if err != nil {
		return nil, fmt.Errorf("%w: reading who can execute the append function", ErrMigrationFailed)
	}
	for rows.Next() {
		var role string
		var can bool
		if err := rows.Scan(&role, &can); err != nil {
			rows.Close()
			return nil, fmt.Errorf("%w: reading an execute privilege", ErrMigrationFailed)
		}
		switch {
		case can && !holders[role]:
			add("function", options.Schema, w.function+"()", "EXECUTE", role+" can execute the append function and is not a caller")
		case !can && holders[role]:
			add("function", options.Schema, w.function+"()", "EXECUTE", role+" is a caller and cannot execute the append function")
		}
	}
	rows.Close()
	return violations, nil
}

func listContains(list []string, want string) bool {
	for _, item := range list {
		if item == want {
			return true
		}
	}
	return false
}

func quoteIdentifierRaw(name string) string { return `"` + name + `"` }
