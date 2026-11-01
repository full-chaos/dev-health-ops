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

// PushLifecycleFunction is the one function go-api may execute in the auth
// schema (migration 0039), and PushLifecycleFunctionSignature its argument
// list. It runs as PushLifecycleWriterRole and appends one External Push
// lifecycle event to the outbox under the commit guard.
const (
	PushLifecycleFunction          = "append_external_push_lifecycle_event"
	PushLifecycleFunctionSignature = "(text, text, jsonb, jsonb)"
)

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
	if len(o.PushLifecycleCallerRoles) == 0 {
		return fmt.Errorf("%w: the push lifecycle writer role needs at least one caller role", ErrInvalidOptions)
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

// applyPushLifecycleWriter hands the function to the writer role and sets the
// exact grants of the writer and of the callers, in ONE transaction.
//
// ALTER FUNCTION OWNER needs two things PostgreSQL will not waive: the
// migration role must be able to SET ROLE to the new owner (a membership that
// role provisioning creates, see docs), and the new owner must hold CREATE on
// the schema. The writer gets CREATE inside this transaction and loses it
// before the commit, so no committed state shows it and the writer owns
// exactly this function (a posture check proves both afterwards).
func applyPushLifecycleWriter(ctx context.Context, conn pooledConn, options Options, logger *slog.Logger) error {
	if options.PushLifecycleWriterRole == "" {
		logger.InfoContext(ctx, "push lifecycle writer role not configured; the append function stays unexecutable",
			"schema", options.Schema)
		return nil
	}
	schemaID, err := NewValidatedIdentifier(options.Schema)
	if err != nil {
		return err
	}
	writerID, err := NewValidatedIdentifier(options.PushLifecycleWriterRole)
	if err != nil {
		return err
	}
	schema, writer := quoteIdentifier(schemaID), quoteIdentifier(writerID)
	function := schema + "." + PushLifecycleFunction + PushLifecycleFunctionSignature

	var canLogin, isMember bool
	err = conn.QueryRow(ctx, `SELECT r.rolcanlogin, pg_has_role(current_user, r.oid, 'USAGE')
		FROM pg_roles r WHERE r.rolname = $1`, options.PushLifecycleWriterRole).Scan(&canLogin, &isMember)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return fmt.Errorf("%w: role %q does not exist; create it first (CREATE ROLE %s NOLOGIN) and GRANT it to the migration role, "+
			"as a privileged step before this migration (docs: auth control plane deployment)",
			ErrPushLifecycleWriter, options.PushLifecycleWriterRole, options.PushLifecycleWriterRole)
	case err != nil:
		return fmt.Errorf("%w: reading the push lifecycle writer role", ErrMigrationFailed)
	case canLogin:
		return fmt.Errorf("%w: role %q can log in; it must be NOLOGIN", ErrPushLifecycleWriter, options.PushLifecycleWriterRole)
	case !isMember:
		return fmt.Errorf("%w: the migration role is not a member of %q; GRANT %s TO the migration role as a privileged step before this migration",
			ErrPushLifecycleWriter, options.PushLifecycleWriterRole, options.PushLifecycleWriterRole)
	}

	statements := []string{
		fmt.Sprintf(`GRANT CREATE ON SCHEMA %s TO %s`, schema, writer),
		fmt.Sprintf(`ALTER FUNCTION %s OWNER TO %s`, function, writer),
		fmt.Sprintf(`REVOKE CREATE ON SCHEMA %s FROM %s`, schema, writer),
		fmt.Sprintf(`GRANT USAGE ON SCHEMA %s TO %s`, schema, writer),
		fmt.Sprintf(`REVOKE ALL ON ALL TABLES IN SCHEMA %s FROM %s`, schema, writer),
		fmt.Sprintf(`REVOKE ALL ON ALL SEQUENCES IN SCHEMA %s FROM %s`, schema, writer),
	}
	for _, table := range sortedKeys(pushWriterTables) {
		tableID, err := NewValidatedIdentifier(table)
		if err != nil {
			return err
		}
		statements = append(statements, fmt.Sprintf(`GRANT %s ON %s.%s TO %s`,
			strings.Join(pushWriterTables[table], ", "), schema, quoteIdentifier(tableID), writer))
	}
	for _, sequence := range sortedKeys(pushWriterSequences) {
		sequenceID, err := NewValidatedIdentifier(sequence)
		if err != nil {
			return err
		}
		statements = append(statements, fmt.Sprintf(`GRANT %s ON SEQUENCE %s.%s TO %s`,
			strings.Join(pushWriterSequences[sequence], ", "), schema, quoteIdentifier(sequenceID), writer))
	}

	// EXECUTE: the callers and nobody else. PUBLIC, the runtime role and every
	// other session reader are revoked by name, so a drifted grant is corrected.
	statements = append(statements, fmt.Sprintf(`REVOKE ALL ON FUNCTION %s FROM PUBLIC`, function))
	executors := map[string]bool{}
	for _, caller := range options.PushLifecycleCallerRoles {
		executors[caller] = true
	}
	for _, other := range append([]string{options.RuntimeRole}, options.SessionReaderRoles...) {
		if executors[other] {
			continue
		}
		otherID, err := NewValidatedIdentifier(other)
		if err != nil {
			return err
		}
		statements = append(statements, fmt.Sprintf(`REVOKE ALL ON FUNCTION %s FROM %s`, function, quoteIdentifier(otherID)))
	}
	for _, caller := range options.PushLifecycleCallerRoles {
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
		return fmt.Errorf("%w: beginning the push lifecycle writer transaction", ErrMigrationFailed)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := aclguard.Lock(ctx, tx); err != nil {
		return fmt.Errorf("%w: taking the privilege writer lock", ErrMigrationFailed)
	}
	for _, statement := range statements {
		if _, err := tx.Exec(ctx, statement); err != nil {
			return fmt.Errorf("%w: %s: %v", ErrMigrationFailed, statement, redactPGError(err))
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("%w: committing the push lifecycle writer grants", ErrMigrationFailed)
	}
	logger.InfoContext(ctx, "applied push lifecycle writer grants", "schema", options.Schema,
		"writer_role", options.PushLifecycleWriterRole, "callers", len(options.PushLifecycleCallerRoles))
	return nil
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
	if options.PushLifecycleWriterRole == "" {
		return nil, nil
	}
	var violations []Violation
	add := func(kind, schema, object, privilege, detail string) {
		violations = append(violations, Violation{Kind: kind, Schema: schema, Object: object, Privilege: privilege, Detail: detail})
	}
	writer := options.PushLifecycleWriterRole

	var login, super, createdb, createrole, replication, bypass bool
	if err := conn.QueryRow(ctx, `SELECT rolcanlogin, rolsuper, rolcreatedb, rolcreaterole, rolreplication, rolbypassrls
		FROM pg_roles WHERE rolname = $1`, writer).Scan(&login, &super, &createdb, &createrole, &replication, &bypass); err != nil {
		return nil, fmt.Errorf("%w: reading the push lifecycle writer role", ErrMigrationFailed)
	}
	for attribute, set := range map[string]bool{"LOGIN": login, "SUPERUSER": super, "CREATEDB": createdb, "CREATEROLE": createrole, "REPLICATION": replication, "BYPASSRLS": bypass} {
		if set {
			add("role", "", writer, attribute, "the push lifecycle writer role must be NOLOGIN with no attribute")
		}
	}

	rows, err := conn.Query(ctx, `SELECT parent.rolname FROM pg_auth_members m
		JOIN pg_roles member ON member.oid = m.member JOIN pg_roles parent ON parent.oid = m.roleid WHERE member.rolname = $1`, writer)
	if err != nil {
		return nil, fmt.Errorf("%w: reading the push lifecycle writer memberships", ErrMigrationFailed)
	}
	for rows.Next() {
		var parent string
		if err := rows.Scan(&parent); err != nil {
			rows.Close()
			return nil, fmt.Errorf("%w: reading a membership", ErrMigrationFailed)
		}
		add("membership", "", writer, "MEMBER", "the push lifecycle writer role is a member of "+parent)
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
		if schema != options.Schema || name != PushLifecycleFunction {
			add("function", schema, name+"()", "OWNER", "the push lifecycle writer role owns a function other than the append function")
		}
	}
	rows.Close()
	if owned == 0 {
		add("function", options.Schema, PushLifecycleFunction+"()", "OWNER", "the push lifecycle writer role does not own the append function")
	}
	var ownedRelations int
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM pg_class c JOIN pg_roles o ON o.oid = c.relowner WHERE o.rolname = $1`, writer).Scan(&ownedRelations); err != nil {
		return nil, fmt.Errorf("%w: reading the relations the writer owns", ErrMigrationFailed)
	}
	if ownedRelations > 0 {
		add("relation", "", writer, "OWNER", fmt.Sprintf("the push lifecycle writer role owns %d relation(s)", ownedRelations))
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
		allowed := pushWriterTables
		if kind == "S" {
			allowed = pushWriterSequences
		}
		if schema == options.Schema && listContains(allowed[name], privilege) {
			continue
		}
		add("relation", schema, name, privilege, "the push lifecycle writer role holds a privilege outside its declared set")
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
		add("schema", schema, schema, privilege, "the push lifecycle writer role holds a schema privilege outside its declared set")
	}
	rows.Close()

	var databaseCreate bool
	if err := conn.QueryRow(ctx, `SELECT has_database_privilege($1, current_database(), 'CREATE')`, writer).Scan(&databaseCreate); err != nil {
		return nil, fmt.Errorf("%w: reading the writer's database privilege", ErrMigrationFailed)
	}
	if databaseCreate {
		add("database", "", "current_database()", "CREATE", "the push lifecycle writer role can create schemas")
	}

	holders := map[string]bool{}
	for _, caller := range options.PushLifecycleCallerRoles {
		holders[caller] = true
	}
	function := quoteIdentifierRaw(options.Schema) + "." + PushLifecycleFunction + PushLifecycleFunctionSignature
	var public bool
	if err := conn.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_proc p, aclexplode(coalesce(p.proacl, acldefault('f', p.proowner))) a
		WHERE p.oid = $1::regprocedure AND a.grantee = 0 AND a.privilege_type = 'EXECUTE')`, function).Scan(&public); err != nil {
		return nil, fmt.Errorf("%w: reading the append function's grants", ErrMigrationFailed)
	}
	if public {
		add("function", options.Schema, PushLifecycleFunction+"()", "EXECUTE", "PUBLIC can execute the append function")
	}
	rows, err = conn.Query(ctx, `SELECT r.rolname, has_function_privilege(r.oid, $1::regprocedure, 'EXECUTE')
		FROM pg_roles r WHERE r.rolname = ANY($2) AND r.rolname <> $3`,
		function, append(append([]string{options.RuntimeRole}, options.SessionReaderRoles...), options.PushLifecycleCallerRoles...), writer)
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
			add("function", options.Schema, PushLifecycleFunction+"()", "EXECUTE", role+" can execute the append function and is not a caller")
		case !can && holders[role]:
			add("function", options.Schema, PushLifecycleFunction+"()", "EXECUTE", role+" is a caller and cannot execute the append function")
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
