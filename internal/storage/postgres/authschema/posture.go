package authschema

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
)

// EscalationPath is one way the runtime role could obtain authority the
// manifest does not declare.
//
// These are deliberately REPORTED rather than silently repaired. Role
// memberships and database-level grants are cluster and database scoped: they
// are an operator's or a DBA's, not this migrator's, and a schema tool that
// quietly rewrites them would be doing something far more surprising than the
// problem it fixes. Refusing loudly, naming the exact object and the exact
// statement that would remove it, leaves the decision where it belongs while
// making it impossible for a run to report success over a broken posture.
type EscalationPath struct {
	Kind   string
	Detail string
	Remedy string
}

func (e EscalationPath) String() string {
	return fmt.Sprintf("%s: %s (remedy: %s)", e.Kind, e.Detail, e.Remedy)
}

// ErrRuntimeRoleCanEscalate reports a runtime role that retains authority
// beyond RuntimePosture after grants were applied.
var ErrRuntimeRoleCanEscalate = fmt.Errorf("%w: the runtime role can escalate beyond its declared posture", ErrMigrationFailed)

// VerifyRuntimePosture re-derives, against the live database, whether the
// runtime role can reach authority the manifest does not declare.
//
// ApplyRuntimeGrants returning nil proves the statements it issued succeeded.
// It does NOT prove the role holds only what the manifest declares, because
// object-level grants are not the only route to authority — and codex round 1
// on this PR demonstrated exactly that, twice, against a live server: a
// pre-existing `GRANT <migration role> TO <runtime role>` survived a
// successful reapply, after which the runtime could `SET ROLE` to the
// object-owning migration role and CREATE a table; and a direct
// `GRANT CREATE ON DATABASE` likewise survived and let it create a schema
// outside auth.
//
// So "the migration returned no error" and "the runtime role owns no DDL" are
// two different claims, and only this check proves the second. It is the same
// shape as cmd/dev-health-worker-migrate's executed grant-posture gate, for
// the same reason: a posture asserted by the granting code is a posture nobody
// has verified.
func VerifyRuntimePosture(ctx context.Context, conn *pgx.Conn, options Options) ([]EscalationPath, error) {
	if err := ValidateIdentifier(options.Schema); err != nil {
		return nil, err
	}
	if err := ValidateIdentifier(options.RuntimeRole); err != nil {
		return nil, err
	}
	var paths []EscalationPath

	// 1. Role membership. Inheriting or assuming another role carries that
	//    role's privileges wholesale, which defeats every object-level grant
	//    this package makes. The migration role is the sharpest case, since it
	//    OWNS every object in the schema, but any membership is a route.
	rows, err := conn.Query(ctx, `
		SELECT r.rolname
		FROM pg_auth_members m
		JOIN pg_roles r ON r.oid = m.roleid
		WHERE m.member = (SELECT oid FROM pg_roles WHERE rolname = $1)
		ORDER BY r.rolname`, options.RuntimeRole)
	if err != nil {
		return nil, fmt.Errorf("%w: reading role memberships", ErrMigrationFailed)
	}
	var memberships []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			rows.Close()
			return nil, fmt.Errorf("%w: reading a role membership", ErrMigrationFailed)
		}
		memberships = append(memberships, name)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%w: reading role memberships", ErrMigrationFailed)
	}
	sort.Strings(memberships)
	for _, name := range memberships {
		paths = append(paths, EscalationPath{
			Kind:   "role_membership",
			Detail: fmt.Sprintf("%q is a member of %q", options.RuntimeRole, name),
			Remedy: fmt.Sprintf("REVOKE %q FROM %q", name, options.RuntimeRole),
		})
	}

	// 2. Database-level CREATE. It lets the role make its own schema and own
	//    everything in it, which is DDL by another name.
	//
	//    CONNECT and TEMPORARY are deliberately NOT checked, for different
	//    reasons. CONNECT is required by the runtime. TEMPORARY was checked in
	//    the first version of this function and had to be removed: PostgreSQL
	//    grants it to PUBLIC on every database by default, so EVERY role holds
	//    it and the check fired on every legitimate deployment -- caught by
	//    this package's own clean-path posture test before it was committed.
	//    It is also not a route to the auth schema: a temp table lives in
	//    pg_temp, is session-scoped, and cannot touch a schema-qualified
	//    object. A check that fires on a default privilege every role has is
	//    not a check; it is noise that teaches an operator to ignore the
	//    output.
	var holdsCreate bool
	if err := conn.QueryRow(ctx,
		`SELECT has_database_privilege($1, current_database(), 'CREATE')`,
		options.RuntimeRole,
	).Scan(&holdsCreate); err != nil {
		return nil, fmt.Errorf("%w: reading database privileges", ErrMigrationFailed)
	}
	if holdsCreate {
		paths = append(paths, EscalationPath{
			Kind:   "database_privilege",
			Detail: fmt.Sprintf("%q holds CREATE on the current database", options.RuntimeRole),
			Remedy: fmt.Sprintf("REVOKE CREATE ON DATABASE <database> FROM %q", options.RuntimeRole),
		})
	}

	// 3. Ownership inside the auth schema. An owner holds DDL over its own
	//    objects permanently and no REVOKE can take that away.
	var owned int
	if err := conn.QueryRow(ctx, `
		SELECT count(*)
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		JOIN pg_roles r ON r.oid = c.relowner
		WHERE n.nspname = $1 AND r.rolname = $2`,
		options.Schema, options.RuntimeRole,
	).Scan(&owned); err != nil {
		return nil, fmt.Errorf("%w: reading object ownership", ErrMigrationFailed)
	}
	if owned > 0 {
		paths = append(paths, EscalationPath{
			Kind:   "object_ownership",
			Detail: fmt.Sprintf("%q owns %d object(s) in schema %q", options.RuntimeRole, owned, options.Schema),
			Remedy: "re-create the schema with the migration role as owner; ownership cannot be revoked",
		})
	}

	// 4. CREATE on the auth schema itself. ApplyRuntimeGrants revokes this,
	//    so its presence means something re-granted it after the fact.
	var schemaCreate bool
	if err := conn.QueryRow(ctx,
		`SELECT has_schema_privilege($1, $2, 'CREATE')`,
		options.RuntimeRole, options.Schema,
	).Scan(&schemaCreate); err != nil {
		return nil, fmt.Errorf("%w: reading schema privileges", ErrMigrationFailed)
	}
	if schemaCreate {
		paths = append(paths, EscalationPath{
			Kind:   "schema_privilege",
			Detail: fmt.Sprintf("%q holds CREATE on schema %q", options.RuntimeRole, options.Schema),
			Remedy: fmt.Sprintf("REVOKE CREATE ON SCHEMA %q FROM %q", options.Schema, options.RuntimeRole),
		})
	}

	return paths, nil
}

// describeEscalation renders paths for an error message, one per line.
func describeEscalation(paths []EscalationPath) string {
	rendered := make([]string, 0, len(paths))
	for _, path := range paths {
		rendered = append(rendered, "  - "+path.String())
	}
	return strings.Join(rendered, "\n")
}
