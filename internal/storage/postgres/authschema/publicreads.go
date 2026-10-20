package authschema

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/jackc/pgx/v5"
)

// ColumnGrant is one table's column-level SELECT entry in the runtime role's
// read-only access to the ops (public) schema.
type ColumnGrant struct {
	Table   string
	Columns []string
}

// publicSchema is the ops-owned schema the auth runtime reads until the users
// cutover. It is fixed: the auth schema name is configurable, this one is not.
const publicSchema = "public"

// PublicReadColumns is the complete, closed list of ops-schema columns the
// auth runtime role may SELECT, and nothing else in that schema, until the
// users cutover revokes it. It is column-level so a table's other columns stay
// unreadable; no INSERT, UPDATE, DELETE or REFERENCES is ever granted.
//
// password_hash and encrypted_secrets are readable by decision of the
// identity-store plan: the store verifies passwords and decrypts provider
// secrets itself while the ops tables remain the system of record.
func PublicReadColumns() []ColumnGrant {
	return []ColumnGrant{
		{Table: "users", Columns: []string{
			"id", "email", "password_hash", "full_name", "username", "auth_provider", "auth_provider_id",
			"is_active", "is_verified", "is_superuser", "token_version",
		}},
		{Table: "memberships", Columns: []string{"user_id", "org_id", "role"}},
		{Table: "sso_providers", Columns: []string{
			"id", "org_id", "protocol", "status", "allow_idp_initiated", "auto_provision_users",
			"config", "allowed_domains", "encrypted_secrets", "is_default", "created_at",
		}},
		{Table: "organizations", Columns: []string{"id", "slug", "name", "is_active", "tier"}},
		{Table: "saml_assertion_replays", Columns: []string{"provider_id", "assertion_id", "expires_at"}},
		{Table: "org_licenses", Columns: []string{"org_id", "tier", "features_override"}},
	}
}

// SessionReaderColumns is what a session reader role may SELECT in the auth
// schema: column-level, on the sessions table only.
func SessionReaderColumns() ColumnGrant {
	return ColumnGrant{Table: "sessions", Columns: []string{"id", "principal_id", "revoked_at"}}
}

// rowQuerier1 is the slice of a connection the relation lookup needs.
type rowQuerier1 interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// publicRelationKind reports the relkind of public.<table> ("" when no such
// relation exists). A view or foreign table of the same name is a different
// object from the ops table the manifest describes.
func publicRelationKind(ctx context.Context, conn rowQuerier1, table string) (string, error) {
	var kind string
	err := conn.QueryRow(ctx,
		`SELECT c.relkind::text FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		 WHERE n.nspname = $1 AND c.relname = $2`, publicSchema, table,
	).Scan(&kind)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("%w: looking for %s.%s", ErrMigrationFailed, publicSchema, table)
	}
	return kind, nil
}

// publicTableState classifies public.<table> for the manifest: absent, a table
// ('r' or 'p'), or something else, which is an error rather than a skip.
func publicTableState(ctx context.Context, conn rowQuerier1, table string) (exists bool, err error) {
	kind, err := publicRelationKind(ctx, conn, table)
	if err != nil {
		return false, err
	}
	switch kind {
	case "":
		return false, nil
	case "r", "p":
		return true, nil
	}
	return false, fmt.Errorf("%w: %s.%s is not a table (relkind %q) but PublicReadColumns names a table",
		ErrMigrationFailed, publicSchema, table, kind)
}

// publicReadStatements returns the statements that make the runtime role's
// access to the ops schema equal PublicReadColumns: for each manifest table that
// EXISTS, revoke every privilege the role holds on it (table and column level)
// and grant SELECT on exactly the manifest columns. A manifest table that does
// not exist (an auth-only database) is skipped and logged by name;
// VerifyRuntimePosture requires the grants as soon as it does exist. They run
// inside applyRuntimeGrants' transaction, so a privilege they would remove is a
// narrowing that needs the same acknowledgement as any other.
//
// The migration login must own those tables or hold GRANT OPTION on them; if it
// does not, the statement fails with PostgreSQL's permission error rather than
// widening anything.
func publicReadStatements(ctx context.Context, conn querier, options Options, logger *slog.Logger) ([]string, error) {
	roleID, err := NewValidatedIdentifier(options.RuntimeRole)
	if err != nil {
		return nil, err
	}
	role := quoteIdentifier(roleID)
	var statements []string
	for _, entry := range PublicReadColumns() {
		exists, err := publicTableState(ctx, conn, entry.Table)
		if err != nil {
			return nil, err
		}
		if !exists {
			logger.WarnContext(ctx, "skipped public read grants: the table does not exist",
				"schema", publicSchema, "table", entry.Table)
			continue
		}
		tableID, err := NewValidatedIdentifier(entry.Table)
		if err != nil {
			return nil, err
		}
		columns := make([]string, 0, len(entry.Columns))
		for _, name := range entry.Columns {
			columnID, err := NewValidatedIdentifier(name)
			if err != nil {
				return nil, err
			}
			columns = append(columns, quoteIdentifier(columnID))
		}
		table := quoteIdentifier(mustValidatedIdentifier(publicSchema)) + "." + quoteIdentifier(tableID)
		statements = append(statements,
			fmt.Sprintf(`REVOKE ALL ON TABLE %s FROM %s`, table, role),
			fmt.Sprintf(`GRANT SELECT (%s) ON TABLE %s TO %s`, strings.Join(columns, ", "), table, role),
		)
		logger.InfoContext(ctx, "applying public read grants",
			"schema", publicSchema, "table", entry.Table, "columns", len(entry.Columns))
	}
	return statements, nil
}

// publicReadViolations compares the role's effective column privileges with
// PublicReadColumns in both directions: a manifest column the role cannot read
// on a table that exists is a violation, and so is any column privilege the
// manifest does not declare anywhere outside the auth schema. Table-level
// privileges are reported by relationViolations, so they are not repeated here.
func publicReadViolations(ctx context.Context, conn *pgx.Conn, options Options) ([]Violation, error) {
	declared := make(map[string]map[string]struct{}, len(PublicReadColumns()))
	for _, entry := range PublicReadColumns() {
		columns := make(map[string]struct{}, len(entry.Columns))
		for _, column := range entry.Columns {
			columns[column] = struct{}{}
		}
		declared[entry.Table] = columns
	}

	var violations []Violation
	for _, entry := range PublicReadColumns() {
		exists, err := publicTableState(ctx, conn, entry.Table)
		if err != nil {
			return nil, err
		}
		if !exists {
			continue
		}
		for _, column := range entry.Columns {
			var readable bool
			if err := conn.QueryRow(ctx,
				`SELECT has_column_privilege($1, format('%I.%I', $2::text, $3::text), $4, 'SELECT')`,
				options.RuntimeRole, publicSchema, entry.Table, column,
			).Scan(&readable); err != nil {
				return nil, fmt.Errorf("%w: column %s.%s.%s is missing or unreadable to the posture check",
					ErrMigrationFailed, publicSchema, entry.Table, column)
			}
			if !readable {
				violations = append(violations, Violation{
					Kind: "column", Schema: publicSchema, Object: entry.Table + "." + column, Privilege: "SELECT",
					Detail: "declared in PublicReadColumns but the runtime role cannot read it (missing grant)",
				})
			}
		}
	}

	// One literal statement: the role, the schema and the system-schema filter
	// (systemSchemaNames, systemSchemaPattern) are bound parameters.
	rows, err := conn.Query(ctx, `
		SELECT n.nspname, c.relname, c.relkind::text, a.attname, p.priv
		FROM pg_attribute a
		JOIN pg_class c ON c.oid = a.attrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		CROSS JOIN LATERAL unnest(ARRAY['SELECT','INSERT','UPDATE','REFERENCES']) AS p(priv)
		WHERE n.nspname <> ALL($3::text[]) AND n.nspname NOT LIKE $4
		  AND n.nspname <> $2
		  AND c.relkind IN ('r', 'p', 'v', 'm', 'f')
		  AND a.attnum > 0 AND NOT a.attisdropped
		  AND NOT has_table_privilege($1, c.oid, p.priv)
		  AND has_column_privilege($1, c.oid, a.attnum, p.priv)`,
		options.RuntimeRole, options.Schema, systemSchemaNames, systemSchemaPattern,
	)
	if err != nil {
		return nil, fmt.Errorf("%w: reading effective column privileges", ErrMigrationFailed)
	}
	defer rows.Close()
	for rows.Next() {
		var schema, table, kind, column, privilege string
		if err := rows.Scan(&schema, &table, &kind, &column, &privilege); err != nil {
			return nil, fmt.Errorf("%w: reading a column privilege", ErrMigrationFailed)
		}
		if schema == publicSchema && privilege == "SELECT" && (kind == "r" || kind == "p") {
			if _, ok := declared[table][column]; ok {
				continue
			}
		}
		violations = append(violations, Violation{
			Kind: "column", Schema: schema, Object: table + "." + column, Privilege: privilege,
			Detail: "not declared in PublicReadColumns",
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%w: reading effective column privileges", ErrMigrationFailed)
	}
	return violations, nil
}
