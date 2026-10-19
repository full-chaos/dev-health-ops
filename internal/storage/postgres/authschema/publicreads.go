package authschema

import (
	"context"
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
			"config", "allowed_domains", "encrypted_secrets",
		}},
		{Table: "organizations", Columns: []string{"id", "slug", "name", "is_active"}},
		{Table: "saml_assertion_replays", Columns: []string{"provider_id", "assertion_id", "expires_at"}},
		{Table: "org_licenses", Columns: []string{"org_id", "tier", "features_override"}},
	}
}

func publicTableExists(ctx context.Context, conn *pgx.Conn, table string) (bool, error) {
	var exists bool
	if err := conn.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		 WHERE n.nspname = $1 AND c.relname = $2 AND c.relkind IN ('r', 'p'))`, publicSchema, table,
	).Scan(&exists); err != nil {
		return false, fmt.Errorf("%w: looking for %s.%s", ErrMigrationFailed, publicSchema, table)
	}
	return exists, nil
}

// ApplyPublicReads makes the runtime role's access to the ops schema equal
// PublicReadColumns. For each manifest table that EXISTS it revokes every
// privilege the role holds on the table (table and column level) and grants
// SELECT on exactly the manifest columns. A manifest table that does not exist
// (an auth-only database) is skipped and logged by name; VerifyRuntimePosture
// requires the grants as soon as it does exist.
//
// The migration login must own those tables or hold GRANT OPTION on them; if it
// does not, this fails with PostgreSQL's permission error rather than widening
// anything.
func ApplyPublicReads(ctx context.Context, conn *pgx.Conn, options Options) error {
	roleID, err := NewValidatedIdentifier(options.RuntimeRole)
	if err != nil {
		return err
	}
	role := quoteIdentifier(roleID)
	logger := options.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	for _, entry := range PublicReadColumns() {
		exists, err := publicTableExists(ctx, conn, entry.Table)
		if err != nil {
			return err
		}
		if !exists {
			logger.WarnContext(ctx, "skipped public read grants: the table does not exist",
				"schema", publicSchema, "table", entry.Table)
			continue
		}
		tableID, err := NewValidatedIdentifier(entry.Table)
		if err != nil {
			return err
		}
		columns := make([]string, 0, len(entry.Columns))
		for _, name := range entry.Columns {
			columnID, err := NewValidatedIdentifier(name)
			if err != nil {
				return err
			}
			columns = append(columns, quoteIdentifier(columnID))
		}
		table := quoteIdentifier(mustValidatedIdentifier(publicSchema)) + "." + quoteIdentifier(tableID)
		for _, statement := range []string{
			fmt.Sprintf(`REVOKE ALL ON TABLE %s FROM %s`, table, role),
			fmt.Sprintf(`GRANT SELECT (%s) ON TABLE %s TO %s`, strings.Join(columns, ", "), table, role),
		} {
			if _, err := conn.Exec(ctx, statement); err != nil {
				return fmt.Errorf("%w: %s: %v", ErrMigrationFailed, statement, redactPGError(err))
			}
		}
		logger.InfoContext(ctx, "applied public read grants",
			"schema", publicSchema, "table", entry.Table, "columns", len(entry.Columns))
	}
	return nil
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
		exists, err := publicTableExists(ctx, conn, entry.Table)
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

	rows, err := conn.Query(ctx, `
		SELECT n.nspname, c.relname, a.attname, p.priv
		FROM pg_attribute a
		JOIN pg_class c ON c.oid = a.attrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		CROSS JOIN LATERAL unnest(ARRAY['SELECT','INSERT','UPDATE','REFERENCES']) AS p(priv)
		WHERE `+systemSchemaFilter+`
		  AND n.nspname <> $2
		  AND c.relkind IN ('r', 'p', 'v', 'm', 'f')
		  AND a.attnum > 0 AND NOT a.attisdropped
		  AND NOT has_table_privilege($1, c.oid, p.priv)
		  AND has_column_privilege($1, c.oid, a.attnum, p.priv)`,
		options.RuntimeRole, options.Schema,
	)
	if err != nil {
		return nil, fmt.Errorf("%w: reading effective column privileges", ErrMigrationFailed)
	}
	defer rows.Close()
	for rows.Next() {
		var schema, table, column, privilege string
		if err := rows.Scan(&schema, &table, &column, &privilege); err != nil {
			return nil, fmt.Errorf("%w: reading a column privilege", ErrMigrationFailed)
		}
		if schema == publicSchema && privilege == "SELECT" {
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
