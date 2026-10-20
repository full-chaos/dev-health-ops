package authschema

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

// sessionReaderStatements makes each session reader role hold exactly
// SessionReaderColumns in the auth schema: every privilege it holds there is
// revoked first (table-level revokes take the column privileges with them),
// then USAGE on the schema and the column SELECT are granted. A reader role
// that does not exist is refused by name.
func sessionReaderStatements(ctx context.Context, conn querier, options Options, schemaID ValidatedIdentifier) ([]string, error) {
	if len(options.SessionReaderRoles) == 0 {
		return nil, nil
	}
	read := SessionReaderColumns()
	tableID, err := NewValidatedIdentifier(read.Table)
	if err != nil {
		return nil, err
	}
	columns := make([]string, 0, len(read.Columns))
	for _, name := range read.Columns {
		columnID, err := NewValidatedIdentifier(name)
		if err != nil {
			return nil, err
		}
		columns = append(columns, quoteIdentifier(columnID))
	}
	schema := quoteIdentifier(schemaID)
	var statements []string
	for _, reader := range options.SessionReaderRoles {
		roleID, err := NewValidatedIdentifier(reader)
		if err != nil {
			return nil, err
		}
		var exists bool
		if err := conn.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = $1)`, reader).Scan(&exists); err != nil {
			return nil, fmt.Errorf("%w: reading session reader role %q: %v", ErrMigrationFailed, reader, redactPGError(err))
		}
		if !exists {
			return nil, fmt.Errorf("%w: session reader role %q does not exist", ErrInvalidOptions, reader)
		}
		role := quoteIdentifier(roleID)
		statements = append(statements,
			fmt.Sprintf(`REVOKE ALL ON ALL TABLES IN SCHEMA %s FROM %s`, schema, role),
			fmt.Sprintf(`REVOKE ALL ON ALL SEQUENCES IN SCHEMA %s FROM %s`, schema, role),
			fmt.Sprintf(`REVOKE ALL ON SCHEMA %s FROM %s`, schema, role),
			fmt.Sprintf(`GRANT USAGE ON SCHEMA %s TO %s`, schema, role),
			fmt.Sprintf(`GRANT SELECT (%s) ON %s.%s TO %s`, strings.Join(columns, ", "), schema, quoteIdentifier(tableID), role),
		)
	}
	return statements, nil
}

// sessionReaderViolations compares what each session reader role can do in
// the auth schema with SessionReaderColumns, in both directions: a declared
// column it cannot read, any other column or table privilege, any sequence
// privilege, CREATE on the schema, or no USAGE.
func sessionReaderViolations(ctx context.Context, conn *pgx.Conn, options Options) ([]Violation, error) {
	read := SessionReaderColumns()
	declared := map[string]bool{}
	for _, column := range read.Columns {
		declared[column] = true
	}
	var violations []Violation
	for _, reader := range options.SessionReaderRoles {
		if err := ValidateIdentifier(reader); err != nil {
			return nil, err
		}
		detail := "session reader " + reader
		var usage, create bool
		if err := conn.QueryRow(ctx, `SELECT has_schema_privilege($1, $2, 'USAGE'), has_schema_privilege($1, $2, 'CREATE')`,
			reader, options.Schema).Scan(&usage, &create); err != nil {
			return nil, fmt.Errorf("%w: schema privileges of session reader %q: %v", ErrMigrationFailed, reader, redactPGError(err))
		}
		if !usage {
			violations = append(violations, Violation{Kind: "session_reader", Schema: options.Schema, Privilege: "USAGE", Detail: detail + " lacks it"})
		}
		if create {
			violations = append(violations, Violation{Kind: "session_reader", Schema: options.Schema, Privilege: "CREATE", Detail: detail})
		}
		rows, err := conn.Query(ctx, `
			SELECT c.relname, c.relkind::text, p.priv,
			       has_table_privilege($1, c.oid, p.priv)
			FROM pg_class c
			JOIN pg_namespace n ON n.oid = c.relnamespace
			CROSS JOIN LATERAL unnest(ARRAY['SELECT','INSERT','UPDATE','DELETE','TRUNCATE','REFERENCES','TRIGGER']) AS p(priv)
			WHERE n.nspname = $2 AND c.relkind IN ('r', 'p', 'v', 'm', 'f')
			ORDER BY 1, 3`, reader, options.Schema)
		if err != nil {
			return nil, fmt.Errorf("%w: table privileges of session reader %q: %v", ErrMigrationFailed, reader, redactPGError(err))
		}
		for rows.Next() {
			var table, kind, privilege string
			var held bool
			if err := rows.Scan(&table, &kind, &privilege, &held); err != nil {
				rows.Close()
				return nil, fmt.Errorf("%w: scanning table privileges: %v", ErrMigrationFailed, redactPGError(err))
			}
			if held {
				violations = append(violations, Violation{Kind: "session_reader", Schema: options.Schema, Object: table, Privilege: privilege,
					Detail: detail + " holds a table-level privilege"})
			}
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("%w: table privileges of session reader %q: %v", ErrMigrationFailed, reader, redactPGError(err))
		}
		rows, err = conn.Query(ctx, `
			SELECT c.relname, a.attname, p.priv, has_column_privilege($1, c.oid, a.attnum, p.priv)
			FROM pg_attribute a
			JOIN pg_class c ON c.oid = a.attrelid
			JOIN pg_namespace n ON n.oid = c.relnamespace
			CROSS JOIN LATERAL unnest(ARRAY['SELECT','INSERT','UPDATE','REFERENCES']) AS p(priv)
			WHERE n.nspname = $2 AND c.relkind IN ('r', 'p', 'v', 'm', 'f') AND a.attnum > 0 AND NOT a.attisdropped
			ORDER BY 1, 2, 3`, reader, options.Schema)
		if err != nil {
			return nil, fmt.Errorf("%w: column privileges of session reader %q: %v", ErrMigrationFailed, reader, redactPGError(err))
		}
		for rows.Next() {
			var table, column, privilege string
			var held bool
			if err := rows.Scan(&table, &column, &privilege, &held); err != nil {
				rows.Close()
				return nil, fmt.Errorf("%w: scanning column privileges: %v", ErrMigrationFailed, redactPGError(err))
			}
			want := table == read.Table && declared[column] && privilege == "SELECT"
			switch {
			case held && !want:
				violations = append(violations, Violation{Kind: "session_reader", Schema: options.Schema, Object: table + "." + column,
					Privilege: privilege, Detail: detail + " holds an undeclared column privilege"})
			case !held && want:
				violations = append(violations, Violation{Kind: "session_reader", Schema: options.Schema, Object: table + "." + column,
					Privilege: privilege, Detail: detail + " cannot read a declared column (missing grant)"})
			}
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("%w: column privileges of session reader %q: %v", ErrMigrationFailed, reader, redactPGError(err))
		}
		var sequences int
		if err := conn.QueryRow(ctx, `
			SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
			CROSS JOIN LATERAL unnest(ARRAY['USAGE','SELECT','UPDATE']) AS p(priv)
			WHERE n.nspname = $2 AND c.relkind = 'S' AND has_sequence_privilege($1, c.oid, p.priv)`,
			reader, options.Schema).Scan(&sequences); err != nil {
			return nil, fmt.Errorf("%w: sequence privileges of session reader %q: %v", ErrMigrationFailed, reader, redactPGError(err))
		}
		if sequences > 0 {
			violations = append(violations, Violation{Kind: "session_reader", Schema: options.Schema, Privilege: "sequence",
				Detail: fmt.Sprintf("%s holds %d sequence privileges", detail, sequences)})
		}
	}
	return violations, nil
}
