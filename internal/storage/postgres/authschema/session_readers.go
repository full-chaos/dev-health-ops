package authschema

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

// sessionReaderStatements makes each session reader role hold exactly
// SessionReaderReads in the auth schema: every privilege it holds there is
// revoked first (table-level revokes take the column privileges with them),
// then USAGE on the schema and the column SELECTs are granted. A reader role
// that does not exist is refused by name.
func sessionReaderStatements(ctx context.Context, conn querier, options Options, schemaID ValidatedIdentifier) ([]string, error) {
	return readerStatements(ctx, conn, options, schemaID, options.SessionReaderRoles, SessionReaderReads(options.PlatformRolesOwned), "session reader")
}

// presentReads keeps the reads whose table exists in the schema.
func presentReads(ctx context.Context, conn querier, schema string, reads []ColumnGrant) ([]ColumnGrant, error) {
	var kept []ColumnGrant
	for _, read := range reads {
		var present bool
		if err := conn.QueryRow(ctx, `SELECT to_regclass(format('%I.%I', $1::text, $2::text)) IS NOT NULL`, schema, read.Table).Scan(&present); err != nil {
			return nil, fmt.Errorf("%w: reading whether table %q exists: %v", ErrMigrationFailed, read.Table, redactPGError(err))
		}
		if present {
			kept = append(kept, read)
		}
	}
	return kept, nil
}

func readerStatements(ctx context.Context, conn querier, options Options, schemaID ValidatedIdentifier, roles []string, reads []ColumnGrant, label string) ([]string, error) {
	if len(roles) == 0 {
		return nil, nil
	}
	tableIDs := make([]ValidatedIdentifier, len(reads))
	for i, read := range reads {
		tableID, err := NewValidatedIdentifier(read.Table)
		if err != nil {
			return nil, err
		}
		tableIDs[i] = tableID
	}
	schema := quoteIdentifier(schemaID)
	var statements []string
	for _, reader := range roles {
		roleID, err := NewValidatedIdentifier(reader)
		if err != nil {
			return nil, err
		}
		var exists bool
		if err := conn.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = $1)`, reader).Scan(&exists); err != nil {
			return nil, fmt.Errorf("%w: reading %s role %q: %v", ErrMigrationFailed, label, reader, redactPGError(err))
		}
		if !exists {
			return nil, fmt.Errorf("%w: %s role %q does not exist", ErrInvalidOptions, label, reader)
		}
		// The revokes below strip every ACL entry the reader holds in the
		// schema. For a reader that is, or inherits from, an owner of the
		// schema or of one of its tables or sequences, that strips the owner's
		// own entries; refuse before any statement runs.
		var owner string
		switch err := conn.QueryRow(ctx, `
			SELECT r.rolname FROM pg_roles r
			WHERE pg_has_role($1, r.oid, 'USAGE')
			  AND (r.oid = (SELECT nspowner FROM pg_namespace WHERE nspname = $2)
			       OR EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
			                  WHERE n.nspname = $2 AND c.relowner = r.oid AND c.relkind IN ('r', 'p', 'S', 'v', 'm', 'f')))
			ORDER BY r.rolname LIMIT 1`, reader, options.Schema).Scan(&owner); {
		case err == nil:
			return nil, fmt.Errorf("%w: %s role %q is, or inherits from, %q, which owns objects in schema %q",
				ErrInvalidOptions, label, reader, owner, options.Schema)
		case !errors.Is(err, pgx.ErrNoRows):
			return nil, fmt.Errorf("%w: reading ownership of %s role %q: %v", ErrMigrationFailed, label, reader, redactPGError(err))
		}
		role := quoteIdentifier(roleID)
		statements = append(statements,
			fmt.Sprintf(`REVOKE ALL ON ALL TABLES IN SCHEMA %s FROM %s`, schema, role),
			fmt.Sprintf(`REVOKE ALL ON ALL SEQUENCES IN SCHEMA %s FROM %s`, schema, role),
			fmt.Sprintf(`REVOKE ALL ON SCHEMA %s FROM %s`, schema, role),
			fmt.Sprintf(`GRANT USAGE ON SCHEMA %s TO %s`, schema, role),
		)
		for i, read := range reads {
			columns := make([]string, 0, len(read.Columns))
			for _, name := range read.Columns {
				columnID, err := NewValidatedIdentifier(name)
				if err != nil {
					return nil, err
				}
				columns = append(columns, quoteIdentifier(columnID))
			}
			statements = append(statements,
				fmt.Sprintf(`GRANT SELECT (%s) ON %s.%s TO %s`, strings.Join(columns, ", "), schema, quoteIdentifier(tableIDs[i]), role))
		}
	}
	return statements, nil
}

// sessionReaderViolations compares what each session reader role can do in
// the auth schema with SessionReaderReads, in both directions: a declared
// column it cannot read, any other column or table privilege, any sequence
// privilege, CREATE on the schema, or no USAGE.
func sessionReaderViolations(ctx context.Context, conn *pgx.Conn, options Options) ([]Violation, error) {
	return readerViolationsOf(ctx, conn, options, options.SessionReaderRoles, SessionReaderReads(options.PlatformRolesOwned), "session_reader")
}

// projectionReaderViolations is the same comparison for the projection readers.
func projectionReaderViolations(ctx context.Context, conn *pgx.Conn, options Options) ([]Violation, error) {
	reads, err := presentReads(ctx, conn, options.Schema, ProjectionReaderReads())
	if err != nil {
		return nil, err
	}
	return readerViolationsOf(ctx, conn, options, options.ProjectionReaderRoles, reads, "projection_reader")
}

func readerViolationsOf(ctx context.Context, conn *pgx.Conn, options Options, roles []string, reads []ColumnGrant, kind string) ([]Violation, error) {
	declared := map[string]map[string]bool{}
	label := strings.ReplaceAll(kind, "_", " ")
	for _, read := range reads {
		declared[read.Table] = map[string]bool{}
		for _, column := range read.Columns {
			declared[read.Table][column] = true
		}
	}
	var violations []Violation
	for _, reader := range roles {
		if err := ValidateIdentifier(reader); err != nil {
			return nil, err
		}
		detail := label + " " + reader
		var usage, create bool
		if err := conn.QueryRow(ctx, `SELECT has_schema_privilege($1, $2, 'USAGE'), has_schema_privilege($1, $2, 'CREATE')`,
			reader, options.Schema).Scan(&usage, &create); err != nil {
			return nil, fmt.Errorf("%w: schema privileges of %s %q: %v", ErrMigrationFailed, label, reader, redactPGError(err))
		}
		if !usage {
			violations = append(violations, Violation{Kind: kind, Schema: options.Schema, Privilege: "USAGE", Detail: detail + " lacks it"})
		}
		if create {
			violations = append(violations, Violation{Kind: kind, Schema: options.Schema, Privilege: "CREATE", Detail: detail})
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
			return nil, fmt.Errorf("%w: table privileges of %s %q: %v", ErrMigrationFailed, label, reader, redactPGError(err))
		}
		for rows.Next() {
			var table, kind, privilege string
			var held bool
			if err := rows.Scan(&table, &kind, &privilege, &held); err != nil {
				rows.Close()
				return nil, fmt.Errorf("%w: scanning table privileges: %v", ErrMigrationFailed, redactPGError(err))
			}
			if held {
				violations = append(violations, Violation{Kind: kind, Schema: options.Schema, Object: table, Privilege: privilege,
					Detail: detail + " holds a table-level privilege"})
			}
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("%w: table privileges of %s %q: %v", ErrMigrationFailed, label, reader, redactPGError(err))
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
			return nil, fmt.Errorf("%w: column privileges of %s %q: %v", ErrMigrationFailed, label, reader, redactPGError(err))
		}
		for rows.Next() {
			var table, column, privilege string
			var held bool
			if err := rows.Scan(&table, &column, &privilege, &held); err != nil {
				rows.Close()
				return nil, fmt.Errorf("%w: scanning column privileges: %v", ErrMigrationFailed, redactPGError(err))
			}
			want := declared[table][column] && privilege == "SELECT"
			switch {
			case held && !want:
				violations = append(violations, Violation{Kind: kind, Schema: options.Schema, Object: table + "." + column,
					Privilege: privilege, Detail: detail + " holds an undeclared column privilege"})
			case !held && want:
				violations = append(violations, Violation{Kind: kind, Schema: options.Schema, Object: table + "." + column,
					Privilege: privilege, Detail: detail + " cannot read a declared column (missing grant)"})
			}
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("%w: column privileges of %s %q: %v", ErrMigrationFailed, label, reader, redactPGError(err))
		}
		var sequences int
		if err := conn.QueryRow(ctx, `
			SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
			CROSS JOIN LATERAL unnest(ARRAY['USAGE','SELECT','UPDATE']) AS p(priv)
			WHERE n.nspname = $2 AND c.relkind = 'S' AND has_sequence_privilege($1, c.oid, p.priv)`,
			reader, options.Schema).Scan(&sequences); err != nil {
			return nil, fmt.Errorf("%w: sequence privileges of %s %q: %v", ErrMigrationFailed, label, reader, redactPGError(err))
		}
		if sequences > 0 {
			violations = append(violations, Violation{Kind: kind, Schema: options.Schema, Privilege: "sequence",
				Detail: fmt.Sprintf("%s holds %d sequence privileges", detail, sequences)})
		}
	}
	return violations, nil
}
